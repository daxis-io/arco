#!/usr/bin/env bash
#
# Refresh existing Arco Cloud Run service revisions without Terraform state changes.
#
# This is an escape hatch for dev/UAT environments where services already exist
# outside the local Terraform state. It updates images and revision-scoped env
# vars only; it does not create services, IAM bindings, buckets, queues, or jobs.
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DEPLOY_OWNER_LABEL="arco_deploy_owner"

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
DRY_RUN=false
SCOPE_ONLY=false
DEPLOY_LOCK_DIR=""
DEPLOY_LOCK_ACQUIRED=false
REFRESH_EXPECTATIONS=()

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV      Environment (dev, staging, prod). Default: dev
  --dry-run      Print gcloud commands without updating Cloud Run services
  --scope-only   Update deployed tenant/workspace env vars without changing images
  -h, --help     Show this help message

Required env vars:
  PROJECT_ID
  API_IMAGE              Not required with --scope-only
  COMPACTOR_IMAGE        Not required with --scope-only
  FLOW_COMPACTOR_IMAGE   Not required with --scope-only

Optional env vars:
  REGION
  ARCO_DEPLOY_OWNER       Required for non-dry-run live deployment mutations
                          Must be a GCP label-safe value so services can record ownership
  ARCO_DEPLOY_LOCK_DIR    Override the local deploy lock directory
  ARCO_DEPLOY_LOCK_HELD   Set to true when a parent repair wrapper already
                          holds ARCO_DEPLOY_LOCK_DIR
  API_GIT_SHA
  API_CODE_VERSION
  FLOW_TENANT_ID or TF_VAR_flow_tenant_id
  FLOW_WORKSPACE_ID or TF_VAR_flow_workspace_id
  FLOW_COMPACTOR_INGRESS  Optional explicit flow compactor ingress: internal or all
  FLOW_DISPATCHER_IMAGE
  FLOW_SWEEPER_IMAGE
  FLOW_TIMER_INGEST_IMAGE
  FLOW_WORKER_IMAGE
  FLOW_AUTOMATION_RECONCILER_IMAGE
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

deploy_lock_dir() {
  if [[ -n "${ARCO_DEPLOY_LOCK_DIR:-}" ]]; then
    printf '%s' "$ARCO_DEPLOY_LOCK_DIR"
    return 0
  fi

  printf '%s/arco-deploy-locks/%s-%s-%s' "${TMPDIR:-/tmp}" "$PROJECT_ID" "$REGION" "$ENVIRONMENT"
}

release_deploy_lock() {
  if [[ "$DEPLOY_LOCK_ACQUIRED" == "true" && -n "$DEPLOY_LOCK_DIR" ]]; then
    rm -f "${DEPLOY_LOCK_DIR}/owner"
    rmdir "$DEPLOY_LOCK_DIR" >/dev/null 2>&1 || true
  fi
}

print_existing_deploy_lock() {
  local owner_file="${DEPLOY_LOCK_DIR}/owner"
  echo "Another live deployment appears to own ${PROJECT_ID}/${REGION}/${ENVIRONMENT}." >&2
  if [[ -f "$owner_file" ]]; then
    echo "Active deploy lock metadata:" >&2
    while IFS= read -r line; do
      echo "  $line" >&2
    done <"$owner_file"
  fi
}

acquire_live_deploy_lock() {
  local parent_dir host started_at

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  if [[ "${ARCO_DEPLOY_LOCK_HELD:-}" == "true" ]]; then
    return 0
  fi

  if [[ -z "${ARCO_DEPLOY_OWNER:-}" ]]; then
    die "ARCO_DEPLOY_OWNER is required for live deployment mutations; set it to a human/session label before mutating ${PROJECT_ID}/${REGION}/${ENVIRONMENT}."
  fi

  if ! [[ "$ARCO_DEPLOY_OWNER" =~ ^[a-z0-9]([-a-z0-9_]{0,61}[a-z0-9])?$ ]]; then
    die "ARCO_DEPLOY_OWNER must be a GCP label-safe value: lowercase letters, digits, '-' or '_', 1-63 chars, starting and ending with a letter or digit."
  fi

  DEPLOY_LOCK_DIR="$(deploy_lock_dir)"
  parent_dir="${DEPLOY_LOCK_DIR%/*}"
  if [[ "$parent_dir" == "$DEPLOY_LOCK_DIR" ]]; then
    parent_dir="."
  fi
  mkdir -p "$parent_dir"

  if ! mkdir "$DEPLOY_LOCK_DIR" 2>/dev/null; then
    print_existing_deploy_lock
    die "Refusing live deployment mutation while deploy lock exists: $DEPLOY_LOCK_DIR"
  fi

  DEPLOY_LOCK_ACQUIRED=true
  trap release_deploy_lock EXIT

  host="$(hostname 2>/dev/null || echo unknown)"
  started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  {
    printf 'owner=%s\n' "$ARCO_DEPLOY_OWNER"
    printf 'pid=%s\n' "$$"
    printf 'host=%s\n' "$host"
    printf 'started_at=%s\n' "$started_at"
    printf 'script=%s\n' "$0"
    printf 'project=%s\n' "$PROJECT_ID"
    printf 'region=%s\n' "$REGION"
    printf 'environment=%s\n' "$ENVIRONMENT"
  } >"${DEPLOY_LOCK_DIR}/owner"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

target_cloud_run_services() {
  local tenant_id workspace_id
  tenant_id="$(flow_tenant_id)"
  workspace_id="$(flow_workspace_id)"

  if [[ "$SCOPE_ONLY" == "true" ]]; then
    printf '%s\n' "arco-compactor-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-compactor-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-worker-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-timer-ingest-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-dispatcher-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-sweeper-${ENVIRONMENT}"
    return 0
  fi

  printf '%s\n' "arco-api-${ENVIRONMENT}"
  printf '%s\n' "arco-compactor-${ENVIRONMENT}"
  printf '%s\n' "arco-flow-compactor-${ENVIRONMENT}"

  if [[ -n "$tenant_id" && -n "$workspace_id" && -n "${FLOW_DISPATCHER_IMAGE:-}" && -n "${FLOW_SWEEPER_IMAGE:-}" && -n "${FLOW_TIMER_INGEST_IMAGE:-}" && -n "${FLOW_WORKER_IMAGE:-}" ]]; then
    printf '%s\n' "arco-flow-worker-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-timer-ingest-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-dispatcher-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-sweeper-${ENVIRONMENT}"
  fi

  if [[ -n "$tenant_id" && -n "$workspace_id" && -n "${FLOW_AUTOMATION_RECONCILER_IMAGE:-}" ]]; then
    printf '%s\n' "arco-flow-automation-reconciler-${ENVIRONMENT}"
  fi
}

cloud_run_deploy_owner() {
  local service_name="$1"
  local output

  if output="$(gcloud run services describe "$service_name" \
    "--project=${PROJECT_ID}" \
    "--region=${REGION}" \
    "--format=value(metadata.labels.${DEPLOY_OWNER_LABEL})" 2>&1)"; then
    printf '%s' "$output"
    return 0
  fi

  die "Unable to inspect Cloud Run deploy owner for '${service_name}': $output"
}

cloud_run_env_value() {
  local service_json="$1"
  local env_name="$2"
  jq -r --arg name "$env_name" \
    'first(.spec.template.spec.containers[0].env[]? | select(.name == $name) | .value) // ""' \
    <<<"$service_json"
}

cloud_run_service_json() {
  local service_name="$1"
  local output

  if output="$(gcloud run services describe "$service_name" \
    "--project=${PROJECT_ID}" \
    "--region=${REGION}" \
    "--format=json(metadata.labels,spec.template.spec.containers,status.latestReadyRevisionName,status.traffic)" 2>&1)"; then
    printf '%s' "$output"
    return 0
  fi

  die "Cloud Run revision refresh verification failed: ${service_name} could not be described after update: $output"
}

verify_cloud_run_service_update() {
  local service_name="$1"
  local expected_image="$2"
  local expected_env_vars="${3:-}"
  local service_json owner actual_image latest_ready traffic_percent
  local pair env_name expected_value actual_value
  local -a env_pairs=()

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  service_json="$(cloud_run_service_json "$service_name")"

  owner="$(jq -r --arg label "$DEPLOY_OWNER_LABEL" '.metadata.labels[$label] // ""' <<<"$service_json")"
  if [[ "$owner" != "$ARCO_DEPLOY_OWNER" ]]; then
    die "Cloud Run revision refresh verification failed: ${service_name} ${DEPLOY_OWNER_LABEL}=${owner:-unset}; expected ${ARCO_DEPLOY_OWNER}"
  fi

  if [[ -n "$expected_image" ]]; then
    actual_image="$(jq -r 'first(.spec.template.spec.containers[]?.image) // ""' <<<"$service_json")"
    if [[ "$actual_image" != "$expected_image" ]]; then
      die "Cloud Run revision refresh verification failed: ${service_name} image=${actual_image:-unset}; expected ${expected_image}"
    fi
  fi

  if [[ -n "$expected_env_vars" ]]; then
    IFS=, read -r -a env_pairs <<<"$expected_env_vars"
    for pair in "${env_pairs[@]}"; do
      [[ -n "$pair" ]] || continue
      env_name="${pair%%=*}"
      expected_value="${pair#*=}"
      actual_value="$(cloud_run_env_value "$service_json" "$env_name")"
      if [[ "$actual_value" != "$expected_value" ]]; then
        die "Cloud Run revision refresh verification failed: ${service_name} ${env_name}=${actual_value:-unset}; expected ${expected_value}"
      fi
    done
  fi

  latest_ready="$(jq -r '.status.latestReadyRevisionName // ""' <<<"$service_json")"
  if [[ -z "$latest_ready" ]]; then
    die "Cloud Run revision refresh verification failed: ${service_name} latestReadyRevisionName is unset"
  fi

  traffic_percent="$(jq -r --arg revision "$latest_ready" 'first(.status.traffic[]? | select(.revisionName == $revision) | .percent) // ""' <<<"$service_json")"
  if [[ "$traffic_percent" != "100" ]]; then
    die "Cloud Run revision refresh verification failed: ${service_name} latestReadyRevision=${latest_ready} traffic=${traffic_percent:-unset}; expected 100"
  fi
}

record_cloud_run_service_expectation() {
  local service_name="$1"
  local expected_image="$2"
  local expected_env_vars="${3:-}"

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  REFRESH_EXPECTATIONS+=("${service_name}|${expected_image}|${expected_env_vars}")
}

verify_all_cloud_run_service_updates() {
  local expectation service_name expected_image expected_env_vars

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  for expectation in "${REFRESH_EXPECTATIONS[@]}"; do
    IFS='|' read -r service_name expected_image expected_env_vars <<<"$expectation"
    verify_cloud_run_service_update "$service_name" "$expected_image" "$expected_env_vars"
  done
}

preflight_cloud_deploy_owner() {
  local service_name current_owner

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  while IFS= read -r service_name; do
    [[ -n "$service_name" ]] || continue
    current_owner="$(cloud_run_deploy_owner "$service_name")"
    if [[ -n "$current_owner" && "$current_owner" != "$ARCO_DEPLOY_OWNER" ]]; then
      die "Cloud Run deploy owner mismatch for ${service_name}: current ${DEPLOY_OWNER_LABEL}=${current_owner}, requested ARCO_DEPLOY_OWNER=${ARCO_DEPLOY_OWNER}."
    fi
  done < <(target_cloud_run_services)
}

print_command() {
  local arg
  printf '%s' "$1"
  shift
  for arg in "$@"; do
    printf ' %s' "$arg"
  done
  printf '\n'
}

api_git_sha() {
  if [[ -n "${API_GIT_SHA:-}" ]]; then
    printf '%s' "$API_GIT_SHA"
    return 0
  fi

  git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || true
}

flow_tenant_id() {
  printf '%s' "${FLOW_TENANT_ID:-${TF_VAR_flow_tenant_id:-}}"
}

flow_workspace_id() {
  printf '%s' "${FLOW_WORKSPACE_ID:-${TF_VAR_flow_workspace_id:-}}"
}

validate_flow_core_config() {
  local tenant_id workspace_id
  tenant_id="$(flow_tenant_id)"
  workspace_id="$(flow_workspace_id)"

  if [[ -n "${tenant_id}${workspace_id}${FLOW_DISPATCHER_IMAGE:-}${FLOW_SWEEPER_IMAGE:-}${FLOW_TIMER_INGEST_IMAGE:-}${FLOW_WORKER_IMAGE:-}" ]]; then
    if [[ -z "$tenant_id" || -z "$workspace_id" || -z "${FLOW_DISPATCHER_IMAGE:-}" || -z "${FLOW_SWEEPER_IMAGE:-}" || -z "${FLOW_TIMER_INGEST_IMAGE:-}" || -z "${FLOW_WORKER_IMAGE:-}" ]]; then
      die "Flow control-plane config is partial. Set FLOW_TENANT_ID, FLOW_WORKSPACE_ID, and flow dispatcher/sweeper/timer-ingest/worker images together."
    fi
  fi

  if [[ -n "${FLOW_AUTOMATION_RECONCILER_IMAGE:-}" && (-z "$tenant_id" || -z "$workspace_id") ]]; then
    die "Flow automation reconciler image requires FLOW_TENANT_ID and FLOW_WORKSPACE_ID."
  fi
}

validate_flow_compactor_ingress() {
  case "${FLOW_COMPACTOR_INGRESS:-}" in
  "" | internal | all) ;;
  *) die "FLOW_COMPACTOR_INGRESS must be one of: internal, all" ;;
  esac
}

validate_env() {
  [[ -n "${PROJECT_ID:-}" ]] || die "PROJECT_ID is required"

  case "$ENVIRONMENT" in
  dev | staging | prod) ;;
  *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac

  if [[ "$SCOPE_ONLY" == "true" ]]; then
    [[ -n "$(flow_tenant_id)" ]] || die "FLOW_TENANT_ID or TF_VAR_flow_tenant_id is required for --scope-only"
    [[ -n "$(flow_workspace_id)" ]] || die "FLOW_WORKSPACE_ID or TF_VAR_flow_workspace_id is required for --scope-only"
    validate_flow_compactor_ingress
    return 0
  fi

  [[ -n "${API_IMAGE:-}" ]] || die "API_IMAGE is required"
  [[ -n "${COMPACTOR_IMAGE:-}" ]] || die "COMPACTOR_IMAGE is required"
  [[ -n "${FLOW_COMPACTOR_IMAGE:-}" ]] || die "FLOW_COMPACTOR_IMAGE is required"

  validate_flow_core_config
  validate_flow_compactor_ingress
}

join_csv() {
  local IFS=,
  printf '%s' "$*"
}

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    print_command "$@"
  else
    "$@"
  fi
}

update_cloud_run_service() {
  local service_name="$1"
  local image="$2"
  local env_vars="${3:-}"
  local ingress="${4:-}"
  local cmd=(
    gcloud run services update "$service_name"
    "--project=${PROJECT_ID}"
    "--region=${REGION}"
  )

  if [[ -n "$image" ]]; then
    cmd+=(--image "$image")
  fi

  if [[ -n "$env_vars" ]]; then
    cmd+=(--update-env-vars "$env_vars")
  fi

  if [[ -n "$ingress" ]]; then
    cmd+=(--ingress "$ingress")
  fi

  if [[ "$DRY_RUN" != "true" ]]; then
    cmd+=(--update-labels "${DEPLOY_OWNER_LABEL}=${ARCO_DEPLOY_OWNER}")
  fi

  cmd+=(--quiet)
  run_cmd "${cmd[@]}"
  record_cloud_run_service_expectation "$service_name" "$image" "$env_vars"
  verify_cloud_run_service_update "$service_name" "$image" "$env_vars"
}

refresh_revisions() {
  local git_sha tenant_id workspace_id
  git_sha="$(api_git_sha)"
  tenant_id="$(flow_tenant_id)"
  workspace_id="$(flow_workspace_id)"

  if [[ "$SCOPE_ONLY" == "true" ]]; then
    update_cloud_run_service \
      "arco-compactor-${ENVIRONMENT}" \
      "" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-compactor-${ENVIRONMENT}" \
      "" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")" \
      "${FLOW_COMPACTOR_INGRESS:-}"
    update_cloud_run_service \
      "arco-flow-worker-${ENVIRONMENT}" \
      "" \
      "$(join_csv "ARCO_FLOW_TENANT_ID=${tenant_id}" "ARCO_FLOW_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-timer-ingest-${ENVIRONMENT}" \
      "" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-dispatcher-${ENVIRONMENT}" \
      "" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-sweeper-${ENVIRONMENT}" \
      "" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
    return 0
  fi

  update_cloud_run_service \
    "arco-api-${ENVIRONMENT}" \
    "$API_IMAGE" \
    "$(join_csv "ARCO_GIT_SHA=${git_sha}" "ARCO_API_IMAGE=${API_IMAGE}" "ARCO_CODE_VERSION=${API_CODE_VERSION:-}" "ARCO_COMPACTOR_AUTH_MODE=gcp_id_token")"

  if [[ -n "$tenant_id" && -n "$workspace_id" ]]; then
    update_cloud_run_service \
      "arco-compactor-${ENVIRONMENT}" \
      "$COMPACTOR_IMAGE" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
  else
    update_cloud_run_service "arco-compactor-${ENVIRONMENT}" "$COMPACTOR_IMAGE"
  fi

  if [[ -n "$tenant_id" && -n "$workspace_id" ]]; then
    update_cloud_run_service \
      "arco-flow-compactor-${ENVIRONMENT}" \
      "$FLOW_COMPACTOR_IMAGE" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")" \
      "${FLOW_COMPACTOR_INGRESS:-}"
  else
    update_cloud_run_service "arco-flow-compactor-${ENVIRONMENT}" "$FLOW_COMPACTOR_IMAGE" "" "${FLOW_COMPACTOR_INGRESS:-}"
  fi

  if [[ -n "$tenant_id" && -n "$workspace_id" && -n "${FLOW_DISPATCHER_IMAGE:-}" && -n "${FLOW_SWEEPER_IMAGE:-}" && -n "${FLOW_TIMER_INGEST_IMAGE:-}" && -n "${FLOW_WORKER_IMAGE:-}" ]]; then
    update_cloud_run_service \
      "arco-flow-worker-${ENVIRONMENT}" \
      "$FLOW_WORKER_IMAGE" \
      "$(join_csv "ARCO_FLOW_TENANT_ID=${tenant_id}" "ARCO_FLOW_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-timer-ingest-${ENVIRONMENT}" \
      "$FLOW_TIMER_INGEST_IMAGE" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-dispatcher-${ENVIRONMENT}" \
      "$FLOW_DISPATCHER_IMAGE" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
    update_cloud_run_service \
      "arco-flow-sweeper-${ENVIRONMENT}" \
      "$FLOW_SWEEPER_IMAGE" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
  fi

  if [[ -n "$tenant_id" && -n "$workspace_id" && -n "${FLOW_AUTOMATION_RECONCILER_IMAGE:-}" ]]; then
    update_cloud_run_service \
      "arco-flow-automation-reconciler-${ENVIRONMENT}" \
      "$FLOW_AUTOMATION_RECONCILER_IMAGE" \
      "$(join_csv "ARCO_TENANT_ID=${tenant_id}" "ARCO_WORKSPACE_ID=${workspace_id}")"
  fi
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --env)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --scope-only)
      SCOPE_ONLY=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage
      die "Unknown option: $1"
      ;;
    esac
  done

  validate_env

  acquire_live_deploy_lock

  if [[ "$DRY_RUN" != "true" ]]; then
    require_cmd gcloud
    require_cmd jq
    preflight_cloud_deploy_owner
  fi

  refresh_revisions
  verify_all_cloud_run_service_updates
}

main "$@"
