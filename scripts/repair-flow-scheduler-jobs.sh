#!/usr/bin/env bash
#
# Resume existing Arco Flow Cloud Scheduler jobs for deployed UAT.
#
# This repairs paused Scheduler jobs only. It does not create Scheduler jobs,
# Cloud Run services, IAM bindings, queues, buckets, or Terraform state.
#
set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
DRY_RUN=false
REPAIR_TARGETS=false
DEPLOY_LOCK_DIR=""
DEPLOY_LOCK_ACQUIRED=false

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV    Environment (dev, staging, prod). Default: dev
  --repair-targets
               Also repair HTTP target URI, method, OIDC service account,
               and OIDC audience for dispatcher/sweeper jobs
  --dry-run    Print gcloud commands without resuming Scheduler jobs
  -h, --help   Show this help message

Required env vars:
  PROJECT_ID
  PROJECT_NUMBER or ARCO_UAT_CLOUD_RUN_PROJECT_NUMBER
               Required with --repair-targets

Required env vars for non-dry-run:
  ARCO_DEPLOY_OWNER   Explicit single-owner label for the live repair window

Optional env vars:
  REGION
  ARCO_DEPLOY_LOCK_DIR    Override the local deploy lock directory
  ARCO_DEPLOY_LOCK_HELD   Set to true when a wrapper already holds the lock
  ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
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

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    print_command "$@"
  else
    "$@"
  fi
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

  DEPLOY_LOCK_DIR="$(deploy_lock_dir)"
  parent_dir="${DEPLOY_LOCK_DIR%/*}"
  if [[ "$parent_dir" == "$DEPLOY_LOCK_DIR" ]]; then
    parent_dir="."
  fi
  mkdir -p "$parent_dir"

  if ! mkdir "$DEPLOY_LOCK_DIR" 2>/dev/null; then
    print_existing_deploy_lock
    die "Refusing live Scheduler repair while deploy lock exists: $DEPLOY_LOCK_DIR"
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

flow_scheduler_jobs() {
  printf '%s\n' "arco-flow-dispatcher-run-${ENVIRONMENT}"
  printf '%s\n' "arco-flow-sweeper-run-${ENVIRONMENT}"
}

cloud_run_project_number() {
  printf '%s' "${ARCO_UAT_CLOUD_RUN_PROJECT_NUMBER:-${PROJECT_NUMBER:-}}"
}

flow_scheduler_invoker_service_account() {
  printf '%s' "${ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT:-arco-invoker-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com}"
}

flow_scheduler_target_specs() {
  local project_number
  project_number="$(cloud_run_project_number)"
  printf '%s %s\n' "arco-flow-dispatcher-run-${ENVIRONMENT}" "https://arco-flow-dispatcher-${ENVIRONMENT}-${project_number}.${REGION}.run.app"
  printf '%s %s\n' "arco-flow-sweeper-run-${ENVIRONMENT}" "https://arco-flow-sweeper-${ENVIRONMENT}-${project_number}.${REGION}.run.app"
}

validate_env() {
  [[ -n "${PROJECT_ID:-}" ]] || die "PROJECT_ID is required"

  case "$ENVIRONMENT" in
  dev | staging | prod) ;;
  *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac

  if [[ "$REPAIR_TARGETS" == "true" && -z "$(cloud_run_project_number)" ]]; then
    die "PROJECT_NUMBER or ARCO_UAT_CLOUD_RUN_PROJECT_NUMBER is required with --repair-targets"
  fi

  if [[ "$DRY_RUN" == "false" ]]; then
    [[ -n "${ARCO_DEPLOY_OWNER:-}" ]] || die "ARCO_DEPLOY_OWNER is required for live Scheduler repair; set it before mutating ${PROJECT_ID}/${REGION}/${ENVIRONMENT}."
    if ! [[ "$ARCO_DEPLOY_OWNER" =~ ^[a-z0-9]([-a-z0-9_]{0,61}[a-z0-9])?$ ]]; then
      die "ARCO_DEPLOY_OWNER must be a GCP label-safe value: lowercase letters, digits, '-' or '_', 1-63 chars, starting and ending with a letter or digit."
    fi
  fi
}

repair_scheduler_jobs() {
  local job state

  if [[ "$DRY_RUN" == "true" ]]; then
    while IFS= read -r job; do
      [[ -n "$job" ]] || continue
      run_cmd gcloud scheduler jobs describe "$job" \
        "--project=${PROJECT_ID}" \
        "--location=${REGION}" \
        "--format=value(state)"
      run_cmd gcloud scheduler jobs resume "$job" \
        "--project=${PROJECT_ID}" \
        "--location=${REGION}" \
        --quiet
    done < <(flow_scheduler_jobs)
    return 0
  fi

  require_cmd gcloud
  while IFS= read -r job; do
    [[ -n "$job" ]] || continue
    if ! state="$(gcloud scheduler jobs describe "$job" \
      "--project=${PROJECT_ID}" \
      "--location=${REGION}" \
      "--format=value(state)")"; then
      die "Unable to inspect Scheduler job '${job}' in ${PROJECT_ID}/${REGION}"
    fi
    state="$(printf '%s' "$state" | tr -d '[:space:]')"
    case "$state" in
    ENABLED)
      echo "${job}=ENABLED (no resume needed)"
      ;;
    PAUSED)
      gcloud scheduler jobs resume "$job" \
        "--project=${PROJECT_ID}" \
        "--location=${REGION}" \
        --quiet
      ;;
    *)
      die "Scheduler job '${job}' state is ${state:-unknown}; expected ENABLED or PAUSED"
      ;;
    esac
  done < <(flow_scheduler_jobs)
}

repair_scheduler_targets() {
  local job service_url invoker job_json state uri audience service_account http_method
  local expected_uri

  if [[ "$REPAIR_TARGETS" != "true" ]]; then
    return 0
  fi

  invoker="$(flow_scheduler_invoker_service_account)"
  if [[ "$DRY_RUN" == "false" ]]; then
    require_cmd gcloud
    require_cmd jq
  fi

  while read -r job service_url; do
    [[ -n "$job" ]] || continue
    expected_uri="${service_url}/run"
    if [[ "$DRY_RUN" == "false" ]]; then
      if ! job_json="$(gcloud scheduler jobs describe "$job" \
        "--project=${PROJECT_ID}" \
        "--location=${REGION}" \
        "--format=json(state,httpTarget)")"; then
        die "Unable to inspect Scheduler target '${job}' in ${PROJECT_ID}/${REGION}"
      fi
      state="$(jq -r '.state // ""' <<<"$job_json")"
      uri="$(jq -r '.httpTarget.uri // ""' <<<"$job_json")"
      audience="$(jq -r '.httpTarget.oidcToken.audience // ""' <<<"$job_json")"
      service_account="$(jq -r '.httpTarget.oidcToken.serviceAccountEmail // ""' <<<"$job_json")"
      http_method="$(jq -r '.httpTarget.httpMethod // ""' <<<"$job_json" | tr '[:lower:]' '[:upper:]')"

      if [[ "$uri" == "$expected_uri" &&
        "$audience" == "$service_url" &&
        "$service_account" == "$invoker" &&
        "$http_method" == "POST" ]]; then
        echo "${job}=target ready (state=${state:-unknown}; no update needed)"
        continue
      fi
    fi

    run_cmd gcloud scheduler jobs update http "$job" \
      "--project=${PROJECT_ID}" \
      "--location=${REGION}" \
      "--uri=${expected_uri}" \
      "--http-method=post" \
      "--oidc-service-account-email=${invoker}" \
      "--oidc-token-audience=${service_url}" \
      --quiet
  done < <(flow_scheduler_target_specs)
}

main() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
    --env)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --repair-targets)
      REPAIR_TARGETS=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
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
  repair_scheduler_jobs
  repair_scheduler_targets
}

main "$@"
