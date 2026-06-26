#!/usr/bin/env bash
#
# Repair deployed UAT prerequisites in the order required before a full
# deployed API/worker acceptance run can produce evidence.
#
# This script composes existing narrow repair helpers. It does not create
# Terraform resources, services, IAM, queues, buckets, or evidence artifacts.
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
DRY_RUN=false
RUN_LIVE_DEPLOYED=false
CLOUD_RUN_SERVICE=""
STATUS_OUTPUT_DIR=""
DEPLOY_LOCK_DIR=""
DEPLOY_LOCK_ACQUIRED=false

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV                 Environment (dev, staging, prod). Default: dev
  --cloud-run-service NAME  API Cloud Run service for final UAT status.
                            Default: arco-api-<env>
  --status-output-dir DIR   Save strict readiness snapshots as
                            initial-status.txt and final-status.txt.
  --run-live-deployed       After repair and strict readiness, run the full
                            deployed API/worker UAT gate under the same local
                            owner window.
  --dry-run                 Print or dry-run repair commands without mutating
  -h, --help                Show this help message

Required env vars:
  PROJECT_ID
  PROJECT_NUMBER or ARCO_UAT_CLOUD_RUN_PROJECT_NUMBER
  FLOW_TENANT_ID or TF_VAR_flow_tenant_id
  FLOW_WORKSPACE_ID or TF_VAR_flow_workspace_id
  ARCO_UAT_EXPECTED_API_CODE_VERSION
  ARCO_UAT_EXPECTED_API_GIT_SHA
  ARCO_UAT_EXPECTED_API_IMAGE

Required env vars for non-dry-run:
  ARCO_DEPLOY_OWNER   Explicit single-owner label for the live repair window

Optional env vars:
  REGION
  ARCO_DEPLOY_LOCK_DIR    Override the local deploy lock directory
  ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
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

project_number() {
  printf '%s' "${ARCO_UAT_CLOUD_RUN_PROJECT_NUMBER:-${PROJECT_NUMBER:-}}"
}

flow_tenant_id() {
  printf '%s' "${FLOW_TENANT_ID:-${TF_VAR_flow_tenant_id:-}}"
}

flow_workspace_id() {
  printf '%s' "${FLOW_WORKSPACE_ID:-${TF_VAR_flow_workspace_id:-}}"
}

deploy_lock_dir() {
  if [[ -n "${ARCO_DEPLOY_LOCK_DIR:-}" ]]; then
    printf '%s' "$ARCO_DEPLOY_LOCK_DIR"
    return 0
  fi

  printf '%s/arco-deploy-locks/%s-%s-%s' "${TMPDIR:-/tmp}" "$PROJECT_ID" "$REGION" "$ENVIRONMENT"
}

cloud_run_service() {
  printf '%s' "${CLOUD_RUN_SERVICE:-arco-api-${ENVIRONMENT}}"
}

live_deployed_proxy_port() {
  printf '%s' "${ARCO_UAT_CLOUD_RUN_PORT:-18080}"
}

status_output_path() {
  local phase="$1"
  if [[ -z "$STATUS_OUTPUT_DIR" ]]; then
    return 1
  fi

  printf '%s/%s-status.txt' "${STATUS_OUTPUT_DIR%/}" "$phase"
}

copy_ready_status_to_final_snapshot() {
  local initial_path final_path

  if [[ -z "$STATUS_OUTPUT_DIR" ]]; then
    return 0
  fi

  initial_path="$(status_output_path initial)"
  final_path="$(status_output_path final)"
  mkdir -p "$(dirname "$final_path")"
  cp "$initial_path" "$final_path"
}

preflight_live_deployed_proxy_port() {
  local port

  if [[ "$RUN_LIVE_DEPLOYED" != "true" || "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi

  port="$(live_deployed_proxy_port)"
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    die "ARCO_UAT_CLOUD_RUN_PORT=${port} is already in use; stop the existing local Cloud Run proxy or set ARCO_UAT_CLOUD_RUN_PORT to a free port before --run-live-deployed."
  fi
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

  DEPLOY_LOCK_DIR="$(deploy_lock_dir)"
  parent_dir="${DEPLOY_LOCK_DIR%/*}"
  if [[ "$parent_dir" == "$DEPLOY_LOCK_DIR" ]]; then
    parent_dir="."
  fi
  mkdir -p "$parent_dir"

  if ! mkdir "$DEPLOY_LOCK_DIR" 2>/dev/null; then
    print_existing_deploy_lock
    die "Refusing live deployed UAT prerequisite repair while deploy lock exists: $DEPLOY_LOCK_DIR"
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

validate_env() {
  [[ -n "${PROJECT_ID:-}" ]] || die "PROJECT_ID is required"
  [[ -n "$(project_number)" ]] || die "PROJECT_NUMBER or ARCO_UAT_CLOUD_RUN_PROJECT_NUMBER is required"
  [[ -n "$(flow_tenant_id)" ]] || die "FLOW_TENANT_ID or TF_VAR_flow_tenant_id is required"
  [[ -n "$(flow_workspace_id)" ]] || die "FLOW_WORKSPACE_ID or TF_VAR_flow_workspace_id is required"
  [[ -n "${ARCO_UAT_EXPECTED_API_CODE_VERSION:-}" ]] || die "ARCO_UAT_EXPECTED_API_CODE_VERSION is required for strict deployed API provenance checks"
  [[ -n "${ARCO_UAT_EXPECTED_API_GIT_SHA:-}" ]] || die "ARCO_UAT_EXPECTED_API_GIT_SHA is required for strict deployed API provenance checks"
  [[ -n "${ARCO_UAT_EXPECTED_API_IMAGE:-}" ]] || die "ARCO_UAT_EXPECTED_API_IMAGE is required for strict deployed API provenance checks"

  case "$ENVIRONMENT" in
  dev | staging | prod) ;;
  *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac

  if [[ "$DRY_RUN" == "false" ]]; then
    [[ -n "${ARCO_DEPLOY_OWNER:-}" ]] || die "ARCO_DEPLOY_OWNER is required for live deployed UAT prerequisite repair; use --dry-run to review commands without mutating ${PROJECT_ID}/${REGION}/${ENVIRONMENT}."
    if ! [[ "$ARCO_DEPLOY_OWNER" =~ ^[a-z0-9]([-a-z0-9_]{0,61}[a-z0-9])?$ ]]; then
      die "ARCO_DEPLOY_OWNER must be a GCP label-safe value: lowercase letters, digits, '-' or '_', 1-63 chars, starting and ending with a letter or digit."
    fi
  fi
}

run_scope_repair() {
  local -a args=("--env" "$ENVIRONMENT" "--scope-only")
  if [[ "$DRY_RUN" == "true" ]]; then
    args+=("--dry-run")
  fi

  PROJECT_ID="$PROJECT_ID" \
    REGION="$REGION" \
    FLOW_TENANT_ID="$(flow_tenant_id)" \
    FLOW_WORKSPACE_ID="$(flow_workspace_id)" \
    ARCO_DEPLOY_OWNER="${ARCO_DEPLOY_OWNER:-}" \
    ARCO_DEPLOY_LOCK_DIR="${DEPLOY_LOCK_DIR:-${ARCO_DEPLOY_LOCK_DIR:-}}" \
    ARCO_DEPLOY_LOCK_HELD="${DEPLOY_LOCK_ACQUIRED}" \
    "${ROOT_DIR}/scripts/refresh-cloud-run-revisions.sh" "${args[@]}"
}

run_scheduler_repair() {
  local -a args=("--env" "$ENVIRONMENT" "--repair-targets")
  if [[ "$DRY_RUN" == "true" ]]; then
    args+=("--dry-run")
  fi

  PROJECT_ID="$PROJECT_ID" \
    PROJECT_NUMBER="$(project_number)" \
    REGION="$REGION" \
    ARCO_DEPLOY_OWNER="${ARCO_DEPLOY_OWNER:-}" \
    ARCO_DEPLOY_LOCK_DIR="${DEPLOY_LOCK_DIR:-${ARCO_DEPLOY_LOCK_DIR:-}}" \
    ARCO_DEPLOY_LOCK_HELD="${DEPLOY_LOCK_ACQUIRED}" \
    ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT="${ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT:-}" \
    "${ROOT_DIR}/scripts/repair-flow-scheduler-jobs.sh" "${args[@]}"
}

probe_status_check() {
  local phase status_output
  local -a status_args=("--require-live-deployed-ready")
  phase="${1:-status}"

  if [[ -n "$STATUS_OUTPUT_DIR" ]]; then
    status_args+=("--status-output" "$(status_output_path "$phase")")
  fi

  if status_output="$(
    ARCO_DEPLOY_OWNER="$ARCO_DEPLOY_OWNER" \
      ARCO_UAT_CLOUD_RUN_SERVICE="$(cloud_run_service)" \
      PROJECT_ID="$PROJECT_ID" \
      REGION="$REGION" \
      ARCO_UAT_TENANT="$(flow_tenant_id)" \
      ARCO_UAT_WORKSPACE="$(flow_workspace_id)" \
      ARCO_UAT_EXPECTED_API_CODE_VERSION="$ARCO_UAT_EXPECTED_API_CODE_VERSION" \
      ARCO_UAT_EXPECTED_API_GIT_SHA="$ARCO_UAT_EXPECTED_API_GIT_SHA" \
      ARCO_UAT_EXPECTED_API_IMAGE="$ARCO_UAT_EXPECTED_API_IMAGE" \
      ARCO_UAT_CLOUD_RUN_PORT="$(live_deployed_proxy_port)" \
      "${ROOT_DIR}/scripts/run_user_acceptance_pipeline_uat.sh" "${status_args[@]}" 2>&1
  )"; then
    printf '%s\n' "$status_output"
    return 0
  fi

  printf '%s\n' "$status_output"
  return 1
}

print_status_check_command() {
  local phase="${1:-final}"
  local -a status_args=("--require-live-deployed-ready")
  if [[ -n "$STATUS_OUTPUT_DIR" ]]; then
    status_args+=("--status-output" "$(status_output_path "$phase")")
  fi

  print_command \
    "ARCO_DEPLOY_OWNER=${ARCO_DEPLOY_OWNER:-}" \
    "ARCO_UAT_CLOUD_RUN_SERVICE=$(cloud_run_service)" \
    "PROJECT_ID=${PROJECT_ID}" \
    "REGION=${REGION}" \
    "ARCO_UAT_TENANT=$(flow_tenant_id)" \
    "ARCO_UAT_WORKSPACE=$(flow_workspace_id)" \
    "ARCO_UAT_EXPECTED_API_CODE_VERSION=${ARCO_UAT_EXPECTED_API_CODE_VERSION}" \
    "ARCO_UAT_EXPECTED_API_GIT_SHA=${ARCO_UAT_EXPECTED_API_GIT_SHA}" \
    "ARCO_UAT_EXPECTED_API_IMAGE=${ARCO_UAT_EXPECTED_API_IMAGE}" \
    "ARCO_UAT_CLOUD_RUN_PORT=$(live_deployed_proxy_port)" \
    "./scripts/run_user_acceptance_pipeline_uat.sh" \
    "${status_args[@]}"
}

run_status_check() {
  if [[ "$DRY_RUN" == "true" ]]; then
    print_status_check_command final
    return 0
  fi

  if ! probe_status_check final; then
    die "deployed UAT prerequisites are still not ready after Cloud Run scope and Scheduler repair; live deployed gate was not attempted"
  fi
}

run_repair_phases() {
  if ! run_scope_repair; then
    die "Cloud Run scope repair failed; Scheduler repair and live deployed gate were not attempted"
  fi

  if ! run_scheduler_repair; then
    die "Scheduler repair failed after Cloud Run scope repair; final readiness check and live deployed gate were not attempted"
  fi
}

run_live_deployed_gate() {
  if [[ "$RUN_LIVE_DEPLOYED" != "true" ]]; then
    return 0
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    print_command \
      "ARCO_DEPLOY_OWNER=${ARCO_DEPLOY_OWNER:-}" \
      "ARCO_UAT_CLOUD_RUN_SERVICE=$(cloud_run_service)" \
      "PROJECT_ID=${PROJECT_ID}" \
      "REGION=${REGION}" \
      "ARCO_UAT_TENANT=$(flow_tenant_id)" \
      "ARCO_UAT_WORKSPACE=$(flow_workspace_id)" \
      "ARCO_UAT_EXPECTED_API_CODE_VERSION=${ARCO_UAT_EXPECTED_API_CODE_VERSION}" \
      "ARCO_UAT_EXPECTED_API_GIT_SHA=${ARCO_UAT_EXPECTED_API_GIT_SHA}" \
      "ARCO_UAT_EXPECTED_API_IMAGE=${ARCO_UAT_EXPECTED_API_IMAGE}" \
      "ARCO_UAT_CLOUD_RUN_PORT=$(live_deployed_proxy_port)" \
      "./scripts/run_user_acceptance_pipeline_uat.sh" \
      --live-deployed
    return 0
  fi

  ARCO_DEPLOY_OWNER="$ARCO_DEPLOY_OWNER" \
    ARCO_UAT_CLOUD_RUN_SERVICE="$(cloud_run_service)" \
    PROJECT_ID="$PROJECT_ID" \
    REGION="$REGION" \
    ARCO_UAT_TENANT="$(flow_tenant_id)" \
    ARCO_UAT_WORKSPACE="$(flow_workspace_id)" \
    ARCO_UAT_EXPECTED_API_CODE_VERSION="$ARCO_UAT_EXPECTED_API_CODE_VERSION" \
    ARCO_UAT_EXPECTED_API_GIT_SHA="$ARCO_UAT_EXPECTED_API_GIT_SHA" \
    ARCO_UAT_EXPECTED_API_IMAGE="$ARCO_UAT_EXPECTED_API_IMAGE" \
    ARCO_UAT_CLOUD_RUN_PORT="$(live_deployed_proxy_port)" \
    "${ROOT_DIR}/scripts/run_user_acceptance_pipeline_uat.sh" --live-deployed
}

main() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
    --env)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --cloud-run-service)
      CLOUD_RUN_SERVICE="$2"
      shift 2
      ;;
    --status-output-dir)
      [[ -n "${2:-}" ]] || die "--status-output-dir requires DIR"
      STATUS_OUTPUT_DIR="$2"
      shift 2
      ;;
    --run-live-deployed)
      RUN_LIVE_DEPLOYED=true
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
  preflight_live_deployed_proxy_port
  acquire_live_deploy_lock
  if [[ "$DRY_RUN" == "false" ]]; then
    if probe_status_check initial; then
      echo "deployed UAT prerequisites already ready; skipping repairs"
      copy_ready_status_to_final_snapshot
      run_live_deployed_gate
      exit 0
    fi
    echo "deployed UAT prerequisites not ready; running repairs under owner ${ARCO_DEPLOY_OWNER} for ${PROJECT_ID}/${REGION}/${ENVIRONMENT}"
  fi
  if [[ "$DRY_RUN" == "true" && -n "$STATUS_OUTPUT_DIR" ]]; then
    print_status_check_command initial
  fi
  run_repair_phases
  run_status_check
  run_live_deployed_gate
}

main "$@"
