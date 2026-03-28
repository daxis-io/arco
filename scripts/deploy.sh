#!/usr/bin/env bash
#
# Arco Deployment Script
#
# Deploys Arco Cloud Run services with a compactor-first hard gate:
# - Wait until the compactor revision is Ready and has completed at least one
#   successful compaction cycle before considering the deploy successful.
#
# Internal-only Cloud Run services are not reachable from a local shell, even via
# `gcloud run services proxy`, so health checks use Cloud Run status and Cloud
# Logging instead of direct HTTP probes.
#
# Usage:
#   ./scripts/deploy.sh [--env dev|staging|prod] [--tfvars FILE] [--dry-run] [--timeout SECONDS]
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
DRY_RUN=false
COMPACTOR_HEALTH_TIMEOUT="${COMPACTOR_HEALTH_TIMEOUT:-300}"
FLOW_COMPACTOR_HEALTH_TIMEOUT="${FLOW_COMPACTOR_HEALTH_TIMEOUT:-120}"
API_HEALTH_TIMEOUT="${API_HEALTH_TIMEOUT:-120}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10}"
TFVARS_FILE="${TFVARS_FILE:-}"
TERRAFORM_VAR_ARGS=()

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV           Environment (dev, staging, prod). Default: dev
  --tfvars FILE       Terraform tfvars file relative to infra/terraform or absolute path
  --dry-run           Show what would be deployed without making changes
  --timeout SECONDS   Compactor health timeout. Default: ${COMPACTOR_HEALTH_TIMEOUT}
  -h, --help          Show this help message

Required env vars:
  PROJECT_ID
  API_IMAGE
  COMPACTOR_IMAGE
  FLOW_COMPACTOR_IMAGE
  FLOW_DISPATCHER_IMAGE
  FLOW_SWEEPER_IMAGE
  FLOW_TIMER_INGEST_IMAGE
  FLOW_WORKER_IMAGE

Optional env vars:
  REGION
  PROJECT_NUMBER
  COMPACTOR_HEALTH_TIMEOUT
  API_HEALTH_TIMEOUT
  HEALTH_CHECK_INTERVAL
EOF
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

die() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

validate_env() {
  [[ -z "${PROJECT_ID:-}" ]] && die "PROJECT_ID is required"
  [[ -z "${API_IMAGE:-}" ]] && die "API_IMAGE is required"
  [[ -z "${COMPACTOR_IMAGE:-}" ]] && die "COMPACTOR_IMAGE is required"
  [[ -z "${FLOW_COMPACTOR_IMAGE:-}" ]] && die "FLOW_COMPACTOR_IMAGE is required"
  [[ -z "${FLOW_DISPATCHER_IMAGE:-}" ]] && die "FLOW_DISPATCHER_IMAGE is required"
  [[ -z "${FLOW_SWEEPER_IMAGE:-}" ]] && die "FLOW_SWEEPER_IMAGE is required"
  [[ -z "${FLOW_TIMER_INGEST_IMAGE:-}" ]] && die "FLOW_TIMER_INGEST_IMAGE is required"
  [[ -z "${FLOW_WORKER_IMAGE:-}" ]] && die "FLOW_WORKER_IMAGE is required"

  case "$ENVIRONMENT" in
    dev|staging|prod) ;;
    *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac
}

resolve_tfvars_file() {
  if [[ -n "${TFVARS_FILE}" ]]; then
    if [[ "${TFVARS_FILE}" = /* ]]; then
      echo "${TFVARS_FILE}"
    else
      echo "${ROOT_DIR}/infra/terraform/${TFVARS_FILE}"
    fi
    return
  fi

  echo "${ROOT_DIR}/infra/terraform/environments/${ENVIRONMENT}.tfvars"
}

resolve_project_number() {
  if [[ -n "${PROJECT_NUMBER:-}" ]]; then
    echo "${PROJECT_NUMBER}"
    return
  fi

  # Best-effort: avoids Terraform needing Cloud Resource Manager permissions just to read project number.
  gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)' 2>/dev/null || true
}

build_terraform_var_args() {
  local project_number="$1"
  TERRAFORM_VAR_ARGS=(
    "-var=project_id=${PROJECT_ID}"
    "-var=project_number=${project_number}"
    "-var=region=${REGION}"
    "-var=environment=${ENVIRONMENT}"
    "-var=api_image=${API_IMAGE}"
    "-var=compactor_image=${COMPACTOR_IMAGE}"
    "-var=flow_compactor_image=${FLOW_COMPACTOR_IMAGE}"
    "-var=flow_dispatcher_image=${FLOW_DISPATCHER_IMAGE}"
    "-var=flow_sweeper_image=${FLOW_SWEEPER_IMAGE}"
    "-var=flow_timer_ingest_image=${FLOW_TIMER_INGEST_IMAGE}"
    "-var=flow_worker_image=${FLOW_WORKER_IMAGE}"
  )
}

get_cloud_run_service_json() {
  local service_name="$1"
  gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format=json
}

get_cloud_run_ready_status() {
  local service_name="$1"
  get_cloud_run_service_json "$service_name" | jq -r '
    ([.status.conditions[]? | select(.type == "Ready") | .status][0]) // "False"
  '
}

get_cloud_run_latest_ready_revision() {
  local service_name="$1"
  get_cloud_run_service_json "$service_name" | jq -r '.status.latestReadyRevisionName // ""'
}

wait_for_cloud_run_ready() {
  local service_name="$1"
  local timeout_secs="$2"
  local label="$3"

  log "Waiting for ${label} Cloud Run service readiness (timeout: ${timeout_secs}s)..."

  local elapsed=0
  while [[ "$elapsed" -lt "$timeout_secs" ]]; do
    local ready_status latest_revision
    ready_status="$(get_cloud_run_ready_status "$service_name")"
    latest_revision="$(get_cloud_run_latest_ready_revision "$service_name")"

    if [[ "$ready_status" == "True" && -n "$latest_revision" ]]; then
      log "${label} Cloud Run service ready (revision=${latest_revision})"
      return 0
    fi

    log "${label} Cloud Run service not ready yet (ready=${ready_status}, revision=${latest_revision:-none}). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "${label} Cloud Run readiness timed out after ${timeout_secs}s"
}

get_latest_compactor_success_timestamp() {
  local service_name="$1"
  local revision_name="$2"

  gcloud logging read \
    "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${service_name}\" AND resource.labels.revision_name=\"${revision_name}\" AND jsonPayload.fields.message=\"Compaction cycle completed successfully\"" \
    --project="$PROJECT_ID" \
    --limit=1 \
    --format='value(timestamp)' 2>/dev/null || true
}

wait_for_compactor_health() {
  local service_name="arco-compactor-${ENVIRONMENT}"

  wait_for_cloud_run_ready "$service_name" "$COMPACTOR_HEALTH_TIMEOUT" "Compactor"

  local elapsed=0
  while [[ "$elapsed" -lt "$COMPACTOR_HEALTH_TIMEOUT" ]]; do
    local revision_name last_compaction
    revision_name="$(get_cloud_run_latest_ready_revision "$service_name")"
    last_compaction="$(get_latest_compactor_success_timestamp "$service_name" "$revision_name")"

    if [[ -n "$last_compaction" ]]; then
      log "Compactor healthy (revision=${revision_name}, last_successful_compaction=${last_compaction})"
      return 0
    fi

    log "Compactor revision ${revision_name} is ready but has not logged a successful compaction yet. Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "Compactor health check timed out after ${COMPACTOR_HEALTH_TIMEOUT}s"
}

wait_for_flow_compactor_health() {
  local service_name="arco-flow-compactor-${ENVIRONMENT}"
  wait_for_cloud_run_ready "$service_name" "$FLOW_COMPACTOR_HEALTH_TIMEOUT" "Flow compactor"
}

wait_for_api_health() {
  local service_name="arco-api-${ENVIRONMENT}"
  wait_for_cloud_run_ready "$service_name" "$API_HEALTH_TIMEOUT" "API"
}

deploy_terraform() {
  log "Deploying infrastructure with Terraform..."

  pushd "${ROOT_DIR}/infra/terraform" >/dev/null

  local tfvars_file
  tfvars_file="$(resolve_tfvars_file)"
  if [[ ! -f "$tfvars_file" ]]; then
    die "tfvars file not found: $tfvars_file"
  fi

  local project_number
  project_number="$(resolve_project_number)"
  if [[ -z "$project_number" ]]; then
    die "PROJECT_NUMBER is required or must be discoverable via gcloud"
  fi
  build_terraform_var_args "$project_number"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN: Would deploy compactors first (terraform plan -target=google_cloud_run_v2_service.compactor -target=google_cloud_run_v2_service.flow_compactor)"
    log "Using tfvars baseline: $tfvars_file"
    terraform init -upgrade
    terraform plan -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}" \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor
    log "DRY RUN: Would deploy remaining resources (terraform plan)"
    terraform plan -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}"
  else
    terraform init -upgrade
    # HARD GATE ENFORCEMENT:
    # Deploy compactors first, wait for them to become healthy, then deploy API.
    log "Using tfvars baseline: $tfvars_file"
    terraform apply -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}" -auto-approve \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor
  fi

  popd >/dev/null
}

deploy_terraform_remaining() {
  pushd "${ROOT_DIR}/infra/terraform" >/dev/null

  local tfvars_file
  tfvars_file="$(resolve_tfvars_file)"
  local project_number
  project_number="$(resolve_project_number)"
  if [[ -z "$project_number" ]]; then
    die "PROJECT_NUMBER is required or must be discoverable via gcloud"
  fi
  build_terraform_var_args "$project_number"

  terraform apply -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}" -auto-approve

  popd >/dev/null
}

main() {
  require_cmd gcloud
  require_cmd terraform
  require_cmd curl
  require_cmd jq

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env)
        ENVIRONMENT="$2"
        shift 2
        ;;
      --tfvars)
        TFVARS_FILE="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      --timeout)
        COMPACTOR_HEALTH_TIMEOUT="$2"
        shift 2
        ;;
      -h|--help)
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

  log "Starting Arco deployment (env=$ENVIRONMENT, project=$PROJECT_ID, region=$REGION)"

  deploy_terraform

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN complete"
    exit 0
  fi

  # HARD GATE: compactor must be healthy before deploy is considered successful.
  wait_for_compactor_health
  wait_for_flow_compactor_health

  # Only after the compactor is healthy do we deploy the API revision / remaining infra.
  deploy_terraform_remaining

  # Verify API health after compactor gate passes.
  wait_for_api_health

  log "Deployment successful"
}

main "$@"
