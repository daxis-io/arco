#!/usr/bin/env bash
#
# Arco Deployment Script
#
# Deploys Arco Cloud Run services with a compactor-first hard gate:
# - Wait until the compactor reports ready+healthy and has completed at least one
#   successful compaction cycle before considering the deploy successful.
#
# This script uses `gcloud run services proxy` for health checks so it works with:
# - `INGRESS_TRAFFIC_INTERNAL_ONLY`
# - IAM-protected services (no unauthenticated curl)
#
# Usage:
#   ./scripts/deploy.sh [--env dev|staging|prod] [--dry-run] [--timeout SECONDS]
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

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV           Environment (dev, staging, prod). Default: dev
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
  [[ -z "${FLOW_WORKER_IMAGE:-}" ]] && die "FLOW_WORKER_IMAGE is required"

  case "$ENVIRONMENT" in
    dev|staging|prod) ;;
    *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac
}

start_run_proxy() {
  local service_name="$1"
  local port="$2"

  # `--quiet` avoids interactive prompts.
  gcloud run services proxy "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --port="$port" \
    --quiet \
    >/dev/null 2>&1 &

  echo "$!"
}

stop_run_proxy() {
  local pid="$1"
  if [[ -n "${pid}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

wait_for_compactor_health() {
  local service_name="arco-compactor-${ENVIRONMENT}"
  local local_port="18081"
  local pid=""

  log "Waiting for compactor health via proxy (timeout: ${COMPACTOR_HEALTH_TIMEOUT}s)..."

  pid="$(start_run_proxy "$service_name" "$local_port")"
  trap 'stop_run_proxy "$pid"' RETURN

  local elapsed=0
  while [[ "$elapsed" -lt "$COMPACTOR_HEALTH_TIMEOUT" ]]; do
    local response
    response="$(curl -sf "http://127.0.0.1:${local_port}/ready" 2>/dev/null || echo "{}")"

    local ready healthy successful
    ready="$(echo "$response" | jq -r '.ready // false')"
    healthy="$(echo "$response" | jq -r '.healthy // false')"
    successful="$(echo "$response" | jq -r '.successful_compactions // 0')"

    if [[ "$ready" == "true" && "$healthy" == "true" && "$successful" != "0" ]]; then
      local last_compaction
      last_compaction="$(echo "$response" | jq -r '.last_successful_compaction // "unknown"')"
      log "Compactor healthy (successful_compactions=$successful, last=$last_compaction)"
      return 0
    fi

    log "Compactor not ready (ready=$ready, healthy=$healthy, successful_compactions=$successful). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "Compactor health check timed out after ${COMPACTOR_HEALTH_TIMEOUT}s"
}

wait_for_flow_compactor_health() {
  local service_name="arco-flow-compactor-${ENVIRONMENT}"
  local local_port="18082"
  local pid=""

  log "Waiting for flow compactor health via proxy (timeout: ${FLOW_COMPACTOR_HEALTH_TIMEOUT}s)..."

  pid="$(start_run_proxy "$service_name" "$local_port")"
  trap 'stop_run_proxy "$pid"' RETURN

  local elapsed=0
  while [[ "$elapsed" -lt "$FLOW_COMPACTOR_HEALTH_TIMEOUT" ]]; do
    local status
    status="$(curl -sf "http://127.0.0.1:${local_port}/health" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")"

    if [[ "$status" == "200" ]]; then
      log "Flow compactor healthy"
      return 0
    fi

    log "Flow compactor not healthy yet (status=$status). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "Flow compactor health check timed out after ${FLOW_COMPACTOR_HEALTH_TIMEOUT}s"
}

wait_for_api_health() {
  local service_name="arco-api-${ENVIRONMENT}"
  local local_port="18080"
  local pid=""

  log "Waiting for API health via proxy (timeout: ${API_HEALTH_TIMEOUT}s)..."

  pid="$(start_run_proxy "$service_name" "$local_port")"
  trap 'stop_run_proxy "$pid"' RETURN

  local elapsed=0
  while [[ "$elapsed" -lt "$API_HEALTH_TIMEOUT" ]]; do
    local status
    status="$(curl -sf "http://127.0.0.1:${local_port}/health" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")"

    if [[ "$status" == "200" ]]; then
      log "API healthy"
      return 0
    fi

    log "API not healthy yet (status=$status). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "API health check timed out after ${API_HEALTH_TIMEOUT}s"
}

deploy_terraform() {
  log "Deploying infrastructure with Terraform..."

  pushd "${ROOT_DIR}/infra/terraform" >/dev/null

  local tfvars_file="environments/${ENVIRONMENT}.tfvars"
  if [[ ! -f "$tfvars_file" ]]; then
    die "tfvars file not found: $tfvars_file"
  fi

  export TF_VAR_api_image="$API_IMAGE"
  export TF_VAR_compactor_image="$COMPACTOR_IMAGE"
  export TF_VAR_flow_compactor_image="$FLOW_COMPACTOR_IMAGE"
  export TF_VAR_flow_dispatcher_image="$FLOW_DISPATCHER_IMAGE"
  export TF_VAR_flow_sweeper_image="$FLOW_SWEEPER_IMAGE"
  export TF_VAR_flow_worker_image="$FLOW_WORKER_IMAGE"
  if [[ -n "${PROJECT_NUMBER:-}" ]]; then
    export TF_VAR_project_number="$PROJECT_NUMBER"
  else
    # Best-effort: avoids Terraform needing Cloud Resource Manager permissions just to read project number.
    TF_VAR_project_number="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)' 2>/dev/null || true)"
    export TF_VAR_project_number
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN: Would deploy compactors first (terraform plan -target=google_cloud_run_v2_service.compactor -target=google_cloud_run_v2_service.flow_compactor)"
    terraform init -upgrade
    terraform plan -var-file="$tfvars_file" \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor
    log "DRY RUN: Would deploy remaining resources (terraform plan)"
    terraform plan -var-file="$tfvars_file"
  else
    terraform init -upgrade
    # HARD GATE ENFORCEMENT:
    # Deploy compactors first, wait for them to become healthy, then deploy API.
    terraform apply -var-file="$tfvars_file" -auto-approve \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor
  fi

  popd >/dev/null
}

deploy_terraform_remaining() {
  pushd "${ROOT_DIR}/infra/terraform" >/dev/null

  local tfvars_file="environments/${ENVIRONMENT}.tfvars"
  export TF_VAR_api_image="$API_IMAGE"
  export TF_VAR_compactor_image="$COMPACTOR_IMAGE"
  export TF_VAR_flow_compactor_image="$FLOW_COMPACTOR_IMAGE"
  export TF_VAR_flow_dispatcher_image="$FLOW_DISPATCHER_IMAGE"
  export TF_VAR_flow_sweeper_image="$FLOW_SWEEPER_IMAGE"
  export TF_VAR_flow_worker_image="$FLOW_WORKER_IMAGE"
  if [[ -n "${PROJECT_NUMBER:-}" ]]; then
    export TF_VAR_project_number="$PROJECT_NUMBER"
  else
    TF_VAR_project_number="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)' 2>/dev/null || true)"
    export TF_VAR_project_number
  fi

  terraform apply -var-file="$tfvars_file" -auto-approve

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
