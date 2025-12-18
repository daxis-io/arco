#!/usr/bin/env bash
#
# Arco Rollback Script
#
# Rolls back Cloud Run services to the previous revision.
# Uses authenticated proxy-based health checks so it works for internal-only/IAM services.
#
# Usage:
#   ./scripts/rollback.sh [--env dev|staging|prod] [--include-compactor] [--dry-run]
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
DRY_RUN=false
INCLUDE_COMPACTOR=false
HEALTH_CHECK_TIMEOUT="${HEALTH_CHECK_TIMEOUT:-120}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10}"

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV             Environment (dev, staging, prod). Default: dev
  --include-compactor   Also rollback the compactor service
  --dry-run             Show what would be rolled back without making changes
  -h, --help            Show this help message

Required env vars:
  PROJECT_ID

Optional env vars:
  REGION
  HEALTH_CHECK_TIMEOUT
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

  case "$ENVIRONMENT" in
    dev|staging|prod) ;;
    *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac
}

get_previous_revision() {
  local service_name="$1"
  gcloud run revisions list \
    --service="$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format="value(metadata.name)" \
    --sort-by="~metadata.creationTimestamp" \
    --limit=2 | tail -n1
}

rollback_service() {
  local service_name="$1"
  local target_revision="$2"

  log "Rolling back $service_name to revision $target_revision"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN: Would run gcloud run services update-traffic $service_name --to-revisions=$target_revision=100"
    return 0
  fi

  gcloud run services update-traffic "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --to-revisions="$target_revision=100"
}

start_run_proxy() {
  local service_name="$1"
  local port="$2"

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

wait_for_health_via_proxy() {
  local service_name="$1"
  local health_path="${2:-/health}"
  local local_port="$3"
  local pid=""

  pid="$(start_run_proxy "$service_name" "$local_port")"
  trap 'stop_run_proxy "$pid"' RETURN

  local elapsed=0
  while [[ "$elapsed" -lt "$HEALTH_CHECK_TIMEOUT" ]]; do
    local status
    status="$(curl -sf "http://127.0.0.1:${local_port}${health_path}" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")"
    if [[ "$status" == "200" ]]; then
      log "$service_name is healthy"
      return 0
    fi

    log "$service_name not healthy yet (status=$status). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "$service_name health check timed out after ${HEALTH_CHECK_TIMEOUT}s"
}

rollback_api() {
  local service_name="arco-api-${ENVIRONMENT}"

  log "=== Rolling back API ==="

  local current_revision previous_revision
  current_revision="$(gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format="value(status.traffic[0].revisionName)" 2>/dev/null || echo "")"
  previous_revision="$(get_previous_revision "$service_name")"

  [[ -z "$previous_revision" ]] && die "No previous revision found for $service_name"

  if [[ "$current_revision" == "$previous_revision" ]]; then
    log "Current revision is already the oldest revision. Nothing to rollback."
    return 0
  fi

  rollback_service "$service_name" "$previous_revision"

  if [[ "$DRY_RUN" != "true" ]]; then
    wait_for_health_via_proxy "$service_name" "/health" "18080"
  fi

  log "API rollback complete"
}

rollback_compactor() {
  local service_name="arco-compactor-${ENVIRONMENT}"

  log "=== Rolling back Compactor ==="

  local current_revision previous_revision
  current_revision="$(gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format="value(status.traffic[0].revisionName)" 2>/dev/null || echo "")"
  previous_revision="$(get_previous_revision "$service_name")"

  [[ -z "$previous_revision" ]] && die "No previous revision found for $service_name"

  if [[ "$current_revision" == "$previous_revision" ]]; then
    log "Current revision is already the oldest revision. Nothing to rollback."
    return 0
  fi

  rollback_service "$service_name" "$previous_revision"

  if [[ "$DRY_RUN" != "true" ]]; then
    wait_for_health_via_proxy "$service_name" "/health" "18081"
  fi

  log "Compactor rollback complete"
}

main() {
  require_cmd gcloud
  require_cmd curl

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env)
        ENVIRONMENT="$2"
        shift 2
        ;;
      --include-compactor)
        INCLUDE_COMPACTOR=true
        shift
        ;;
      --dry-run)
        DRY_RUN=true
        shift
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

  log "Starting rollback (env=$ENVIRONMENT, project=$PROJECT_ID, region=$REGION)"

  # Rollback API first to minimize exposure window.
  rollback_api

  if [[ "$INCLUDE_COMPACTOR" == "true" ]]; then
    rollback_compactor
  fi

  log "Rollback complete"
}

main "$@"
