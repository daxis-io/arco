#!/usr/bin/env bash
#
# Arco Deployment Script
#
# Deploys Arco Cloud Run services with a compactor-first hard gate:
# - Wait until the compactor reports ready+healthy and has completed at least one
#   successful compaction cycle before considering the deploy successful.
#
# This script uses `gcloud run services proxy` for health checks so it works with:
# - IAM-protected services (no unauthenticated curl)
#
# Internal-only services are validated through Cloud Run revision readiness and,
# for the catalog compactor, Cloud Logging evidence of a successful compaction.
#
# Usage:
#   ./scripts/deploy.sh [--env dev|staging|prod] [--dry-run] [--timeout SECONDS]
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
TFVARS_FILE="${TFVARS_FILE:-}"
DRY_RUN=false
COMPACTOR_HEALTH_TIMEOUT="${COMPACTOR_HEALTH_TIMEOUT:-300}"
FLOW_COMPACTOR_HEALTH_TIMEOUT="${FLOW_COMPACTOR_HEALTH_TIMEOUT:-120}"
API_HEALTH_TIMEOUT="${API_HEALTH_TIMEOUT:-120}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10}"
PROXY_PIDS=""

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV           Environment (dev, staging, prod). Default: dev
  --tfvars-file FILE  Terraform tfvars file. Default: infra/terraform/environments/<env>.tfvars
  --dry-run           Show what would be deployed without making changes
  --timeout SECONDS   Compactor health timeout. Default: ${COMPACTOR_HEALTH_TIMEOUT}
  -h, --help          Show this help message

Required env vars:
  PROJECT_ID
  API_IMAGE
  COMPACTOR_IMAGE
  FLOW_COMPACTOR_IMAGE

Optional env vars:
  REGION
  PROJECT_NUMBER
  COMPACTOR_HEALTH_TIMEOUT
  API_HEALTH_TIMEOUT
  HEALTH_CHECK_INTERVAL
  FLOW_AUTOMATION_RECONCILER_IMAGE
  FLOW_DISPATCHER_IMAGE
  FLOW_SWEEPER_IMAGE
  FLOW_TIMER_INGEST_IMAGE
  FLOW_WORKER_IMAGE
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

tfvars_file_path() {
  local path="${TFVARS_FILE:-infra/terraform/environments/${ENVIRONMENT}.tfvars}"
  if [[ "$path" = /* ]]; then
    echo "$path"
  else
    echo "${ROOT_DIR}/${path}"
  fi
}

tfvars_string_value() {
  local key="$1"
  local tfvars_file
  tfvars_file="$(tfvars_file_path)"

  [[ -f "$tfvars_file" ]] || return 0

  awk -F= -v key="$key" '
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" {
      value = $2
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      if (value ~ /^".*"$/) {
        sub(/^"/, "", value)
        sub(/"$/, "", value)
      }
      print value
      exit
    }
  ' "$tfvars_file"
}

effective_tfvar_value() {
  local tfvar_name="$1"
  local direct_env_name="${2:-}"
  local tf_var_env_name="TF_VAR_${tfvar_name}"

  if [[ -n "$direct_env_name" && -n "${!direct_env_name:-}" ]]; then
    printf '%s' "${!direct_env_name}"
    return 0
  fi

  if [[ -n "${!tf_var_env_name:-}" ]]; then
    printf '%s' "${!tf_var_env_name}"
    return 0
  fi

  tfvars_string_value "$tfvar_name"
}

export_tf_var_if_set() {
  local tfvar_name="$1"
  local env_var_name="$2"
  local value="${!env_var_name:-}"

  if [[ -n "$value" ]]; then
    export "TF_VAR_${tfvar_name}=$value"
  fi
}

build_terraform_var_args() {
  local tfvars_file="$1"
  local project_number="${PROJECT_NUMBER:-}"
  if [[ -z "$project_number" ]]; then
    # Best-effort: avoids Terraform needing Cloud Resource Manager permissions just to read project number.
    project_number="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)' 2>/dev/null || true)"
  fi

  local api_code_version
  local flow_tenant_id flow_workspace_id
  local flow_dispatcher_image flow_sweeper_image flow_timer_ingest_image flow_worker_image
  local flow_automation_reconciler_image

  api_code_version="$(effective_tfvar_value "api_code_version" "API_CODE_VERSION")"
  flow_tenant_id="$(effective_tfvar_value "flow_tenant_id")"
  flow_workspace_id="$(effective_tfvar_value "flow_workspace_id")"
  flow_dispatcher_image="$(effective_tfvar_value "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE")"
  flow_sweeper_image="$(effective_tfvar_value "flow_sweeper_image" "FLOW_SWEEPER_IMAGE")"
  flow_timer_ingest_image="$(effective_tfvar_value "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE")"
  flow_worker_image="$(effective_tfvar_value "flow_worker_image" "FLOW_WORKER_IMAGE")"
  flow_automation_reconciler_image="$(effective_tfvar_value "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE")"

  TERRAFORM_VAR_ARGS=(
    "-var-file=$tfvars_file"
    "-var=api_image=$API_IMAGE"
    "-var=compactor_image=$COMPACTOR_IMAGE"
    "-var=flow_compactor_image=$FLOW_COMPACTOR_IMAGE"
  )

  [[ -n "$project_number" ]] && TERRAFORM_VAR_ARGS+=("-var=project_number=$project_number")
  [[ -n "$api_code_version" ]] && TERRAFORM_VAR_ARGS+=("-var=api_code_version=$api_code_version")
  [[ -n "$flow_tenant_id" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_tenant_id=$flow_tenant_id")
  [[ -n "$flow_workspace_id" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_workspace_id=$flow_workspace_id")
  [[ -n "$flow_dispatcher_image" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_dispatcher_image=$flow_dispatcher_image")
  [[ -n "$flow_sweeper_image" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_sweeper_image=$flow_sweeper_image")
  [[ -n "$flow_timer_ingest_image" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_timer_ingest_image=$flow_timer_ingest_image")
  [[ -n "$flow_worker_image" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_worker_image=$flow_worker_image")
  [[ -n "$flow_automation_reconciler_image" ]] && TERRAFORM_VAR_ARGS+=("-var=flow_automation_reconciler_image=$flow_automation_reconciler_image")
  return 0
}

terraform_state_has_resource() {
  local address="$1"
  local state_address

  while IFS= read -r state_address; do
    [[ "$state_address" == "$address" ]] && return 0
  done <<<"$TERRAFORM_STATE_LIST"

  return 1
}

record_unmanaged_existing_resource() {
  local address="$1"
  local name="$2"
  local import_id="$3"

  UNMANAGED_EXISTING_RESOURCES+=("${address}|${name}|${import_id}")
}

check_cloud_run_service_imported() {
  local address="$1"
  local service_name="$2"

  terraform_state_has_resource "$address" && return 0
  if gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(metadata.name)' \
    >/dev/null 2>&1; then
    record_unmanaged_existing_resource \
      "$address" \
      "$service_name" \
      "projects/${PROJECT_ID}/locations/${REGION}/services/${service_name}"
  fi
}

check_cloud_run_job_imported() {
  local address="$1"
  local job_name="$2"

  terraform_state_has_resource "$address" && return 0
  if gcloud run jobs describe "$job_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(metadata.name)' \
    >/dev/null 2>&1; then
    record_unmanaged_existing_resource \
      "$address" \
      "$job_name" \
      "projects/${PROJECT_ID}/locations/${REGION}/jobs/${job_name}"
  fi
}

check_service_account_imported() {
  local address="$1"
  local email="$2"

  terraform_state_has_resource "$address" && return 0
  if gcloud iam service-accounts describe "$email" \
    --project="$PROJECT_ID" \
    --format='value(email)' \
    >/dev/null 2>&1; then
    record_unmanaged_existing_resource \
      "$address" \
      "$email" \
      "projects/${PROJECT_ID}/serviceAccounts/${email}"
  fi
}

check_project_custom_role_imported() {
  local address="$1"
  local role_id="$2"

  terraform_state_has_resource "$address" && return 0
  if gcloud iam roles describe "$role_id" \
    --project="$PROJECT_ID" \
    --format='value(name)' \
    >/dev/null 2>&1; then
    record_unmanaged_existing_resource \
      "$address" \
      "projects/${PROJECT_ID}/roles/${role_id}" \
      "projects/${PROJECT_ID}/roles/${role_id}"
  fi
}

verify_existing_resources_imported() {
  local flow_tenant_id flow_workspace_id
  local flow_dispatcher_image flow_sweeper_image flow_timer_ingest_image flow_worker_image
  local flow_automation_reconciler_image

  TERRAFORM_STATE_LIST="$(terraform state list 2>/dev/null || true)"
  UNMANAGED_EXISTING_RESOURCES=()

  check_cloud_run_service_imported "google_cloud_run_v2_service.api" "arco-api-${ENVIRONMENT}"
  check_cloud_run_service_imported "google_cloud_run_v2_service.compactor" "arco-compactor-${ENVIRONMENT}"
  check_cloud_run_service_imported "google_cloud_run_v2_service.flow_compactor" "arco-flow-compactor-${ENVIRONMENT}"
  check_cloud_run_job_imported "google_cloud_run_v2_job.compactor_antientropy" "arco-compactor-antientropy-${ENVIRONMENT}"
  check_project_custom_role_imported "google_project_iam_custom_role.storage_object_lister" "storageObjectLister"
  check_project_custom_role_imported "google_project_iam_custom_role.storage_object_writer_no_list" "storageObjectWriterNoList"

  flow_tenant_id="$(effective_tfvar_value "flow_tenant_id")"
  flow_workspace_id="$(effective_tfvar_value "flow_workspace_id")"
  flow_dispatcher_image="$(effective_tfvar_value "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE")"
  flow_sweeper_image="$(effective_tfvar_value "flow_sweeper_image" "FLOW_SWEEPER_IMAGE")"
  flow_timer_ingest_image="$(effective_tfvar_value "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE")"
  flow_worker_image="$(effective_tfvar_value "flow_worker_image" "FLOW_WORKER_IMAGE")"
  flow_automation_reconciler_image="$(effective_tfvar_value "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE")"

  if [[ -n "$flow_tenant_id" && -n "$flow_workspace_id" && -n "$flow_dispatcher_image" && -n "$flow_sweeper_image" && -n "$flow_timer_ingest_image" && -n "$flow_worker_image" ]]; then
    check_service_account_imported "google_service_account.flow_timer_ingest[0]" "arco-flow-timer-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
    check_cloud_run_service_imported "google_cloud_run_v2_service.flow_dispatcher[0]" "arco-flow-dispatcher-${ENVIRONMENT}"
    check_cloud_run_service_imported "google_cloud_run_v2_service.flow_sweeper[0]" "arco-flow-sweeper-${ENVIRONMENT}"
    check_cloud_run_service_imported "google_cloud_run_v2_service.flow_timer_ingest[0]" "arco-flow-timer-ingest-${ENVIRONMENT}"
    check_cloud_run_service_imported "google_cloud_run_v2_service.flow_worker[0]" "arco-flow-worker-${ENVIRONMENT}"
  fi

  if [[ -n "$flow_tenant_id" && -n "$flow_workspace_id" && -n "$flow_automation_reconciler_image" ]]; then
    check_cloud_run_service_imported "google_cloud_run_v2_service.flow_automation_reconciler[0]" "arco-flow-automation-reconciler-${ENVIRONMENT}"
  fi

  if [[ "${#UNMANAGED_EXISTING_RESOURCES[@]}" -gt 0 ]]; then
    local message="Existing GCP resources are not tracked by Terraform state. Import them before running deploy, or deploy to a fresh environment:"
    local entry address name import_id
    for entry in "${UNMANAGED_EXISTING_RESOURCES[@]}"; do
      IFS='|' read -r address name import_id <<<"$entry"
      message+=$'\n'"  - ${name} exists in GCP but ${address} is not in Terraform state"
      message+=$'\n'"    terraform import '${address}' '${import_id}'"
    done
    die "$message"
  fi
}

validate_flow_core_config() {
  local flow_tenant_id flow_workspace_id
  local flow_dispatcher_image flow_sweeper_image flow_timer_ingest_image flow_worker_image
  local flow_automation_reconciler_image

  flow_tenant_id="$(effective_tfvar_value "flow_tenant_id")"
  flow_workspace_id="$(effective_tfvar_value "flow_workspace_id")"
  flow_dispatcher_image="$(effective_tfvar_value "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE")"
  flow_sweeper_image="$(effective_tfvar_value "flow_sweeper_image" "FLOW_SWEEPER_IMAGE")"
  flow_timer_ingest_image="$(effective_tfvar_value "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE")"
  flow_worker_image="$(effective_tfvar_value "flow_worker_image" "FLOW_WORKER_IMAGE")"
  flow_automation_reconciler_image="$(effective_tfvar_value "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE")"

  if [[ -n "${flow_tenant_id}${flow_workspace_id}${flow_dispatcher_image}${flow_sweeper_image}${flow_timer_ingest_image}${flow_worker_image}" ]]; then
    if [[ -z "$flow_tenant_id" || -z "$flow_workspace_id" || -z "$flow_dispatcher_image" || -z "$flow_sweeper_image" || -z "$flow_timer_ingest_image" || -z "$flow_worker_image" ]]; then
      die "Flow control-plane config is partial. Set flow_tenant_id, flow_workspace_id, and flow dispatcher/sweeper/timer-ingest/worker images together."
    fi
  fi

  if [[ -n "$flow_automation_reconciler_image" && (-z "$flow_tenant_id" || -z "$flow_workspace_id") ]]; then
    die "Flow automation reconciler image requires flow_tenant_id and flow_workspace_id."
  fi
}

validate_env() {
  [[ -z "${PROJECT_ID:-}" ]] && die "PROJECT_ID is required"
  [[ -z "${API_IMAGE:-}" ]] && die "API_IMAGE is required"
  [[ -z "${COMPACTOR_IMAGE:-}" ]] && die "COMPACTOR_IMAGE is required"
  [[ -z "${FLOW_COMPACTOR_IMAGE:-}" ]] && die "FLOW_COMPACTOR_IMAGE is required"

  case "$ENVIRONMENT" in
  dev | staging | prod) ;;
  *) die "Invalid --env '$ENVIRONMENT' (expected dev|staging|prod)" ;;
  esac

  validate_flow_core_config
}

cloud_run_service_ingress() {
  local service_name="$1"

  gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(metadata.annotations."run.googleapis.com/ingress-status")'
}

cloud_run_latest_created_revision() {
  local service_name="$1"

  gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.latestCreatedRevisionName)'
}

cloud_run_latest_ready_revision() {
  local service_name="$1"

  gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.latestReadyRevisionName)'
}

service_requires_internal_health_check() {
  local service_name="$1"
  local ingress
  ingress="$(cloud_run_service_ingress "$service_name" 2>/dev/null || true)"

  [[ "$ingress" == "internal" || "$ingress" == "internal-and-cloud-load-balancing" ]]
}

wait_for_cloud_run_service_ready() {
  local service_name="$1"
  local label="$2"
  local timeout="$3"
  local elapsed=0

  log "Waiting for ${label} Cloud Run revision readiness (timeout: ${timeout}s)..."

  while [[ "$elapsed" -lt "$timeout" ]]; do
    local created_revision ready_revision
    created_revision="$(cloud_run_latest_created_revision "$service_name" 2>/dev/null || true)"
    ready_revision="$(cloud_run_latest_ready_revision "$service_name" 2>/dev/null || true)"

    if [[ -n "$created_revision" && "$created_revision" == "$ready_revision" ]]; then
      log "${label} Cloud Run revision ready (revision=${ready_revision})"
      return 0
    fi

    log "${label} Cloud Run revision not ready (created=${created_revision:-unknown}, ready=${ready_revision:-none}). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "${label} Cloud Run revision readiness timed out after ${timeout}s"
}

wait_for_compactor_success_log() {
  local service_name="$1"
  local revision="$2"
  local timeout="$3"
  local elapsed=0
  local query
  query="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${service_name}\" AND resource.labels.revision_name=\"${revision}\" AND jsonPayload.fields.message=\"Compaction cycle completed successfully\""

  log "Waiting for compactor success log (revision=${revision}, timeout: ${timeout}s)..."

  while [[ "$elapsed" -lt "$timeout" ]]; do
    local output timestamp line
    if ! output="$(gcloud logging read "$query" \
      --project="$PROJECT_ID" \
      --limit=1 \
      --format='value(timestamp)' 2>&1)"; then
      die "Cloud Logging health check failed for ${service_name}: ${output}"
    fi

    timestamp=""
    while IFS= read -r line; do
      if [[ -n "$line" ]]; then
        timestamp="$line"
        break
      fi
    done <<<"$output"

    if [[ -n "$timestamp" ]]; then
      log "Compactor healthy via Cloud Logging (revision=${revision}, last_success=${timestamp})"
      return 0
    fi

    log "Compactor waiting for successful compaction log (revision=${revision}). Waiting..."
    sleep "$HEALTH_CHECK_INTERVAL"
    elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
  done

  die "Compactor success log check timed out after ${timeout}s"
}

wait_for_internal_compactor_health() {
  local service_name="$1"
  local revision

  wait_for_cloud_run_service_ready "$service_name" "Compactor" "$COMPACTOR_HEALTH_TIMEOUT"
  revision="$(cloud_run_latest_ready_revision "$service_name")"
  wait_for_compactor_success_log "$service_name" "$revision" "$COMPACTOR_HEALTH_TIMEOUT"
}

start_run_proxy() {
  local service_name="$1"
  local port="$2"
  local pid

  # `--quiet` avoids interactive prompts.
  gcloud run services proxy "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --port="$port" \
    --quiet \
    >/dev/null 2>&1 &

  pid="$!"
  PROXY_PIDS="${PROXY_PIDS} ${pid}"
  echo "$pid"
}

kill_run_proxy_pid() {
  local pid="$1"

  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
}

stop_run_proxy() {
  local pid="$1"
  if [[ -n "${pid}" ]]; then
    kill_run_proxy_pid "$pid"
  fi

  local remaining=""
  local existing
  for existing in $PROXY_PIDS; do
    [[ "$existing" != "$pid" ]] && remaining="${remaining} ${existing}"
  done
  PROXY_PIDS="$remaining"
}

cleanup_run_proxies() {
  local pids="$PROXY_PIDS"
  local pid

  PROXY_PIDS=""
  for pid in $pids; do
    kill_run_proxy_pid "$pid"
  done
}

wait_for_compactor_health() {
  local service_name="arco-compactor-${ENVIRONMENT}"
  local local_port="18081"
  local pid=""

  if service_requires_internal_health_check "$service_name"; then
    log "Compactor is internal-only; using Cloud Run readiness and Cloud Logging health evidence"
    wait_for_internal_compactor_health "$service_name"
    return 0
  fi

  log "Waiting for compactor health via proxy (timeout: ${COMPACTOR_HEALTH_TIMEOUT}s)..."

  pid="$(start_run_proxy "$service_name" "$local_port")"

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
      stop_run_proxy "$pid"
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

  if service_requires_internal_health_check "$service_name"; then
    log "Flow compactor is internal-only; using Cloud Run revision readiness"
    wait_for_cloud_run_service_ready "$service_name" "Flow compactor" "$FLOW_COMPACTOR_HEALTH_TIMEOUT"
    return 0
  fi

  log "Waiting for flow compactor health via proxy (timeout: ${FLOW_COMPACTOR_HEALTH_TIMEOUT}s)..."

  pid="$(start_run_proxy "$service_name" "$local_port")"

  local elapsed=0
  while [[ "$elapsed" -lt "$FLOW_COMPACTOR_HEALTH_TIMEOUT" ]]; do
    local status
    status="$(curl -sf "http://127.0.0.1:${local_port}/health" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")"

    if [[ "$status" == "200" ]]; then
      log "Flow compactor healthy"
      stop_run_proxy "$pid"
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

  if service_requires_internal_health_check "$service_name"; then
    log "API is internal-only; using Cloud Run revision readiness"
    wait_for_cloud_run_service_ready "$service_name" "API" "$API_HEALTH_TIMEOUT"
    return 0
  fi

  log "Waiting for API health via proxy (timeout: ${API_HEALTH_TIMEOUT}s)..."

  pid="$(start_run_proxy "$service_name" "$local_port")"

  local elapsed=0
  while [[ "$elapsed" -lt "$API_HEALTH_TIMEOUT" ]]; do
    local status
    status="$(curl -sf "http://127.0.0.1:${local_port}/health" -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")"

    if [[ "$status" == "200" ]]; then
      log "API healthy"
      stop_run_proxy "$pid"
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

  local tfvars_file
  tfvars_file="$(tfvars_file_path)"
  if [[ ! -f "$tfvars_file" ]]; then
    die "tfvars file not found: $tfvars_file"
  fi

  export TF_VAR_api_image="$API_IMAGE"
  export TF_VAR_compactor_image="$COMPACTOR_IMAGE"
  export TF_VAR_flow_compactor_image="$FLOW_COMPACTOR_IMAGE"
  export_tf_var_if_set "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE"
  export_tf_var_if_set "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE"
  export_tf_var_if_set "flow_sweeper_image" "FLOW_SWEEPER_IMAGE"
  export_tf_var_if_set "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE"
  export_tf_var_if_set "flow_worker_image" "FLOW_WORKER_IMAGE"
  build_terraform_var_args "$tfvars_file"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN: Would deploy compactors first (terraform plan -target=google_cloud_run_v2_service.compactor -target=google_cloud_run_v2_service.flow_compactor -target=google_service_account.flow_worker)"
    terraform init -lockfile=readonly
    verify_existing_resources_imported
    terraform plan "${TERRAFORM_VAR_ARGS[@]}" \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor \
      -target=google_service_account.flow_worker
    log "DRY RUN: Would deploy remaining resources (terraform plan)"
    terraform plan "${TERRAFORM_VAR_ARGS[@]}"
  else
    terraform init -lockfile=readonly
    verify_existing_resources_imported
    # HARD GATE ENFORCEMENT:
    # Deploy compactors first, wait for them to become healthy, then deploy API.
    terraform apply "${TERRAFORM_VAR_ARGS[@]}" -auto-approve \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor \
      -target=google_service_account.flow_worker
  fi

  popd >/dev/null
}

deploy_terraform_remaining() {
  pushd "${ROOT_DIR}/infra/terraform" >/dev/null

  local tfvars_file
  tfvars_file="$(tfvars_file_path)"
  export TF_VAR_api_image="$API_IMAGE"
  export TF_VAR_compactor_image="$COMPACTOR_IMAGE"
  export TF_VAR_flow_compactor_image="$FLOW_COMPACTOR_IMAGE"
  export_tf_var_if_set "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE"
  export_tf_var_if_set "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE"
  export_tf_var_if_set "flow_sweeper_image" "FLOW_SWEEPER_IMAGE"
  export_tf_var_if_set "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE"
  export_tf_var_if_set "flow_worker_image" "FLOW_WORKER_IMAGE"
  build_terraform_var_args "$tfvars_file"

  terraform apply "${TERRAFORM_VAR_ARGS[@]}" -auto-approve

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
    --tfvars-file)
      TFVARS_FILE="$2"
      shift 2
      ;;
    --timeout)
      COMPACTOR_HEALTH_TIMEOUT="$2"
      shift 2
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

  trap cleanup_run_proxies EXIT

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
