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
#   ./scripts/deploy.sh [--env dev|staging|prod] [--tfvars-file PATH] [--dry-run] [--timeout SECONDS]
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DEPLOY_OWNER_LABEL="arco_deploy_owner"

ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${REGION:-us-central1}"
DRY_RUN=false
TFVARS_FILE="${TFVARS_FILE:-}"
COMPACTOR_HEALTH_TIMEOUT="${COMPACTOR_HEALTH_TIMEOUT:-300}"
FLOW_COMPACTOR_HEALTH_TIMEOUT="${FLOW_COMPACTOR_HEALTH_TIMEOUT:-120}"
API_HEALTH_TIMEOUT="${API_HEALTH_TIMEOUT:-120}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10}"
TERRAFORM_VAR_ARGS=()
UNMANAGED_TERRAFORM_RESOURCES=()
TERRAFORM_IMPORT_GUIDANCE=()
DEPLOY_LOCK_DIR=""
DEPLOY_LOCK_ACQUIRED=false

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --env ENV           Environment (dev, staging, prod). Default: dev
  --tfvars-file PATH  Terraform tfvars file. Relative paths resolve from repo root.
                      Default: infra/terraform/environments/<env>.tfvars
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
  ARCO_DEPLOY_OWNER       Required for non-dry-run live deployment mutations
                          Must be a GCP label-safe value so services can record ownership
  ARCO_DEPLOY_LOCK_DIR    Override the local deploy lock directory
  COMPACTOR_HEALTH_TIMEOUT
  API_HEALTH_TIMEOUT
  HEALTH_CHECK_INTERVAL
  FLOW_AUTOMATION_RECONCILER_IMAGE
  FLOW_COMPACTOR_INGRESS
  FLOW_DISPATCHER_IMAGE
  FLOW_SWEEPER_IMAGE
  FLOW_TIMER_INGEST_IMAGE
  FLOW_WORKER_IMAGE
  API_GIT_SHA
  API_CODE_VERSION
EOF
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

die() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
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

  printf '%s\n' "arco-api-${ENVIRONMENT}"
  printf '%s\n' "arco-compactor-${ENVIRONMENT}"
  printf '%s\n' "arco-flow-compactor-${ENVIRONMENT}"

  if [[ -n "$flow_tenant_id" && -n "$flow_workspace_id" && -n "$flow_dispatcher_image" && -n "$flow_sweeper_image" && -n "$flow_timer_ingest_image" && -n "$flow_worker_image" ]]; then
    printf '%s\n' "arco-flow-dispatcher-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-sweeper-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-timer-ingest-${ENVIRONMENT}"
    printf '%s\n' "arco-flow-worker-${ENVIRONMENT}"
  fi

  if [[ -n "$flow_tenant_id" && -n "$flow_workspace_id" && -n "$flow_automation_reconciler_image" ]]; then
    printf '%s\n' "arco-flow-automation-reconciler-${ENVIRONMENT}"
  fi
}

cloud_run_deploy_owner() {
  local service_name="$1"
  local output

  if output="$(gcloud run services describe "$service_name" \
    "--region=${REGION}" \
    "--project=${PROJECT_ID}" \
    "--format=value(metadata.labels.${DEPLOY_OWNER_LABEL})" 2>&1)"; then
    printf '%s' "$output"
    return 0
  fi

  if grep -Eiq '(not found|NOT_FOUND|404|does not exist)' <<<"$output"; then
    return 0
  fi

  die "Unable to inspect Cloud Run deploy owner for '${service_name}': $output"
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

tfvars_file_path() {
  if [[ -n "$TFVARS_FILE" ]]; then
    case "$TFVARS_FILE" in
    /*) echo "$TFVARS_FILE" ;;
    *) echo "${ROOT_DIR}/${TFVARS_FILE}" ;;
    esac
    return 0
  fi

  echo "${ROOT_DIR}/infra/terraform/environments/${ENVIRONMENT}.tfvars"
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

add_terraform_var_arg() {
  local name="$1"
  local value="$2"

  if [[ -n "$value" ]]; then
    TERRAFORM_VAR_ARGS+=("-var=${name}=${value}")
  fi
}

build_terraform_var_args() {
  TERRAFORM_VAR_ARGS=()

  add_terraform_var_arg "api_image" "$API_IMAGE"
  add_terraform_var_arg "api_git_sha" "$TF_VAR_api_git_sha"
  add_terraform_var_arg "api_code_version" "${API_CODE_VERSION:-}"
  add_terraform_var_arg "deploy_owner" "${ARCO_DEPLOY_OWNER:-}"
  add_terraform_var_arg "compactor_image" "$COMPACTOR_IMAGE"
  add_terraform_var_arg "flow_compactor_image" "$FLOW_COMPACTOR_IMAGE"
  add_terraform_var_arg "flow_compactor_ingress" "${FLOW_COMPACTOR_INGRESS:-}"
  add_terraform_var_arg "flow_automation_reconciler_image" "${FLOW_AUTOMATION_RECONCILER_IMAGE:-}"
  add_terraform_var_arg "flow_dispatcher_image" "${FLOW_DISPATCHER_IMAGE:-}"
  add_terraform_var_arg "flow_sweeper_image" "${FLOW_SWEEPER_IMAGE:-}"
  add_terraform_var_arg "flow_timer_ingest_image" "${FLOW_TIMER_INGEST_IMAGE:-}"
  add_terraform_var_arg "flow_worker_image" "${FLOW_WORKER_IMAGE:-}"
  add_terraform_var_arg "flow_tenant_id" "$(effective_tfvar_value "flow_tenant_id")"
  add_terraform_var_arg "flow_workspace_id" "$(effective_tfvar_value "flow_workspace_id")"
  add_terraform_var_arg "project_number" "${TF_VAR_project_number:-}"
}

api_git_sha() {
  if [[ -n "${API_GIT_SHA:-}" ]]; then
    printf '%s' "$API_GIT_SHA"
    return 0
  fi

  git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || true
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

validate_flow_compactor_ingress() {
  case "${FLOW_COMPACTOR_INGRESS:-}" in
  "" | internal | all) ;;
  *) die "FLOW_COMPACTOR_INGRESS must be one of: internal, all" ;;
  esac
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
  validate_flow_compactor_ingress
}

terraform_state_has_address() {
  local address="$1"
  local state_addresses="$2"

  grep -Fxq "$address" <<<"$state_addresses"
}

gcp_cloud_run_service_exists() {
  local service_name="$1"
  local output

  if output="$(gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(metadata.name)' 2>&1)"; then
    return 0
  fi

  if grep -Eiq '(not found|NOT_FOUND|404|does not exist)' <<<"$output"; then
    return 1
  fi

  die "Unable to inspect Cloud Run service '$service_name': $output"
}

gcp_cloud_run_job_exists() {
  local job_name="$1"
  local output

  if output="$(gcloud run jobs describe "$job_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(metadata.name)' 2>&1)"; then
    return 0
  fi

  if grep -Eiq '(not found|NOT_FOUND|404|does not exist)' <<<"$output"; then
    return 1
  fi

  die "Unable to inspect Cloud Run job '$job_name': $output"
}

gcp_service_account_exists() {
  local email="$1"
  local output

  if output="$(gcloud iam service-accounts describe "$email" \
    --project="$PROJECT_ID" \
    --format='value(email)' 2>&1)"; then
    return 0
  fi

  if grep -Eiq '(not found|NOT_FOUND|404|does not exist)' <<<"$output"; then
    return 1
  fi

  die "Unable to inspect service account '$email': $output"
}

gcp_project_custom_role_exists() {
  local role_id="$1"
  local output

  if output="$(gcloud iam roles describe "$role_id" \
    --project="$PROJECT_ID" \
    --format='value(name)' 2>&1)"; then
    return 0
  fi

  if grep -Eiq '(not found|NOT_FOUND|404|does not exist)' <<<"$output"; then
    return 1
  fi

  die "Unable to inspect project IAM role '$role_id': $output"
}

gcp_storage_bucket_exists() {
  local bucket_name="$1"
  local output

  if output="$(gcloud storage buckets describe "gs://${bucket_name}" \
    --format='value(name)' 2>&1)"; then
    return 0
  fi

  if grep -Eiq '(not found|NOT_FOUND|404|does not exist)' <<<"$output"; then
    return 1
  fi

  die "Unable to inspect storage bucket '$bucket_name': $output"
}

append_import_guidance() {
  local tfvars_file="$1"
  local address="$2"
  local import_id="$3"

  TERRAFORM_IMPORT_GUIDANCE+=("terraform -chdir=infra/terraform import -var-file=${tfvars_file} ${address} ${import_id}")
}

check_unmanaged_cloud_run_service() {
  local state_addresses="$1"
  local tfvars_file="$2"
  local address="$3"
  local service_name="$4"

  if terraform_state_has_address "$address" "$state_addresses"; then
    return 0
  fi

  if gcp_cloud_run_service_exists "$service_name"; then
    UNMANAGED_TERRAFORM_RESOURCES+=("${service_name} exists in GCP but ${address} is not in Terraform state")
    append_import_guidance \
      "$tfvars_file" \
      "$address" \
      "projects/${PROJECT_ID}/locations/${REGION}/services/${service_name}"
  fi
}

check_unmanaged_cloud_run_job() {
  local state_addresses="$1"
  local tfvars_file="$2"
  local address="$3"
  local job_name="$4"

  if terraform_state_has_address "$address" "$state_addresses"; then
    return 0
  fi

  if gcp_cloud_run_job_exists "$job_name"; then
    UNMANAGED_TERRAFORM_RESOURCES+=("${job_name} exists in GCP but ${address} is not in Terraform state")
    append_import_guidance \
      "$tfvars_file" \
      "$address" \
      "projects/${PROJECT_ID}/locations/${REGION}/jobs/${job_name}"
  fi
}

check_unmanaged_service_account() {
  local state_addresses="$1"
  local tfvars_file="$2"
  local address="$3"
  local email="$4"

  if terraform_state_has_address "$address" "$state_addresses"; then
    return 0
  fi

  if gcp_service_account_exists "$email"; then
    UNMANAGED_TERRAFORM_RESOURCES+=("${email} exists in GCP but ${address} is not in Terraform state")
    append_import_guidance \
      "$tfvars_file" \
      "$address" \
      "projects/${PROJECT_ID}/serviceAccounts/${email}"
  fi
}

check_unmanaged_project_custom_role() {
  local state_addresses="$1"
  local tfvars_file="$2"
  local address="$3"
  local role_id="$4"

  if terraform_state_has_address "$address" "$state_addresses"; then
    return 0
  fi

  if gcp_project_custom_role_exists "$role_id"; then
    UNMANAGED_TERRAFORM_RESOURCES+=("projects/${PROJECT_ID}/roles/${role_id} exists in GCP but ${address} is not in Terraform state")
    append_import_guidance \
      "$tfvars_file" \
      "$address" \
      "projects/${PROJECT_ID}/roles/${role_id}"
  fi
}

check_unmanaged_storage_bucket() {
  local state_addresses="$1"
  local tfvars_file="$2"
  local address="$3"
  local bucket_name="$4"

  if terraform_state_has_address "$address" "$state_addresses"; then
    return 0
  fi

  if gcp_storage_bucket_exists "$bucket_name"; then
    UNMANAGED_TERRAFORM_RESOURCES+=("${bucket_name} exists in GCP but ${address} is not in Terraform state")
    append_import_guidance "$tfvars_file" "$address" "$bucket_name"
  fi
}

preflight_managed_live_resources() {
  local tfvars_file="$1"
  local state_addresses
  local flow_tenant_id flow_workspace_id flow_dispatcher_image flow_sweeper_image
  local flow_timer_ingest_image flow_worker_image flow_automation_reconciler_image

  UNMANAGED_TERRAFORM_RESOURCES=()
  TERRAFORM_IMPORT_GUIDANCE=()
  state_addresses="$(terraform state list 2>/dev/null || true)"

  check_unmanaged_storage_bucket \
    "$state_addresses" \
    "$tfvars_file" \
    "google_storage_bucket.catalog" \
    "${PROJECT_ID}-arco-catalog-${ENVIRONMENT}"

  check_unmanaged_cloud_run_service \
    "$state_addresses" \
    "$tfvars_file" \
    "google_cloud_run_v2_service.api" \
    "arco-api-${ENVIRONMENT}"
  check_unmanaged_cloud_run_service \
    "$state_addresses" \
    "$tfvars_file" \
    "google_cloud_run_v2_service.compactor" \
    "arco-compactor-${ENVIRONMENT}"
  check_unmanaged_cloud_run_service \
    "$state_addresses" \
    "$tfvars_file" \
    "google_cloud_run_v2_service.flow_compactor" \
    "arco-flow-compactor-${ENVIRONMENT}"
  check_unmanaged_cloud_run_job \
    "$state_addresses" \
    "$tfvars_file" \
    "google_cloud_run_v2_job.compactor_antientropy" \
    "arco-compactor-antientropy-${ENVIRONMENT}"

  check_unmanaged_service_account \
    "$state_addresses" \
    "$tfvars_file" \
    "google_service_account.api" \
    "arco-api-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  check_unmanaged_service_account \
    "$state_addresses" \
    "$tfvars_file" \
    "google_service_account.compactor" \
    "arco-compactor-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  check_unmanaged_service_account \
    "$state_addresses" \
    "$tfvars_file" \
    "google_service_account.compactor_antientropy" \
    "arco-compactor-ae-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  check_unmanaged_service_account \
    "$state_addresses" \
    "$tfvars_file" \
    "google_service_account.invoker" \
    "arco-invoker-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  check_unmanaged_service_account \
    "$state_addresses" \
    "$tfvars_file" \
    "google_service_account.flow_controller" \
    "arco-flow-controller-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  check_unmanaged_service_account \
    "$state_addresses" \
    "$tfvars_file" \
    "google_service_account.flow_task_invoker" \
    "arco-flow-task-invoker-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  check_unmanaged_project_custom_role \
    "$state_addresses" \
    "$tfvars_file" \
    "google_project_iam_custom_role.storage_object_reader_no_list" \
    "storageObjectReaderNoList"
  check_unmanaged_project_custom_role \
    "$state_addresses" \
    "$tfvars_file" \
    "google_project_iam_custom_role.storage_object_lister" \
    "storageObjectLister"
  check_unmanaged_project_custom_role \
    "$state_addresses" \
    "$tfvars_file" \
    "google_project_iam_custom_role.storage_object_writer_no_list" \
    "storageObjectWriterNoList"

  flow_tenant_id="$(effective_tfvar_value "flow_tenant_id")"
  flow_workspace_id="$(effective_tfvar_value "flow_workspace_id")"
  flow_dispatcher_image="$(effective_tfvar_value "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE")"
  flow_sweeper_image="$(effective_tfvar_value "flow_sweeper_image" "FLOW_SWEEPER_IMAGE")"
  flow_timer_ingest_image="$(effective_tfvar_value "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE")"
  flow_worker_image="$(effective_tfvar_value "flow_worker_image" "FLOW_WORKER_IMAGE")"
  flow_automation_reconciler_image="$(effective_tfvar_value "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE")"

  if [[ -n "$flow_tenant_id" && -n "$flow_workspace_id" && -n "$flow_dispatcher_image" && -n "$flow_sweeper_image" && -n "$flow_timer_ingest_image" && -n "$flow_worker_image" ]]; then
    check_unmanaged_cloud_run_service \
      "$state_addresses" \
      "$tfvars_file" \
      "google_cloud_run_v2_service.flow_dispatcher[0]" \
      "arco-flow-dispatcher-${ENVIRONMENT}"
    check_unmanaged_cloud_run_service \
      "$state_addresses" \
      "$tfvars_file" \
      "google_cloud_run_v2_service.flow_sweeper[0]" \
      "arco-flow-sweeper-${ENVIRONMENT}"
    check_unmanaged_cloud_run_service \
      "$state_addresses" \
      "$tfvars_file" \
      "google_cloud_run_v2_service.flow_timer_ingest[0]" \
      "arco-flow-timer-ingest-${ENVIRONMENT}"
    check_unmanaged_cloud_run_service \
      "$state_addresses" \
      "$tfvars_file" \
      "google_cloud_run_v2_service.flow_worker[0]" \
      "arco-flow-worker-${ENVIRONMENT}"
    check_unmanaged_service_account \
      "$state_addresses" \
      "$tfvars_file" \
      "google_service_account.flow_timer_ingest[0]" \
      "arco-flow-timer-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
    check_unmanaged_service_account \
      "$state_addresses" \
      "$tfvars_file" \
      "google_service_account.flow_worker[0]" \
      "arco-flow-worker-${ENVIRONMENT}@${PROJECT_ID}.iam.gserviceaccount.com"
  fi

  if [[ -n "$flow_tenant_id" && -n "$flow_workspace_id" && -n "$flow_automation_reconciler_image" ]]; then
    check_unmanaged_cloud_run_service \
      "$state_addresses" \
      "$tfvars_file" \
      "google_cloud_run_v2_service.flow_automation_reconciler[0]" \
      "arco-flow-automation-reconciler-${ENVIRONMENT}"
  fi

  if [[ "${#UNMANAGED_TERRAFORM_RESOURCES[@]}" -gt 0 ]]; then
    {
      echo "The following resources already exist in GCP but are missing from Terraform state:"
      printf '  - %s\n' "${UNMANAGED_TERRAFORM_RESOURCES[@]}"
      echo
      echo "Import or adopt these resources before running deploy:"
      printf '  %s\n' "${TERRAFORM_IMPORT_GUIDANCE[@]}"
    } >&2
    exit 1
  fi
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

  local tfvars_file
  tfvars_file="$(tfvars_file_path)"
  if [[ ! -f "$tfvars_file" ]]; then
    die "tfvars file not found: $tfvars_file"
  fi

  export TF_VAR_api_image="$API_IMAGE"
  TF_VAR_api_git_sha="$(api_git_sha)"
  export TF_VAR_api_git_sha
  export TF_VAR_compactor_image="$COMPACTOR_IMAGE"
  export TF_VAR_flow_compactor_image="$FLOW_COMPACTOR_IMAGE"
  export_tf_var_if_set "api_code_version" "API_CODE_VERSION"
  export_tf_var_if_set "deploy_owner" "ARCO_DEPLOY_OWNER"
  export_tf_var_if_set "flow_compactor_ingress" "FLOW_COMPACTOR_INGRESS"
  export_tf_var_if_set "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE"
  export_tf_var_if_set "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE"
  export_tf_var_if_set "flow_sweeper_image" "FLOW_SWEEPER_IMAGE"
  export_tf_var_if_set "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE"
  export_tf_var_if_set "flow_worker_image" "FLOW_WORKER_IMAGE"
  if [[ -n "${PROJECT_NUMBER:-}" ]]; then
    export TF_VAR_project_number="$PROJECT_NUMBER"
  else
    # Best-effort: avoids Terraform needing Cloud Resource Manager permissions just to read project number.
    TF_VAR_project_number="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)' 2>/dev/null || true)"
    export TF_VAR_project_number
  fi
  build_terraform_var_args

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY RUN: Would deploy compactors first (terraform plan -target=google_cloud_run_v2_service.compactor -target=google_cloud_run_v2_service.flow_compactor -target=google_service_account.flow_worker)"
    terraform init -lockfile=readonly
    preflight_managed_live_resources "$tfvars_file"
    terraform plan -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}" \
      -target=google_cloud_run_v2_service.compactor \
      -target=google_cloud_run_v2_service.flow_compactor \
      -target=google_service_account.flow_worker
    log "DRY RUN: Would deploy remaining resources (terraform plan)"
    terraform plan -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}"
  else
    terraform init -lockfile=readonly
    preflight_managed_live_resources "$tfvars_file"
    # HARD GATE ENFORCEMENT:
    # Deploy compactors first, wait for them to become healthy, then deploy API.
    terraform apply -var-file="$tfvars_file" "${TERRAFORM_VAR_ARGS[@]}" -auto-approve \
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
  TF_VAR_api_git_sha="$(api_git_sha)"
  export TF_VAR_api_git_sha
  export TF_VAR_compactor_image="$COMPACTOR_IMAGE"
  export TF_VAR_flow_compactor_image="$FLOW_COMPACTOR_IMAGE"
  export_tf_var_if_set "api_code_version" "API_CODE_VERSION"
  export_tf_var_if_set "deploy_owner" "ARCO_DEPLOY_OWNER"
  export_tf_var_if_set "flow_compactor_ingress" "FLOW_COMPACTOR_INGRESS"
  export_tf_var_if_set "flow_automation_reconciler_image" "FLOW_AUTOMATION_RECONCILER_IMAGE"
  export_tf_var_if_set "flow_dispatcher_image" "FLOW_DISPATCHER_IMAGE"
  export_tf_var_if_set "flow_sweeper_image" "FLOW_SWEEPER_IMAGE"
  export_tf_var_if_set "flow_timer_ingest_image" "FLOW_TIMER_INGEST_IMAGE"
  export_tf_var_if_set "flow_worker_image" "FLOW_WORKER_IMAGE"
  if [[ -n "${PROJECT_NUMBER:-}" ]]; then
    export TF_VAR_project_number="$PROJECT_NUMBER"
  else
    TF_VAR_project_number="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)' 2>/dev/null || true)"
    export TF_VAR_project_number
  fi
  build_terraform_var_args

  preflight_managed_live_resources "$tfvars_file"
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
    --tfvars-file)
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
  preflight_cloud_deploy_owner

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
