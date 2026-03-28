#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAR_FILE="${1:-environments/dev.tfvars}"

require_bin() {
  local bin="$1"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "missing required binary: $bin" >&2
    exit 1
  fi
}

require_bin terraform
require_bin jq

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

plan_file="$tmpdir/plan.tfplan"
plan_json="$tmpdir/plan.json"

terraform -chdir="$ROOT_DIR" plan -lock=false -var-file="$VAR_FILE" -out="$plan_file" >/dev/null
terraform -chdir="$ROOT_DIR" show -json "$plan_file" >"$plan_json"

query() {
  local expr="$1"
  jq -r "$expr" "$plan_json"
}

fail() {
  local message="$1"
  echo "FAIL: $message" >&2
  exit 1
}

expect_eq() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if [[ "$actual" != "$expected" ]]; then
    fail "$message (expected '$expected', got '$actual')"
  fi
}

expect_empty() {
  local actual="$1"
  local message="$2"
  if [[ -n "$actual" && "$actual" != "null" ]]; then
    fail "$message (got '$actual')"
  fi
}

resource_attr() {
  local address="$1"
  local expr="$2"
  query ".planned_values.root_module.resources[] | select(.address==\"$address\") | $expr"
}

cloud_run_address() {
  local service="$1"
  case "$service" in
    flow_timer_ingest)
      echo "google_cloud_run_v2_service.flow_timer_ingest[0]"
      ;;
    *)
      echo "google_cloud_run_v2_service.${service}"
      ;;
  esac
}

api_ingress="$(resource_attr "google_cloud_run_v2_service.api" ".values.ingress")"
expect_eq "$api_ingress" "INGRESS_TRAFFIC_ALL" "API must use public ingress for dev callback testing"

for service in flow_compactor flow_dispatcher flow_sweeper flow_worker flow_timer_ingest; do
  ingress="$(resource_attr "$(cloud_run_address "$service")" ".values.ingress")"
  expect_eq "$ingress" "INGRESS_TRAFFIC_ALL" "${service} must use public ingress when no VPC connector is configured"
done

for service in api compactor flow_compactor flow_dispatcher flow_sweeper flow_timer_ingest flow_worker; do
  min_instances="$(resource_attr "$(cloud_run_address "$service")" ".values.template[0].scaling[0].min_instance_count")"
  expect_eq "$min_instances" "0" "${service} must scale to zero in dev"
done

dispatcher_uri="$(resource_attr "google_cloud_scheduler_job.flow_dispatcher_run[0]" ".values.http_target[0].uri")"
dispatcher_audience="$(resource_attr "google_cloud_scheduler_job.flow_dispatcher_run[0]" ".values.http_target[0].oidc_token[0].audience")"
expect_eq "$dispatcher_audience" "$dispatcher_uri" "Dispatcher scheduler audience must match the Cloud Run URL"

sweeper_uri="$(resource_attr "google_cloud_scheduler_job.flow_sweeper_run[0]" ".values.http_target[0].uri")"
sweeper_audience="$(resource_attr "google_cloud_scheduler_job.flow_sweeper_run[0]" ".values.http_target[0].oidc_token[0].audience")"
expect_eq "$sweeper_audience" "$sweeper_uri" "Sweeper scheduler audience must match the Cloud Run URL"

flow_controller_email="$(resource_attr "google_service_account.flow_controller" ".values.email")"
flow_controller_member="serviceAccount:${flow_controller_email}"
api_email="$(resource_attr "google_service_account.api" ".values.email")"
api_member="serviceAccount:${api_email}"
flow_task_invoker_name="$(resource_attr "google_service_account.flow_task_invoker" ".values.name")"

dispatcher_enqueuer_member="$(resource_attr "google_project_iam_member.flow_dispatcher_cloudtasks_enqueuer[0]" ".values.member")"
expect_eq "$dispatcher_enqueuer_member" "$flow_controller_member" "Dispatcher Cloud Tasks role must be granted to the runtime service account"

sweeper_enqueuer_member="$(resource_attr "google_project_iam_member.flow_sweeper_cloudtasks_enqueuer[0]" ".values.member")"
expect_eq "$sweeper_enqueuer_member" "$flow_controller_member" "Sweeper Cloud Tasks role must be granted to the runtime service account"

dispatcher_act_as_member="$(resource_attr "google_service_account_iam_member.flow_dispatcher_act_as_tasks_oidc[0]" ".values.member")"
expect_eq "$dispatcher_act_as_member" "$flow_controller_member" "Dispatcher OIDC actAs binding must target the runtime service account"
dispatcher_act_as_target="$(resource_attr "google_service_account_iam_member.flow_dispatcher_act_as_tasks_oidc[0]" ".values.service_account_id")"
expect_eq "$dispatcher_act_as_target" "$flow_task_invoker_name" "Dispatcher OIDC actAs binding must target the worker invoker service account"

sweeper_act_as_member="$(resource_attr "google_service_account_iam_member.flow_sweeper_act_as_tasks_oidc[0]" ".values.member")"
expect_eq "$sweeper_act_as_member" "$flow_controller_member" "Sweeper OIDC actAs binding must target the runtime service account"
sweeper_act_as_target="$(resource_attr "google_service_account_iam_member.flow_sweeper_act_as_tasks_oidc[0]" ".values.service_account_id")"
expect_eq "$sweeper_act_as_target" "$flow_task_invoker_name" "Sweeper OIDC actAs binding must target the worker invoker service account"

flow_compactor_invoker_member="$(resource_attr "google_cloud_run_v2_service_iam_member.flow_controller_flow_compactor[0]" ".values.member")"
expect_eq "$flow_compactor_invoker_member" "$flow_controller_member" "Flow controller must be allowed to invoke the flow compactor"

api_manifest_writer_member="$(resource_attr "google_storage_bucket_iam_member.api_write_manifests" ".values.member")"
expect_eq "$api_manifest_writer_member" "$api_member" "API must be allowed to write manifest objects"

api_relaxed_member="$(resource_attr "google_storage_bucket_iam_member.api_relaxed_catalog_access[0]" ".values.member")"
expect_eq "$api_relaxed_member" "$api_member" "Dev test env must grant the API broad bucket object access"

flow_controller_relaxed_member="$(resource_attr "google_storage_bucket_iam_member.flow_controller_relaxed_catalog_access[0]" ".values.member")"
expect_eq "$flow_controller_relaxed_member" "$flow_controller_member" "Dev test env must grant the flow controller broad bucket object access"

compactor_email="$(resource_attr "google_service_account.compactor" ".values.email")"
compactor_member="serviceAccount:${compactor_email}"
compactor_relaxed_member="$(resource_attr "google_storage_bucket_iam_member.compactor_relaxed_catalog_access[0]" ".values.member")"
expect_eq "$compactor_relaxed_member" "$compactor_member" "Dev test env must grant the compactor broad bucket object access"

api_flow_compactor_env="$(
  query '.planned_values.root_module.resources[] 
    | select(.address=="google_cloud_run_v2_service.api") 
    | .values.template[0].containers[0].env[]?
    | select(.name=="ARCO_ORCH_COMPACTOR_URL")
    | .value'
)"
expect_empty "$api_flow_compactor_env" "API dev deployment must not force remote orchestration compaction"

echo "PASS: arco-testing dev routing contract holds"
