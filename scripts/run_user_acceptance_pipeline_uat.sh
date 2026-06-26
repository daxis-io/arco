#!/usr/bin/env bash
#
# Runs Arco's cataloged pipeline user-acceptance suite.
#
# Usage:
#   ./scripts/run_user_acceptance_pipeline_uat.sh [--deterministic] [--with-hygiene] [--live-durable] [--live-deployed] [--all] [--dry-run] [--status] [--status-output PATH] [--require-live-deployed-ready]
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DEPLOY_OWNER_LABEL="arco_deploy_owner"

RUN_DETERMINISTIC=false
RUN_LIVE_DURABLE=false
RUN_LIVE_DEPLOYED=false
RUN_HYGIENE=false
DRY_RUN=false
SHOW_STATUS=false
REQUIRE_LIVE_DEPLOYED_READY=false
PREFLIGHT_ONLY=false
STATUS_OUTPUT_PATH=""
LIVE_DEPLOYED_PROXY_PID=""
LIVE_DEPLOYED_STATUS_READY=false

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --deterministic    Run CI-safe deterministic UAT tests.
  --with-hygiene     Also run focused local shell smoke, clippy, fmt, and diff checks.
  --live-durable     Run ignored durable object-storage UAT gate.
                     Validates evidence artifacts after the live run.
  --live-deployed    Run ignored deployed API plus worker UAT gate.
                     Validates evidence artifacts after the live run.
  --all              Run deterministic tests plus both live gates.
  --dry-run          Print commands without executing them.
  --preflight-only   Run live-gate preflights without tests or evidence writes.
  --status           Print deterministic/live gate readiness without running tests.
  --status-output PATH
                     Write the printed readiness status snapshot to PATH.
  --require-live-deployed-ready
                     Print status and exit nonzero unless live-deployed is ready.
  -h, --help         Show this help message.

Required env vars for --live-durable:
  ARCO_UAT_STORAGE_BUCKET

Required env vars for --live-deployed:
  ARCO_UAT_API_URL
    or
  ARCO_UAT_CLOUD_RUN_SERVICE and PROJECT_ID
  ARCO_UAT_EXPECTED_API_CODE_VERSION
  ARCO_UAT_EXPECTED_API_GIT_SHA
  ARCO_UAT_EXPECTED_API_IMAGE
  ARCO_DEPLOY_OWNER     Required for full non-dry-run deployed UAT runs.
                        Not required for --dry-run or --preflight-only.

Optional env vars:
  ARCO_UAT_TENANT
  ARCO_UAT_WORKSPACE
  ARCO_UAT_API_TOKEN
  ARCO_UAT_CLOUD_RUN_PORT  Default: 18080
  ARCO_UAT_PREFLIGHT_TIMEOUT_SECS   Default: 30
  ARCO_UAT_PREFLIGHT_INTERVAL_SECS  Default: 2
  ARCO_UAT_CLOUD_RUN_PROJECT_ID     Cloud Run project for owner-label checks
                                    Default: PROJECT_ID when set
  ARCO_UAT_CLOUD_SCHEDULER_PROJECT  Default: PROJECT_ID when set
  ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT
                           Default: arco-invoker-<env>@<scheduler-project>.iam.gserviceaccount.com
  ARCO_UAT_ENVIRONMENT              Default: dev
  REGION                   Default: us-central1
  ARCO_UAT_RUN_TIMEOUT_SECS
  ARCO_UAT_EVIDENCE_DIR     Default: target/uat-evidence
EOF
}

print_env_status() {
  local name="$1"
  local default_value="${2:-}"

  if [[ -n "${!name:-}" ]]; then
    if [[ "${name}" == *TOKEN* || "${name}" == *SECRET* || "${name}" == *PASSWORD* ]]; then
      echo "  ${name}=set"
    else
      echo "  ${name}=${!name}"
    fi
  elif [[ -n "${default_value}" ]]; then
    echo "  ${name}=${default_value}"
  else
    echo "  ${name}=unset"
  fi
}

join_status_reasons() {
  local reason output=""

  for reason in "$@"; do
    if [[ -n "${output}" ]]; then
      output+="; "
    fi
    output+="${reason}"
  done
  printf '%s' "${output}"
}

print_status() {
  local scheduler_output scheduler_status owner_output owner_status scope_output scope_status
  local api_version_output api_version_status
  local strict_mode scheduler_checked owner_checked scope_checked api_version_checked
  LIVE_DEPLOYED_STATUS_READY=false
  strict_mode="${REQUIRE_LIVE_DEPLOYED_READY:-false}"
  scheduler_checked=true
  owner_checked=true
  scope_checked=true
  api_version_checked=false

  set +e
  scheduler_output="$(print_live_deployed_scheduler_status)"
  scheduler_status=$?
  owner_output="$(print_live_deployed_cloud_run_owner_status)"
  owner_status=$?
  api_version_status=0
  api_version_output="live-deployed-api-version: not checked (use --require-live-deployed-ready to verify deployed API provenance)"
  scope_status=0
  scope_output="live-deployed-flow-scope: not checked (set ARCO_DEPLOY_OWNER and PROJECT_ID or ARCO_UAT_CLOUD_RUN_PROJECT_ID for deployed UAT)"
  if [[ -z "$(flow_scheduler_project)" ]]; then
    scheduler_checked=false
  fi
  if [[ -z "${ARCO_DEPLOY_OWNER:-}" || -z "$(cloud_run_owner_project)" ]]; then
    owner_checked=false
  fi
  scope_checked=false
  if [[ -n "${ARCO_DEPLOY_OWNER:-}" && "${owner_status}" -eq 0 ]]; then
    scope_output="$(print_live_deployed_flow_scope_status)"
    scope_status=$?
    if [[ -n "$(cloud_run_owner_project)" ]]; then
      scope_checked=true
    fi
  fi
  if [[ "${strict_mode}" == true ]]; then
    api_version_output="$(print_live_deployed_api_version_status)"
    api_version_status=$?
    api_version_checked=true
    if [[ "${api_version_status}" -eq 4 ]]; then
      api_version_checked=false
      api_version_status=0
    fi
  fi
  set -e

  echo "User acceptance pipeline UAT status"
  echo "deterministic: ready"

  if [[ -n "${ARCO_UAT_STORAGE_BUCKET:-}" ]]; then
    echo "live-durable: ready"
  else
    echo "live-durable: missing ARCO_UAT_STORAGE_BUCKET"
  fi
  print_env_status ARCO_UAT_STORAGE_BUCKET
  print_env_status ARCO_UAT_TENANT "arco-uat-tenant"
  print_env_status ARCO_UAT_WORKSPACE "arco-uat-workspace"
  print_env_status ARCO_UAT_EVIDENCE_DIR "target/uat-evidence"

  if live_deployed_access_ready && [[ "${scheduler_status}" -eq 0 && "${owner_status}" -eq 0 && "${scope_status}" -eq 0 && "${api_version_status}" -eq 0 && ("${strict_mode}" != true || ("${scheduler_checked}" == true && "${owner_checked}" == true && "${scope_checked}" == true && "${api_version_checked}" == true)) ]]; then
    LIVE_DEPLOYED_STATUS_READY=true
    echo "live-deployed: ready"
  elif live_deployed_access_ready; then
    local -a not_ready_reasons=()
    if [[ "${strict_mode}" == true && "${scheduler_checked}" != true ]]; then
      echo "live-deployed: not ready (flow scheduler readiness not checked)"
    elif [[ "${strict_mode}" == true && "${owner_checked}" != true ]]; then
      echo "live-deployed: not ready (Cloud Run deploy owner not checked)"
    elif [[ "${strict_mode}" == true && "${scope_checked}" != true ]]; then
      echo "live-deployed: not ready (flow service tenant/workspace not checked)"
    elif [[ "${strict_mode}" == true && "${api_version_checked}" != true ]]; then
      echo "live-deployed: not ready (deployed API provenance not checked)"
    else
      if [[ "${scheduler_status}" -ne 0 ]]; then
        if [[ "${scheduler_status}" -eq 2 ]]; then
          not_ready_reasons+=("flow scheduler target metadata mismatch")
        elif [[ "${scheduler_status}" -eq 3 ]]; then
          not_ready_reasons+=("flow scheduler jobs and target metadata are not ready")
        else
          not_ready_reasons+=("flow scheduler jobs are not ENABLED")
        fi
      fi
      if [[ "${owner_status}" -ne 0 ]]; then
        not_ready_reasons+=("Cloud Run deploy owner mismatch")
      fi
      if [[ "${scope_status}" -ne 0 ]]; then
        not_ready_reasons+=("flow service tenant/workspace mismatch")
      fi
      if [[ "${api_version_status}" -ne 0 ]]; then
        not_ready_reasons+=("deployed API provenance mismatch")
      fi
      if [[ "${#not_ready_reasons[@]}" -eq 0 ]]; then
        echo "live-deployed: not ready (unknown deployed readiness failure)"
      else
        echo "live-deployed: not ready ($(join_status_reasons "${not_ready_reasons[@]}"))"
      fi
    fi
  else
    echo "live-deployed: missing ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE/PROJECT_ID"
  fi
  printf '%s\n' "${scheduler_output}"
  printf '%s\n' "${owner_output}"
  printf '%s\n' "${scope_output}"
  printf '%s\n' "${api_version_output}"
  print_env_status ARCO_UAT_API_URL
  print_env_status ARCO_UAT_CLOUD_RUN_SERVICE
  print_env_status PROJECT_ID
  print_env_status ARCO_UAT_CLOUD_RUN_PROJECT_ID
  print_env_status ARCO_UAT_CLOUD_SCHEDULER_PROJECT
  print_env_status ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT
  print_env_status ARCO_UAT_ENVIRONMENT "dev"
  print_env_status REGION "us-central1"
  print_env_status ARCO_UAT_CLOUD_RUN_PORT "18080"
  print_env_status ARCO_UAT_EXPECTED_API_CODE_VERSION
  print_env_status ARCO_UAT_EXPECTED_API_GIT_SHA
  print_env_status ARCO_UAT_EXPECTED_API_IMAGE
  print_env_status ARCO_DEPLOY_OWNER
  print_env_status ARCO_UAT_API_TOKEN
  print_env_status ARCO_UAT_RUN_TIMEOUT_SECS "300"
}

write_status_output() {
  local status_output_file status_output_dir

  status_output_file="$(mktemp "${TMPDIR:-/tmp}/arco-uat-status.XXXXXX")"
  print_status >"${status_output_file}"
  cat "${status_output_file}"

  if [[ -n "${STATUS_OUTPUT_PATH}" ]]; then
    status_output_dir="$(dirname "${STATUS_OUTPUT_PATH}")"
    mkdir -p "${status_output_dir}"
    cp "${status_output_file}" "${STATUS_OUTPUT_PATH}"
  fi

  rm -f "${status_output_file}"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || die "${name} is required"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

run_cmd() {
  if [[ "${DRY_RUN}" == true ]]; then
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

run_env_cmd() {
  local -a env_args=()
  while [[ "$#" -gt 0 && "$1" == *=* ]]; do
    env_args+=("$1")
    shift
  done

  if [[ "${DRY_RUN}" == true ]]; then
    printf '%q ' "${env_args[@]}" "$@"
    printf '\n'
  else
    env "${env_args[@]}" "$@"
  fi
}

cloud_run_proxy_requested() {
  [[ -n "${ARCO_UAT_CLOUD_RUN_SERVICE:-}" ]]
}

live_deployed_access_ready() {
  if [[ -n "${ARCO_UAT_API_URL:-}" ]]; then
    return 0
  fi

  [[ -n "${ARCO_UAT_CLOUD_RUN_SERVICE:-}" && -n "${PROJECT_ID:-}" ]]
}

validate_live_deployed_access() {
  if [[ -n "${ARCO_UAT_API_URL:-}" && -n "${ARCO_UAT_CLOUD_RUN_SERVICE:-}" ]]; then
    die "Set either ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE, not both."
  fi

  if cloud_run_proxy_requested; then
    require_env PROJECT_ID
    return 0
  fi

  if [[ -z "${ARCO_UAT_API_URL:-}" ]]; then
    die "ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE/PROJECT_ID is required"
  fi
}

require_full_live_deployed_owner() {
  if [[ "${DRY_RUN}" == true || "${PREFLIGHT_ONLY}" == true ]]; then
    return 0
  fi

  if [[ -z "${ARCO_DEPLOY_OWNER:-}" ]]; then
    die "ARCO_DEPLOY_OWNER is required for full deployed UAT runs; use --preflight-only for read-only readiness checks without claiming a live run window."
  fi
}

require_live_deployed_api_provenance_expectations() {
  local -a missing=()

  [[ -n "${ARCO_UAT_EXPECTED_API_CODE_VERSION:-}" ]] || missing+=("ARCO_UAT_EXPECTED_API_CODE_VERSION")
  [[ -n "${ARCO_UAT_EXPECTED_API_GIT_SHA:-}" ]] || missing+=("ARCO_UAT_EXPECTED_API_GIT_SHA")
  [[ -n "${ARCO_UAT_EXPECTED_API_IMAGE:-}" ]] || missing+=("ARCO_UAT_EXPECTED_API_IMAGE")

  if [[ "${#missing[@]}" -gt 0 ]]; then
    die "$(join_status_reasons "${missing[@]}") are required for deployed API provenance checks"
  fi
}

live_deployed_api_url() {
  if cloud_run_proxy_requested; then
    printf 'http://127.0.0.1:%s' "${ARCO_UAT_CLOUD_RUN_PORT:-18080}"
    return 0
  fi

  printf '%s' "$ARCO_UAT_API_URL"
}

live_deployed_access_mode() {
  if cloud_run_proxy_requested; then
    printf 'cloud-run-proxy'
  else
    printf 'direct-url'
  fi
}

live_deployed_ingress_mode() {
  if cloud_run_proxy_requested; then
    printf 'internal-only'
  else
    printf 'not-recorded'
  fi
}

flow_scheduler_project() {
  printf '%s' "${ARCO_UAT_CLOUD_SCHEDULER_PROJECT:-${PROJECT_ID:-}}"
}

flow_scheduler_region() {
  printf '%s' "${REGION:-us-central1}"
}

flow_scheduler_environment() {
  printf '%s' "${ARCO_UAT_ENVIRONMENT:-${ENVIRONMENT:-dev}}"
}

flow_scheduler_expected_invoker_service_account() {
  local project environment
  project="$(flow_scheduler_project)"
  environment="$(flow_scheduler_environment)"
  printf '%s' "${ARCO_UAT_CLOUD_SCHEDULER_INVOKER_SERVICE_ACCOUNT:-arco-invoker-${environment}@${project}.iam.gserviceaccount.com}"
}

cloud_run_owner_project() {
  printf '%s' "${ARCO_UAT_CLOUD_RUN_PROJECT_ID:-${PROJECT_ID:-}}"
}

flow_scheduler_jobs() {
  local environment
  environment="$(flow_scheduler_environment)"
  printf '%s\n' "arco-flow-dispatcher-run-${environment}"
  printf '%s\n' "arco-flow-sweeper-run-${environment}"
}

live_deployed_cloud_run_services() {
  local environment
  environment="$(flow_scheduler_environment)"
  printf '%s\n' "arco-api-${environment}"
  printf '%s\n' "arco-compactor-${environment}"
  printf '%s\n' "arco-flow-compactor-${environment}"
  printf '%s\n' "arco-flow-dispatcher-${environment}"
  printf '%s\n' "arco-flow-sweeper-${environment}"
  printf '%s\n' "arco-flow-timer-ingest-${environment}"
  printf '%s\n' "arco-flow-worker-${environment}"
}

live_deployed_flow_scope_services() {
  local environment
  environment="$(flow_scheduler_environment)"
  printf '%s %s %s\n' "arco-compactor-${environment}" "ARCO_TENANT_ID" "ARCO_WORKSPACE_ID"
  printf '%s %s %s\n' "arco-flow-compactor-${environment}" "ARCO_TENANT_ID" "ARCO_WORKSPACE_ID"
  printf '%s %s %s\n' "arco-flow-dispatcher-${environment}" "ARCO_TENANT_ID" "ARCO_WORKSPACE_ID"
  printf '%s %s %s\n' "arco-flow-sweeper-${environment}" "ARCO_TENANT_ID" "ARCO_WORKSPACE_ID"
  printf '%s %s %s\n' "arco-flow-timer-ingest-${environment}" "ARCO_TENANT_ID" "ARCO_WORKSPACE_ID"
  printf '%s %s %s\n' "arco-flow-worker-${environment}" "ARCO_FLOW_TENANT_ID" "ARCO_FLOW_WORKSPACE_ID"
}

cloud_run_env_value() {
  local service_json="$1"
  local env_name="$2"
  jq -r --arg name "$env_name" \
    'first(.spec.template.spec.containers[0].env[]? | select(.name == $name) | .value) // ""' \
    <<<"$service_json"
}

validate_live_deployed_api_version_json() {
  local version_json="$1"

  python3 - "$version_json" \
    "${ARCO_UAT_EXPECTED_API_CODE_VERSION:-}" \
    "${ARCO_UAT_EXPECTED_API_GIT_SHA:-}" \
    "${ARCO_UAT_EXPECTED_API_IMAGE:-}" <<'PY'
import json
import sys


def fail(message):
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


try:
    doc = json.loads(sys.argv[1])
except json.JSONDecodeError as error:
    fail(f"deployed API /version returned invalid JSON: {error}")

if not isinstance(doc, dict):
    fail("deployed API /version must be a JSON object")

required_strings = (
    "service",
    "packageVersion",
    "codeVersion",
    "gitSha",
    "image",
    "cloudRunRevision",
)
for field in required_strings:
    value = doc.get(field)
    if not isinstance(value, str) or not value.strip():
        fail(f"deployed API /version {field} must be a non-empty string")

for field in ("codeVersion", "gitSha", "image", "cloudRunRevision"):
    if doc[field].strip().lower() == "unknown":
        fail(f"deployed API /version {field} must not be unknown")

expected = {
    "codeVersion": sys.argv[2],
    "gitSha": sys.argv[3],
    "image": sys.argv[4],
}
for field, expected_value in expected.items():
    if not expected_value.strip():
        fail(f"expected deployed API {field} is required")
    actual = doc[field]
    if actual != expected_value:
        fail(f"deployed API /version {field} is {actual}; expected {expected_value}")
PY
}

fetch_live_deployed_api_version_json() {
  local api_url version_url timeout interval elapsed tick version_json
  api_url="$(live_deployed_api_url)"
  version_url="${api_url%/}/version"

  timeout="${ARCO_UAT_PREFLIGHT_TIMEOUT_SECS:-30}"
  interval="${ARCO_UAT_PREFLIGHT_INTERVAL_SECS:-2}"
  elapsed=0
  tick="$interval"
  if [[ "$tick" -lt 1 ]]; then
    tick=1
  fi
  version_json=""
  while [[ "$elapsed" -le "$timeout" ]]; do
    if version_json="$(curl -fsS "$version_url")"; then
      printf '%s' "$version_json"
      return 0
    fi
    if [[ "$elapsed" -ge "$timeout" ]]; then
      break
    fi
    if [[ "$interval" -gt 0 ]]; then
      sleep "$interval"
    fi
    elapsed=$((elapsed + tick))
  done

  return 1
}

print_live_deployed_api_version_status() {
  local version_json validation_output

  if ! live_deployed_access_ready; then
    echo "live-deployed-api-version: not checked (set ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE/PROJECT_ID)"
    return 4
  fi
  if [[ -z "${ARCO_UAT_EXPECTED_API_CODE_VERSION:-}" || -z "${ARCO_UAT_EXPECTED_API_GIT_SHA:-}" || -z "${ARCO_UAT_EXPECTED_API_IMAGE:-}" ]]; then
    echo "live-deployed-api-version: not checked (set ARCO_UAT_EXPECTED_API_CODE_VERSION, ARCO_UAT_EXPECTED_API_GIT_SHA, and ARCO_UAT_EXPECTED_API_IMAGE)"
    return 4
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "live-deployed-api-version: unable to check deployed API /version (curl is required)"
    return 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "live-deployed-api-version: unable to check deployed API /version (python3 is required)"
    return 1
  fi

  start_live_deployed_proxy
  if ! version_json="$(fetch_live_deployed_api_version_json)"; then
    stop_live_deployed_proxy
    echo "live-deployed-api-version: not ready (deployed API /version preflight failed)"
    return 1
  fi
  stop_live_deployed_proxy

  if validation_output="$(validate_live_deployed_api_version_json "$version_json" 2>&1)"; then
    echo "live-deployed-api-version: ready codeVersion=${ARCO_UAT_EXPECTED_API_CODE_VERSION} gitSha=${ARCO_UAT_EXPECTED_API_GIT_SHA} image=${ARCO_UAT_EXPECTED_API_IMAGE}"
    return 0
  fi

  validation_output="${validation_output#ERROR: }"
  echo "live-deployed-api-version: not ready (${validation_output})"
  return 1
}

print_live_deployed_scheduler_status() {
  local project region environment job job_json state uri audience service_account http_method
  local expected_audience expected_service_account all_ready state_ready target_ready status_lines line
  project="$(flow_scheduler_project)"
  if [[ -z "$project" ]]; then
    echo "live-deployed-scheduler: not checked (set PROJECT_ID or ARCO_UAT_CLOUD_SCHEDULER_PROJECT for Cloud Run deployed UAT)"
    return 0
  fi

  region="$(flow_scheduler_region)"
  environment="$(flow_scheduler_environment)"
  if ! command -v gcloud >/dev/null 2>&1; then
    echo "live-deployed-scheduler: unable to check ${project}/${region} env=${environment} (gcloud is required)"
    return 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "live-deployed-scheduler: unable to check ${project}/${region} env=${environment} (jq is required)"
    return 1
  fi

  all_ready=1
  state_ready=1
  target_ready=1
  expected_service_account="$(flow_scheduler_expected_invoker_service_account)"
  status_lines=()
  while IFS= read -r job; do
    [[ -n "$job" ]] || continue
    if ! job_json="$(gcloud scheduler jobs describe "$job" \
      --project="$project" \
      --location="$region" \
      --format="json(state,httpTarget)" 2>/dev/null)"; then
      all_ready=0
      state_ready=0
      status_lines+=("  ${job}=unknown (describe failed)")
      continue
    fi

    if ! state="$(jq -r '.state // ""' <<<"$job_json")"; then
      all_ready=0
      state_ready=0
      status_lines+=("  ${job}=unknown (invalid describe JSON)")
      continue
    fi
    uri="$(jq -r '.httpTarget.uri // ""' <<<"$job_json")"
    audience="$(jq -r '.httpTarget.oidcToken.audience // ""' <<<"$job_json")"
    service_account="$(jq -r '.httpTarget.oidcToken.serviceAccountEmail // ""' <<<"$job_json")"
    http_method="$(jq -r '.httpTarget.httpMethod // ""' <<<"$job_json")"

    state="$(printf '%s' "$state" | tr -d '[:space:]')"
    if [[ "$state" != "ENABLED" ]]; then
      all_ready=0
      state_ready=0
      status_lines+=("  ${job}=${state:-unknown} (expected ENABLED)")
      continue
    fi

    line="  ${job}=ENABLED uri=${uri:-unset}"
    if [[ "$uri" != */run ]]; then
      all_ready=0
      target_ready=0
      line+=" (expected /run target)"
    fi

    expected_audience=""
    if [[ "$uri" == */run ]]; then
      expected_audience="${uri%/run}"
    fi
    line+=" audience=${audience:-unset}"
    if [[ -z "$expected_audience" || "$audience" != "$expected_audience" ]]; then
      all_ready=0
      target_ready=0
      line+=" (expected ${expected_audience:-Cloud Run service URL without /run})"
    fi
    line+=" serviceAccount=${service_account:-unset}"
    if [[ "$service_account" != "$expected_service_account" ]]; then
      all_ready=0
      target_ready=0
      line+=" (expected ${expected_service_account})"
    fi
    line+=" method=${http_method:-unset}"
    if [[ "$http_method" != "POST" ]]; then
      all_ready=0
      target_ready=0
      line+=" (expected POST)"
    fi
    status_lines+=("$line")
  done < <(flow_scheduler_jobs)

  if [[ "$all_ready" -eq 1 ]]; then
    echo "live-deployed-scheduler: ready for ${project}/${region} env=${environment}"
    printf '%s\n' "${status_lines[@]}"
    return 0
  fi

  echo "live-deployed-scheduler: not ready for ${project}/${region} env=${environment}"
  printf '%s\n' "${status_lines[@]}"
  if [[ "$state_ready" -eq 1 && "$target_ready" -eq 0 ]]; then
    return 2
  fi
  if [[ "$state_ready" -eq 0 && "$target_ready" -eq 0 ]]; then
    return 3
  fi
  return 1
}

print_live_deployed_flow_scope_status() {
  local project region environment expected_tenant expected_workspace all_ready status_lines
  local service tenant_env workspace_env service_json tenant workspace

  project="$(cloud_run_owner_project)"
  region="$(flow_scheduler_region)"
  environment="$(flow_scheduler_environment)"
  expected_tenant="${ARCO_UAT_TENANT:-arco-uat-tenant}"
  expected_workspace="${ARCO_UAT_WORKSPACE:-arco-uat-workspace}"

  if [[ -z "$project" ]]; then
    echo "live-deployed-flow-scope: not checked (set PROJECT_ID or ARCO_UAT_CLOUD_RUN_PROJECT_ID for deployed UAT)"
    return 0
  fi

  if ! command -v gcloud >/dev/null 2>&1; then
    echo "live-deployed-flow-scope: unable to check ${project}/${region} env=${environment} (gcloud is required)"
    return 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "live-deployed-flow-scope: unable to check ${project}/${region} env=${environment} (jq is required)"
    return 1
  fi

  all_ready=1
  status_lines=()
  while read -r service tenant_env workspace_env; do
    [[ -n "$service" ]] || continue
    if ! service_json="$(gcloud run services describe "$service" \
      --project="$project" \
      --region="$region" \
      "--format=json(spec.template.spec.containers[0].env)" 2>/dev/null)"; then
      all_ready=0
      status_lines+=("  ${service}=unknown (describe failed)")
      continue
    fi

    tenant="$(cloud_run_env_value "$service_json" "$tenant_env")"
    workspace="$(cloud_run_env_value "$service_json" "$workspace_env")"
    if [[ "$tenant" == "$expected_tenant" && "$workspace" == "$expected_workspace" ]]; then
      status_lines+=("  ${service}=${tenant}/${workspace}")
    else
      all_ready=0
      status_lines+=("  ${service}=${tenant:-unset}/${workspace:-unset} (expected ${expected_tenant}/${expected_workspace})")
    fi
  done < <(live_deployed_flow_scope_services)

  if [[ "$all_ready" -eq 1 ]]; then
    echo "live-deployed-flow-scope: ready for ${project}/${region} env=${environment} expected tenant=${expected_tenant} workspace=${expected_workspace}"
    printf '%s\n' "${status_lines[@]}"
    return 0
  fi

  echo "live-deployed-flow-scope: not ready for ${project}/${region} env=${environment} expected tenant=${expected_tenant} workspace=${expected_workspace}"
  printf '%s\n' "${status_lines[@]}"
  return 1
}

print_live_deployed_cloud_run_owner_status() {
  local project region environment service owner all_ready status_lines

  if [[ -z "${ARCO_DEPLOY_OWNER:-}" ]]; then
    echo "live-deployed-cloud-run-owner: not checked (set ARCO_DEPLOY_OWNER and PROJECT_ID or ARCO_UAT_CLOUD_RUN_PROJECT_ID for deployed UAT)"
    return 0
  fi

  project="$(cloud_run_owner_project)"
  region="$(flow_scheduler_region)"
  environment="$(flow_scheduler_environment)"

  if [[ -z "$project" ]]; then
    echo "live-deployed-cloud-run-owner: unable to check ${region} env=${environment} (set PROJECT_ID or ARCO_UAT_CLOUD_RUN_PROJECT_ID)"
    return 1
  fi

  if ! command -v gcloud >/dev/null 2>&1; then
    echo "live-deployed-cloud-run-owner: unable to check ${project}/${region} env=${environment} (gcloud is required)"
    return 1
  fi

  all_ready=1
  status_lines=()
  while IFS= read -r service; do
    [[ -n "$service" ]] || continue
    if ! owner="$(gcloud run services describe "$service" \
      --project="$project" \
      --region="$region" \
      "--format=value(metadata.labels.${DEPLOY_OWNER_LABEL})" 2>/dev/null)"; then
      all_ready=0
      status_lines+=("  ${service}=unknown (describe failed)")
      continue
    fi
    owner="$(printf '%s' "$owner" | tr -d '[:space:]')"
    if [[ "$owner" == "$ARCO_DEPLOY_OWNER" ]]; then
      status_lines+=("  ${service}=${owner}")
    else
      all_ready=0
      status_lines+=("  ${service}=${owner:-unset} (expected ${ARCO_DEPLOY_OWNER})")
    fi
  done < <(live_deployed_cloud_run_services)

  if [[ "$all_ready" -eq 1 ]]; then
    echo "live-deployed-cloud-run-owner: ready for ${project}/${region} env=${environment}"
    printf '%s\n' "${status_lines[@]}"
    return 0
  fi

  echo "live-deployed-cloud-run-owner: not ready for ${project}/${region} env=${environment}"
  printf '%s\n' "${status_lines[@]}"
  return 1
}

preflight_flow_service_scope() {
  local project region service tenant_env workspace_env service_json tenant workspace
  local expected_tenant expected_workspace

  project="$(cloud_run_owner_project)"
  if [[ -z "$project" ]]; then
    return 0
  fi

  region="$(flow_scheduler_region)"
  expected_tenant="${ARCO_UAT_TENANT:-arco-uat-tenant}"
  expected_workspace="${ARCO_UAT_WORKSPACE:-arco-uat-workspace}"

  while read -r service tenant_env workspace_env; do
    [[ -n "$service" ]] || continue
    if [[ "${DRY_RUN}" == true ]]; then
      run_cmd gcloud run services describe "$service" \
        "--project=${project}" \
        "--region=${region}" \
        "--format=json(spec.template.spec.containers[0].env)"
      continue
    fi

    require_cmd gcloud
    require_cmd jq
    if ! service_json="$(gcloud run services describe "$service" \
      --project="$project" \
      --region="$region" \
      "--format=json(spec.template.spec.containers[0].env)")"; then
      die "deployed flow service scope preflight failed: ${service} could not be described in ${project}/${region}"
    fi
    tenant="$(cloud_run_env_value "$service_json" "$tenant_env")"
    workspace="$(cloud_run_env_value "$service_json" "$workspace_env")"
    if [[ "$tenant" != "$expected_tenant" || "$workspace" != "$expected_workspace" ]]; then
      die "deployed flow service scope preflight failed: ${service} tenant/workspace ${tenant:-unset}/${workspace:-unset}; expected ${expected_tenant}/${expected_workspace}"
    fi
  done < <(live_deployed_flow_scope_services)
}

preflight_flow_scheduler_jobs() {
  local project region environment job job_json state uri audience service_account http_method
  local expected_audience expected_service_account
  project="$(flow_scheduler_project)"
  if [[ -z "$project" ]]; then
    return 0
  fi

  region="$(flow_scheduler_region)"
  environment="$(flow_scheduler_environment)"
  expected_service_account="$(flow_scheduler_expected_invoker_service_account)"

  while IFS= read -r job; do
    [[ -n "$job" ]] || continue
    if [[ "${DRY_RUN}" == true ]]; then
      run_cmd gcloud scheduler jobs describe "$job" \
        "--project=${project}" \
        "--location=${region}" \
        "--format=json(state,httpTarget)"
      continue
    fi

    require_cmd gcloud
    require_cmd jq
    if ! job_json="$(gcloud scheduler jobs describe "$job" \
      --project="$project" \
      --location="$region" \
      --format="json(state,httpTarget)")"; then
      die "deployed flow scheduler preflight failed: ${job} could not be described in ${project}/${region}"
    fi
    if ! state="$(jq -r '.state // ""' <<<"$job_json")"; then
      die "deployed flow scheduler preflight failed: ${job} returned invalid describe JSON"
    fi
    uri="$(jq -r '.httpTarget.uri // ""' <<<"$job_json")"
    audience="$(jq -r '.httpTarget.oidcToken.audience // ""' <<<"$job_json")"
    service_account="$(jq -r '.httpTarget.oidcToken.serviceAccountEmail // ""' <<<"$job_json")"
    http_method="$(jq -r '.httpTarget.httpMethod // ""' <<<"$job_json")"
    state="$(printf '%s' "$state" | tr -d '[:space:]')"
    if [[ "$state" != "ENABLED" ]]; then
      die "deployed flow scheduler preflight failed: ${job} state is ${state:-unknown}; expected ENABLED"
    fi
    if [[ "$uri" != */run ]]; then
      die "deployed flow scheduler preflight failed: ${job} target uri is ${uri:-unset}; expected /run path"
    fi
    expected_audience="${uri%/run}"
    if [[ "$audience" != "$expected_audience" ]]; then
      die "deployed flow scheduler preflight failed: ${job} OIDC audience is ${audience:-unset}; expected ${expected_audience}"
    fi
    if [[ "$service_account" != "$expected_service_account" ]]; then
      die "deployed flow scheduler preflight failed: ${job} OIDC service account is ${service_account:-unset}; expected ${expected_service_account}"
    fi
    if [[ "$http_method" != "POST" ]]; then
      die "deployed flow scheduler preflight failed: ${job} HTTP method is ${http_method:-unset}; expected POST"
    fi
  done < <(flow_scheduler_jobs)
}

preflight_cloud_run_deploy_owner() {
  local project region service owner

  if [[ -z "${ARCO_DEPLOY_OWNER:-}" ]]; then
    return 0
  fi

  project="$(cloud_run_owner_project)"
  region="$(flow_scheduler_region)"
  if [[ -z "$project" ]]; then
    die "ARCO_DEPLOY_OWNER requires PROJECT_ID or ARCO_UAT_CLOUD_RUN_PROJECT_ID for deployed Cloud Run owner checks"
  fi

  while IFS= read -r service; do
    [[ -n "$service" ]] || continue
    if [[ "${DRY_RUN}" == true ]]; then
      run_cmd gcloud run services describe "$service" \
        "--project=${project}" \
        "--region=${region}" \
        "--format=value(metadata.labels.${DEPLOY_OWNER_LABEL})"
      continue
    fi

    require_cmd gcloud
    if ! owner="$(gcloud run services describe "$service" \
      --project="$project" \
      --region="$region" \
      "--format=value(metadata.labels.${DEPLOY_OWNER_LABEL})")"; then
      die "deployed Cloud Run owner preflight failed: ${service} could not be described in ${project}/${region}"
    fi
    owner="$(printf '%s' "$owner" | tr -d '[:space:]')"
    if [[ "$owner" != "$ARCO_DEPLOY_OWNER" ]]; then
      die "deployed Cloud Run owner preflight failed: ${service} current ${DEPLOY_OWNER_LABEL}=${owner:-unset}; expected ARCO_DEPLOY_OWNER=${ARCO_DEPLOY_OWNER}"
    fi
  done < <(live_deployed_cloud_run_services)
}

start_live_deployed_proxy() {
  local service_name port region

  if ! cloud_run_proxy_requested; then
    return 0
  fi

  service_name="$ARCO_UAT_CLOUD_RUN_SERVICE"
  port="${ARCO_UAT_CLOUD_RUN_PORT:-18080}"
  region="${REGION:-us-central1}"

  if [[ "${DRY_RUN}" == true ]]; then
    run_cmd gcloud run services proxy "$service_name" \
      "--project=${PROJECT_ID}" \
      "--region=${region}" \
      "--port=${port}" \
      --quiet
    return 0
  fi

  require_cmd gcloud
  gcloud run services proxy "$service_name" \
    --project="$PROJECT_ID" \
    --region="$region" \
    --port="$port" \
    --quiet \
    >/dev/null 2>&1 &
  LIVE_DEPLOYED_PROXY_PID="$!"
  trap stop_live_deployed_proxy EXIT
}

stop_live_deployed_proxy() {
  if [[ -n "${LIVE_DEPLOYED_PROXY_PID}" ]]; then
    kill "$LIVE_DEPLOYED_PROXY_PID" >/dev/null 2>&1 || true
    wait "$LIVE_DEPLOYED_PROXY_PID" >/dev/null 2>&1 || true
    LIVE_DEPLOYED_PROXY_PID=""
  fi
}

run_deterministic() {
  run_cmd cargo test -p arco-api test_parse_partition_selector
  run_cmd cargo test -p arco-api --test system_tables_api query_exposes_system_orchestration_runs_when_state_is_only_in_l0
  run_cmd cargo test -p arco-flow --test orchestration_rebuild_dr rebuild_orders_replay_by_event_id_when_event_timestamps_are_skewed
  run_cmd cargo test -p arco-api test_trigger_run_reemits_when_reservation_exists
  run_cmd cargo test -p arco-flow test_handle_task_completed_failure
  run_cmd cargo test -p arco-integration-tests --test user_acceptance_pipeline
  run_cmd cargo test -p arco-integration-tests --test orchestration_external_worker_e2e
}

run_hygiene() {
  run_cmd bash tools/test_user_acceptance_uat_runner.sh
  run_cmd bash tools/test_user_acceptance_evidence_validator.sh
  run_cmd bash tools/test_actionlint_runner.sh
  run_cmd cargo clippy -p arco-integration-tests --test user_acceptance_pipeline -- -D warnings
  run_cmd cargo clippy -p arco-api -- -D warnings
  run_cmd cargo fmt --check
  run_cmd git diff --check
}

uat_evidence_dir() {
  local evidence_dir="${ARCO_UAT_EVIDENCE_DIR:-target/uat-evidence}"
  case "${evidence_dir}" in
  /*) echo "${evidence_dir}" ;;
  *) echo "${ROOT_DIR}/${evidence_dir}" ;;
  esac
}

uat_evidence_marker() {
  echo "$(uat_evidence_dir)/.uat-validation-start"
}

preflight_live_deployed() {
  local api_url version_url version_json
  api_url="$(live_deployed_api_url)"
  version_url="${api_url%/}/version"

  if [[ "${DRY_RUN}" == true ]]; then
    run_cmd curl -fsS "$version_url"
    preflight_flow_scheduler_jobs
    return 0
  fi

  require_cmd curl
  require_cmd python3

  if ! version_json="$(fetch_live_deployed_api_version_json)"; then
    die "deployed API /version preflight failed: ${version_url}"
  fi

  if ! validate_live_deployed_api_version_json "$version_json"; then
    die "deployed API /version preflight returned stale or invalid metadata"
  fi

  preflight_flow_scheduler_jobs
}

mark_live_evidence_start() {
  run_cmd mkdir -p "$(uat_evidence_dir)"
  run_cmd touch "$(uat_evidence_marker)"
}

run_live_durable() {
  require_env ARCO_UAT_STORAGE_BUCKET
  run_env_cmd \
    "ARCO_UAT_STORAGE_BUCKET=${ARCO_UAT_STORAGE_BUCKET}" \
    "ARCO_UAT_TENANT=${ARCO_UAT_TENANT:-arco-uat-tenant}" \
    "ARCO_UAT_WORKSPACE=${ARCO_UAT_WORKSPACE:-arco-uat-workspace}" \
    "ARCO_UAT_EVIDENCE_DIR=$(uat_evidence_dir)" \
    cargo test -p arco-integration-tests --test user_acceptance_pipeline \
    -- --ignored live_user_acceptance_pipeline_runs_against_durable_storage
}

run_live_deployed() {
  local api_url access_mode ingress_mode region cloud_run_project_id
  api_url="$(live_deployed_api_url)"
  access_mode="$(live_deployed_access_mode)"
  ingress_mode="$(live_deployed_ingress_mode)"
  region="${REGION:-us-central1}"
  cloud_run_project_id="$(cloud_run_owner_project)"

  run_env_cmd \
    "ARCO_UAT_API_URL=${api_url}" \
    "ARCO_UAT_API_ACCESS_MODE=${access_mode}" \
    "ARCO_UAT_API_INGRESS_MODE=${ingress_mode}" \
    "ARCO_UAT_CLOUD_RUN_SERVICE=${ARCO_UAT_CLOUD_RUN_SERVICE:-}" \
    "ARCO_UAT_CLOUD_RUN_PROJECT_ID=${cloud_run_project_id}" \
    "ARCO_UAT_CLOUD_RUN_REGION=${region}" \
    "ARCO_UAT_API_TOKEN=${ARCO_UAT_API_TOKEN:-}" \
    "ARCO_UAT_TENANT=${ARCO_UAT_TENANT:-arco-uat-tenant}" \
    "ARCO_UAT_WORKSPACE=${ARCO_UAT_WORKSPACE:-arco-uat-workspace}" \
    "ARCO_UAT_RUN_TIMEOUT_SECS=${ARCO_UAT_RUN_TIMEOUT_SECS:-300}" \
    "ARCO_UAT_EVIDENCE_DIR=$(uat_evidence_dir)" \
    cargo test -p arco-integration-tests --test user_acceptance_pipeline \
    -- --ignored live_deployed_user_acceptance_pipeline_runs_through_api_and_workers
}

validate_live_evidence() {
  local -a validator_args=(
    --newer-than "$(uat_evidence_marker)"
  )
  if [[ "${RUN_LIVE_DURABLE}" == true ]]; then
    validator_args+=(--require-kind durable_storage)
  fi
  if [[ "${RUN_LIVE_DEPLOYED}" == true ]]; then
    validator_args+=(--require-kind deployed_api_worker)
    validator_args+=(
      --expect-api-code-version "${ARCO_UAT_EXPECTED_API_CODE_VERSION}"
      --expect-api-git-sha "${ARCO_UAT_EXPECTED_API_GIT_SHA}"
      --expect-api-image "${ARCO_UAT_EXPECTED_API_IMAGE}"
    )
  fi
  validator_args+=("$(uat_evidence_dir)")
  run_cmd tools/validate_user_acceptance_evidence.sh "${validator_args[@]}"
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
  --deterministic)
    RUN_DETERMINISTIC=true
    ;;
  --with-hygiene)
    RUN_HYGIENE=true
    ;;
  --live-durable)
    RUN_LIVE_DURABLE=true
    ;;
  --live-deployed)
    RUN_LIVE_DEPLOYED=true
    ;;
  --all)
    RUN_DETERMINISTIC=true
    RUN_LIVE_DURABLE=true
    RUN_LIVE_DEPLOYED=true
    ;;
  --dry-run)
    DRY_RUN=true
    ;;
  --preflight-only)
    PREFLIGHT_ONLY=true
    ;;
  --status)
    SHOW_STATUS=true
    ;;
  --status-output)
    [[ -n "${2:-}" ]] || die "--status-output requires PATH"
    STATUS_OUTPUT_PATH="$2"
    SHOW_STATUS=true
    shift
    ;;
  --require-live-deployed-ready)
    SHOW_STATUS=true
    REQUIRE_LIVE_DEPLOYED_READY=true
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    die "unknown option: $1"
    ;;
  esac
  shift
done

if [[ "${RUN_DETERMINISTIC}" == false && "${RUN_LIVE_DURABLE}" == false && "${RUN_LIVE_DEPLOYED}" == false && "${SHOW_STATUS}" == false ]]; then
  RUN_DETERMINISTIC=true
fi

cd "${ROOT_DIR}"

if [[ "${SHOW_STATUS}" == true ]]; then
  write_status_output
  if [[ "${REQUIRE_LIVE_DEPLOYED_READY}" == true && "${LIVE_DEPLOYED_STATUS_READY}" != true ]]; then
    die "live-deployed readiness check failed"
  fi
fi
if [[ "${RUN_LIVE_DURABLE}" == true ]]; then
  require_env ARCO_UAT_STORAGE_BUCKET
fi
if [[ "${RUN_LIVE_DEPLOYED}" == true ]]; then
  validate_live_deployed_access
  require_live_deployed_api_provenance_expectations
  require_full_live_deployed_owner
  preflight_cloud_run_deploy_owner
  preflight_flow_service_scope
  start_live_deployed_proxy
  preflight_live_deployed
fi
if [[ "${PREFLIGHT_ONLY}" == true ]]; then
  exit 0
fi
if [[ "${RUN_LIVE_DURABLE}" == true || "${RUN_LIVE_DEPLOYED}" == true ]]; then
  mark_live_evidence_start
fi
if [[ "${RUN_DETERMINISTIC}" == true ]]; then
  run_deterministic
fi
if [[ "${RUN_LIVE_DURABLE}" == true ]]; then
  run_live_durable
fi
if [[ "${RUN_LIVE_DEPLOYED}" == true ]]; then
  run_live_deployed
fi
if [[ "${RUN_LIVE_DURABLE}" == true || "${RUN_LIVE_DEPLOYED}" == true ]]; then
  validate_live_evidence
fi
if [[ "${RUN_HYGIENE}" == true ]]; then
  run_hygiene
fi
