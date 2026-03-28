#!/usr/bin/env bash
#
# Repeatable cloud smoke for the Arco orchestration path:
# manifest deploy -> run trigger -> dispatcher tick -> worker callback -> SUCCEEDED
#
# Usage:
#   PROJECT_ID=arco-testing-20260320 bash scripts/cloud-orchestration-smoke.sh
#
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${REGION:-us-central1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
TENANT_ID="${TENANT_ID:-tenant-dev}"
WORKSPACE_ID="${WORKSPACE_ID:-workspace-dev}"
TASK_KEY="${TASK_KEY:-analytics.users}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-90}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
API_URL="${API_URL:-}"
DISPATCHER_URL="${DISPATCHER_URL:-}"
ARCO_AUTH_TOKEN="${ARCO_AUTH_TOKEN:-}"
USE_GCLOUD_ID_TOKEN="${USE_GCLOUD_ID_TOKEN:-true}"
API_ID_TOKEN="${API_ID_TOKEN:-}"
DISPATCHER_ID_TOKEN="${DISPATCHER_ID_TOKEN:-}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --project PROJECT_ID       GCP project id (default: current gcloud project)
  --region REGION            Cloud Run region (default: ${REGION})
  --env ENVIRONMENT          Environment suffix for service names (default: ${ENVIRONMENT})
  --tenant TENANT_ID         Tenant header value (default: ${TENANT_ID})
  --workspace WORKSPACE_ID   Workspace header value (default: ${WORKSPACE_ID})
  --task-key TASK_KEY        Asset/task key to deploy and trigger (default: ${TASK_KEY})
  --timeout SECONDS          Max wait for SUCCEEDED (default: ${TIMEOUT_SECONDS})
  --poll-interval SECONDS    Poll interval between dispatcher ticks (default: ${POLL_INTERVAL_SECONDS})
  --api-url URL              Override arco-api base URL
  --dispatcher-url URL       Override arco-flow-dispatcher base URL
  -h, --help                 Show this help text

Environment:
  PROJECT_ID
  REGION
  ENVIRONMENT
  TENANT_ID
  WORKSPACE_ID
  TASK_KEY
  TIMEOUT_SECONDS
  POLL_INTERVAL_SECONDS
  API_URL
  DISPATCHER_URL
  ARCO_AUTH_TOKEN            Optional bearer token for API/dispatcher requests
  USE_GCLOUD_ID_TOKEN        Auto-mint Cloud Run ID tokens with gcloud (default: true)
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

discover_service_url() {
  local service_name="$1"
  gcloud run services describe "$service_name" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.url)'
}

discover_id_token() {
  local audience="$1"
  local token
  token="$(gcloud auth print-identity-token --audiences="$audience" 2>/dev/null || true)"
  if [[ -n "$token" ]]; then
    printf '%s' "$token"
    return 0
  fi

  gcloud auth print-identity-token 2>/dev/null || true
}

post_json() {
  local url="$1"
  local payload_file="$2"
  local output_file="$3"
  local serverless_token="${4:-}"
  local curl_args=(
    -sS
    -X POST
    -o "$output_file"
    -w '%{http_code}'
    -H "Content-Type: application/json"
    -H "X-Tenant-Id: ${TENANT_ID}"
    -H "X-Workspace-Id: ${WORKSPACE_ID}"
    --data "@${payload_file}"
  )

  if [[ -n "$ARCO_AUTH_TOKEN" ]]; then
    curl_args+=(-H "Authorization: Bearer ${ARCO_AUTH_TOKEN}")
  fi
  if [[ -n "$serverless_token" ]]; then
    curl_args+=(-H "X-Serverless-Authorization: Bearer ${serverless_token}")
  fi

  curl "${curl_args[@]}" "$url"
}

post_empty() {
  local url="$1"
  local output_file="$2"
  local serverless_token="${3:-}"
  local curl_args=(
    -sS
    -X POST
    -o "$output_file"
    -w '%{http_code}'
  )

  if [[ -n "$ARCO_AUTH_TOKEN" ]]; then
    curl_args+=(-H "Authorization: Bearer ${ARCO_AUTH_TOKEN}")
  fi
  if [[ -n "$serverless_token" ]]; then
    curl_args+=(-H "X-Serverless-Authorization: Bearer ${serverless_token}")
  fi

  curl "${curl_args[@]}" "$url"
}

get_json() {
  local url="$1"
  local output_file="$2"
  local serverless_token="${3:-}"
  local curl_args=(
    -sS
    -o "$output_file"
    -w '%{http_code}'
    -H "X-Tenant-Id: ${TENANT_ID}"
    -H "X-Workspace-Id: ${WORKSPACE_ID}"
  )

  if [[ -n "$ARCO_AUTH_TOKEN" ]]; then
    curl_args+=(-H "Authorization: Bearer ${ARCO_AUTH_TOKEN}")
  fi
  if [[ -n "$serverless_token" ]]; then
    curl_args+=(-H "X-Serverless-Authorization: Bearer ${serverless_token}")
  fi

  curl "${curl_args[@]}" "$url"
}

assert_http() {
  local actual="$1"
  local expected="$2"
  local body_file="$3"
  local label="$4"

  if [[ "$actual" != "$expected" ]]; then
    echo >&2
    echo "=== ${label} response body ===" >&2
    cat "$body_file" >&2
    echo >&2
    die "${label} returned HTTP ${actual}, expected ${expected}"
  fi
}

canonical_asset_id() {
  echo "$1" | tr '.-' '__'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --env)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --tenant)
      TENANT_ID="$2"
      shift 2
      ;;
    --workspace)
      WORKSPACE_ID="$2"
      shift 2
      ;;
    --task-key)
      TASK_KEY="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --poll-interval)
      POLL_INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --api-url)
      API_URL="$2"
      shift 2
      ;;
    --dispatcher-url)
      DISPATCHER_URL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

require_cmd curl
require_cmd gcloud
require_cmd jq

[[ -n "$PROJECT_ID" ]] || die "PROJECT_ID is required"
[[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || die "--timeout must be an integer"
[[ "$POLL_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "--poll-interval must be an integer"
(( TIMEOUT_SECONDS > 0 )) || die "--timeout must be > 0"
(( POLL_INTERVAL_SECONDS > 0 )) || die "--poll-interval must be > 0"

if [[ -z "$API_URL" ]]; then
  API_URL="$(discover_service_url "arco-api-${ENVIRONMENT}")"
fi
if [[ -z "$DISPATCHER_URL" ]]; then
  DISPATCHER_URL="$(discover_service_url "arco-flow-dispatcher-${ENVIRONMENT}")"
fi

[[ -n "$API_URL" ]] || die "Could not resolve API URL"
[[ -n "$DISPATCHER_URL" ]] || die "Could not resolve dispatcher URL"

if [[ "$USE_GCLOUD_ID_TOKEN" == "true" ]]; then
  API_ID_TOKEN="${API_ID_TOKEN:-$(discover_id_token "$API_URL")}"
  DISPATCHER_ID_TOKEN="${DISPATCHER_ID_TOKEN:-$(discover_id_token "$DISPATCHER_URL")}"
fi

smoke_id="cloud-smoke-$(date -u '+%Y%m%dT%H%M%SZ')"
run_key="${smoke_id}-${RANDOM}"
asset_namespace="${TASK_KEY%.*}"
asset_name="${TASK_KEY##*.}"
asset_id="$(canonical_asset_id "$TASK_KEY")_smoke"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

manifest_payload="${tmpdir}/manifest.json"
run_payload="${tmpdir}/run.json"
manifest_response="${tmpdir}/manifest-response.json"
run_response="${tmpdir}/run-response.json"
dispatcher_response="${tmpdir}/dispatcher-response.json"
status_response="${tmpdir}/status-response.json"

cat >"$manifest_payload" <<EOF
{
  "manifestVersion": "1.0",
  "codeVersionId": "${smoke_id}",
  "assets": [
    {
      "key": {
        "namespace": "${asset_namespace}",
        "name": "${asset_name}"
      },
      "id": "${asset_id}",
      "dependencies": []
    }
  ],
  "schedules": []
}
EOF

cat >"$run_payload" <<EOF
{
  "selection": ["${TASK_KEY}"],
  "partitions": [],
  "labels": {
    "smoke": "true",
    "smokeId": "${smoke_id}"
  },
  "runKey": "${run_key}"
}
EOF

log "Project=${PROJECT_ID} region=${REGION} environment=${ENVIRONMENT}"
log "Tenant=${TENANT_ID} workspace=${WORKSPACE_ID} task=${TASK_KEY}"
log "API URL=${API_URL}"
log "Dispatcher URL=${DISPATCHER_URL}"
if [[ -n "$API_ID_TOKEN" || -n "$DISPATCHER_ID_TOKEN" ]]; then
  log "Cloud Run invoke auth: enabled via gcloud identity token"
else
  log "Cloud Run invoke auth: disabled"
fi

manifest_status="$(post_json "${API_URL}/api/v1/workspaces/${WORKSPACE_ID}/manifests" "$manifest_payload" "$manifest_response" "$API_ID_TOKEN")"
assert_http "$manifest_status" "201" "$manifest_response" "Manifest deploy"
log "Manifest deployed"

run_status="$(post_json "${API_URL}/api/v1/workspaces/${WORKSPACE_ID}/runs" "$run_payload" "$run_response" "$API_ID_TOKEN")"
assert_http "$run_status" "201" "$run_response" "Run trigger"

run_id="$(jq -r '.runId // .run_id // empty' "$run_response")"
[[ -n "$run_id" ]] || die "Run trigger response did not contain runId"
log "Triggered run ${run_id}"

max_polls=$(( (TIMEOUT_SECONDS + POLL_INTERVAL_SECONDS - 1) / POLL_INTERVAL_SECONDS ))
for ((attempt = 1; attempt <= max_polls; attempt++)); do
  dispatch_status="$(post_empty "${DISPATCHER_URL}/run" "$dispatcher_response" "$DISPATCHER_ID_TOKEN")"
  assert_http "$dispatch_status" "200" "$dispatcher_response" "Dispatcher tick"
  dispatch_summary="$(jq -c '{ready_dispatch_emitted,dispatch_actions,dispatch_enqueued,dispatch_failed,timer_actions,timer_enqueued,errors}' "$dispatcher_response" 2>/dev/null || cat "$dispatcher_response")"

  status_code="$(get_json "${API_URL}/api/v1/workspaces/${WORKSPACE_ID}/runs/${run_id}" "$status_response" "$API_ID_TOKEN")"
  assert_http "$status_code" "200" "$status_response" "Run status"

  run_state="$(jq -r '.state // empty' "$status_response")"
  task_state="$(jq -r --arg task_key "$TASK_KEY" '.tasks[]? | select(.taskKey == $task_key) | .state' "$status_response")"
  task_attempt="$(jq -r --arg task_key "$TASK_KEY" '.tasks[]? | select(.taskKey == $task_key) | .attempt' "$status_response")"

  if [[ -z "$task_state" ]]; then
    echo >&2
    echo "=== Run status response body ===" >&2
    cat "$status_response" >&2
    echo >&2
    die "Run ${run_id} did not contain task ${TASK_KEY}"
  fi

  log "Poll ${attempt}/${max_polls}: dispatcher=${dispatch_summary} run_state=${run_state} task_state=${task_state} attempt=${task_attempt}"

  if [[ "$run_state" == "SUCCEEDED" && "$task_state" == "SUCCEEDED" ]]; then
    materialization_id="$(jq -r --arg task_key "$TASK_KEY" '
      .tasks[]?
      | select(.taskKey == $task_key)
      | (
          .executionLineageRef
          | if type == "string" then (fromjson? // {}) elif type == "object" then . else {} end
          | .materializationId // empty
        )
    ' "$status_response")"
    log "Smoke passed for run ${run_id}"
    if [[ -n "$materialization_id" ]]; then
      log "Materialization id: ${materialization_id}"
    fi
    jq '.' "$status_response"
    exit 0
  fi

  if [[ "$run_state" == "FAILED" || "$run_state" == "CANCELLED" || "$task_state" == "FAILED" || "$task_state" == "CANCELLED" ]]; then
    echo >&2
    echo "=== Run status response body ===" >&2
    cat "$status_response" >&2
    echo >&2
    die "Smoke failed for run ${run_id}"
  fi

  sleep "$POLL_INTERVAL_SECONDS"
done

echo >&2
echo "=== Final dispatcher response body ===" >&2
cat "$dispatcher_response" >&2
echo >&2
echo >&2
echo "=== Final run status response body ===" >&2
cat "$status_response" >&2
echo >&2
die "Timed out waiting ${TIMEOUT_SECONDS}s for run ${run_id} to reach SUCCEEDED"
