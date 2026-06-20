#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/durable_storage_sample.json" <<'JSON'
{
  "kind": "durable_storage",
  "scenarios": {
    "pipeline": {"proof": {"runId": "run_1"}},
    "schedule": {"proof": {"tickId": "tick_1"}},
    "backfill": {"proof": {"backfillId": "bf_1"}},
    "retry": {"proof": {"taskKey": "task_1"}}
  }
}
JSON

cat >"$tmpdir/deployed_api_worker_sample.json" <<'JSON'
{
  "kind": "deployed_api_worker",
  "apiBaseUrl": "https://example.invalid",
  "tenantId": "tenant",
  "workspaceId": "workspace",
  "runId": "run_1",
  "identity": {"runKey": "uat:run", "codeVersionId": "code_v1"},
  "queries": {
    "catalogTables": [{"name": "daily_orders"}],
    "runs": [{"run_id": "run_1", "state": "SUCCEEDED"}],
    "tasks": [{"run_id": "run_1", "task_key": "analytics.daily_orders", "state": "SUCCEEDED"}],
    "dependencies": [{"run_id": "run_1", "satisfied": true}],
    "catalogRunIndex": [{"run_id": "run_1"}],
    "partitionStatus": [{"last_attempt_run_id": "run_1"}]
  }
}
JSON

tools/validate_user_acceptance_evidence.sh --require-kind durable_storage --require-kind deployed_api_worker "$tmpdir"

bad_run_dir="$tmpdir/bad-run"
mkdir "$bad_run_dir"
cat >"$bad_run_dir/deployed_api_worker_bad.json" <<'JSON'
{
  "kind": "deployed_api_worker",
  "apiBaseUrl": "https://example.invalid",
  "tenantId": "tenant",
  "workspaceId": "workspace",
  "runId": "run_1",
  "identity": {"runKey": "uat:run", "codeVersionId": "code_v1"},
  "queries": {
    "catalogTables": [{"name": "daily_orders"}],
    "runs": [{"run_id": "run_other", "state": "SUCCEEDED"}],
    "tasks": [{"run_id": "run_1", "task_key": "analytics.daily_orders", "state": "SUCCEEDED"}],
    "dependencies": [{"run_id": "run_1", "satisfied": true}],
    "catalogRunIndex": [{"run_id": "run_1"}],
    "partitionStatus": [{"last_attempt_run_id": "run_1"}]
  }
}
JSON
if tools/validate_user_acceptance_evidence.sh "$bad_run_dir" >/tmp/evidence-run.out 2>&1; then
  echo "expected deployed evidence with mismatched run rows to fail" >&2
  exit 1
fi
grep -q "runs must include runId run_1" /tmp/evidence-run.out

bad_terminal_dir="$tmpdir/bad-terminal"
mkdir "$bad_terminal_dir"
cat >"$bad_terminal_dir/deployed_api_worker_bad.json" <<'JSON'
{
  "kind": "deployed_api_worker",
  "apiBaseUrl": "https://example.invalid",
  "tenantId": "tenant",
  "workspaceId": "workspace",
  "runId": "run_1",
  "identity": {"runKey": "uat:run", "codeVersionId": "code_v1"},
  "queries": {
    "catalogTables": [{"name": "daily_orders"}],
    "runs": [{"run_id": "run_1", "state": "FAILED"}],
    "tasks": [{"run_id": "run_1", "task_key": "analytics.daily_orders", "state": "SUCCEEDED"}],
    "dependencies": [{"run_id": "run_1", "satisfied": true}],
    "catalogRunIndex": [{"run_id": "run_1"}],
    "partitionStatus": [{"last_attempt_run_id": "run_1"}]
  }
}
JSON
if tools/validate_user_acceptance_evidence.sh "$bad_terminal_dir" >/tmp/evidence-terminal.out 2>&1; then
  echo "expected deployed evidence with failed run state to fail" >&2
  exit 1
fi
grep -q "state must be successful" /tmp/evidence-terminal.out

bad_secret_dir="$tmpdir/bad-secret"
mkdir "$bad_secret_dir"
cat >"$bad_secret_dir/deployed_api_worker_bad.json" <<'JSON'
{
  "kind": "deployed_api_worker",
  "apiBaseUrl": "https://example.invalid",
  "tenantId": "tenant",
  "workspaceId": "workspace",
  "runId": "run_1",
  "authorization": "Bearer nope",
  "identity": {"runKey": "uat:run", "codeVersionId": "code_v1"},
  "queries": {
    "catalogTables": [{"name": "daily_orders"}],
    "runs": [{"run_id": "run_1", "state": "SUCCEEDED"}],
    "tasks": [{"run_id": "run_1", "task_key": "analytics.daily_orders", "state": "SUCCEEDED"}],
    "dependencies": [{"run_id": "run_1", "satisfied": true}],
    "catalogRunIndex": [{"run_id": "run_1"}],
    "partitionStatus": [{"last_attempt_run_id": "run_1"}]
  }
}
JSON
if tools/validate_user_acceptance_evidence.sh "$bad_secret_dir" >/tmp/evidence-secret.out 2>&1; then
  echo "expected secret-like key validation to fail" >&2
  exit 1
fi
grep -q "secret-like key" /tmp/evidence-secret.out

bad_json_dir="$tmpdir/bad-json"
mkdir "$bad_json_dir"
printf '{not json' >"$bad_json_dir/durable_storage_bad.json"
if tools/validate_user_acceptance_evidence.sh "$bad_json_dir" >/tmp/evidence-json.out 2>&1; then
  echo "expected invalid JSON validation to fail" >&2
  exit 1
fi
grep -q "invalid JSON" /tmp/evidence-json.out

empty_dir="$tmpdir/empty"
mkdir "$empty_dir"
if tools/validate_user_acceptance_evidence.sh "$empty_dir" >/tmp/evidence-empty.out 2>&1; then
  echo "expected empty evidence validation to fail" >&2
  exit 1
fi
grep -q "no evidence JSON artifacts" /tmp/evidence-empty.out

echo "user acceptance evidence validator smoke passed"
