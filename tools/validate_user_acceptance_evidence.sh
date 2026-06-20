#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tools/validate_user_acceptance_evidence.sh [--require-kind KIND] [DIR]

Validates local UAT evidence JSON artifacts without running live/cloud UAT.
Default DIR is target/uat-evidence.
EOF
}

require_kinds=()
evidence_dir="target/uat-evidence"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --require-kind)
      if [[ $# -lt 2 ]]; then
        echo "--require-kind requires a value" >&2
        exit 2
      fi
      require_kinds+=("$2")
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    -*)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      evidence_dir="$1"
      ;;
  esac
  shift
done

python_args=("$evidence_dir")
if ((${#require_kinds[@]})); then
  python_args+=("${require_kinds[@]}")
fi

python3 - "${python_args[@]}" <<'PY'
import json
import pathlib
import sys

secret_terms = ("token", "secret", "password", "authorization", "credential")
successful_states = {"COMPLETED", "SUCCESS", "SUCCEEDED"}
required_kinds = set(sys.argv[2:])
root = pathlib.Path(sys.argv[1])

if not root.exists() or not root.is_dir():
    raise SystemExit(f"ERROR: evidence directory does not exist: {root}")

files = sorted(root.glob("*.json"))
if not files:
    raise SystemExit(f"ERROR: no evidence JSON artifacts found in {root}")

seen_kinds = set()

def fail(path, message):
    raise SystemExit(f"ERROR: {path.name}: {message}")

def find_secret_key(value, prefix=""):
    if isinstance(value, dict):
        for key, child in value.items():
            key_path = f"{prefix}.{key}" if prefix else str(key)
            lowered = str(key).lower()
            if any(term in lowered for term in secret_terms):
                return key_path
            found = find_secret_key(child, key_path)
            if found:
                return found
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found = find_secret_key(child, f"{prefix}[{index}]")
            if found:
                return found
    return None

def require_mapping(path, value, key):
    child = value.get(key)
    if not isinstance(child, dict) or not child:
        fail(path, f"missing non-empty object field {key}")
    return child

def require_scalar_field(path, value, key):
    text = scalar_text(value.get(key))
    if text is None:
        fail(path, f"missing required field {key}")
    return text

def require_list(path, value, key):
    child = value.get(key)
    if not isinstance(child, list) or not child:
        fail(path, f"missing non-empty array field {key}")
    return child

def scalar_text(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        text = str(value).strip()
        return text or None
    return None

def row_text(row, *keys):
    if not isinstance(row, dict):
        return None
    for key in keys:
        text = scalar_text(row.get(key))
        if text is not None:
            return text
    return None

def normalized_state(value):
    text = scalar_text(value)
    if text is None:
        return None
    return text.replace("-", "_").replace(" ", "_").upper()

def require_object_rows(path, rows, label):
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            fail(path, f"{label}[{index}] must be an object")

def require_matching_rows(path, rows, label, expected, *keys):
    matched = []
    for index, row in enumerate(rows):
        value = row_text(row, *keys)
        if value == expected:
            matched.append((index, row))
    if not matched:
        fail(path, f"{label} must include runId {expected}")
    return matched

def require_rows_match_scope(path, rows, label, key, expected):
    for index, row in enumerate(rows):
        value = row_text(row, key)
        if value is not None and value != expected:
            fail(path, f"{label}[{index}].{key} must match {expected}")

def require_successful_state(path, row, label):
    state = normalized_state(
        row.get("state") or row.get("status") or row.get("final_state") or row.get("finalState")
    )
    if state not in successful_states:
        fail(path, f"{label} state must be successful")

def require_successful_dependencies(path, rows):
    for index, row in enumerate(rows):
        value = row.get("satisfied")
        if value is None:
            value = row.get("is_satisfied")
        if value is None:
            value = row.get("isSatisfied")
        if value is not True:
            fail(path, f"dependencies[{index}] must be satisfied")

def validate_durable(path, artifact):
    bucket = require_scalar_field(path, artifact, "bucket")
    if "://" in bucket or "redacted" not in bucket.lower():
        fail(path, "bucket must be a redacted bucket identifier, not a live URI")

    scenarios = require_mapping(path, artifact, "scenarios")
    required_fields = {
        "pipeline": ("tenantId", "workspaceId", "runId", "planId"),
        "schedule": ("tenantId", "workspaceId", "scheduleId", "tickId", "runId", "planId"),
        "backfill": (
            "tenantId",
            "workspaceId",
            "backfillId",
            "runId",
            "planId",
            "totalPartitions",
            "plannedChunks",
        ),
        "retry": ("tenantId", "workspaceId", "runId", "planId", "taskKey", "attempt"),
    }
    for name, fields in required_fields.items():
        scenario = require_mapping(path, scenarios, name)
        proof = require_mapping(path, scenario, "proof")
        for field in fields:
            require_scalar_field(path, proof, field)

def validate_deployed(path, artifact):
    for key in ("apiBaseUrl", "tenantId", "workspaceId", "runId"):
        if not artifact.get(key):
            fail(path, f"missing required field {key}")
    require_mapping(path, artifact, "identity")
    queries = require_mapping(path, artifact, "queries")
    catalog_tables = require_list(path, queries, "catalogTables")
    runs = require_list(path, queries, "runs")
    tasks = require_list(path, queries, "tasks")
    dependencies = require_list(path, queries, "dependencies")
    catalog_run_index = require_list(path, queries, "catalogRunIndex")
    partition_status = require_list(path, queries, "partitionStatus")

    for label, rows in (
        ("catalogTables", catalog_tables),
        ("runs", runs),
        ("tasks", tasks),
        ("dependencies", dependencies),
        ("catalogRunIndex", catalog_run_index),
        ("partitionStatus", partition_status),
    ):
        require_object_rows(path, rows, label)
        require_rows_match_scope(path, rows, label, "tenant_id", artifact["tenantId"])
        require_rows_match_scope(path, rows, label, "tenantId", artifact["tenantId"])
        require_rows_match_scope(path, rows, label, "org_id", artifact["tenantId"])
        require_rows_match_scope(path, rows, label, "orgId", artifact["tenantId"])
        require_rows_match_scope(path, rows, label, "workspace_id", artifact["workspaceId"])
        require_rows_match_scope(path, rows, label, "workspaceId", artifact["workspaceId"])

    run_rows = require_matching_rows(path, runs, "runs", artifact["runId"], "run_id", "runId")
    for index, row in run_rows:
        require_successful_state(path, row, f"runs[{index}]")

    task_rows = require_matching_rows(path, tasks, "tasks", artifact["runId"], "run_id", "runId")
    for index, row in task_rows:
        require_successful_state(path, row, f"tasks[{index}]")

    dependency_rows = require_matching_rows(
        path,
        dependencies,
        "dependencies",
        artifact["runId"],
        "run_id",
        "runId",
    )
    require_successful_dependencies(path, [row for _, row in dependency_rows])

    require_matching_rows(
        path,
        catalog_run_index,
        "catalogRunIndex",
        artifact["runId"],
        "run_id",
        "runId",
    )
    require_matching_rows(
        path,
        partition_status,
        "partitionStatus",
        artifact["runId"],
        "last_attempt_run_id",
        "lastAttemptRunId",
        "last_materialization_run_id",
        "lastMaterializationRunId",
    )

for path in files:
    try:
        artifact = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        fail(path, f"invalid JSON: {exc}")
    if not isinstance(artifact, dict):
        fail(path, "top-level artifact must be an object")

    secret_key = find_secret_key(artifact)
    if secret_key:
        fail(path, f"secret-like key is not allowed: {secret_key}")

    kind = artifact.get("kind")
    if kind not in {"durable_storage", "deployed_api_worker"}:
        fail(path, "kind must be durable_storage or deployed_api_worker")
    seen_kinds.add(kind)

    if kind == "durable_storage":
        validate_durable(path, artifact)
    else:
        validate_deployed(path, artifact)
    print(f"{path.name}: ok")

missing = sorted(required_kinds - seen_kinds)
if missing:
    raise SystemExit(f"ERROR: missing required evidence kind(s): {', '.join(missing)}")

print(f"validated {len(files)} UAT evidence artifact(s) from {root}")
PY
