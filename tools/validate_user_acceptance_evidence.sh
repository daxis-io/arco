#!/usr/bin/env bash
#
# Validates live UAT evidence artifacts emitted by the cataloged pipeline suite.
#
# Usage:
#   tools/validate_user_acceptance_evidence.sh [OPTIONS] [EVIDENCE_DIR]
#
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [OPTIONS] [EVIDENCE_DIR]

Validates durable_storage_*.json, deployed_api_worker_*.json, and
deployed_api_worker_failure_*.json artifacts.
EVIDENCE_DIR defaults to target/uat-evidence.

Options:
  --newer-than PATH       Only validate artifacts newer than PATH.
  --require-kind KIND     Require an artifact kind. Repeatable.
                          Supported: durable_storage, deployed_api_worker,
                          deployed_api_worker_failure.
  --expect-api-code-version VALUE
                          Require deployed API evidence to match this codeVersion.
                          Default: ARCO_UAT_EXPECTED_API_CODE_VERSION.
  --expect-api-git-sha VALUE
                          Require deployed API evidence to match this gitSha.
                          Default: ARCO_UAT_EXPECTED_API_GIT_SHA.
  --expect-api-image VALUE
                          Require deployed API evidence to match this image.
                          Default: ARCO_UAT_EXPECTED_API_IMAGE.
  -h, --help             Show this help message.

The validator checks JSON syntax, required proof fields, deployed API version
metadata, non-empty query results for deployed runs, succeeded query semantics,
failure evidence context, and rejects recursive secret-like keys.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required to validate UAT evidence artifacts." >&2
  exit 127
fi

EVIDENCE_DIR="target/uat-evidence"
NEWER_THAN=""
REQUIRED_KINDS=()
EXPECTED_API_CODE_VERSION="${ARCO_UAT_EXPECTED_API_CODE_VERSION:-}"
EXPECTED_API_GIT_SHA="${ARCO_UAT_EXPECTED_API_GIT_SHA:-}"
EXPECTED_API_IMAGE="${ARCO_UAT_EXPECTED_API_IMAGE:-}"

while [[ "$#" -gt 0 ]]; do
  case "$1" in
  --newer-than)
    [[ "$#" -ge 2 ]] || {
      usage >&2
      exit 2
    }
    NEWER_THAN="$2"
    shift 2
    ;;
  --require-kind)
    [[ "$#" -ge 2 ]] || {
      usage >&2
      exit 2
    }
    REQUIRED_KINDS+=("$2")
    shift 2
    ;;
  --expect-api-code-version)
    [[ "$#" -ge 2 ]] || {
      usage >&2
      exit 2
    }
    EXPECTED_API_CODE_VERSION="$2"
    shift 2
    ;;
  --expect-api-git-sha)
    [[ "$#" -ge 2 ]] || {
      usage >&2
      exit 2
    }
    EXPECTED_API_GIT_SHA="$2"
    shift 2
    ;;
  --expect-api-image)
    [[ "$#" -ge 2 ]] || {
      usage >&2
      exit 2
    }
    EXPECTED_API_IMAGE="$2"
    shift 2
    ;;
  -*)
    usage >&2
    exit 2
    ;;
  *)
    EVIDENCE_DIR="$1"
    shift
    if [[ "$#" -gt 0 ]]; then
      usage >&2
      exit 2
    fi
    ;;
  esac
done

PYTHON_ARGS=("${EVIDENCE_DIR}" "${NEWER_THAN}" "${EXPECTED_API_CODE_VERSION}" "${EXPECTED_API_GIT_SHA}" "${EXPECTED_API_IMAGE}")
if [[ "${#REQUIRED_KINDS[@]}" -gt 0 ]]; then
  PYTHON_ARGS+=("${REQUIRED_KINDS[@]}")
fi

python3 - "${PYTHON_ARGS[@]}" <<'PY'
import json
import re
import sys
from pathlib import Path

evidence_dir = Path(sys.argv[1])
newer_than_arg = sys.argv[2]
expected_api_provenance = {
    "codeVersion": sys.argv[3],
    "gitSha": sys.argv[4],
    "image": sys.argv[5],
}
required_kinds = set(sys.argv[6:])
secret_key = re.compile(r"(token|secret|password|authorization|credential)", re.IGNORECASE)
supported_kinds = {"durable_storage", "deployed_api_worker", "deployed_api_worker_failure"}


def fail(message):
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def require_object(value, path):
    if not isinstance(value, dict):
        fail(f"{path} must be an object")
    return value


def require_string(value, path):
    if not isinstance(value, str) or not value.strip():
        fail(f"{path} must be a non-empty string")
    return value


def require_provenance_string(value, path):
    text = require_string(value, path)
    if text.strip().lower() == "unknown":
        fail(f"{path} must not be unknown")
    return text


def require_array(value, path):
    if not isinstance(value, list) or not value:
        fail(f"{path} must be a non-empty array")
    return value


def require_empty_array(value, path):
    if not isinstance(value, list):
        fail(f"{path} must be an array")
    if value:
        fail(f"{path} must be empty")
    return value


def require_number(value, path):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail(f"{path} must be a number")
    return value


def require_field(obj, key, path):
    if key not in obj:
        fail(f"{path}.{key} is required")
    return obj[key]


def field_as_string(obj, key):
    value = obj.get(key)
    return value if isinstance(value, str) else None


def parse_lineage_ref(value, path):
    text = require_string(value, path)
    try:
        lineage = json.loads(text)
    except json.JSONDecodeError as error:
        fail(f"{path} must contain valid JSON lineage: {error}")
    return require_object(lineage, path)


def require_lineage_matches(lineage, path, *, run_id, task_key, materialization_id, delta_table, delta_version, delta_partition):
    expected_strings = {
        "runId": run_id,
        "taskKey": task_key,
        "materializationId": materialization_id,
        "deltaTable": delta_table,
        "deltaPartition": delta_partition,
    }
    for key, expected in expected_strings.items():
        if field_as_string(lineage, key) != expected:
            fail(f"{path}.{key} must match {expected}")
    if require_number(require_field(lineage, "deltaVersion", path), f"{path}.deltaVersion") != delta_version:
        fail(f"{path}.deltaVersion must match {delta_version}")


def reject_secret_like_keys(value, path):
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if secret_key.search(key):
                fail(f"{child_path} contains a secret-like key")
            reject_secret_like_keys(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_secret_like_keys(child, f"{path}[{index}]")


def validate_durable(path, artifact):
    require_string(require_field(artifact, "bucket", str(path)), f"{path}.bucket")
    if "<redacted>" not in artifact["bucket"]:
        fail(f"{path}.bucket must be redacted")

    scenarios = require_object(
        require_field(artifact, "scenarios", str(path)),
        f"{path}.scenarios",
    )
    for scenario in ("pipeline", "schedule", "backfill", "sensor", "retry"):
        scenario_obj = require_object(
            require_field(scenarios, scenario, f"{path}.scenarios"),
            f"{path}.scenarios.{scenario}",
        )
        for field in ("tenantId", "workspaceId", "otherWorkspaceId", "runId", "planId"):
            require_string(
                require_field(scenario_obj, field, f"{path}.scenarios.{scenario}"),
                f"{path}.scenarios.{scenario}.{field}",
            )
        assertion_rows = require_array(
            require_field(scenario_obj, "assertions", f"{path}.scenarios.{scenario}"),
            f"{path}.scenarios.{scenario}.assertions",
        )
        actual_assertions = {
            require_string(value, f"{path}.scenarios.{scenario}.assertions")
            for value in assertion_rows
        }
        expected_assertions = {
            "pipeline": {
                "catalog_metadata",
                "run_summary",
                "task_state",
                "dependency_satisfaction",
                "publication",
                "materialization_lineage",
                "partition_status",
                "workspace_isolation",
            },
            "schedule": {"schedule_definition", "schedule_tick"},
            "backfill": {"backfill_request", "backfill_chunks", "backfill_run_code_version"},
            "sensor": {"sensor_eval", "sensor_run_code_version"},
            "retry": {"retry_dispatch"},
        }[scenario]
        missing_assertions = sorted(expected_assertions - actual_assertions)
        if missing_assertions:
            fail(
                f"{path}.scenarios.{scenario}.assertions missing required assertion: "
                f"{missing_assertions[0]}"
            )
        validate_durable_scenario_proof(path, scenario, scenario_obj)


def validate_durable_scenario_proof(path, scenario, scenario_obj):
    proof = require_object(
        require_field(scenario_obj, "proof", f"{path}.scenarios.{scenario}"),
        f"{path}.scenarios.{scenario}.proof",
    )
    if scenario == "pipeline":
        for field in ("runId", "planId", "runKey"):
            require_string(
                require_field(proof, field, f"{path}.scenarios.{scenario}.proof"),
                f"{path}.scenarios.{scenario}.proof.{field}",
            )
    elif scenario == "schedule":
        for field in ("tickId", "runKey"):
            require_string(
                require_field(proof, field, f"{path}.scenarios.{scenario}.proof"),
                f"{path}.scenarios.{scenario}.proof.{field}",
            )
    elif scenario == "backfill":
        require_string(
            require_field(proof, "backfillId", f"{path}.scenarios.{scenario}.proof"),
            f"{path}.scenarios.{scenario}.proof.backfillId",
        )
        chunk_keys = require_array(
            require_field(proof, "chunkRunKeys", f"{path}.scenarios.{scenario}.proof"),
            f"{path}.scenarios.{scenario}.proof.chunkRunKeys",
        )
        for index, key in enumerate(chunk_keys):
            require_string(key, f"{path}.scenarios.{scenario}.proof.chunkRunKeys[{index}]")
    elif scenario == "sensor":
        for field in ("sensorId", "runKey"):
            require_string(
                require_field(proof, field, f"{path}.scenarios.{scenario}.proof"),
                f"{path}.scenarios.{scenario}.proof.{field}",
            )
    elif scenario == "retry":
        for field in ("taskKey", "attemptId", "dispatchId"):
            require_string(
                require_field(proof, field, f"{path}.scenarios.{scenario}.proof"),
                f"{path}.scenarios.{scenario}.proof.{field}",
            )
        require_number(
            require_field(proof, "attempt", f"{path}.scenarios.{scenario}.proof"),
            f"{path}.scenarios.{scenario}.proof.attempt",
        )


def validate_deployed(path, artifact):
    for field in ("apiBaseUrl", "tenantId", "workspaceId", "runId"):
        require_string(require_field(artifact, field, str(path)), f"{path}.{field}")
    validate_deployed_api_access(
        path,
        require_object(require_field(artifact, "apiAccess", str(path)), f"{path}.apiAccess"),
    )
    validate_deployed_api_version(
        path,
        require_object(require_field(artifact, "apiVersion", str(path)), f"{path}.apiVersion"),
    )

    identity = require_object(
        require_field(artifact, "identity", str(path)),
        f"{path}.identity",
    )
    for field in (
        "namespace",
        "rawAsset",
        "preparedAsset",
        "table",
        "rawAssetKey",
        "preparedAssetKey",
        "targetAssetKey",
        "runKey",
        "codeVersionId",
    ):
        require_string(require_field(identity, field, f"{path}.identity"), f"{path}.identity.{field}")

    queries = require_object(
        require_field(artifact, "queries", str(path)),
        f"{path}.queries",
    )
    catalog_rows = require_array(
        require_field(queries, "catalogTables", f"{path}.queries"),
        f"{path}.queries.catalogTables",
    )
    catalog_metadata_rows = require_array(
        require_field(queries, "catalogMetadata", f"{path}.queries"),
        f"{path}.queries.catalogMetadata",
    )
    run_rows = require_array(
        require_field(queries, "runs", f"{path}.queries"),
        f"{path}.queries.runs",
    )
    task_rows = require_array(
        require_field(queries, "tasks", f"{path}.queries"),
        f"{path}.queries.tasks",
    )
    dependency_rows = require_array(
        require_field(queries, "dependencies", f"{path}.queries"),
        f"{path}.queries.dependencies",
    )
    catalog_run_rows = require_array(
        require_field(queries, "catalogRunIndex", f"{path}.queries"),
        f"{path}.queries.catalogRunIndex",
    )
    partition_rows = require_array(
        require_field(queries, "partitionStatus", f"{path}.queries"),
        f"{path}.queries.partitionStatus",
    )
    isolation = require_object(
        require_field(queries, "isolation", f"{path}.queries"),
        f"{path}.queries.isolation",
    )

    run_id = artifact["runId"]
    run_key = identity["runKey"]
    code_version_id = identity["codeVersionId"]
    target_asset = identity["targetAssetKey"]
    expected_tasks = {
        identity["rawAssetKey"],
        identity["preparedAssetKey"],
        target_asset,
    }
    expected_dependencies = {
        (identity["rawAssetKey"], identity["preparedAssetKey"]),
        (identity["preparedAssetKey"], target_asset),
    }
    proof = require_object(
        require_field(artifact, "proof", str(path)),
        f"{path}.proof",
    )
    validate_deployed_proof_identity(
        path,
        proof,
        run_id=run_id,
        run_key=run_key,
        code_version_id=code_version_id,
        expected_tasks=expected_tasks,
        expected_dependencies=expected_dependencies,
    )

    if not any(
        field_as_string(row, "name") == identity["table"]
        or field_as_string(row, "table_name") == identity["table"]
        for row in catalog_rows
        if isinstance(row, dict)
    ):
        fail(f"{path}.queries.catalogTables must include the target table")
    validate_deployed_catalog_metadata(path, catalog_metadata_rows, identity)
    validate_deployed_isolation(path, isolation, workspace_id=artifact["workspaceId"])

    if not any(
        field_as_string(row, "run_id") == run_id
        and field_as_string(row, "run_key") == run_key
        and field_as_string(row, "state") == "SUCCEEDED"
        and field_as_string(row, "code_version") == code_version_id
        for row in run_rows
        if isinstance(row, dict)
    ):
        fail(f"{path}.queries.runs must include a SUCCEEDED row for runId, runKey, and codeVersionId")

    task_keys = {
        field_as_string(row, "task_key")
        for row in task_rows
        if isinstance(row, dict) and field_as_string(row, "state") == "SUCCEEDED"
    }
    if task_keys != expected_tasks:
        fail(f"{path}.queries.tasks must prove all three pipeline tasks succeeded")

    dependency_edges = {
        (field_as_string(row, "upstream_task_key"), field_as_string(row, "downstream_task_key"))
        for row in dependency_rows
        if isinstance(row, dict)
        and row.get("satisfied") is True
        and field_as_string(row, "resolution") == "SUCCESS"
    }
    if dependency_edges != expected_dependencies:
        fail(f"{path}.queries.dependencies must prove both dependency edges were satisfied")

    if not any(
        field_as_string(row, "run_id") == run_id
        and field_as_string(row, "task_key") == target_asset
        and field_as_string(row, "target_namespace") == identity["namespace"]
        and field_as_string(row, "target_table") == identity["table"]
        and field_as_string(row, "task_status") == "SUCCEEDED"
        for row in catalog_run_rows
        if isinstance(row, dict)
    ):
        fail(f"{path}.queries.catalogRunIndex must prove target publication succeeded")
    publication_row = next(
        row
        for row in catalog_run_rows
        if isinstance(row, dict)
        and field_as_string(row, "run_id") == run_id
        and field_as_string(row, "task_key") == target_asset
        and field_as_string(row, "target_namespace") == identity["namespace"]
        and field_as_string(row, "target_table") == identity["table"]
        and field_as_string(row, "task_status") == "SUCCEEDED"
    )
    materialization_id = require_string(
        require_field(publication_row, "materialization_id", f"{path}.queries.catalogRunIndex"),
        f"{path}.queries.catalogRunIndex.materialization_id",
    )
    output_visibility = require_string(
        require_field(publication_row, "output_visibility_state", f"{path}.queries.catalogRunIndex"),
        f"{path}.queries.catalogRunIndex.output_visibility_state",
    )
    if output_visibility != "VISIBLE":
        fail(f"{path}.queries.catalogRunIndex.output_visibility_state must be VISIBLE")
    delta_table = require_string(
        require_field(publication_row, "delta_table", f"{path}.queries.catalogRunIndex"),
        f"{path}.queries.catalogRunIndex.delta_table",
    )
    delta_version = require_number(
        require_field(publication_row, "delta_version", f"{path}.queries.catalogRunIndex"),
        f"{path}.queries.catalogRunIndex.delta_version",
    )
    delta_partition = require_string(
        require_field(publication_row, "delta_partition", f"{path}.queries.catalogRunIndex"),
        f"{path}.queries.catalogRunIndex.delta_partition",
    )
    if delta_table != target_asset:
        fail(f"{path}.queries.catalogRunIndex.delta_table must match the target asset")
    publication_lineage = parse_lineage_ref(
        require_field(publication_row, "execution_lineage_ref", f"{path}.queries.catalogRunIndex"),
        f"{path}.queries.catalogRunIndex.execution_lineage_ref",
    )
    require_lineage_matches(
        publication_lineage,
        f"{path}.queries.catalogRunIndex.execution_lineage_ref",
        run_id=run_id,
        task_key=target_asset,
        materialization_id=materialization_id,
        delta_table=delta_table,
        delta_version=delta_version,
        delta_partition=delta_partition,
    )

    if not any(
        field_as_string(row, "asset_key") == target_asset
        and require_string(row.get("partition_key"), f"{path}.queries.partitionStatus.partition_key")
        and field_as_string(row, "last_materialization_run_id") == run_id
        and field_as_string(row, "last_attempt_outcome") == "SUCCEEDED"
        for row in partition_rows
        if isinstance(row, dict)
    ):
        fail(f"{path}.queries.partitionStatus must prove target partition succeeded")
    partition_row = next(
        row
        for row in partition_rows
        if isinstance(row, dict)
        and field_as_string(row, "asset_key") == target_asset
        and field_as_string(row, "last_materialization_run_id") == run_id
        and field_as_string(row, "last_attempt_outcome") == "SUCCEEDED"
    )
    partition_delta_table = require_string(
        require_field(partition_row, "delta_table", f"{path}.queries.partitionStatus"),
        f"{path}.queries.partitionStatus.delta_table",
    )
    partition_delta_version = require_number(
        require_field(partition_row, "delta_version", f"{path}.queries.partitionStatus"),
        f"{path}.queries.partitionStatus.delta_version",
    )
    partition_delta_partition = require_string(
        require_field(partition_row, "delta_partition", f"{path}.queries.partitionStatus"),
        f"{path}.queries.partitionStatus.delta_partition",
    )
    if (
        partition_delta_table != delta_table
        or partition_delta_version != delta_version
        or partition_delta_partition != delta_partition
    ):
        fail(f"{path}.queries.partitionStatus delta fields must match catalogRunIndex")
    partition_lineage = parse_lineage_ref(
        require_field(partition_row, "execution_lineage_ref", f"{path}.queries.partitionStatus"),
        f"{path}.queries.partitionStatus.execution_lineage_ref",
    )
    require_lineage_matches(
        partition_lineage,
        f"{path}.queries.partitionStatus.execution_lineage_ref",
        run_id=run_id,
        task_key=target_asset,
        materialization_id=materialization_id,
        delta_table=delta_table,
        delta_version=delta_version,
        delta_partition=delta_partition,
    )
    validate_deployed_proof_materialization(
        path,
        proof,
        target_asset=target_asset,
        materialization_id=materialization_id,
        delta_table=delta_table,
        delta_version=delta_version,
        delta_partition=delta_partition,
        partition_key=partition_row["partition_key"],
    )


def validate_deployed_failure(path, artifact):
    for field in ("apiBaseUrl", "tenantId", "workspaceId", "runId"):
        require_string(require_field(artifact, field, str(path)), f"{path}.{field}")
    validate_deployed_api_access(
        path,
        require_object(require_field(artifact, "apiAccess", str(path)), f"{path}.apiAccess"),
    )
    validate_deployed_api_version(
        path,
        require_object(require_field(artifact, "apiVersion", str(path)), f"{path}.apiVersion"),
    )

    identity = require_object(
        require_field(artifact, "identity", str(path)),
        f"{path}.identity",
    )
    for field in (
        "namespace",
        "rawAsset",
        "preparedAsset",
        "table",
        "rawAssetKey",
        "preparedAssetKey",
        "targetAssetKey",
        "runKey",
        "codeVersionId",
    ):
        require_string(require_field(identity, field, f"{path}.identity"), f"{path}.identity.{field}")

    failure = require_object(
        require_field(artifact, "failure", str(path)),
        f"{path}.failure",
    )
    require_string(require_field(failure, "stage", f"{path}.failure"), f"{path}.failure.stage")
    require_string(require_field(failure, "error", f"{path}.failure"), f"{path}.failure.error")

    queries = require_object(
        require_field(artifact, "queries", str(path)),
        f"{path}.queries",
    )
    run_rows = require_array(
        require_field(queries, "runs", f"{path}.queries"),
        f"{path}.queries.runs",
    )
    task_rows = require_array(
        require_field(queries, "tasks", f"{path}.queries"),
        f"{path}.queries.tasks",
    )

    run_id = artifact["runId"]
    run_key = identity["runKey"]
    code_version_id = identity["codeVersionId"]
    if not any(
        field_as_string(row, "run_id") == run_id
        and field_as_string(row, "run_key") == run_key
        and field_as_string(row, "code_version") == code_version_id
        for row in run_rows
        if isinstance(row, dict)
    ):
        fail(f"{path}.queries.runs must include a row for runId, runKey, and codeVersionId")

    expected_tasks = {
        identity["rawAssetKey"],
        identity["preparedAssetKey"],
        identity["targetAssetKey"],
    }
    if not any(
        field_as_string(row, "task_key") in expected_tasks
        and isinstance(field_as_string(row, "state"), str)
        for row in task_rows
        if isinstance(row, dict)
    ):
        fail(f"{path}.queries.tasks must include at least one deployed pipeline task row")

    if "dependencies" in queries:
        dependency_rows = queries["dependencies"]
        if not isinstance(dependency_rows, list):
            fail(f"{path}.queries.dependencies must be an array")
        for index, row in enumerate(dependency_rows):
            row = require_object(row, f"{path}.queries.dependencies[{index}]")
            require_string(
                require_field(row, "upstream_task_key", f"{path}.queries.dependencies[{index}]"),
                f"{path}.queries.dependencies[{index}].upstream_task_key",
            )
            require_string(
                require_field(row, "downstream_task_key", f"{path}.queries.dependencies[{index}]"),
                f"{path}.queries.dependencies[{index}].downstream_task_key",
            )


def validate_deployed_api_version(path, api_version):
    for field in (
        "service",
        "packageVersion",
    ):
        require_string(require_field(api_version, field, f"{path}.apiVersion"), f"{path}.apiVersion.{field}")
    for field in (
        "codeVersion",
        "gitSha",
        "image",
        "cloudRunRevision",
    ):
        actual = require_provenance_string(
            require_field(api_version, field, f"{path}.apiVersion"),
            f"{path}.apiVersion.{field}",
        )
        expected = expected_api_provenance.get(field, "")
        if expected and actual != expected:
            fail(f"{path}.apiVersion.{field} must match expected deployed API {field}: {expected}")


def validate_deployed_api_access(path, api_access):
    mode = require_string(
        require_field(api_access, "mode", f"{path}.apiAccess"),
        f"{path}.apiAccess.mode",
    )
    require_string(
        require_field(api_access, "ingressMode", f"{path}.apiAccess"),
        f"{path}.apiAccess.ingressMode",
    )

    if mode == "cloud-run-proxy":
        for field in ("cloudRunService", "cloudRunProjectId", "cloudRunRegion"):
            require_string(
                require_field(api_access, field, f"{path}.apiAccess"),
                f"{path}.apiAccess.{field}",
            )
    elif mode != "direct-url":
        fail(f"{path}.apiAccess.mode must be direct-url or cloud-run-proxy")


def validate_deployed_proof_identity(
    path,
    proof,
    *,
    run_id,
    run_key,
    code_version_id,
    expected_tasks,
    expected_dependencies,
):
    if require_string(require_field(proof, "runId", f"{path}.proof"), f"{path}.proof.runId") != run_id:
        fail(f"{path}.proof.runId must match {run_id}")
    if require_string(require_field(proof, "runKey", f"{path}.proof"), f"{path}.proof.runKey") != run_key:
        fail(f"{path}.proof.runKey must match {run_key}")
    if (
        require_string(require_field(proof, "codeVersionId", f"{path}.proof"), f"{path}.proof.codeVersionId")
        != code_version_id
    ):
        fail(f"{path}.proof.codeVersionId must match {code_version_id}")

    task_keys = {
        require_string(value, f"{path}.proof.taskKeys[{index}]")
        for index, value in enumerate(require_array(require_field(proof, "taskKeys", f"{path}.proof"), f"{path}.proof.taskKeys"))
    }
    if task_keys != expected_tasks:
        fail(f"{path}.proof.taskKeys must list all deployed pipeline tasks")

    dependency_edges = set()
    for index, edge in enumerate(
        require_array(
            require_field(proof, "dependencyEdges", f"{path}.proof"),
            f"{path}.proof.dependencyEdges",
        )
    ):
        edge = require_object(edge, f"{path}.proof.dependencyEdges[{index}]")
        dependency_edges.add(
            (
                require_string(
                    require_field(edge, "upstreamTaskKey", f"{path}.proof.dependencyEdges[{index}]"),
                    f"{path}.proof.dependencyEdges[{index}].upstreamTaskKey",
                ),
                require_string(
                    require_field(edge, "downstreamTaskKey", f"{path}.proof.dependencyEdges[{index}]"),
                    f"{path}.proof.dependencyEdges[{index}].downstreamTaskKey",
                ),
            )
        )
    if dependency_edges != expected_dependencies:
        fail(f"{path}.proof.dependencyEdges must list both deployed pipeline edges")


def validate_deployed_proof_materialization(
    path,
    proof,
    *,
    target_asset,
    materialization_id,
    delta_table,
    delta_version,
    delta_partition,
    partition_key,
):
    publication = require_object(
        require_field(proof, "publication", f"{path}.proof"),
        f"{path}.proof.publication",
    )
    expected_publication_strings = {
        "taskKey": target_asset,
        "materializationId": materialization_id,
        "deltaTable": delta_table,
        "deltaPartition": delta_partition,
    }
    for key, expected in expected_publication_strings.items():
        if require_string(require_field(publication, key, f"{path}.proof.publication"), f"{path}.proof.publication.{key}") != expected:
            fail(f"{path}.proof.publication.{key} must match {expected}")
    if require_number(require_field(publication, "deltaVersion", f"{path}.proof.publication"), f"{path}.proof.publication.deltaVersion") != delta_version:
        fail(f"{path}.proof.publication.deltaVersion must match {delta_version}")

    partition = require_object(
        require_field(proof, "partition", f"{path}.proof"),
        f"{path}.proof.partition",
    )
    if require_string(require_field(partition, "assetKey", f"{path}.proof.partition"), f"{path}.proof.partition.assetKey") != target_asset:
        fail(f"{path}.proof.partition.assetKey must match {target_asset}")
    if require_string(require_field(partition, "partitionKey", f"{path}.proof.partition"), f"{path}.proof.partition.partitionKey") != partition_key:
        fail(f"{path}.proof.partition.partitionKey must match {partition_key}")


def validate_deployed_isolation(path, isolation, *, workspace_id):
    other_workspace_id = require_string(
        require_field(isolation, "workspaceId", f"{path}.queries.isolation"),
        f"{path}.queries.isolation.workspaceId",
    )
    if other_workspace_id == workspace_id:
        fail(f"{path}.queries.isolation.workspaceId must differ from workspaceId")
    for field in (
        "catalogTables",
        "runs",
        "tasks",
        "dependencies",
        "catalogRunIndex",
        "partitionStatus",
    ):
        require_empty_array(
            require_field(isolation, field, f"{path}.queries.isolation"),
            f"{path}.queries.isolation.{field}",
        )


def validate_deployed_catalog_metadata(path, rows, identity):
    expected_tables = {
        identity["rawAsset"],
        identity["preparedAsset"],
        identity["table"],
    }
    observed = {
        (
            field_as_string(row, "namespace_name"),
            field_as_string(row, "table_name"),
            field_as_string(row, "column_name"),
            field_as_string(row, "data_type"),
            row.get("is_nullable") if isinstance(row, dict) else None,
            row.get("ordinal") if isinstance(row, dict) else None,
        )
        for row in rows
        if isinstance(row, dict)
    }
    expected = set()
    for table in expected_tables:
        expected.add((identity["namespace"], table, "order_id", "STRING", False, 0))
        expected.add((identity["namespace"], table, "order_total", "DOUBLE", True, 1))
    missing = sorted(expected - observed)
    if missing:
        namespace, table, column, data_type, is_nullable, ordinal = missing[0]
        fail(
            f"{path}.queries.catalogMetadata missing "
            f"{namespace}.{table}.{column}/{data_type}/{is_nullable}/{ordinal}"
        )


if not evidence_dir.is_dir():
    fail(f"evidence directory does not exist: {evidence_dir}")

unsupported_required = sorted(required_kinds - supported_kinds)
if unsupported_required:
    fail(f"unsupported required UAT evidence kind: {unsupported_required[0]}")

paths = sorted(
    set(evidence_dir.glob("durable_storage_*.json"))
    | set(evidence_dir.glob("deployed_api_worker_*.json"))
)
if newer_than_arg:
    marker = Path(newer_than_arg)
    if not marker.exists():
        fail(f"freshness marker does not exist: {marker}")
    marker_mtime_ns = marker.stat().st_mtime_ns
    paths = [path for path in paths if path.stat().st_mtime_ns > marker_mtime_ns]
if not paths:
    if newer_than_arg:
        fail(f"no UAT evidence artifacts found in {evidence_dir} newer than {newer_than_arg}")
    fail(f"no UAT evidence artifacts found in {evidence_dir}")

seen_kinds = set()
for path in paths:
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        fail(f"{path}: invalid JSON: {error}")

    require_object(artifact, str(path))
    reject_secret_like_keys(artifact, str(path))

    kind = require_string(require_field(artifact, "kind", str(path)), f"{path}.kind")
    if kind == "durable_storage":
        validate_durable(path, artifact)
    elif kind == "deployed_api_worker":
        validate_deployed(path, artifact)
    elif kind == "deployed_api_worker_failure":
        validate_deployed_failure(path, artifact)
    else:
        fail(f"{path}.kind has unsupported value: {kind}")
    seen_kinds.add(kind)

    print(f"{path.name}: ok")

for kind in sorted(required_kinds - seen_kinds):
    fail(f"missing required UAT evidence kind: {kind}")

print(f"validated {len(paths)} UAT evidence artifact(s) from {evidence_dir}")
PY
