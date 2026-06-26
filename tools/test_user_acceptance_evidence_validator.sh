#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

VALIDATOR="tools/validate_user_acceptance_evidence.sh"

if [[ ! -f "${VALIDATOR}" ]]; then
  echo "missing ${VALIDATOR}" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

valid_dir="${tmp_dir}/valid"
mkdir -p "${valid_dir}"

cat >"${valid_dir}/durable_storage_pipeline.json" <<'JSON'
{
  "kind": "durable_storage",
  "recordedAt": "2026-06-01T00:00:00Z",
  "bucket": "gs://<redacted>",
  "scenarios": {
    "pipeline": {
      "tenantId": "tenant-pipeline",
      "workspaceId": "workspace-pipeline",
      "otherWorkspaceId": "workspace-other-pipeline",
      "runId": "uat_pipeline_01",
      "planId": "uat_plan_pipeline_01",
      "proof": {
        "runId": "run_pipeline_01",
        "planId": "plan_pipeline_01",
        "runKey": "uat-cataloged-pipeline-run"
      },
      "assertions": [
        "catalog_metadata",
        "run_summary",
        "task_state",
        "dependency_satisfaction",
        "publication",
        "materialization_lineage",
        "partition_status",
        "workspace_isolation"
      ]
    },
    "schedule": {
      "tenantId": "tenant-schedule",
      "workspaceId": "workspace-schedule",
      "otherWorkspaceId": "workspace-other-schedule",
      "runId": "uat_schedule_01",
      "planId": "uat_plan_schedule_01",
      "proof": {
        "tickId": "daily-orders:1740787200",
        "runKey": "sched:daily-orders:1740787200"
      },
      "assertions": [
        "schedule_definition",
        "schedule_tick"
      ]
    },
    "backfill": {
      "tenantId": "tenant-backfill",
      "workspaceId": "workspace-backfill",
      "otherWorkspaceId": "workspace-other-backfill",
      "runId": "uat_backfill_01",
      "planId": "uat_plan_backfill_01",
      "proof": {
        "backfillId": "backfill-01",
        "chunkRunKeys": [
          "backfill:backfill-01:chunk:0",
          "backfill:backfill-01:chunk:1"
        ]
      },
      "assertions": [
        "backfill_request",
        "backfill_chunks",
        "backfill_run_code_version"
      ]
    },
    "sensor": {
      "tenantId": "tenant-sensor",
      "workspaceId": "workspace-sensor",
      "otherWorkspaceId": "workspace-other-sensor",
      "runId": "uat_sensor_01",
      "planId": "uat_plan_sensor_01",
      "proof": {
        "sensorId": "daily-orders-sensor",
        "runKey": "sensor:daily-orders-sensor:msg:uat-sensor-message-01"
      },
      "assertions": [
        "sensor_eval",
        "sensor_run_code_version"
      ]
    },
    "retry": {
      "tenantId": "tenant-retry",
      "workspaceId": "workspace-retry",
      "otherWorkspaceId": "workspace-other-retry",
      "runId": "uat_retry_01",
      "planId": "uat_plan_retry_01",
      "proof": {
        "taskKey": "load_daily_orders",
        "attempt": 2,
        "attemptId": "attempt-02",
        "dispatchId": "dispatch-02"
      },
      "assertions": [
        "retry_dispatch"
      ]
    }
  }
}
JSON

cat >"${valid_dir}/deployed_api_worker_pipeline.json" <<'JSON'
{
  "kind": "deployed_api_worker",
  "recordedAt": "2026-06-01T00:00:00Z",
  "apiBaseUrl": "https://arco.acceptance.example",
  "apiAccess": {
    "mode": "direct-url",
    "ingressMode": "not-recorded"
  },
  "apiVersion": {
    "service": "arco-api",
    "packageVersion": "0.2.0",
    "codeVersion": "uat-api-code-version",
    "gitSha": "abc123",
    "image": "us-central1-docker.pkg.dev/example/arco-api:abc123",
    "cloudRunRevision": "arco-api-dev-00042-test"
  },
  "tenantId": "tenant-worker",
  "workspaceId": "workspace-worker",
  "identity": {
    "namespace": "uat_ns",
    "rawAsset": "raw_orders",
    "preparedAsset": "daily_orders_prepared",
    "table": "daily_orders",
    "rawAssetKey": "uat_ns.raw_orders",
    "preparedAssetKey": "uat_ns.daily_orders_prepared",
    "targetAssetKey": "uat_ns.daily_orders",
    "runKey": "uat_run_key",
    "codeVersionId": "uat-pipeline-v1"
  },
  "runId": "run-01",
  "proof": {
    "runId": "run-01",
    "runKey": "uat_run_key",
    "codeVersionId": "uat-pipeline-v1",
    "taskKeys": [
      "uat_ns.raw_orders",
      "uat_ns.daily_orders_prepared",
      "uat_ns.daily_orders"
    ],
    "dependencyEdges": [
      {
        "upstreamTaskKey": "uat_ns.raw_orders",
        "downstreamTaskKey": "uat_ns.daily_orders_prepared"
      },
      {
        "upstreamTaskKey": "uat_ns.daily_orders_prepared",
        "downstreamTaskKey": "uat_ns.daily_orders"
      }
    ],
    "publication": {
      "taskKey": "uat_ns.daily_orders",
      "materializationId": "mat-01",
      "deltaTable": "uat_ns.daily_orders",
      "deltaVersion": 7,
      "deltaPartition": "date=2026-06-01"
    },
    "partition": {
      "assetKey": "uat_ns.daily_orders",
      "partitionKey": "date=d:2026-06-01"
    }
  },
  "queries": {
    "catalogTables": [{"table_name": "daily_orders"}],
    "catalogMetadata": [
      {
        "namespace_name": "uat_ns",
        "table_name": "daily_orders",
        "column_name": "order_id",
        "data_type": "STRING",
        "is_nullable": false,
        "ordinal": 0
      },
      {
        "namespace_name": "uat_ns",
        "table_name": "daily_orders",
        "column_name": "order_total",
        "data_type": "DOUBLE",
        "is_nullable": true,
        "ordinal": 1
      },
      {
        "namespace_name": "uat_ns",
        "table_name": "daily_orders_prepared",
        "column_name": "order_id",
        "data_type": "STRING",
        "is_nullable": false,
        "ordinal": 0
      },
      {
        "namespace_name": "uat_ns",
        "table_name": "daily_orders_prepared",
        "column_name": "order_total",
        "data_type": "DOUBLE",
        "is_nullable": true,
        "ordinal": 1
      },
      {
        "namespace_name": "uat_ns",
        "table_name": "raw_orders",
        "column_name": "order_id",
        "data_type": "STRING",
        "is_nullable": false,
        "ordinal": 0
      },
      {
        "namespace_name": "uat_ns",
        "table_name": "raw_orders",
        "column_name": "order_total",
        "data_type": "DOUBLE",
        "is_nullable": true,
        "ordinal": 1
      }
    ],
    "runs": [{
      "run_id": "run-01",
      "run_key": "uat_run_key",
      "state": "SUCCEEDED",
      "code_version": "uat-pipeline-v1"
    }],
    "tasks": [
      {"task_key": "uat_ns.daily_orders", "state": "SUCCEEDED"},
      {"task_key": "uat_ns.daily_orders_prepared", "state": "SUCCEEDED"},
      {"task_key": "uat_ns.raw_orders", "state": "SUCCEEDED"}
    ],
    "dependencies": [
      {
        "upstream_task_key": "uat_ns.daily_orders_prepared",
        "downstream_task_key": "uat_ns.daily_orders",
        "satisfied": true,
        "resolution": "SUCCESS",
        "satisfying_attempt": 1
      },
      {
        "upstream_task_key": "uat_ns.raw_orders",
        "downstream_task_key": "uat_ns.daily_orders_prepared",
        "satisfied": true,
        "resolution": "SUCCESS",
        "satisfying_attempt": 1
      }
    ],
    "catalogRunIndex": [
      {
        "run_id": "run-01",
        "task_key": "uat_ns.daily_orders",
        "target_namespace": "uat_ns",
        "target_table": "daily_orders",
        "task_status": "SUCCEEDED",
        "materialization_id": "mat-01",
        "output_visibility_state": "VISIBLE",
        "delta_table": "uat_ns.daily_orders",
        "delta_version": 7,
        "delta_partition": "date=2026-06-01",
        "execution_lineage_ref": "{\"runId\":\"run-01\",\"taskKey\":\"uat_ns.daily_orders\",\"materializationId\":\"mat-01\",\"deltaTable\":\"uat_ns.daily_orders\",\"deltaVersion\":7,\"deltaPartition\":\"date=2026-06-01\"}"
      }
    ],
    "partitionStatus": [
      {
        "asset_key": "uat_ns.daily_orders",
        "partition_key": "date=d:2026-06-01",
        "last_materialization_run_id": "run-01",
        "last_attempt_outcome": "SUCCEEDED",
        "delta_table": "uat_ns.daily_orders",
        "delta_version": 7,
        "delta_partition": "date=2026-06-01",
        "execution_lineage_ref": "{\"runId\":\"run-01\",\"taskKey\":\"uat_ns.daily_orders\",\"materializationId\":\"mat-01\",\"deltaTable\":\"uat_ns.daily_orders\",\"deltaVersion\":7,\"deltaPartition\":\"date=2026-06-01\"}"
      }
    ],
    "isolation": {
      "workspaceId": "workspace-worker-other",
      "catalogTables": [],
      "runs": [],
      "tasks": [],
      "dependencies": [],
      "catalogRunIndex": [],
      "partitionStatus": []
    }
  }
}
JSON

valid_output="$(bash "${VALIDATOR}" "${valid_dir}")"
grep -Fq "durable_storage_pipeline.json: ok" <<<"${valid_output}"
grep -Fq "deployed_api_worker_pipeline.json: ok" <<<"${valid_output}"
grep -Fq "validated 2 UAT evidence artifact(s)" <<<"${valid_output}"

required_kind_output="$(bash "${VALIDATOR}" --require-kind durable_storage --require-kind deployed_api_worker "${valid_dir}")"
grep -Fq "validated 2 UAT evidence artifact(s)" <<<"${required_kind_output}"

missing_kind_dir="${tmp_dir}/missing-kind"
mkdir -p "${missing_kind_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_kind_dir}/deployed_api_worker_pipeline.json"
if bash "${VALIDATOR}" --require-kind durable_storage "${missing_kind_dir}" >/tmp/arco-uat-evidence-missing-kind.out 2>/tmp/arco-uat-evidence-missing-kind.err; then
  echo "validator should fail when a required evidence kind is missing" >&2
  exit 1
fi
grep -Fq "missing required UAT evidence kind: durable_storage" /tmp/arco-uat-evidence-missing-kind.err

stale_marker="${tmp_dir}/stale-marker"
touch "${stale_marker}"
if bash "${VALIDATOR}" --newer-than "${stale_marker}" "${valid_dir}" >/tmp/arco-uat-evidence-stale.out 2>/tmp/arco-uat-evidence-stale.err; then
  echo "validator should fail when all artifacts are older than the freshness marker" >&2
  exit 1
fi
grep -Fq "newer than" /tmp/arco-uat-evidence-stale.err

missing_assertion_dir="${tmp_dir}/missing-assertion"
mkdir -p "${missing_assertion_dir}"
cp "${valid_dir}/durable_storage_pipeline.json" "${missing_assertion_dir}/durable_storage_missing_assertion.json"
python3 - "${missing_assertion_dir}/durable_storage_missing_assertion.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["scenarios"]["pipeline"]["assertions"].remove("workspace_isolation")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_assertion_dir}" >/tmp/arco-uat-evidence-missing-assertion.out 2>/tmp/arco-uat-evidence-missing-assertion.err; then
  echo "validator should fail when durable evidence omits a required assertion" >&2
  exit 1
fi
grep -Fq "workspace_isolation" /tmp/arco-uat-evidence-missing-assertion.err

missing_proof_dir="${tmp_dir}/missing-proof"
mkdir -p "${missing_proof_dir}"
cp "${valid_dir}/durable_storage_pipeline.json" "${missing_proof_dir}/durable_storage_missing_proof.json"
python3 - "${missing_proof_dir}/durable_storage_missing_proof.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["scenarios"]["pipeline"].pop("proof")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_proof_dir}" >/tmp/arco-uat-evidence-missing-proof.out 2>/tmp/arco-uat-evidence-missing-proof.err; then
  echo "validator should fail when durable evidence omits concrete scenario proof" >&2
  exit 1
fi
grep -Fq "scenarios.pipeline.proof" /tmp/arco-uat-evidence-missing-proof.err

missing_deployed_proof_dir="${tmp_dir}/missing-deployed-proof"
mkdir -p "${missing_deployed_proof_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_deployed_proof_dir}/deployed_api_worker_missing_proof.json"
python3 - "${missing_deployed_proof_dir}/deployed_api_worker_missing_proof.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact.pop("proof")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_deployed_proof_dir}" >/tmp/arco-uat-evidence-missing-deployed-proof.out 2>/tmp/arco-uat-evidence-missing-deployed-proof.err; then
  echo "validator should fail when deployed evidence omits concrete proof" >&2
  exit 1
fi
grep -Fq "deployed_api_worker_missing_proof.json.proof" /tmp/arco-uat-evidence-missing-deployed-proof.err

missing_catalog_metadata_dir="${tmp_dir}/missing-catalog-metadata"
mkdir -p "${missing_catalog_metadata_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_catalog_metadata_dir}/deployed_api_worker_missing_catalog_metadata.json"
python3 - "${missing_catalog_metadata_dir}/deployed_api_worker_missing_catalog_metadata.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"].pop("catalogMetadata")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_catalog_metadata_dir}" >/tmp/arco-uat-evidence-missing-catalog-metadata.out 2>/tmp/arco-uat-evidence-missing-catalog-metadata.err; then
  echo "validator should fail when deployed evidence omits catalog metadata query proof" >&2
  exit 1
fi
grep -Fq "queries.catalogMetadata" /tmp/arco-uat-evidence-missing-catalog-metadata.err

missing_deployed_isolation_dir="${tmp_dir}/missing-deployed-isolation"
mkdir -p "${missing_deployed_isolation_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_deployed_isolation_dir}/deployed_api_worker_missing_isolation.json"
python3 - "${missing_deployed_isolation_dir}/deployed_api_worker_missing_isolation.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"].pop("isolation")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_deployed_isolation_dir}" >/tmp/arco-uat-evidence-missing-deployed-isolation.out 2>/tmp/arco-uat-evidence-missing-deployed-isolation.err; then
  echo "validator should fail when deployed evidence omits workspace isolation proof" >&2
  exit 1
fi
grep -Fq "queries.isolation" /tmp/arco-uat-evidence-missing-deployed-isolation.err

leaky_deployed_isolation_dir="${tmp_dir}/leaky-deployed-isolation"
mkdir -p "${leaky_deployed_isolation_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${leaky_deployed_isolation_dir}/deployed_api_worker_leaky_isolation.json"
python3 - "${leaky_deployed_isolation_dir}/deployed_api_worker_leaky_isolation.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"]["isolation"]["runs"].append({"run_id": "run-01"})
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${leaky_deployed_isolation_dir}" >/tmp/arco-uat-evidence-leaky-deployed-isolation.out 2>/tmp/arco-uat-evidence-leaky-deployed-isolation.err; then
  echo "validator should fail when deployed evidence leaks rows into the isolation workspace" >&2
  exit 1
fi
grep -Fq "queries.isolation.runs" /tmp/arco-uat-evidence-leaky-deployed-isolation.err

missing_deployed_api_version_dir="${tmp_dir}/missing-deployed-api-version"
mkdir -p "${missing_deployed_api_version_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_deployed_api_version_dir}/deployed_api_worker_missing_api_version.json"
python3 - "${missing_deployed_api_version_dir}/deployed_api_worker_missing_api_version.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact.pop("apiVersion")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_deployed_api_version_dir}" >/tmp/arco-uat-evidence-missing-deployed-api-version.out 2>/tmp/arco-uat-evidence-missing-deployed-api-version.err; then
  echo "validator should fail when deployed evidence omits API version proof" >&2
  exit 1
fi
grep -Fq "apiVersion" /tmp/arco-uat-evidence-missing-deployed-api-version.err

expected_deployed_api_version_output="$(
  bash "${VALIDATOR}" \
    --expect-api-code-version uat-api-code-version \
    --expect-api-git-sha abc123 \
    --expect-api-image us-central1-docker.pkg.dev/example/arco-api:abc123 \
    --require-kind deployed_api_worker \
    "${valid_dir}"
)"
grep -Fq "deployed_api_worker_pipeline.json: ok" <<<"${expected_deployed_api_version_output}"

unknown_deployed_api_version_dir="${tmp_dir}/unknown-deployed-api-version"
mkdir -p "${unknown_deployed_api_version_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${unknown_deployed_api_version_dir}/deployed_api_worker_unknown_api_version.json"
python3 - "${unknown_deployed_api_version_dir}/deployed_api_worker_unknown_api_version.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
for key in ("codeVersion", "gitSha", "image", "cloudRunRevision"):
    artifact["apiVersion"][key] = "unknown"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${unknown_deployed_api_version_dir}" >/tmp/arco-uat-evidence-unknown-deployed-api-version.out 2>/tmp/arco-uat-evidence-unknown-deployed-api-version.err; then
  echo "validator should fail when deployed evidence records unknown API provenance" >&2
  exit 1
fi
grep -Fq "apiVersion.codeVersion" /tmp/arco-uat-evidence-unknown-deployed-api-version.err

if bash "${VALIDATOR}" \
  --expect-api-code-version uat-api-code-version \
  --expect-api-git-sha def456 \
  --expect-api-image us-central1-docker.pkg.dev/example/arco-api:abc123 \
  --require-kind deployed_api_worker \
  "${valid_dir}" >/tmp/arco-uat-evidence-mismatched-deployed-api-version.out 2>/tmp/arco-uat-evidence-mismatched-deployed-api-version.err; then
  echo "validator should fail when deployed evidence API provenance does not match expected values" >&2
  exit 1
fi
grep -Fq "apiVersion.gitSha" /tmp/arco-uat-evidence-mismatched-deployed-api-version.err

missing_deployed_api_access_dir="${tmp_dir}/missing-deployed-api-access"
mkdir -p "${missing_deployed_api_access_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_deployed_api_access_dir}/deployed_api_worker_missing_api_access.json"
python3 - "${missing_deployed_api_access_dir}/deployed_api_worker_missing_api_access.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact.pop("apiAccess")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_deployed_api_access_dir}" >/tmp/arco-uat-evidence-missing-deployed-api-access.out 2>/tmp/arco-uat-evidence-missing-deployed-api-access.err; then
  echo "validator should fail when deployed evidence omits API access proof" >&2
  exit 1
fi
grep -Fq "apiAccess" /tmp/arco-uat-evidence-missing-deployed-api-access.err

invalid_dir="${tmp_dir}/invalid-json"
mkdir -p "${invalid_dir}"
printf '{not-json' >"${invalid_dir}/durable_storage_bad.json"
if bash "${VALIDATOR}" "${invalid_dir}" >/tmp/arco-uat-evidence-invalid.out 2>/tmp/arco-uat-evidence-invalid.err; then
  echo "validator should fail invalid JSON" >&2
  exit 1
fi
grep -Fq "invalid JSON" /tmp/arco-uat-evidence-invalid.err

missing_query_dir="${tmp_dir}/missing-query"
mkdir -p "${missing_query_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_query_dir}/deployed_api_worker_missing_query.json"
python3 - "${missing_query_dir}/deployed_api_worker_missing_query.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"].pop("partitionStatus")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_query_dir}" >/tmp/arco-uat-evidence-missing-query.out 2>/tmp/arco-uat-evidence-missing-query.err; then
  echo "validator should fail when deployed evidence is missing partitionStatus query proof" >&2
  exit 1
fi
grep -Fq "queries.partitionStatus" /tmp/arco-uat-evidence-missing-query.err

failed_task_dir="${tmp_dir}/failed-task"
mkdir -p "${failed_task_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${failed_task_dir}/deployed_api_worker_failed_task.json"
python3 - "${failed_task_dir}/deployed_api_worker_failed_task.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"]["tasks"][0]["state"] = "FAILED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${failed_task_dir}" >/tmp/arco-uat-evidence-failed-task.out 2>/tmp/arco-uat-evidence-failed-task.err; then
  echo "validator should fail when deployed evidence does not prove succeeded tasks" >&2
  exit 1
fi
grep -Fq "queries.tasks" /tmp/arco-uat-evidence-failed-task.err

missing_code_version_dir="${tmp_dir}/missing-code-version"
mkdir -p "${missing_code_version_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_code_version_dir}/deployed_api_worker_missing_code_version.json"
python3 - "${missing_code_version_dir}/deployed_api_worker_missing_code_version.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"]["runs"][0].pop("code_version")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_code_version_dir}" >/tmp/arco-uat-evidence-missing-code-version.out 2>/tmp/arco-uat-evidence-missing-code-version.err; then
  echo "validator should fail when deployed evidence lacks run code_version" >&2
  exit 1
fi
grep -Fq "queries.runs" /tmp/arco-uat-evidence-missing-code-version.err

missing_lineage_dir="${tmp_dir}/missing-lineage"
mkdir -p "${missing_lineage_dir}"
cp "${valid_dir}/deployed_api_worker_pipeline.json" "${missing_lineage_dir}/deployed_api_worker_missing_lineage.json"
python3 - "${missing_lineage_dir}/deployed_api_worker_missing_lineage.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"]["catalogRunIndex"][0].pop("execution_lineage_ref")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${missing_lineage_dir}" >/tmp/arco-uat-evidence-missing-lineage.out 2>/tmp/arco-uat-evidence-missing-lineage.err; then
  echo "validator should fail when deployed evidence lacks materialization lineage" >&2
  exit 1
fi
grep -Fq "execution_lineage_ref" /tmp/arco-uat-evidence-missing-lineage.err

failure_artifact_dir="${tmp_dir}/failure-artifact"
mkdir -p "${failure_artifact_dir}"
cat >"${failure_artifact_dir}/deployed_api_worker_failure_pipeline.json" <<'JSON'
{
  "kind": "deployed_api_worker_failure",
  "recordedAt": "2026-06-04T00:00:00Z",
  "apiBaseUrl": "https://arco.acceptance.example",
  "apiAccess": {
    "mode": "direct-url",
    "ingressMode": "not-recorded"
  },
  "apiVersion": {
    "service": "arco-api",
    "packageVersion": "0.2.0",
    "codeVersion": "uat-api-code-version",
    "gitSha": "abc123",
    "image": "us-central1-docker.pkg.dev/example/arco-api:abc123",
    "cloudRunRevision": "arco-api-dev-00042-test"
  },
  "tenantId": "arco-uat-tenant",
  "workspaceId": "arco-uat-workspace",
  "identity": {
    "namespace": "uat_worker_pipeline_01",
    "rawAsset": "raw_orders_01",
    "preparedAsset": "daily_orders_prepared_01",
    "table": "daily_orders_01",
    "rawAssetKey": "uat_worker_pipeline_01.raw_orders_01",
    "preparedAssetKey": "uat_worker_pipeline_01.daily_orders_prepared_01",
    "targetAssetKey": "uat_worker_pipeline_01.daily_orders_01",
    "runKey": "uat:worker_pipeline:01",
    "codeVersionId": "uat-worker_pipeline-01"
  },
  "runId": "01KT9Y5PNHMGWC0DSSD5Z62YJ5",
  "failure": {
    "stage": "tasks",
    "error": "timed out polling deployed UAT query"
  },
  "queries": {
    "runs": [{
      "run_id": "01KT9Y5PNHMGWC0DSSD5Z62YJ5",
      "run_key": "uat:worker_pipeline:01",
      "state": "RUNNING",
      "code_version": "uat-worker_pipeline-01"
    }],
    "tasks": [{
      "task_key": "uat_worker_pipeline_01.raw_orders_01",
      "state": "READY"
    }],
    "dependencies": [{
      "upstream_task_key": "uat_worker_pipeline_01.raw_orders_01",
      "downstream_task_key": "uat_worker_pipeline_01.daily_orders_prepared_01",
      "satisfied": false,
      "resolution": "PENDING"
    }]
  }
}
JSON
failure_kind_output="$(bash "${VALIDATOR}" --require-kind deployed_api_worker_failure "${failure_artifact_dir}")"
grep -Fq "deployed_api_worker_failure_pipeline.json: ok" <<<"${failure_kind_output}"
grep -Fq "validated 1 UAT evidence artifact(s)" <<<"${failure_kind_output}"

missing_failure_run_context_dir="${tmp_dir}/missing-failure-run-context"
mkdir -p "${missing_failure_run_context_dir}"
cp "${failure_artifact_dir}/deployed_api_worker_failure_pipeline.json" "${missing_failure_run_context_dir}/deployed_api_worker_failure_missing_run_context.json"
python3 - "${missing_failure_run_context_dir}/deployed_api_worker_failure_missing_run_context.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["queries"]["runs"][0].pop("run_key")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" --require-kind deployed_api_worker_failure "${missing_failure_run_context_dir}" >/tmp/arco-uat-evidence-missing-failure-run-context.out 2>/tmp/arco-uat-evidence-missing-failure-run-context.err; then
  echo "validator should fail when deployed failure evidence lacks run key context" >&2
  exit 1
fi
grep -Fq "queries.runs" /tmp/arco-uat-evidence-missing-failure-run-context.err

if bash "${VALIDATOR}" --require-kind deployed_api_worker "${failure_artifact_dir}" >/tmp/arco-uat-evidence-failure-artifact.out 2>/tmp/arco-uat-evidence-failure-artifact.err; then
  echo "validator should not accept deployed failure artifacts as success evidence" >&2
  exit 1
fi
grep -Fq "missing required UAT evidence kind: deployed_api_worker" /tmp/arco-uat-evidence-failure-artifact.err

secret_dir="${tmp_dir}/secret"
mkdir -p "${secret_dir}"
cp "${valid_dir}/durable_storage_pipeline.json" "${secret_dir}/durable_storage_secret.json"
python3 - "${secret_dir}/durable_storage_secret.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    artifact = json.load(handle)
artifact["apiToken"] = "should-not-be-written"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(artifact, handle)
PY
if bash "${VALIDATOR}" "${secret_dir}" >/tmp/arco-uat-evidence-secret.out 2>/tmp/arco-uat-evidence-secret.err; then
  echo "validator should fail evidence containing secret-like keys" >&2
  exit 1
fi
grep -Fq "secret-like key" /tmp/arco-uat-evidence-secret.err

empty_dir="${tmp_dir}/empty"
mkdir -p "${empty_dir}"
if bash "${VALIDATOR}" "${empty_dir}" >/tmp/arco-uat-evidence-empty.out 2>/tmp/arco-uat-evidence-empty.err; then
  echo "validator should fail when no UAT evidence artifacts are present" >&2
  exit 1
fi
grep -Fq "no UAT evidence artifacts found" /tmp/arco-uat-evidence-empty.err

echo "User acceptance evidence validator checks passed."
