//! User acceptance coverage for a cataloged, orchestrated pipeline workflow.
//!
//! The default tests are deterministic and in-process. The ignored live gate
//! runs the same acceptance flows against durable object storage:
//!
//! ```bash
//! ARCO_UAT_STORAGE_BUCKET=gs://arco-uat \
//! ARCO_UAT_TENANT=arco-uat-tenant \
//! ARCO_UAT_WORKSPACE=arco-uat-workspace \
//! ARCO_UAT_EVIDENCE_DIR=target/uat-evidence \
//! cargo test -p arco-integration-tests --test user_acceptance_pipeline \
//!   -- --ignored live_user_acceptance_pipeline_runs_against_durable_storage
//! ```
//!
//! The deployed smoke is also ignored by default and requires a running Arco
//! API plus its real dispatch/worker services:
//!
//! ```bash
//! ARCO_UAT_API_URL=https://arco.example.com \
//! ARCO_UAT_API_TOKEN=... \
//! ARCO_UAT_TENANT=arco-uat-tenant \
//! ARCO_UAT_WORKSPACE=arco-uat-workspace \
//! ARCO_UAT_EVIDENCE_DIR=target/uat-evidence \
//! cargo test -p arco-integration-tests --test user_acceptance_pipeline \
//!   -- --ignored live_deployed_user_acceptance_pipeline_runs_through_api_and_workers
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use ulid::Ulid;

use arco_api::server::ServerBuilder;
use arco_core::storage::{MemoryBackend, ObjectStoreBackend, StorageBackend, WritePrecondition};
use arco_core::{ScopedStorage, TaskTokenConfig, mint_task_token_for_attempt};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::callbacks::{
    ErrorCategory, TaskCompletedRequest, TaskError, TaskOutput, TaskOutputVisibilityState,
    TaskStartedRequest, WorkerOutcome,
};
use arco_flow::orchestration::compactor::{
    MicroCompactor, OrchestrationManifest, OrchestrationManifestPointer,
};
use arco_flow::orchestration::controllers::{
    BackfillController, DispatchAction, DispatcherController, PartitionResolver, PollSensorResult,
    PubSubMessage, ReadyDispatchAction, ReadyDispatchController, RetryAction, RetryHandler,
    RunRequestProcessor, ScheduleController, SensorEvaluationError, SensorEvaluator,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, RunRequest, TaskDef, TriggerInfo,
};
use arco_flow::orchestration::ids::dispatch_internal_id;
use arco_flow::orchestration::worker_contract::{
    WorkerDispatchEnvelope, callback_task_id, parse_callback_task_id,
};
use arco_flow::orchestration_manifest_pointer_path;

const DEFAULT_TENANT: &str = "acceptance-tenant";
const DEFAULT_WORKSPACE: &str = "analytics-workspace";
const DEFAULT_OTHER_WORKSPACE: &str = "other-workspace";
const DEFAULT_RUN_ID: &str = "uat_run_01";
const DEFAULT_PLAN_ID: &str = "uat_plan_01";

#[tokio::test]
async fn user_acceptance_pipeline_runs_to_queryable_catalog_state() {
    assert_cataloged_pipeline_workflow(AcceptanceHarness::new()).await;
}

#[tokio::test]
async fn user_acceptance_schedule_tick_is_queryable_after_manifest_deploy() {
    assert_schedule_tick_workflow(AcceptanceHarness::new()).await;
}

#[tokio::test]
async fn user_acceptance_backfill_request_is_queryable_after_chunk_planning() {
    assert_backfill_workflow(AcceptanceHarness::new()).await;
}

#[tokio::test]
async fn user_acceptance_sensor_evaluate_is_queryable_after_run_bridge() {
    assert_sensor_workflow(AcceptanceHarness::with_sensor_evaluator(Arc::new(
        AcceptanceSensorEvaluator,
    )))
    .await;
}

#[tokio::test]
async fn user_acceptance_failed_task_retry_state_is_queryable() {
    assert_retry_workflow(AcceptanceHarness::new()).await;
}

#[tokio::test]
async fn user_acceptance_worker_callback_routes_execute_task_to_queryable_state() {
    let harness = AcceptanceHarness::new();
    harness
        .create_catalog_table("analytics", "daily_orders")
        .await;
    let task = PipelineTask::new("analytics.daily_orders", "analytics.daily_orders", vec![]);
    let run = harness
        .deploy_and_trigger_pipeline_run(std::slice::from_ref(&task))
        .await;
    let dispatch = harness.request_ready_dispatch(&run.run_id, &task).await;
    let worker_dispatch = harness
        .enqueue_dispatch(&run.run_id, &task, &dispatch)
        .await;

    let evidence = harness
        .complete_worker_task_through_public_api(
            &task,
            worker_dispatch,
            TaskCompletion::Succeeded { delta_version: 1 },
        )
        .await;

    assert_eq!(evidence.started_status, StatusCode::OK);
    assert_eq!(evidence.completed_status, StatusCode::OK);

    let task_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT task_key, state FROM system.orchestration.tasks WHERE run_id = '{}'",
                run.run_id
            )
        }))
        .await;
    assert_eq!(
        task_rows,
        json!([{ "task_key": "analytics.daily_orders", "state": "SUCCEEDED" }])
    );
}

#[test]
fn live_acceptance_config_builds_isolated_identity_from_env() {
    let vars = HashMap::from([
        (
            "ARCO_UAT_STORAGE_BUCKET".to_string(),
            "gs://arco-uat".to_string(),
        ),
        ("ARCO_UAT_TENANT".to_string(), "tenant".to_string()),
        ("ARCO_UAT_WORKSPACE".to_string(), "workspace".to_string()),
        (
            "ARCO_UAT_EVIDENCE_DIR".to_string(),
            "target/custom-uat-evidence".to_string(),
        ),
    ]);

    let config = LiveAcceptanceConfig::from_vars(&vars).expect("live UAT config");
    let identity = config.identity("pipeline");

    assert_eq!(config.bucket, "gs://arco-uat");
    assert_eq!(
        config.evidence_dir,
        PathBuf::from("target/custom-uat-evidence")
    );
    assert!(identity.tenant_id.starts_with("tenant-pipeline-"));
    assert!(identity.workspace_id.starts_with("workspace-pipeline-"));
    assert!(
        identity
            .other_workspace_id
            .starts_with("workspace-pipeline-other-")
    );
    assert_ne!(identity.workspace_id, identity.other_workspace_id);
}

#[test]
fn deployed_acceptance_config_requires_api_url_and_scopes_request() {
    let missing = DeployedAcceptanceConfig::from_vars(&HashMap::new())
        .expect_err("deployed UAT should require an API URL");
    assert_eq!(missing, "ARCO_UAT_API_URL is required for deployed UAT");

    let vars = HashMap::from([
        (
            "ARCO_UAT_API_URL".to_string(),
            "https://arco.acceptance.example/".to_string(),
        ),
        (
            "ARCO_UAT_API_TOKEN".to_string(),
            "acceptance-token".to_string(),
        ),
        (
            "ARCO_UAT_TENANT".to_string(),
            "acceptance-tenant".to_string(),
        ),
        (
            "ARCO_UAT_WORKSPACE".to_string(),
            "acceptance-workspace".to_string(),
        ),
        ("ARCO_UAT_RUN_TIMEOUT_SECS".to_string(), "120".to_string()),
        (
            "ARCO_UAT_EVIDENCE_DIR".to_string(),
            "target/deployed-uat-evidence".to_string(),
        ),
        (
            "ARCO_UAT_API_ACCESS_MODE".to_string(),
            "cloud-run-proxy".to_string(),
        ),
        (
            "ARCO_UAT_API_INGRESS_MODE".to_string(),
            "internal-only".to_string(),
        ),
        (
            "ARCO_UAT_CLOUD_RUN_SERVICE".to_string(),
            "arco-api-dev".to_string(),
        ),
        (
            "ARCO_UAT_CLOUD_RUN_PROJECT_ID".to_string(),
            "arco-testing-20260320".to_string(),
        ),
        (
            "ARCO_UAT_CLOUD_RUN_REGION".to_string(),
            "us-central1".to_string(),
        ),
    ]);

    let config = DeployedAcceptanceConfig::from_vars(&vars).expect("deployed UAT config");
    let identity = DeployedAcceptanceConfig::identity("worker pipeline");

    assert_eq!(config.api_base_url, "https://arco.acceptance.example");
    assert_eq!(config.bearer_token.as_deref(), Some("acceptance-token"));
    assert_eq!(config.tenant_id, "acceptance-tenant");
    assert_eq!(config.workspace_id, "acceptance-workspace");
    assert_eq!(config.run_timeout, StdDuration::from_secs(120));
    assert_eq!(
        config.evidence_dir,
        PathBuf::from("target/deployed-uat-evidence")
    );
    assert_eq!(config.api_access.mode, "cloud-run-proxy");
    assert_eq!(config.api_access.ingress_mode, "internal-only");
    assert_eq!(
        config.api_access.cloud_run_service.as_deref(),
        Some("arco-api-dev")
    );
    assert_eq!(
        config.api_access.cloud_run_project_id.as_deref(),
        Some("arco-testing-20260320")
    );
    assert_eq!(
        config.api_access.cloud_run_region.as_deref(),
        Some("us-central1")
    );
    assert!(identity.namespace.starts_with("uat_worker_pipeline_"));
    assert!(identity.table.starts_with("daily_orders_"));
    assert!(identity.run_key.starts_with("uat:worker_pipeline:"));
}

#[test]
fn deployed_acceptance_isolation_workspace_uses_separate_scope() {
    let config = DeployedAcceptanceConfig {
        api_base_url: "https://arco.acceptance.example".to_string(),
        api_access: DeployedApiAccess {
            mode: "direct-url".to_string(),
            ingress_mode: "not-recorded".to_string(),
            cloud_run_service: None,
            cloud_run_project_id: None,
            cloud_run_region: None,
        },
        bearer_token: None,
        tenant_id: "acceptance-tenant".to_string(),
        workspace_id: "acceptance-workspace".to_string(),
        run_timeout: StdDuration::from_secs(300),
        evidence_dir: PathBuf::from("target/uat-evidence"),
    };
    let identity = DeployedAcceptanceIdentity {
        namespace: "uat_worker_pipeline_01".to_string(),
        raw_asset: "raw_orders_01".to_string(),
        prepared_asset: "daily_orders_prepared_01".to_string(),
        table: "daily_orders_01".to_string(),
        run_key: "uat:worker_pipeline:01".to_string(),
        code_version_id: "uat-worker-pipeline-01".to_string(),
    };

    assert_eq!(
        deployed_isolation_workspace(&config, &identity),
        "acceptance-workspace-isolation-uat_worker_pipeline_01"
    );
}

#[test]
fn deployed_acceptance_request_errors_include_request_context() {
    let message = deployed_request_error_message(
        "POST",
        "/api/v1/namespaces",
        "http://127.0.0.1:18080/api/v1/namespaces",
        "connection closed before message completed",
    );

    assert!(message.contains("method=POST"));
    assert!(message.contains("path=/api/v1/namespaces"));
    assert!(message.contains("url=http://127.0.0.1:18080/api/v1/namespaces"));
    assert!(message.contains("connection closed before message completed"));
}

#[test]
fn deployed_acceptance_evidence_proof_summarizes_live_query_identifiers() {
    let identity = DeployedAcceptanceIdentity {
        namespace: "uat_ns".to_string(),
        raw_asset: "raw_orders".to_string(),
        prepared_asset: "daily_orders_prepared".to_string(),
        table: "daily_orders".to_string(),
        run_key: "uat_run_key".to_string(),
        code_version_id: "uat-pipeline-v1".to_string(),
    };
    let publication_rows = json!([{
        "task_key": "uat_ns.daily_orders",
        "materialization_id": "mat-01",
        "delta_table": "uat_ns.daily_orders",
        "delta_version": 7,
        "delta_partition": "date=2026-06-01"
    }]);
    let partition_rows = json!([{
        "asset_key": "uat_ns.daily_orders",
        "partition_key": "date=d:2026-06-01"
    }]);

    let proof = deployed_evidence_proof(&identity, "run-01", &publication_rows, &partition_rows);

    assert_eq!(
        proof,
        json!({
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
        })
    );
}

#[test]
fn deployed_acceptance_catalog_metadata_rows_cover_all_pipeline_assets() {
    let identity = DeployedAcceptanceIdentity {
        namespace: "uat_ns".to_string(),
        raw_asset: "raw_orders".to_string(),
        prepared_asset: "daily_orders_prepared".to_string(),
        table: "daily_orders".to_string(),
        run_key: "uat_run_key".to_string(),
        code_version_id: "uat-pipeline-v1".to_string(),
    };

    assert_eq!(
        deployed_catalog_metadata_rows(&identity),
        json!([
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
        ])
    );
}

#[test]
fn live_acceptance_evidence_writer_persists_reviewable_json_without_secrets() {
    let evidence_dir = env::temp_dir().join(format!("arco-uat-evidence-{}", Ulid::new()));
    let artifact = json!({
        "kind": "deployed_api_worker",
        "apiBaseUrl": "https://arco.acceptance.example",
        "apiAccess": {
            "mode": "direct-url",
            "ingressMode": "not-recorded"
        },
        "apiToken": "must-not-be-written",
        "runId": "run_123",
        "queries": {
            "tasks": [{ "task_key": "analytics.daily_orders", "state": "SUCCEEDED" }]
        }
    });

    let path = write_uat_evidence(
        &evidence_dir,
        "deployed api worker",
        sanitize_uat_evidence(artifact),
    )
    .expect("write evidence artifact");
    let persisted: Value =
        serde_json::from_str(&fs::read_to_string(&path).expect("read evidence artifact"))
            .expect("parse evidence JSON");

    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("deployed_api_worker.json")
    );
    assert_eq!(persisted.get("kind"), Some(&json!("deployed_api_worker")));
    assert_eq!(persisted.get("runId"), Some(&json!("run_123")));
    assert!(persisted.get("apiToken").is_none());
    assert_eq!(
        persisted
            .get("queries")
            .and_then(|queries| queries.get("tasks")),
        Some(&json!([{ "task_key": "analytics.daily_orders", "state": "SUCCEEDED" }]))
    );

    fs::remove_dir_all(evidence_dir).expect("remove evidence dir");
}

#[test]
fn deployed_failure_evidence_writer_preserves_partial_live_context_without_secrets() {
    let evidence_dir = env::temp_dir().join(format!("arco-uat-failure-evidence-{}", Ulid::new()));
    let config = DeployedAcceptanceConfig {
        api_base_url: "https://arco.acceptance.example".to_string(),
        api_access: DeployedApiAccess {
            mode: "direct-url".to_string(),
            ingress_mode: "not-recorded".to_string(),
            cloud_run_service: None,
            cloud_run_project_id: None,
            cloud_run_region: None,
        },
        bearer_token: Some("must-not-be-written".to_string()),
        tenant_id: "arco-uat-tenant".to_string(),
        workspace_id: "arco-uat-workspace".to_string(),
        run_timeout: StdDuration::from_secs(1),
        evidence_dir: evidence_dir.clone(),
    };
    let identity = DeployedAcceptanceIdentity {
        namespace: "uat_worker_pipeline_01".to_string(),
        raw_asset: "raw_orders_01".to_string(),
        prepared_asset: "daily_orders_prepared_01".to_string(),
        table: "daily_orders_01".to_string(),
        run_key: "uat:worker_pipeline:01".to_string(),
        code_version_id: "uat-worker_pipeline-01".to_string(),
    };
    let api_version = json!({
        "service": "arco-api-dev",
        "packageVersion": "0.2.0",
        "codeVersion": "uat-live",
        "gitSha": "abc123",
        "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        "cloudRunRevision": "arco-api-dev-00001-test"
    });
    let failure = DeployedUatFailureEvidence {
        stage: "tasks".to_string(),
        error: "timed out polling deployed UAT query".to_string(),
        run_id: Some("01KT9Y5PNHMGWC0DSSD5Z62YJ5".to_string()),
        run_rows: Some(json!([{
            "run_id": "01KT9Y5PNHMGWC0DSSD5Z62YJ5",
            "run_key": "uat:worker_pipeline:01",
            "state": "RUNNING",
            "code_version": "uat-worker_pipeline-01"
        }])),
        task_rows: Some(json!([{
            "task_key": "uat_worker_pipeline_01.raw_orders_01",
            "state": "READY"
        }])),
        dependency_rows: Some(json!([{
            "upstream_task_key": "uat_worker_pipeline_01.raw_orders_01",
            "downstream_task_key": "uat_worker_pipeline_01.daily_orders_prepared_01",
            "satisfied": false
        }])),
    };

    let path = write_deployed_failure_evidence(&config, &identity, Some(api_version), failure)
        .expect("write deployed failure evidence");
    let persisted: Value =
        serde_json::from_str(&fs::read_to_string(&path).expect("read failure artifact"))
            .expect("parse failure evidence");

    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("deployed_api_worker_failure_uat_worker_pipeline_01.json")
    );
    assert_eq!(
        persisted.get("kind"),
        Some(&json!("deployed_api_worker_failure"))
    );
    assert_eq!(
        persisted.get("runId"),
        Some(&json!("01KT9Y5PNHMGWC0DSSD5Z62YJ5"))
    );
    assert_eq!(persisted.pointer("/failure/stage"), Some(&json!("tasks")));
    assert_eq!(
        persisted.pointer("/queries/tasks"),
        Some(&json!([{
            "task_key": "uat_worker_pipeline_01.raw_orders_01",
            "state": "READY"
        }]))
    );
    assert!(persisted.get("bearerToken").is_none());
    assert!(persisted.get("apiToken").is_none());

    fs::remove_dir_all(evidence_dir).expect("remove failure evidence dir");
}

#[test]
fn deployed_failure_exit_error_includes_artifact_path() {
    let evidence_dir =
        env::temp_dir().join(format!("arco-uat-failure-exit-evidence-{}", Ulid::new()));
    let config = DeployedAcceptanceConfig {
        api_base_url: "https://arco.acceptance.example".to_string(),
        api_access: DeployedApiAccess {
            mode: "direct-url".to_string(),
            ingress_mode: "not-recorded".to_string(),
            cloud_run_service: None,
            cloud_run_project_id: None,
            cloud_run_region: None,
        },
        bearer_token: None,
        tenant_id: "arco-uat-tenant".to_string(),
        workspace_id: "arco-uat-workspace".to_string(),
        run_timeout: StdDuration::from_secs(1),
        evidence_dir: evidence_dir.clone(),
    };
    let identity = DeployedAcceptanceIdentity {
        namespace: "uat_worker_pipeline_01".to_string(),
        raw_asset: "raw_orders_01".to_string(),
        prepared_asset: "daily_orders_prepared_01".to_string(),
        table: "daily_orders_01".to_string(),
        run_key: "uat:worker_pipeline:01".to_string(),
        code_version_id: "uat-worker_pipeline-01".to_string(),
    };
    let failure = DeployedUatFailureEvidence {
        stage: "runs".to_string(),
        error: "timed out polling deployed UAT query".to_string(),
        run_id: Some("01KT9Y5PNHMGWC0DSSD5Z62YJ5".to_string()),
        run_rows: Some(json!([{
            "run_id": "01KT9Y5PNHMGWC0DSSD5Z62YJ5",
            "state": "RUNNING"
        }])),
        task_rows: None,
        dependency_rows: None,
    };

    let error = deployed_failure_test_error(
        &config,
        &identity,
        None,
        failure,
        "01KT9Y5PNHMGWC0DSSD5Z62YJ5",
    )
    .expect_err("failure helper should return the deployed test error");
    assert!(error.contains("deployed UAT failed after run_id=01KT9Y5PNHMGWC0DSSD5Z62YJ5"));
    assert!(error.contains("failure evidence:"));
    assert!(error.contains("deployed_api_worker_failure_uat_worker_pipeline_01.json"));
    let artifact_path =
        evidence_dir.join("deployed_api_worker_failure_uat_worker_pipeline_01.json");
    assert!(artifact_path.exists(), "failure artifact should be written");

    fs::remove_dir_all(evidence_dir).expect("remove failure exit evidence dir");
}

#[tokio::test]
#[ignore = "requires ARCO_UAT_STORAGE_BUCKET and cloud credentials"]
async fn live_user_acceptance_pipeline_runs_against_durable_storage() {
    let config = LiveAcceptanceConfig::from_env().expect("live UAT config");
    let backend: Arc<dyn StorageBackend> = Arc::new(
        ObjectStoreBackend::from_bucket(&config.bucket).expect("configure durable storage backend"),
    );

    let pipeline_identity = config.identity("pipeline");
    let pipeline_proof = assert_cataloged_pipeline_workflow(AcceptanceHarness::with_backend(
        backend.clone(),
        pipeline_identity.clone(),
    ))
    .await;
    let schedule_identity = config.identity("schedule");
    let schedule_proof = assert_schedule_tick_workflow(AcceptanceHarness::with_backend(
        backend.clone(),
        schedule_identity.clone(),
    ))
    .await;
    let backfill_identity = config.identity("backfill");
    let backfill_proof = assert_backfill_workflow(AcceptanceHarness::with_backend(
        backend.clone(),
        backfill_identity.clone(),
    ))
    .await;
    let sensor_identity = config.identity("sensor");
    let sensor_proof =
        assert_sensor_workflow(AcceptanceHarness::with_backend_and_sensor_evaluator(
            backend.clone(),
            sensor_identity.clone(),
            Arc::new(AcceptanceSensorEvaluator),
        ))
        .await;
    let retry_identity = config.identity("retry");
    let retry_proof = assert_retry_workflow(AcceptanceHarness::with_backend(
        backend,
        retry_identity.clone(),
    ))
    .await;

    let evidence = json!({
        "kind": "durable_storage",
        "recordedAt": Utc::now(),
        "bucket": redact_storage_bucket(&config.bucket),
        "scenarios": {
            "pipeline": durable_scenario_evidence(&pipeline_identity, &[
                "catalog_metadata",
                "run_summary",
                "task_state",
                "dependency_satisfaction",
                "publication",
                "materialization_lineage",
                "partition_status",
                "workspace_isolation"
            ], json!({
                "runId": pipeline_proof.run_id,
                "planId": pipeline_proof.plan_id,
                "runKey": pipeline_proof.run_key
            })),
            "schedule": durable_scenario_evidence(&schedule_identity, &[
                "schedule_definition",
                "schedule_tick"
            ], json!({
                "tickId": schedule_proof.tick_id,
                "runKey": schedule_proof.run_key
            })),
            "backfill": durable_scenario_evidence(&backfill_identity, &[
                "backfill_request",
                "backfill_chunks",
                "backfill_run_code_version"
            ], json!({
                "backfillId": backfill_proof.backfill_id,
                "chunkRunKeys": backfill_proof.chunk_run_keys
            })),
            "sensor": durable_scenario_evidence(&sensor_identity, &[
                "sensor_eval",
                "sensor_run_code_version"
            ], json!({
                "sensorId": sensor_proof.sensor_id,
                "runKey": sensor_proof.run_key
            })),
            "retry": durable_scenario_evidence(&retry_identity, &[
                "retry_dispatch"
            ], json!({
                "taskKey": retry_proof.task_key,
                "attempt": retry_proof.dispatch.attempt,
                "attemptId": retry_proof.dispatch.attempt_id,
                "dispatchId": retry_proof.dispatch.dispatch_id
            }))
        }
    });
    write_uat_evidence(
        &config.evidence_dir,
        &format!("durable storage {}", pipeline_identity.workspace_id),
        evidence,
    )
    .expect("write durable UAT evidence");
}

#[tokio::test]
#[ignore = "requires ARCO_UAT_API_URL, deployed task dispatch, workers, and catalog storage"]
async fn live_deployed_user_acceptance_pipeline_runs_through_api_and_workers() -> Result<(), String>
{
    let config = DeployedAcceptanceConfig::from_env().expect("deployed UAT config");
    let client = DeployedAcceptanceClient::new(config.clone());
    let identity = DeployedAcceptanceConfig::identity("worker_pipeline");
    let api_version = client
        .get_json("/version")
        .await
        .expect("fetch deployed API version")
        .expect_success("fetch deployed API version");

    create_deployed_catalog_table(&client, &identity).await;
    deploy_deployed_pipeline_manifest(&client, &identity).await;
    let run_id = trigger_deployed_pipeline_run(&client, &identity).await;

    let deployed_proof =
        match collect_deployed_success_proof(&client, &config, &identity, &run_id).await {
            Ok(proof) => proof,
            Err(error) => {
                let failure =
                    collect_deployed_failure_evidence(&client, &identity, &run_id, error).await;
                return deployed_failure_test_error(
                    &config,
                    &identity,
                    Some(api_version.clone()),
                    failure,
                    &run_id,
                );
            }
        };
    let proof = deployed_evidence_proof(
        &identity,
        &run_id,
        &deployed_proof.publication_rows,
        &deployed_proof.partition_rows,
    );

    let evidence = json!({
        "kind": "deployed_api_worker",
        "recordedAt": Utc::now(),
        "apiBaseUrl": config.api_base_url,
        "apiAccess": config.api_access.evidence_json(),
        "apiVersion": api_version,
        "tenantId": config.tenant_id,
        "workspaceId": config.workspace_id,
        "identity": identity.evidence_json(),
        "runId": run_id,
        "proof": proof,
        "queries": {
            "catalogTables": deployed_proof.catalog_rows,
            "catalogMetadata": deployed_proof.catalog_metadata_rows,
            "runs": deployed_proof.run_rows,
            "tasks": deployed_proof.task_rows,
            "dependencies": deployed_proof.dependency_rows,
            "catalogRunIndex": deployed_proof.publication_rows,
            "partitionStatus": deployed_proof.partition_rows,
            "isolation": deployed_proof.isolation
        }
    });
    write_uat_evidence(
        &config.evidence_dir,
        &format!("deployed api worker {}", identity.namespace),
        evidence,
    )
    .expect("write deployed UAT evidence");
    Ok(())
}

async fn collect_deployed_success_proof(
    client: &DeployedAcceptanceClient,
    config: &DeployedAcceptanceConfig,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
) -> Result<DeployedSuccessProof, DeployedUatError> {
    let catalog_rows = assert_deployed_catalog_visible(client, identity, config.run_timeout)
        .await
        .map_err(|error| DeployedUatError::new("catalog_tables", error))?;
    let catalog_metadata_rows =
        assert_deployed_catalog_metadata_visible(client, identity, config.run_timeout)
            .await
            .map_err(|error| DeployedUatError::new("catalog_metadata", error))?;
    let run_rows = assert_deployed_run_visible(client, identity, run_id, config.run_timeout)
        .await
        .map_err(|error| DeployedUatError::new("runs", error))?;
    let task_rows = assert_deployed_tasks_succeeded(client, identity, run_id, config.run_timeout)
        .await
        .map_err(|error| DeployedUatError::new("tasks", error))?;
    let dependency_rows =
        assert_deployed_dependencies_satisfied(client, identity, run_id, config.run_timeout)
            .await
            .map_err(|error| DeployedUatError::new("dependencies", error))?;
    let publication_rows =
        assert_deployed_publication_visible(client, identity, run_id, config.run_timeout)
            .await
            .map_err(|error| DeployedUatError::new("catalog_run_index", error))?;
    let partition_rows =
        assert_deployed_partition_status_visible(client, identity, run_id, config.run_timeout)
            .await
            .map_err(|error| DeployedUatError::new("partition_status", error))?;
    let isolation = assert_deployed_workspace_isolation(client, config, identity, run_id)
        .await
        .map_err(|error| DeployedUatError::new("isolation", error))?;

    Ok(DeployedSuccessProof {
        catalog_rows,
        catalog_metadata_rows,
        run_rows,
        task_rows,
        dependency_rows,
        publication_rows,
        partition_rows,
        isolation,
    })
}

async fn collect_deployed_failure_evidence(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    error: DeployedUatError,
) -> DeployedUatFailureEvidence {
    let run_rows = deployed_failure_query_rows(
        client,
        format!(
            "SELECT run_id, run_key, state, tasks_total, tasks_completed, tasks_succeeded, \
                    tasks_failed, tasks_skipped, tasks_cancelled, code_version \
             FROM system.orchestration.runs \
             WHERE run_id = '{run_id}'"
        ),
    )
    .await;
    let task_rows = deployed_failure_query_rows(
        client,
        format!(
            "SELECT task_key, state, attempt, max_attempts \
             FROM system.orchestration.tasks \
             WHERE run_id = '{run_id}' ORDER BY task_key"
        ),
    )
    .await;
    let dependency_rows = deployed_failure_query_rows(
        client,
        format!(
            "SELECT upstream_task_key, downstream_task_key, satisfied, resolution, satisfying_attempt \
             FROM system.orchestration.dep_satisfaction \
             WHERE run_id = '{run_id}' ORDER BY downstream_task_key, upstream_task_key"
        ),
    )
    .await;

    DeployedUatFailureEvidence {
        stage: error.stage,
        error: error.error,
        run_id: Some(run_id.to_string()),
        run_rows,
        task_rows: task_rows.or_else(|| {
            Some(json!([{
                "task_key": identity.raw_asset_key(),
                "state": "UNKNOWN"
            }]))
        }),
        dependency_rows,
    }
}

async fn deployed_failure_query_rows(
    client: &DeployedAcceptanceClient,
    sql: String,
) -> Option<Value> {
    match client.query(sql).await {
        Ok(response) if response.status == StatusCode::OK => Some(response.body),
        Ok(response) => Some(json!({
            "status": response.status.as_u16(),
            "body": response.body
        })),
        Err(error) => Some(json!({
            "error": error
        })),
    }
}

async fn assert_deployed_workspace_isolation(
    client: &DeployedAcceptanceClient,
    config: &DeployedAcceptanceConfig,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
) -> Result<Value, String> {
    let isolation_workspace = deployed_isolation_workspace(config, identity);
    assert_ne!(isolation_workspace, config.workspace_id);

    let isolation_client = client.with_workspace(isolation_workspace.clone());
    create_deployed_isolation_catalog_seed(&isolation_client, identity).await;

    let catalog_rows = assert_deployed_empty_query(
        &isolation_client,
        format!(
            "SELECT name FROM system.catalog.tables \
             WHERE name IN ('{}', '{}', '{}') ORDER BY name",
            identity.raw_asset, identity.prepared_asset, identity.table
        ),
        "deployed isolation catalog tables",
    )
    .await?;
    let run_rows = assert_deployed_empty_query(
        &isolation_client,
        format!("SELECT run_id FROM system.orchestration.runs WHERE run_id = '{run_id}'"),
        "deployed isolation runs",
    )
    .await?;
    let task_rows = assert_deployed_empty_query(
        &isolation_client,
        format!(
            "SELECT task_key FROM system.orchestration.tasks \
             WHERE run_id = '{run_id}' ORDER BY task_key"
        ),
        "deployed isolation tasks",
    )
    .await?;
    let dependency_rows = assert_deployed_empty_query(
        &isolation_client,
        format!(
            "SELECT upstream_task_key, downstream_task_key \
             FROM system.orchestration.dep_satisfaction \
             WHERE run_id = '{run_id}' ORDER BY downstream_task_key, upstream_task_key"
        ),
        "deployed isolation dependency satisfaction",
    )
    .await?;
    let catalog_run_rows = assert_deployed_empty_query(
        &isolation_client,
        format!(
            "SELECT task_key FROM system.orchestration.catalog_run_index \
             WHERE run_id = '{run_id}' ORDER BY task_key"
        ),
        "deployed isolation catalog run index",
    )
    .await?;
    let partition_rows = assert_deployed_empty_query(
        &isolation_client,
        format!(
            "SELECT asset_key FROM system.orchestration.partition_status \
             WHERE asset_key = '{}'",
            identity.target_asset_key()
        ),
        "deployed isolation partition status",
    )
    .await?;

    Ok(json!({
        "workspaceId": isolation_workspace,
        "catalogTables": catalog_rows,
        "runs": run_rows,
        "tasks": task_rows,
        "dependencies": dependency_rows,
        "catalogRunIndex": catalog_run_rows,
        "partitionStatus": partition_rows
    }))
}

async fn assert_deployed_catalog_visible(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    timeout: StdDuration,
) -> Result<Value, String> {
    let catalog_rows = client
        .poll_query_until(
            format!(
                "SELECT name FROM system.catalog.tables \
                 WHERE name = '{}'",
                identity.table
            ),
            timeout,
            has_rows,
        )
        .await?;
    let expected = json!([{
        "name": identity.table
    }]);
    if catalog_rows != expected {
        return Err(format!(
            "deployed catalog table rows mismatch: expected={expected} actual={catalog_rows}"
        ));
    }
    Ok(catalog_rows)
}

async fn assert_deployed_catalog_metadata_visible(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    timeout: StdDuration,
) -> Result<Value, String> {
    let metadata_rows = client
        .poll_query_until(
            format!(
                "SELECT n.name AS namespace_name, t.name AS table_name, c.name AS column_name, \
                        c.data_type, c.is_nullable, c.ordinal \
                 FROM system.catalog.namespaces n \
                 JOIN system.catalog.tables t ON t.namespace_id = n.id \
                 JOIN system.catalog.columns c ON c.table_id = t.id \
                 WHERE n.name = '{}' \
                 ORDER BY table_name, ordinal",
                identity.namespace
            ),
            timeout,
            deployed_catalog_metadata_matches(identity),
        )
        .await?;
    let expected = deployed_catalog_metadata_rows(identity);
    if metadata_rows != expected {
        return Err(format!(
            "deployed catalog metadata rows mismatch: expected={expected} actual={metadata_rows}"
        ));
    }
    Ok(metadata_rows)
}

async fn assert_deployed_run_visible(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    timeout: StdDuration,
) -> Result<Value, String> {
    let run_rows = client
        .poll_query_until(
            format!(
                "SELECT run_id, run_key, state, code_version FROM system.orchestration.runs \
                 WHERE run_id = '{run_id}'"
            ),
            timeout,
            deployed_run_succeeded_with_code_version(&identity.code_version_id),
        )
        .await?;
    let expected = json!([{
        "run_id": run_id,
        "run_key": identity.run_key,
        "state": "SUCCEEDED",
        "code_version": identity.code_version_id
    }]);
    if run_rows != expected {
        return Err(format!(
            "deployed run rows mismatch: expected={expected} actual={run_rows}"
        ));
    }
    Ok(run_rows)
}

async fn assert_deployed_tasks_succeeded(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    timeout: StdDuration,
) -> Result<Value, String> {
    let task_rows = client
        .poll_query_until(
            format!(
                "SELECT task_key, state FROM system.orchestration.tasks \
                 WHERE run_id = '{run_id}' ORDER BY task_key"
            ),
            timeout,
            all_deployed_pipeline_tasks_succeeded,
        )
        .await?;
    let expected = json!([
        {
            "task_key": identity.target_asset_key(),
            "state": "SUCCEEDED"
        },
        {
            "task_key": identity.prepared_asset_key(),
            "state": "SUCCEEDED"
        },
        {
            "task_key": identity.raw_asset_key(),
            "state": "SUCCEEDED"
        }
    ]);
    if task_rows != expected {
        return Err(format!(
            "deployed task rows mismatch: expected={expected} actual={task_rows}"
        ));
    }
    Ok(task_rows)
}

async fn assert_deployed_dependencies_satisfied(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    timeout: StdDuration,
) -> Result<Value, String> {
    let dependency_rows = client
        .poll_query_until(
            format!(
                "SELECT upstream_task_key, downstream_task_key, satisfied, resolution, satisfying_attempt \
                 FROM system.orchestration.dep_satisfaction \
                 WHERE run_id = '{run_id}' ORDER BY downstream_task_key, upstream_task_key"
            ),
            timeout,
            deployed_dependency_edges_satisfied,
        )
        .await?;
    let expected = json!([
        {
            "upstream_task_key": identity.prepared_asset_key(),
            "downstream_task_key": identity.target_asset_key(),
            "satisfied": true,
            "resolution": "SUCCESS",
            "satisfying_attempt": 1
        },
        {
            "upstream_task_key": identity.raw_asset_key(),
            "downstream_task_key": identity.prepared_asset_key(),
            "satisfied": true,
            "resolution": "SUCCESS",
            "satisfying_attempt": 1
        }
    ]);
    if dependency_rows != expected {
        return Err(format!(
            "deployed dependency rows mismatch: expected={expected} actual={dependency_rows}"
        ));
    }
    Ok(dependency_rows)
}

async fn assert_deployed_publication_visible(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    timeout: StdDuration,
) -> Result<Value, String> {
    let published_rows = client
        .poll_query_until(
            format!(
                "SELECT run_id, task_key, target_namespace, target_table, task_status, \
                        materialization_id, output_visibility_state, delta_table, delta_version, \
                        delta_partition, execution_lineage_ref \
                 FROM system.orchestration.catalog_run_index \
                 WHERE run_id = '{run_id}' \
                   AND target_namespace = '{}' \
                   AND target_table = '{}'",
                identity.namespace, identity.table
            ),
            timeout,
            deployed_publication_materialized,
        )
        .await?;
    let published_row = published_rows
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
        .ok_or_else(|| "missing deployed publication row".to_string())?;
    let materialization_id = published_row
        .get("materialization_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing deployed publication materialization id".to_string())?;
    let delta_version = published_row
        .get("delta_version")
        .and_then(Value::as_i64)
        .ok_or_else(|| "missing deployed publication delta version".to_string())?;
    let expected_lineage = execution_lineage_ref(
        run_id,
        &identity.target_asset_key(),
        materialization_id,
        delta_version,
    );
    let expected = json!([{
        "run_id": run_id,
        "task_key": identity.target_asset_key(),
        "target_namespace": identity.namespace,
        "target_table": identity.table,
        "task_status": "SUCCEEDED",
        "materialization_id": materialization_id,
        "output_visibility_state": "VISIBLE",
        "delta_table": identity.target_asset_key(),
        "delta_version": delta_version,
        "delta_partition": "date=2026-05-31",
        "execution_lineage_ref": expected_lineage
    }]);
    if published_rows != expected {
        return Err(format!(
            "deployed publication rows mismatch: expected={expected} actual={published_rows}"
        ));
    }
    Ok(published_rows)
}

async fn assert_deployed_partition_status_visible(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    timeout: StdDuration,
) -> Result<Value, String> {
    let partition_rows = client
        .poll_query_until(
            format!(
                "SELECT asset_key, partition_key, last_materialization_run_id, last_attempt_run_id, \
                        last_attempt_outcome, delta_table, delta_version, delta_partition, \
                        execution_lineage_ref \
                 FROM system.orchestration.partition_status \
                 WHERE asset_key = '{}'",
                identity.target_asset_key()
            ),
            timeout,
            deployed_partition_status_materialized,
        )
        .await?;
    let partition_row = partition_rows
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
        .ok_or_else(|| "missing deployed partition status row".to_string())?;
    let delta_version = partition_row
        .get("delta_version")
        .and_then(Value::as_i64)
        .ok_or_else(|| "missing deployed partition status delta version".to_string())?;
    let expected_lineage = partition_row
        .get("execution_lineage_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing deployed partition status execution lineage ref".to_string())?
        .to_string();
    let expected = json!([{
        "asset_key": identity.target_asset_key(),
        "partition_key": "date=d:2026-05-31",
        "last_materialization_run_id": run_id,
        "last_attempt_run_id": run_id,
        "last_attempt_outcome": "SUCCEEDED",
        "delta_table": identity.target_asset_key(),
        "delta_version": delta_version,
        "delta_partition": "date=2026-05-31",
        "execution_lineage_ref": expected_lineage
    }]);
    if partition_rows != expected {
        return Err(format!(
            "deployed partition status rows mismatch: expected={expected} actual={partition_rows}"
        ));
    }
    Ok(partition_rows)
}

async fn assert_cataloged_pipeline_workflow(harness: AcceptanceHarness) -> PipelineRun {
    create_cataloged_pipeline_metadata(&harness).await;

    let run = Box::pin(harness.run_pipeline(cataloged_pipeline_tasks())).await;
    assert_ne!(run.run_id, DEFAULT_RUN_ID);
    assert_ne!(run.plan_id, DEFAULT_PLAN_ID);

    assert_cataloged_run_and_catalog_queries(&harness, &run).await;
    assert_cataloged_task_and_publication_queries(&harness, &run).await;
    assert_cataloged_dependency_satisfaction_query(&harness, &run).await;
    assert_cataloged_materialization_queries(&harness, &run).await;
    assert_cataloged_partition_status_query(&harness, &run).await;
    assert_cataloged_workspace_isolation(&harness, &run).await;
    run
}

async fn create_cataloged_pipeline_metadata(harness: &AcceptanceHarness) {
    harness.create_catalog_namespace("staging").await;
    harness
        .create_catalog_table_in_existing_namespace("staging", "raw_orders")
        .await;
    harness
        .create_catalog_table_in_existing_namespace("staging", "daily_orders_prepared")
        .await;
    harness
        .create_catalog_table("analytics", "daily_orders")
        .await;
    harness
        .create_catalog_table_as(harness.other_workspace(), "finance", "payments")
        .await;
}

fn cataloged_pipeline_tasks() -> Vec<PipelineTask> {
    vec![
        PipelineTask::new("staging.raw_orders", "staging.raw_orders", vec![]),
        PipelineTask::new(
            "staging.daily_orders_prepared",
            "staging.daily_orders_prepared",
            vec!["staging.raw_orders"],
        ),
        PipelineTask::new(
            "analytics.daily_orders",
            "analytics.daily_orders",
            vec!["staging.daily_orders_prepared"],
        ),
    ]
}

async fn assert_cataloged_run_and_catalog_queries(harness: &AcceptanceHarness, run: &PipelineRun) {
    let run_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT run_id, run_key, state, tasks_total, tasks_completed, tasks_succeeded, \
                        tasks_failed, tasks_skipped, tasks_cancelled, cancel_requested, labels, code_version \
                 FROM system.orchestration.runs \
                 WHERE run_id = '{}'",
                run.run_id
            )
        }))
        .await;
    assert_eq!(
        run_rows,
        json!([{
            "run_id": run.run_id.clone(),
            "run_key": run.run_key.clone(),
            "state": "SUCCEEDED",
            "tasks_total": 3,
            "tasks_completed": 3,
            "tasks_succeeded": 3,
            "tasks_failed": 0,
            "tasks_skipped": 0,
            "tasks_cancelled": 0,
            "cancel_requested": false,
            "labels": "{\"acceptance_suite\":\"cataloged_pipeline\"}",
            "code_version": "uat-pipeline-v1"
        }])
    );

    let catalog_rows = harness
        .query(json!({
            "sql": "SELECT name FROM system.catalog.tables WHERE name = 'daily_orders'"
        }))
        .await;
    assert_eq!(catalog_rows, json!([{ "name": "daily_orders" }]));

    let metastore_rows = harness
        .query(json!({
            "sql": "SELECT n.name AS namespace_name, t.name AS table_name, c.name AS column_name, \
                           c.data_type, c.is_nullable, c.ordinal \
                    FROM system.catalog.namespaces n \
                    JOIN system.catalog.tables t ON t.namespace_id = n.id \
                    JOIN system.catalog.columns c ON c.table_id = t.id \
                    WHERE n.name IN ('analytics', 'staging') \
                    ORDER BY namespace_name, table_name, ordinal"
        }))
        .await;
    assert_eq!(
        metastore_rows,
        json!([
            {
                "namespace_name": "analytics",
                "table_name": "daily_orders",
                "column_name": "order_id",
                "data_type": "STRING",
                "is_nullable": false,
                "ordinal": 0
            },
            {
                "namespace_name": "analytics",
                "table_name": "daily_orders",
                "column_name": "order_total",
                "data_type": "DOUBLE",
                "is_nullable": true,
                "ordinal": 1
            },
            {
                "namespace_name": "staging",
                "table_name": "daily_orders_prepared",
                "column_name": "order_id",
                "data_type": "STRING",
                "is_nullable": false,
                "ordinal": 0
            },
            {
                "namespace_name": "staging",
                "table_name": "daily_orders_prepared",
                "column_name": "order_total",
                "data_type": "DOUBLE",
                "is_nullable": true,
                "ordinal": 1
            },
            {
                "namespace_name": "staging",
                "table_name": "raw_orders",
                "column_name": "order_id",
                "data_type": "STRING",
                "is_nullable": false,
                "ordinal": 0
            },
            {
                "namespace_name": "staging",
                "table_name": "raw_orders",
                "column_name": "order_total",
                "data_type": "DOUBLE",
                "is_nullable": true,
                "ordinal": 1
            }
        ])
    );
}

async fn assert_cataloged_task_and_publication_queries(
    harness: &AcceptanceHarness,
    run: &PipelineRun,
) {
    let task_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT task_key, state FROM system.orchestration.tasks \
                 WHERE run_id = '{}' ORDER BY task_key",
                run.run_id
            )
        }))
        .await;
    assert_eq!(
        task_rows,
        json!([
            { "task_key": "analytics.daily_orders", "state": "SUCCEEDED" },
            { "task_key": "staging.daily_orders_prepared", "state": "SUCCEEDED" },
            { "task_key": "staging.raw_orders", "state": "SUCCEEDED" }
        ])
    );

    let published_rows = harness
        .query(json!({
            "sql": "SELECT task_key, target_namespace, target_table, delta_table, delta_version \
                    FROM system.orchestration.catalog_run_index \
                    WHERE target_namespace = 'analytics' AND target_table = 'daily_orders'"
        }))
        .await;
    assert_eq!(
        published_rows,
        json!([{
            "task_key": "analytics.daily_orders",
            "target_namespace": "analytics",
            "target_table": "daily_orders",
            "delta_table": "analytics.daily_orders",
            "delta_version": 3
        }])
    );
}

async fn assert_cataloged_dependency_satisfaction_query(
    harness: &AcceptanceHarness,
    run: &PipelineRun,
) {
    let dependency_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT upstream_task_key, downstream_task_key, satisfied, resolution, satisfying_attempt \
                 FROM system.orchestration.dep_satisfaction \
                 WHERE run_id = '{}' ORDER BY downstream_task_key, upstream_task_key",
                run.run_id
            )
        }))
        .await;
    assert_eq!(
        dependency_rows,
        json!([
            {
                "upstream_task_key": "staging.daily_orders_prepared",
                "downstream_task_key": "analytics.daily_orders",
                "satisfied": true,
                "resolution": "SUCCESS",
                "satisfying_attempt": 1
            },
            {
                "upstream_task_key": "staging.raw_orders",
                "downstream_task_key": "staging.daily_orders_prepared",
                "satisfied": true,
                "resolution": "SUCCESS",
                "satisfying_attempt": 1
            }
        ])
    );
}

async fn assert_cataloged_materialization_queries(harness: &AcceptanceHarness, run: &PipelineRun) {
    let expected_lineage = execution_lineage_ref(
        &run.run_id,
        "analytics.daily_orders",
        "mat_analytics.daily_orders",
        3,
    );
    let materialization_rows = harness
        .query(json!({
            "sql": "SELECT materialization_id, output_visibility_state, delta_partition, execution_lineage_ref \
                    FROM system.orchestration.catalog_run_index \
                    WHERE target_namespace = 'analytics' AND target_table = 'daily_orders'"
        }))
        .await;
    assert_eq!(
        materialization_rows,
        json!([{
            "materialization_id": "mat_analytics.daily_orders",
            "output_visibility_state": "VISIBLE",
            "delta_partition": "date=2026-05-31",
            "execution_lineage_ref": expected_lineage
        }])
    );
}

async fn assert_cataloged_partition_status_query(harness: &AcceptanceHarness, run: &PipelineRun) {
    let expected_lineage = execution_lineage_ref(
        &run.run_id,
        "analytics.daily_orders",
        "mat_analytics.daily_orders",
        3,
    );
    let partition_rows = harness
        .query(json!({
            "sql": "SELECT asset_key, partition_key, last_materialization_run_id, last_attempt_run_id, \
                           last_attempt_outcome, delta_table, delta_version, delta_partition, execution_lineage_ref \
                    FROM system.orchestration.partition_status \
                    WHERE asset_key = 'analytics.daily_orders'"
        }))
        .await;
    assert_eq!(
        partition_rows,
        json!([{
            "asset_key": "analytics.daily_orders",
            "partition_key": "date=d:2026-05-31",
            "last_materialization_run_id": run.run_id.clone(),
            "last_attempt_run_id": run.run_id.clone(),
            "last_attempt_outcome": "SUCCEEDED",
            "delta_table": "analytics.daily_orders",
            "delta_version": 3,
            "delta_partition": "date=2026-05-31",
            "execution_lineage_ref": expected_lineage
        }])
    );
}

async fn assert_cataloged_workspace_isolation(harness: &AcceptanceHarness, run: &PipelineRun) {
    let leaked_rows = harness
        .query_as(
            harness.other_workspace(),
            json!({
                "sql": "SELECT name FROM system.catalog.tables WHERE name = 'daily_orders'"
            }),
        )
        .await;
    assert_eq!(leaked_rows, json!([]));

    let leaked_run_rows = harness
        .query_as(
            harness.other_workspace(),
            json!({
                "sql": format!(
                    "SELECT run_id FROM system.orchestration.runs WHERE run_id = '{}'",
                    run.run_id
                )
            }),
        )
        .await;
    assert_eq!(leaked_run_rows, json!([]));

    let leaked_task_rows = harness
        .query_as(
            harness.other_workspace(),
            json!({
                "sql": format!(
                    "SELECT task_key FROM system.orchestration.tasks WHERE run_id = '{}'",
                    run.run_id
                )
            }),
        )
        .await;
    assert_eq!(leaked_task_rows, json!([]));

    let leaked_dependency_rows = harness
        .query_as(
            harness.other_workspace(),
            json!({
                "sql": format!(
                    "SELECT upstream_task_key, downstream_task_key \
                     FROM system.orchestration.dep_satisfaction WHERE run_id = '{}'",
                    run.run_id
                )
            }),
        )
        .await;
    assert_eq!(leaked_dependency_rows, json!([]));

    let leaked_publication_rows = harness
        .query_as(
            harness.other_workspace(),
            json!({
                "sql": format!(
                    "SELECT task_key FROM system.orchestration.catalog_run_index WHERE run_id = '{}'",
                    run.run_id
                )
            }),
        )
        .await;
    assert_eq!(leaked_publication_rows, json!([]));

    let leaked_partition_rows = harness
        .query_as(
            harness.other_workspace(),
            json!({
                "sql": "SELECT asset_key FROM system.orchestration.partition_status \
                        WHERE asset_key = 'analytics.daily_orders'"
            }),
        )
        .await;
    assert_eq!(leaked_partition_rows, json!([]));
}

async fn assert_schedule_tick_workflow(harness: AcceptanceHarness) -> ScheduleWorkflowEvidence {
    let tick_id = harness.deploy_and_tick_daily_orders_schedule().await;
    let schedule_run_key = "sched:daily-orders:1740787200";

    let schedule_rows = harness
        .query(json!({
            "sql": "SELECT schedule_id, cron_expression, timezone, enabled, code_version \
                    FROM system.orchestration.schedule_definitions \
                    WHERE schedule_id = 'daily-orders'"
        }))
        .await;
    assert_eq!(
        schedule_rows,
        json!([{
            "schedule_id": "daily-orders",
            "cron_expression": "0 0 * * *",
            "timezone": "UTC",
            "enabled": true,
            "code_version": "uat-schedule-v1"
        }])
    );

    let tick_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT tick_id, schedule_id, status, run_key \
                 FROM system.orchestration.schedule_ticks WHERE tick_id = '{tick_id}'"
            )
        }))
        .await;
    assert_eq!(
        tick_rows,
        json!([{
            "tick_id": tick_id,
            "schedule_id": "daily-orders",
            "status": "{\"status\":\"triggered\"}",
            "run_key": schedule_run_key
        }])
    );

    let run_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT run_key, code_version FROM system.orchestration.runs \
                 WHERE run_key = '{schedule_run_key}'"
            )
        }))
        .await;
    assert_eq!(
        run_rows,
        json!([{
            "run_key": schedule_run_key,
            "code_version": "uat-schedule-v1"
        }])
    );
    ScheduleWorkflowEvidence {
        tick_id,
        run_key: schedule_run_key.to_string(),
    }
}

async fn assert_backfill_workflow(harness: AcceptanceHarness) -> BackfillWorkflowEvidence {
    let backfill_id = harness.request_and_plan_daily_orders_backfill().await;

    let backfill_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT backfill_id, state, total_partitions, planned_chunks, chunk_size, code_version \
                 FROM system.orchestration.backfills WHERE backfill_id = '{backfill_id}'"
            )
        }))
        .await;
    assert_eq!(
        backfill_rows,
        json!([{
            "backfill_id": backfill_id,
            "state": "RUNNING",
            "total_partitions": 4,
            "planned_chunks": 2,
            "chunk_size": 2,
            "code_version": "uat-backfill-v1"
        }])
    );

    let chunk_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT chunk_index, state, run_key \
                 FROM system.orchestration.backfill_chunks \
                 WHERE backfill_id = '{backfill_id}' ORDER BY chunk_index"
            )
        }))
        .await;
    assert_eq!(
        chunk_rows,
        json!([
            {
                "chunk_index": 0,
                "state": "PLANNED",
                "run_key": format!("backfill:{backfill_id}:chunk:0")
            },
            {
                "chunk_index": 1,
                "state": "PLANNED",
                "run_key": format!("backfill:{backfill_id}:chunk:1")
            }
        ])
    );

    let run_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT run_key, code_version FROM system.orchestration.runs \
                 WHERE run_key IN ('backfill:{backfill_id}:chunk:0', 'backfill:{backfill_id}:chunk:1') \
                 ORDER BY run_key"
            )
        }))
        .await;
    assert_eq!(
        run_rows,
        json!([
            {
                "run_key": format!("backfill:{backfill_id}:chunk:0"),
                "code_version": "uat-backfill-v1"
            },
            {
                "run_key": format!("backfill:{backfill_id}:chunk:1"),
                "code_version": "uat-backfill-v1"
            }
        ])
    );
    BackfillWorkflowEvidence {
        backfill_id: backfill_id.clone(),
        chunk_run_keys: vec![
            format!("backfill:{backfill_id}:chunk:0"),
            format!("backfill:{backfill_id}:chunk:1"),
        ],
    }
}

async fn assert_sensor_workflow(harness: AcceptanceHarness) -> SensorWorkflowEvidence {
    harness
        .create_catalog_table("analytics", "daily_orders")
        .await;
    let run_key = harness.evaluate_daily_orders_sensor().await;

    let eval_rows = harness
        .query(json!({
            "sql": "SELECT sensor_id, status, run_requests \
                    FROM system.orchestration.sensor_evals \
                    WHERE sensor_id = 'daily-orders-sensor'"
        }))
        .await;
    assert_eq!(
        eval_rows
            .as_array()
            .map(Vec::len)
            .expect("sensor eval rows array"),
        1
    );
    let eval_row = eval_rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("sensor eval row");
    assert_eq!(
        eval_row.get("sensor_id").and_then(Value::as_str),
        Some("daily-orders-sensor")
    );
    assert_eq!(
        eval_row.get("status").and_then(Value::as_str),
        Some("{\"status\":\"triggered\"}")
    );
    let run_requests: Value = serde_json::from_str(
        eval_row
            .get("run_requests")
            .and_then(Value::as_str)
            .expect("sensor run_requests JSON"),
    )
    .expect("parse sensor run_requests");
    assert_eq!(
        run_requests
            .as_array()
            .and_then(|requests| requests.first())
            .and_then(|request| request.get("code_version"))
            .and_then(Value::as_str),
        Some("uat-sensor-v1")
    );

    let run_rows = harness
        .query(json!({
            "sql": format!(
                "SELECT run_key, code_version FROM system.orchestration.runs \
                 WHERE run_key = '{run_key}'"
            )
        }))
        .await;
    assert_eq!(
        run_rows,
        json!([{
            "run_key": run_key,
            "code_version": "uat-sensor-v1"
        }])
    );
    SensorWorkflowEvidence {
        sensor_id: "daily-orders-sensor".to_string(),
        run_key,
    }
}

async fn assert_retry_workflow(harness: AcceptanceHarness) -> RetryWorkflowEvidence {
    let task = PipelineTask::new("load_daily_orders", "analytics.daily_orders", vec![])
        .with_max_attempts(2);

    harness.seed_pipeline_run(vec![task.clone()]).await;
    let retry_evidence = harness.fail_and_request_retry(&task).await;

    assert_eq!(retry_evidence.callback.completed_status, StatusCode::OK);
    assert_json_bool(
        &retry_evidence.callback.completed_body,
        "acknowledged",
        true,
    );
    assert_json_str(
        &retry_evidence.callback.completed_body,
        "finalState",
        "FAILED",
    );

    let task_rows = harness
        .query(json!({
            "sql": "SELECT task_key, state, attempt, error_message \
                    FROM system.orchestration.tasks WHERE task_key = 'load_daily_orders'"
        }))
        .await;
    assert_eq!(
        task_rows,
        json!([{
            "task_key": "load_daily_orders",
            "state": "DISPATCHED",
            "attempt": 2,
            "error_message": "temporary warehouse outage"
        }])
    );

    let catalog_run_rows = harness
        .query(json!({
            "sql": "SELECT task_key, task_status, attempt, attempt_id \
                    FROM system.orchestration.catalog_run_index \
                    WHERE task_key = 'load_daily_orders'"
        }))
        .await;
    assert_eq!(
        catalog_run_rows,
        json!([{
            "task_key": "load_daily_orders",
            "task_status": "DISPATCHED",
            "attempt": 2,
            "attempt_id": retry_evidence.dispatch.attempt_id.clone()
        }])
    );
    retry_evidence
}

#[derive(Debug, Clone)]
struct AcceptanceIdentity {
    tenant_id: String,
    workspace_id: String,
    other_workspace_id: String,
    run_id: String,
    plan_id: String,
    callback_base_url: String,
}

impl AcceptanceIdentity {
    fn deterministic() -> Self {
        Self {
            tenant_id: DEFAULT_TENANT.to_string(),
            workspace_id: DEFAULT_WORKSPACE.to_string(),
            other_workspace_id: DEFAULT_OTHER_WORKSPACE.to_string(),
            run_id: DEFAULT_RUN_ID.to_string(),
            plan_id: DEFAULT_PLAN_ID.to_string(),
            callback_base_url: "https://api.acceptance.arco.dev".to_string(),
        }
    }

    fn isolated(base_tenant: &str, base_workspace: &str, scenario: &str) -> Self {
        let scenario = normalize_identity_segment(scenario);
        let suffix = Ulid::new().to_string().to_ascii_lowercase();
        Self {
            tenant_id: format!("{base_tenant}-{scenario}-{suffix}"),
            workspace_id: format!("{base_workspace}-{scenario}-{suffix}"),
            other_workspace_id: format!("{base_workspace}-{scenario}-other-{suffix}"),
            run_id: format!("uat_{scenario}_{suffix}"),
            plan_id: format!("uat_plan_{scenario}_{suffix}"),
            callback_base_url: "https://api.acceptance.arco.dev".to_string(),
        }
    }

    fn evidence_json(&self) -> Value {
        json!({
            "tenantId": self.tenant_id,
            "workspaceId": self.workspace_id,
            "otherWorkspaceId": self.other_workspace_id,
            "runId": self.run_id,
            "planId": self.plan_id
        })
    }
}

fn normalize_identity_segment(segment: &str) -> String {
    let normalized: String = segment
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    normalized.trim_matches('-').to_string()
}

#[derive(Debug, Clone)]
struct LiveAcceptanceConfig {
    bucket: String,
    tenant_base: String,
    workspace_base: String,
    evidence_dir: PathBuf,
}

impl LiveAcceptanceConfig {
    fn from_env() -> Result<Self, String> {
        let vars: HashMap<String, String> = env::vars().collect();
        Self::from_vars(&vars)
    }

    fn from_vars(vars: &HashMap<String, String>) -> Result<Self, String> {
        let bucket = required_var(vars, "ARCO_UAT_STORAGE_BUCKET")?;
        Ok(Self {
            bucket,
            tenant_base: optional_var(vars, "ARCO_UAT_TENANT", "arco-uat-tenant"),
            workspace_base: optional_var(vars, "ARCO_UAT_WORKSPACE", "arco-uat-workspace"),
            evidence_dir: optional_path_var(vars, "ARCO_UAT_EVIDENCE_DIR"),
        })
    }

    fn identity(&self, scenario: &str) -> AcceptanceIdentity {
        AcceptanceIdentity::isolated(&self.tenant_base, &self.workspace_base, scenario)
    }
}

#[derive(Debug, Clone)]
struct DeployedAcceptanceConfig {
    api_base_url: String,
    api_access: DeployedApiAccess,
    bearer_token: Option<String>,
    tenant_id: String,
    workspace_id: String,
    run_timeout: StdDuration,
    evidence_dir: PathBuf,
}

impl DeployedAcceptanceConfig {
    fn from_env() -> Result<Self, String> {
        let vars: HashMap<String, String> = env::vars().collect();
        Self::from_vars(&vars)
    }

    fn from_vars(vars: &HashMap<String, String>) -> Result<Self, String> {
        let api_base_url = required_deployed_var(vars, "ARCO_UAT_API_URL")?
            .trim_end_matches('/')
            .to_string();
        let bearer_token = vars
            .get("ARCO_UAT_API_TOKEN")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let run_timeout = parse_timeout(
            vars.get("ARCO_UAT_RUN_TIMEOUT_SECS").map(String::as_str),
            300,
        )?;

        Ok(Self {
            api_base_url,
            api_access: DeployedApiAccess::from_vars(vars),
            bearer_token,
            tenant_id: optional_var(vars, "ARCO_UAT_TENANT", "arco-uat-tenant"),
            workspace_id: optional_var(vars, "ARCO_UAT_WORKSPACE", "arco-uat-workspace"),
            run_timeout,
            evidence_dir: optional_path_var(vars, "ARCO_UAT_EVIDENCE_DIR"),
        })
    }

    fn identity(scenario: &str) -> DeployedAcceptanceIdentity {
        DeployedAcceptanceIdentity::isolated(scenario)
    }
}

#[derive(Debug)]
struct DeployedSuccessProof {
    catalog_rows: Value,
    catalog_metadata_rows: Value,
    run_rows: Value,
    task_rows: Value,
    dependency_rows: Value,
    publication_rows: Value,
    partition_rows: Value,
    isolation: Value,
}

#[derive(Debug)]
struct DeployedUatError {
    stage: String,
    error: String,
}

impl DeployedUatError {
    fn new(stage: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            error: error.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct DeployedUatFailureEvidence {
    stage: String,
    error: String,
    run_id: Option<String>,
    run_rows: Option<Value>,
    task_rows: Option<Value>,
    dependency_rows: Option<Value>,
}

#[derive(Debug, Clone)]
struct DeployedApiAccess {
    mode: String,
    ingress_mode: String,
    cloud_run_service: Option<String>,
    cloud_run_project_id: Option<String>,
    cloud_run_region: Option<String>,
}

impl DeployedApiAccess {
    fn from_vars(vars: &HashMap<String, String>) -> Self {
        Self {
            mode: optional_var(vars, "ARCO_UAT_API_ACCESS_MODE", "direct-url"),
            ingress_mode: optional_var(vars, "ARCO_UAT_API_INGRESS_MODE", "not-recorded"),
            cloud_run_service: optional_string_var(vars, "ARCO_UAT_CLOUD_RUN_SERVICE"),
            cloud_run_project_id: optional_string_var(vars, "ARCO_UAT_CLOUD_RUN_PROJECT_ID"),
            cloud_run_region: optional_string_var(vars, "ARCO_UAT_CLOUD_RUN_REGION"),
        }
    }

    fn evidence_json(&self) -> Value {
        let mut access = serde_json::Map::new();
        access.insert("mode".to_string(), json!(self.mode));
        access.insert("ingressMode".to_string(), json!(self.ingress_mode));
        if let Some(service) = &self.cloud_run_service {
            access.insert("cloudRunService".to_string(), json!(service));
        }
        if let Some(project_id) = &self.cloud_run_project_id {
            access.insert("cloudRunProjectId".to_string(), json!(project_id));
        }
        if let Some(region) = &self.cloud_run_region {
            access.insert("cloudRunRegion".to_string(), json!(region));
        }
        Value::Object(access)
    }
}

#[derive(Debug, Clone)]
struct DeployedAcceptanceIdentity {
    namespace: String,
    raw_asset: String,
    prepared_asset: String,
    table: String,
    run_key: String,
    code_version_id: String,
}

impl DeployedAcceptanceIdentity {
    fn isolated(scenario: &str) -> Self {
        let scenario = normalize_identifier_segment(scenario);
        let suffix = Ulid::new().to_string().to_ascii_lowercase();
        Self {
            namespace: format!("uat_{scenario}_{suffix}"),
            raw_asset: format!("raw_orders_{suffix}"),
            prepared_asset: format!("daily_orders_prepared_{suffix}"),
            table: format!("daily_orders_{suffix}"),
            run_key: format!("uat:{scenario}:{suffix}"),
            code_version_id: format!("uat-{scenario}-{suffix}"),
        }
    }

    fn raw_asset_key(&self) -> String {
        format!("{}.{}", self.namespace, self.raw_asset)
    }

    fn prepared_asset_key(&self) -> String {
        format!("{}.{}", self.namespace, self.prepared_asset)
    }

    fn target_asset_key(&self) -> String {
        format!("{}.{}", self.namespace, self.table)
    }

    fn evidence_json(&self) -> Value {
        json!({
            "namespace": self.namespace,
            "rawAsset": self.raw_asset,
            "preparedAsset": self.prepared_asset,
            "table": self.table,
            "rawAssetKey": self.raw_asset_key(),
            "preparedAssetKey": self.prepared_asset_key(),
            "targetAssetKey": self.target_asset_key(),
            "runKey": self.run_key,
            "codeVersionId": self.code_version_id
        })
    }
}

fn required_var(vars: &HashMap<String, String>, name: &str) -> Result<String, String> {
    vars.get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("{name} is required for live UAT"))
}

fn required_deployed_var(vars: &HashMap<String, String>, name: &str) -> Result<String, String> {
    vars.get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("{name} is required for deployed UAT"))
}

fn optional_var(vars: &HashMap<String, String>, name: &str, default: &str) -> String {
    vars.get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map_or_else(|| default.to_string(), str::to_string)
}

fn optional_string_var(vars: &HashMap<String, String>, name: &str) -> Option<String> {
    vars.get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn optional_path_var(vars: &HashMap<String, String>, name: &str) -> PathBuf {
    vars.get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map_or_else(default_uat_evidence_dir, PathBuf::from)
}

fn default_uat_evidence_dir() -> PathBuf {
    PathBuf::from("target/uat-evidence")
}

fn durable_scenario_evidence(
    identity: &AcceptanceIdentity,
    assertions: &[&str],
    proof: Value,
) -> Value {
    let mut evidence = identity.evidence_json();
    let object = evidence
        .as_object_mut()
        .expect("acceptance identity evidence must be an object");
    object.insert("proof".to_string(), proof);
    object.insert("assertions".to_string(), json!(assertions));
    evidence
}

fn deployed_evidence_proof(
    identity: &DeployedAcceptanceIdentity,
    run_id: &str,
    publication_rows: &Value,
    partition_rows: &Value,
) -> Value {
    let publication = first_evidence_row(publication_rows, "catalogRunIndex");
    let partition = first_evidence_row(partition_rows, "partitionStatus");
    json!({
        "runId": run_id,
        "runKey": identity.run_key,
        "codeVersionId": identity.code_version_id,
        "taskKeys": [
            identity.raw_asset_key(),
            identity.prepared_asset_key(),
            identity.target_asset_key()
        ],
        "dependencyEdges": [
            {
                "upstreamTaskKey": identity.raw_asset_key(),
                "downstreamTaskKey": identity.prepared_asset_key()
            },
            {
                "upstreamTaskKey": identity.prepared_asset_key(),
                "downstreamTaskKey": identity.target_asset_key()
            }
        ],
        "publication": {
            "taskKey": evidence_string(publication, "task_key", "catalogRunIndex"),
            "materializationId": evidence_string(publication, "materialization_id", "catalogRunIndex"),
            "deltaTable": evidence_string(publication, "delta_table", "catalogRunIndex"),
            "deltaVersion": evidence_i64(publication, "delta_version", "catalogRunIndex"),
            "deltaPartition": evidence_string(publication, "delta_partition", "catalogRunIndex")
        },
        "partition": {
            "assetKey": evidence_string(partition, "asset_key", "partitionStatus"),
            "partitionKey": evidence_string(partition, "partition_key", "partitionStatus")
        }
    })
}

fn write_deployed_failure_evidence(
    config: &DeployedAcceptanceConfig,
    identity: &DeployedAcceptanceIdentity,
    api_version: Option<Value>,
    failure: DeployedUatFailureEvidence,
) -> Result<PathBuf, String> {
    let DeployedUatFailureEvidence {
        stage,
        error,
        run_id,
        run_rows,
        task_rows,
        dependency_rows,
    } = failure;

    let mut queries = serde_json::Map::new();
    if let Some(rows) = run_rows {
        queries.insert("runs".to_string(), rows);
    }
    if let Some(rows) = task_rows {
        queries.insert("tasks".to_string(), rows);
    }
    if let Some(rows) = dependency_rows {
        queries.insert("dependencies".to_string(), rows);
    }

    let mut artifact = serde_json::Map::new();
    artifact.insert("kind".to_string(), json!("deployed_api_worker_failure"));
    artifact.insert("recordedAt".to_string(), json!(Utc::now()));
    artifact.insert("apiBaseUrl".to_string(), json!(config.api_base_url));
    artifact.insert("apiAccess".to_string(), config.api_access.evidence_json());
    if let Some(version) = api_version {
        artifact.insert("apiVersion".to_string(), version);
    }
    artifact.insert("tenantId".to_string(), json!(config.tenant_id));
    artifact.insert("workspaceId".to_string(), json!(config.workspace_id));
    artifact.insert("identity".to_string(), identity.evidence_json());
    if let Some(run_id) = run_id {
        artifact.insert("runId".to_string(), json!(run_id));
    }
    artifact.insert(
        "failure".to_string(),
        json!({
            "stage": stage,
            "error": error
        }),
    );
    artifact.insert("queries".to_string(), Value::Object(queries));

    let path = write_uat_evidence(
        &config.evidence_dir,
        &format!("deployed api worker failure {}", identity.namespace),
        Value::Object(artifact),
    )?;
    Ok(path)
}

fn deployed_failure_test_error(
    config: &DeployedAcceptanceConfig,
    identity: &DeployedAcceptanceIdentity,
    api_version: Option<Value>,
    failure: DeployedUatFailureEvidence,
    run_id: &str,
) -> Result<(), String> {
    let error = failure.error.clone();
    let path = write_deployed_failure_evidence(config, identity, api_version, failure)?;
    Err(format!(
        "deployed UAT failed after run_id={run_id}; failure evidence: {}; error: {error}",
        path.display()
    ))
}

fn first_evidence_row<'a>(rows: &'a Value, _context: &str) -> &'a serde_json::Map<String, Value> {
    rows.as_array()
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
        .expect("evidence row")
}

fn evidence_string(row: &serde_json::Map<String, Value>, field: &str, _context: &str) -> String {
    row.get(field)
        .and_then(Value::as_str)
        .expect("evidence string")
        .to_string()
}

fn evidence_i64(row: &serde_json::Map<String, Value>, field: &str, _context: &str) -> i64 {
    row.get(field)
        .and_then(Value::as_i64)
        .expect("evidence integer")
}

fn parse_timeout(value: Option<&str>, default_secs: u64) -> Result<StdDuration, String> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(StdDuration::from_secs(default_secs));
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|_| "ARCO_UAT_RUN_TIMEOUT_SECS must be a positive integer".to_string())?;
    if seconds == 0 {
        return Err("ARCO_UAT_RUN_TIMEOUT_SECS must be a positive integer".to_string());
    }
    Ok(StdDuration::from_secs(seconds))
}

fn normalize_identifier_segment(segment: &str) -> String {
    let normalized: String = segment
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let normalized = normalized.trim_matches('_');
    if normalized.is_empty() {
        "uat".to_string()
    } else {
        normalized.to_string()
    }
}

fn deployed_isolation_workspace(
    config: &DeployedAcceptanceConfig,
    identity: &DeployedAcceptanceIdentity,
) -> String {
    format!(
        "{}-isolation-{}",
        config.workspace_id,
        normalize_identifier_segment(&identity.namespace)
    )
}

fn write_uat_evidence(
    evidence_dir: &Path,
    artifact_name: &str,
    artifact: Value,
) -> Result<PathBuf, String> {
    fs::create_dir_all(evidence_dir).map_err(|err| {
        format!(
            "failed to create UAT evidence dir {}: {err}",
            evidence_dir.display()
        )
    })?;
    let file_name = format!("{}.json", normalize_identifier_segment(artifact_name));
    let path = evidence_dir.join(file_name);
    let body = serde_json::to_string_pretty(&sanitize_uat_evidence(artifact))
        .map_err(|err| format!("failed to serialize UAT evidence JSON: {err}"))?;
    fs::write(&path, format!("{body}\n")).map_err(|err| {
        format!(
            "failed to write UAT evidence artifact {}: {err}",
            path.display()
        )
    })?;
    Ok(path)
}

fn sanitize_uat_evidence(value: Value) -> Value {
    match value {
        Value::Array(values) => {
            Value::Array(values.into_iter().map(sanitize_uat_evidence).collect())
        }
        Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .filter_map(|(key, value)| {
                    let lower = key.to_ascii_lowercase();
                    if lower.contains("token") || lower == "authorization" {
                        None
                    } else {
                        Some((key, sanitize_uat_evidence(value)))
                    }
                })
                .collect(),
        ),
        value => value,
    }
}

fn redact_storage_bucket(bucket: &str) -> String {
    bucket.split_once("://").map_or_else(
        || "<redacted>".to_string(),
        |(scheme, _)| format!("{scheme}://<redacted>"),
    )
}

fn deployed_request_error_message(
    method: &str,
    path: &str,
    url: &str,
    detail: impl AsRef<str>,
) -> String {
    format!(
        "deployed API request failed: method={method} path={path} url={url}: {}",
        detail.as_ref()
    )
}

#[derive(Clone)]
struct DeployedAcceptanceClient {
    http: reqwest::Client,
    config: DeployedAcceptanceConfig,
}

impl DeployedAcceptanceClient {
    fn new(config: DeployedAcceptanceConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    fn with_workspace(&self, workspace_id: String) -> Self {
        let mut config = self.config.clone();
        config.workspace_id = workspace_id;
        Self {
            http: self.http.clone(),
            config,
        }
    }

    async fn post_json(&self, path: &str, body: Value) -> Result<JsonResponse, String> {
        self.send_json(Method::POST, path, Some(body), &[]).await
    }

    async fn get_json(&self, path: &str) -> Result<JsonResponse, String> {
        self.send_json(Method::GET, path, None, &[]).await
    }

    async fn post_json_with_headers(
        &self,
        path: &str,
        body: Value,
        extra_headers: &[(&str, &str)],
    ) -> Result<JsonResponse, String> {
        self.send_json(Method::POST, path, Some(body), extra_headers)
            .await
    }

    async fn query(&self, sql: String) -> Result<JsonResponse, String> {
        self.post_json("/api/v1/query?format=json", json!({ "sql": sql }))
            .await
    }

    async fn poll_query_until(
        &self,
        sql: String,
        timeout: StdDuration,
        predicate: impl Fn(&Value) -> bool,
    ) -> Result<Value, String> {
        let started = tokio::time::Instant::now();

        loop {
            let last_response = match self.query(sql.clone()).await {
                Ok(response) => {
                    if response.status == StatusCode::OK && predicate(&response.body) {
                        return Ok(response.body);
                    }
                    format!("status={} body={}", response.status, response.body)
                }
                Err(err) => err,
            };

            if started.elapsed() >= timeout {
                return Err(format!(
                    "timed out polling deployed UAT query after {timeout:?}; sql={sql}; last_response={last_response}"
                ));
            }
            tokio::time::sleep(StdDuration::from_secs(2)).await;
        }
    }

    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        extra_headers: &[(&str, &str)],
    ) -> Result<JsonResponse, String> {
        let url = format!("{}{}", self.config.api_base_url, path);
        let method_name = method.as_str().to_string();
        let mut request = match method {
            Method::GET => self.http.get(&url),
            Method::POST => self.http.post(&url),
            _ => return Err(format!("unsupported deployed UAT method: {method}")),
        }
        .header("Accept", "application/json")
        .header("X-Tenant-Id", &self.config.tenant_id)
        .header("X-Workspace-Id", &self.config.workspace_id);

        if let Some(token) = self.config.bearer_token.as_deref() {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .json(&body);
        }
        for (key, value) in extra_headers {
            request = request.header(*key, *value);
        }

        let response = request.send().await.map_err(|err| {
            deployed_request_error_message(&method_name, path, &url, format!("{err:?}"))
        })?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| format!("failed to read deployed API response: {err}"))?;
        let body = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text)
                .map_err(|err| format!("failed to parse deployed API JSON: {err}; body={text}"))?
        };

        Ok(JsonResponse { status, body })
    }
}

async fn create_deployed_catalog_table(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
) {
    let namespace_response = client
        .post_json(
            "/api/v1/namespaces",
            json!({
                "name": identity.namespace,
                "description": "deployed UAT namespace"
            }),
        )
        .await
        .expect("create deployed namespace");
    assert_eq!(
        namespace_response.status,
        StatusCode::CREATED,
        "namespace response body: {}",
        namespace_response.body
    );

    for table in [
        &identity.raw_asset,
        &identity.prepared_asset,
        &identity.table,
    ] {
        let table_response = client
            .post_json(
                &format!("/api/v1/namespaces/{}/tables", identity.namespace),
                json!({
                    "name": table,
                    "description": "deployed UAT pipeline asset",
                    "columns": [
                        { "name": "order_id", "data_type": "STRING", "nullable": false },
                        { "name": "order_total", "data_type": "DOUBLE", "nullable": true }
                    ]
                }),
            )
            .await
            .expect("create deployed table");
        assert_eq!(
            table_response.status,
            StatusCode::CREATED,
            "table response body: {}",
            table_response.body
        );
    }
}

async fn create_deployed_isolation_catalog_seed(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
) {
    let namespace = format!("{}_isolation", identity.namespace);
    let namespace_response = client
        .post_json(
            "/api/v1/namespaces",
            json!({
                "name": namespace,
                "description": "deployed UAT isolation namespace"
            }),
        )
        .await
        .expect("create deployed isolation namespace");
    assert_eq!(
        namespace_response.status,
        StatusCode::CREATED,
        "isolation namespace response body: {}",
        namespace_response.body
    );

    let table_response = client
        .post_json(
            &format!("/api/v1/namespaces/{namespace}/tables"),
            json!({
                "name": "unrelated_orders",
                "description": "deployed UAT isolation asset",
                "columns": [
                    { "name": "order_id", "data_type": "STRING", "nullable": false }
                ]
            }),
        )
        .await
        .expect("create deployed isolation table");
    assert_eq!(
        table_response.status,
        StatusCode::CREATED,
        "isolation table response body: {}",
        table_response.body
    );
}

async fn deploy_deployed_pipeline_manifest(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
) {
    let idempotency_key = format!("manifest-{}", identity.run_key);
    let response = client
        .post_json_with_headers(
            &format!(
                "/api/v1/workspaces/{}/manifests",
                client.config.workspace_id
            ),
            json!({
                "manifestVersion": "1.0",
                "codeVersionId": identity.code_version_id,
                "assets": [
                    {
                        "key": { "namespace": identity.namespace, "name": identity.raw_asset },
                        "id": format!("asset-{}", identity.raw_asset),
                        "description": "raw deployed UAT asset",
                        "partitioning": daily_partitioning(),
                        "dependencies": []
                    },
                    {
                        "key": { "namespace": identity.namespace, "name": identity.prepared_asset },
                        "id": format!("asset-{}", identity.prepared_asset),
                        "description": "prepared deployed UAT asset",
                        "partitioning": daily_partitioning(),
                        "dependencies": [{
                            "upstreamKey": {
                                "namespace": identity.namespace,
                                "name": identity.raw_asset
                            }
                        }]
                    },
                    {
                        "key": { "namespace": identity.namespace, "name": identity.table },
                        "id": format!("asset-{}", identity.table),
                        "description": "published deployed UAT asset",
                        "partitioning": daily_partitioning(),
                        "dependencies": [{
                            "upstreamKey": {
                                "namespace": identity.namespace,
                                "name": identity.prepared_asset
                            }
                        }]
                    }
                ]
            }),
            &[("Idempotency-Key", idempotency_key.as_str())],
        )
        .await
        .expect("deploy manifest through deployed API");
    assert_eq!(
        response.status,
        StatusCode::CREATED,
        "manifest response body: {}",
        response.body
    );
}

async fn trigger_deployed_pipeline_run(
    client: &DeployedAcceptanceClient,
    identity: &DeployedAcceptanceIdentity,
) -> String {
    let response = client
        .post_json(
            &format!("/api/v1/workspaces/{}/runs", client.config.workspace_id),
            json!({
                "selection": [identity.target_asset_key()],
                "includeUpstream": true,
                "partitionKey": "date=d:2026-05-31",
                "runKey": identity.run_key,
                "labels": {
                    "acceptance_suite": "deployed_pipeline_uat",
                    "acceptance_namespace": identity.namespace
                }
            }),
        )
        .await
        .expect("trigger deployed run");
    assert_eq!(
        response.status,
        StatusCode::CREATED,
        "trigger response body: {}",
        response.body
    );
    response
        .body
        .get("runId")
        .and_then(Value::as_str)
        .expect("run id")
        .to_string()
}

fn has_rows(value: &Value) -> bool {
    value.as_array().is_some_and(|rows| !rows.is_empty())
}

async fn assert_deployed_empty_query(
    client: &DeployedAcceptanceClient,
    sql: String,
    context: &str,
) -> Result<Value, String> {
    let response = client
        .query(sql)
        .await
        .map_err(|err| format!("{context} query failed: {err}"))?;
    if response.status != StatusCode::OK {
        return Err(format!(
            "{context} response status={} body: {}",
            response.status, response.body
        ));
    }
    if response.body != json!([]) {
        return Err(format!("{context} leaked rows: {}", response.body));
    }
    Ok(response.body)
}

fn deployed_run_succeeded_with_code_version(code_version_id: &str) -> impl Fn(&Value) -> bool + '_ {
    move |value: &Value| {
        let Some(rows) = value.as_array() else {
            return false;
        };
        let Some(row) = rows.first() else {
            return false;
        };
        rows.len() == 1
            && row.get("state").and_then(Value::as_str) == Some("SUCCEEDED")
            && row.get("code_version").and_then(Value::as_str) == Some(code_version_id)
    }
}

fn deployed_catalog_metadata_matches(
    identity: &DeployedAcceptanceIdentity,
) -> impl Fn(&Value) -> bool + '_ {
    move |value: &Value| value == &deployed_catalog_metadata_rows(identity)
}

fn deployed_catalog_metadata_rows(identity: &DeployedAcceptanceIdentity) -> Value {
    let rows: Vec<Value> = [
        identity.table.as_str(),
        identity.prepared_asset.as_str(),
        identity.raw_asset.as_str(),
    ]
    .into_iter()
    .flat_map(|table| {
        [
            json!({
                "namespace_name": identity.namespace,
                "table_name": table,
                "column_name": "order_id",
                "data_type": "STRING",
                "is_nullable": false,
                "ordinal": 0
            }),
            json!({
                "namespace_name": identity.namespace,
                "table_name": table,
                "column_name": "order_total",
                "data_type": "DOUBLE",
                "is_nullable": true,
                "ordinal": 1
            }),
        ]
    })
    .collect();
    Value::Array(rows)
}

fn all_deployed_pipeline_tasks_succeeded(value: &Value) -> bool {
    let Some(rows) = value.as_array() else {
        return false;
    };
    rows.len() == 3
        && rows
            .iter()
            .all(|row| row.get("state").and_then(Value::as_str) == Some("SUCCEEDED"))
}

fn deployed_dependency_edges_satisfied(value: &Value) -> bool {
    let Some(rows) = value.as_array() else {
        return false;
    };
    rows.len() == 2
        && rows.iter().all(|row| {
            row.get("satisfied").and_then(Value::as_bool) == Some(true)
                && row.get("resolution").and_then(Value::as_str) == Some("SUCCESS")
        })
}

fn deployed_publication_materialized(value: &Value) -> bool {
    let Some(rows) = value.as_array() else {
        return false;
    };
    rows.len() == 1
        && rows.iter().all(|row| {
            row.get("task_status").and_then(Value::as_str) == Some("SUCCEEDED")
                && row
                    .get("materialization_id")
                    .and_then(Value::as_str)
                    .is_some()
                && row.get("output_visibility_state").and_then(Value::as_str) == Some("VISIBLE")
                && row.get("delta_table").and_then(Value::as_str).is_some()
                && row.get("delta_version").and_then(Value::as_i64).is_some()
                && row.get("delta_partition").and_then(Value::as_str).is_some()
                && row
                    .get("execution_lineage_ref")
                    .and_then(Value::as_str)
                    .is_some()
        })
}

fn deployed_partition_status_materialized(value: &Value) -> bool {
    let Some(rows) = value.as_array() else {
        return false;
    };
    rows.len() == 1
        && rows.iter().all(|row| {
            row.get("last_attempt_outcome").and_then(Value::as_str) == Some("SUCCEEDED")
                && row
                    .get("last_materialization_run_id")
                    .and_then(Value::as_str)
                    .is_some()
                && row
                    .get("last_attempt_run_id")
                    .and_then(Value::as_str)
                    .is_some()
                && row.get("delta_table").and_then(Value::as_str).is_some()
                && row.get("delta_version").and_then(Value::as_i64).is_some()
                && row.get("delta_partition").and_then(Value::as_str).is_some()
                && row
                    .get("execution_lineage_ref")
                    .and_then(Value::as_str)
                    .is_some()
        })
}

fn daily_partitioning() -> Value {
    json!({
        "isPartitioned": true,
        "dimensions": [{
            "name": "date",
            "kind": "time",
            "granularity": "day"
        }]
    })
}

fn pipeline_manifest_assets(tasks: &[PipelineTask]) -> Vec<Value> {
    tasks.iter().map(pipeline_manifest_asset).collect()
}

fn pipeline_manifest_asset(task: &PipelineTask) -> Value {
    let (namespace, name) = asset_key_parts(&task.asset_key);
    json!({
        "key": { "namespace": namespace, "name": name },
        "id": format!("asset_{}", normalize_identifier_segment(&task.asset_key)),
        "description": format!("{} acceptance asset", task.asset_key),
        "partitioning": daily_partitioning(),
        "dependencies": pipeline_manifest_dependencies(task)
    })
}

fn pipeline_manifest_dependencies(task: &PipelineTask) -> Vec<Value> {
    task.depends_on
        .iter()
        .map(|dependency| {
            let (upstream_namespace, upstream_name) = asset_key_parts(dependency);
            json!({
                "upstreamKey": {
                    "namespace": upstream_namespace,
                    "name": upstream_name
                }
            })
        })
        .collect()
}

struct AcceptanceSensorEvaluator;

impl SensorEvaluator for AcceptanceSensorEvaluator {
    fn evaluate_push(
        &self,
        sensor_id: &str,
        message: &PubSubMessage,
    ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
        Ok(vec![RunRequest {
            run_key: format!("sensor:{sensor_id}:msg:{}", message.message_id),
            request_fingerprint: format!("fp:sensor:{sensor_id}:{}", message.message_id),
            asset_selection: vec!["analytics.daily_orders".to_string()],
            partition_selection: Some(vec!["date=d:2026-05-31".to_string()]),
            code_version: Some("uat-sensor-v1".to_string()),
        }])
    }

    fn evaluate_poll(
        &self,
        _sensor_id: &str,
        cursor_before: Option<&str>,
    ) -> Result<PollSensorResult, SensorEvaluationError> {
        Ok(PollSensorResult {
            cursor_after: cursor_before.map(str::to_string),
            run_requests: Vec::new(),
        })
    }
}

#[derive(Clone)]
struct AcceptanceHarness {
    router: axum::Router,
    storage: ScopedStorage,
    ledger: LedgerWriter,
    compactor: MicroCompactor,
    token_config: TaskTokenConfig,
    identity: AcceptanceIdentity,
}

impl AcceptanceHarness {
    fn new() -> Self {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        Self::with_backend(backend, AcceptanceIdentity::deterministic())
    }

    fn with_sensor_evaluator(evaluator: Arc<dyn SensorEvaluator>) -> Self {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        Self::with_backend_and_sensor_evaluator(
            backend,
            AcceptanceIdentity::deterministic(),
            evaluator,
        )
    }

    fn with_backend(backend: Arc<dyn StorageBackend>, identity: AcceptanceIdentity) -> Self {
        Self::build(backend, identity, None)
    }

    fn with_backend_and_sensor_evaluator(
        backend: Arc<dyn StorageBackend>,
        identity: AcceptanceIdentity,
        evaluator: Arc<dyn SensorEvaluator>,
    ) -> Self {
        Self::build(backend, identity, Some(evaluator))
    }

    fn build(
        backend: Arc<dyn StorageBackend>,
        identity: AcceptanceIdentity,
        evaluator: Option<Arc<dyn SensorEvaluator>>,
    ) -> Self {
        let mut builder = ServerBuilder::new()
            .debug(true)
            .storage_backend(backend.clone());
        if let Some(evaluator) = evaluator {
            builder = builder.sensor_evaluator(evaluator);
        }
        let router = builder.build().test_router();
        let storage = ScopedStorage::new(backend, &identity.tenant_id, &identity.workspace_id)
            .expect("scoped storage");
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage.clone());
        let token_config = TaskTokenConfig {
            hs256_secret: "acceptance-task-secret".to_string(),
            issuer: Some("https://issuer.acceptance".to_string()),
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 900,
        };

        Self {
            router,
            storage,
            ledger,
            compactor,
            token_config,
            identity,
        }
    }

    fn other_workspace(&self) -> &str {
        &self.identity.other_workspace_id
    }

    async fn create_catalog_table(&self, namespace: &str, table: &str) {
        self.create_catalog_table_as(&self.identity.workspace_id, namespace, table)
            .await;
    }

    async fn create_catalog_namespace(&self, namespace: &str) {
        self.create_catalog_namespace_as(&self.identity.workspace_id, namespace)
            .await;
    }

    async fn create_catalog_table_in_existing_namespace(&self, namespace: &str, table: &str) {
        self.create_catalog_table_in_existing_namespace_as(
            &self.identity.workspace_id,
            namespace,
            table,
        )
        .await;
    }

    async fn create_catalog_table_as(&self, workspace: &str, namespace: &str, table: &str) {
        self.create_catalog_namespace_as(workspace, namespace).await;
        self.create_catalog_table_in_existing_namespace_as(workspace, namespace, table)
            .await;
    }

    async fn create_catalog_namespace_as(&self, workspace: &str, namespace: &str) {
        let namespace_response = self
            .send_json_as(
                workspace,
                Method::POST,
                "/api/v1/namespaces",
                json!({
                    "name": namespace,
                    "description": format!("{namespace} namespace")
                }),
            )
            .await;
        assert_eq!(
            namespace_response.status,
            StatusCode::CREATED,
            "namespace response body: {}",
            namespace_response.body
        );
    }

    async fn create_catalog_table_in_existing_namespace_as(
        &self,
        workspace: &str,
        namespace: &str,
        table: &str,
    ) {
        let table_response = self
            .send_json_as(
                workspace,
                Method::POST,
                &format!("/api/v1/namespaces/{namespace}/tables"),
                json!({
                    "name": table,
                    "description": format!("{table} acceptance target"),
                    "columns": [
                        { "name": "order_id", "data_type": "STRING", "nullable": false },
                        { "name": "order_total", "data_type": "DOUBLE", "nullable": true }
                    ]
                }),
            )
            .await;
        assert_eq!(
            table_response.status,
            StatusCode::CREATED,
            "table response body: {}",
            table_response.body
        );
    }

    async fn run_pipeline(&self, tasks: Vec<PipelineTask>) -> PipelineRun {
        let run = self.deploy_and_trigger_pipeline_run(&tasks).await;

        for (index, task) in tasks.iter().enumerate() {
            Box::pin(self.run_ready_task(
                &run.run_id,
                task,
                i64::try_from(index + 1).expect("delta version"),
            ))
            .await;
        }

        run
    }

    async fn deploy_and_trigger_pipeline_run(&self, tasks: &[PipelineTask]) -> PipelineRun {
        let deploy_response = self
            .send_json_as(
                &self.identity.workspace_id,
                Method::POST,
                &format!(
                    "/api/v1/workspaces/{}/manifests",
                    self.identity.workspace_id
                ),
                json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "uat-pipeline-v1",
                    "assets": pipeline_manifest_assets(tasks)
                }),
            )
            .await;
        assert_eq!(
            deploy_response.status,
            StatusCode::CREATED,
            "manifest response body: {}",
            deploy_response.body
        );

        let root_asset = tasks
            .last()
            .map(|task| task.asset_key.as_str())
            .expect("pipeline has a root asset");
        let run_key = format!("uat-cataloged-pipeline-{}", self.identity.run_id);
        let trigger_response = self.trigger_pipeline_run(root_asset, &run_key).await;
        assert_eq!(
            trigger_response.status,
            StatusCode::CREATED,
            "trigger response body: {}",
            trigger_response.body
        );

        PipelineRun {
            run_id: trigger_response
                .body
                .get("runId")
                .and_then(Value::as_str)
                .expect("run id")
                .to_string(),
            plan_id: trigger_response
                .body
                .get("planId")
                .and_then(Value::as_str)
                .expect("plan id")
                .to_string(),
            run_key: trigger_response
                .body
                .get("runKey")
                .and_then(Value::as_str)
                .map_or_else(
                    || format!("uat-cataloged-pipeline-{}", self.identity.run_id),
                    str::to_string,
                ),
        }
    }

    async fn trigger_pipeline_run(&self, root_asset: &str, run_key: &str) -> JsonResponse {
        self.send_json_as(
            &self.identity.workspace_id,
            Method::POST,
            &format!("/api/v1/workspaces/{}/runs", self.identity.workspace_id),
            json!({
                "selection": [root_asset],
                "includeUpstream": true,
                "partitionKey": "date=d:2026-05-31",
                "runKey": run_key,
                "labels": {
                    "acceptance_suite": "cataloged_pipeline"
                }
            }),
        )
        .await
    }

    async fn seed_pipeline_run(&self, tasks: Vec<PipelineTask>) {
        self.seed_orchestration_manifest().await;

        let root_assets = tasks
            .last()
            .map(|task| vec![task.asset_key.clone()])
            .unwrap_or_default();
        let run_triggered = OrchestrationEvent::new(
            self.identity.tenant_id.clone(),
            self.identity.workspace_id.clone(),
            OrchestrationEventData::RunTriggered {
                run_id: self.identity.run_id.clone(),
                plan_id: self.identity.plan_id.clone(),
                trigger: TriggerInfo::Manual {
                    user_id: "acceptance-user@example.com".to_string(),
                },
                root_assets,
                run_key: Some(format!("{}-daily-orders-2026-05-31", self.identity.run_id)),
                labels: HashMap::from([(
                    "acceptance_suite".to_string(),
                    "cataloged_pipeline".to_string(),
                )]),
                code_version: Some("uat-v1".to_string()),
            },
        );
        let plan_created = OrchestrationEvent::new(
            self.identity.tenant_id.clone(),
            self.identity.workspace_id.clone(),
            OrchestrationEventData::PlanCreated {
                run_id: self.identity.run_id.clone(),
                plan_id: self.identity.plan_id.clone(),
                tasks: tasks
                    .iter()
                    .map(|task| TaskDef {
                        key: task.key.clone(),
                        depends_on: task.depends_on.clone(),
                        asset_key: Some(task.asset_key.clone()),
                        partition_key: Some("2026-05-31".to_string()),
                        max_attempts: task.max_attempts,
                        heartbeat_timeout_sec: 300,
                        requires_visible_output: false,
                    })
                    .collect(),
            },
        );

        self.append_and_compact(vec![run_triggered, plan_created])
            .await;
    }

    async fn deploy_and_tick_daily_orders_schedule(&self) -> String {
        self.seed_orchestration_manifest().await;

        let deploy_response = self
            .send_json_as(
                &self.identity.workspace_id,
                Method::POST,
                &format!(
                    "/api/v1/workspaces/{}/manifests",
                    self.identity.workspace_id
                ),
                json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "uat-schedule-v1",
                    "assets": [{
                        "key": { "namespace": "analytics", "name": "daily_orders" },
                        "id": "01JACCEPTDAILYORDERS",
                        "description": "Daily orders acceptance asset"
                    }],
                    "schedules": [{
                        "id": "daily-orders",
                        "cron": "0 0 * * *",
                        "assets": ["analytics/daily_orders"],
                        "timezone": "UTC"
                    }]
                }),
            )
            .await;
        assert_eq!(
            deploy_response.status,
            StatusCode::CREATED,
            "manifest response body: {}",
            deploy_response.body
        );

        let (_, state) = self.compactor.load_state().await.expect("load state");
        let definition = state
            .schedule_definitions
            .get("daily-orders")
            .expect("daily-orders schedule definition")
            .clone();
        let scheduled_for = Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap();
        let events = ScheduleController::new().reconcile(&[definition], &[], scheduled_for);
        assert!(
            events
                .iter()
                .any(|event| matches!(&event.data, OrchestrationEventData::ScheduleTicked { .. })),
            "expected schedule tick event"
        );

        self.append_and_compact(events).await;

        let (_, state) = self.compactor.load_state().await.expect("load state");
        let run_request_events = RunRequestProcessor::new().reconcile(&state);
        assert!(
            run_request_events
                .iter()
                .any(|event| matches!(&event.data, OrchestrationEventData::RunTriggered { .. })),
            "expected schedule run request processor to emit RunTriggered"
        );
        self.append_and_compact(run_request_events).await;

        format!("daily-orders:{}", scheduled_for.timestamp())
    }

    async fn request_and_plan_daily_orders_backfill(&self) -> String {
        self.seed_orchestration_manifest().await;

        let deploy_response = self
            .send_json_as(
                &self.identity.workspace_id,
                Method::POST,
                &format!(
                    "/api/v1/workspaces/{}/manifests",
                    self.identity.workspace_id
                ),
                json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "uat-backfill-v1",
                    "assets": [{
                        "key": { "namespace": "analytics", "name": "daily_orders" },
                        "id": "01JACCEPTBACKFILLORDERS",
                        "description": "Daily orders backfill acceptance asset",
                        "partitioning": daily_partitioning()
                    }]
                }),
            )
            .await;
        assert_eq!(
            deploy_response.status,
            StatusCode::CREATED,
            "manifest response body: {}",
            deploy_response.body
        );

        let response = self
            .send_json_as(
                &self.identity.workspace_id,
                Method::POST,
                &format!(
                    "/api/v1/workspaces/{}/backfills",
                    self.identity.workspace_id
                ),
                json!({
                    "assetSelection": ["analytics.daily_orders"],
                    "partitionSelector": {
                        "type": "range",
                        "start": "2026-05-28",
                        "end": "2026-05-31"
                    },
                    "clientRequestId": "uat-backfill-daily-orders",
                    "chunkSize": 2,
                    "maxConcurrentRuns": 2
                }),
            )
            .await;
        assert_eq!(
            response.status,
            StatusCode::ACCEPTED,
            "backfill response body: {}",
            response.body
        );
        let backfill_id = response
            .body
            .get("backfillId")
            .and_then(Value::as_str)
            .expect("backfill id")
            .to_string();

        let (manifest, state) = self.compactor.load_state().await.expect("load state");
        let controller = BackfillController::new(Arc::new(DailyPartitionResolver));
        let events = controller.reconcile(
            &manifest.watermarks,
            &state.backfills,
            &state.backfill_chunks,
            &state.runs,
        );
        assert!(
            events.iter().any(|event| matches!(
                &event.data,
                OrchestrationEventData::BackfillChunkPlanned { .. }
            )),
            "expected backfill chunk planning event"
        );

        self.append_and_compact(events).await;

        let (_, state) = self.compactor.load_state().await.expect("load state");
        let run_request_events = RunRequestProcessor::new().reconcile(&state);
        assert!(
            run_request_events
                .iter()
                .filter(|event| matches!(&event.data, OrchestrationEventData::RunTriggered { .. }))
                .count()
                >= 2,
            "expected backfill chunks to emit RunTriggered events"
        );
        self.append_and_compact(run_request_events).await;

        backfill_id
    }

    async fn evaluate_daily_orders_sensor(&self) -> String {
        self.seed_orchestration_manifest().await;

        let response = self
            .send_json_as(
                &self.identity.workspace_id,
                Method::POST,
                &format!(
                    "/api/v1/workspaces/{}/sensors/daily-orders-sensor/evaluate",
                    self.identity.workspace_id
                ),
                json!({
                    "messageId": "uat-sensor-message-01",
                    "payload": {
                        "asset": "analytics.daily_orders",
                        "partition": "date=d:2026-05-31"
                    },
                    "attributes": {
                        "source": "uat"
                    }
                }),
            )
            .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "sensor evaluate response body: {}",
            response.body
        );
        assert_eq!(
            response
                .body
                .get("runRequests")
                .and_then(Value::as_array)
                .and_then(|requests| requests.first())
                .and_then(|request| request.get("codeVersion"))
                .and_then(Value::as_str),
            Some("uat-sensor-v1")
        );

        let (_, state) = self.compactor.load_state().await.expect("load state");
        let run_request_events = RunRequestProcessor::new().reconcile(&state);
        assert!(
            run_request_events
                .iter()
                .any(|event| matches!(&event.data, OrchestrationEventData::RunTriggered { .. })),
            "expected sensor run request processor to emit RunTriggered"
        );
        self.append_and_compact(run_request_events).await;

        "sensor:daily-orders-sensor:msg:uat-sensor-message-01".to_string()
    }

    async fn fail_and_request_retry(&self, task: &PipelineTask) -> RetryWorkflowEvidence {
        let dispatch = Box::pin(self.request_ready_dispatch(&self.identity.run_id, task)).await;
        let worker_dispatch =
            Box::pin(self.enqueue_dispatch(&self.identity.run_id, task, &dispatch)).await;
        let callback = Box::pin(self.complete_worker_task_through_public_api(
            task,
            worker_dispatch,
            TaskCompletion::Failed {
                message: "temporary warehouse outage".to_string(),
            },
        ))
        .await;

        let (manifest, state) = self.compactor.load_state().await.expect("load state");
        let task_row = state
            .tasks
            .get(&(self.identity.run_id.clone(), task.key.clone()))
            .expect("retryable failed task");
        let retry = RetryHandler::with_defaults().fire(&manifest.watermarks, task_row, Utc::now());
        let (run_id, task_key, new_attempt, new_attempt_id) = match retry {
            RetryAction::IncrementAttempt {
                run_id,
                task_key,
                new_attempt,
                new_attempt_id,
            } => Some((run_id, task_key, new_attempt, new_attempt_id)),
            RetryAction::Reschedule { .. } | RetryAction::NoOp { .. } => None,
        }
        .expect("expected retry attempt increment");

        let dispatch_id = dispatch_internal_id(&run_id, &task_key, new_attempt);
        let retry_dispatch = ReadyDispatch {
            attempt: new_attempt,
            attempt_id: new_attempt_id,
            worker_queue: "default-queue".to_string(),
            dispatch_id,
        };

        self.append_and_compact(vec![OrchestrationEvent::new(
            self.identity.tenant_id.clone(),
            self.identity.workspace_id.clone(),
            OrchestrationEventData::DispatchRequested {
                run_id,
                task_key,
                attempt: retry_dispatch.attempt,
                attempt_id: retry_dispatch.attempt_id.clone(),
                worker_queue: retry_dispatch.worker_queue.clone(),
                dispatch_id: retry_dispatch.dispatch_id.clone(),
            },
        )])
        .await;

        RetryWorkflowEvidence {
            task_key: task.key.clone(),
            callback,
            dispatch: retry_dispatch,
        }
    }

    async fn query(&self, body: Value) -> Value {
        self.query_as(&self.identity.workspace_id, body).await
    }

    async fn query_as(&self, workspace: &str, body: Value) -> Value {
        let response = self
            .send_json_as(workspace, Method::POST, "/api/v1/query?format=json", body)
            .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "query response body: {}",
            response.body
        );
        response.body
    }

    async fn seed_orchestration_manifest(&self) {
        let manifest = OrchestrationManifest::new(Ulid::new().to_string());
        let manifest_path = format!(
            "state/orchestration/manifests/{}.json",
            manifest.manifest_id
        );
        self.storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).expect("serialize manifest")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write orchestration manifest");

        let pointer = OrchestrationManifestPointer {
            manifest_id: manifest.manifest_id,
            manifest_path,
            epoch: 0,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        self.storage
            .put_raw(
                orchestration_manifest_pointer_path(),
                Bytes::from(serde_json::to_vec(&pointer).expect("serialize manifest pointer")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write orchestration manifest pointer");
    }

    async fn append_and_compact(&self, events: Vec<OrchestrationEvent>) {
        let paths: Vec<_> = events.iter().map(LedgerWriter::event_path).collect();
        self.ledger.append_all(events).await.expect("append events");
        self.compactor
            .compact_events(paths)
            .await
            .expect("compact events");
    }

    async fn run_ready_task(&self, run_id: &str, task: &PipelineTask, delta_version: i64) {
        let dispatch = Box::pin(self.request_ready_dispatch(run_id, task)).await;
        let worker_dispatch = Box::pin(self.enqueue_dispatch(run_id, task, &dispatch)).await;
        Box::pin(self.complete_worker_task(task, worker_dispatch, delta_version)).await;
    }

    async fn request_ready_dispatch(
        &self,
        expected_run_id: &str,
        task: &PipelineTask,
    ) -> ReadyDispatch {
        let (manifest, state) = self.compactor.load_state().await.expect("load state");
        let ready_actions = ReadyDispatchController::with_defaults().reconcile(&manifest, &state);
        let dispatch_request = ready_actions
            .into_iter()
            .find_map(|action| match action {
                ReadyDispatchAction::EmitDispatchRequested {
                    run_id,
                    task_key,
                    attempt,
                    attempt_id,
                    worker_queue,
                    dispatch_id,
                } => Some((
                    run_id,
                    task_key,
                    attempt,
                    attempt_id,
                    worker_queue,
                    dispatch_id,
                )),
                ReadyDispatchAction::Skip { .. } => None,
            })
            .expect("ready dispatch action");
        let (run_id, task_key, attempt, attempt_id, worker_queue, dispatch_id) = dispatch_request;
        assert_eq!(run_id, expected_run_id);
        assert_eq!(task_key, task.key);

        self.append_and_compact(vec![OrchestrationEvent::new(
            self.identity.tenant_id.clone(),
            self.identity.workspace_id.clone(),
            OrchestrationEventData::DispatchRequested {
                run_id: run_id.clone(),
                task_key: task_key.clone(),
                attempt,
                attempt_id: attempt_id.clone(),
                worker_queue: worker_queue.clone(),
                dispatch_id: dispatch_id.clone(),
            },
        )])
        .await;

        ReadyDispatch {
            attempt,
            attempt_id,
            worker_queue,
            dispatch_id,
        }
    }

    async fn enqueue_dispatch(
        &self,
        expected_run_id: &str,
        task: &PipelineTask,
        dispatch: &ReadyDispatch,
    ) -> WorkerDispatch {
        let (manifest, state) = self.compactor.load_state().await.expect("load state");
        let outbox_rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();
        let dispatch_actions =
            DispatcherController::with_defaults().reconcile(&manifest, &outbox_rows);
        let cloud_task_id = dispatch_actions
            .into_iter()
            .find_map(|action| match action {
                DispatchAction::CreateCloudTask {
                    dispatch_id: action_dispatch_id,
                    cloud_task_id,
                    run_id: action_run_id,
                    task_key: action_task_key,
                    attempt: action_attempt,
                    attempt_id: action_attempt_id,
                    worker_queue: action_worker_queue,
                } => {
                    assert_eq!(action_dispatch_id, dispatch.dispatch_id);
                    assert_eq!(action_run_id, expected_run_id);
                    assert_eq!(action_task_key, task.key);
                    assert_eq!(action_attempt, dispatch.attempt);
                    assert_eq!(action_attempt_id, dispatch.attempt_id);
                    assert_eq!(action_worker_queue, dispatch.worker_queue);
                    Some(cloud_task_id)
                }
                DispatchAction::Skip { .. } => None,
            })
            .expect("cloud task dispatch action");

        let callback_task_id = callback_task_id(expected_run_id, &task.key);
        let minted = mint_task_token_for_attempt(
            &self.token_config,
            &callback_task_id,
            &self.identity.tenant_id,
            &self.identity.workspace_id,
            expected_run_id,
            dispatch.attempt,
            &dispatch.attempt_id,
            Utc::now(),
        )
        .expect("mint task token");
        let envelope = WorkerDispatchEnvelope {
            tenant_id: self.identity.tenant_id.clone(),
            workspace_id: self.identity.workspace_id.clone(),
            task_id: callback_task_id,
            task_key: task.key.clone(),
            run_id: expected_run_id.to_string(),
            attempt: dispatch.attempt,
            attempt_id: dispatch.attempt_id.clone(),
            dispatch_id: dispatch.dispatch_id.clone(),
            execution_location_id: None,
            worker_queue: dispatch.worker_queue.clone(),
            callback_base_url: self.identity.callback_base_url.clone(),
            task_token: minted.token,
            token_expires_at: minted.expires_at,
            traceparent: None,
            payload: json!({
                "assetKey": task.asset_key,
                "pipeline": "daily_orders"
            }),
        };
        assert_eq!(envelope.task_key, task.key);
        assert!(parse_callback_task_id(&envelope.task_id).is_ok());

        self.append_and_compact(vec![OrchestrationEvent::new(
            self.identity.tenant_id.clone(),
            self.identity.workspace_id.clone(),
            OrchestrationEventData::DispatchEnqueued {
                dispatch_id: dispatch.dispatch_id.clone(),
                run_id: Some(expected_run_id.to_string()),
                task_key: Some(task.key.clone()),
                attempt: Some(dispatch.attempt),
                cloud_task_id,
            },
        )])
        .await;

        WorkerDispatch { envelope }
    }

    async fn complete_worker_task(
        &self,
        task: &PipelineTask,
        worker_dispatch: WorkerDispatch,
        delta_version: i64,
    ) {
        self.complete_worker_task_with(
            task,
            worker_dispatch,
            TaskCompletion::Succeeded { delta_version },
        )
        .await;
    }

    async fn complete_worker_task_with(
        &self,
        task: &PipelineTask,
        worker_dispatch: WorkerDispatch,
        completion: TaskCompletion,
    ) {
        let evidence = self
            .complete_worker_task_through_public_api(task, worker_dispatch, completion)
            .await;
        assert_eq!(evidence.started_status, StatusCode::OK);
        assert_json_bool(&evidence.started_body, "acknowledged", true);
        assert_eq!(evidence.completed_status, StatusCode::OK);
        assert_json_bool(&evidence.completed_body, "acknowledged", true);
    }

    async fn complete_worker_task_through_public_api(
        &self,
        task: &PipelineTask,
        worker_dispatch: WorkerDispatch,
        completion: TaskCompletion,
    ) -> WorkerCallbackEvidence {
        let envelope = worker_dispatch.envelope;

        let started_body = serde_json::to_value(TaskStartedRequest {
            attempt: envelope.attempt,
            attempt_id: envelope.attempt_id.clone(),
            worker_id: "acceptance-worker-1".to_string(),
            traceparent: None,
            started_at: Some(Utc::now()),
        })
        .expect("serialize task started request");
        let started = self
            .send_worker_json(
                Method::POST,
                &format!("/api/v1/tasks/{}/started", envelope.task_id),
                &envelope.task_token,
                started_body,
            )
            .await;

        let (outcome, output, error) =
            completion.into_callback_parts(task, &self.identity.workspace_id);
        let completed_body = serde_json::to_value(TaskCompletedRequest {
            attempt: envelope.attempt,
            attempt_id: envelope.attempt_id,
            worker_id: "acceptance-worker-1".to_string(),
            traceparent: None,
            outcome,
            completed_at: Some(Utc::now()),
            output,
            error,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        })
        .expect("serialize task completed request");
        let completed = self
            .send_worker_json(
                Method::POST,
                &format!("/api/v1/tasks/{}/completed", envelope.task_id),
                &envelope.task_token,
                completed_body,
            )
            .await;

        WorkerCallbackEvidence {
            started_status: started.status,
            started_body: started.body,
            completed_status: completed.status,
            completed_body: completed.body,
        }
    }

    async fn send_json_as(
        &self,
        workspace: &str,
        method: Method,
        uri: &str,
        body: Value,
    ) -> JsonResponse {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header("X-Tenant-Id", &self.identity.tenant_id)
            .header("X-Workspace-Id", workspace)
            .body(Body::from(body.to_string()))
            .expect("build request");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("route response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read response body");
        let body = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).expect("parse response JSON")
        };
        JsonResponse { status, body }
    }

    async fn send_worker_json(
        &self,
        method: Method,
        uri: &str,
        task_token: &str,
        body: Value,
    ) -> JsonResponse {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {task_token}"))
            .header("X-Tenant-Id", &self.identity.tenant_id)
            .header("X-Workspace-Id", &self.identity.workspace_id)
            .body(Body::from(body.to_string()))
            .expect("build worker callback request");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("worker route response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read worker response body");
        let body = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).expect("parse worker response JSON")
        };
        JsonResponse { status, body }
    }
}

#[derive(Debug)]
struct JsonResponse {
    status: StatusCode,
    body: Value,
}

impl JsonResponse {
    fn expect_success(self, context: &str) -> Value {
        assert!(
            self.status.is_success(),
            "{context} failed: status={} body={}",
            self.status,
            self.body
        );
        self.body
    }
}

fn assert_json_bool(body: &Value, field: &str, expected: bool) {
    assert_eq!(
        body.get(field).and_then(Value::as_bool),
        Some(expected),
        "unexpected boolean field {field} in response body {body}"
    );
}

fn assert_json_str(body: &Value, field: &str, expected: &str) {
    assert_eq!(
        body.get(field).and_then(Value::as_str),
        Some(expected),
        "unexpected string field {field} in response body {body}"
    );
}

struct PipelineRun {
    run_id: String,
    plan_id: String,
    run_key: String,
}

struct ScheduleWorkflowEvidence {
    tick_id: String,
    run_key: String,
}

struct BackfillWorkflowEvidence {
    backfill_id: String,
    chunk_run_keys: Vec<String>,
}

struct SensorWorkflowEvidence {
    sensor_id: String,
    run_key: String,
}

#[derive(Debug, Clone)]
struct PipelineTask {
    key: String,
    asset_key: String,
    depends_on: Vec<String>,
    max_attempts: u32,
}

impl PipelineTask {
    fn new(key: &str, asset_key: &str, depends_on: Vec<&str>) -> Self {
        Self {
            key: key.to_string(),
            asset_key: asset_key.to_string(),
            depends_on: depends_on.into_iter().map(str::to_string).collect(),
            max_attempts: 1,
        }
    }

    fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

fn asset_key_parts(asset_key: &str) -> (&str, &str) {
    asset_key
        .split_once('.')
        .expect("asset key must be namespace.name")
}

fn execution_lineage_ref(
    run_id: &str,
    task_key: &str,
    materialization_id: &str,
    delta_version: i64,
) -> String {
    format!(
        "{{\"runId\":\"{run_id}\",\"taskKey\":\"{task_key}\",\"attempt\":1,\
         \"materializationId\":\"{materialization_id}\",\
         \"deltaTable\":\"{task_key}\",\"deltaVersion\":{delta_version},\
         \"deltaPartition\":\"date=2026-05-31\"}}"
    )
}

enum TaskCompletion {
    Succeeded { delta_version: i64 },
    Failed { message: String },
}

impl TaskCompletion {
    fn into_callback_parts(
        self,
        task: &PipelineTask,
        workspace: &str,
    ) -> (WorkerOutcome, Option<TaskOutput>, Option<TaskError>) {
        match self {
            Self::Succeeded { delta_version } => (
                WorkerOutcome::Succeeded,
                Some(TaskOutput {
                    materialization_id: Some(format!("mat_{}", task.key)),
                    row_count: Some(100),
                    byte_size: Some(4096),
                    output_path: Some(format!("gs://acceptance/{workspace}/{}", task.key)),
                    delta_table: Some(task.asset_key.clone()),
                    delta_version: Some(delta_version),
                    delta_partition: Some("date=2026-05-31".to_string()),
                    output_visibility_state: Some(TaskOutputVisibilityState::Visible),
                    published_at: Some(Utc::now()),
                    publish_error: None,
                }),
                None,
            ),
            Self::Failed { message } => (
                WorkerOutcome::Failed,
                None,
                Some(TaskError {
                    category: ErrorCategory::Infrastructure,
                    message,
                    stack_trace: None,
                    retryable: Some(true),
                }),
            ),
        }
    }
}

struct ReadyDispatch {
    attempt: u32,
    attempt_id: String,
    worker_queue: String,
    dispatch_id: String,
}

struct WorkerDispatch {
    envelope: WorkerDispatchEnvelope,
}

struct WorkerCallbackEvidence {
    started_status: StatusCode,
    started_body: Value,
    completed_status: StatusCode,
    completed_body: Value,
}

struct RetryWorkflowEvidence {
    task_key: String,
    callback: WorkerCallbackEvidence,
    dispatch: ReadyDispatch,
}

#[derive(Debug)]
struct DailyPartitionResolver;

impl PartitionResolver for DailyPartitionResolver {
    fn count_range(&self, _asset_key: &str, start: &str, end: &str) -> u32 {
        let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").expect("parse start date");
        let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").expect("parse end date");
        u32::try_from((end - start).num_days() + 1).expect("date range fits u32")
    }

    fn list_range_chunk(
        &self,
        _asset_key: &str,
        start: &str,
        end: &str,
        offset: u32,
        limit: u32,
    ) -> Vec<String> {
        let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").expect("parse start date");
        let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").expect("parse end date");

        (0..)
            .map(|day| start + Duration::days(day))
            .take_while(|date| *date <= end)
            .skip(usize::try_from(offset).expect("offset fits usize"))
            .take(usize::try_from(limit).expect("limit fits usize"))
            .map(|date| date.format("%Y-%m-%d").to_string())
            .collect()
    }
}
