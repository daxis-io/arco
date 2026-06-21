//! Deterministic user-acceptance coverage for cataloged orchestration pipelines.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::large_futures,
    clippy::unwrap_used
)]

use std::future::Future;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::http::{Method, Request, StatusCode, header};
use chrono::Utc;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tower::ServiceExt;

use arco_api::server::ServerBuilder;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_core::{ScopedStorage, TaskTokenConfig, mint_task_token_for_attempt};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::callbacks::{
    CallbackContext, CallbackResult, TaskCompletedRequest, TaskOutput, TaskStartedRequest,
    TaskState as CallbackTaskState, TaskStateLookup, TaskTokenValidator, WorkerOutcome,
    handle_task_completed, handle_task_started,
};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::TaskState;
use arco_flow::orchestration::controllers::{
    BackfillController, DispatchAction, DispatcherController, PartitionResolver,
    ReadyDispatchAction, ReadyDispatchController,
};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::ledger::OrchestrationLedgerWriter;
use arco_flow::orchestration::worker_contract::parse_callback_task_id;

const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";
const CODE_VERSION: &str = "uat-pipeline-v1";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TriggerRunResponse {
    run_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBackfillResponse {
    backfill_id: String,
}

#[derive(Clone)]
struct UatHarness {
    router: axum::Router,
    ledger: LedgerWriter,
    compactor: MicroCompactor,
}

impl UatHarness {
    fn new() -> TestResult<Self> {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let router = ServerBuilder::new()
            .debug(true)
            .storage_backend(backend.clone())
            .build()
            .test_router();
        let storage = ScopedStorage::new(backend, TENANT, WORKSPACE)?;
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage);
        Ok(Self {
            router,
            ledger,
            compactor,
        })
    }

    async fn deploy_manifest(&self) -> TestResult<()> {
        let (status, _body): (_, serde_json::Value) = post_json_with_headers(
            self.router.clone(),
            "/api/v1/workspaces/test-workspace/manifests",
            json!({
                "manifestVersion": "1.0",
                "codeVersionId": CODE_VERSION,
                "assets": [
                    {
                        "key": {"namespace": "analytics", "name": "raw_orders"},
                        "id": "01JRAWORDERS0000000000001",
                        "description": "Raw order input"
                    },
                    {
                        "key": {"namespace": "analytics", "name": "daily_orders"},
                        "id": "01JDAILYORDERS0000000001",
                        "description": "Daily order model",
                        "dependencies": [{
                            "upstreamKey": {"namespace": "analytics", "name": "raw_orders"},
                            "parameterName": "raw_orders"
                        }]
                    },
                    {
                        "key": {"namespace": "analytics", "name": "order_metrics"},
                        "id": "01JORDERMETRICS000000001",
                        "description": "Order metrics model",
                        "dependencies": [{
                            "upstreamKey": {"namespace": "analytics", "name": "daily_orders"},
                            "parameterName": "daily_orders"
                        }]
                    }
                ],
                "schedules": [{
                    "id": "daily-orders",
                    "cron": "0 0 * * *",
                    "assets": ["analytics.daily_orders"],
                    "timezone": "UTC"
                }]
            }),
            &[("Idempotency-Key", "uat-manifest")],
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        Ok(())
    }

    async fn trigger_pipeline_run(&self) -> TestResult<String> {
        let (status, response): (_, TriggerRunResponse) = post_json(
            self.router.clone(),
            "/api/v1/workspaces/test-workspace/runs",
            json!({
                "selection": ["analytics.order_metrics"],
                "includeUpstream": true,
                "includeDownstream": false,
                "partitions": []
            }),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        Ok(response.run_id)
    }

    async fn dispatch_and_complete_all_ready_tasks(&self, run_id: &str) -> TestResult<()> {
        loop {
            let (manifest, state) = self.compactor.load_state().await?;
            let ready_actions =
                ReadyDispatchController::with_defaults().reconcile(&manifest, &state);
            if ready_actions.is_empty() {
                break;
            }

            let mut progressed = false;
            for action in ready_actions {
                let ReadyDispatchAction::EmitDispatchRequested {
                    run_id: action_run_id,
                    task_key,
                    attempt,
                    attempt_id,
                    worker_queue,
                    dispatch_id,
                } = action
                else {
                    continue;
                };
                if action_run_id != run_id {
                    continue;
                }
                progressed = true;

                self.append_and_compact(vec![OrchestrationEvent::new(
                    TENANT,
                    WORKSPACE,
                    OrchestrationEventData::DispatchRequested {
                        run_id: action_run_id.clone(),
                        task_key: task_key.clone(),
                        attempt,
                        attempt_id: attempt_id.clone(),
                        worker_queue,
                        dispatch_id: dispatch_id.clone(),
                    },
                )])
                .await?;

                let (manifest, state) = self.compactor.load_state().await?;
                let outbox_rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();
                let dispatch_actions =
                    DispatcherController::with_defaults().reconcile(&manifest, &outbox_rows);
                for dispatch_action in dispatch_actions {
                    let DispatchAction::CreateCloudTask {
                        dispatch_id: candidate_dispatch_id,
                        cloud_task_id,
                        ..
                    } = dispatch_action
                    else {
                        continue;
                    };
                    if candidate_dispatch_id != dispatch_id {
                        continue;
                    }

                    self.append_and_compact(vec![OrchestrationEvent::new(
                        TENANT,
                        WORKSPACE,
                        OrchestrationEventData::DispatchEnqueued {
                            dispatch_id: dispatch_id.clone(),
                            run_id: Some(action_run_id.clone()),
                            task_key: Some(task_key.clone()),
                            attempt: Some(attempt),
                            cloud_task_id,
                        },
                    )])
                    .await?;
                }

                self.complete_task(run_id, &task_key, attempt, &attempt_id)
                    .await?;
            }

            if !progressed {
                break;
            }
        }
        Ok(())
    }

    async fn complete_task(
        &self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
    ) -> TestResult<()> {
        let token_config = TaskTokenConfig {
            hs256_secret: "test-task-secret".to_string(),
            issuer: Some("https://issuer.task".to_string()),
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 900,
        };
        let minted = mint_task_token_for_attempt(
            &token_config,
            task_key,
            TENANT,
            WORKSPACE,
            run_id,
            attempt,
            Utc::now(),
        )?;
        let callback_ctx = CallbackContext::new(
            Arc::new(CompactingLedger {
                ledger: self.ledger.clone(),
                compactor: self.compactor.clone(),
            }),
            Arc::new(UatTokenValidator {
                config: token_config,
            }),
            TENANT,
            WORKSPACE,
        );
        let lookup = CompactorLookup {
            compactor: self.compactor.clone(),
            run_id_scope: run_id.to_string(),
        };

        let started = handle_task_started(
            &callback_ctx,
            task_key,
            &minted.token,
            TaskStartedRequest {
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-1".to_string(),
                traceparent: None,
                started_at: Some(Utc::now()),
            },
            &lookup,
        )
        .await;
        assert!(matches!(started, CallbackResult::Ok(_)));

        let completed = handle_task_completed(
            &callback_ctx,
            task_key,
            &minted.token,
            TaskCompletedRequest {
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-1".to_string(),
                traceparent: None,
                outcome: WorkerOutcome::Succeeded,
                completed_at: Some(Utc::now()),
                output: Some(TaskOutput {
                    materialization_id: Some(format!("mat-{task_key}")),
                    row_count: Some(100),
                    byte_size: Some(2048),
                    output_path: Some(format!("memory://{task_key}")),
                    delta_table: Some(task_key.to_string()),
                    delta_version: Some(i64::from(attempt) + 40),
                    delta_partition: Some("date=2026-05-31".to_string()),
                    output_visibility_state: None,
                    published_at: Some(Utc::now()),
                    publish_error: None,
                }),
                error: None,
                metrics: None,
                cancelled_during_phase: None,
                partial_progress: None,
            },
            &lookup,
        )
        .await;
        assert!(matches!(completed, CallbackResult::Ok(_)));
        Ok(())
    }

    async fn append_and_compact(&self, events: Vec<OrchestrationEvent>) -> TestResult<()> {
        let paths: Vec<_> = events.iter().map(LedgerWriter::event_path).collect();
        self.ledger.append_all(events).await?;
        self.compactor.compact_events(paths).await?;
        Ok(())
    }

    async fn query_rows(&self, sql: &str) -> TestResult<Vec<serde_json::Value>> {
        let request = make_request(
            Method::POST,
            "/api/v1/query?format=json",
            Some(json!({ "sql": sql })),
        )?;
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;
        let (status, body) = response_body(response).await?;
        assert_eq!(
            status,
            StatusCode::OK,
            "query failed: {}",
            String::from_utf8_lossy(&body)
        );
        Ok(serde_json::from_slice(&body)?)
    }

    async fn create_and_plan_backfill(&self) -> TestResult<String> {
        let (status, response): (_, CreateBackfillResponse) = post_json(
            self.router.clone(),
            "/api/v1/workspaces/test-workspace/backfills",
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
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (manifest, state) = self.compactor.load_state().await?;
        let controller = BackfillController::with_defaults(
            Arc::new(DailyPartitionResolver),
            2,
            2,
            chrono::Duration::seconds(30),
        );
        let events = controller.reconcile(
            &manifest.watermarks,
            &state.backfills,
            &state.backfill_chunks,
            &state.runs,
        );
        self.append_and_compact(events).await?;

        Ok(response.backfill_id)
    }
}

#[derive(Debug)]
struct DailyPartitionResolver;

impl PartitionResolver for DailyPartitionResolver {
    fn count_range(&self, _asset_key: &str, start: &str, end: &str) -> u32 {
        if start == "2026-05-28" && end == "2026-05-31" {
            4
        } else {
            0
        }
    }

    fn list_range_chunk(
        &self,
        _asset_key: &str,
        _start: &str,
        _end: &str,
        offset: u32,
        limit: u32,
    ) -> Vec<String> {
        ["2026-05-28", "2026-05-29", "2026-05-30", "2026-05-31"]
            .into_iter()
            .skip(usize::try_from(offset).unwrap_or(usize::MAX))
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(str::to_string)
            .collect()
    }
}

#[derive(Clone)]
struct CompactingLedger {
    ledger: LedgerWriter,
    compactor: MicroCompactor,
}

impl OrchestrationLedgerWriter for CompactingLedger {
    fn write_event(
        &self,
        event: &OrchestrationEvent,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let writer = self.clone();
        let event = event.clone();
        async move {
            let path = LedgerWriter::event_path(&event);
            writer
                .ledger
                .append(event)
                .await
                .map_err(|err| err.to_string())?;
            writer
                .compactor
                .compact_events(vec![path])
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        }
    }

    fn write_events(
        &self,
        events: Vec<OrchestrationEvent>,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let writer = self.clone();
        async move {
            let paths = events
                .iter()
                .map(LedgerWriter::event_path)
                .collect::<Vec<_>>();
            writer
                .ledger
                .append_all(events)
                .await
                .map_err(|err| err.to_string())?;
            writer
                .compactor
                .compact_events(paths)
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        }
    }
}

#[derive(Clone)]
struct UatTokenValidator {
    config: TaskTokenConfig,
}

impl TaskTokenValidator for UatTokenValidator {
    fn validate_task_token(
        &self,
        task_id: &str,
        run_id: &str,
        attempt: u32,
        token: &str,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let config = self.config.clone();
        let task_id = task_id.to_string();
        let run_id = run_id.to_string();
        let token = token.to_string();
        async move {
            let claims =
                arco_core::decode_task_token(&config, &token).map_err(|e| e.to_string())?;
            if claims.task_id != task_id {
                return Err("task_id_mismatch".to_string());
            }
            if claims.run_id.as_deref() != Some(run_id.as_str()) {
                return Err("run_id_mismatch".to_string());
            }
            if claims.attempt != Some(attempt) {
                return Err("attempt_mismatch".to_string());
            }
            Ok(())
        }
    }
}

#[derive(Clone)]
struct CompactorLookup {
    compactor: MicroCompactor,
    run_id_scope: String,
}

impl TaskStateLookup for CompactorLookup {
    fn get_task_state(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<CallbackTaskState>, String>> + Send {
        let task_id = task_id.to_string();
        let compactor = self.compactor.clone();
        let run_id_scope = self.run_id_scope.clone();
        async move {
            let (_, state) = compactor.load_state().await.map_err(|e| e.to_string())?;
            let parsed = parse_callback_task_id(&task_id).ok();
            let task_key = match &parsed {
                Some(parsed) if parsed.run_id == run_id_scope => parsed.task_key.clone(),
                Some(_) => return Ok(None),
                None => task_id.clone(),
            };
            let Some(row) = state.tasks.get(&(run_id_scope.clone(), task_key)) else {
                return Ok(None);
            };
            let run = state.runs.get(&row.run_id);
            Ok(Some(CallbackTaskState {
                state: task_state_label(row.state).to_string(),
                attempt: row.attempt,
                attempt_id: row.attempt_id.clone().unwrap_or_default(),
                run_id: row.run_id.clone(),
                task_key: row.task_key.clone(),
                asset_key: row.asset_key.clone(),
                partition_key: row.partition_key.clone(),
                code_version: run.and_then(|run| run.code_version.clone()),
                cancel_requested: run.is_some_and(|run| run.cancel_requested),
            }))
        }
    }
}

fn task_state_label(state: TaskState) -> &'static str {
    match state {
        TaskState::Planned => "PLANNED",
        TaskState::Blocked => "BLOCKED",
        TaskState::Ready => "READY",
        TaskState::Dispatched => "DISPATCHED",
        TaskState::Running => "RUNNING",
        TaskState::RetryWait => "RETRY_WAIT",
        TaskState::Skipped => "SKIPPED",
        TaskState::Cancelled => "CANCELLED",
        TaskState::Failed => "FAILED",
        TaskState::Succeeded => "SUCCEEDED",
    }
}

fn assert_partition_keys(value: &serde_json::Value, expected: &[&str]) -> TestResult {
    let keys: Vec<String> = match value {
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(ToString::to_string)
                    .ok_or("partition key is not a string")
            })
            .collect::<Result<_, _>>()?,
        serde_json::Value::String(serialized) => serde_json::from_str(serialized)?,
        _ => return Err("partition_keys must be an array or serialized array".into()),
    };
    assert_eq!(keys, expected);
    Ok(())
}

#[tokio::test]
async fn user_acceptance_pipeline_runs_to_queryable_catalog_state() -> TestResult {
    let harness = UatHarness::new()?;
    harness.deploy_manifest().await?;

    let schedule_rows = harness
        .query_rows(
            "SELECT schedule_id, code_version FROM system.orchestration.schedule_definitions WHERE schedule_id = 'daily-orders'",
        )
        .await?;
    assert_eq!(schedule_rows.len(), 1);
    assert_eq!(schedule_rows[0]["code_version"], CODE_VERSION);

    let run_id = harness.trigger_pipeline_run().await?;
    harness
        .dispatch_and_complete_all_ready_tasks(&run_id)
        .await?;

    let run_rows = harness
        .query_rows(&format!(
            "SELECT run_id, state, tasks_total, tasks_completed, tasks_succeeded, tasks_failed, tasks_cancelled, cancel_requested, code_version FROM system.orchestration.runs WHERE run_id = '{run_id}'"
        ))
        .await?;
    assert_eq!(run_rows.len(), 1);
    assert_eq!(run_rows[0]["state"], "SUCCEEDED");
    assert_eq!(run_rows[0]["tasks_total"], 3);
    assert_eq!(run_rows[0]["tasks_completed"], 3);
    assert_eq!(run_rows[0]["tasks_succeeded"], 3);
    assert_eq!(run_rows[0]["tasks_failed"], 0);
    assert_eq!(run_rows[0]["tasks_cancelled"], 0);
    assert_eq!(run_rows[0]["cancel_requested"], false);
    assert_eq!(run_rows[0]["code_version"], CODE_VERSION);

    let task_rows = harness
        .query_rows(&format!(
            "SELECT task_key, state, delta_table, delta_version, delta_partition, execution_lineage_ref FROM system.orchestration.tasks WHERE run_id = '{run_id}' ORDER BY task_key"
        ))
        .await?;
    assert_eq!(task_rows.len(), 3);
    assert!(task_rows.iter().all(|row| {
        row["state"] == "SUCCEEDED"
            && row["delta_table"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
            && row["delta_version"].as_u64().is_some()
            && row["delta_partition"] == "date=2026-05-31"
            && row["execution_lineage_ref"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
    }));

    let index_rows = harness
        .query_rows(&format!(
            "SELECT run_id, task_key, run_status, task_status, delta_table, delta_version, delta_partition, execution_lineage_ref, code_version FROM system.orchestration.catalog_run_index WHERE run_id = '{run_id}' ORDER BY task_key"
        ))
        .await?;
    assert_eq!(index_rows.len(), 3);
    assert!(index_rows.iter().all(|row| {
        row["run_status"] == "SUCCEEDED"
            && row["task_status"] == "SUCCEEDED"
            && row["code_version"] == CODE_VERSION
            && row["execution_lineage_ref"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
    }));

    let partition_rows = harness
        .query_rows("SELECT asset_key, partition_key, last_attempt_run_id, last_materialization_run_id, last_materialization_code_version, delta_table, delta_version, delta_partition, execution_lineage_ref FROM system.orchestration.partition_status ORDER BY asset_key")
        .await?;
    assert!(
        partition_rows
            .iter()
            .any(|row| row["asset_key"] == "analytics.order_metrics"
                && row["partition_key"] == "date=2026-05-31"
                && row["last_attempt_run_id"] == run_id
                && row["last_materialization_run_id"] == run_id
                && row["last_materialization_code_version"] == CODE_VERSION
                && row["delta_table"] == "analytics.order_metrics"
                && row["delta_version"].as_u64().is_some()
                && row["delta_partition"] == "date=2026-05-31"
                && row["execution_lineage_ref"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())),
        "missing order_metrics partition status row in {partition_rows:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn user_acceptance_backfill_range_is_queryable_with_planned_chunks() -> TestResult {
    let harness = UatHarness::new()?;
    harness.deploy_manifest().await?;

    let backfill_id = harness.create_and_plan_backfill().await?;

    let backfill_rows = harness
        .query_rows(&format!(
            "SELECT backfill_id, total_partitions, planned_chunks, chunk_size, state FROM system.orchestration.backfills WHERE backfill_id = '{backfill_id}'"
        ))
        .await?;
    assert_eq!(backfill_rows.len(), 1);
    assert_eq!(backfill_rows[0]["total_partitions"], 4);
    assert_eq!(backfill_rows[0]["planned_chunks"], 2);
    assert_eq!(backfill_rows[0]["chunk_size"], 2);
    assert_eq!(backfill_rows[0]["state"], "RUNNING");

    let chunk_rows = harness
        .query_rows(&format!(
            "SELECT chunk_index, partition_keys FROM system.orchestration.backfill_chunks WHERE backfill_id = '{backfill_id}' ORDER BY chunk_index"
        ))
        .await?;
    assert_eq!(chunk_rows.len(), 2);
    assert_partition_keys(
        &chunk_rows[0]["partition_keys"],
        &["2026-05-28", "2026-05-29"],
    )?;
    assert_partition_keys(
        &chunk_rows[1]["partition_keys"],
        &["2026-05-30", "2026-05-31"],
    )?;

    Ok(())
}

#[tokio::test]
async fn user_acceptance_pipeline_is_workspace_isolated() -> TestResult {
    let harness = UatHarness::new()?;
    harness.deploy_manifest().await?;
    let run_id = harness.trigger_pipeline_run().await?;

    let request = make_request_with_scope(
        Method::POST,
        "/api/v1/query?format=json",
        TENANT,
        "other-workspace",
        Some(json!({
            "sql": format!("SELECT run_id FROM system.orchestration.runs WHERE run_id = '{run_id}'")
        })),
    )?;
    let response = harness
        .router
        .clone()
        .oneshot(request)
        .await
        .map_err(|err| match err {})?;
    let (status, body) = response_body(response).await?;
    assert_eq!(status, StatusCode::OK);
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&body)?;
    assert!(rows.is_empty());

    Ok(())
}

fn make_request(
    method: Method,
    uri: &str,
    body: Option<serde_json::Value>,
) -> TestResult<Request<Body>> {
    make_request_with_scope(method, uri, TENANT, WORKSPACE, body)
}

fn make_request_with_scope(
    method: Method,
    uri: &str,
    tenant: &str,
    workspace: &str,
    body: Option<serde_json::Value>,
) -> TestResult<Request<Body>> {
    make_request_with_scope_and_headers(method, uri, tenant, workspace, body, &[])
}

fn make_request_with_headers(
    method: Method,
    uri: &str,
    body: Option<serde_json::Value>,
    headers: &[(&str, &str)],
) -> TestResult<Request<Body>> {
    make_request_with_scope_and_headers(method, uri, TENANT, WORKSPACE, body, headers)
}

fn make_request_with_scope_and_headers(
    method: Method,
    uri: &str,
    tenant: &str,
    workspace: &str,
    body: Option<serde_json::Value>,
    headers: &[(&str, &str)],
) -> TestResult<Request<Body>> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header(header::CONTENT_TYPE, "application/json");

    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }

    let body = match body {
        Some(value) => Body::from(serde_json::to_vec(&value)?),
        None => Body::empty(),
    };
    Ok(builder.body(body)?)
}

async fn response_body(response: axum::response::Response) -> TestResult<(StatusCode, Bytes)> {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((status, body))
}

async fn post_json<T: DeserializeOwned>(
    router: axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> TestResult<(StatusCode, T)> {
    let request = make_request(Method::POST, uri, Some(body))?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    let (status, body) = response_body(response).await?;
    let json = serde_json::from_slice(&body)?;
    Ok((status, json))
}

async fn post_json_with_headers<T: DeserializeOwned>(
    router: axum::Router,
    uri: &str,
    body: serde_json::Value,
    headers: &[(&str, &str)],
) -> TestResult<(StatusCode, T)> {
    let request = make_request_with_headers(Method::POST, uri, Some(body), headers)?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    let (status, body) = response_body(response).await?;
    let json = serde_json::from_slice(&body)?;
    Ok((status, json))
}
