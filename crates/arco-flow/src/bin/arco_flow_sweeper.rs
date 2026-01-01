//! Arco Flow orchestration anti-entropy sweeper service.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Serialize;

use arco_core::ScopedStorage;
use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_flow::dispatch::cloud_tasks::{CloudTasksConfig, CloudTasksDispatcher};
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::DispatchOutboxRow;
use arco_flow::orchestration::controllers::{AntiEntropySweeper, DispatchPayload, Repair};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::ids::{cloud_task_id, deterministic_attempt_id};

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    cloud_tasks: Arc<CloudTasksDispatcher>,
    dispatch_target_url: String,
}

#[derive(Debug, Serialize)]
struct RunError {
    kind: String,
    id: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    repairs_created: usize,
    redispatch_attempted: usize,
    redispatch_enqueued: usize,
    redispatch_deduplicated: usize,
    redispatch_failed: usize,
    skipped_due_to_lag: usize,
    errors: Vec<RunError>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    message: String,
    summary: Option<RunSummary>,
}

impl ApiError {
    fn from_summary(summary: RunSummary) -> Self {
        Self {
            message: "sweeper run completed with errors".to_string(),
            summary: Some(summary),
        }
    }
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            message: error.to_string(),
            summary: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        if let Some(summary) = self.summary {
            return (status, Json(summary)).into_response();
        }

        (
            status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;
    let sweeper = AntiEntropySweeper::with_defaults();

    let tasks: Vec<_> = fold_state.tasks.values().cloned().collect();
    let outbox: Vec<_> = fold_state.dispatch_outbox.values().cloned().collect();
    let outbox_by_id: HashMap<String, DispatchOutboxRow> = outbox
        .iter()
        .cloned()
        .map(|row| (row.dispatch_id.clone(), row))
        .collect();

    let repairs = sweeper.scan(&manifest.watermarks, &tasks, &outbox, Utc::now());

    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut repairs_created = 0;
    let mut redispatch_attempted = 0;
    let mut redispatch_enqueued = 0;
    let mut redispatch_deduplicated = 0;
    let mut redispatch_failed = 0;
    let mut skipped_due_to_lag = 0;

    for repair in repairs {
        match repair {
            Repair::CreateDispatchOutbox {
                run_id,
                task_key,
                attempt,
                ..
            } => {
                let dispatch_id = DispatchOutboxRow::dispatch_id(&run_id, &task_key, attempt);
                let attempt_id = deterministic_attempt_id(&dispatch_id);

                events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchRequested {
                        run_id,
                        task_key,
                        attempt,
                        attempt_id,
                        worker_queue: "default-queue".to_string(),
                        dispatch_id,
                    },
                ));

                repairs_created += 1;
            }
            Repair::RedispatchStuckTask {
                run_id,
                task_key,
                attempt,
                original_dispatch_id,
                ..
            } => {
                redispatch_attempted += 1;

                let attempt_id = outbox_by_id
                    .get(&original_dispatch_id)
                    .map(|row| row.attempt_id.clone())
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| deterministic_attempt_id(&original_dispatch_id));

                let payload =
                    DispatchPayload::new(run_id.clone(), task_key.clone(), attempt, attempt_id);

                let body = payload
                    .to_json()
                    .map_err(|e| Error::serialization(format!("dispatch payload error: {e}")))?;

                let cloud_id = cloud_task_id("d", &original_dispatch_id);
                let options = EnqueueOptions::new();

                let result = state
                    .cloud_tasks
                    .enqueue_http(
                        &cloud_id,
                        &state.dispatch_target_url,
                        body.as_bytes(),
                        options,
                        Some(state.dispatch_target_url.as_str()),
                    )
                    .await;

                match result {
                    Ok(EnqueueResult::Enqueued { .. }) => {
                        redispatch_enqueued += 1;
                        events.push(OrchestrationEvent::new(
                            state.tenant_id.clone(),
                            state.workspace_id.clone(),
                            OrchestrationEventData::DispatchEnqueued {
                                dispatch_id: original_dispatch_id.clone(),
                                run_id: Some(run_id),
                                task_key: Some(task_key),
                                attempt: Some(attempt),
                                cloud_task_id: cloud_id,
                            },
                        ));
                    }
                    Ok(EnqueueResult::Deduplicated { .. }) => {
                        redispatch_deduplicated += 1;
                        events.push(OrchestrationEvent::new(
                            state.tenant_id.clone(),
                            state.workspace_id.clone(),
                            OrchestrationEventData::DispatchEnqueued {
                                dispatch_id: original_dispatch_id.clone(),
                                run_id: Some(run_id),
                                task_key: Some(task_key),
                                attempt: Some(attempt),
                                cloud_task_id: cloud_id,
                            },
                        ));
                    }
                    Ok(EnqueueResult::QueueFull) => {
                        redispatch_failed += 1;
                        errors.push(RunError {
                            kind: "redispatch_queue_full".to_string(),
                            id: original_dispatch_id,
                            message: "queue full".to_string(),
                        });
                    }
                    Err(err) => {
                        redispatch_failed += 1;
                        errors.push(RunError {
                            kind: "redispatch_enqueue_failed".to_string(),
                            id: original_dispatch_id,
                            message: err.to_string(),
                        });
                    }
                }
            }
            Repair::SkippedDueToLag { .. } => {
                skipped_due_to_lag += 1;
            }
        }
    }

    if !events.is_empty() {
        state.ledger.append_all(events).await?;
    }

    let summary = RunSummary {
        repairs_created,
        redispatch_attempted,
        redispatch_enqueued,
        redispatch_deduplicated,
        redispatch_failed,
        skipped_due_to_lag,
        errors,
    };

    if summary.errors.is_empty() {
        Ok(Json(summary))
    } else {
        Err(ApiError::from_summary(summary))
    }
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    std::env::var(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid PORT"));
    }

    if let Ok(port) = std::env::var("ARCO_FLOW_PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid ARCO_FLOW_PORT"));
    }

    Ok(8080)
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

#[allow(clippy::unused_async)]
async fn build_cloud_tasks(config: CloudTasksConfig) -> Result<CloudTasksDispatcher> {
    #[cfg(feature = "gcp")]
    {
        CloudTasksDispatcher::new(config).await
    }

    #[cfg(not(feature = "gcp"))]
    {
        CloudTasksDispatcher::new(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let dispatch_target_url = required_env("ARCO_FLOW_DISPATCH_TARGET_URL")?;
    let project_id = required_env("ARCO_GCP_PROJECT_ID")?;
    let location = required_env("ARCO_GCP_LOCATION")?;
    let queue_name =
        optional_env("ARCO_FLOW_QUEUE").unwrap_or_else(|| "arco-flow-dispatch".to_string());
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    let port = resolve_port()?;

    let mut cloud_config = CloudTasksConfig::new(
        project_id,
        location,
        queue_name,
        dispatch_target_url.clone(),
    );

    if let Some(email) = service_account_email {
        cloud_config = cloud_config.with_service_account(email);
    }

    let apply_queue_updates = parse_bool_env("ARCO_FLOW_APPLY_QUEUE_RETRY_CONFIG", false);
    if !apply_queue_updates {
        cloud_config = cloud_config.with_queue_retry_updates(false);
    }

    if let Ok(timeout) = std::env::var("ARCO_FLOW_TASK_TIMEOUT_SECS") {
        if let Ok(secs) = timeout.parse::<u64>() {
            cloud_config = cloud_config.with_task_timeout(std::time::Duration::from_secs(secs));
        }
    }

    let cloud_tasks = build_cloud_tasks(cloud_config).await?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id.clone(), workspace_id.clone())?;

    let state = AppState {
        tenant_id,
        workspace_id,
        compactor: MicroCompactor::new(storage.clone()),
        ledger: LedgerWriter::new(storage),
        cloud_tasks: Arc::new(cloud_tasks),
        dispatch_target_url,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/run", post(run_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}
