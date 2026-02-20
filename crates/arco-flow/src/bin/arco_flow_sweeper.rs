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

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{
    DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS, DEFAULT_TASK_TOKEN_TTL_SECONDS, ScopedStorage,
    TaskTokenConfig, mint_task_token,
};
use arco_flow::dispatch::cloud_tasks::{CloudTasksConfig, CloudTasksDispatcher};
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::DispatchOutboxRow;
use arco_flow::orchestration::controllers::{AntiEntropySweeper, Repair};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::ids::{cloud_task_id, deterministic_attempt_id};
use arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope;

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    cloud_tasks: Arc<CloudTasksDispatcher>,
    dispatch_target_url: String,
    callback_base_url: String,
    task_token_config: TaskTokenConfig,
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

                let minted = mint_task_token(
                    &state.task_token_config,
                    task_key.clone(),
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    Utc::now(),
                )
                .map_err(|e| Error::configuration(format!("task token minting failed: {e}")))?;

                let envelope = WorkerDispatchEnvelope {
                    tenant_id: state.tenant_id.clone(),
                    workspace_id: state.workspace_id.clone(),
                    run_id: run_id.clone(),
                    task_key: task_key.clone(),
                    attempt,
                    attempt_id,
                    dispatch_id: original_dispatch_id.clone(),
                    worker_queue: "default-queue".to_string(),
                    callback_base_url: state.callback_base_url.clone(),
                    task_token: minted.token,
                    token_expires_at: minted.expires_at,
                    traceparent: None,
                    payload: serde_json::Value::Object(serde_json::Map::new()),
                };

                let body = envelope
                    .to_json()
                    .map_err(|e| Error::serialization(format!("dispatch envelope error: {e}")))?;

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
                        None,
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

fn parse_u64_env(key: &str, default: u64) -> Result<u64> {
    parse_u64_value(optional_env(key).as_deref(), key, default)
}

fn parse_u64_value(raw: Option<&str>, key: &str, default: u64) -> Result<u64> {
    let Some(raw) = raw else {
        return Ok(default);
    };

    raw.parse::<u64>()
        .map_err(|_| Error::configuration(format!("invalid {key}")))
}

fn task_token_config_from_env(task_timeout_secs: u64) -> Result<TaskTokenConfig> {
    task_token_config_from_parts(
        required_env("ARCO_FLOW_TASK_TOKEN_SECRET")?,
        optional_env("ARCO_FLOW_TASK_TOKEN_ISSUER"),
        optional_env("ARCO_FLOW_TASK_TOKEN_AUDIENCE"),
        parse_u64_env(
            "ARCO_FLOW_TASK_TOKEN_TTL_SECS",
            DEFAULT_TASK_TOKEN_TTL_SECONDS,
        )?,
        task_timeout_secs,
    )
}

fn task_token_config_from_parts(
    hs256_secret: String,
    issuer: Option<String>,
    audience: Option<String>,
    ttl_seconds: u64,
    task_timeout_secs: u64,
) -> Result<TaskTokenConfig> {
    let config = TaskTokenConfig {
        hs256_secret,
        issuer,
        audience,
        ttl_seconds,
    };
    config
        .validate_for_dispatch(task_timeout_secs, true)
        .map_err(|e| Error::configuration(e.to_string()))?;
    Ok(config)
}

fn task_timeout_seconds_from_env() -> Result<u64> {
    let timeout = parse_u64_env(
        "ARCO_FLOW_TASK_TIMEOUT_SECS",
        DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
    )?;
    validate_task_timeout_seconds(timeout)
}

fn validate_task_timeout_seconds(timeout: u64) -> Result<u64> {
    if timeout == 0 {
        return Err(Error::configuration(
            "ARCO_FLOW_TASK_TIMEOUT_SECS must be greater than zero",
        ));
    }
    Ok(timeout)
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
    let callback_base_url = required_env("ARCO_FLOW_CALLBACK_BASE_URL")?;
    let project_id = required_env("ARCO_GCP_PROJECT_ID")?;
    let location = required_env("ARCO_GCP_LOCATION")?;
    let queue_name =
        optional_env("ARCO_FLOW_QUEUE").unwrap_or_else(|| "arco-flow-dispatch".to_string());
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    let task_timeout_secs = task_timeout_seconds_from_env()?;
    let task_token_config = task_token_config_from_env(task_timeout_secs)?;
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

    cloud_config =
        cloud_config.with_task_timeout(std::time::Duration::from_secs(task_timeout_secs));

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
        callback_base_url,
        task_token_config,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_token_config_from_parts_rejects_missing_issuer() {
        let err = task_token_config_from_parts(
            "secret".to_string(),
            None,
            Some("audience".to_string()),
            3_600,
            1_800,
        )
        .expect_err("missing issuer must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn task_token_config_from_parts_rejects_missing_audience() {
        let err = task_token_config_from_parts(
            "secret".to_string(),
            Some("issuer".to_string()),
            None,
            3_600,
            1_800,
        )
        .expect_err("missing audience must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn parse_u64_value_rejects_invalid_timeout_env() {
        let err = parse_u64_value(
            Some("not-a-number"),
            "ARCO_FLOW_TASK_TIMEOUT_SECS",
            DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
        )
        .expect_err("invalid timeout must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn validate_task_timeout_seconds_rejects_zero() {
        let err = validate_task_timeout_seconds(0).expect_err("zero timeout must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }
}
