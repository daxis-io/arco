//! Servo orchestration dispatcher service.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;

use arco_core::observability::{init_logging, LogFormat};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::ScopedStorage;
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::dispatch::cloud_tasks::{CloudTasksConfig, CloudTasksDispatcher};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::controllers::{
    DispatchAction, DispatchPayload, DispatcherController, ReadyDispatchController,
    TimerAction, TimerController,
};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::LedgerWriter;

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    cloud_tasks: Arc<CloudTasksDispatcher>,
    dispatch_target_url: String,
    timer_target_url: Option<String>,
    timer_queue: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunError {
    kind: String,
    id: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    ready_dispatch_emitted: usize,
    ready_dispatch_skipped: usize,
    dispatch_actions: usize,
    dispatch_enqueued: usize,
    dispatch_deduplicated: usize,
    dispatch_failed: usize,
    timer_actions: usize,
    timer_enqueued: usize,
    timer_deduplicated: usize,
    timer_failed: usize,
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
            message: "dispatcher run completed with errors".to_string(),
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

#[derive(Debug, Serialize)]
struct TimerPayload {
    timer_id: String,
    run_id: Option<String>,
    task_key: Option<String>,
    attempt: Option<u32>,
    fire_at: DateTime<Utc>,
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;

    let ready_controller = ReadyDispatchController::with_defaults();
    let ready_actions = ready_controller.reconcile(&manifest, &fold_state);

    let mut ready_events = Vec::new();
    let mut ready_emitted = 0;
    let mut ready_skipped = 0;

    for action in ready_actions {
        match action.into_event_data() {
            Some(data) => {
                ready_emitted += 1;
                ready_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    data,
                ));
            }
            None => ready_skipped += 1,
        }
    }

    if !ready_events.is_empty() {
        state.ledger.append_all(ready_events).await?;
    }

    let outbox_rows: Vec<_> = fold_state.dispatch_outbox.values().cloned().collect();
    let dispatcher = DispatcherController::with_defaults();
    let dispatch_actions = dispatcher.reconcile(&manifest, &outbox_rows);

    let mut dispatch_events = Vec::new();
    let mut dispatch_enqueued = 0;
    let mut dispatch_deduplicated = 0;
    let mut dispatch_failed = 0;
    let mut errors = Vec::new();

    for action in &dispatch_actions {
        let DispatchAction::CreateCloudTask {
            dispatch_id,
            cloud_task_id,
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_queue,
        } = action else {
            continue;
        };

        let payload = DispatchPayload::new(
            run_id.clone(),
            task_key.clone(),
            *attempt,
            attempt_id.clone(),
        );
        let body = payload
            .to_json()
            .map_err(|e| Error::serialization(format!("dispatch payload error: {e}")))?;

        let mut options = EnqueueOptions::new();
        if worker_queue != "default-queue" {
            options = options.with_routing_key(worker_queue.clone());
        }

        let result = state
            .cloud_tasks
            .enqueue_http(
                cloud_task_id,
                &state.dispatch_target_url,
                body.as_bytes(),
                options,
                Some(state.dispatch_target_url.as_str()),
            )
            .await;

        match result {
            Ok(EnqueueResult::Enqueued { .. }) => {
                dispatch_enqueued += 1;
                dispatch_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchEnqueued {
                        dispatch_id: dispatch_id.clone(),
                        run_id: Some(run_id.clone()),
                        task_key: Some(task_key.clone()),
                        attempt: Some(*attempt),
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::Deduplicated { .. }) => {
                dispatch_deduplicated += 1;
                dispatch_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchEnqueued {
                        dispatch_id: dispatch_id.clone(),
                        run_id: Some(run_id.clone()),
                        task_key: Some(task_key.clone()),
                        attempt: Some(*attempt),
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::QueueFull) => {
                dispatch_failed += 1;
                errors.push(RunError {
                    kind: "dispatch_queue_full".to_string(),
                    id: dispatch_id.clone(),
                    message: "queue full".to_string(),
                });
            }
            Err(err) => {
                dispatch_failed += 1;
                errors.push(RunError {
                    kind: "dispatch_enqueue_failed".to_string(),
                    id: dispatch_id.clone(),
                    message: err.to_string(),
                });
            }
        }
    }

    if !dispatch_events.is_empty() {
        state.ledger.append_all(dispatch_events).await?;
    }

    let timer_rows: Vec<_> = fold_state.timers.values().cloned().collect();
    let timer_controller = TimerController::with_defaults();
    let timer_actions = timer_controller.reconcile(&manifest, &timer_rows);

    let mut timer_events = Vec::new();
    let mut timer_enqueued = 0;
    let mut timer_deduplicated = 0;
    let mut timer_failed = 0;

    if !timer_actions.is_empty() && state.timer_target_url.is_none() {
        errors.push(RunError {
            kind: "timer_target_missing".to_string(),
            id: "timers".to_string(),
            message: "ARCO_SERVO_TIMER_TARGET_URL is not set".to_string(),
        });
    }

    for action in &timer_actions {
        let TimerAction::CreateTimer {
            timer_id,
            cloud_task_id,
            fire_at,
            run_id,
            task_key,
            attempt,
            ..
        } = action else {
            continue;
        };

        let Some(target_url) = state.timer_target_url.as_ref() else {
            timer_failed += 1;
            continue;
        };

        let payload = TimerPayload {
            timer_id: timer_id.clone(),
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            fire_at: *fire_at,
        };
        let body = serde_json::to_vec(&payload)
            .map_err(|e| Error::serialization(format!("timer payload error: {e}")))?;

        let now = Utc::now();
        let delay = fire_at
            .signed_duration_since(now)
            .to_std()
            .ok();

        let mut options = EnqueueOptions::new();
        if let Some(delay) = delay {
            options = options.with_delay(delay);
        }
        if let Some(queue) = state.timer_queue.as_ref() {
            options = options.with_routing_key(queue.clone());
        }

        let result = state
            .cloud_tasks
            .enqueue_http(
                cloud_task_id,
                target_url,
                &body,
                options,
                Some(target_url.as_str()),
            )
            .await;

        match result {
            Ok(EnqueueResult::Enqueued { .. }) => {
                timer_enqueued += 1;
                timer_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TimerEnqueued {
                        timer_id: timer_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt: *attempt,
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::Deduplicated { .. }) => {
                timer_deduplicated += 1;
                timer_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TimerEnqueued {
                        timer_id: timer_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt: *attempt,
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::QueueFull) => {
                timer_failed += 1;
                errors.push(RunError {
                    kind: "timer_queue_full".to_string(),
                    id: timer_id.clone(),
                    message: "queue full".to_string(),
                });
            }
            Err(err) => {
                timer_failed += 1;
                errors.push(RunError {
                    kind: "timer_enqueue_failed".to_string(),
                    id: timer_id.clone(),
                    message: err.to_string(),
                });
            }
        }
    }

    if !timer_events.is_empty() {
        state.ledger.append_all(timer_events).await?;
    }

    let summary = RunSummary {
        ready_dispatch_emitted: ready_emitted,
        ready_dispatch_skipped: ready_skipped,
        dispatch_actions: dispatch_actions.len(),
        dispatch_enqueued,
        dispatch_deduplicated,
        dispatch_failed,
        timer_actions: timer_actions.len(),
        timer_enqueued,
        timer_deduplicated,
        timer_failed,
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
    match std::env::var(key) {
        Ok(value) => value.eq_ignore_ascii_case("true"),
        Err(_) => default,
    }
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid PORT"));
    }

    if let Ok(port) = std::env::var("ARCO_SERVO_PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid ARCO_SERVO_PORT"));
    }

    Ok(8080)
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

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
    let dispatch_target_url = required_env("ARCO_SERVO_DISPATCH_TARGET_URL")?;
    let project_id = required_env("ARCO_GCP_PROJECT_ID")?;
    let location = required_env("ARCO_GCP_LOCATION")?;
    let queue_name = optional_env("ARCO_SERVO_QUEUE").unwrap_or_else(|| "servo-dispatch".to_string());
    let timer_target_url = optional_env("ARCO_SERVO_TIMER_TARGET_URL");
    let timer_queue = optional_env("ARCO_SERVO_TIMER_QUEUE");
    let service_account_email = optional_env("ARCO_SERVO_SERVICE_ACCOUNT_EMAIL");
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

    let apply_queue_updates = parse_bool_env("ARCO_SERVO_APPLY_QUEUE_RETRY_CONFIG", false);
    if !apply_queue_updates {
        cloud_config = cloud_config.with_queue_retry_updates(false);
    }

    if let Ok(timeout) = std::env::var("ARCO_SERVO_TASK_TIMEOUT_SECS") {
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
        timer_target_url,
        timer_queue,
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
