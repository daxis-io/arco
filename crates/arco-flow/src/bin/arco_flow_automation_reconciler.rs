//! Arco Flow automation reconciler service.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;

use arco_core::ScopedStorage;
use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::FoldState;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::{
    DispatchOutboxRow, TimerRow, TimerState, TimerType,
};
use arco_flow::orchestration::compactor::manifest::Watermarks;
use arco_flow::orchestration::controllers::{
    BackfillController, HeartbeatAction, HeartbeatHandler, PartitionResolver, PollSensorController,
    RetryAction, RetryHandler, RunRequestProcessor, ScheduleController,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskOutcome, TimerType as EventTimerType,
};

const DEFAULT_WORKER_QUEUE: &str = "default-queue";

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    message: String,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize, Default)]
struct RunSummary {
    schedule_events: usize,
    sensor_events: usize,
    backfill_events: usize,
    timer_events: usize,
    run_request_events: usize,
    total_events: usize,
}

#[derive(Debug)]
struct PendingTimerEvents {
    events: Vec<OrchestrationEvent>,
    emitted: usize,
}

#[derive(Debug)]
struct ProjectionPartitionResolver;

impl PartitionResolver for ProjectionPartitionResolver {
    fn count_range(&self, _asset_key: &str, _start: &str, _end: &str) -> u32 {
        0
    }

    fn list_range_chunk(
        &self,
        _asset_key: &str,
        _start: &str,
        _end: &str,
        _offset: u32,
        _limit: u32,
    ) -> Vec<String> {
        Vec::new()
    }
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;
    let now = Utc::now();

    let mut summary = RunSummary::default();
    let mut pending = Vec::<OrchestrationEvent>::new();

    let schedule_controller = ScheduleController::new();
    let definitions: Vec<_> = fold_state.schedule_definitions.values().cloned().collect();
    let schedule_state: Vec<_> = fold_state.schedule_state.values().cloned().collect();
    let schedule_events = schedule_controller.reconcile(&definitions, &schedule_state, now);
    summary.schedule_events = schedule_events.len();
    pending.extend(schedule_events);

    let sensor_controller = PollSensorController::new();
    let mut sensor_events = Vec::new();
    let poll_epoch = manifest.watermarks.last_processed_at.timestamp();
    for row in fold_state.sensor_state.values() {
        if !sensor_controller.should_evaluate(row, now) {
            continue;
        }
        sensor_events.extend(sensor_controller.evaluate(
            &row.sensor_id,
            &row.tenant_id,
            &row.workspace_id,
            row,
            poll_epoch,
        ));
    }
    summary.sensor_events = sensor_events.len();
    pending.extend(sensor_events);

    let backfill_controller = BackfillController::new(Arc::new(ProjectionPartitionResolver));
    let backfill_events = backfill_controller.reconcile(
        &manifest.watermarks,
        &fold_state.backfills,
        &fold_state.backfill_chunks,
        &fold_state.runs,
    );
    summary.backfill_events = backfill_events.len();
    pending.extend(backfill_events);

    let timer_events = process_fired_timers(
        &fold_state,
        &manifest.watermarks,
        &state.tenant_id,
        &state.workspace_id,
        now,
    );
    summary.timer_events = timer_events.emitted;
    pending.extend(timer_events.events);

    let run_request_processor = RunRequestProcessor::new();
    let run_request_events = run_request_processor.reconcile(&fold_state);
    summary.run_request_events = run_request_events.len();
    pending.extend(run_request_events);

    summary.total_events = pending.len();
    if !pending.is_empty() {
        state.ledger.append_all(pending).await?;
    }

    Ok(Json(summary))
}

fn process_fired_timers(
    fold_state: &FoldState,
    watermarks: &Watermarks,
    tenant_id: &str,
    workspace_id: &str,
    now: DateTime<Utc>,
) -> PendingTimerEvents {
    let heartbeat_handler = HeartbeatHandler::with_defaults();
    let retry_handler = RetryHandler::with_defaults();
    let mut emitted = 0usize;
    let mut events = Vec::new();

    for timer in fold_state
        .timers
        .values()
        .filter(|timer| timer.state == TimerState::Fired)
    {
        match timer.timer_type {
            TimerType::HeartbeatCheck => {
                let (Some(run_id), Some(task_key)) =
                    (timer.run_id.as_ref(), timer.task_key.as_ref())
                else {
                    continue;
                };
                let Some(task) = fold_state.tasks.get(&(run_id.clone(), task_key.clone())) else {
                    continue;
                };

                match heartbeat_handler.check(watermarks, task, now) {
                    HeartbeatAction::Reschedule { delay, .. } => {
                        if let Some(event) = reschedule_timer_event(
                            tenant_id,
                            workspace_id,
                            timer,
                            now + delay,
                            "heartbeat_reschedule",
                        ) {
                            emitted += 1;
                            events.push(event);
                        }
                    }
                    HeartbeatAction::FailTask {
                        run_id,
                        task_key,
                        attempt,
                        attempt_id,
                        reason,
                    } => {
                        let task_row = fold_state.tasks.get(&(run_id.clone(), task_key.clone()));
                        let code_version = fold_state
                            .runs
                            .get(run_id.as_str())
                            .and_then(|run| run.code_version.clone());

                        let task_finished = OrchestrationEvent::new(
                            tenant_id,
                            workspace_id,
                            OrchestrationEventData::TaskFinished {
                                run_id,
                                task_key,
                                attempt,
                                attempt_id,
                                worker_id: "arco_flow_automation_reconciler".to_string(),
                                outcome: TaskOutcome::Failed,
                                materialization_id: None,
                                error_message: Some(reason),
                                output: None,
                                error: None,
                                metrics: None,
                                cancelled_during_phase: None,
                                partial_progress: None,
                                asset_key: task_row.and_then(|row| row.asset_key.clone()),
                                partition_key: task_row.and_then(|row| row.partition_key.clone()),
                                code_version,
                            },
                        );
                        emitted += 1;
                        events.push(task_finished);
                    }
                    HeartbeatAction::NoOp { .. } => {}
                }
            }
            TimerType::Retry => {
                let (Some(run_id), Some(task_key)) =
                    (timer.run_id.as_ref(), timer.task_key.as_ref())
                else {
                    continue;
                };
                let Some(task) = fold_state.tasks.get(&(run_id.clone(), task_key.clone())) else {
                    continue;
                };

                match retry_handler.fire(watermarks, task, now) {
                    RetryAction::Reschedule { delay, .. } => {
                        if let Some(event) = reschedule_timer_event(
                            tenant_id,
                            workspace_id,
                            timer,
                            now + delay,
                            "retry_reschedule",
                        ) {
                            emitted += 1;
                            events.push(event);
                        }
                    }
                    RetryAction::IncrementAttempt {
                        run_id,
                        task_key,
                        new_attempt,
                        new_attempt_id,
                    } => {
                        let dispatch_id =
                            DispatchOutboxRow::dispatch_id(&run_id, &task_key, new_attempt);
                        let dispatch_requested = OrchestrationEvent::new(
                            tenant_id,
                            workspace_id,
                            OrchestrationEventData::DispatchRequested {
                                run_id,
                                task_key,
                                attempt: new_attempt,
                                attempt_id: new_attempt_id,
                                worker_queue: DEFAULT_WORKER_QUEUE.to_string(),
                                dispatch_id,
                            },
                        );
                        emitted += 1;
                        events.push(dispatch_requested);
                    }
                    RetryAction::NoOp { .. } => {}
                }
            }
            TimerType::Cron | TimerType::SlaCheck => {}
        }
    }

    PendingTimerEvents { events, emitted }
}

fn reschedule_timer_event(
    tenant_id: &str,
    workspace_id: &str,
    timer: &TimerRow,
    fire_at: DateTime<Utc>,
    suffix: &str,
) -> Option<OrchestrationEvent> {
    let timer_type = match timer.timer_type {
        TimerType::Retry => EventTimerType::Retry,
        TimerType::HeartbeatCheck => EventTimerType::HeartbeatCheck,
        TimerType::Cron => EventTimerType::Cron,
        TimerType::SlaCheck => EventTimerType::SlaCheck,
    };

    Some(OrchestrationEvent::new(
        tenant_id,
        workspace_id,
        OrchestrationEventData::TimerRequested {
            timer_id: format!("{}:{suffix}", timer.timer_id),
            timer_type,
            run_id: timer.run_id.clone(),
            task_key: timer.task_key.clone(),
            attempt: timer.attempt,
            fire_at,
        },
    ))
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
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

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let port = resolve_port()?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id.clone(), workspace_id.clone())?;

    let state = AppState {
        tenant_id,
        workspace_id,
        compactor: MicroCompactor::new(storage.clone()),
        ledger: LedgerWriter::new(storage),
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
