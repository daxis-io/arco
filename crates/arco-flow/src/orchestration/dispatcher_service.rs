//! Operator-hosted Flow dispatcher router.

use std::result::Result;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::dispatch::{EnqueueOptions, EnqueueResult, HttpTaskEnqueuer, enqueue_worker_dispatch};
use crate::error::Error;
use crate::orchestration::LedgerWriter;
use crate::orchestration::compactor::MicroCompactor;
use crate::orchestration::controllers::{
    DispatchAction, DispatcherController, ReadyDispatchController, TimerAction, TimerController,
};
use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use crate::orchestration::flow_service::append_events_and_compact;
use crate::orchestration::worker_contract::{DispatchEnvelopeSpec, dispatch_envelope_for_attempt};
use arco_core::{ScopedStorage, TaskTokenConfig, mint_task_token_for_attempt};

/// Operator-selected Flow dispatch endpoints and authentication settings.
#[derive(Clone)]
pub struct DispatcherServiceConfig {
    /// Optional remote Flow compactor URL.
    pub orch_compactor_url: Option<String>,
    /// Headers delivered to the worker.
    pub worker_dispatch_headers: std::collections::HashMap<String, String>,
    /// Worker dispatch endpoint.
    pub dispatch_target_url: String,
    /// Authentication audience for worker delivery.
    pub dispatch_target_audience: String,
    /// API callback base URL placed in worker envelopes.
    pub callback_base_url: String,
    /// Configuration for scoped task tokens.
    pub task_token_config: TaskTokenConfig,
    /// Maximum worker task duration used to validate token lifetime.
    pub task_timeout_secs: u64,
    /// Optional timer callback endpoint.
    pub timer_target_url: Option<String>,
    /// Optional authentication audience for timers.
    pub timer_target_audience: Option<String>,
    /// Optional routing key for timer tasks.
    pub timer_queue: Option<String>,
}

/// A dispatcher bound to one workspace authority root and one queue.
#[derive(Clone)]
pub struct DispatcherServiceState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    orch_compactor_url: Option<String>,
    task_queue: Arc<dyn HttpTaskEnqueuer>,
    worker_dispatch_headers: std::collections::HashMap<String, String>,
    dispatch_target_url: String,
    dispatch_target_audience: String,
    callback_base_url: String,
    task_token_config: TaskTokenConfig,
    timer_target_url: Option<String>,
    timer_target_audience: Option<String>,
    timer_queue: Option<String>,
}

impl DispatcherServiceState {
    /// Binds the controller and queue to one workspace root before I/O.
    ///
    /// # Errors
    ///
    /// Rejects another root kind or an invalid task-token lifetime.
    pub fn new(
        storage: ScopedStorage,
        task_queue: Arc<dyn HttpTaskEnqueuer>,
        config: DispatcherServiceConfig,
    ) -> Result<Self, Error> {
        let workspace_id = storage.scope().workspace_id().ok_or_else(|| {
            Error::configuration("Flow dispatcher requires a workspace authority root")
        })?;
        config
            .task_token_config
            .validate_for_dispatch(config.task_timeout_secs, true)
            .map_err(|error| Error::configuration(error.to_string()))?;
        Ok(Self {
            tenant_id: storage.tenant_id().to_string(),
            workspace_id: workspace_id.to_string(),
            compactor: MicroCompactor::new(storage.clone()),
            ledger: LedgerWriter::new(storage),
            orch_compactor_url: config.orch_compactor_url,
            task_queue,
            worker_dispatch_headers: config.worker_dispatch_headers,
            dispatch_target_url: config.dispatch_target_url,
            dispatch_target_audience: config.dispatch_target_audience,
            callback_base_url: config.callback_base_url,
            task_token_config: config.task_token_config,
            timer_target_url: config.timer_target_url,
            timer_target_audience: config.timer_target_audience,
            timer_queue: config.timer_queue,
        })
    }
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

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<DispatcherServiceState>,
) -> Result<Json<RunSummary>, ApiError> {
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
        append_events_and_compact(
            &state.ledger,
            state.orch_compactor_url.as_deref(),
            ready_events,
        )
        .await?;
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
        } = action
        else {
            continue;
        };

        let callback_task_id = arco_worker_contract::callback_task_id(run_id, task_key);
        let minted = mint_task_token_for_attempt(
            &state.task_token_config,
            callback_task_id,
            state.tenant_id.clone(),
            state.workspace_id.clone(),
            run_id.clone(),
            *attempt,
            attempt_id.clone(),
            Utc::now(),
        )
        .map_err(|e| Error::configuration(format!("task token minting failed: {e}")))?;

        let task_row = fold_state
            .tasks
            .get(&(run_id.clone(), task_key.clone()))
            .ok_or_else(|| {
                Error::dispatch(format!(
                    "refusing unscoped dispatch for missing task row: run={run_id} task={task_key}"
                ))
            })?;
        let mut envelope = dispatch_envelope_for_attempt(
            DispatchEnvelopeSpec {
                tenant_id: state.tenant_id.clone(),
                workspace_id: state.workspace_id.clone(),
                run_id: run_id.clone(),
                task_key: task_key.clone(),
                attempt: *attempt,
                attempt_id: attempt_id.clone(),
                dispatch_id: dispatch_id.clone(),
                worker_queue: worker_queue.clone(),
                callback_base_url: state.callback_base_url.clone(),
                task_token: minted.token,
                token_expires_at: minted.expires_at,
            },
            Some(task_row),
        );
        let run = fold_state
            .runs
            .get(run_id)
            .ok_or_else(|| Error::dispatch("dispatch run is missing"))?;
        crate::orchestration::accepted_plan::populate_accepted_payload(
            &state.ledger.storage(),
            run,
            &mut envelope,
        )
        .await?;
        let result = enqueue_worker_dispatch(
            state.task_queue.as_ref(),
            cloud_task_id,
            &state.dispatch_target_url,
            &state.dispatch_target_audience,
            &state.worker_dispatch_headers,
            &envelope,
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
        append_events_and_compact(
            &state.ledger,
            state.orch_compactor_url.as_deref(),
            dispatch_events,
        )
        .await?;
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
            message: "ARCO_FLOW_TIMER_TARGET_URL is not set".to_string(),
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
        } = action
        else {
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
        let delay = fire_at.signed_duration_since(now).to_std().ok();

        let mut options = EnqueueOptions::new();
        if let Some(delay) = delay {
            options = options.with_delay(delay);
        }
        if let Some(queue) = state.timer_queue.as_ref() {
            options = options.with_routing_key(queue.clone());
        }

        let result = state
            .task_queue
            .enqueue_http(
                cloud_task_id,
                target_url,
                &body,
                options,
                Some(
                    state
                        .timer_target_audience
                        .as_deref()
                        .unwrap_or(target_url.as_str()),
                ),
                None,
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
        append_events_and_compact(
            &state.ledger,
            state.orch_compactor_url.as_deref(),
            timer_events,
        )
        .await?;
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

/// Builds the dispatcher HTTP routes with an operator-supplied queue.
///
/// The caller owns listener placement, authentication to this control route,
/// scheduling, and queue delivery. `POST /run` performs one reconciliation.
pub fn router(state: DispatcherServiceState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/run", post(run_handler))
        .with_state(state)
}
