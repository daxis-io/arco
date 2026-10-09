//! Callback handler implementations per ADR-023.
//!
//! These handlers are framework-agnostic - they take parsed requests and return
//! response types. HTTP routing and serialization is handled by the API layer.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use arco_worker_contract::{PublicationDescriptor, callback_task_id};

use super::types::{
    CallbackError, CallbackResult, HeartbeatRequest, HeartbeatResponse, TaskCompletedRequest,
    TaskCompletedResponse, TaskOutputVisibilityState, TaskStartedRequest, TaskStartedResponse,
    WorkerOutcome,
};
use crate::metrics::{labels as metrics_labels, names as metrics_names};
use crate::orchestration::OrchestrationLedgerWriter;
use crate::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, OutputVisibilityState, OutputVisibilityUpdate,
    TaskOutcome,
};

/// Largest difference tolerated between a worker-reported timestamp and the
/// server's receipt time before the report is flagged as clock skew.
///
/// Reports outside this band are still accepted — a callback must never fail
/// because a worker's clock drifted — but they are recorded as skew and never
/// become the event's time.
const MAX_WORKER_CLOCK_SKEW: chrono::Duration = chrono::Duration::minutes(5);
const PUBLICATION_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Returns the server receipt time for a callback and records observed skew.
///
/// Worker-reported timestamps (`startedAt`, `heartbeatAt`, `completedAt`) are
/// *observations*, not authority. Copying them into `OrchestrationEvent::timestamp`
/// let a skewed or hostile worker steer server-side scheduling and retention:
/// a completion stamped years in the future pushed `retry_not_before` past any
/// horizon so the attempt never retried, and one stamped years in the past made
/// the just-finished run look older than the retention window so the merged
/// retention sweep tombstoned fresh terminal evidence. Both are avoided by
/// timing events from the server's own clock at the moment the callback is
/// accepted.
///
/// The reported value is kept as span metadata so operators can still see and
/// diagnose worker clock drift.
fn server_receipt_time(handler: &str, reported: Option<DateTime<Utc>>) -> DateTime<Utc> {
    let server_now = Utc::now();
    if let Some(reported) = reported {
        let skew = reported.signed_duration_since(server_now);
        if skew.abs() > MAX_WORKER_CLOCK_SKEW {
            tracing::Span::current().record("worker_clock_skew_secs", skew.num_seconds());
            tracing::warn!(
                handler,
                reported = %reported,
                server_now = %server_now,
                skew_secs = skew.num_seconds(),
                "worker-reported timestamp is outside the tolerated skew band; \
                 using server receipt time for the event"
            );
            metrics::counter!(
                metrics_names::ORCH_CALLBACKS_TOTAL,
                metrics_labels::HANDLER => handler.to_string(),
                metrics_labels::STATUS => "worker_clock_skew".to_string(),
            )
            .increment(1);
        }
    }
    server_now
}

/// Context for callback handlers.
pub struct CallbackContext<W: OrchestrationLedgerWriter, V: TaskTokenValidator> {
    /// Ledger writer for emitting events.
    pub ledger: Arc<W>,
    /// Token validator for task callbacks.
    pub token_validator: Arc<V>,
    /// Tenant ID from request context.
    pub tenant_id: String,
    /// Workspace ID from request context.
    pub workspace_id: String,
    /// Owner-controlled publication verifier. Absence fails publication closed.
    pub publication_verifier: Option<Arc<dyn super::PublicationVerifier>>,
}

impl<W: OrchestrationLedgerWriter, V: TaskTokenValidator> CallbackContext<W, V> {
    /// Creates a new callback context.
    #[must_use]
    pub fn new(
        ledger: Arc<W>,
        token_validator: Arc<V>,
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            ledger,
            token_validator,
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            publication_verifier: None,
        }
    }

    /// Adds owner-controlled publication verification for this callback scope.
    #[must_use]
    pub fn with_publication_verifier(
        mut self,
        verifier: Arc<dyn super::PublicationVerifier>,
    ) -> Self {
        self.publication_verifier = Some(verifier);
        self
    }
}

/// State of a task for callback validation.
#[derive(Debug, Clone)]
pub struct TaskState {
    /// Current task state.
    pub state: String,
    /// Current attempt number.
    pub attempt: u32,
    /// Current attempt ID.
    pub attempt_id: String,
    /// Run ID this task belongs to.
    pub run_id: String,
    /// Semantic task key for event emission.
    pub task_key: String,
    /// Asset key for this task (if any).
    pub asset_key: Option<String>,
    /// Partition key for this task (if any).
    pub partition_key: Option<String>,
    /// Code version for the run (if available).
    pub code_version: Option<String>,
    /// Whether cancellation has been requested.
    pub cancel_requested: bool,
    /// Whether downstream progress requires a verified readable output.
    pub requires_visible_output: bool,
}

impl TaskState {
    /// Returns true if the task is in a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state.as_str(),
            "SUCCEEDED" | "FAILED" | "SKIPPED" | "CANCELLED"
        )
    }
}

/// Trait for looking up task state.
///
/// This is implemented by the state store (Parquet-based or in-memory for tests).
pub trait TaskStateLookup: Send + Sync {
    /// Looks up the current state of a task.
    ///
    /// Returns `None` if the task doesn't exist.
    fn get_task_state(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<TaskState>, String>> + Send;

    /// Looks up a publication claim already bound to the current attempt.
    ///
    /// Legacy stores may return `None`; a later publication may then establish
    /// the first immutable identity for an already successful attempt.
    fn get_task_publication(
        &self,
        _task_id: &str,
    ) -> impl Future<Output = Result<Option<PublicationDescriptor>, String>> + Send {
        async { Ok(None) }
    }
}

/// Trait for validating task tokens.
pub trait TaskTokenValidator: Send + Sync {
    /// Validates the task token for the given task ID.
    fn validate_task_token(
        &self,
        task_id: &str,
        run_id: &str,
        attempt: u32,
        attempt_id: &str,
        token: &str,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

fn record_callback_metrics<T>(handler: &str, result: &CallbackResult<T>) {
    let status = result.status_code().to_string();
    metrics::counter!(
        metrics_names::ORCH_CALLBACKS_TOTAL,
        metrics_labels::HANDLER => handler.to_string(),
        metrics_labels::RESULT => status.clone(),
    )
    .increment(1);

    if !matches!(result, CallbackResult::Ok(_)) {
        metrics::counter!(
            metrics_names::ORCH_CALLBACK_ERRORS_TOTAL,
            metrics_labels::HANDLER => handler.to_string(),
            metrics_labels::RESULT => status,
        )
        .increment(1);
    }
}

fn finish_callback<T>(handler: &str, result: CallbackResult<T>) -> CallbackResult<T> {
    record_callback_metrics(handler, &result);
    result
}

fn lookup_error<T>(handler: &str, task_id: &str, error: String) -> CallbackResult<T> {
    if error.starts_with("task_id_ambiguous:") {
        finish_callback(
            handler,
            CallbackResult::BadRequest(CallbackError::task_id_ambiguous(task_id)),
        )
    } else {
        finish_callback(handler, CallbackResult::InternalError(error))
    }
}

async fn validate_task_token_for_state<T, V>(
    handler: &str,
    validator: &V,
    path_task_id: &str,
    state: &TaskState,
    attempt: u32,
    task_token: &str,
) -> Option<CallbackResult<T>>
where
    V: TaskTokenValidator,
{
    let canonical_task_id = callback_task_id(&state.run_id, &state.task_key);
    let canonical_result = validator
        .validate_task_token(
            &canonical_task_id,
            &state.run_id,
            attempt,
            &state.attempt_id,
            task_token,
        )
        .await;

    match canonical_result {
        Ok(()) => None,
        Err(reason) if reason == "task_id_mismatch" && path_task_id != canonical_task_id => {
            match validator
                .validate_task_token(
                    path_task_id,
                    &state.run_id,
                    attempt,
                    &state.attempt_id,
                    task_token,
                )
                .await
            {
                Ok(()) => None,
                Err(legacy_reason) => {
                    let reason = if legacy_reason == "task_id_mismatch" {
                        reason
                    } else {
                        legacy_reason
                    };
                    Some(finish_callback(
                        handler,
                        CallbackResult::Unauthorized(CallbackError::invalid_token(&reason)),
                    ))
                }
            }
        }
        Err(reason) => Some(finish_callback(
            handler,
            CallbackResult::Unauthorized(CallbackError::invalid_token(&reason)),
        )),
    }
}

async fn verify_publication<W, V>(
    ctx: &CallbackContext<W, V>,
    descriptor: &mut PublicationDescriptor,
) -> Result<(), String>
where
    W: OrchestrationLedgerWriter,
    V: TaskTokenValidator,
{
    descriptor.owner_evidence = None;
    let verifier = ctx
        .publication_verifier
        .as_ref()
        .ok_or_else(|| "publication owner verifier is unavailable".to_string())?;
    let evidence = tokio::time::timeout(
        PUBLICATION_VERIFICATION_TIMEOUT,
        verifier.verify(descriptor),
    )
    .await
    .map_err(|_| "publication owner verification timed out".to_string())??;
    descriptor.owner_evidence = Some(evidence);
    Ok(())
}

/// Handles the `/v1/tasks/{task_id}/started` callback.
///
/// Validates the attempt number and emits a `TaskStarted` event.
#[tracing::instrument(
    skip(ctx, request, lookup, task_token),
    fields(
        tenant_id = %ctx.tenant_id,
        workspace_id = %ctx.workspace_id,
        task_id = %task_id,
        run_id = tracing::field::Empty,
        attempt = tracing::field::Empty,
        attempt_id = tracing::field::Empty,
        worker_id = tracing::field::Empty,
        traceparent = tracing::field::Empty,
    )
)]
#[allow(clippy::too_many_lines)]
pub async fn handle_task_started<W, V, L>(
    ctx: &CallbackContext<W, V>,
    task_id: &str,
    task_token: &str,
    request: TaskStartedRequest,
    lookup: &L,
) -> CallbackResult<TaskStartedResponse>
where
    W: OrchestrationLedgerWriter,
    V: TaskTokenValidator,
    L: TaskStateLookup,
{
    let _guard = crate::metrics::TimingGuard::new(|duration| {
        metrics::histogram!(
            metrics_names::ORCH_CALLBACK_DURATION_SECONDS,
            metrics_labels::HANDLER => "task_started".to_string(),
        )
        .record(duration.as_secs_f64());
    });

    let TaskStartedRequest {
        attempt,
        attempt_id,
        worker_id,
        traceparent,
        started_at,
    } = request;

    if attempt == 0 {
        return finish_callback(
            "task_started",
            CallbackResult::BadRequest(CallbackError::invalid_argument("attempt", "must be >= 1")),
        );
    }

    // Look up current task state
    let state = match lookup.get_task_state(task_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return finish_callback(
                "task_started",
                CallbackResult::NotFound(CallbackError::task_not_found(task_id)),
            );
        }
        Err(e) => {
            return lookup_error("task_started", task_id, e);
        }
    };

    tracing::Span::current().record("run_id", tracing::field::display(&state.run_id));
    tracing::Span::current().record("attempt", tracing::field::display(attempt));
    tracing::Span::current().record("attempt_id", tracing::field::display(&attempt_id));
    tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
    if let Some(traceparent) = &traceparent {
        tracing::Span::current().record("traceparent", tracing::field::display(traceparent));
    }

    if let Some(result) = validate_task_token_for_state(
        "task_started",
        ctx.token_validator.as_ref(),
        task_id,
        &state,
        state.attempt,
        task_token,
    )
    .await
    {
        return result;
    }

    // Check if task is already terminal
    if state.is_terminal() {
        return finish_callback(
            "task_started",
            CallbackResult::Conflict(CallbackError::task_already_terminal(&state.state)),
        );
    }

    // Durable terminal dedup for the attempt itself (issue #328 / H5).
    //
    // A failed attempt with retries left leaves the task in RETRY_WAIT while
    // `attempt` still names the attempt that just finished, so a redelivered
    // dispatch for that attempt used to pass every check below and re-execute
    // the asset. Worker-local memory cannot close this: the process that
    // reported the completion may have died before recording anything, and the
    // replacement worker starts with an empty set. Only control-plane state
    // knows the attempt is finished, so it answers authoritatively here and
    // the worker suppresses execution on this response.
    if state.state == "RETRY_WAIT" && attempt == state.attempt {
        return finish_callback(
            "task_started",
            CallbackResult::Conflict(CallbackError::attempt_already_completed(
                &state.state,
                attempt,
            )),
        );
    }

    // An attempt older than the current one is already covered by the
    // `attempt_mismatch` conflict below, which a worker treats as equally
    // authoritative: the control plane has moved past that attempt.

    // Validate attempt number
    if attempt != state.attempt {
        return finish_callback(
            "task_started",
            CallbackResult::Conflict(CallbackError::attempt_mismatch(state.attempt, attempt)),
        );
    }

    if attempt_id != state.attempt_id {
        return finish_callback(
            "task_started",
            CallbackResult::Conflict(CallbackError::attempt_id_mismatch(
                &state.attempt_id,
                &attempt_id,
            )),
        );
    }

    // If cancellation requested before start, return conflict to stop the worker.
    if state.cancel_requested {
        return finish_callback(
            "task_started",
            CallbackResult::Conflict(CallbackError::task_already_terminal("CANCELLED")),
        );
    }

    // Emit TaskStarted event
    let mut event = OrchestrationEvent::new(
        &ctx.tenant_id,
        &ctx.workspace_id,
        OrchestrationEventData::TaskStarted {
            run_id: state.run_id.clone(),
            task_key: state.task_key.clone(),
            attempt,
            attempt_id: state.attempt_id.clone(),
            worker_id,
        },
    );
    // The worker's `startedAt` is observation metadata only; the event is timed
    // from the server's receipt of the callback (see `server_receipt_time`).
    event.timestamp = server_receipt_time("task_started", started_at);

    if let Err(e) = ctx.ledger.write_event(&event).await {
        return finish_callback(
            "task_started",
            CallbackResult::InternalError(format!("Failed to write event: {e}")),
        );
    }

    finish_callback(
        "task_started",
        CallbackResult::Ok(TaskStartedResponse {
            acknowledged: true,
            server_time: Utc::now(),
        }),
    )
}

/// Handles the `/v1/tasks/{task_id}/heartbeat` callback.
///
/// Updates the last heartbeat time and checks for cancellation signals.
#[tracing::instrument(
    skip(ctx, request, lookup, task_token),
    fields(
        tenant_id = %ctx.tenant_id,
        workspace_id = %ctx.workspace_id,
        task_id = %task_id,
        run_id = tracing::field::Empty,
        attempt = tracing::field::Empty,
        attempt_id = tracing::field::Empty,
        worker_id = tracing::field::Empty,
        traceparent = tracing::field::Empty,
    )
)]
#[allow(clippy::too_many_lines)]
pub async fn handle_heartbeat<W, V, L>(
    ctx: &CallbackContext<W, V>,
    task_id: &str,
    task_token: &str,
    request: HeartbeatRequest,
    lookup: &L,
) -> CallbackResult<HeartbeatResponse>
where
    W: OrchestrationLedgerWriter,
    V: TaskTokenValidator,
    L: TaskStateLookup,
{
    let _guard = crate::metrics::TimingGuard::new(|duration| {
        metrics::histogram!(
            metrics_names::ORCH_CALLBACK_DURATION_SECONDS,
            metrics_labels::HANDLER => "heartbeat".to_string(),
        )
        .record(duration.as_secs_f64());
    });

    let HeartbeatRequest {
        attempt,
        attempt_id,
        worker_id,
        traceparent,
        heartbeat_at,
        progress_pct,
        message,
    } = request;

    // A heartbeat is proof that the server heard from the worker *now*. Taking
    // the recorded liveness time from the worker let a skewed clock either push
    // `last_heartbeat_at` far into the future (making a dead worker's task
    // permanently un-reapable) or far into the past (making a live worker's
    // task look stale). Both are avoided by recording server receipt time.
    let event_timestamp = server_receipt_time("heartbeat", heartbeat_at);
    let heartbeat_at = Some(event_timestamp);

    if attempt == 0 {
        return finish_callback(
            "heartbeat",
            CallbackResult::BadRequest(CallbackError::invalid_argument("attempt", "must be >= 1")),
        );
    }

    if let Some(progress_pct) = progress_pct {
        if progress_pct > 100 {
            return finish_callback(
                "heartbeat",
                CallbackResult::BadRequest(CallbackError::invalid_argument(
                    "progressPct",
                    "must be between 0 and 100",
                )),
            );
        }
    }

    // Look up current task state
    let state = match lookup.get_task_state(task_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return finish_callback(
                "heartbeat",
                CallbackResult::NotFound(CallbackError::task_not_found(task_id)),
            );
        }
        Err(e) => {
            return lookup_error("heartbeat", task_id, e);
        }
    };

    tracing::Span::current().record("run_id", tracing::field::display(&state.run_id));
    tracing::Span::current().record("attempt", tracing::field::display(attempt));
    tracing::Span::current().record("attempt_id", tracing::field::display(&attempt_id));
    tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
    if let Some(traceparent) = &traceparent {
        tracing::Span::current().record("traceparent", tracing::field::display(traceparent));
    }

    if let Some(result) = validate_task_token_for_state(
        "heartbeat",
        ctx.token_validator.as_ref(),
        task_id,
        &state,
        state.attempt,
        task_token,
    )
    .await
    {
        return result;
    }

    // Check if task is no longer active (410 Gone)
    if state.is_terminal() {
        let mut error = CallbackError::task_expired();
        error.state = Some(state.state.clone());
        return finish_callback("heartbeat", CallbackResult::Gone(error));
    }

    // Validate attempt number
    if attempt != state.attempt {
        return finish_callback(
            "heartbeat",
            CallbackResult::Conflict(CallbackError::attempt_mismatch(state.attempt, attempt)),
        );
    }

    if attempt_id != state.attempt_id {
        return finish_callback(
            "heartbeat",
            CallbackResult::Conflict(CallbackError::attempt_id_mismatch(
                &state.attempt_id,
                &attempt_id,
            )),
        );
    }

    // Emit TaskHeartbeat event
    let mut event = OrchestrationEvent::new(
        &ctx.tenant_id,
        &ctx.workspace_id,
        OrchestrationEventData::TaskHeartbeat {
            run_id: state.run_id.clone(),
            task_key: state.task_key.clone(),
            attempt,
            attempt_id: state.attempt_id.clone(),
            worker_id,
            heartbeat_at,
            progress_pct,
            message,
        },
    );
    event.timestamp = event_timestamp;

    if let Err(e) = ctx.ledger.write_event(&event).await {
        return finish_callback(
            "heartbeat",
            CallbackResult::InternalError(format!("Failed to write event: {e}")),
        );
    }

    // Check if cancellation was requested
    let (should_cancel, cancel_reason) = if state.cancel_requested {
        (true, Some("user_requested".to_string()))
    } else {
        (false, None)
    };

    finish_callback(
        "heartbeat",
        CallbackResult::Ok(HeartbeatResponse {
            acknowledged: true,
            should_cancel,
            cancel_reason,
            server_time: Utc::now(),
        }),
    )
}

/// Handles the `/v1/tasks/{task_id}/completed` callback.
///
/// Records the task result and transitions the task to a terminal state.
#[tracing::instrument(
    skip(ctx, request, lookup, task_token),
    fields(
        tenant_id = %ctx.tenant_id,
        workspace_id = %ctx.workspace_id,
        task_id = %task_id,
        run_id = tracing::field::Empty,
        attempt = tracing::field::Empty,
        attempt_id = tracing::field::Empty,
        worker_id = tracing::field::Empty,
        traceparent = tracing::field::Empty,
    )
)]
#[allow(clippy::too_many_lines)]
pub async fn handle_task_completed<W, V, L>(
    ctx: &CallbackContext<W, V>,
    task_id: &str,
    task_token: &str,
    request: TaskCompletedRequest,
    lookup: &L,
) -> CallbackResult<TaskCompletedResponse>
where
    W: OrchestrationLedgerWriter,
    V: TaskTokenValidator,
    L: TaskStateLookup,
{
    let _guard = crate::metrics::TimingGuard::new(|duration| {
        metrics::histogram!(
            metrics_names::ORCH_CALLBACK_DURATION_SECONDS,
            metrics_labels::HANDLER => "task_completed".to_string(),
        )
        .record(duration.as_secs_f64());
    });

    let TaskCompletedRequest {
        attempt,
        attempt_id,
        worker_id,
        traceparent,
        outcome: worker_outcome,
        completed_at,
        output: mut request_output,
        error: request_error,
        metrics: request_metrics,
        cancelled_during_phase,
        partial_progress,
    } = request;

    if attempt == 0 {
        return finish_callback(
            "task_completed",
            CallbackResult::BadRequest(CallbackError::invalid_argument("attempt", "must be >= 1")),
        );
    }

    // Look up current task state
    let state = match lookup.get_task_state(task_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return finish_callback(
                "task_completed",
                CallbackResult::NotFound(CallbackError::task_not_found(task_id)),
            );
        }
        Err(e) => {
            return lookup_error("task_completed", task_id, e);
        }
    };

    tracing::Span::current().record("run_id", tracing::field::display(&state.run_id));
    tracing::Span::current().record("attempt", tracing::field::display(attempt));
    tracing::Span::current().record("attempt_id", tracing::field::display(&attempt_id));
    tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
    if let Some(traceparent) = &traceparent {
        tracing::Span::current().record("traceparent", tracing::field::display(traceparent));
    }

    if let Some(result) = validate_task_token_for_state(
        "task_completed",
        ctx.token_validator.as_ref(),
        task_id,
        &state,
        state.attempt,
        task_token,
    )
    .await
    {
        return result;
    }

    // Validate attempt number
    if attempt != state.attempt {
        return finish_callback(
            "task_completed",
            CallbackResult::Conflict(CallbackError::attempt_mismatch(state.attempt, attempt)),
        );
    }

    if attempt_id != state.attempt_id {
        return finish_callback(
            "task_completed",
            CallbackResult::Conflict(CallbackError::attempt_id_mismatch(
                &state.attempt_id,
                &attempt_id,
            )),
        );
    }

    // A lost response after successful computation may replay the exact
    // completion solely to reconcile publication. Authentication and attempt
    // fencing above run before any storage I/O.
    if state.is_terminal() {
        let publication = request_output
            .as_mut()
            .and_then(|output| output.publication.as_mut());
        if state.state == "SUCCEEDED"
            && worker_outcome == WorkerOutcome::Succeeded
            && let Some(publication) = publication
        {
            match lookup.get_task_publication(task_id).await {
                Ok(Some(existing)) if !existing.same_immutable_claim(publication) => {
                    return finish_callback(
                        "task_completed",
                        CallbackResult::Conflict(CallbackError::invalid_argument(
                            "output.publication",
                            "does not match the publication already bound to this attempt",
                        )),
                    );
                }
                Ok(_) => {}
                Err(error) => return lookup_error("task_completed", task_id, error),
            }
            let (visibility_state, published_at, publish_error) =
                match verify_publication(ctx, publication).await {
                    Ok(()) => (OutputVisibilityState::Visible, Some(Utc::now()), None),
                    Err(error) => (OutputVisibilityState::Failed, None, Some(error)),
                };
            let event = OrchestrationEvent::new(
                &ctx.tenant_id,
                &ctx.workspace_id,
                OrchestrationEventData::TaskOutputVisibilityChanged {
                    run_id: state.run_id.clone(),
                    task_key: state.task_key.clone(),
                    attempt,
                    attempt_id: state.attempt_id.clone(),
                    visibility_state,
                    published_at,
                    publish_error,
                    publication: Some(publication.clone()),
                },
            );
            if let Err(error) = ctx.ledger.write_event(&event).await {
                return finish_callback(
                    "task_completed",
                    CallbackResult::InternalError(format!("Failed to write event: {error}")),
                );
            }
            return finish_callback(
                "task_completed",
                CallbackResult::Ok(TaskCompletedResponse {
                    acknowledged: true,
                    final_state: "SUCCEEDED".to_string(),
                    server_time: Utc::now(),
                }),
            );
        }
        return finish_callback(
            "task_completed",
            CallbackResult::Conflict(CallbackError::task_already_terminal(&state.state)),
        );
    }

    // Map worker outcome to task outcome
    let outcome = match worker_outcome {
        WorkerOutcome::Succeeded => TaskOutcome::Succeeded,
        WorkerOutcome::Failed => TaskOutcome::Failed,
        WorkerOutcome::Cancelled => TaskOutcome::Cancelled,
    };

    // Extract materialization ID and error message
    let materialization_id = request_output
        .as_ref()
        .and_then(|o| o.materialization_id.clone());
    let error_message = request_error.as_ref().map(|e| e.message.clone());
    if let Some(output) = request_output.as_mut() {
        // Worker visibility and owner evidence are claims, never authority.
        output.output_visibility_state = None;
        output.published_at = None;
        output.publish_error = None;
        if let Some(publication) = output.publication.as_mut() {
            publication.owner_evidence = None;
            if worker_outcome == WorkerOutcome::Succeeded {
                output.output_visibility_state = Some(TaskOutputVisibilityState::Pending);
            }
        }
    }
    let publication = request_output
        .as_ref()
        .and_then(|output| output.publication.clone());
    let output = request_output;
    let error_payload = request_error.as_ref().map(|value| {
        let mut normalized = value.clone();
        if normalized.retryable.is_none() {
            normalized.retryable = Some(normalized.effective_retryable());
        }
        normalized
    });
    let error = error_payload;
    let metrics = request_metrics;
    let partial_progress_json = match &partial_progress {
        Some(value) => match serde_json::to_string(value) {
            Ok(payload) => Some(payload),
            Err(e) => {
                return finish_callback(
                    "task_completed",
                    CallbackResult::InternalError(format!(
                        "Failed to serialize partial progress JSON: {e}"
                    )),
                );
            }
        },
        None => None,
    };

    // Persist computation success and the immutable, unverified publication
    // identity before any potentially slow owner I/O. A lost response or
    // process interruption can then retry publication without rerunning work.
    // `completedAt` is worker-reported observation metadata. Timing the
    // completion event from it let a skewed worker both defer its own retry
    // past any horizon and, on the final attempt, make its just-terminal run
    // look older than the retention window so retention erased it.
    let finished_at = server_receipt_time("task_completed", completed_at);
    let output_visibility = (outcome == TaskOutcome::Succeeded
        && (publication.is_some() || state.requires_visible_output))
        .then(|| OutputVisibilityUpdate {
            visibility_state: OutputVisibilityState::Pending,
            published_at: None,
            publish_error: None,
            publication: publication.clone(),
        });

    let mut event = OrchestrationEvent::new(
        &ctx.tenant_id,
        &ctx.workspace_id,
        OrchestrationEventData::TaskCompletionRecorded {
            run_id: state.run_id.clone(),
            task_key: state.task_key.clone(),
            attempt,
            attempt_id: state.attempt_id.clone(),
            worker_id,
            outcome,
            materialization_id,
            error_message,
            output,
            error,
            metrics,
            cancelled_during_phase,
            partial_progress_json,
            asset_key: state.asset_key.clone(),
            partition_key: state.partition_key.clone(),
            code_version: state.code_version.clone(),
            output_visibility,
        },
    );
    event.timestamp = finished_at;

    if let Err(e) = ctx.ledger.write_event(&event).await {
        return finish_callback(
            "task_completed",
            CallbackResult::InternalError(format!("Failed to write event: {e}")),
        );
    }

    if outcome == TaskOutcome::Succeeded
        && let Some(mut publication) = publication
    {
        let (visibility_state, published_at, publish_error) =
            match verify_publication(ctx, &mut publication).await {
                Ok(()) => (OutputVisibilityState::Visible, Some(Utc::now()), None),
                Err(error) => (OutputVisibilityState::Failed, None, Some(error)),
            };
        let visibility_event = OrchestrationEvent::new(
            &ctx.tenant_id,
            &ctx.workspace_id,
            OrchestrationEventData::TaskOutputVisibilityChanged {
                run_id: state.run_id.clone(),
                task_key: state.task_key.clone(),
                attempt,
                attempt_id: state.attempt_id.clone(),
                visibility_state,
                published_at,
                publish_error,
                publication: Some(publication),
            },
        );
        if let Err(error) = ctx.ledger.write_event(&visibility_event).await {
            return finish_callback(
                "task_completed",
                CallbackResult::InternalError(format!("Failed to write event: {error}")),
            );
        }
    }

    // Determine final state string
    let final_state = match worker_outcome {
        WorkerOutcome::Succeeded => "SUCCEEDED",
        WorkerOutcome::Failed => "FAILED",
        WorkerOutcome::Cancelled => "CANCELLED",
    };

    finish_callback(
        "task_completed",
        CallbackResult::Ok(TaskCompletedResponse {
            acknowledged: true,
            final_state: final_state.to_string(),
            server_time: Utc::now(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    /// Mock ledger writer for testing.
    #[derive(Default)]
    struct MockLedger {
        events: Mutex<Vec<OrchestrationEvent>>,
        fail_after_writes: Mutex<Option<usize>>,
    }

    impl MockLedger {
        fn fail_after_writes(writes: usize) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                fail_after_writes: Mutex::new(Some(writes)),
            }
        }
    }

    impl OrchestrationLedgerWriter for MockLedger {
        async fn write_event(&self, event: &OrchestrationEvent) -> Result<(), String> {
            let mut events = self.events.lock().unwrap();
            if self
                .fail_after_writes
                .lock()
                .unwrap()
                .is_some_and(|limit| events.len() >= limit)
            {
                return Err("injected ledger write failure".to_string());
            }
            events.push(event.clone());
            Ok(())
        }

        async fn write_events(&self, batch: Vec<OrchestrationEvent>) -> Result<(), String> {
            let mut events = self.events.lock().unwrap();
            if self
                .fail_after_writes
                .lock()
                .unwrap()
                .is_some_and(|limit| events.len() + batch.len() > limit)
            {
                return Err("injected ledger write failure".to_string());
            }
            events.extend(batch);
            Ok(())
        }
    }

    /// Mock token validator for testing.
    #[derive(Default)]
    struct MockTokenValidator {
        allow: bool,
    }

    impl MockTokenValidator {
        fn allow_all() -> Self {
            Self { allow: true }
        }
    }

    impl TaskTokenValidator for MockTokenValidator {
        async fn validate_task_token(
            &self,
            _task_id: &str,
            _run_id: &str,
            _attempt: u32,
            _attempt_id: &str,
            _token: &str,
        ) -> Result<(), String> {
            if self.allow {
                Ok(())
            } else {
                Err("invalid token".to_string())
            }
        }
    }

    #[derive(Default)]
    struct MockPublicationVerifier {
        calls: AtomicUsize,
    }

    impl super::super::PublicationVerifier for MockPublicationVerifier {
        fn verify<'a>(
            &'a self,
            descriptor: &'a PublicationDescriptor,
        ) -> std::pin::Pin<
            Box<
                dyn Future<Output = Result<super::super::types::PublicationOwnerEvidence, String>>
                    + Send
                    + 'a,
            >,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(super::super::types::PublicationOwnerEvidence {
                    verified_at: Utc::now(),
                    object_version: descriptor.object_version.clone(),
                    etag: None,
                })
            })
        }
    }

    struct BlockingPublicationVerifier {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl super::super::PublicationVerifier for BlockingPublicationVerifier {
        fn verify<'a>(
            &'a self,
            _descriptor: &'a PublicationDescriptor,
        ) -> std::pin::Pin<
            Box<
                dyn Future<Output = Result<super::super::types::PublicationOwnerEvidence, String>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Err("released without publication".to_string())
            })
        }
    }

    fn publication_descriptor() -> PublicationDescriptor {
        let checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        PublicationDescriptor {
            version: 1,
            manifest_id: format!("sha256:{checksum}"),
            object_path: "outputs/result.parquet".to_string(),
            object_version: "1".to_string(),
            checksum_sha256: checksum.to_string(),
            byte_size: 4,
            format: "parquet".to_string(),
            schema_ref: format!("sha256:{checksum}#parquet-schema"),
            owner_evidence: None,
        }
    }

    struct RequiredTaskIdTokenValidator {
        accepted_task_ids: Vec<String>,
        seen_task_ids: Mutex<Vec<String>>,
    }

    impl RequiredTaskIdTokenValidator {
        fn accepting(accepted_task_ids: Vec<String>) -> Self {
            Self {
                accepted_task_ids,
                seen_task_ids: Mutex::new(Vec::new()),
            }
        }
    }

    impl TaskTokenValidator for RequiredTaskIdTokenValidator {
        async fn validate_task_token(
            &self,
            task_id: &str,
            _run_id: &str,
            _attempt: u32,
            _attempt_id: &str,
            _token: &str,
        ) -> Result<(), String> {
            self.seen_task_ids.lock().unwrap().push(task_id.to_string());
            if self
                .accepted_task_ids
                .iter()
                .any(|accepted| accepted == task_id)
            {
                Ok(())
            } else {
                Err("task_id_mismatch".to_string())
            }
        }
    }

    /// Mock task state lookup for testing.
    struct MockTaskLookup {
        tasks: HashMap<String, TaskState>,
        publications: HashMap<String, PublicationDescriptor>,
    }

    impl MockTaskLookup {
        fn new() -> Self {
            Self {
                tasks: HashMap::new(),
                publications: HashMap::new(),
            }
        }

        fn add_task(&mut self, task_id: &str, state: TaskState) {
            self.tasks.insert(task_id.to_string(), state);
        }

        fn bind_publication(&mut self, task_id: &str, publication: PublicationDescriptor) {
            self.publications.insert(task_id.to_string(), publication);
        }
    }

    impl TaskStateLookup for MockTaskLookup {
        async fn get_task_state(&self, task_id: &str) -> Result<Option<TaskState>, String> {
            Ok(self.tasks.get(task_id).cloned())
        }

        async fn get_task_publication(
            &self,
            task_id: &str,
        ) -> Result<Option<PublicationDescriptor>, String> {
            Ok(self.publications.get(task_id).cloned())
        }
    }

    struct ErrorTaskLookup;

    impl TaskStateLookup for ErrorTaskLookup {
        async fn get_task_state(&self, task_id: &str) -> Result<Option<TaskState>, String> {
            Err(format!("task_id_ambiguous: {task_id}"))
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_success() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
                requires_visible_output: false,
            },
        );
        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: Some(Utc::now()),
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => {
                assert!(response.acknowledged);
            }
            other => panic!("Expected Ok, got {:?}", other),
        }

        // Verify event was written
        let events = ledger.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "TaskStarted");
    }

    #[tokio::test]
    async fn test_handle_task_started_not_found() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");
        let lookup = MockTaskLookup::new();

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "nonexistent", "token", request, &lookup).await;

        match result {
            CallbackResult::NotFound(err) => {
                assert_eq!(err.error, "task_not_found");
            }
            other => panic!("Expected NotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_ambiguous_legacy_task_id_is_bad_request() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");
        let lookup = ErrorTaskLookup;

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "extract", "token", request, &lookup).await;

        match result {
            CallbackResult::BadRequest(err) => {
                assert_eq!(err.error, "task_id_ambiguous");
                assert!(err.message.contains("opaque taskId"));
            }
            other => panic!("Expected BadRequest, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_legacy_path_validates_canonical_callback_task_id() {
        let ledger = Arc::new(MockLedger::default());
        let canonical_task_id = callback_task_id("run-1", "extract");
        let validator = Arc::new(RequiredTaskIdTokenValidator::accepting(vec![
            canonical_task_id.clone(),
        ]));
        let ctx = CallbackContext::new(
            ledger.clone(),
            Arc::clone(&validator),
            "tenant-1",
            "workspace-1",
        );

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "extract",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "extract".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "extract", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => assert!(response.acknowledged),
            other => panic!("Expected Ok, got {:?}", other),
        }
        assert_eq!(
            validator.seen_task_ids.lock().unwrap().as_slice(),
            &[canonical_task_id]
        );
    }

    #[tokio::test]
    async fn test_handle_task_started_legacy_path_accepts_legacy_task_key_token() {
        let ledger = Arc::new(MockLedger::default());
        let canonical_task_id = callback_task_id("run-1", "extract");
        let validator = Arc::new(RequiredTaskIdTokenValidator::accepting(vec![
            "extract".to_string(),
        ]));
        let ctx = CallbackContext::new(
            ledger.clone(),
            Arc::clone(&validator),
            "tenant-1",
            "workspace-1",
        );

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "extract",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "extract".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "extract", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => assert!(response.acknowledged),
            other => panic!("Expected Ok, got {:?}", other),
        }
        assert_eq!(
            validator.seen_task_ids.lock().unwrap().as_slice(),
            &[canonical_task_id, "extract".to_string()]
        );
    }

    #[tokio::test]
    async fn test_handle_task_started_terminal() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "SUCCEEDED".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "task_already_terminal");
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    /// H5: a failed attempt with retries left leaves the task in RETRY_WAIT
    /// with `attempt` still naming the finished attempt. A redelivered dispatch
    /// for that attempt must be suppressed by durable control-plane state, not
    /// by the worker's process-local memory — the worker that reported the
    /// completion may not exist any more.
    #[tokio::test]
    async fn task_started_reports_attempt_already_completed_for_a_redelivered_retry_wait_attempt() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RETRY_WAIT".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-replacement".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "attempt_already_completed");
                assert_eq!(err.state.as_deref(), Some("RETRY_WAIT"));
                assert_eq!(err.received_attempt, Some(1));
            }
            other => panic!("Expected Conflict, got {other:?}"),
        }
    }

    /// The suppression must be scoped to the finished attempt: the retry's own
    /// attempt must still be allowed to start.
    #[tokio::test]
    async fn task_started_allows_the_next_attempt_after_a_retry_wait_completion() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "DISPATCHED".to_string(),
                attempt: 2,
                attempt_id: "att-2".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 2,
            attempt_id: "att-2".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;
        assert!(
            matches!(result, CallbackResult::Ok(_)),
            "the retry attempt must be allowed to start: {result:?}"
        );
    }

    /// H3: a worker-reported timestamp never becomes the event's time.
    #[tokio::test]
    async fn callback_events_are_timed_from_server_receipt_not_worker_clocks() {
        for skew_days in [-3650_i64, 3650_i64] {
            let ledger = Arc::new(MockLedger::default());
            let validator = Arc::new(MockTokenValidator::allow_all());
            let ctx =
                CallbackContext::new(Arc::clone(&ledger), validator, "tenant-1", "workspace-1");

            let mut lookup = MockTaskLookup::new();
            lookup.add_task(
                "task-1",
                TaskState {
                    state: "RUNNING".to_string(),
                    attempt: 1,
                    attempt_id: "att-1".to_string(),
                    run_id: "run-1".to_string(),
                    task_key: "task-1".to_string(),
                    asset_key: None,
                    partition_key: None,
                    code_version: None,
                    cancel_requested: false,
                    requires_visible_output: false,
                },
            );

            let skewed = Utc::now() + chrono::Duration::days(skew_days);
            let before = Utc::now();
            let result = handle_task_completed(
                &ctx,
                "task-1",
                "token",
                TaskCompletedRequest {
                    attempt: 1,
                    attempt_id: "att-1".to_string(),
                    worker_id: "worker-abc".to_string(),
                    traceparent: None,
                    outcome: WorkerOutcome::Failed,
                    completed_at: Some(skewed),
                    output: None,
                    error: None,
                    metrics: None,
                    cancelled_during_phase: None,
                    partial_progress: None,
                },
                &lookup,
            )
            .await;
            assert!(matches!(result, CallbackResult::Ok(_)), "{result:?}");

            let events = ledger.events.lock().expect("ledger events");
            let event = events.first().expect("a completion event was written");
            let after = Utc::now();
            assert!(
                event.timestamp >= before && event.timestamp <= after,
                "event time must be the server's receipt time, not the worker's \
                 {skew_days}-day skewed clock: {}",
                event.timestamp
            );
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_attempt_mismatch() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 2,
                attempt_id: "att-2".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1, // Old attempt
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "attempt_mismatch");
                assert_eq!(err.expected_attempt, Some(2));
                assert_eq!(err.received_attempt, Some(1));
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_attempt_id_mismatch() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-2".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "attempt_id_mismatch");
                assert_eq!(err.expected_attempt_id.as_deref(), Some("att-1"));
                assert_eq!(err.received_attempt_id.as_deref(), Some("att-2"));
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_cancel_requested() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "QUEUED".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: true,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "task_already_terminal");
                assert_eq!(err.state.as_deref(), Some("CANCELLED"));
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_cancelled_task_conflicts() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "CANCELLED".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "task_already_terminal");
                assert_eq!(err.state.as_deref(), Some("CANCELLED"));
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
        assert!(ledger.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_heartbeat_with_cancel() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: true, // Cancellation requested
                requires_visible_output: false,
            },
        );

        let request = HeartbeatRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            heartbeat_at: None,
            progress_pct: Some(50),
            message: Some("Processing...".to_string()),
        };

        let result = handle_heartbeat(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => {
                assert!(response.acknowledged);
                assert!(response.should_cancel);
                assert_eq!(response.cancel_reason, Some("user_requested".to_string()));
            }
            other => panic!("Expected Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_attempt_id_mismatch() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = HeartbeatRequest {
            attempt: 1,
            attempt_id: "att-2".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            heartbeat_at: None,
            progress_pct: None,
            message: None,
        };

        let result = handle_heartbeat(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "attempt_id_mismatch");
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_invalid_progress_pct() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = HeartbeatRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            heartbeat_at: None,
            progress_pct: Some(200),
            message: None,
        };

        let result = handle_heartbeat(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::BadRequest(err) => {
                assert_eq!(err.error, "invalid_argument");
            }
            other => panic!("Expected BadRequest, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_terminal_returns_gone() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "SUCCEEDED".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = HeartbeatRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            heartbeat_at: None,
            progress_pct: None,
            message: None,
        };

        let result = handle_heartbeat(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Gone(err) => {
                assert_eq!(err.error, "task_expired");
                assert_eq!(err.state.as_deref(), Some("SUCCEEDED"));
            }
            other => panic!("Expected Gone, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_sets_server_timestamp() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = HeartbeatRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            heartbeat_at: None,
            progress_pct: None,
            message: None,
        };

        let result = handle_heartbeat(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(_) => {}
            other => panic!("Expected Ok, got {:?}", other),
        }

        let events = ledger.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        if let OrchestrationEventData::TaskHeartbeat { heartbeat_at, .. } = &events[0].data {
            let heartbeat_at = heartbeat_at.expect("heartbeat_at should be set");
            assert_eq!(heartbeat_at, events[0].timestamp);
        } else {
            panic!("Expected TaskHeartbeat event");
        }
    }

    #[tokio::test]
    async fn test_handle_task_completed_success() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: Some(Utc::now()),
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-123".to_string()),
                row_count: Some(1000),
                byte_size: Some(1024),
                output_path: None,
                delta_table: Some("analytics.daily".to_string()),
                delta_version: Some(17),
                delta_partition: Some("2025-01-15".to_string()),
                output_visibility_state: None,
                published_at: None,
                publish_error: None,
                publication: None,
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => {
                assert!(response.acknowledged);
                assert_eq!(response.final_state, "SUCCEEDED");
            }
            other => panic!("Expected Ok, got {:?}", other),
        }

        // Verify event was written
        let events = ledger.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "TaskCompletionRecorded");
        if let OrchestrationEventData::TaskCompletionRecorded {
            asset_key,
            partition_key,
            code_version,
            output,
            ..
        } = &events[0].data
        {
            assert_eq!(asset_key.as_deref(), Some("analytics.daily"));
            assert_eq!(partition_key.as_deref(), Some("2025-01-15"));
            assert_eq!(code_version.as_deref(), Some("v1.2.3"));

            let output = output.as_ref().expect("expected task output payload");
            assert_eq!(output.delta_table.as_deref(), Some("analytics.daily"));
            assert_eq!(output.delta_version, Some(17));
            assert_eq!(output.delta_partition.as_deref(), Some("2025-01-15"));
        } else {
            panic!("Expected TaskCompletionRecorded event");
        }
    }

    #[tokio::test]
    async fn worker_visibility_claim_does_not_establish_verified_visibility() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
                requires_visible_output: true,
            },
        );

        let published_at = Utc::now();
        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: Some(Utc::now()),
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-123".to_string()),
                row_count: Some(1000),
                byte_size: Some(1024),
                output_path: None,
                delta_table: Some("analytics.daily".to_string()),
                delta_version: Some(17),
                delta_partition: Some("2025-01-15".to_string()),
                output_visibility_state: Some(TaskOutputVisibilityState::Visible),
                published_at: Some(published_at),
                publish_error: None,
                publication: None,
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;
        match result {
            CallbackResult::Ok(response) => {
                assert!(response.acknowledged);
                assert_eq!(response.final_state, "SUCCEEDED");
            }
            other => panic!("Expected Ok, got {:?}", other),
        }

        let events = ledger.events.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "required output must remain pending without an owner-verifiable descriptor"
        );
        assert_eq!(events[0].event_type, "TaskCompletionRecorded");
        if let OrchestrationEventData::TaskCompletionRecorded {
            output_visibility, ..
        } = &events[0].data
        {
            assert_eq!(
                output_visibility
                    .as_ref()
                    .map(|update| update.visibility_state),
                Some(OutputVisibilityState::Pending)
            );
        } else {
            panic!("Expected TaskCompletionRecorded event");
        }
    }

    #[tokio::test]
    async fn test_handle_task_completed_emits_legacy_compatible_completion_without_visibility() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: Some(Utc::now()),
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-123".to_string()),
                row_count: Some(1000),
                byte_size: Some(1024),
                output_path: None,
                delta_table: Some("analytics.daily".to_string()),
                delta_version: Some(17),
                delta_partition: Some("2025-01-15".to_string()),
                output_visibility_state: None,
                published_at: None,
                publish_error: None,
                publication: None,
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;
        assert!(matches!(result, CallbackResult::Ok(_)));

        let events = ledger.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        if let OrchestrationEventData::TaskCompletionRecorded {
            output_visibility,
            code_version,
            ..
        } = &events[0].data
        {
            assert!(output_visibility.is_none());
            assert_eq!(code_version.as_deref(), Some("v1.2.3"));
        } else {
            panic!("Expected TaskCompletionRecorded event");
        }
    }

    #[tokio::test]
    async fn computation_completion_precedes_visibility_write() {
        let ledger = Arc::new(MockLedger::fail_after_writes(1));
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1")
            .with_publication_verifier(Arc::new(MockPublicationVerifier::default()));

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let published_at = Utc::now();
        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: Some(Utc::now()),
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-123".to_string()),
                row_count: Some(1000),
                byte_size: Some(12),
                output_path: None,
                delta_table: Some("analytics.daily".to_string()),
                delta_version: Some(17),
                delta_partition: Some("2025-01-15".to_string()),
                output_visibility_state: Some(TaskOutputVisibilityState::Pending),
                published_at: Some(published_at),
                publish_error: None,
                publication: Some(publication_descriptor()),
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;
        assert!(matches!(result, CallbackResult::InternalError(_)));

        let events = ledger.events.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "computation completion must survive a later visibility write failure"
        );
        if let OrchestrationEventData::TaskCompletionRecorded {
            output_visibility, ..
        } = &events[0].data
        {
            let output_visibility = output_visibility.as_ref().expect("expected visibility");
            assert_eq!(
                output_visibility.visibility_state,
                OutputVisibilityState::Pending
            );
            assert!(
                output_visibility
                    .publication
                    .as_ref()
                    .and_then(|publication| publication.owner_evidence.as_ref())
                    .is_none(),
                "completion must persist the unverified descriptor first"
            );
            assert!(output_visibility.published_at.is_none());
        } else {
            panic!("Expected TaskCompletionRecorded event");
        }
    }

    #[tokio::test]
    async fn cancellation_during_owner_verification_preserves_pending_completion() {
        let ledger = Arc::new(MockLedger::default());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let ctx = CallbackContext::new(
            Arc::clone(&ledger),
            Arc::new(MockTokenValidator::allow_all()),
            "tenant-1",
            "workspace-1",
        )
        .with_publication_verifier(Arc::new(BlockingPublicationVerifier {
            entered: Arc::clone(&entered),
            release,
        }));
        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: true,
            },
        );
        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-1".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: None,
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-1".to_string()),
                row_count: None,
                byte_size: Some(4),
                output_path: None,
                delta_table: None,
                delta_version: None,
                delta_partition: None,
                output_visibility_state: None,
                published_at: None,
                publish_error: None,
                publication: Some(publication_descriptor()),
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let callback = tokio::spawn(async move {
            handle_task_completed(&ctx, "task-1", "token", request, &lookup).await
        });
        entered.notified().await;

        let events = ledger.events.lock().expect("events");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].data,
            OrchestrationEventData::TaskCompletionRecorded {
                outcome: TaskOutcome::Succeeded,
                output_visibility: Some(OutputVisibilityUpdate {
                    visibility_state: OutputVisibilityState::Pending,
                    ..
                }),
                ..
            }
        ));
        drop(events);

        callback.abort();
        assert!(
            callback
                .await
                .expect_err("callback must be cancelled")
                .is_cancelled()
        );
        assert_eq!(ledger.events.lock().expect("events").len(), 1);
    }

    #[tokio::test]
    async fn test_handle_task_completed_attempt_id_mismatch() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-2".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Failed,
            completed_at: Some(Utc::now()),
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Conflict(err) => {
                assert_eq!(err.error, "attempt_id_mismatch");
            }
            other => panic!("Expected Conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn terminal_success_replay_verifies_publication_without_repeating_computation() {
        let ledger = Arc::new(MockLedger::default());
        let verifier = Arc::new(MockPublicationVerifier::default());
        let ctx = CallbackContext::new(
            Arc::clone(&ledger),
            Arc::new(MockTokenValidator::allow_all()),
            "tenant-1",
            "workspace-1",
        )
        .with_publication_verifier(verifier.clone());
        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "SUCCEEDED".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );
        lookup.bind_publication("task-1", publication_descriptor());
        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-1".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: None,
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-1".to_string()),
                row_count: None,
                byte_size: Some(4),
                output_path: None,
                delta_table: None,
                delta_version: None,
                delta_partition: None,
                output_visibility_state: Some(TaskOutputVisibilityState::Visible),
                published_at: None,
                publish_error: None,
                publication: Some(publication_descriptor()),
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let mut changed_request = request.clone();
        changed_request
            .output
            .as_mut()
            .and_then(|output| output.publication.as_mut())
            .expect("publication")
            .object_path = "outputs/different.parquet".to_string();
        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;
        assert!(matches!(result, CallbackResult::Ok(_)));
        assert_eq!(verifier.calls.load(Ordering::SeqCst), 1);
        let events = ledger.events.lock().expect("events");
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0].data,
            OrchestrationEventData::TaskOutputVisibilityChanged {
                visibility_state: OutputVisibilityState::Visible,
                ..
            }
        ));
        drop(events);

        let changed =
            handle_task_completed(&ctx, "task-1", "token", changed_request, &lookup).await;
        assert!(matches!(changed, CallbackResult::Conflict(_)));
        assert_eq!(verifier.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ledger.events.lock().expect("events").len(), 1);
    }

    #[tokio::test]
    async fn test_handle_task_completed_failure() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger.clone(), validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Failed,
            completed_at: Some(Utc::now()),
            output: None,
            error: Some(super::super::types::TaskError {
                category: super::super::types::ErrorCategory::UserCode,
                message: "KeyError: 'missing_col'".to_string(),
                stack_trace: Some("...".to_string()),
                retryable: Some(true),
            }),
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => {
                assert!(response.acknowledged);
                assert_eq!(response.final_state, "FAILED");
            }
            other => panic!("Expected Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_completed_allows_partial_output_on_failure() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator::allow_all());
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );

        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Failed,
            completed_at: Some(Utc::now()),
            output: Some(super::super::types::TaskOutput {
                materialization_id: Some("mat-123".to_string()),
                row_count: Some(100),
                byte_size: Some(2048),
                output_path: None,
                delta_table: None,
                delta_version: None,
                delta_partition: None,
                output_visibility_state: None,
                published_at: None,
                publish_error: None,
                publication: None,
            }),
            error: Some(super::super::types::TaskError {
                category: super::super::types::ErrorCategory::Infrastructure,
                message: "transient failure".to_string(),
                stack_trace: None,
                retryable: None,
            }),
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let result = handle_task_completed(&ctx, "task-1", "token", request, &lookup).await;

        match result {
            CallbackResult::Ok(response) => {
                assert!(response.acknowledged);
                assert_eq!(response.final_state, "FAILED");
            }
            other => panic!("Expected Ok, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_task_started_invalid_token() {
        let ledger = Arc::new(MockLedger::default());
        let validator = Arc::new(MockTokenValidator { allow: false });
        let ctx = CallbackContext::new(ledger, validator, "tenant-1", "workspace-1");

        let mut lookup = MockTaskLookup::new();
        lookup.add_task(
            "task-1",
            TaskState {
                state: "RUNNING".to_string(),
                attempt: 1,
                attempt_id: "att-1".to_string(),
                run_id: "run-1".to_string(),
                task_key: "task-1".to_string(),
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
                requires_visible_output: false,
            },
        );
        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "att-1".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: None,
        };

        let result = handle_task_started(&ctx, "task-1", "bad-token", request, &lookup).await;

        match result {
            CallbackResult::Unauthorized(err) => {
                assert_eq!(err.error, "invalid_token");
            }
            other => panic!("Expected Unauthorized, got {:?}", other),
        }
    }
}
