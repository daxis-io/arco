//! Callback handler implementations per ADR-023.
//!
//! These handlers are framework-agnostic - they take parsed requests and return
//! response types. HTTP routing and serialization is handled by the API layer.

use std::future::Future;
use std::sync::Arc;

use chrono::Utc;

use super::types::{
    CallbackError, CallbackResult, HeartbeatRequest, HeartbeatResponse, TaskCompletedRequest,
    TaskCompletedResponse, TaskStartedRequest, TaskStartedResponse, WorkerOutcome,
};
use crate::metrics::{labels as metrics_labels, names as metrics_names};
use crate::orchestration::OrchestrationLedgerWriter;
use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskOutcome};

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
        }
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
    /// Asset key for this task (if any).
    pub asset_key: Option<String>,
    /// Partition key for this task (if any).
    pub partition_key: Option<String>,
    /// Code version for the run (if available).
    pub code_version: Option<String>,
    /// Whether cancellation has been requested.
    pub cancel_requested: bool,
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
}

/// Trait for validating task tokens.
pub trait TaskTokenValidator: Send + Sync {
    /// Validates the task token for the given task ID.
    fn validate_task_token(
        &self,
        task_id: &str,
        run_id: &str,
        attempt: u32,
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
            return finish_callback("task_started", CallbackResult::InternalError(e));
        }
    };

    tracing::Span::current().record("run_id", tracing::field::display(&state.run_id));
    tracing::Span::current().record("attempt", tracing::field::display(attempt));
    tracing::Span::current().record("attempt_id", tracing::field::display(&attempt_id));
    tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
    if let Some(traceparent) = &traceparent {
        tracing::Span::current().record("traceparent", tracing::field::display(traceparent));
    }

    if let Err(reason) = ctx
        .token_validator
        .validate_task_token(task_id, &state.run_id, attempt, task_token)
        .await
    {
        return finish_callback(
            "task_started",
            CallbackResult::Unauthorized(CallbackError::invalid_token(&reason)),
        );
    }

    // Check if task is already terminal
    if state.is_terminal() {
        return finish_callback(
            "task_started",
            CallbackResult::Conflict(CallbackError::task_already_terminal(&state.state)),
        );
    }

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
            task_key: task_id.to_string(),
            attempt,
            attempt_id: state.attempt_id.clone(),
            worker_id,
        },
    );
    event.timestamp = started_at.unwrap_or_else(Utc::now);

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

    let event_timestamp = heartbeat_at.unwrap_or_else(Utc::now);
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
            return finish_callback("heartbeat", CallbackResult::InternalError(e));
        }
    };

    tracing::Span::current().record("run_id", tracing::field::display(&state.run_id));
    tracing::Span::current().record("attempt", tracing::field::display(attempt));
    tracing::Span::current().record("attempt_id", tracing::field::display(&attempt_id));
    tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
    if let Some(traceparent) = &traceparent {
        tracing::Span::current().record("traceparent", tracing::field::display(traceparent));
    }

    if let Err(reason) = ctx
        .token_validator
        .validate_task_token(task_id, &state.run_id, attempt, task_token)
        .await
    {
        return finish_callback(
            "heartbeat",
            CallbackResult::Unauthorized(CallbackError::invalid_token(&reason)),
        );
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
            task_key: task_id.to_string(),
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
        output: request_output,
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
            return finish_callback("task_completed", CallbackResult::InternalError(e));
        }
    };

    tracing::Span::current().record("run_id", tracing::field::display(&state.run_id));
    tracing::Span::current().record("attempt", tracing::field::display(attempt));
    tracing::Span::current().record("attempt_id", tracing::field::display(&attempt_id));
    tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
    if let Some(traceparent) = &traceparent {
        tracing::Span::current().record("traceparent", tracing::field::display(traceparent));
    }

    if let Err(reason) = ctx
        .token_validator
        .validate_task_token(task_id, &state.run_id, attempt, task_token)
        .await
    {
        return finish_callback(
            "task_completed",
            CallbackResult::Unauthorized(CallbackError::invalid_token(&reason)),
        );
    }

    // Check if task is already terminal
    if state.is_terminal() {
        return finish_callback(
            "task_completed",
            CallbackResult::Conflict(CallbackError::task_already_terminal(&state.state)),
        );
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
    let output = match &request_output {
        Some(value) => match serde_json::to_value(value) {
            Ok(payload) => Some(payload),
            Err(e) => {
                return finish_callback(
                    "task_completed",
                    CallbackResult::InternalError(format!("Failed to serialize task output: {e}")),
                );
            }
        },
        None => None,
    };
    let error_payload = request_error.as_ref().map(|value| {
        let mut normalized = value.clone();
        if normalized.retryable.is_none() {
            normalized.retryable = Some(normalized.effective_retryable());
        }
        normalized
    });
    let error = match error_payload.as_ref() {
        Some(value) => match serde_json::to_value(value) {
            Ok(payload) => Some(payload),
            Err(e) => {
                return finish_callback(
                    "task_completed",
                    CallbackResult::InternalError(format!("Failed to serialize task error: {e}")),
                );
            }
        },
        None => None,
    };
    let metrics = match &request_metrics {
        Some(value) => match serde_json::to_value(value) {
            Ok(payload) => Some(payload),
            Err(e) => {
                return finish_callback(
                    "task_completed",
                    CallbackResult::InternalError(format!("Failed to serialize task metrics: {e}")),
                );
            }
        },
        None => None,
    };

    // Emit TaskFinished event
    let mut event = OrchestrationEvent::new(
        &ctx.tenant_id,
        &ctx.workspace_id,
        OrchestrationEventData::TaskFinished {
            run_id: state.run_id.clone(),
            task_key: task_id.to_string(),
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
            partial_progress,
            asset_key: state.asset_key.clone(),
            partition_key: state.partition_key.clone(),
            code_version: state.code_version.clone(),
        },
    );
    event.timestamp = completed_at.unwrap_or_else(Utc::now);

    if let Err(e) = ctx.ledger.write_event(&event).await {
        return finish_callback(
            "task_completed",
            CallbackResult::InternalError(format!("Failed to write event: {e}")),
        );
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

    /// Mock ledger writer for testing.
    #[derive(Default)]
    struct MockLedger {
        events: Mutex<Vec<OrchestrationEvent>>,
    }

    impl OrchestrationLedgerWriter for MockLedger {
        async fn write_event(&self, event: &OrchestrationEvent) -> Result<(), String> {
            self.events.lock().unwrap().push(event.clone());
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
            _token: &str,
        ) -> Result<(), String> {
            if self.allow {
                Ok(())
            } else {
                Err("invalid token".to_string())
            }
        }
    }

    /// Mock task state lookup for testing.
    struct MockTaskLookup {
        tasks: HashMap<String, TaskState>,
    }

    impl MockTaskLookup {
        fn new() -> Self {
            Self {
                tasks: HashMap::new(),
            }
        }

        fn add_task(&mut self, task_id: &str, state: TaskState) {
            self.tasks.insert(task_id.to_string(), state);
        }
    }

    impl TaskStateLookup for MockTaskLookup {
        async fn get_task_state(&self, task_id: &str) -> Result<Option<TaskState>, String> {
            Ok(self.tasks.get(task_id).cloned())
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
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: true,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: true, // Cancellation requested
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: Some("v1.2.3".to_string()),
                cancel_requested: false,
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
        assert_eq!(events[0].event_type, "TaskFinished");
        if let OrchestrationEventData::TaskFinished {
            asset_key,
            partition_key,
            code_version,
            ..
        } = &events[0].data
        {
            assert_eq!(asset_key.as_deref(), Some("analytics.daily"));
            assert_eq!(partition_key.as_deref(), Some("2025-01-15"));
            assert_eq!(code_version.as_deref(), Some("v1.2.3"));
        } else {
            panic!("Expected TaskFinished event");
        }
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
                asset_key: None,
                partition_key: None,
                code_version: None,
                cancel_requested: false,
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
