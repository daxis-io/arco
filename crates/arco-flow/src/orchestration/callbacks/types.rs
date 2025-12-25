//! Request and response types for worker callbacks per ADR-023.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ============================================================================
// TaskStarted
// ============================================================================

/// Request body for `/v1/tasks/{task_id}/started`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStartedRequest {
    /// Attempt number (1-indexed).
    pub attempt: u32,
    /// Attempt identifier - concurrency guard.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// When execution started (optional, uses server time if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

/// Response body for successful `/v1/tasks/{task_id}/started`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStartedResponse {
    /// Whether the callback was acknowledged.
    pub acknowledged: bool,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

// ============================================================================
// Heartbeat
// ============================================================================

/// Request body for `/v1/tasks/{task_id}/heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    /// Attempt number.
    pub attempt: u32,
    /// Attempt identifier - concurrency guard.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// When heartbeat was sent (optional, uses server time if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// Optional progress percentage (0-100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_pct: Option<u8>,
    /// Optional status message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Response body for successful `/v1/tasks/{task_id}/heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was acknowledged.
    pub acknowledged: bool,
    /// Whether the worker should cancel.
    pub should_cancel: bool,
    /// Reason for cancellation (if `should_cancel` is true).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

// ============================================================================
// TaskCompleted
// ============================================================================

/// Request body for `/v1/tasks/{task_id}/completed`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedRequest {
    /// Attempt number.
    pub attempt: u32,
    /// Attempt identifier - concurrency guard.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Task outcome.
    pub outcome: WorkerOutcome,
    /// When execution completed (optional, uses server time if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Output for successful tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<TaskOutput>,
    /// Error details for failed tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    /// Execution metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<TaskMetrics>,
    /// Phase when cancellation occurred (for CANCELLED outcome).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelled_during_phase: Option<String>,
    /// Partial progress at cancellation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_progress: Option<serde_json::Value>,
}

/// Response body for successful `/v1/tasks/{task_id}/completed`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedResponse {
    /// Whether the completion was acknowledged.
    pub acknowledged: bool,
    /// Final task state.
    pub final_state: String,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Worker-reported task outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerOutcome {
    /// Task completed successfully.
    Succeeded,
    /// Task failed (may retry).
    Failed,
    /// Task was cancelled.
    Cancelled,
}

/// Output from a successful task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutput {
    /// Materialization identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialization_id: Option<String>,
    /// Number of rows produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    /// Size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_size: Option<u64>,
    /// Output path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
}

/// Error details from a failed task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    /// Error category per ADR-023.
    pub category: ErrorCategory,
    /// Error message.
    pub message: String,
    /// Stack trace (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
    /// Whether the error is retryable (optional, defaults by category).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl TaskError {
    /// Returns the retryability, falling back to the category default.
    #[must_use]
    pub fn effective_retryable(&self) -> bool {
        self.retryable
            .unwrap_or_else(|| self.category.default_retryable())
    }
}

/// Error category per ADR-023.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCategory {
    /// Error in user/asset code.
    UserCode,
    /// Input data fails validation.
    DataQuality,
    /// Infrastructure/cloud errors.
    Infrastructure,
    /// Configuration errors.
    Configuration,
    /// Execution timeout.
    Timeout,
    /// Task was cancelled.
    Cancelled,
}

impl ErrorCategory {
    /// Returns whether this error category is retryable by default.
    #[must_use]
    pub fn default_retryable(&self) -> bool {
        matches!(self, Self::UserCode | Self::Infrastructure | Self::Timeout)
    }
}

/// Execution metrics from the worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetrics {
    /// CPU time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_time_ms: Option<u64>,
    /// Peak memory usage in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_memory_bytes: Option<u64>,
    /// I/O read bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_read_bytes: Option<u64>,
    /// I/O write bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_write_bytes: Option<u64>,
}

// ============================================================================
// Error Responses
// ============================================================================

/// Error response for callback failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallbackError {
    /// Error code.
    pub error: String,
    /// Human-readable message.
    pub message: String,
    /// Current task state (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Expected attempt (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_attempt: Option<u32>,
    /// Received attempt (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_attempt: Option<u32>,
    /// Expected attempt ID (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_attempt_id: Option<String>,
    /// Received attempt ID (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_attempt_id: Option<String>,
}

impl CallbackError {
    /// Creates an "invalid argument" error (400).
    #[must_use]
    pub fn invalid_argument(field: &str, message: &str) -> Self {
        Self {
            error: "invalid_argument".to_string(),
            message: format!("Invalid {field}: {message}"),
            state: None,
            expected_attempt: None,
            received_attempt: None,
            expected_attempt_id: None,
            received_attempt_id: None,
        }
    }

    /// Creates a "task already terminal" error (409).
    #[must_use]
    pub fn task_already_terminal(state: &str) -> Self {
        Self {
            error: "task_already_terminal".to_string(),
            message: format!("Task is already in terminal state: {state}"),
            state: Some(state.to_string()),
            expected_attempt: None,
            received_attempt: None,
            expected_attempt_id: None,
            received_attempt_id: None,
        }
    }

    /// Creates an "attempt mismatch" error (409).
    #[must_use]
    pub fn attempt_mismatch(expected: u32, received: u32) -> Self {
        Self {
            error: "attempt_mismatch".to_string(),
            message: format!("Expected attempt {expected}, received {received}"),
            state: None,
            expected_attempt: Some(expected),
            received_attempt: Some(received),
            expected_attempt_id: None,
            received_attempt_id: None,
        }
    }

    /// Creates an "attempt ID mismatch" error (409).
    #[must_use]
    pub fn attempt_id_mismatch(expected: &str, received: &str) -> Self {
        Self {
            error: "attempt_id_mismatch".to_string(),
            message: format!("Expected attempt_id {expected}, received {received}"),
            state: None,
            expected_attempt: None,
            received_attempt: None,
            expected_attempt_id: Some(expected.to_string()),
            received_attempt_id: Some(received.to_string()),
        }
    }

    /// Creates a "task expired" error (410).
    #[must_use]
    pub fn task_expired() -> Self {
        Self {
            error: "task_expired".to_string(),
            message: "Task is no longer active".to_string(),
            state: None,
            expected_attempt: None,
            received_attempt: None,
            expected_attempt_id: None,
            received_attempt_id: None,
        }
    }

    /// Creates an "invalid token" error (401).
    #[must_use]
    pub fn invalid_token(reason: &str) -> Self {
        Self {
            error: "invalid_token".to_string(),
            message: reason.to_string(),
            state: None,
            expected_attempt: None,
            received_attempt: None,
            expected_attempt_id: None,
            received_attempt_id: None,
        }
    }

    /// Creates a "task not found" error (404).
    #[must_use]
    pub fn task_not_found(task_id: &str) -> Self {
        Self {
            error: "task_not_found".to_string(),
            message: format!("Task not found: {task_id}"),
            state: None,
            expected_attempt: None,
            received_attempt: None,
            expected_attempt_id: None,
            received_attempt_id: None,
        }
    }
}

/// Result type for callback handlers.
#[derive(Debug, Clone)]
pub enum CallbackResult<T> {
    /// Success (200 OK).
    Ok(T),
    /// Invalid request (400 Bad Request).
    BadRequest(CallbackError),
    /// Task already terminal (409 Conflict).
    Conflict(CallbackError),
    /// Task expired (410 Gone).
    Gone(CallbackError),
    /// Invalid token (401 Unauthorized).
    Unauthorized(CallbackError),
    /// Task not found (404 Not Found).
    NotFound(CallbackError),
    /// Internal error (500).
    InternalError(String),
}

impl<T> CallbackResult<T> {
    /// Returns the HTTP status code for this result.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Ok(_) => 200,
            Self::BadRequest(_) => 400,
            Self::Conflict(_) => 409,
            Self::Gone(_) => 410,
            Self::Unauthorized(_) => 401,
            Self::NotFound(_) => 404,
            Self::InternalError(_) => 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_started_request_serialization() {
        let request = TaskStartedRequest {
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            started_at: Some(Utc::now()),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        assert!(json.contains("attempt"));
        assert!(json.contains("attemptId"));
        assert!(json.contains("workerId"));
        assert!(json.contains("startedAt"));
    }

    #[test]
    fn test_heartbeat_request_optional_fields() {
        let json = r#"{"attempt": 1, "attemptId": "01HQ123ATT", "workerId": "worker-abc"}"#;
        let request: HeartbeatRequest = serde_json::from_str(json).expect("deserialize");

        assert_eq!(request.attempt, 1);
        assert_eq!(request.attempt_id, "01HQ123ATT");
        assert_eq!(request.worker_id, "worker-abc");
        assert!(request.heartbeat_at.is_none());
        assert!(request.progress_pct.is_none());
        assert!(request.message.is_none());
    }

    #[test]
    fn test_task_completed_success() {
        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: Some(Utc::now()),
            output: Some(TaskOutput {
                materialization_id: Some("mat_123".to_string()),
                row_count: Some(1000),
                byte_size: Some(52428800),
                output_path: Some("gs://bucket/path".to_string()),
            }),
            error: None,
            metrics: Some(TaskMetrics {
                cpu_time_ms: Some(45000),
                peak_memory_bytes: Some(2147483648),
                io_read_bytes: Some(10485760),
                io_write_bytes: Some(52428800),
            }),
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let json = serde_json::to_string(&request).expect("serialize");
        assert!(json.contains("SUCCEEDED"));
        assert!(json.contains("materializationId"));
    }

    #[test]
    fn test_task_completed_failure() {
        let request = TaskCompletedRequest {
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_id: "worker-abc".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Failed,
            completed_at: Some(Utc::now()),
            output: None,
            error: Some(TaskError {
                category: ErrorCategory::UserCode,
                message: "KeyError: 'missing_column'".to_string(),
                stack_trace: Some("Traceback...".to_string()),
                retryable: Some(true),
            }),
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        };

        let json = serde_json::to_string(&request).expect("serialize");
        assert!(json.contains("FAILED"));
        assert!(json.contains("USER_CODE"));
    }

    #[test]
    fn test_error_category_default_retryable() {
        assert!(ErrorCategory::UserCode.default_retryable());
        assert!(ErrorCategory::Infrastructure.default_retryable());
        assert!(ErrorCategory::Timeout.default_retryable());
        assert!(!ErrorCategory::DataQuality.default_retryable());
        assert!(!ErrorCategory::Configuration.default_retryable());
        assert!(!ErrorCategory::Cancelled.default_retryable());
    }

    #[test]
    fn test_task_error_effective_retryable() {
        let defaulted = TaskError {
            category: ErrorCategory::DataQuality,
            message: "bad input".to_string(),
            stack_trace: None,
            retryable: None,
        };
        assert!(!defaulted.effective_retryable());

        let overridden = TaskError {
            category: ErrorCategory::DataQuality,
            message: "bad input".to_string(),
            stack_trace: None,
            retryable: Some(true),
        };
        assert!(overridden.effective_retryable());
    }

    #[test]
    fn test_callback_error_variants() {
        let terminal = CallbackError::task_already_terminal("SUCCEEDED");
        assert_eq!(terminal.error, "task_already_terminal");
        assert!(terminal.state.is_some());

        let mismatch = CallbackError::attempt_mismatch(2, 1);
        assert_eq!(mismatch.error, "attempt_mismatch");
        assert_eq!(mismatch.expected_attempt, Some(2));
        assert_eq!(mismatch.received_attempt, Some(1));

        let id_mismatch = CallbackError::attempt_id_mismatch("att-1", "att-2");
        assert_eq!(id_mismatch.error, "attempt_id_mismatch");
        assert_eq!(id_mismatch.expected_attempt_id.as_deref(), Some("att-1"));
        assert_eq!(id_mismatch.received_attempt_id.as_deref(), Some("att-2"));

        let expired = CallbackError::task_expired();
        assert_eq!(expired.error, "task_expired");
    }

    #[test]
    fn test_callback_result_status_codes() {
        let ok: CallbackResult<()> = CallbackResult::Ok(());
        assert_eq!(ok.status_code(), 200);

        let bad: CallbackResult<()> =
            CallbackResult::BadRequest(CallbackError::invalid_argument("attempt", "must be >= 1"));
        assert_eq!(bad.status_code(), 400);

        let conflict: CallbackResult<()> =
            CallbackResult::Conflict(CallbackError::task_already_terminal("SUCCEEDED"));
        assert_eq!(conflict.status_code(), 409);

        let gone: CallbackResult<()> = CallbackResult::Gone(CallbackError::task_expired());
        assert_eq!(gone.status_code(), 410);

        let unauthorized: CallbackResult<()> =
            CallbackResult::Unauthorized(CallbackError::invalid_token("expired"));
        assert_eq!(unauthorized.status_code(), 401);
    }
}
