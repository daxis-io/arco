//! Versioned worker protocol contracts for embedded Arco deployments.
//!
//! This crate owns the JSON/OpenAPI worker wire surface. Runtime crates may
//! re-export these types, but they should not redefine callback or dispatch
//! payloads locally.

#![forbid(unsafe_code)]

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

const CALLBACK_TASK_ID_PREFIX: &str = "ct1_";

/// Parsed components of an opaque worker callback task identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCallbackTaskId {
    /// Run identifier embedded in the callback task id.
    pub run_id: String,
    /// Task key embedded in the callback task id.
    pub task_key: String,
}

/// Error returned when a callback task id cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallbackTaskIdError {
    /// Identifier does not use the supported callback task id prefix.
    #[error("callback task id must start with {CALLBACK_TASK_ID_PREFIX}")]
    MissingPrefix,
    /// Identifier payload is not valid URL-safe base64.
    #[error("callback task id payload is not valid base64: {0}")]
    InvalidBase64(String),
    /// Identifier payload does not contain exactly one run/task separator.
    #[error("callback task id payload must encode run_id and task_key")]
    InvalidPayload,
    /// Identifier payload is not valid UTF-8.
    #[error("callback task id payload is not valid UTF-8")]
    InvalidUtf8,
}

/// Returns a stable opaque task id for worker callback paths.
#[must_use]
pub fn callback_task_id(run_id: &str, task_key: &str) -> String {
    let mut payload = Vec::with_capacity(run_id.len() + task_key.len() + 1);
    payload.extend_from_slice(run_id.as_bytes());
    payload.push(0);
    payload.extend_from_slice(task_key.as_bytes());
    format!(
        "{CALLBACK_TASK_ID_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(payload)
    )
}

/// Parses an opaque callback task id into its run and task-key components.
///
/// # Errors
///
/// Returns an error if the id does not use the supported format.
pub fn parse_callback_task_id(task_id: &str) -> Result<ParsedCallbackTaskId, CallbackTaskIdError> {
    let Some(encoded) = task_id.strip_prefix(CALLBACK_TASK_ID_PREFIX) else {
        return Err(CallbackTaskIdError::MissingPrefix);
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|err| CallbackTaskIdError::InvalidBase64(err.to_string()))?;
    let mut parts = decoded.split(|byte| *byte == 0);
    let Some(run_id_bytes) = parts.next() else {
        return Err(CallbackTaskIdError::InvalidPayload);
    };
    let Some(task_key_bytes) = parts.next() else {
        return Err(CallbackTaskIdError::InvalidPayload);
    };
    if parts.next().is_some() {
        return Err(CallbackTaskIdError::InvalidPayload);
    }
    let run_id =
        String::from_utf8(run_id_bytes.to_vec()).map_err(|_| CallbackTaskIdError::InvalidUtf8)?;
    let task_key =
        String::from_utf8(task_key_bytes.to_vec()).map_err(|_| CallbackTaskIdError::InvalidUtf8)?;
    if run_id.is_empty() || task_key.is_empty() {
        return Err(CallbackTaskIdError::InvalidPayload);
    }
    Ok(ParsedCallbackTaskId { run_id, task_key })
}

/// Generates a deterministic attempt id from a dispatch id.
#[must_use]
pub fn deterministic_attempt_id(dispatch_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"attempt:");
    hasher.update(dispatch_id.as_bytes());
    let hash = hasher.finalize();
    let hex_encoded = hex::encode(hash.get(..12).unwrap_or(&hash));
    format!("att_{hex_encoded}")
}

/// Canonical dispatch envelope sent to external workers.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDispatchEnvelope {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Opaque task identifier workers use in callback URLs.
    pub task_id: String,
    /// Semantic task key workers use to select work.
    pub task_key: String,
    /// Run identifier.
    pub run_id: String,
    /// Attempt number, starting at one.
    #[schema(minimum = 1)]
    pub attempt: u32,
    /// Attempt identifier used for stale-worker protection.
    pub attempt_id: String,
    /// Dispatch identifier.
    pub dispatch_id: String,
    /// Optional execution location selected by orchestration planning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_location_id: Option<String>,
    /// Target worker queue.
    pub worker_queue: String,
    /// Base URL workers use for task callbacks.
    pub callback_base_url: String,
    /// Per-task callback bearer token.
    pub task_token: String,
    /// Token expiry timestamp.
    pub token_expires_at: DateTime<Utc>,
    /// Optional traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Engine-specific payload for worker execution.
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawWorkerDispatchEnvelope {
    #[serde(alias = "tenant_id")]
    tenant_id: String,
    #[serde(alias = "workspace_id")]
    workspace_id: String,
    #[serde(default, alias = "task_id")]
    task_id: Option<String>,
    #[serde(alias = "task_key")]
    task_key: String,
    #[serde(alias = "run_id")]
    run_id: String,
    attempt: u32,
    #[serde(alias = "attempt_id")]
    attempt_id: String,
    #[serde(alias = "dispatch_id")]
    dispatch_id: String,
    #[serde(default, alias = "execution_location_id")]
    execution_location_id: Option<String>,
    #[serde(alias = "worker_queue")]
    worker_queue: String,
    #[serde(alias = "callback_base_url")]
    callback_base_url: String,
    #[serde(alias = "task_token")]
    task_token: String,
    #[serde(alias = "token_expires_at")]
    token_expires_at: DateTime<Utc>,
    traceparent: Option<String>,
    #[serde(default)]
    payload: Value,
}

impl<'de> Deserialize<'de> for WorkerDispatchEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawWorkerDispatchEnvelope::deserialize(deserializer)?;
        let task_id = raw.task_id.unwrap_or_else(|| raw.task_key.clone());
        Ok(Self {
            tenant_id: raw.tenant_id,
            workspace_id: raw.workspace_id,
            task_id,
            task_key: raw.task_key,
            run_id: raw.run_id,
            attempt: raw.attempt,
            attempt_id: raw.attempt_id,
            dispatch_id: raw.dispatch_id,
            execution_location_id: raw.execution_location_id,
            worker_queue: raw.worker_queue,
            callback_base_url: raw.callback_base_url,
            task_token: raw.task_token,
            token_expires_at: raw.token_expires_at,
            traceparent: raw.traceparent,
            payload: raw.payload,
        })
    }
}

impl WorkerDispatchEnvelope {
    /// Serializes the envelope to JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserializes the envelope from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Request body for task-started callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskStartedRequest {
    /// Attempt number.
    #[schema(minimum = 1)]
    pub attempt: u32,
    /// Attempt identifier.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Worker-reported start timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

/// Response body for task-started callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskStartedResponse {
    /// Whether the callback was acknowledged.
    pub acknowledged: bool,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Request body for heartbeat callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    /// Attempt number.
    #[schema(minimum = 1)]
    pub attempt: u32,
    /// Attempt identifier.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Worker-reported heartbeat timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// Optional progress percentage from zero to one hundred.
    #[schema(minimum = 0, maximum = 100)]
    #[serde(
        default,
        deserialize_with = "deserialize_progress_pct",
        skip_serializing_if = "Option::is_none"
    )]
    pub progress_pct: Option<u8>,
    /// Optional status message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Response body for heartbeat callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was acknowledged.
    pub acknowledged: bool,
    /// Whether the worker should cancel.
    pub should_cancel: bool,
    /// Optional cancellation reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Request body for task-completed callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedRequest {
    /// Attempt number.
    #[schema(minimum = 1)]
    pub attempt: u32,
    /// Attempt identifier.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Worker-reported outcome.
    pub outcome: WorkerOutcome,
    /// Worker-reported completion timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Output for successful tasks.
    #[serde(alias = "result", skip_serializing_if = "Option::is_none")]
    pub output: Option<TaskOutput>,
    /// Error details for failed tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    /// Execution metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<TaskMetrics>,
    /// Phase when cancellation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelled_during_phase: Option<String>,
    /// Partial progress at cancellation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_progress: Option<Value>,
}

/// Response body for task-completed callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedResponse {
    /// Whether the completion callback was acknowledged.
    pub acknowledged: bool,
    /// Final task state.
    pub final_state: String,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Worker-reported task outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerOutcome {
    /// Task completed successfully.
    Succeeded,
    /// Task failed and may be retryable.
    Failed,
    /// Task was cancelled.
    Cancelled,
}

/// Worker-reported output visibility state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskOutputVisibilityState {
    /// Output exists but is not consumable yet.
    Pending,
    /// Output is published and consumable.
    Visible,
    /// Output failed to become visible.
    Failed,
}

/// Output from a successful task.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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
    /// Delta table identifier for lineage correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_table: Option<String>,
    /// Delta version for lineage correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_version: Option<i64>,
    /// Delta partition for lineage correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_partition: Option<String>,
    /// Output visibility state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_visibility_state: Option<TaskOutputVisibilityState>,
    /// Timestamp when output became visible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
    /// Publish failure details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_error: Option<String>,
}

/// Error details from a failed task.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    /// Error category.
    pub category: ErrorCategory,
    /// Error message.
    pub message: String,
    /// Optional stack trace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
    /// Optional retryability override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl TaskError {
    /// Returns retryability, falling back to the category default.
    #[must_use]
    pub fn effective_retryable(&self) -> bool {
        self.retryable
            .unwrap_or_else(|| self.category.default_retryable())
    }
}

/// Worker error category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCategory {
    /// Error in user code.
    UserCode,
    /// Input data failed validation.
    DataQuality,
    /// Infrastructure failure.
    Infrastructure,
    /// Configuration failure.
    Configuration,
    /// Execution timeout.
    Timeout,
    /// Task cancellation.
    Cancelled,
}

impl ErrorCategory {
    /// Returns the default retryability for this category.
    #[must_use]
    pub fn default_retryable(self) -> bool {
        matches!(self, Self::UserCode | Self::Infrastructure | Self::Timeout)
    }
}

/// Execution metrics reported by the worker.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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

/// Error response for callback failures.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CallbackErrorResponse {
    /// Stable error code.
    pub error: String,
    /// Human-readable message.
    pub message: String,
    /// Current task state, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Expected attempt, when relevant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_attempt: Option<u32>,
    /// Received attempt, when relevant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_attempt: Option<u32>,
    /// Expected attempt id, when relevant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_attempt_id: Option<String>,
    /// Received attempt id, when relevant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_attempt_id: Option<String>,
}

fn deserialize_progress_pct<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u8>::deserialize(deserializer)?;
    if let Some(progress) = value {
        if progress > 100 {
            return Err(de::Error::custom("progressPct must be between 0 and 100"));
        }
    }
    Ok(value)
}
