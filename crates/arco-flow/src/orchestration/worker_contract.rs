//! Canonical worker dispatch contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Canonical dispatch envelope sent to external workers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerDispatchEnvelope {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Task key within the run.
    pub task_key: String,
    /// Attempt number (1-indexed).
    pub attempt: u32,
    /// Attempt identifier used for stale-worker protection.
    pub attempt_id: String,
    /// Dispatch identifier.
    pub dispatch_id: String,
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
