//! Shared Delta types (requests, responses, coordinator state).

use serde::{Deserialize, Serialize};

/// Result of staging a Delta commit payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedCommit {
    /// Scope-relative path where the staged payload was stored.
    pub staged_path: String,
    /// Object version token for the staged payload (used for CAS validation).
    pub staged_version: String,
}

/// Request to finalize a staged Delta commit (Mode B / Arco-coordinated).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDeltaRequest {
    /// Optimistic concurrency token: the version the client read.
    pub read_version: i64,
    /// Path returned by [`StagedCommit::staged_path`].
    pub staged_path: String,
    /// Version returned by [`StagedCommit::staged_version`].
    pub staged_version: String,
    /// Idempotency key for safe retries (`UUIDv7` in canonical form).
    pub idempotency_key: String,
}

/// Response for a successful Delta commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDeltaResponse {
    /// Delta log version committed.
    pub version: i64,
    /// Scope-relative path of the committed delta log JSON file.
    pub delta_log_path: String,
}

/// Coordinator state stored as JSON in object storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaCoordinatorState {
    /// Latest committed Delta log version (starts at -1 for empty tables).
    ///
    /// During crash recovery this can lag a materialized delta log until
    /// replay/recovery finalization advances it.
    pub latest_version: i64,
    /// Inflight reserved commit (used for crash recovery).
    ///
    /// Replay paths are responsible for clearing stale matching inflight state.
    pub inflight: Option<InflightCommit>,
}

impl Default for DeltaCoordinatorState {
    fn default() -> Self {
        Self {
            latest_version: -1,
            inflight: None,
        }
    }
}

/// Details of an inflight (reserved) commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InflightCommit {
    /// Commit identifier (typically derived from the idempotency key).
    pub commit_id: String,
    /// Optimistic concurrency token observed by the client when reserving this commit.
    ///
    /// Used to reconstruct the idempotency request hash during crash recovery.
    /// When missing (older state), it can be inferred as `version - 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_version: Option<i64>,
    /// Reserved Delta log version to write.
    pub version: i64,
    /// Staged payload path.
    pub staged_path: String,
    /// Staged payload object version.
    pub staged_version: String,
    /// Start timestamp in milliseconds since epoch.
    pub started_at_ms: i64,
    /// Expiry timestamp in milliseconds since epoch.
    pub expires_at_ms: i64,
}
