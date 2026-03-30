//! Shared request/response types for synchronous Tier-1 compaction (ADR-018).

use serde::{Deserialize, Serialize};

/// Visibility outcome for a control-plane publish.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VisibilityStatus {
    /// Pointer CAS publish succeeded; state is visible to readers.
    Visible,
    /// Immutable state persisted, but pointer visibility did not advance.
    PersistedNotVisible,
}

impl VisibilityStatus {
    /// Stable string representation for logging and responses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::PersistedNotVisible => "persisted_not_visible",
        }
    }
}

/// Request for synchronous compaction (Tier-1 DDL operations).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCompactRequest {
    /// Domain to compact (e.g., "catalog", "lineage").
    pub domain: String,

    /// Explicit event file paths to process (no listing).
    ///
    /// Paths are relative to the tenant/workspace root.
    /// Example: `["ledger/catalog/01JFXYZ.json"]`
    pub event_paths: Vec<String>,

    /// Fencing token from the distributed lock.
    ///
    /// Semantically this is the lock epoch: compaction requests must carry the
    /// current lock sequence number and stale epochs must be rejected.
    pub fencing_token: u64,

    /// Optional canonical lock path supplied by the caller.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_path: Option<String>,

    /// Optional request ID for tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Response from synchronous compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCompactResponse {
    /// New manifest version after compaction.
    pub manifest_version: String,

    /// Commit ULID for audit trail.
    pub commit_ulid: String,

    /// Number of events processed.
    pub events_processed: usize,

    /// Snapshot version after compaction.
    pub snapshot_version: u64,

    /// Visibility outcome for this compaction acknowledgement.
    pub visibility_status: VisibilityStatus,

    /// Whether post-commit repair is still pending.
    #[serde(default)]
    pub repair_pending: bool,
}
