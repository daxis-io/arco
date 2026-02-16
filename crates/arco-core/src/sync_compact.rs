//! Shared request/response types for synchronous Tier-1 compaction (ADR-018).

use serde::{Deserialize, Serialize};

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
}
