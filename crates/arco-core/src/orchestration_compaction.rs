//! Shared request/response types for orchestration compaction RPCs.

use serde::{Deserialize, Serialize};

use crate::sync_compact::VisibilityStatus;

/// Request for orchestration compaction over explicit event paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationCompactRequest {
    /// Explicit event file paths to process.
    pub event_paths: Vec<String>,

    /// Fencing token from the orchestration compaction lock.
    pub fencing_token: u64,

    /// Canonical lock path held by the caller.
    pub lock_path: String,

    /// Optional request identifier for tracing and idempotency diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Request for orchestration rebuild from an explicit rebuild manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationRebuildRequest {
    /// Path to the stored rebuild manifest JSON.
    pub rebuild_manifest_path: String,

    /// Fencing token from the orchestration compaction lock.
    pub fencing_token: u64,

    /// Canonical lock path held by the caller.
    pub lock_path: String,

    /// Optional request identifier for tracing and idempotency diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Response from orchestration compaction/rebuild requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrchestrationCompactionResponse {
    /// Number of events processed.
    pub events_processed: u32,

    /// Delta identifier when a new delta was written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_id: Option<String>,

    /// New manifest revision identifier.
    pub manifest_revision: String,

    /// Visibility outcome of this compaction request.
    pub visibility_status: VisibilityStatus,

    /// Whether post-commit repair is still pending.
    #[serde(default)]
    pub repair_pending: bool,
}
