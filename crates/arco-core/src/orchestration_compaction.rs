//! Shared request/response types for orchestration compaction RPCs.

use serde::{Deserialize, Serialize};

use crate::sync_compact::VisibilityStatus;

/// Request for orchestration compaction over explicit event paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrchestrationCompactRequest {
    /// Explicit event file paths to process.
    pub event_paths: Vec<String>,

    /// Fencing token from the orchestration compaction lock.
    ///
    /// During PI-1 migration this also accepts the legacy `epoch` field name.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "epoch")]
    pub fencing_token: Option<u64>,

    /// Canonical lock path held by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_path: Option<String>,

    /// Optional request identifier for tracing and idempotency diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Request for orchestration rebuild from an explicit rebuild manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrchestrationRebuildRequest {
    /// Path to the stored rebuild manifest JSON.
    pub rebuild_manifest_path: String,

    /// Fencing token from the orchestration compaction lock.
    ///
    /// During PI-1 migration this also accepts the legacy `epoch` field name.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "epoch")]
    pub fencing_token: Option<u64>,

    /// Canonical lock path held by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_path: Option<String>,

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
