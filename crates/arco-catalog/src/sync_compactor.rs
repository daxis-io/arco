//! Sync compaction client abstraction for Tier-1 DDL operations (ADR-018).

use async_trait::async_trait;

use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

use crate::error::{CatalogError, Result};
use crate::tier1_compactor::Tier1Compactor;

/// Trait for invoking synchronous compaction.
#[async_trait]
pub trait SyncCompactor: Send + Sync {
    /// Runs sync compaction for a set of explicit event paths.
    async fn sync_compact(&self, request: SyncCompactRequest) -> Result<SyncCompactResponse>;
}

#[async_trait]
impl SyncCompactor for Tier1Compactor {
    async fn sync_compact(&self, request: SyncCompactRequest) -> Result<SyncCompactResponse> {
        let result = Self::sync_compact(
            self,
            &request.domain,
            request.event_paths,
            request.fencing_token,
        )
        .await
        .map_err(CatalogError::from)?;

        Ok(SyncCompactResponse {
            manifest_version: result.manifest_version,
            commit_ulid: result.commit_ulid,
            events_processed: result.events_processed,
            snapshot_version: result.snapshot_version,
        })
    }
}
