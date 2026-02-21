//! Synchronous compaction handler for Tier-1 strong consistency (ADR-018).
//!
//! This module implements the sync compaction RPC that allows API to trigger
//! immediate compaction of specific events while holding a distributed lock.
//!
//! # Architecture (ADR-018)
//!
//! The sync compaction flow is:
//! 1. API acquires distributed lock (fencing token N)
//! 2. API appends event(s) to `ledger/{domain}/`
//! 3. API calls sync compaction RPC with explicit event paths + fencing token
//! 4. Compactor validates fencing token matches current lock holder
//! 5. Compactor processes events (no listing!) and publishes manifest
//! 6. API receives confirmation
//! 7. API releases lock
//!
//! # Critical Invariants
//!
//! - **No listing**: Events are passed explicitly, not discovered via list
//! - **Fencing validation**: Stale lock epochs are rejected
//! - **Sole writer**: Only compactor writes Parquet + manifest

use serde::{Deserialize, Serialize};

use arco_catalog::{Tier1CompactionError, Tier1Compactor};
use arco_core::ScopedStorage;

// Re-export request/response types for use by main.rs handlers.
pub use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

/// Error from synchronous compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncCompactError {
    /// Fencing token doesn't match current lock holder.
    StaleFencingToken {
        /// Expected fencing token value.
        expected: u64,
        /// Provided fencing token value.
        provided: u64,
    },
    /// Sync compaction is not yet implemented.
    NotImplemented {
        /// Domain requested for sync compaction.
        domain: String,
        /// Human-readable explanation.
        message: String,
    },
    /// Domain is not supported for sync compaction.
    UnsupportedDomain {
        /// The unsupported domain name.
        domain: String,
    },
    /// Failed to read event files.
    EventReadError {
        /// Path to the event file.
        path: String,
        /// Error message.
        message: String,
    },
    /// Failed to process events.
    ProcessingError {
        /// Error message.
        message: String,
    },
    /// Failed to publish manifest (CAS conflict).
    PublishFailed {
        /// Error message.
        message: String,
    },
}

impl std::fmt::Display for SyncCompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleFencingToken { expected, provided } => {
                write!(
                    f,
                    "stale fencing token: expected {expected}, got {provided}"
                )
            }
            Self::NotImplemented { domain, message } => {
                write!(f, "sync compaction not implemented for {domain}: {message}")
            }
            Self::UnsupportedDomain { domain } => {
                write!(f, "unsupported domain for sync compaction: {domain}")
            }
            Self::EventReadError { path, message } => {
                write!(f, "failed to read event '{path}': {message}")
            }
            Self::ProcessingError { message } => {
                write!(f, "event processing error: {message}")
            }
            Self::PublishFailed { message } => {
                write!(f, "manifest publish failed: {message}")
            }
        }
    }
}

impl std::error::Error for SyncCompactError {}

/// Handler for synchronous compaction requests.
///
/// This is the compactor-side implementation of the sync compaction RPC.
///
/// # Example
///
/// ```rust,ignore
/// use arco_compactor::sync_compact::{SyncCompactHandler, SyncCompactRequest};
///
/// let handler = SyncCompactHandler::new(storage);
/// let response = handler.handle(request).await?;
/// ```
pub struct SyncCompactHandler {
    storage: ScopedStorage,
}

impl SyncCompactHandler {
    /// Creates a new sync compaction handler.
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Handles a synchronous compaction request.
    ///
    /// # Process
    ///
    /// 1. Validate fencing token matches current lock holder
    /// 2. Load events from explicit paths (NO listing)
    /// 3. Read current manifest
    /// 4. Fold events into new snapshot
    /// 5. Write Parquet to state/
    /// 6. Publish manifest via CAS
    /// 7. Return new manifest version
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Fencing token is stale
    /// - Event files cannot be read
    /// - Processing fails
    /// - Manifest CAS fails
    #[allow(clippy::unused_async)] // async needed for future implementation
    pub async fn handle(
        &self,
        request: SyncCompactRequest,
    ) -> Result<SyncCompactResponse, SyncCompactError> {
        tracing::info!(
            domain = %request.domain,
            event_count = request.event_paths.len(),
            fencing_token = request.fencing_token,
            request_id = ?request.request_id,
            "handling sync compaction request"
        );

        let compactor = Tier1Compactor::new(self.storage.clone());

        match compactor
            .sync_compact(&request.domain, request.event_paths, request.fencing_token)
            .await
        {
            Ok(result) => Ok(SyncCompactResponse {
                manifest_version: result.manifest_version,
                commit_ulid: result.commit_ulid,
                events_processed: result.events_processed,
                snapshot_version: result.snapshot_version,
            }),
            Err(Tier1CompactionError::StaleFencingToken { expected, provided }) => {
                Err(SyncCompactError::StaleFencingToken { expected, provided })
            }
            Err(Tier1CompactionError::UnsupportedDomain { domain }) => {
                Err(SyncCompactError::UnsupportedDomain { domain })
            }
            Err(Tier1CompactionError::NotImplemented { domain, message }) => {
                Err(SyncCompactError::NotImplemented { domain, message })
            }
            Err(Tier1CompactionError::EventReadError { path, message }) => {
                Err(SyncCompactError::EventReadError { path, message })
            }
            Err(Tier1CompactionError::ProcessingError { message }) => {
                Err(SyncCompactError::ProcessingError { message })
            }
            Err(Tier1CompactionError::PublishFailed { message }) => {
                Err(SyncCompactError::PublishFailed { message })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_compact_request_serialization() {
        let request = SyncCompactRequest {
            domain: "catalog".to_string(),
            event_paths: vec!["ledger/catalog/01JFXYZ.json".to_string()],
            fencing_token: 42,
            request_id: Some("req-123".to_string()),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        let parsed: SyncCompactRequest = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.domain, "catalog");
        assert_eq!(parsed.event_paths.len(), 1);
        assert_eq!(parsed.fencing_token, 42);
    }

    #[test]
    fn test_sync_compact_error_display() {
        let err = SyncCompactError::StaleFencingToken {
            expected: 5,
            provided: 3,
        };
        assert!(err.to_string().contains("stale fencing token"));

        let err = SyncCompactError::NotImplemented {
            domain: "catalog".to_string(),
            message: "not ready".to_string(),
        };
        assert!(err.to_string().contains("not implemented"));

        let err = SyncCompactError::UnsupportedDomain {
            domain: "unknown".to_string(),
        };
        assert!(err.to_string().contains("unsupported domain"));
    }

    #[tokio::test]
    async fn test_sync_compact_executions_is_unsupported() {
        let backend = std::sync::Arc::new(arco_core::storage::MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").expect("storage");
        let handler = SyncCompactHandler::new(storage);
        let request = SyncCompactRequest {
            domain: "executions".to_string(),
            event_paths: vec!["ledger/executions/01JFXYZ.json".to_string()],
            fencing_token: 1,
            request_id: None,
        };

        let result = handler.handle(request).await;
        assert!(matches!(
            result,
            Err(SyncCompactError::UnsupportedDomain { .. })
        ));
    }
}
