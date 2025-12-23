//! Durable idempotency markers for exactly-once commit semantics.
//!
//! See design doc Section 3 for the two-phase marker protocol.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;
use uuid::Uuid;

use crate::error::IcebergErrorResponse;

/// Status of an idempotency marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IdempotencyStatus {
    /// Commit is in progress - marker claimed, not yet finalized.
    InProgress,
    /// Commit succeeded - response cached for replay.
    Committed,
    /// Commit failed with a terminal error - error cached for replay.
    Failed,
}

/// Durable idempotency marker for commit deduplication.
///
/// Path: `_catalog/iceberg_idempotency/{table_uuid}/{hash_prefix}/{idempotency_key_hash}.json`
///
/// The marker goes through these states:
/// 1. `InProgress` - Claimed before starting commit work
/// 2. `Committed` - Finalized after successful pointer CAS
/// 3. `Failed` - Finalized after terminal 4xx error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyMarker {
    /// Current status of this marker.
    pub status: IdempotencyStatus,

    /// The raw idempotency key from the request header.
    pub idempotency_key: String,

    /// SHA256 hash of the idempotency key (used in path).
    pub idempotency_key_hash: String,

    /// The table this commit targets.
    pub table_uuid: Uuid,

    /// SHA256 hash of the canonical request body (RFC 8785 JCS).
    pub request_hash: String,

    /// When this marker was created/claimed.
    pub started_at: DateTime<Utc>,

    /// When the commit was finalized (success or failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<DateTime<Utc>>,

    /// When the commit failed (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<DateTime<Utc>>,

    /// HTTP status code of the failure (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_http_status: Option<u16>,

    /// Cached error response (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_payload: Option<IcebergErrorResponse>,

    /// Event ID allocated at claim time (stable for crash recovery).
    pub event_id: Ulid,

    /// Pointer's metadata location when marker was claimed.
    pub base_metadata_location: String,

    /// Deterministic new metadata location for this commit.
    pub metadata_location: String,

    /// Cached success response (only set for Committed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_metadata_location: Option<String>,
}

impl IdempotencyMarker {
    /// Computes the SHA256 hash of an idempotency key.
    #[must_use]
    pub fn hash_key(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Returns the storage path for this marker.
    ///
    /// Path: `_catalog/iceberg_idempotency/{table_uuid}/{hash_prefix}/{key_hash}.json`
    #[must_use]
    pub fn storage_path(table_uuid: &Uuid, idempotency_key_hash: &str) -> String {
        let prefix = &idempotency_key_hash[..2.min(idempotency_key_hash.len())];
        format!("_catalog/iceberg_idempotency/{table_uuid}/{prefix}/{idempotency_key_hash}.json")
    }

    /// Creates a new in-progress marker.
    #[must_use]
    pub fn new_in_progress(
        idempotency_key: String,
        table_uuid: Uuid,
        request_hash: String,
        base_metadata_location: String,
        metadata_location: String,
    ) -> Self {
        let idempotency_key_hash = Self::hash_key(&idempotency_key);
        Self {
            status: IdempotencyStatus::InProgress,
            idempotency_key,
            idempotency_key_hash,
            table_uuid,
            request_hash,
            started_at: Utc::now(),
            committed_at: None,
            failed_at: None,
            error_http_status: None,
            error_payload: None,
            event_id: Ulid::new(),
            base_metadata_location,
            metadata_location,
            response_metadata_location: None,
        }
    }

    /// Finalizes the marker as committed.
    #[must_use]
    pub fn finalize_committed(mut self, response_metadata_location: String) -> Self {
        self.status = IdempotencyStatus::Committed;
        self.committed_at = Some(Utc::now());
        self.failed_at = None;
        self.error_http_status = None;
        self.error_payload = None;
        self.response_metadata_location = Some(response_metadata_location);
        self
    }

    /// Finalizes the marker as failed.
    #[must_use]
    pub fn finalize_failed(mut self, http_status: u16, error: IcebergErrorResponse) -> Self {
        debug_assert!(
            (400..500).contains(&http_status),
            "finalize_failed should only be used for terminal 4xx responses"
        );
        self.status = IdempotencyStatus::Failed;
        self.failed_at = Some(Utc::now());
        self.error_http_status = Some(http_status);
        self.error_payload = Some(error);
        self.committed_at = None;
        self.response_metadata_location = None;
        self
    }

    /// Returns whether this marker can be taken over (stale in-progress).
    #[must_use]
    pub fn is_stale(&self, timeout: chrono::Duration) -> bool {
        self.status == IdempotencyStatus::InProgress && self.started_at + timeout < Utc::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IcebergError;

    #[test]
    fn test_hash_key_deterministic() {
        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash1 = IdempotencyMarker::hash_key(key);
        let hash2 = IdempotencyMarker::hash_key(key);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn test_storage_path_format() {
        let table_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let key_hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let path = IdempotencyMarker::storage_path(&table_uuid, key_hash);
        assert_eq!(
            path,
            "_catalog/iceberg_idempotency/550e8400-e29b-41d4-a716-446655440000/ab/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.json"
        );
    }

    #[test]
    fn test_marker_state_transitions() {
        let marker = IdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            Uuid::new_v4(),
            "request_hash".to_string(),
            "base/metadata.json".to_string(),
            "new/metadata.json".to_string(),
        );
        assert_eq!(marker.status, IdempotencyStatus::InProgress);
        assert!(marker.committed_at.is_none());

        let committed = marker.finalize_committed("new/metadata.json".to_string());
        assert_eq!(committed.status, IdempotencyStatus::Committed);
        assert!(committed.committed_at.is_some());
        assert!(committed.failed_at.is_none());
        assert!(committed.error_http_status.is_none());
        assert!(committed.error_payload.is_none());
    }

    #[test]
    fn test_marker_serialization_roundtrip() {
        let marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        let json = serde_json::to_string(&marker).expect("serialize");
        let parsed: IdempotencyMarker = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(marker.idempotency_key, parsed.idempotency_key);
        assert_eq!(marker.status, parsed.status);
    }

    #[test]
    fn test_marker_finalize_failed_clears_commit_fields() {
        let marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        let error_payload = IcebergErrorResponse::from(&IcebergError::commit_conflict("conflict"));

        let failed = marker.finalize_failed(409, error_payload);
        assert_eq!(failed.status, IdempotencyStatus::Failed);
        assert!(failed.failed_at.is_some());
        assert_eq!(failed.error_http_status, Some(409));
        assert!(failed.error_payload.is_some());
        assert!(failed.committed_at.is_none());
        assert!(failed.response_metadata_location.is_none());

        let json = serde_json::to_string(&failed).expect("serialize");
        let parsed: IdempotencyMarker = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.status, IdempotencyStatus::Failed);
        assert_eq!(parsed.error_http_status, Some(409));
        assert!(parsed.error_payload.is_some());
    }

    #[test]
    fn test_marker_staleness_boundaries() {
        let mut marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        let timeout = chrono::Duration::minutes(10);

        marker.started_at = Utc::now() - timeout - chrono::Duration::seconds(1);
        assert!(marker.is_stale(timeout));

        marker.started_at = Utc::now() - timeout + chrono::Duration::seconds(10);
        assert!(!marker.is_stale(timeout));
    }
}
