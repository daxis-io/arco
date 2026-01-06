//! Durable idempotency markers for exactly-once catalog DDL semantics.
//!
//! This module provides idempotency support for catalog DDL operations:
//! - `create_namespace`: Creates a namespace with idempotent retry semantics
//! - `register_table`: Registers a table with idempotent retry semantics
//!
//! ## Protocol
//!
//! 1. Client sends request with `Idempotency-Key` header (must be `UUIDv7`)
//! 2. Server computes SHA256 hash of canonical request body (RFC 8785 JCS)
//! 3. Server attempts to claim marker (`DoesNotExist` precondition):
//!    - Success: Proceed with operation, finalize marker on completion
//!    - Exists with same `request_hash`: Return cached response (idempotent replay)
//!    - Exists with different `request_hash`: Return 409 Conflict
//!
//! ## Storage Layout
//!
//! ```text
//! _catalog/idempotency/{operation}/{key_hash_prefix}/{key_hash}.json
//! ```
//!
//! Where:
//! - `operation`: One of `create_namespace`, `register_table`
//! - `key_hash_prefix`: First 2 characters of `SHA256(idempotency_key)`
//! - `key_hash`: Full `SHA256(idempotency_key)`

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{CatalogError, Result};
use crate::metrics::{record_idempotency_check, record_idempotency_takeover};

/// Prefix for catalog idempotency markers.
pub const CATALOG_IDEMPOTENCY_PREFIX: &str = "_catalog/idempotency";

/// Default timeout for stale in-progress markers (5 minutes).
pub const DEFAULT_STALE_TIMEOUT: chrono::Duration = chrono::Duration::minutes(5);

/// Calculates Retry-After value with jitter to prevent thundering herd.
///
/// Returns remaining time until marker becomes stale, with +0% to +20% jitter,
/// clamped to `[1, 300]` seconds. Jitter is non-negative so clients retry
/// at or after the stale deadline, avoiding wasted 409 responses.
#[must_use]
pub fn calculate_retry_after(started_at: DateTime<Utc>, stale_timeout: chrono::Duration) -> u64 {
    calculate_retry_after_at(Utc::now(), started_at, stale_timeout)
}

#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn calculate_retry_after_at(
    now: DateTime<Utc>,
    started_at: DateTime<Utc>,
    stale_timeout: chrono::Duration,
) -> u64 {
    let elapsed = now.signed_duration_since(started_at);
    let remaining_secs = (stale_timeout - elapsed).num_seconds().max(0) as f64;

    let nanos = f64::from(now.timestamp_subsec_nanos());
    let jitter_factor = nanos.mul_add(0.2 / 1_000_000_000.0, 1.0);
    let with_jitter = (remaining_secs * jitter_factor) as u64;

    with_jitter.clamp(1, 300)
}

/// Type of catalog DDL operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogOperation {
    /// Create a namespace.
    CreateNamespace,
    /// Register a table in the catalog.
    RegisterTable,
}

impl CatalogOperation {
    /// Returns the operation as a path segment.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateNamespace => "create_namespace",
            Self::RegisterTable => "register_table",
        }
    }
}

/// Status of an idempotency marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IdempotencyStatus {
    /// Operation is in progress - marker claimed, not yet finalized.
    InProgress,
    /// Operation succeeded - response cached for replay.
    Committed,
    /// Operation failed with a terminal error - error cached for replay.
    Failed,
}

/// Durable idempotency marker for catalog DDL deduplication.
///
/// Path: `_catalog/idempotency/{operation}/{key_hash_prefix}/{key_hash}.json`
///
/// The marker goes through these states:
/// 1. `InProgress` - Claimed before starting DDL work
/// 2. `Committed` - Finalized after successful DDL completion
/// 3. `Failed` - Finalized after terminal 4xx error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogIdempotencyMarker {
    /// Current status of this marker.
    pub status: IdempotencyStatus,

    /// The raw idempotency key from the request header.
    pub idempotency_key: String,

    /// SHA256 hash of the idempotency key (used in path).
    pub idempotency_key_hash: String,

    /// Type of catalog operation.
    pub operation: CatalogOperation,

    /// SHA256 hash of the canonical request body (RFC 8785 JCS).
    pub request_hash: String,

    /// When this marker was created/claimed.
    pub started_at: DateTime<Utc>,

    /// When the operation was finalized (success or failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<DateTime<Utc>>,

    /// When the operation failed (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<DateTime<Utc>>,

    /// HTTP status code of the failure (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_http_status: Option<u16>,

    /// Error message (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,

    /// Entity ID created by this operation (`namespace_id` or `table_id`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,

    /// Entity name created by this operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_name: Option<String>,
}

impl CatalogIdempotencyMarker {
    /// Computes the SHA256 hash of an idempotency key.
    #[must_use]
    pub fn hash_key(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Returns the storage path for a marker.
    ///
    /// Path: `_catalog/idempotency/{operation}/{key_hash_prefix}/{key_hash}.json`
    #[must_use]
    pub fn storage_path(operation: CatalogOperation, idempotency_key_hash: &str) -> String {
        let prefix = &idempotency_key_hash[..2.min(idempotency_key_hash.len())];
        format!(
            "{}/{}/{}/{}.json",
            CATALOG_IDEMPOTENCY_PREFIX,
            operation.as_str(),
            prefix,
            idempotency_key_hash
        )
    }

    /// Creates a new in-progress marker.
    #[must_use]
    pub fn new_in_progress(
        idempotency_key: String,
        operation: CatalogOperation,
        request_hash: String,
    ) -> Self {
        let idempotency_key_hash = Self::hash_key(&idempotency_key);
        Self {
            status: IdempotencyStatus::InProgress,
            idempotency_key,
            idempotency_key_hash,
            operation,
            request_hash,
            started_at: Utc::now(),
            committed_at: None,
            failed_at: None,
            error_http_status: None,
            error_message: None,
            entity_id: None,
            entity_name: None,
        }
    }

    /// Finalizes the marker as committed.
    #[must_use]
    pub fn finalize_committed(mut self, entity_id: String, entity_name: String) -> Self {
        self.status = IdempotencyStatus::Committed;
        self.committed_at = Some(Utc::now());
        self.failed_at = None;
        self.error_http_status = None;
        self.error_message = None;
        self.entity_id = Some(entity_id);
        self.entity_name = Some(entity_name);
        self
    }

    /// Finalizes the marker as failed.
    #[must_use]
    pub fn finalize_failed(mut self, http_status: u16, message: String) -> Self {
        debug_assert!(
            (400..500).contains(&http_status),
            "finalize_failed should only be used for terminal 4xx responses"
        );
        self.status = IdempotencyStatus::Failed;
        self.failed_at = Some(Utc::now());
        self.error_http_status = Some(http_status);
        self.error_message = Some(message);
        self.committed_at = None;
        self.entity_id = None;
        self.entity_name = None;
        self
    }

    /// Returns whether this marker can be taken over (stale in-progress).
    #[must_use]
    pub fn is_stale(&self, timeout: chrono::Duration) -> bool {
        self.status == IdempotencyStatus::InProgress && self.started_at + timeout < Utc::now()
    }

    /// Returns the path for this marker.
    #[must_use]
    pub fn path(&self) -> String {
        Self::storage_path(self.operation, &self.idempotency_key_hash)
    }
}

/// Error canonicalizing a JSON request body for idempotency hashing.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalizationError {
    /// Failed to canonicalize JSON per RFC 8785.
    #[error("Failed to canonicalize JSON: {0}")]
    Canonicalize(#[from] serde_json::Error),
}

/// Computes SHA256 hash of RFC 8785 JCS canonical JSON.
///
/// # Errors
///
/// Returns an error if the JSON value cannot be canonicalized.
pub fn canonical_request_hash(value: &serde_json::Value) -> std::result::Result<String, CanonicalizationError> {
    let canonical = serde_jcs::to_string(value)?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

/// Error validating an idempotency key.
#[derive(Debug, thiserror::Error)]
pub enum IdempotencyKeyError {
    /// Key is not a valid UUID.
    #[error("Idempotency-Key must be a valid UUID")]
    InvalidFormat,

    /// Key has a non-RFC4122 variant.
    #[error("Idempotency-Key must use RFC4122 variant, found {found_variant:?}")]
    InvalidVariant {
        /// The variant found in the UUID.
        found_variant: uuid::Variant,
    },

    /// Key is not in canonical string form.
    #[error("Idempotency-Key must be a canonical lowercase UUID string")]
    NotCanonical,

    /// Key is not `UUIDv7`.
    #[error("Idempotency-Key must be UUIDv7 (RFC 9562), found version {found_version}")]
    NotUuidV7 {
        /// The version number found.
        found_version: usize,
    },
}

/// Validates that an idempotency key is a valid `UUIDv7`.
///
/// Per design: Idempotency-Key must be `UUIDv7` (RFC 9562) in canonical string form.
///
/// # Errors
///
/// Returns an error if the key is not a valid `UUIDv7`.
pub fn validate_uuidv7(key: &str) -> std::result::Result<uuid::Uuid, IdempotencyKeyError> {
    let uuid = uuid::Uuid::parse_str(key).map_err(|_| IdempotencyKeyError::InvalidFormat)?;

    if uuid.get_variant() != uuid::Variant::RFC4122 {
        return Err(IdempotencyKeyError::InvalidVariant {
            found_variant: uuid.get_variant(),
        });
    }

    // UUIDv7 has version nibble = 7 (bits 48-51)
    let version = uuid.get_version_num();
    if version != 7 {
        return Err(IdempotencyKeyError::NotUuidV7 {
            found_version: version,
        });
    }

    if uuid.to_string() != key {
        return Err(IdempotencyKeyError::NotCanonical);
    }

    Ok(uuid)
}

/// Object version for CAS operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion(String);

impl ObjectVersion {
    /// Creates a new object version.
    #[must_use]
    pub fn new(version: impl Into<String>) -> Self {
        Self(version.into())
    }

    /// Returns the version as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Result of attempting to claim an idempotency marker.
#[derive(Debug, Clone)]
pub enum ClaimResult {
    /// Successfully claimed the marker (first claim).
    Success {
        /// Version of the written marker.
        version: ObjectVersion,
    },
    /// Marker already exists (duplicate claim).
    Exists {
        /// The existing marker (boxed to reduce enum size).
        marker: Box<CatalogIdempotencyMarker>,
        /// Version of the existing marker.
        version: ObjectVersion,
    },
}

/// Result of a finalize operation.
#[derive(Debug, Clone)]
pub enum FinalizeResult {
    /// Successfully finalized the marker.
    Success {
        /// New version after finalization.
        version: ObjectVersion,
    },
    /// Version mismatch (another process finalized first).
    Conflict {
        /// Current version that caused the conflict.
        current_version: ObjectVersion,
    },
}

/// Result of a takeover attempt.
#[derive(Debug, Clone)]
pub enum TakeoverResult {
    /// Successfully took over the stale marker.
    Success {
        /// The refreshed marker.
        marker: CatalogIdempotencyMarker,
        /// New version after takeover.
        version: ObjectVersion,
    },
    /// CAS failed - marker was modified concurrently.
    RaceDetected {
        /// Current marker state after the race.
        current_marker: Box<CatalogIdempotencyMarker>,
        /// Current version.
        current_version: ObjectVersion,
    },
}

/// Trait for idempotency marker storage operations.
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Claims an idempotency marker (write with `DoesNotExist` precondition).
    async fn claim(&self, marker: &CatalogIdempotencyMarker) -> Result<ClaimResult>;

    /// Loads an existing marker.
    async fn load(
        &self,
        operation: CatalogOperation,
        idempotency_key_hash: &str,
    ) -> Result<Option<(CatalogIdempotencyMarker, ObjectVersion)>>;

    /// Updates a marker with CAS (to finalize as committed/failed).
    async fn finalize(
        &self,
        marker: &CatalogIdempotencyMarker,
        expected_version: &ObjectVersion,
    ) -> Result<FinalizeResult>;

    /// Takes over a stale InProgress marker using CAS.
    async fn takeover(
        &self,
        stale_marker: &CatalogIdempotencyMarker,
        expected_version: &ObjectVersion,
    ) -> Result<TakeoverResult>;
}

/// Implementation of `IdempotencyStore` using `StorageBackend`.
pub struct IdempotencyStoreImpl<S> {
    storage: Arc<S>,
}

impl<S: StorageBackend> IdempotencyStoreImpl<S> {
    /// Creates a new idempotency store.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: StorageBackend> IdempotencyStore for IdempotencyStoreImpl<S> {
    async fn claim(&self, marker: &CatalogIdempotencyMarker) -> Result<ClaimResult> {
        let path = marker.path();
        let bytes = serde_json::to_vec(marker).map_err(|e| CatalogError::InvariantViolation {
            message: format!("Failed to serialize idempotency marker: {e}"),
        })?;

        match self
            .storage
            .put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(ClaimResult::Success {
                version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                // Marker exists - load it to return
                let existing = self
                    .load(marker.operation, &marker.idempotency_key_hash)
                    .await?;
                match existing {
                    Some((existing_marker, _)) => Ok(ClaimResult::Exists {
                        marker: Box::new(existing_marker),
                        version: ObjectVersion::new(current_version),
                    }),
                    None => {
                        // Race condition: marker was deleted between precondition fail and load
                        Err(CatalogError::InvariantViolation {
                            message: "Idempotency marker disappeared during claim".to_string(),
                        })
                    }
                }
            }
            Err(e) => Err(CatalogError::InvariantViolation {
                message: format!("Failed to claim idempotency marker: {e}"),
            }),
        }
    }

    async fn load(
        &self,
        operation: CatalogOperation,
        idempotency_key_hash: &str,
    ) -> Result<Option<(CatalogIdempotencyMarker, ObjectVersion)>> {
        let path = CatalogIdempotencyMarker::storage_path(operation, idempotency_key_hash);

        let meta = self
            .storage
            .head(&path)
            .await
            .map_err(|e| CatalogError::InvariantViolation {
                message: format!("Failed to check idempotency marker existence: {e}"),
            })?;

        let Some(meta) = meta else {
            return Ok(None);
        };

        let bytes = self
            .storage
            .get(&path)
            .await
            .map_err(|e| CatalogError::InvariantViolation {
                message: format!("Failed to read idempotency marker: {e}"),
            })?;

        let marker: CatalogIdempotencyMarker =
            serde_json::from_slice(&bytes).map_err(|e| CatalogError::InvariantViolation {
                message: format!("Failed to parse idempotency marker: {e}"),
            })?;

        Ok(Some((marker, ObjectVersion::new(meta.version))))
    }

    async fn finalize(
        &self,
        marker: &CatalogIdempotencyMarker,
        expected_version: &ObjectVersion,
    ) -> Result<FinalizeResult> {
        let path = marker.path();
        let bytes = serde_json::to_vec(marker).map_err(|e| CatalogError::InvariantViolation {
            message: format!("Failed to serialize idempotency marker: {e}"),
        })?;

        let precondition = WritePrecondition::MatchesVersion(expected_version.as_str().to_string());

        match self
            .storage
            .put(&path, Bytes::from(bytes), precondition)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(FinalizeResult::Success {
                version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                Ok(FinalizeResult::Conflict {
                    current_version: ObjectVersion::new(current_version),
                })
            }
            Err(e) => Err(CatalogError::InvariantViolation {
                message: format!("Failed to finalize idempotency marker: {e}"),
            }),
        }
    }

    async fn takeover(
        &self,
        stale_marker: &CatalogIdempotencyMarker,
        expected_version: &ObjectVersion,
    ) -> Result<TakeoverResult> {
        let refreshed = CatalogIdempotencyMarker::new_in_progress(
            stale_marker.idempotency_key.clone(),
            stale_marker.operation,
            stale_marker.request_hash.clone(),
        );

        let path = refreshed.path();
        let bytes = serde_json::to_vec(&refreshed).map_err(|e| CatalogError::InvariantViolation {
            message: format!("Failed to serialize idempotency marker: {e}"),
        })?;

        let precondition = WritePrecondition::MatchesVersion(expected_version.as_str().to_string());

        match self
            .storage
            .put(&path, Bytes::from(bytes), precondition)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(TakeoverResult::Success {
                marker: refreshed,
                version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                let loaded = self
                    .load(stale_marker.operation, &stale_marker.idempotency_key_hash)
                    .await?;
                match loaded {
                    Some((current_marker, _)) => Ok(TakeoverResult::RaceDetected {
                        current_marker: Box::new(current_marker),
                        current_version: ObjectVersion::new(current_version),
                    }),
                    None => Err(CatalogError::InvariantViolation {
                        message: "Marker disappeared during takeover".to_string(),
                    }),
                }
            }
            Err(e) => Err(CatalogError::InvariantViolation {
                message: format!("Failed to takeover stale marker: {e}"),
            }),
        }
    }
}

/// Result of checking idempotency for a request.
#[derive(Debug)]
pub enum IdempotencyCheck {
    /// No idempotency key provided - proceed without idempotency tracking.
    NoKey,
    /// New request - proceed with operation, marker claimed.
    Proceed {
        /// The claimed marker.
        marker: CatalogIdempotencyMarker,
        /// Version of the marker for finalization.
        version: ObjectVersion,
    },
    /// Idempotent replay - return cached success response.
    Replay {
        /// Entity ID from the original operation.
        entity_id: String,
        /// Entity name from the original operation.
        entity_name: String,
    },
    /// Conflict - same key used with different request payload.
    Conflict,
    /// Previous request failed - return cached error.
    PreviousFailed {
        /// HTTP status of the original failure.
        http_status: u16,
        /// Error message from the original failure.
        message: String,
    },
    /// Previous request still in progress.
    InProgress {
        /// When the in-progress request started (for Retry-After calculation).
        started_at: DateTime<Utc>,
    },
}

/// Checks idempotency for a catalog DDL request.
///
/// # Errors
///
/// Returns an error if the idempotency key is invalid or storage operations fail.
pub async fn check_idempotency<S: StorageBackend>(
    store: &IdempotencyStoreImpl<S>,
    idempotency_key: Option<&str>,
    operation: CatalogOperation,
    request_hash: &str,
    stale_timeout: chrono::Duration,
) -> Result<IdempotencyCheck> {
    let op_str = operation.as_str();

    let Some(key) = idempotency_key else {
        record_idempotency_check(op_str, "no_key");
        return Ok(IdempotencyCheck::NoKey);
    };

    validate_uuidv7(key).map_err(|e| CatalogError::Validation {
        message: e.to_string(),
    })?;

    let marker = CatalogIdempotencyMarker::new_in_progress(
        key.to_string(),
        operation,
        request_hash.to_string(),
    );

    match store.claim(&marker).await? {
        ClaimResult::Success { version } => {
            record_idempotency_check(op_str, "proceed");
            Ok(IdempotencyCheck::Proceed { marker, version })
        }
        ClaimResult::Exists { marker: existing, version: existing_version } => {
            if existing.request_hash != request_hash {
                record_idempotency_check(op_str, "conflict");
                return Ok(IdempotencyCheck::Conflict);
            }

            match existing.status {
                IdempotencyStatus::InProgress => {
                    if existing.is_stale(stale_timeout) {
                        tracing::warn!(
                            idempotency_key = %key,
                            operation = ?operation,
                            started_at = %existing.started_at,
                            "Taking over stale in-progress idempotency marker"
                        );
                        match store.takeover(&existing, &existing_version).await? {
                            TakeoverResult::Success { marker: refreshed, version } => {
                                record_idempotency_takeover(op_str, "success");
                                record_idempotency_check(op_str, "proceed");
                                Ok(IdempotencyCheck::Proceed { marker: refreshed, version })
                            }
                            TakeoverResult::RaceDetected { current_marker, .. } => {
                                record_idempotency_takeover(op_str, "race_detected");
                                handle_marker_status_with_metrics(op_str, &current_marker)
                            }
                        }
                    } else {
                        record_idempotency_check(op_str, "in_progress");
                        Ok(IdempotencyCheck::InProgress {
                            started_at: existing.started_at,
                        })
                    }
                }
                IdempotencyStatus::Committed | IdempotencyStatus::Failed => {
                    handle_marker_status_with_metrics(op_str, &existing)
                }
            }
        }
    }
}

fn handle_marker_status(marker: &CatalogIdempotencyMarker) -> Result<IdempotencyCheck> {
    match marker.status {
        IdempotencyStatus::InProgress => Ok(IdempotencyCheck::InProgress {
            started_at: marker.started_at,
        }),
        IdempotencyStatus::Committed => {
            match (&marker.entity_id, &marker.entity_name) {
                (Some(id), Some(name)) => Ok(IdempotencyCheck::Replay {
                    entity_id: id.clone(),
                    entity_name: name.clone(),
                }),
                _ => Err(CatalogError::InvariantViolation {
                    message: "Committed marker missing entity info".to_string(),
                }),
            }
        }
        IdempotencyStatus::Failed => {
            match (marker.error_http_status, &marker.error_message) {
                (Some(status), Some(msg)) => Ok(IdempotencyCheck::PreviousFailed {
                    http_status: status,
                    message: msg.clone(),
                }),
                _ => Err(CatalogError::InvariantViolation {
                    message: "Failed marker missing error info".to_string(),
                }),
            }
        }
    }
}

fn handle_marker_status_with_metrics(
    operation: &str,
    marker: &CatalogIdempotencyMarker,
) -> Result<IdempotencyCheck> {
    let result = handle_marker_status(marker)?;
    match &result {
        IdempotencyCheck::InProgress { .. } => record_idempotency_check(operation, "in_progress"),
        IdempotencyCheck::Replay { .. } => record_idempotency_check(operation, "replay"),
        IdempotencyCheck::PreviousFailed { .. } => {
            record_idempotency_check(operation, "previous_failed");
        }
        _ => {}
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;

    #[test]
    fn test_hash_key_deterministic() {
        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash1 = CatalogIdempotencyMarker::hash_key(key);
        let hash2 = CatalogIdempotencyMarker::hash_key(key);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn test_storage_path_format() {
        let key_hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let path =
            CatalogIdempotencyMarker::storage_path(CatalogOperation::CreateNamespace, key_hash);
        let expected =
            format!("{CATALOG_IDEMPOTENCY_PREFIX}/create_namespace/ab/{key_hash}.json");
        assert_eq!(path, expected);
    }

    #[test]
    fn test_marker_state_transitions() {
        let marker = CatalogIdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            CatalogOperation::CreateNamespace,
            "request_hash".to_string(),
        );
        assert_eq!(marker.status, IdempotencyStatus::InProgress);
        assert!(marker.committed_at.is_none());

        let committed = marker.finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        assert_eq!(committed.status, IdempotencyStatus::Committed);
        assert!(committed.committed_at.is_some());
        assert!(committed.failed_at.is_none());
        assert_eq!(committed.entity_id, Some("ns_001".to_string()));
        assert_eq!(committed.entity_name, Some("my-namespace".to_string()));
    }

    #[test]
    fn test_marker_finalize_failed() {
        let marker = CatalogIdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            CatalogOperation::RegisterTable,
            "request_hash".to_string(),
        );

        let failed = marker.finalize_failed(409, "Table already exists".to_string());
        assert_eq!(failed.status, IdempotencyStatus::Failed);
        assert!(failed.failed_at.is_some());
        assert_eq!(failed.error_http_status, Some(409));
        assert_eq!(
            failed.error_message,
            Some("Table already exists".to_string())
        );
        assert!(failed.committed_at.is_none());
        assert!(failed.entity_id.is_none());
    }

    #[test]
    fn test_marker_serialization_roundtrip() {
        let marker = CatalogIdempotencyMarker::new_in_progress(
            "key".to_string(),
            CatalogOperation::CreateNamespace,
            "hash".to_string(),
        );
        let json = serde_json::to_string(&marker).expect("serialize");
        let parsed: CatalogIdempotencyMarker = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(marker.idempotency_key, parsed.idempotency_key);
        assert_eq!(marker.status, parsed.status);
        assert_eq!(marker.operation, parsed.operation);
    }

    #[test]
    fn test_validate_uuidv7_valid() {
        // Valid UUIDv7 (version nibble = 7, variant = 10xx)
        let key = "01924a7c-8d9f-7000-8000-000000000001";
        assert!(validate_uuidv7(key).is_ok());
    }

    #[test]
    fn test_validate_uuidv7_invalid_version() {
        // UUIDv4 (version nibble = 4)
        let key = "550e8400-e29b-41d4-a716-446655440000";
        assert!(validate_uuidv7(key).is_err());
    }

    #[test]
    fn test_validate_uuidv7_invalid_format() {
        let key = "not-a-uuid";
        assert!(validate_uuidv7(key).is_err());
    }

    #[test]
    fn test_canonical_hash_deterministic() {
        let request1 = serde_json::json!({
            "name": "my-namespace",
            "description": "A test namespace"
        });
        let request2 = serde_json::json!({
            "description": "A test namespace",
            "name": "my-namespace"
        });
        // Same content with different key order should produce same hash
        let hash1 = canonical_request_hash(&request1).expect("canonical hash");
        let hash2 = canonical_request_hash(&request2).expect("canonical hash");
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // SHA256 hex is 64 chars
    }

    #[test]
    fn test_canonical_hash_different_content() {
        let request1 = serde_json::json!({"name": "ns1"});
        let request2 = serde_json::json!({"name": "ns2"});
        let hash1 = canonical_request_hash(&request1).expect("canonical hash");
        let hash2 = canonical_request_hash(&request2).expect("canonical hash");
        assert_ne!(hash1, hash2);
    }

    #[tokio::test]
    async fn test_idempotency_store_claim() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let marker = CatalogIdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            CatalogOperation::CreateNamespace,
            "request_hash".to_string(),
        );

        // First claim succeeds
        let result = store.claim(&marker).await.expect("claim");
        assert!(matches!(result, ClaimResult::Success { .. }));

        // Second claim finds existing
        let result = store.claim(&marker).await.expect("claim");
        assert!(matches!(result, ClaimResult::Exists { .. }));
    }

    #[tokio::test]
    async fn test_idempotency_store_finalize() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let marker = CatalogIdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            CatalogOperation::CreateNamespace,
            "request_hash".to_string(),
        );

        let result = store.claim(&marker).await.expect("claim");
        let ClaimResult::Success { version } = result else {
            panic!("expected success");
        };

        // Finalize with correct version
        let finalized = marker.finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        let result = store.finalize(&finalized, &version).await.expect("finalize");
        assert!(matches!(result, FinalizeResult::Success { .. }));

        // Load and verify
        let (loaded, _) = store
            .load(
                CatalogOperation::CreateNamespace,
                &finalized.idempotency_key_hash,
            )
            .await
            .expect("load")
            .expect("marker exists");
        assert_eq!(loaded.status, IdempotencyStatus::Committed);
        assert_eq!(loaded.entity_id, Some("ns_001".to_string()));
    }

    #[tokio::test]
    async fn test_idempotency_store_finalize_conflict() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let marker = CatalogIdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            CatalogOperation::CreateNamespace,
            "request_hash".to_string(),
        );

        let result = store.claim(&marker).await.expect("claim");
        let ClaimResult::Success { .. } = result else {
            panic!("expected success");
        };

        let finalized = marker.finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        let stale_version = ObjectVersion::new("stale");
        let result = store
            .finalize(&finalized, &stale_version)
            .await
            .expect("finalize");
        assert!(matches!(result, FinalizeResult::Conflict { .. }));
    }

    #[tokio::test]
    async fn test_check_idempotency_no_key() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let result = check_idempotency(
            &store,
            None,
            CatalogOperation::CreateNamespace,
            "hash",
            DEFAULT_STALE_TIMEOUT,
        )
        .await
        .expect("check");
        assert!(matches!(result, IdempotencyCheck::NoKey));
    }

    #[tokio::test]
    async fn test_check_idempotency_new_request() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let result = check_idempotency(
            &store,
            Some("01924a7c-8d9f-7000-8000-000000000001"),
            CatalogOperation::CreateNamespace,
            "hash",
            DEFAULT_STALE_TIMEOUT,
        )
        .await
        .expect("check");
        assert!(matches!(result, IdempotencyCheck::Proceed { .. }));
    }

    #[tokio::test]
    async fn test_check_idempotency_replay() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash = "test_hash";

        // First request - claim and finalize
        let marker = CatalogIdempotencyMarker::new_in_progress(
            key.to_string(),
            CatalogOperation::CreateNamespace,
            hash.to_string(),
        );
        let result = store.claim(&marker).await.expect("claim");
        let ClaimResult::Success { version } = result else {
            panic!("expected success");
        };
        let finalized = marker.finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        store.finalize(&finalized, &version).await.expect("finalize");

        let result = check_idempotency(
            &store,
            Some(key),
            CatalogOperation::CreateNamespace,
            hash,
            DEFAULT_STALE_TIMEOUT,
        )
        .await
        .expect("check");
        match result {
            IdempotencyCheck::Replay { entity_id, entity_name } => {
                assert_eq!(entity_id, "ns_001");
                assert_eq!(entity_name, "my-namespace");
            }
            _ => panic!("expected Replay, got {result:?}"),
        }
    }

    #[tokio::test]
    async fn test_check_idempotency_conflict() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let key = "01924a7c-8d9f-7000-8000-000000000001";

        let marker = CatalogIdempotencyMarker::new_in_progress(
            key.to_string(),
            CatalogOperation::CreateNamespace,
            "hash1".to_string(),
        );
        let result = store.claim(&marker).await.expect("claim");
        let ClaimResult::Success { version } = result else {
            panic!("expected success");
        };
        let finalized = marker.finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        store.finalize(&finalized, &version).await.expect("finalize");

        let result = check_idempotency(
            &store,
            Some(key),
            CatalogOperation::CreateNamespace,
            "hash2",
            DEFAULT_STALE_TIMEOUT,
        )
        .await
        .expect("check");
        assert!(matches!(result, IdempotencyCheck::Conflict));
    }

    #[tokio::test]
    async fn test_check_idempotency_stale_takeover() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash = "test_hash";

        let mut marker = CatalogIdempotencyMarker::new_in_progress(
            key.to_string(),
            CatalogOperation::CreateNamespace,
            hash.to_string(),
        );
        marker.started_at = Utc::now() - chrono::Duration::minutes(10);

        let result = store.claim(&marker).await.expect("claim");
        assert!(matches!(result, ClaimResult::Success { .. }));

        let result = check_idempotency(
            &store,
            Some(key),
            CatalogOperation::CreateNamespace,
            hash,
            chrono::Duration::minutes(5),
        )
        .await
        .expect("check");

        assert!(
            matches!(result, IdempotencyCheck::Proceed { .. }),
            "stale marker should allow takeover, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_check_idempotency_in_progress_not_stale() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash = "test_hash";

        let marker = CatalogIdempotencyMarker::new_in_progress(
            key.to_string(),
            CatalogOperation::CreateNamespace,
            hash.to_string(),
        );

        let result = store.claim(&marker).await.expect("claim");
        assert!(matches!(result, ClaimResult::Success { .. }));

        let result = check_idempotency(
            &store,
            Some(key),
            CatalogOperation::CreateNamespace,
            hash,
            chrono::Duration::minutes(5),
        )
        .await
        .expect("check");

        assert!(
            matches!(result, IdempotencyCheck::InProgress { .. }),
            "fresh in-progress marker should not allow takeover, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_takeover_race_returns_replay_when_finalized() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash = "test_hash";

        let mut marker = CatalogIdempotencyMarker::new_in_progress(
            key.to_string(),
            CatalogOperation::CreateNamespace,
            hash.to_string(),
        );
        marker.started_at = Utc::now() - chrono::Duration::minutes(10);

        let result = store.claim(&marker).await.expect("claim");
        let ClaimResult::Success { version } = result else {
            panic!("expected success");
        };

        let finalized = marker.finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        store.finalize(&finalized, &version).await.expect("finalize");

        let result = check_idempotency(
            &store,
            Some(key),
            CatalogOperation::CreateNamespace,
            hash,
            chrono::Duration::minutes(5),
        )
        .await
        .expect("check");

        match result {
            IdempotencyCheck::Replay { entity_id, entity_name } => {
                assert_eq!(entity_id, "ns_001");
                assert_eq!(entity_name, "my-namespace");
            }
            _ => panic!("expected Replay after concurrent finalization, got {result:?}"),
        }
    }

    #[test]
    fn test_calculate_retry_after_bounds() {
        let stale_timeout = chrono::Duration::minutes(5);

        let fresh_start = Utc::now();
        let retry = calculate_retry_after(fresh_start, stale_timeout);
        assert!(retry >= 1 && retry <= 300, "fresh marker retry={retry}");

        let old_start = Utc::now() - chrono::Duration::minutes(10);
        let retry = calculate_retry_after(old_start, stale_timeout);
        assert_eq!(retry, 1, "expired marker should return min bound");
    }

    #[test]
    fn test_calculate_retry_after_jitter_range() {
        let stale_timeout = chrono::Duration::seconds(100);
        let started_at = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let now_min_jitter = DateTime::parse_from_rfc3339("2025-01-01T00:00:00.000000000Z")
            .unwrap()
            .with_timezone(&Utc);
        let retry_min = calculate_retry_after_at(now_min_jitter, started_at, stale_timeout);
        assert_eq!(retry_min, 100, "0 nanos → factor 1.0 → 100s");

        let now_max_jitter = DateTime::parse_from_rfc3339("2025-01-01T00:00:00.999999999Z")
            .unwrap()
            .with_timezone(&Utc);
        let retry_max = calculate_retry_after_at(now_max_jitter, started_at, stale_timeout);
        assert!(
            (118..=120).contains(&retry_max),
            "999M nanos → factor ~1.2 → ~120s, got {retry_max}"
        );

        assert!(
            retry_max >= retry_min,
            "jitter must be non-negative: min={retry_min}, max={retry_max}"
        );
    }

    #[tokio::test]
    async fn test_takeover_cas_preserves_finalized_state() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IdempotencyStoreImpl::new(storage);

        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash = "test_hash";

        let mut marker = CatalogIdempotencyMarker::new_in_progress(
            key.to_string(),
            CatalogOperation::CreateNamespace,
            hash.to_string(),
        );
        marker.started_at = Utc::now() - chrono::Duration::minutes(10);

        let result = store.claim(&marker).await.expect("claim");
        let ClaimResult::Success { version } = result else {
            panic!("expected success");
        };

        let finalized = marker.clone().finalize_committed("ns_001".to_string(), "my-namespace".to_string());
        store.finalize(&finalized, &version).await.expect("finalize");

        let stale_version = ObjectVersion::new("stale_version");
        let takeover_result = store.takeover(&marker, &stale_version).await.expect("takeover");

        match takeover_result {
            TakeoverResult::RaceDetected { current_marker, .. } => {
                assert_eq!(current_marker.status, IdempotencyStatus::Committed);
                assert_eq!(current_marker.entity_id, Some("ns_001".to_string()));
            }
            TakeoverResult::Success { .. } => {
                panic!("CAS should fail when marker was already finalized");
            }
        }
    }
}
