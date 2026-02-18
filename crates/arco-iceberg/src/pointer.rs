//! Iceberg table pointer for CAS-based state management.
//!
//! The pointer file tracks the current Iceberg metadata location for each table,
//! enabling atomic updates via conditional writes.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use arco_core::IcebergPaths;
use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{IcebergError, IcebergResult};
use crate::paths::{ICEBERG_POINTER_PREFIX, iceberg_pointer_path};
use crate::transactions::{TransactionStatus, TransactionStore};
use crate::types::ObjectVersion;

/// Tracks the current state of an Iceberg table.
///
/// The pointer is stored at `_catalog/iceberg_pointers/{table_uuid}.json`
/// and updated atomically using CAS (compare-and-swap) semantics.
///
/// # Storage Semantics
///
/// - Create: `WritePrecondition::DoesNotExist`
/// - Update: `WritePrecondition::MatchesVersion(expected_version)`
/// - Delete: `WritePrecondition::MatchesVersion(expected_version)`
///
/// # Example
///
/// ```rust
/// use arco_iceberg::pointer::IcebergTablePointer;
/// use uuid::Uuid;
///
/// let pointer = IcebergTablePointer::new(
///     Uuid::new_v4(),
///     "gs://bucket/warehouse/ns/table/metadata/00000-uuid.metadata.json".to_string(),
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcebergTablePointer {
    /// Schema version for forward compatibility.
    pub version: u32,

    /// The table this pointer belongs to.
    pub table_uuid: Uuid,

    /// Current Iceberg metadata file location.
    pub current_metadata_location: String,

    /// Current snapshot ID (denormalized for fast checks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,

    /// Snapshot refs (main, branches, tags).
    #[serde(default)]
    pub refs: HashMap<String, SnapshotRef>,

    /// Last sequence number assigned.
    pub last_sequence_number: i64,

    /// Previous metadata location (for history building and validation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_metadata_location: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,

    /// Source of the last update.
    #[serde(default)]
    pub updated_by: UpdateSource,

    /// Pending update from an in-progress multi-table transaction.
    ///
    /// When present, readers must check the transaction record status to determine
    /// visibility. See `PendingPointerUpdate` for details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<PendingPointerUpdate>,
}

impl IcebergTablePointer {
    /// Default pointer schema version for new pointers.
    ///
    /// - v1: Original schema without pending field
    /// - v2: Added `pending` field for multi-table transactions (ICE-7)
    ///
    /// New pointers are created with v1 for backward compatibility.
    /// Use `with_pending()` to upgrade to v2 when multi-table tx is needed.
    pub const CURRENT_VERSION: u32 = 1;

    /// Maximum version this binary can read.
    pub const MAX_READABLE_VERSION: u32 = 2;

    /// Minimum version that can be safely read.
    pub const MIN_SUPPORTED_VERSION: u32 = 1;

    /// Creates a new pointer with no pending transaction.
    #[must_use]
    pub fn new(table_uuid: Uuid, current_metadata_location: String) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            table_uuid,
            current_metadata_location,
            current_snapshot_id: None,
            refs: HashMap::new(),
            last_sequence_number: 0,
            previous_metadata_location: None,
            updated_at: Utc::now(),
            updated_by: UpdateSource::Unknown,
            pending: None,
        }
    }

    /// Validates the pointer version is within the supported range.
    ///
    /// # Errors
    ///
    /// Returns `PointerVersionError` if version is outside supported range.
    pub fn validate_version(&self) -> Result<(), PointerVersionError> {
        if self.version > Self::MAX_READABLE_VERSION {
            return Err(PointerVersionError::UnsupportedVersion {
                found: self.version,
                max_supported: Self::MAX_READABLE_VERSION,
            });
        }
        if self.version < Self::MIN_SUPPORTED_VERSION {
            return Err(PointerVersionError::UnsupportedVersion {
                found: self.version,
                max_supported: Self::MAX_READABLE_VERSION,
            });
        }
        Ok(())
    }

    /// Returns `true` if this pointer has a pending transaction update.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Applies pending update to current fields and clears pending.
    #[must_use]
    pub fn finalize_pending(self) -> Self {
        self.finalize_pending_with_source(UpdateSource::Unknown)
    }

    /// Applies pending update with explicit update source.
    #[must_use]
    pub fn finalize_pending_with_source(mut self, source: UpdateSource) -> Self {
        if let Some(pending) = self.pending.take() {
            self.previous_metadata_location = Some(self.current_metadata_location);
            self.current_metadata_location = pending.metadata_location;
            if let Some(snapshot_id) = pending.snapshot_id {
                self.current_snapshot_id = Some(snapshot_id);
            }
            self.last_sequence_number = pending.last_sequence_number;
            self.updated_at = Utc::now();
            self.updated_by = source;
        }
        self
    }

    /// Sets pending state and upgrades schema version to v2.
    #[must_use]
    pub fn with_pending(mut self, pending: PendingPointerUpdate) -> Self {
        self.version = 2;
        self.pending = Some(pending);
        self
    }

    /// Clears pending state.
    #[must_use]
    pub fn without_pending(mut self) -> Self {
        self.pending = None;
        self
    }

    /// Returns the path where this pointer should be stored.
    #[must_use]
    pub fn storage_path(table_uuid: &Uuid) -> String {
        iceberg_pointer_path(table_uuid)
    }
}

/// Reference to a snapshot (branch or tag).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef {
    /// The snapshot ID this ref points to.
    pub snapshot_id: i64,

    /// Type of reference.
    #[serde(rename = "type")]
    pub ref_type: SnapshotRefType,

    /// Maximum age of snapshots to retain for this ref (branches only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,

    /// Maximum age of snapshots to retain (branches only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,

    /// Minimum number of snapshots to retain (branches only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
}

/// Type of snapshot reference.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    /// A mutable branch (like git branch).
    Branch,
    /// An immutable tag (like git tag).
    Tag,
}

/// Pending pointer update from an in-progress multi-table transaction.
///
/// When a multi-table transaction prepares a table, it writes the new metadata
/// and CAS-updates the pointer to include this `pending` field. The transaction
/// record (not the pointer) is the source of truth for commit status.
///
/// # Visibility Rules
///
/// - If `pending` exists and the referenced transaction is `Committed` → treat pending as current
/// - If `pending` exists and the referenced transaction is `Preparing`/`Aborted`/missing → ignore pending
///
/// See ADR-029 for the full protocol specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingPointerUpdate {
    /// Transaction ID (matches `TransactionRecord.tx_id`).
    pub tx_id: String,

    /// New metadata location written during prepare.
    pub metadata_location: String,

    /// New snapshot ID (if a snapshot was added).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,

    /// New last sequence number.
    pub last_sequence_number: i64,

    /// When this pending update was prepared.
    pub prepared_at: DateTime<Utc>,
}

/// Resolves the effective metadata location for a pointer.
///
/// When a pointer has a pending update from a committed transaction, the pending
/// metadata location should be treated as the current location. This function
/// checks the transaction status to determine the effective location.
///
/// # Returns
///
/// - `Ok(metadata_location)` - The effective metadata location
/// - `Err(_)` - A transient error that should NOT fall back to current
///
/// # Critical Safety Note
///
/// Only a 404/not-found transaction record means "not committed". Transient
/// errors (timeout, 5xx) must propagate up - falling back to `current` on
/// transient errors could violate atomicity.
///
/// # Errors
///
/// Returns `IcebergError` if transaction store lookup fails with a transient error.
pub async fn resolve_effective_metadata_location<T: TransactionStore>(
    pointer: &IcebergTablePointer,
    tx_store: &T,
) -> IcebergResult<EffectivePointer> {
    let Some(pending) = &pointer.pending else {
        return Ok(EffectivePointer {
            metadata_location: pointer.current_metadata_location.clone(),
            snapshot_id: pointer.current_snapshot_id,
            last_sequence_number: pointer.last_sequence_number,
            is_pending: false,
        });
    };

    match tx_store.load(&pending.tx_id).await {
        Ok(Some((tx_record, _))) if tx_record.status == TransactionStatus::Committed => {
            Ok(EffectivePointer {
                metadata_location: pending.metadata_location.clone(),
                snapshot_id: pending.snapshot_id,
                last_sequence_number: pending.last_sequence_number,
                is_pending: true,
            })
        }
        Ok(Some(_) | None) => Ok(EffectivePointer {
            metadata_location: pointer.current_metadata_location.clone(),
            snapshot_id: pointer.current_snapshot_id,
            last_sequence_number: pointer.last_sequence_number,
            is_pending: false,
        }),
        Err(e) => {
            tracing::warn!(
                tx_id = %pending.tx_id,
                error = %e,
                "Transient error checking transaction status - NOT falling back"
            );
            Err(e)
        }
    }
}

/// The resolved effective state of a pointer.
#[derive(Debug, Clone)]
pub struct EffectivePointer {
    /// The effective metadata location to use.
    pub metadata_location: String,
    /// The effective current snapshot ID.
    pub snapshot_id: Option<i64>,
    /// The effective last sequence number.
    pub last_sequence_number: i64,
    /// Whether this came from a pending (committed but not finalized) update.
    pub is_pending: bool,
}

/// Source of the last update to the pointer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UpdateSource {
    /// Update from Arco Flow orchestration.
    #[serde(rename = "arco-flow")]
    ArcoFlow {
        /// The run ID.
        run_id: ulid::Ulid,
        /// The task ID.
        task_id: ulid::Ulid,
    },
    /// Update from Iceberg REST API.
    IcebergRest {
        /// Client user-agent string.
        #[serde(skip_serializing_if = "Option::is_none")]
        client_info: Option<String>,
        /// Authenticated principal.
        #[serde(skip_serializing_if = "Option::is_none")]
        principal: Option<String>,
    },
    /// Update from admin API.
    AdminApi {
        /// Admin user ID.
        user_id: String,
    },
    /// Unknown source (for deserialization compatibility).
    #[serde(other)]
    #[default]
    Unknown,
}

/// Error when validating pointer version.
#[derive(Debug, thiserror::Error)]
pub enum PointerVersionError {
    /// Pointer version is newer than supported.
    #[error("Unsupported pointer version {found}, max supported is {max_supported}")]
    UnsupportedVersion {
        /// Version found in the pointer.
        found: u32,
        /// Maximum supported version.
        max_supported: u32,
    },
}

/// Result of a CAS operation on a pointer.
#[derive(Debug, Clone)]
pub enum CasResult {
    /// CAS succeeded.
    Success {
        /// New version after the write.
        new_version: ObjectVersion,
    },
    /// CAS failed due to version mismatch.
    Conflict {
        /// Current version that caused the conflict.
        current_version: ObjectVersion,
    },
}

/// Trait for pointer storage operations.
#[async_trait]
pub trait PointerStore: Send + Sync {
    /// Loads a pointer and its version.
    async fn load(
        &self,
        table_uuid: &Uuid,
    ) -> IcebergResult<Option<(IcebergTablePointer, ObjectVersion)>>;

    /// Creates a new pointer (fails if already exists).
    async fn create(
        &self,
        table_uuid: &Uuid,
        pointer: &IcebergTablePointer,
    ) -> IcebergResult<CasResult>;

    /// Updates a pointer with CAS.
    async fn compare_and_swap(
        &self,
        table_uuid: &Uuid,
        expected_version: &ObjectVersion,
        new_pointer: &IcebergTablePointer,
    ) -> IcebergResult<CasResult>;

    /// Deletes a pointer with version check.
    async fn delete(
        &self,
        table_uuid: &Uuid,
        expected_version: &ObjectVersion,
    ) -> IcebergResult<CasResult>;

    /// Lists all table UUIDs that have pointers.
    ///
    /// This is used by the reconciler to enumerate all tables for reconciliation.
    async fn list_all(&self) -> IcebergResult<Vec<Uuid>>;
}

/// Implementation of `PointerStore` using `StorageBackend`.
pub struct PointerStoreImpl<S> {
    storage: Arc<S>,
}

impl<S: StorageBackend> PointerStoreImpl<S> {
    /// Creates a new pointer store.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: StorageBackend> PointerStore for PointerStoreImpl<S> {
    async fn load(
        &self,
        table_uuid: &Uuid,
    ) -> IcebergResult<Option<(IcebergTablePointer, ObjectVersion)>> {
        let path = IcebergTablePointer::storage_path(table_uuid);

        let meta = self
            .storage
            .head(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to check pointer existence: {e}"),
            })?;

        let Some(meta) = meta else {
            return Ok(None);
        };

        let bytes = self
            .storage
            .get(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to read pointer: {e}"),
            })?;

        let pointer: IcebergTablePointer =
            serde_json::from_slice(&bytes).map_err(|e| IcebergError::Internal {
                message: format!("Failed to parse pointer: {e}"),
            })?;

        Ok(Some((pointer, ObjectVersion::new(meta.version))))
    }

    async fn create(
        &self,
        table_uuid: &Uuid,
        pointer: &IcebergTablePointer,
    ) -> IcebergResult<CasResult> {
        let path = IcebergTablePointer::storage_path(table_uuid);
        let bytes = serde_json::to_vec(pointer).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize pointer: {e}"),
        })?;

        match self
            .storage
            .put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(CasResult::Success {
                new_version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => Ok(CasResult::Conflict {
                current_version: ObjectVersion::new(current_version),
            }),
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to create pointer: {e}"),
            }),
        }
    }

    async fn compare_and_swap(
        &self,
        table_uuid: &Uuid,
        expected_version: &ObjectVersion,
        new_pointer: &IcebergTablePointer,
    ) -> IcebergResult<CasResult> {
        let path = IcebergTablePointer::storage_path(table_uuid);
        let bytes = serde_json::to_vec(new_pointer).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize pointer: {e}"),
        })?;

        let precondition = WritePrecondition::MatchesVersion(expected_version.as_str().to_string());

        match self
            .storage
            .put(&path, Bytes::from(bytes), precondition)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(CasResult::Success {
                new_version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => Ok(CasResult::Conflict {
                current_version: ObjectVersion::new(current_version),
            }),
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to CAS pointer: {e}"),
            }),
        }
    }

    async fn delete(
        &self,
        table_uuid: &Uuid,
        expected_version: &ObjectVersion,
    ) -> IcebergResult<CasResult> {
        // Note: Most object stores don't support conditional delete.
        // For now, we read-check-delete which has a race window.
        // A proper implementation would use versioned delete if available.
        let path = IcebergTablePointer::storage_path(table_uuid);

        let meta = self
            .storage
            .head(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to check pointer: {e}"),
            })?;

        let Some(meta) = meta else {
            return Ok(CasResult::Conflict {
                current_version: ObjectVersion::new("0"),
            });
        };

        if meta.version != expected_version.as_str() {
            return Ok(CasResult::Conflict {
                current_version: ObjectVersion::new(meta.version),
            });
        }

        self.storage
            .delete(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to delete pointer: {e}"),
            })?;

        Ok(CasResult::Success {
            new_version: ObjectVersion::new("0"),
        })
    }

    async fn list_all(&self) -> IcebergResult<Vec<Uuid>> {
        let prefix = format!("{ICEBERG_POINTER_PREFIX}/");

        let entries = self
            .storage
            .list(&prefix)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to list pointers: {e}"),
            })?;

        let mut uuids = Vec::new();
        for entry in entries {
            if let Some(uuid) = IcebergPaths::pointer_uuid_from_path(&entry.path) {
                uuids.push(uuid);
            }
        }

        Ok(uuids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokio_test::block_on;

    #[test]
    fn test_pointer_serialization_roundtrip() {
        let pointer = IcebergTablePointer {
            version: 2,
            table_uuid: Uuid::new_v4(),
            current_metadata_location: "gs://bucket/path/metadata.json".to_string(),
            current_snapshot_id: Some(123),
            refs: HashMap::from([(
                "main".to_string(),
                SnapshotRef {
                    snapshot_id: 123,
                    ref_type: SnapshotRefType::Branch,
                    max_ref_age_ms: None,
                    max_snapshot_age_ms: None,
                    min_snapshots_to_keep: None,
                },
            )]),
            last_sequence_number: 1,
            previous_metadata_location: Some("gs://bucket/path/old.json".to_string()),
            updated_at: Utc::now(),
            updated_by: UpdateSource::IcebergRest {
                client_info: Some("spark/3.5".to_string()),
                principal: Some("user@example.com".to_string()),
            },
            pending: None,
        };

        let json = serde_json::to_string_pretty(&pointer).expect("serialization failed");
        let parsed: IcebergTablePointer =
            serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(pointer, parsed);
    }

    #[test]
    fn test_pointer_v1_backward_compatible() {
        let v1_json = r#"{
            "version": 1,
            "table_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "current_metadata_location": "gs://bucket/path.json",
            "last_sequence_number": 1,
            "updated_at": "2024-01-01T00:00:00Z",
            "updated_by": {"type": "unknown"}
        }"#;
        let parsed: IcebergTablePointer =
            serde_json::from_str(v1_json).expect("v1 deserialization");

        assert_eq!(parsed.version, 1);
        assert!(parsed.pending.is_none());
        assert!(parsed.validate_version().is_ok());
    }

    #[test]
    fn test_pointer_with_pending() {
        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "base.json".to_string());
        pointer.pending = Some(PendingPointerUpdate {
            tx_id: "tx-123".to_string(),
            metadata_location: "pending.json".to_string(),
            snapshot_id: Some(999),
            last_sequence_number: 5,
            prepared_at: Utc::now(),
        });

        assert!(pointer.has_pending());

        let json = serde_json::to_string(&pointer).expect("serialize");
        let parsed: IcebergTablePointer = serde_json::from_str(&json).expect("deserialize");

        assert!(parsed.has_pending());
        let pending = parsed.pending.expect("pending exists");
        assert_eq!(pending.tx_id, "tx-123");
        assert_eq!(pending.metadata_location, "pending.json");
    }

    #[test]
    fn test_finalize_pending() {
        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "base.json".to_string());
        pointer.last_sequence_number = 1;
        pointer.pending = Some(PendingPointerUpdate {
            tx_id: "tx-123".to_string(),
            metadata_location: "new.json".to_string(),
            snapshot_id: Some(999),
            last_sequence_number: 5,
            prepared_at: Utc::now(),
        });

        let finalized = pointer.finalize_pending();

        assert!(!finalized.has_pending());
        assert_eq!(finalized.current_metadata_location, "new.json");
        assert_eq!(
            finalized.previous_metadata_location,
            Some("base.json".to_string())
        );
        assert_eq!(finalized.current_snapshot_id, Some(999));
        assert_eq!(finalized.last_sequence_number, 5);
    }

    #[test]
    fn test_pointer_version_validation() {
        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "test".to_string());

        // Current version should be valid
        assert!(pointer.validate_version().is_ok());

        // Future version should fail
        pointer.version = 999;
        assert!(pointer.validate_version().is_err());
    }

    #[test]
    fn test_storage_path() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid");
        let path = IcebergTablePointer::storage_path(&uuid);
        let expected = format!("{ICEBERG_POINTER_PREFIX}/{uuid}.json");
        assert_eq!(path, expected);
    }

    #[test]
    fn test_update_source_variants() {
        // Test each variant serializes correctly
        let flow = UpdateSource::ArcoFlow {
            run_id: ulid::Ulid::new(),
            task_id: ulid::Ulid::new(),
        };
        let json = serde_json::to_string(&flow).expect("serialization");
        assert!(json.contains("\"type\":\"arco-flow\""));

        let rest = UpdateSource::IcebergRest {
            client_info: Some("test".to_string()),
            principal: None,
        };
        let json = serde_json::to_string(&rest).expect("serialization");
        assert!(json.contains("\"type\":\"iceberg_rest\""));
    }

    #[test]
    fn test_update_source_unknown_fallback() {
        let json = r#"{"type":"new_source","foo":"bar"}"#;
        let parsed: UpdateSource = serde_json::from_str(json).expect("deserialize");
        assert!(matches!(parsed, UpdateSource::Unknown));
    }

    #[tokio::test]
    async fn test_pointer_store_create() {
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let store = PointerStoreImpl::new(storage);

        let table_uuid = Uuid::new_v4();
        let pointer =
            IcebergTablePointer::new(table_uuid, "gs://bucket/metadata/00000.json".to_string());

        let result = store.create(&table_uuid, &pointer).await.expect("create");
        assert!(matches!(result, CasResult::Success { .. }));
    }

    #[tokio::test]
    async fn test_pointer_store_cas_conflict() {
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let store = PointerStoreImpl::new(storage);

        let table_uuid = Uuid::new_v4();
        let pointer =
            IcebergTablePointer::new(table_uuid, "gs://bucket/metadata/00000.json".to_string());

        // Create initial pointer
        let result = store.create(&table_uuid, &pointer).await.expect("create");
        let CasResult::Success { new_version } = result else {
            panic!("expected success");
        };

        // Update with correct version succeeds
        let mut updated = pointer.clone();
        updated.current_metadata_location = "gs://bucket/metadata/00001.json".to_string();
        let result = store
            .compare_and_swap(&table_uuid, &new_version, &updated)
            .await
            .expect("cas");
        assert!(matches!(result, CasResult::Success { .. }));

        // Update with stale version fails
        let result = store
            .compare_and_swap(&table_uuid, &new_version, &updated)
            .await
            .expect("cas");
        assert!(matches!(result, CasResult::Conflict { .. }));
    }

    #[tokio::test]
    async fn test_pointer_store_cas_race_has_single_winner() {
        use arco_core::storage::MemoryBackend;
        use tokio::sync::Barrier;

        let storage = Arc::new(MemoryBackend::new());
        let store = Arc::new(PointerStoreImpl::new(storage));

        let table_uuid = Uuid::new_v4();
        let pointer =
            IcebergTablePointer::new(table_uuid, "gs://bucket/metadata/00000.json".to_string());

        let created = store.create(&table_uuid, &pointer).await.expect("create");
        let CasResult::Success { new_version } = created else {
            panic!("expected initial create success");
        };

        let mut update_a = pointer.clone();
        update_a.current_metadata_location = "gs://bucket/metadata/00001-a.json".to_string();

        let mut update_b = pointer.clone();
        update_b.current_metadata_location = "gs://bucket/metadata/00001-b.json".to_string();

        let barrier = Arc::new(Barrier::new(3));

        let store_a = Arc::clone(&store);
        let barrier_a = Arc::clone(&barrier);
        let table_uuid_a = table_uuid;
        let version_a = new_version.clone();
        let handle_a = tokio::spawn(async move {
            barrier_a.wait().await;
            store_a
                .compare_and_swap(&table_uuid_a, &version_a, &update_a)
                .await
        });

        let store_b = Arc::clone(&store);
        let barrier_b = Arc::clone(&barrier);
        let table_uuid_b = table_uuid;
        let version_b = new_version.clone();
        let handle_b = tokio::spawn(async move {
            barrier_b.wait().await;
            store_b
                .compare_and_swap(&table_uuid_b, &version_b, &update_b)
                .await
        });

        barrier.wait().await;

        let result_a = handle_a.await.expect("join a").expect("cas a");
        let result_b = handle_b.await.expect("join b").expect("cas b");

        let outcomes = [result_a, result_b];
        let success_count = outcomes
            .iter()
            .filter(|result| matches!(result, CasResult::Success { .. }))
            .count();
        let conflict_count = outcomes
            .iter()
            .filter(|result| matches!(result, CasResult::Conflict { .. }))
            .count();

        assert_eq!(success_count, 1, "exactly one CAS winner is required");
        assert_eq!(conflict_count, 1, "losing CAS attempt must conflict");
    }

    #[tokio::test]
    async fn test_pointer_store_list_all() {
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let store = PointerStoreImpl::new(storage);

        // Initially empty
        let uuids = store.list_all().await.expect("list_all");
        assert!(uuids.is_empty());

        // Create three pointers
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        let pointer1 =
            IcebergTablePointer::new(uuid1, "gs://bucket/table1/metadata.json".to_string());
        let pointer2 =
            IcebergTablePointer::new(uuid2, "gs://bucket/table2/metadata.json".to_string());
        let pointer3 =
            IcebergTablePointer::new(uuid3, "gs://bucket/table3/metadata.json".to_string());

        store.create(&uuid1, &pointer1).await.expect("create1");
        store.create(&uuid2, &pointer2).await.expect("create2");
        store.create(&uuid3, &pointer3).await.expect("create3");

        // List should return all three
        let mut uuids = store.list_all().await.expect("list_all");
        uuids.sort();

        let mut expected = vec![uuid1, uuid2, uuid3];
        expected.sort();

        assert_eq!(uuids, expected);
    }

    #[tokio::test]
    async fn test_effective_pointer_no_pending() {
        use crate::transactions::TransactionStoreImpl;
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let tx_store = TransactionStoreImpl::new(storage);

        let pointer = IcebergTablePointer::new(Uuid::new_v4(), "current.json".to_string());

        let effective = resolve_effective_metadata_location(&pointer, &tx_store)
            .await
            .expect("resolve");

        assert_eq!(effective.metadata_location, "current.json");
        assert!(!effective.is_pending);
    }

    #[tokio::test]
    async fn test_effective_pointer_pending_committed() {
        use crate::transactions::{TransactionRecord, TransactionStoreImpl};
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let tx_id = "01941234-5678-7def-8abc-123456789abc";
        let record =
            TransactionRecord::new_preparing(tx_id.to_string(), "hash".to_string()).commit();
        tx_store.create(&record).await.expect("create tx");

        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "current.json".to_string());
        pointer.pending = Some(PendingPointerUpdate {
            tx_id: tx_id.to_string(),
            metadata_location: "pending.json".to_string(),
            snapshot_id: Some(999),
            last_sequence_number: 5,
            prepared_at: Utc::now(),
        });

        let effective = resolve_effective_metadata_location(&pointer, &tx_store)
            .await
            .expect("resolve");

        assert_eq!(effective.metadata_location, "pending.json");
        assert_eq!(effective.snapshot_id, Some(999));
        assert_eq!(effective.last_sequence_number, 5);
        assert!(effective.is_pending);
    }

    #[tokio::test]
    async fn test_effective_pointer_pending_not_committed() {
        use crate::transactions::{TransactionRecord, TransactionStoreImpl};
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let tx_id = "01941234-5678-7def-8abc-123456789abc";
        let record = TransactionRecord::new_preparing(tx_id.to_string(), "hash".to_string());
        tx_store.create(&record).await.expect("create tx");

        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "current.json".to_string());
        pointer.current_snapshot_id = Some(100);
        pointer.last_sequence_number = 1;
        pointer.pending = Some(PendingPointerUpdate {
            tx_id: tx_id.to_string(),
            metadata_location: "pending.json".to_string(),
            snapshot_id: Some(999),
            last_sequence_number: 5,
            prepared_at: Utc::now(),
        });

        let effective = resolve_effective_metadata_location(&pointer, &tx_store)
            .await
            .expect("resolve");

        assert_eq!(effective.metadata_location, "current.json");
        assert_eq!(effective.snapshot_id, Some(100));
        assert_eq!(effective.last_sequence_number, 1);
        assert!(!effective.is_pending);
    }

    #[tokio::test]
    async fn test_effective_pointer_pending_tx_missing() {
        use crate::transactions::TransactionStoreImpl;
        use arco_core::storage::MemoryBackend;
        use std::sync::Arc;

        let storage = Arc::new(MemoryBackend::new());
        let tx_store = TransactionStoreImpl::new(storage);

        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "current.json".to_string());
        pointer.pending = Some(PendingPointerUpdate {
            tx_id: "nonexistent-tx".to_string(),
            metadata_location: "pending.json".to_string(),
            snapshot_id: Some(999),
            last_sequence_number: 5,
            prepared_at: Utc::now(),
        });

        let effective = resolve_effective_metadata_location(&pointer, &tx_store)
            .await
            .expect("resolve");

        assert_eq!(effective.metadata_location, "current.json");
        assert!(!effective.is_pending);
    }

    proptest! {
        #[test]
        fn prop_pointer_store_cas_roundtrip(
            uuid_bytes in proptest::array::uniform16(any::<u8>()),
            location in "[a-z0-9/_\\.-]{1,32}",
            suffix in "[a-z0-9]{1,8}"
        ) {
            use arco_core::storage::MemoryBackend;
            use std::sync::Arc;

            let storage = Arc::new(MemoryBackend::new());
            let store = PointerStoreImpl::new(Arc::clone(&storage));

            let table_uuid = Uuid::from_bytes(uuid_bytes);
            let initial_location = format!("gs://bucket/{location}");
            let pointer = IcebergTablePointer::new(table_uuid, initial_location.clone());

            let result = block_on(store.create(&table_uuid, &pointer)).expect("create");
            let CasResult::Success { new_version } = result else {
                panic!("expected success");
            };

            let mut updated = pointer.clone();
            updated.current_metadata_location = format!("gs://bucket/{location}/{suffix}");
            updated.previous_metadata_location = Some(initial_location);
            updated.last_sequence_number = pointer.last_sequence_number + 1;
            updated.updated_at = Utc::now();

            let result = block_on(store.compare_and_swap(&table_uuid, &new_version, &updated))
                .expect("cas");
            prop_assert!(
                matches!(result, CasResult::Success { .. }),
                "cas result: {:?}",
                result
            );

            let stale_version = ObjectVersion::new(format!("{}-stale", new_version.as_str()));
            let result = block_on(store.compare_and_swap(&table_uuid, &stale_version, &updated))
                .expect("cas");
            prop_assert!(
                matches!(result, CasResult::Conflict { .. }),
                "cas result: {:?}",
                result
            );

            let loaded = block_on(store.load(&table_uuid))
                .expect("load")
                .expect("pointer");
            prop_assert_eq!(loaded.0.current_metadata_location, updated.current_metadata_location);
        }
    }
}
