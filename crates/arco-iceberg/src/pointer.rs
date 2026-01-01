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

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{IcebergError, IcebergResult};
use crate::paths::{ICEBERG_POINTER_PREFIX, iceberg_pointer_path};
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
}

impl IcebergTablePointer {
    /// Current pointer schema version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Creates a new pointer for an Iceberg table.
    ///
    /// The pointer starts with sequence number 0 and no snapshot.
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
        }
    }

    /// Validates the pointer version is supported.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported.
    pub fn validate_version(&self) -> Result<(), PointerVersionError> {
        if self.version > Self::CURRENT_VERSION {
            return Err(PointerVersionError::UnsupportedVersion {
                found: self.version,
                max_supported: Self::CURRENT_VERSION,
            });
        }
        Ok(())
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
            // Extract UUID from path like "_catalog/iceberg_pointers/{uuid}.json"
            if let Some(filename) = entry.path.strip_prefix(&prefix) {
                if let Some(uuid_str) = filename.strip_suffix(".json") {
                    if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                        uuids.push(uuid);
                    }
                }
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
            version: 1,
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
        };

        let json = serde_json::to_string_pretty(&pointer).expect("serialization failed");
        let parsed: IcebergTablePointer =
            serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(pointer, parsed);
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
