//! Read-only pointer store for Iceberg table metadata lookups.
//!
//! Batch 5 read handlers need fast pointer read helpers with clear not-found
//! semantics. This module provides `get` and `exists` operations over the
//! underlying object store path used by `IcebergTablePointer`.

use std::sync::Arc;

use arco_core::error::Error as CoreError;
use arco_core::storage::StorageBackend;
use uuid::Uuid;

use crate::error::{IcebergError, IcebergResult};
use crate::pointer::IcebergTablePointer;
use crate::types::ObjectVersion;

/// Pointer store specialized for table read operations.
pub struct IcebergPointerStore<S: StorageBackend + ?Sized> {
    storage: Arc<S>,
}

impl<S: StorageBackend + ?Sized> IcebergPointerStore<S> {
    /// Creates a new pointer store.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Loads a table pointer and its object version.
    ///
    /// # Errors
    ///
    /// Returns `IcebergError::NotFound` when the pointer does not exist.
    /// Returns `IcebergError::Internal` for storage/serialization failures.
    pub async fn get(
        &self,
        table_uuid: &Uuid,
    ) -> IcebergResult<(IcebergTablePointer, ObjectVersion)> {
        let path = IcebergTablePointer::storage_path(table_uuid);
        let meta = self
            .storage
            .head(&path)
            .await
            .map_err(|err| IcebergError::Internal {
                message: format!("Failed to head pointer for table {table_uuid}: {err}"),
            })?
            .ok_or_else(|| Self::table_not_found(table_uuid))?;

        let pointer_bytes = self
            .storage
            .get(&path)
            .await
            .map_err(|err| Self::map_get_error(table_uuid, err))?;

        let pointer: IcebergTablePointer =
            serde_json::from_slice(&pointer_bytes).map_err(|err| IcebergError::Internal {
                message: format!("Failed to decode pointer for table {table_uuid}: {err}"),
            })?;
        pointer
            .validate_version()
            .map_err(|err| IcebergError::Internal {
                message: format!("Invalid pointer version for table {table_uuid}: {err}"),
            })?;

        Ok((pointer, ObjectVersion::new(meta.version)))
    }

    /// Returns `true` when the table pointer object exists.
    ///
    /// # Errors
    ///
    /// Returns `IcebergError::Internal` when storage metadata lookup fails.
    pub async fn exists(&self, table_uuid: &Uuid) -> IcebergResult<bool> {
        let path = IcebergTablePointer::storage_path(table_uuid);
        self.storage
            .head(&path)
            .await
            .map(|meta| meta.is_some())
            .map_err(|err| IcebergError::Internal {
                message: format!("Failed to check pointer existence for table {table_uuid}: {err}"),
            })
    }

    fn map_get_error(table_uuid: &Uuid, err: CoreError) -> IcebergError {
        match err {
            CoreError::NotFound(_) | CoreError::ResourceNotFound { .. } => {
                Self::table_not_found(table_uuid)
            }
            other => IcebergError::Internal {
                message: format!("Failed to read pointer for table {table_uuid}: {other}"),
            },
        }
    }

    fn table_not_found(table_uuid: &Uuid) -> IcebergError {
        IcebergError::NotFound {
            message: format!("Table pointer does not exist: {table_uuid}"),
            error_type: "NoSuchTableException",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
    use bytes::Bytes;
    use uuid::Uuid;

    use super::IcebergPointerStore;
    use crate::error::IcebergError;
    use crate::pointer::IcebergTablePointer;

    async fn put_pointer(
        storage: &Arc<MemoryBackend>,
        table_uuid: &Uuid,
        pointer: &IcebergTablePointer,
    ) {
        let path = IcebergTablePointer::storage_path(table_uuid);
        let payload = serde_json::to_vec(pointer).expect("serialize pointer");
        storage
            .put(&path, Bytes::from(payload), WritePrecondition::None)
            .await
            .expect("put pointer");
    }

    #[tokio::test]
    async fn test_get_existing_pointer_returns_pointer_and_version() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IcebergPointerStore::new(Arc::clone(&storage));
        let table_uuid = Uuid::new_v4();
        let pointer = IcebergTablePointer::new(table_uuid, "gs://bucket/meta/00000.json".into());

        put_pointer(&storage, &table_uuid, &pointer).await;

        let (loaded, version) = store.get(&table_uuid).await.expect("pointer exists");
        assert_eq!(loaded.table_uuid, table_uuid);
        assert_eq!(
            loaded.current_metadata_location,
            pointer.current_metadata_location
        );
        assert!(
            !version.as_str().is_empty(),
            "object version token must be populated"
        );
    }

    #[tokio::test]
    async fn test_get_missing_pointer_returns_not_found() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IcebergPointerStore::new(storage);
        let missing = Uuid::new_v4();

        let err = store.get(&missing).await.expect_err("pointer missing");
        match err {
            IcebergError::NotFound {
                error_type,
                message,
            } => {
                assert_eq!(error_type, "NoSuchTableException");
                assert!(message.contains(&missing.to_string()));
            }
            other => panic!("expected not found, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_exists_true_and_false() {
        let storage = Arc::new(MemoryBackend::new());
        let store = IcebergPointerStore::new(Arc::clone(&storage));
        let present = Uuid::new_v4();
        let absent = Uuid::new_v4();
        let pointer = IcebergTablePointer::new(present, "gs://bucket/meta/00000.json".into());

        put_pointer(&storage, &present, &pointer).await;

        assert!(store.exists(&present).await.expect("exists"));
        assert!(!store.exists(&absent).await.expect("missing"));
    }

    #[tokio::test]
    async fn test_store_supports_dyn_storage_backend() {
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let store = IcebergPointerStore::new(Arc::clone(&storage));
        let missing = Uuid::new_v4();

        assert!(!store.exists(&missing).await.expect("missing"));
    }
}
