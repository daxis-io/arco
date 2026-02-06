//! Split storage traits for capability-based access control (Gate 5).
//!
//! This module provides fine-grained storage traits that encode access permissions
//! at the type level. Components receive only the capabilities they need.
//!
//! # Design Philosophy
//!
//! - **Capability-based security**: Components get only what they need
//! - **Compile-time enforcement**: Wrong operations are type errors
//! - **Defense in depth**: Traits encode permissions, IAM enforces at runtime
//!
//! # Trait Hierarchy
//!
//! | Trait | Operations | Who Gets It |
//! |-------|------------|-------------|
//! | [`ReadStore`] | get, `get_range` | API, Compactor |
//! | [`LedgerPutStore`] | put to ledger/ | API only |
//! | [`StatePutStore`] | put to state/, snapshots/ | Compactor only |
//! | [`CasStore`] | CAS to manifests/ | Compactor only |
//! | [`ListStore`] | list | Anti-entropy only |
//! | [`MetaStore`] | head | Artifact verification |
//!
//! # Example
//!
//! ```rust,ignore
//! use arco_core::storage_keys::LedgerKey;
//! use arco_core::storage_traits::{ReadStore, LedgerPutStore};
//!
//! // API handler gets ReadStore + LedgerPutStore
//! async fn api_handler<R: ReadStore, W: LedgerPutStore>(
//!     reader: &R,
//!     writer: &W,
//!     key: LedgerKey,
//! ) {
//!     // Can read anything
//!     let data = reader.get(key.as_ref()).await?;
//!
//!     // Can write to ledger/
//!     writer.put_ledger(&key, data).await?;
//!
//!     // CANNOT write to state/ - no StatePutStore!
//!     // writer.put_state(...) // <- This won't compile
//! }
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;

use crate::error::Result;
use crate::storage::{ObjectMeta, WritePrecondition, WriteResult};
use crate::storage_keys::{CommitKey, LedgerKey, LockKey, ManifestKey, StateKey};

// ============================================================================
// ReadStore - Read-only access (API + Compactor)
// ============================================================================

/// Read-only storage access.
///
/// # Access
///
/// - **Who**: API, Compactor
/// - **Operations**: `get`, `get_range`
///
/// This is the most basic storage capability. All components that need to
/// read data get this trait.
#[async_trait]
pub trait ReadStore: Send + Sync + 'static {
    /// Reads entire object.
    ///
    /// Returns `Error::NotFound` if object doesn't exist.
    async fn get(&self, path: &str) -> Result<Bytes>;

    /// Reads a byte range from an object.
    ///
    /// Returns `Error::InvalidInput` if range is invalid.
    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;
}

// ============================================================================
// LedgerPutStore - Append to ledger/ (API only)
// ============================================================================

/// Write access to the ledger (append-only events).
///
/// # Access
///
/// - **Who**: API, Compactor
/// - **Operations**: put with `DoesNotExist` precondition
/// - **Prefix**: `ledger/`
///
/// # Invariant
///
/// Ledger writes MUST use `DoesNotExist` precondition for true append-only
/// semantics. This trait enforces that constraint.
#[async_trait]
pub trait LedgerPutStore: Send + Sync + 'static {
    /// Appends an event to the ledger.
    ///
    /// Uses `DoesNotExist` precondition for append-only semantics.
    /// Returns `Ok(WriteResult::PreconditionFailed)` if event already exists
    /// (duplicate delivery - this is not an error).
    async fn put_ledger(&self, key: &LedgerKey, data: Bytes) -> Result<WriteResult>;
}

// ============================================================================
// StatePutStore - Write to state/, snapshots/ (Compactor only)
// ============================================================================

/// Write access to compacted state (Parquet files).
///
/// # Access
///
/// - **Who**: Compactor only
/// - **Operations**: put with `DoesNotExist` precondition
/// - **Prefixes**: `state/`, `snapshots/`
///
/// # Invariant
///
/// State files are immutable once written. Uses `DoesNotExist` precondition.
/// A collision means another compactor already wrote the same snapshot.
#[async_trait]
pub trait StatePutStore: Send + Sync + 'static {
    /// Writes a state/snapshot file.
    ///
    /// Uses `DoesNotExist` precondition for immutability.
    /// Returns `Ok(WriteResult::PreconditionFailed)` if file already exists.
    async fn put_state(&self, key: &StateKey, data: Bytes) -> Result<WriteResult>;
}

// ============================================================================
// CasStore - CAS manifests/ (Compactor only)
// ============================================================================

/// Compare-and-swap access to manifests.
///
/// # Access
///
/// - **Who**: Compactor only
/// - **Operations**: put with `MatchesVersion` precondition
/// - **Prefix**: `manifests/`
///
/// # Invariant
///
/// Manifest updates MUST use CAS for atomic publish. This trait enforces that
/// by requiring a version token for all writes.
#[async_trait]
pub trait CasStore: Send + Sync + 'static {
    /// CAS-updates a manifest file.
    ///
    /// Returns `Ok(WriteResult::PreconditionFailed)` if version doesn't match.
    async fn cas(&self, key: &ManifestKey, data: Bytes, version: &str) -> Result<WriteResult>;

    /// Creates a manifest if it doesn't exist.
    ///
    /// Returns `Ok(WriteResult::PreconditionFailed)` if manifest already exists.
    async fn create_if_absent(&self, key: &ManifestKey, data: Bytes) -> Result<WriteResult>;
}

// ============================================================================
// LockPutStore - Write to locks/ (API only)
// ============================================================================

/// Write access to distributed locks.
///
/// # Access
///
/// - **Who**: API only
/// - **Operations**: put with various preconditions
/// - **Prefix**: `locks/`
#[async_trait]
pub trait LockPutStore: Send + Sync + 'static {
    /// Writes a lock file with the given precondition.
    async fn put_lock(
        &self,
        key: &LockKey,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult>;
}

// ============================================================================
// CommitPutStore - Write to commits/ (API + Compactor)
// ============================================================================

/// Write access to the audit trail (commit records).
///
/// # Access
///
/// - **Who**: API only
/// - **Operations**: put with `DoesNotExist` precondition
/// - **Prefix**: `commits/`
#[async_trait]
pub trait CommitPutStore: Send + Sync + 'static {
    /// Appends a commit record.
    ///
    /// Uses `DoesNotExist` precondition for append-only semantics.
    async fn put_commit(&self, key: &CommitKey, data: Bytes) -> Result<WriteResult>;
}

// ============================================================================
// ListStore - List operations (anti-entropy only)
// ============================================================================

/// List access for directory enumeration.
///
/// # Access
///
/// - **Who**: Anti-entropy job only (not fast-path compaction)
/// - **Operations**: list
///
/// # Important
///
/// Listing is expensive and should NOT be on the critical path.
/// Fast-path compaction uses explicit event paths from notifications.
#[async_trait]
pub trait ListStore: Send + Sync + 'static {
    /// Lists objects with the given prefix.
    ///
    /// Results are returned in arbitrary order.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;
}

// ============================================================================
// MetaStore - Head operations (artifact verification)
// ============================================================================

/// Metadata access for object existence and version checks.
///
/// # Access
///
/// - **Who**: API (for URL allowlist), Compactor (for artifact verification)
/// - **Operations**: head
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Gets object metadata without reading content.
    ///
    /// Returns `None` if object doesn't exist.
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>>;
}

// ============================================================================
// SignedUrlStore - Generate signed URLs (API only)
// ============================================================================

/// Signed URL generation for direct client access.
///
/// # Access
///
/// - **Who**: API only
/// - **Operations**: `signed_url`
#[async_trait]
pub trait SignedUrlStore: Send + Sync + 'static {
    /// Generates a signed URL for direct access.
    async fn signed_url(&self, path: &str, expiry: std::time::Duration) -> Result<String>;
}

// ============================================================================
// Blanket Implementations from StorageBackend
// ============================================================================

use crate::storage::StorageBackend;

#[async_trait]
impl<S: StorageBackend> ReadStore for S {
    async fn get(&self, path: &str) -> Result<Bytes> {
        StorageBackend::get(self, path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        StorageBackend::get_range(self, path, range).await
    }
}

#[async_trait]
impl<S: StorageBackend> LedgerPutStore for S {
    async fn put_ledger(&self, key: &LedgerKey, data: Bytes) -> Result<WriteResult> {
        StorageBackend::put(self, key.as_ref(), data, WritePrecondition::DoesNotExist).await
    }
}

#[async_trait]
impl<S: StorageBackend> StatePutStore for S {
    async fn put_state(&self, key: &StateKey, data: Bytes) -> Result<WriteResult> {
        StorageBackend::put(self, key.as_ref(), data, WritePrecondition::DoesNotExist).await
    }
}

#[async_trait]
impl<S: StorageBackend> CasStore for S {
    async fn cas(&self, key: &ManifestKey, data: Bytes, version: &str) -> Result<WriteResult> {
        StorageBackend::put(
            self,
            key.as_ref(),
            data,
            WritePrecondition::MatchesVersion(version.to_string()),
        )
        .await
    }

    async fn create_if_absent(&self, key: &ManifestKey, data: Bytes) -> Result<WriteResult> {
        StorageBackend::put(self, key.as_ref(), data, WritePrecondition::DoesNotExist).await
    }
}

#[async_trait]
impl<S: StorageBackend> LockPutStore for S {
    async fn put_lock(
        &self,
        key: &LockKey,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        StorageBackend::put(self, key.as_ref(), data, precondition).await
    }
}

#[async_trait]
impl<S: StorageBackend> CommitPutStore for S {
    async fn put_commit(&self, key: &CommitKey, data: Bytes) -> Result<WriteResult> {
        StorageBackend::put(self, key.as_ref(), data, WritePrecondition::DoesNotExist).await
    }
}

#[async_trait]
impl<S: StorageBackend> ListStore for S {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        StorageBackend::list(self, prefix).await
    }
}

#[async_trait]
impl<S: StorageBackend> MetaStore for S {
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        StorageBackend::head(self, path).await
    }
}

#[async_trait]
impl<S: StorageBackend> SignedUrlStore for S {
    async fn signed_url(&self, path: &str, expiry: std::time::Duration) -> Result<String> {
        StorageBackend::signed_url(self, path, expiry).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_read_store_works() {
        let backend = Arc::new(MemoryBackend::new());

        // Use as StorageBackend to write
        backend
            .put(
                "test.txt",
                Bytes::from("hello"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write");

        // Use as ReadStore to read
        let reader: &dyn ReadStore = backend.as_ref();
        let data = reader.get("test.txt").await.expect("read");
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_ledger_put_store_uses_does_not_exist() {
        let backend = Arc::new(MemoryBackend::new());
        let writer: &dyn LedgerPutStore = backend.as_ref();
        let key = LedgerKey::event(crate::CatalogDomain::Catalog, "test");

        // First write succeeds
        let result = writer
            .put_ledger(&key, Bytes::from("{}"))
            .await
            .expect("write");
        assert!(matches!(result, WriteResult::Success { .. }));

        // Second write fails (DoesNotExist precondition)
        let result = writer
            .put_ledger(&key, Bytes::from("{}"))
            .await
            .expect("write");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn test_cas_store_requires_version() {
        let backend = Arc::new(MemoryBackend::new());
        let key = ManifestKey::root();

        // Create initial manifest
        let result = backend
            .put(
                key.as_ref(),
                Bytes::from("v1"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("create");
        let version = match result {
            WriteResult::Success { version } => version,
            _ => panic!("expected success"),
        };

        // CAS with correct version succeeds
        let cas_store: &dyn CasStore = backend.as_ref();
        let result = cas_store
            .cas(&key, Bytes::from("v2"), &version)
            .await
            .expect("cas");
        assert!(matches!(result, WriteResult::Success { .. }));

        // CAS with stale version fails
        let result = cas_store
            .cas(&key, Bytes::from("v3"), &version)
            .await
            .expect("cas");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn test_meta_store_head() {
        let backend = Arc::new(MemoryBackend::new());
        let meta_store: &dyn MetaStore = backend.as_ref();

        // Non-existent returns None
        let result = meta_store.head("missing.txt").await.expect("head");
        assert!(result.is_none());

        // Create file
        backend
            .put(
                "exists.txt",
                Bytes::from("data"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("create");

        // Existing returns Some
        let result = meta_store.head("exists.txt").await.expect("head");
        assert!(result.is_some());
        assert_eq!(result.unwrap().size, 4);
    }
}
