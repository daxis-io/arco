//! Root-scoped storage seam used by the shared state kernel.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::authority_root::AuthorityScope;
use crate::error::Result;
use crate::identity_storage::IdentityStorage;
use crate::scoped_storage::{ScopedListPage, ScopedObjectMeta, ScopedPath, ScopedStorage};
use crate::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};

/// Typed storage roots shared kernel can operate over.
#[derive(Clone)]
pub enum RootStorage {
    /// Workspace- or metastore-rooted legacy storage.
    Scoped(ScopedStorage),
    /// Tenant identity-rooted storage.
    Identity(IdentityStorage),
}

impl RootStorage {
    #[must_use]
    fn storage(&self) -> &ScopedStorage {
        match self {
            Self::Scoped(storage) => storage,
            Self::Identity(storage) => storage.inner(),
        }
    }

    /// Returns the typed authority scope
    #[must_use]
    pub fn scope(&self) -> &AuthorityScope {
        self.storage().scope()
    }

    /// Returns the tenant ID.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        self.storage().tenant_id()
    }

    /// Returns the backend.
    #[must_use]
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        self.storage().backend()
    }

    /// Returns the legacy root-scoped storage.
    ///
    /// Only workspace/metastore roots yield `ScopedStorage`; identity roots never
    /// expose their inner `ScopedStorage` to preserve their typed boundary.
    #[must_use]
    pub fn as_legacy_scoped(&self) -> Option<&ScopedStorage> {
        match self {
            Self::Scoped(storage) => Some(storage),
            Self::Identity(_) => None,
        }
    }

    /// Validates a scope-relative path.
    ///
    /// # Errors
    /// Returns an error for unsafe paths
    pub fn validate_path(path: &str) -> Result<()> {
        ScopedStorage::validate_path(path)
    }

    /// Reads data at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences or the object is not found.
    pub async fn get_raw(&self, path: &str) -> Result<Bytes> {
        self.storage().get_raw(path).await
    }

    /// Writes data at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn put_raw(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.storage().put_raw(path, data, precondition).await
    }

    /// Deletes data at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn delete(&self, path: &str) -> Result<()> {
        self.storage().delete(path).await
    }

    /// Lists objects at a scope-relative path prefix.
    ///
    /// Returns relative paths (without the scope prefix).
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix contains traversal sequences.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ScopedPath>> {
        self.storage().list(prefix).await
    }

    /// Lists objects and returns metadata at a scope-relative prefix.
    ///
    /// Returned paths are relative to the scope and may be safely passed back to
    /// other `ScopedStorage` methods (e.g., `get_raw`).
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix contains traversal sequences or listing fails.
    pub async fn list_meta(&self, prefix: &str) -> Result<Vec<ScopedObjectMeta>> {
        self.storage().list_meta(prefix).await
    }

    /// Lists one bounded metadata page at a scope-relative prefix.
    ///
    /// `start_after` is an exclusive scope-relative path cursor returned by the
    /// previous page. The bound is delegated to the backend; this method never
    /// falls back to [`Self::list_meta`].
    ///
    /// # Errors
    ///
    /// Returns an error when a path is unsafe, the cursor is outside `prefix`,
    /// or the backend cannot provide bounded ordered listing.
    pub async fn list_page_meta(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ScopedListPage> {
        self.storage()
            .list_page_meta(prefix, start_after, limit)
            .await
    }

    /// Gets metadata at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn head_raw(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.storage().head_raw(path).await
    }

    /// Generates signed URL for a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn signed_url_raw(&self, path: &str, expiry: Duration) -> Result<String> {
        self.storage().signed_url_raw(path, expiry).await
    }
}

impl From<ScopedStorage> for RootStorage {
    fn from(storage: ScopedStorage) -> Self {
        Self::Scoped(storage)
    }
}

impl From<IdentityStorage> for RootStorage {
    fn from(storage: IdentityStorage) -> Self {
        Self::Identity(storage)
    }
}

#[async_trait]
impl StorageBackend for RootStorage {
    async fn get(&self, path: &str) -> Result<Bytes> {
        <ScopedStorage as StorageBackend>::get(self.storage(), path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        <ScopedStorage as StorageBackend>::get_range(self.storage(), path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        <ScopedStorage as StorageBackend>::put(self.storage(), path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        <ScopedStorage as StorageBackend>::delete(self.storage(), path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        <ScopedStorage as StorageBackend>::list(self.storage(), prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        <ScopedStorage as StorageBackend>::list_page(self.storage(), prefix, start_after, limit)
            .await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        <ScopedStorage as StorageBackend>::head(self.storage(), path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        <ScopedStorage as StorageBackend>::signed_url(self.storage(), path, expiry).await
    }
}

#[cfg(test)]
mod tests {
    use crate::IdentityStorage;
    use crate::ScopedStorage;
    use crate::storage::{MemoryBackend, StorageBackend, WritePrecondition};
    use bytes::Bytes;
    use std::sync::Arc;

    use super::RootStorage;

    #[tokio::test]
    async fn root_storage_delegates_for_both_variants() {
        let backend = Arc::new(MemoryBackend::new());
        let scoped =
            RootStorage::Scoped(ScopedStorage::new(backend.clone(), "acme", "prod").unwrap());
        let identity =
            RootStorage::Identity(IdentityStorage::new(backend.clone(), "acme").unwrap());

        scoped
            .put_raw("k", Bytes::from_static(b"s"), WritePrecondition::None)
            .await
            .unwrap();
        identity
            .put_raw("k", Bytes::from_static(b"i"), WritePrecondition::None)
            .await
            .unwrap();

        assert_eq!(scoped.get_raw("k").await.unwrap(), Bytes::from_static(b"s"));
        assert_eq!(
            identity.get_raw("k").await.unwrap(),
            Bytes::from_static(b"i")
        );
        assert_eq!(scoped.scope().prefix(), "tenant=acme/workspace=prod");
        assert_eq!(identity.scope().prefix(), "tenant=acme/identity");
    }

    #[tokio::test]
    async fn root_storage_implements_storage_backend() {
        let root: RootStorage = IdentityStorage::new(Arc::new(MemoryBackend::new()), "acme")
            .unwrap()
            .into();
        root.put("k", Bytes::from_static(b"v"), WritePrecondition::None)
            .await
            .unwrap();

        assert_eq!(
            StorageBackend::get(&root, "k").await.unwrap(),
            Bytes::from_static(b"v")
        );
        assert_eq!(
            StorageBackend::head(&root, "k")
                .await
                .unwrap()
                .unwrap()
                .size,
            1
        );
    }
}
