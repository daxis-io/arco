//! Tenant identity-rooted storage capability over [`ScopedStorage`].

use std::sync::Arc;

use crate::authority_root::AuthorityScope;
use crate::error::Result;
use crate::scoped_storage::ScopedStorage;
use crate::storage::StorageBackend;

/// Identity storage rooted at 'tenant={t}/identity/'.
///
/// It acts as a thin capability wrapper over [`ScopedStorage`],
/// hiding its workspace-shaped APIs. This ensures that `IdentityStorage`
/// cannot enter legacy catalog or ledger APIs.
///
/// The raw backend is not available through this capability:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use arco_core::{IdentityStorage, MemoryBackend};
/// let identity = IdentityStorage::new(Arc::new(MemoryBackend::new()), "acme").unwrap();
/// let _ = identity.backend();
/// ```
#[derive(Clone)]
pub struct IdentityStorage {
    inner: ScopedStorage,
}

impl IdentityStorage {
    /// Creates identity-scoped storage.
    ///
    /// # Errors
    ///
    /// Returns an error if `tenant_id` is invalid.
    pub fn new(backend: Arc<dyn StorageBackend>, tenant_id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            inner: ScopedStorage::from_authority_scope(
                backend,
                &AuthorityScope::tenant_identity(tenant_id)?,
                "",
            )?,
        })
    }

    /// Crate-internal access to the underlying root-scoped storage.
    pub(crate) fn inner(&self) -> &ScopedStorage {
        &self.inner
    }

    /// Returns the tenant ID.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        self.inner.tenant_id()
    }

    /// Returns the typed authority scope.
    #[must_use]
    pub fn scope(&self) -> &AuthorityScope {
        self.inner.scope()
    }
}

#[cfg(test)]
mod tests {
    use crate::authority_root::AuthorityRoot;
    use crate::prelude::ReadStore;
    use crate::storage::{MemoryBackend, WritePrecondition};
    use bytes::Bytes;
    use std::sync::Arc;

    use super::IdentityStorage;

    #[test]
    fn identity_storage_uses_tenant_identity_prefix() {
        let storage = IdentityStorage::new(Arc::new(MemoryBackend::new()), "acme").unwrap();
        assert_eq!(storage.tenant_id(), "acme");
        assert_eq!(storage.scope().prefix(), "tenant=acme/identity");
        assert!(matches!(
            storage.scope().root(),
            AuthorityRoot::TenantIdentity
        ));
    }

    #[test]
    fn identity_storage_rejects_invalid_tenant() {
        assert!(IdentityStorage::new(Arc::new(MemoryBackend::new()), "").is_err());
        assert!(IdentityStorage::new(Arc::new(MemoryBackend::new()), "../acme").is_err());
    }

    #[tokio::test]
    async fn identity_writes_land_under_identity_root_only() {
        let backend = Arc::new(MemoryBackend::new());
        let identity = IdentityStorage::new(backend.clone(), "acme").unwrap();
        identity
            .inner()
            .put_raw("state/x", Bytes::from_static(b"v"), WritePrecondition::None)
            .await
            .unwrap();

        assert_eq!(
            backend.get("tenant=acme/identity/state/x").await.unwrap(),
            Bytes::from_static(b"v")
        );
        assert!(
            backend
                .get("tenant=acme/workspace=acme/state/x")
                .await
                .is_err()
        );
    }
}
