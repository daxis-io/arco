//! Narrow storage capability for immutable authority objects and fenced heads.

use bytes::Bytes;

use crate::Result;
use crate::scoped_storage::ScopedStorage;
use crate::storage::{ObjectMeta, WritePrecondition, WriteResult};

/// Preconditions allowed for authority-object publication.
///
/// Authority storage deliberately excludes unconditional writes: immutable
/// objects are created once and mutable heads advance only through versioned
/// compare-and-swap.
#[derive(Debug, Clone)]
pub enum AuthorityWritePrecondition {
    /// Create an immutable object or initial authority head only when absent.
    DoesNotExist,
    /// Replace an authority head only when its opaque version token matches.
    MatchesVersion(String),
}

impl From<AuthorityWritePrecondition> for WritePrecondition {
    fn from(precondition: AuthorityWritePrecondition) -> Self {
        match precondition {
            AuthorityWritePrecondition::DoesNotExist => Self::DoesNotExist,
            AuthorityWritePrecondition::MatchesVersion(version) => Self::MatchesVersion(version),
        }
    }
}

/// Scope-preserving storage capability for authority artifacts and fenced heads.
///
/// The wrapper intentionally exposes no enumeration, deletion, signed URL, or
/// unconditional write operations. State kernels can therefore depend on the
/// minimum durable authority contract without acquiring provider or lifecycle
/// responsibilities.
#[derive(Clone)]
pub struct ScopedAuthorityStore {
    storage: ScopedStorage,
}

impl ScopedAuthorityStore {
    /// Narrows workspace-scoped storage to the authority capability.
    #[must_use]
    pub const fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Returns the tenant ID enforced by the underlying scope.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        self.storage.tenant_id()
    }

    /// Returns the typed physical authority root.
    #[must_use]
    pub fn scope(&self) -> &crate::AuthorityScope {
        self.storage.scope()
    }

    /// Returns the legacy request workspace context, not the physical root ID.
    /// Use [`Self::scope`] for authority identity; state kernels must reject root
    /// families that their persisted scope representation cannot distinguish.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        self.storage.workspace_id()
    }

    /// Reads an authority object at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns path-validation, not-found, or backend errors.
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        self.storage.get_raw(path).await
    }

    /// Reads authority-object metadata without fetching its content.
    ///
    /// # Errors
    ///
    /// Returns path-validation or backend errors.
    pub async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.storage.head_raw(path).await
    }

    /// Publishes an immutable authority object or advances a fenced head.
    ///
    /// # Errors
    ///
    /// Returns path-validation or backend errors. Failed preconditions are
    /// represented by [`WriteResult::PreconditionFailed`].
    pub async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: AuthorityWritePrecondition,
    ) -> Result<WriteResult> {
        self.storage.put_raw(path, data, precondition.into()).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::{AuthorityWritePrecondition, ScopedAuthorityStore};
    use crate::ScopedStorage;
    use crate::storage::{MemoryBackend, WriteResult};

    #[tokio::test]
    async fn exposes_only_scoped_reads_heads_and_conditional_writes() {
        let scoped =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let authority = ScopedAuthorityStore::new(scoped);

        assert_eq!(authority.tenant_id(), "tenant");
        assert_eq!(authority.workspace_id(), "workspace");
        assert_eq!(authority.scope().workspace_id(), Some("workspace"));
        let created = authority
            .put(
                "control/v1/domains/catalog/current.json",
                Bytes::from_static(b"v1"),
                AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let WriteResult::Success { version } = created else {
            panic!("create must succeed");
        };
        assert_eq!(
            authority
                .get("control/v1/domains/catalog/current.json")
                .await
                .unwrap(),
            Bytes::from_static(b"v1")
        );
        assert_eq!(
            authority
                .head("control/v1/domains/catalog/current.json")
                .await
                .unwrap()
                .unwrap()
                .version,
            version
        );

        let replaced = authority
            .put(
                "control/v1/domains/catalog/current.json",
                Bytes::from_static(b"v2"),
                AuthorityWritePrecondition::MatchesVersion(version),
            )
            .await
            .unwrap();
        assert!(matches!(replaced, WriteResult::Success { .. }));
    }

    #[tokio::test]
    async fn retains_scoped_path_validation() {
        let scoped =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let authority = ScopedAuthorityStore::new(scoped);

        assert!(authority.get("../other/head.json").await.is_err());
        assert!(
            authority
                .put(
                    "/absolute/head.json",
                    Bytes::new(),
                    AuthorityWritePrecondition::DoesNotExist,
                )
                .await
                .is_err()
        );
    }
}
