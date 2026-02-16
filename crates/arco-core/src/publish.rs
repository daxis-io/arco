//! Publish permit system for gating manifest updates (Gate 5).
//!
//! All manifest updates MUST require a `PublishPermit`. This ensures:
//! - Visibility changes are intentional and auditable
//! - Stale lock holders cannot publish (fencing token from lock)
//! - Rollback is detectable via fencing token comparison
//!
//! # Architecture
//!
//! ```text
//! LockGuard ──► PermitIssuer ──► PublishPermit ──► Publisher ──► CAS
//!     │              │                │                │
//!     │              │                │                └─ Only public manifest API
//!     │              │                └─ Private constructor
//!     │              └─ Derives fencing token from lock
//!     └─ Distributed lock acquisition
//! ```
//!
//! # Critical Invariant
//!
//! You cannot create a `PublishPermit` without holding a valid distributed lock.
//! The `FencingToken` constructor is `pub(crate)` to prevent external creation.
//!
//! # Example
//!
//! ```rust,ignore
//! use arco_core::publish::Publisher;
//!
//! // After acquiring a distributed lock:
//! let issuer = lock_guard.permit_issuer();
//! let permit = issuer.issue_permit("catalog", manifest_version);
//!
//! let publisher = Publisher::new(cas_storage);
//! let result = publisher.publish(permit, &manifest_key, manifest_bytes).await?;
//! ```

use std::fmt;
use std::future::Future;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::scoped_storage::ScopedStorage;
use crate::storage::{WritePrecondition, WriteResult};
use crate::storage_keys::ManifestKey;
use crate::storage_traits::CasStore;

// ============================================================================
// FencingToken - Monotonic token from distributed lock
// ============================================================================

/// Fencing token from distributed lock acquisition.
///
/// This is a monotonically increasing token derived from the lock's sequence
/// number. It's stored in the manifest and used to detect stale lock holders.
///
/// # Distributed Fencing
///
/// Unlike process-local counters (which don't work across crashes/failover),
/// the fencing token is stored in the lock object itself and incremented
/// atomically on each acquisition. This ensures:
///
/// 1. New lock holder always has higher token than stale holder
/// 2. Token survives process restart
/// 3. Manifest can reject writes from stale holders
///
/// # Private Constructor
///
/// The `new()` constructor is `pub(crate)` to prevent external creation.
/// Only the lock module can create fencing tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Creates a fencing token from lock metadata.
    ///
    /// # Warning
    ///
    /// This constructor should only be called by distributed lock implementations.
    /// Application code should obtain tokens through `LockGuard::fencing_token()`.
    ///
    /// Creating tokens directly bypasses the distributed fencing guarantee.
    #[must_use]
    pub(crate) fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    /// Returns the raw sequence number for serialization/logging.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for FencingToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FencingToken({})", self.0)
    }
}

// ============================================================================
// PermitIssuer - Issues permits from held lock
// ============================================================================

/// Issues publish permits from a held distributed lock.
///
/// You can only create a `PermitIssuer` from a `LockGuard`, ensuring that
/// permits are only issued while holding the distributed lock.
///
/// # Example
///
/// ```rust,ignore
/// let guard = lock.acquire(ttl, retries).await?;
/// let issuer = guard.permit_issuer();
/// let permit = issuer.issue_permit("catalog", manifest_version);
/// ```
#[derive(Debug)]
pub struct PermitIssuer {
    /// Fencing token from lock acquisition.
    fencing_token: FencingToken,
    /// Resource being locked (for audit).
    resource: String,
}

impl PermitIssuer {
    /// Creates a permit issuer from a validated fencing token.
    ///
    /// # Arguments
    ///
    /// - `fencing_token`: The token from lock acquisition
    /// - `resource`: The lock resource name (for audit logging)
    ///
    /// # Internal
    ///
    /// Callers MUST validate the token against the lock state before constructing
    /// an issuer with this method.
    #[must_use]
    pub(crate) fn from_validated_token(
        fencing_token: FencingToken,
        resource: impl Into<String>,
    ) -> Self {
        Self {
            fencing_token,
            resource: resource.into(),
        }
    }

    /// Issues a publish permit for the given domain.
    ///
    /// The permit carries the fencing token from lock acquisition and the
    /// expected manifest version for CAS.
    ///
    /// # Arguments
    ///
    /// - `domain`: The catalog domain being published
    /// - `expected_version`: The manifest version for CAS precondition
    #[must_use]
    pub fn issue_permit(
        &self,
        domain: impl Into<String>,
        expected_version: String,
    ) -> PublishPermit {
        let commit_ulid = ulid::Ulid::new().to_string();
        self.issue_permit_with_commit_ulid(domain, expected_version, commit_ulid)
    }

    /// Issues a permit for creating a new manifest (`DoesNotExist` precondition).
    ///
    /// Used when initializing a domain for the first time.
    #[must_use]
    pub fn issue_create_permit(&self, domain: impl Into<String>) -> PublishPermit {
        let commit_ulid = ulid::Ulid::new().to_string();
        self.issue_create_permit_with_commit_ulid(domain, commit_ulid)
    }

    /// Issues a publish permit with a caller-supplied commit ULID.
    ///
    /// Use this when the commit ULID must be monotonic relative to a prior manifest.
    #[must_use]
    pub fn issue_permit_with_commit_ulid(
        &self,
        domain: impl Into<String>,
        expected_version: String,
        commit_ulid: impl Into<String>,
    ) -> PublishPermit {
        PublishPermit {
            fencing_token: self.fencing_token,
            domain: domain.into(),
            expected_version: Some(expected_version),
            commit_ulid: commit_ulid.into(),
            consumed: false,
            issuer_resource: self.resource.clone(),
        }
    }

    /// Issues a create permit with a caller-supplied commit ULID.
    #[must_use]
    pub fn issue_create_permit_with_commit_ulid(
        &self,
        domain: impl Into<String>,
        commit_ulid: impl Into<String>,
    ) -> PublishPermit {
        PublishPermit {
            fencing_token: self.fencing_token,
            domain: domain.into(),
            expected_version: None,
            commit_ulid: commit_ulid.into(),
            consumed: false,
            issuer_resource: self.resource.clone(),
        }
    }

    /// Returns the fencing token this issuer uses.
    #[must_use]
    pub fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns the lock resource name.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
}

// ============================================================================
// PublishPermit - Single-use permit for manifest CAS
// ============================================================================

/// A permit authorizing a manifest publish operation.
///
/// Permits are:
/// - **Non-cloneable**: Prevents accidental reuse
/// - **Consumed on use**: Single-use guarantee
/// - **Lock-derived**: Fencing token from distributed lock
///
/// # Private Constructor
///
/// You cannot construct a `PublishPermit` directly. Use `PermitIssuer`
/// which requires holding a distributed lock.
///
/// # Example
///
/// ```rust,ignore
/// let permit = issuer.issue_permit("catalog", version);
/// publisher.publish(permit, &key, data).await?;
/// // permit is now consumed
/// ```
#[derive(Debug)]
pub struct PublishPermit {
    /// Fencing token from lock acquisition.
    fencing_token: FencingToken,
    /// Domain being published to.
    domain: String,
    /// Expected parent version (for CAS). None means `DoesNotExist`.
    expected_version: Option<String>,
    /// Unique commit identifier for this publish.
    commit_ulid: String,
    /// Whether this permit has been consumed.
    consumed: bool,
    /// Resource that issued this permit (for audit).
    issuer_resource: String,
}

impl PublishPermit {
    /// Returns the fencing token for audit logging and stale detection.
    #[must_use]
    pub fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns the domain this permit is for.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the expected version for CAS.
    #[must_use]
    pub fn expected_version(&self) -> Option<&str> {
        self.expected_version.as_deref()
    }

    /// Returns the commit ULID for this publish operation.
    #[must_use]
    pub fn commit_ulid(&self) -> &str {
        &self.commit_ulid
    }

    /// Returns whether this permit has been consumed.
    #[must_use]
    pub fn is_consumed(&self) -> bool {
        self.consumed
    }

    /// Returns the issuer resource name (for audit).
    #[must_use]
    pub fn issuer_resource(&self) -> &str {
        &self.issuer_resource
    }

    /// Consumes the permit, marking it as used.
    ///
    /// # Panics
    ///
    /// Panics if the permit has already been consumed (double-publish attempt).
    fn consume(&mut self) {
        assert!(!self.consumed, "PublishPermit already consumed");
        self.consumed = true;
    }
}

impl Drop for PublishPermit {
    fn drop(&mut self) {
        if !self.consumed {
            tracing::warn!(
                fencing_token = %self.fencing_token,
                domain = %self.domain,
                commit_ulid = %self.commit_ulid,
                issuer = %self.issuer_resource,
                "PublishPermit dropped without being consumed"
            );
        }
    }
}

impl fmt::Display for PublishPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PublishPermit(domain={}, fencing_token={}, commit_ulid={}, consumed={})",
            self.domain,
            self.fencing_token.sequence(),
            self.commit_ulid,
            self.consumed
        )
    }
}

// ============================================================================
// Publisher - The ONLY public API for manifest updates
// ============================================================================

/// Publisher that requires a permit to update manifests.
///
/// This is the ONLY public API for publishing manifests. Direct CAS access
/// to manifests is not exported.
///
/// # Example
///
/// ```rust,ignore
/// let publisher = Publisher::new(&storage);
/// let permit = issuer.issue_permit("catalog", version);
/// let result = publisher.publish(permit, &manifest_key, data).await?;
/// ```
pub struct Publisher<'a, S: CasStore + ?Sized> {
    storage: &'a S,
}

impl<'a, S: CasStore + ?Sized> Publisher<'a, S> {
    /// Creates a new publisher with storage backend reference.
    #[must_use]
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }

    /// Publishes a manifest update using the provided permit.
    ///
    /// The permit is consumed on success. On CAS failure, the permit is
    /// also consumed (you cannot retry with the same permit - issue a new one).
    ///
    /// # Returns
    ///
    /// - `WriteResult::Success` with new version on success
    /// - `WriteResult::PreconditionFailed` if version changed (CAS race)
    ///
    /// # Errors
    ///
    /// Returns error if the storage operation fails.
    pub async fn publish(
        &self,
        mut permit: PublishPermit,
        key: &ManifestKey,
        data: Bytes,
    ) -> Result<WriteResult> {
        tracing::info!(
            fencing_token = %permit.fencing_token(),
            domain = %permit.domain(),
            commit_ulid = %permit.commit_ulid(),
            manifest = %key,
            "Attempting manifest publish"
        );

        let result = match permit.expected_version() {
            Some(version) => self.storage.cas(key, data, version).await?,
            None => self.storage.create_if_absent(key, data).await?,
        };

        // Always consume the permit - even on failure, you need a new one
        permit.consume();

        match &result {
            WriteResult::Success { version } => {
                tracing::info!(
                    fencing_token = %permit.fencing_token(),
                    domain = %permit.domain(),
                    commit_ulid = %permit.commit_ulid(),
                    manifest = %key,
                    new_version = %version,
                    "Manifest published successfully"
                );
            }
            WriteResult::PreconditionFailed { current_version } => {
                tracing::warn!(
                    fencing_token = %permit.fencing_token(),
                    domain = %permit.domain(),
                    commit_ulid = %permit.commit_ulid(),
                    manifest = %key,
                    current_version = %current_version,
                    "Manifest publish failed - CAS race"
                );
            }
        }

        Ok(result)
    }
}

/// Durability policy for snapshot+pointer publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPointerDurability {
    /// Return success only when pointer CAS succeeds (read-visible).
    Visible,
    /// Return success once snapshot is persisted, even if pointer CAS loses a race.
    Persisted,
}

/// Outcome of a snapshot+pointer publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPointerPublishOutcome {
    /// Snapshot persisted and pointer CAS succeeded.
    Visible {
        /// Version token returned by the pointer CAS write.
        pointer_version: String,
    },
    /// Snapshot persisted but pointer CAS lost a race.
    PersistedNotVisible,
}

/// Publishes immutable snapshot then CAS-updates a visibility pointer.
///
/// This helper standardizes the shared snapshot+pointer transaction protocol used by
/// compaction/writer paths:
/// 1. Write immutable snapshot with `DoesNotExist`.
/// 2. Run caller-provided pre-pointer validation.
/// 3. CAS write pointer (`DoesNotExist` or `MatchesVersion`).
/// 4. Optionally mirror legacy manifest path on visible success.
///
/// # Errors
///
/// Returns `Error::PreconditionFailed` for snapshot conflicts or strict-mode pointer CAS loss.
#[allow(clippy::too_many_arguments)]
pub async fn publish_snapshot_pointer_transaction<B>(
    storage: &ScopedStorage,
    snapshot_path: &str,
    snapshot_payload: Bytes,
    pointer_path: &str,
    pointer_payload: Bytes,
    expected_pointer_version: Option<&str>,
    legacy_mirror: Option<(&str, Bytes)>,
    durability: SnapshotPointerDurability,
    before_pointer_publish: B,
) -> Result<SnapshotPointerPublishOutcome>
where
    B: Future<Output = Result<()>>,
{
    let snapshot_result = storage
        .put_raw(
            snapshot_path,
            snapshot_payload,
            WritePrecondition::DoesNotExist,
        )
        .await?;
    match snapshot_result {
        WriteResult::Success { .. } => {}
        WriteResult::PreconditionFailed { .. } => {
            return Err(Error::PreconditionFailed {
                message: format!("immutable manifest snapshot already exists: {snapshot_path}"),
            });
        }
    }

    before_pointer_publish.await?;

    let pointer_precondition = expected_pointer_version
        .map_or(WritePrecondition::DoesNotExist, |v| {
            WritePrecondition::MatchesVersion(v.to_string())
        });
    let pointer_result = storage
        .put_raw(pointer_path, pointer_payload, pointer_precondition)
        .await?;

    match pointer_result {
        WriteResult::Success { version } => {
            if let Some((legacy_path, legacy_payload)) = legacy_mirror {
                let _ = storage
                    .put_raw(legacy_path, legacy_payload, WritePrecondition::None)
                    .await?;
            }
            Ok(SnapshotPointerPublishOutcome::Visible {
                pointer_version: version,
            })
        }
        WriteResult::PreconditionFailed { .. } => {
            if durability == SnapshotPointerDurability::Persisted {
                Ok(SnapshotPointerPublishOutcome::PersistedNotVisible)
            } else {
                Err(Error::PreconditionFailed {
                    message: "manifest publish failed - concurrent write".to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CatalogPaths;
    use crate::scoped_storage::ScopedStorage;
    use crate::storage::MemoryBackend;
    use crate::storage::StorageBackend;
    use crate::storage::WritePrecondition;
    use std::sync::Arc;

    fn test_issuer() -> PermitIssuer {
        PermitIssuer::from_validated_token(FencingToken::new(42), "test-lock")
    }

    #[test]
    fn test_fencing_token_ordering() {
        let t1 = FencingToken::new(1);
        let t2 = FencingToken::new(2);
        let t3 = FencingToken::new(2);

        assert!(t1 < t2);
        assert_eq!(t2, t3);
        assert!(t2 > t1);
    }

    #[test]
    fn test_permit_consumes_once() {
        let issuer = test_issuer();
        let mut permit = issuer.issue_permit("catalog", "v1".into());
        assert!(!permit.consumed);
        permit.consume();
        assert!(permit.consumed);
    }

    #[test]
    #[should_panic(expected = "already consumed")]
    fn test_permit_cannot_consume_twice() {
        let issuer = test_issuer();
        let mut permit = issuer.issue_permit("catalog", "v1".into());
        permit.consume();
        permit.consume(); // Should panic
    }

    #[test]
    fn test_fencing_token_from_issuer() {
        let issuer = PermitIssuer::from_validated_token(FencingToken::new(100), "lock-a");
        let permit = issuer.issue_permit("catalog", "v1".into());
        assert_eq!(permit.fencing_token().sequence(), 100);
    }

    #[test]
    fn test_different_issuers_same_token() {
        // Same fencing token from same lock acquisition
        let issuer1 = PermitIssuer::from_validated_token(FencingToken::new(1), "lock-a");
        let issuer2 = PermitIssuer::from_validated_token(FencingToken::new(1), "lock-a");

        let permit1 = issuer1.issue_permit("a", "v1".into());
        let permit2 = issuer2.issue_permit("b", "v1".into());

        assert_eq!(permit1.fencing_token(), permit2.fencing_token());
    }

    #[test]
    fn test_permit_has_unique_commit_ulid() {
        let issuer = test_issuer();
        let permit1 = issuer.issue_permit("catalog", "v1".into());
        let permit2 = issuer.issue_permit("catalog", "v1".into());

        // Each permit gets a unique commit ULID
        assert_ne!(permit1.commit_ulid(), permit2.commit_ulid());
    }

    #[test]
    fn test_create_permit_has_no_expected_version() {
        let issuer = test_issuer();
        let permit = issuer.issue_create_permit("catalog");
        assert!(permit.expected_version().is_none());
    }

    #[tokio::test]
    async fn test_publisher_success() {
        let backend = Arc::new(MemoryBackend::new());
        let publisher = Publisher::new(backend.as_ref());
        let issuer = test_issuer();

        let permit = issuer.issue_create_permit("catalog");
        let key = ManifestKey::domain(crate::CatalogDomain::Catalog);

        let result = publisher
            .publish(permit, &key, Bytes::from(r#"{"version": 1}"#))
            .await
            .expect("publish");

        assert!(matches!(result, WriteResult::Success { .. }));
    }

    #[tokio::test]
    async fn test_publisher_cas_race() {
        let backend = Arc::new(MemoryBackend::new());

        // First write
        backend
            .put(
                "manifests/catalog.manifest.json",
                Bytes::from(r#"{"version": 1}"#),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("first write");

        let publisher = Publisher::new(backend.as_ref());
        let issuer = test_issuer();

        // Try to publish with wrong version
        let permit = issuer.issue_permit("catalog", "wrong-version".into());
        let key = ManifestKey::domain(crate::CatalogDomain::Catalog);

        let result = publisher
            .publish(permit, &key, Bytes::from(r#"{"version": 2}"#))
            .await
            .expect("publish");

        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn snapshot_pointer_publish_conflict_in_persisted_mode_returns_persisted_not_visible() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped");

        let pointer_path = CatalogPaths::domain_manifest_pointer(crate::CatalogDomain::Catalog);
        storage
            .put_raw(
                &pointer_path,
                Bytes::from_static(b"{\"manifestId\":\"00000000000000000000\"}"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("seed pointer");

        let current = storage
            .head_raw(&pointer_path)
            .await
            .expect("head pointer")
            .expect("pointer exists");

        let outcome = publish_snapshot_pointer_transaction(
            &storage,
            "manifests/catalog/00000000000000000001.json",
            Bytes::from_static(b"{\"manifestId\":\"00000000000000000001\"}"),
            &pointer_path,
            Bytes::from_static(b"{\"manifestId\":\"00000000000000000001\"}"),
            Some("stale-version"),
            None,
            SnapshotPointerDurability::Persisted,
            async { Ok(()) },
        )
        .await
        .expect("publish outcome");
        assert_eq!(outcome, SnapshotPointerPublishOutcome::PersistedNotVisible);

        let pointer_after = storage
            .head_raw(&pointer_path)
            .await
            .expect("head pointer after")
            .expect("pointer exists");
        assert_eq!(pointer_after.version, current.version);
    }

    #[tokio::test]
    async fn snapshot_pointer_publish_visible_writes_legacy_mirror() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped");

        let pointer_path = CatalogPaths::domain_manifest_pointer(crate::CatalogDomain::Catalog);
        let legacy_path = CatalogPaths::domain_manifest(crate::CatalogDomain::Catalog);
        let snapshot_payload = Bytes::from_static(b"{\"manifestId\":\"00000000000000000001\"}");

        let outcome = publish_snapshot_pointer_transaction(
            &storage,
            "manifests/catalog/00000000000000000001.json",
            snapshot_payload.clone(),
            &pointer_path,
            snapshot_payload.clone(),
            None,
            Some((legacy_path.as_str(), snapshot_payload.clone())),
            SnapshotPointerDurability::Visible,
            async { Ok(()) },
        )
        .await
        .expect("publish outcome");
        assert!(matches!(
            outcome,
            SnapshotPointerPublishOutcome::Visible { pointer_version } if !pointer_version.is_empty()
        ));

        let legacy = storage.get_raw(&legacy_path).await.expect("legacy exists");
        assert_eq!(legacy, snapshot_payload);
    }
}
