//! Tier 1 manifest writer with distributed lock + CAS semantics.
//!
//! Tier 1 writes are the strongly-consistent catalog operations (DDL-like):
//! create/update/drop assets, schemas, and other low-frequency mutations.
//!
//! The critical invariants are:
//! - Only one writer enters the critical section at a time (distributed lock)
//! - Manifest updates are committed via CAS (`MatchesVersion`)
//! - Writers retry on CAS conflicts (e.g., if a writer bypasses the lock)
//! - On-disk manifests are physically multi-file (root + domain manifests)

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use arco_core::publish::{
    SnapshotPointerDurability, SnapshotPointerPublishOutcome, publish_snapshot_pointer_transaction,
};
use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_core::storage_keys::{CommitKey, LedgerKey};
use arco_core::storage_traits::{CommitPutStore, LedgerPutStore};
use arco_core::{
    CatalogDomain, CatalogEvent, CatalogEventPayload, CatalogPaths, EventId, ScopedStorage,
};

use crate::error::{CatalogError, Result};
use crate::lock::LockGuard;
use crate::lock::{DEFAULT_LOCK_TTL, DEFAULT_MAX_RETRIES, DistributedLock};
use crate::manifest::{
    CatalogDomainManifest, CatalogManifest, CommitRecord, DomainManifestPointer,
    ExecutionsManifest, LineageManifest, RootManifest, SearchManifest, compute_manifest_hash,
    next_manifest_id,
};

/// Maximum CAS retries for manifest writes.
const DEFAULT_MAX_CAS_RETRIES: u32 = 10;

/// Tier 1 writer for catalog manifests.
///
/// Owns:
/// - Tenant/workspace scoped storage
/// - A distributed lock instance
/// - CAS retry policy
pub struct Tier1Writer {
    storage: ScopedStorage,
    lock: DistributedLock<dyn StorageBackend>,
    lock_ttl: Duration,
    lock_max_retries: u32,
    cas_max_retries: u32,
}

impl Tier1Writer {
    /// Creates a new Tier 1 writer for the given scope.
    ///
    /// The lock path is derived from [`ScopedStorage::lock`].
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        let backend = storage.backend().clone();
        let lock_path = storage.lock(CatalogDomain::Catalog);
        let lock = DistributedLock::new(backend, lock_path);

        Self {
            storage,
            lock,
            lock_ttl: DEFAULT_LOCK_TTL,
            lock_max_retries: DEFAULT_MAX_RETRIES,
            cas_max_retries: DEFAULT_MAX_CAS_RETRIES,
        }
    }

    /// Sets the lock acquisition policy for this writer.
    #[must_use]
    pub const fn with_lock_policy(mut self, ttl: Duration, max_retries: u32) -> Self {
        self.lock_ttl = ttl;
        self.lock_max_retries = max_retries;
        self
    }

    /// Sets the maximum CAS retries for manifest updates.
    #[must_use]
    pub const fn with_cas_retries(mut self, max_retries: u32) -> Self {
        self.cas_max_retries = max_retries;
        self
    }

    /// Initializes the catalog manifests (idempotent).
    ///
    /// Creates:
    /// - `manifests/root.manifest.json` (entry point)
    /// - `manifests/catalog.manifest.json`
    /// - `manifests/lineage.manifest.json`
    /// - `manifests/executions.manifest.json`
    /// - `manifests/search.manifest.json`
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail.
    pub async fn initialize(&self) -> Result<()> {
        let guard = self
            .lock
            .acquire_with_operation(
                self.lock_ttl,
                self.lock_max_retries,
                Some("InitializeCatalog".into()),
            )
            .await
            .map_err(CatalogError::from)?;

        let mut root = RootManifest::new();
        root.normalize_paths();
        self.ensure_json_exists(CatalogPaths::ROOT_MANIFEST, &root)
            .await?;

        self.ensure_json_exists(&root.catalog_manifest_path, &CatalogDomainManifest::new())
            .await?;
        self.ensure_json_exists(&root.lineage_manifest_path, &LineageManifest::new())
            .await?;
        self.ensure_json_exists(&root.executions_manifest_path, &ExecutionsManifest::new())
            .await?;
        self.ensure_json_exists(&root.search_manifest_path, &SearchManifest::new())
            .await?;

        guard.release().await.map_err(CatalogError::from)
    }

    /// Reads the current catalog manifest by loading domain manifests.
    ///
    /// # Errors
    ///
    /// Returns an error if any required manifest is missing or cannot be parsed.
    pub async fn read_manifest(&self) -> Result<CatalogManifest> {
        let mut root: RootManifest = self.read_json(CatalogPaths::ROOT_MANIFEST).await?;
        root.normalize_paths();

        let catalog: CatalogDomainManifest = self.read_json(&root.catalog_manifest_path).await?;
        let lineage: LineageManifest = self.read_json(&root.lineage_manifest_path).await?;
        let executions: ExecutionsManifest = self.read_json(&root.executions_manifest_path).await?;
        let search: SearchManifest = self.read_json(&root.search_manifest_path).await?;

        Ok(CatalogManifest {
            version: root.version,
            catalog,
            lineage,
            executions,
            search,
            created_at: root.updated_at,
            updated_at: Utc::now(),
        })
    }

    /// Applies an update to the manifest and commits via CAS.
    ///
    /// The provided closure may be invoked multiple times if CAS conflicts occur,
    /// and must therefore be free of side effects.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired, manifests are missing, or
    /// if the CAS update fails after all retries.
    pub async fn update<F>(&self, mut update_fn: F) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        let guard = self
            .lock
            .acquire_with_operation(self.lock_ttl, self.lock_max_retries, Some("Update".into()))
            .await
            .map_err(CatalogError::from)?;

        let result = self.update_inner(&guard, &mut update_fn).await;

        match result {
            Ok(commit) => {
                guard.release().await.map_err(CatalogError::from)?;
                Ok(commit)
            }
            Err(e) => Err(e),
        }
    }

    /// Applies an update to the catalog domain manifest while an external lock is held.
    ///
    /// This is used by higher-level writers that acquire the lock once, perform
    /// snapshot writes, then publish by updating the manifest in the same critical
    /// section.
    ///
    /// The passed `guard` is a proof of lock acquisition; it is not otherwise used.
    ///
    /// # Errors
    ///
    /// Returns an error if manifest reads/writes fail or if the update closure returns an error.
    pub async fn update_locked<F>(
        &self,
        guard: &LockGuard<dyn StorageBackend>,
        mut update_fn: F,
    ) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        self.update_inner(guard, &mut update_fn).await
    }

    /// Acquires the catalog domain lock and returns a guard.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired within the retry budget.
    pub async fn acquire_lock(
        &self,
        ttl: Duration,
        max_retries: u32,
    ) -> Result<LockGuard<dyn StorageBackend>> {
        self.lock
            .acquire(ttl, max_retries)
            .await
            .map_err(Self::map_lock)
    }

    fn map_lock(err: arco_core::Error) -> CatalogError {
        CatalogError::from(err)
    }

    /// Appends a ledger event for a Tier-1 DDL operation (ADR-018).
    ///
    /// This is the new flow for Tier-1 operations where API only appends events
    /// to the ledger and the compactor is responsible for writing Parquet and
    /// updating manifests.
    ///
    /// # Flow
    ///
    /// 1. API holds distributed lock
    /// 2. API calls this method to append the DDL event
    /// 3. API calls compactor sync RPC with explicit event paths
    /// 4. Compactor writes Parquet + publishes manifest
    /// 5. API releases lock
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails.
    pub async fn append_ledger_event<T: CatalogEventPayload + serde::Serialize + Sync>(
        &self,
        _guard: &LockGuard<dyn StorageBackend>,
        domain: CatalogDomain,
        payload: &T,
        source: &str,
    ) -> Result<EventId> {
        let event_id = EventId::generate();
        let key = LedgerKey::event(domain, &event_id.to_string());

        let idempotency_key =
            CatalogEvent::<()>::generate_idempotency_key(T::EVENT_TYPE, T::EVENT_VERSION, payload)
                .map_err(|e| CatalogError::Serialization {
                    message: format!("failed to generate idempotency key: {e}"),
                })?;

        let envelope = CatalogEvent {
            event_type: T::EVENT_TYPE.to_string(),
            event_version: T::EVENT_VERSION,
            idempotency_key,
            occurred_at: Utc::now(),
            source: source.to_string(),
            trace_id: None,
            sequence_position: None, // Tier-1 doesn't need sequence positions
            payload,
        };

        envelope
            .validate()
            .map_err(|e| CatalogError::InvariantViolation {
                message: format!("invalid event envelope: {e}"),
            })?;

        let json =
            serde_json::to_vec_pretty(&envelope).map_err(|e| CatalogError::Serialization {
                message: format!("failed to serialize event: {e}"),
            })?;

        // Use DoesNotExist for append-only semantics
        match self.storage.put_ledger(&key, Bytes::from(json)).await? {
            WriteResult::Success { .. } => Ok(event_id),
            WriteResult::PreconditionFailed { .. } => {
                // Event already exists - this is fine for idempotency
                tracing::debug!(event_id = %event_id, "duplicate ledger event (already exists)");
                Ok(event_id)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn update_inner<F>(
        &self,
        guard: &LockGuard<dyn StorageBackend>,
        update_fn: &mut F,
    ) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        let writer_epoch = guard.fencing_token().sequence();

        for attempt in 1..=self.cas_max_retries {
            let mut root: RootManifest = self.read_json(CatalogPaths::ROOT_MANIFEST).await?;
            root.normalize_paths();

            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
            let pointer_meta = self.storage.head_raw(&pointer_path).await?;
            let (pointer_expected_version, pointer_parent_hash, previous_manifest_path, prev_bytes) =
                if let Some(meta) = pointer_meta {
                    let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
                    let pointer: DomainManifestPointer = serde_json::from_slice(&pointer_bytes)
                        .map_err(|e| CatalogError::Serialization {
                            message: format!("parse JSON at {pointer_path}: {e}"),
                        })?;

                    if writer_epoch < pointer.epoch {
                        return Err(CatalogError::PreconditionFailed {
                            message: format!(
                                "stale epoch: writer epoch {writer_epoch} is behind pointer epoch {}",
                                pointer.epoch
                            ),
                        });
                    }

                    (
                        Some(meta.version),
                        Some(compute_manifest_hash(&pointer_bytes)),
                        pointer.manifest_path.clone(),
                        self.storage.get_raw(&pointer.manifest_path).await?,
                    )
                } else {
                    let prev_bytes = self.storage.get_raw(&root.catalog_manifest_path).await?;
                    (None, None, root.catalog_manifest_path.clone(), prev_bytes)
                };

            let mut catalog: CatalogDomainManifest =
                serde_json::from_slice(&prev_bytes).map_err(|e| CatalogError::Serialization {
                    message: format!("parse JSON at {previous_manifest_path}: {e}"),
                })?;
            let prev_raw_hash = compute_manifest_hash(&prev_bytes);
            let prev_catalog = catalog.clone();

            update_fn(&mut catalog)?;

            catalog.updated_at = Utc::now();
            catalog.parent_hash = Some(prev_raw_hash.clone());
            catalog.fencing_token = Some(writer_epoch);
            catalog.epoch = writer_epoch;
            catalog.previous_manifest_path = Some(previous_manifest_path.clone());
            catalog.writer_session_id = Some(Ulid::new().to_string());
            let commit_ulid = next_commit_ulid(prev_catalog.commit_ulid.as_deref())?;
            catalog.commit_ulid = Some(commit_ulid.clone());
            catalog.manifest_id = next_manifest_id(&prev_catalog.manifest_id)
                .map_err(|message| CatalogError::InvariantViolation { message })?;

            catalog
                .validate_succession(&prev_catalog, &prev_raw_hash)
                .map_err(|message| CatalogError::InvariantViolation { message })?;

            let commit = self
                .build_commit_record(&prev_catalog, &catalog, &commit_ulid)
                .await?;
            catalog.last_commit_id = Some(commit.commit_id.clone());

            let catalog_bytes = json_bytes(&catalog)?;
            let snapshot_manifest_path = CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &catalog.manifest_id,
            );

            let pointer = DomainManifestPointer {
                manifest_id: catalog.manifest_id.clone(),
                manifest_path: snapshot_manifest_path.clone(),
                epoch: writer_epoch,
                parent_pointer_hash: pointer_parent_hash.clone(),
                updated_at: Utc::now(),
            };
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
            match publish_snapshot_pointer_transaction(
                &self.storage,
                &snapshot_manifest_path,
                catalog_bytes.clone(),
                &pointer_path,
                json_bytes(&pointer)?,
                pointer_expected_version.as_deref(),
                Some((&root.catalog_manifest_path, catalog_bytes.clone())),
                SnapshotPointerDurability::Visible,
                async { Ok(()) },
            )
            .await
            {
                Ok(SnapshotPointerPublishOutcome::Visible { .. }) => {
                    self.persist_commit_record(&commit).await?;
                    return Ok(commit);
                }
                Ok(SnapshotPointerPublishOutcome::PersistedNotVisible) => {
                    return Err(CatalogError::InvariantViolation {
                        message:
                            "unexpected persisted-not-visible outcome in visible durability mode"
                                .to_string(),
                    });
                }
                Err(arco_core::Error::PreconditionFailed { .. }) => {
                    if attempt == self.cas_max_retries {
                        return Err(CatalogError::PreconditionFailed {
                            message: "manifest update lost CAS race after max retries".into(),
                        });
                    }
                    crate::metrics::record_cas_retry("catalog_manifest_pointer");
                }
                Err(e) => return Err(CatalogError::from(e)),
            }
        }

        Err(CatalogError::InvariantViolation {
            message: "unreachable: CAS retry loop exhausted".into(),
        })
    }

    async fn ensure_json_exists<T>(&self, path: &str, value: &T) -> Result<()>
    where
        T: serde::Serialize + Sync,
    {
        let bytes = json_bytes(value)?;
        match self
            .storage
            .put_raw(path, bytes, WritePrecondition::DoesNotExist)
            .await?
        {
            WriteResult::PreconditionFailed { .. } | WriteResult::Success { .. } => Ok(()),
        }
    }

    async fn read_json<T>(&self, path: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let bytes = self.storage.get_raw(path).await?;
        serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
            message: format!("parse JSON at {path}: {e}"),
        })
    }

    /// Builds a commit record for an update operation.
    ///
    /// If the previous manifest has a `last_commit_id`, this method loads that
    /// commit record from storage and computes its hash for the tamper-evident chain.
    async fn build_commit_record(
        &self,
        prev: &CatalogDomainManifest,
        next: &CatalogDomainManifest,
        commit_id: &str,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&json_vec(next)?);
        let prev_commit_id = prev.last_commit_id.clone();

        // Load and hash previous commit for tamper-evident chain
        let prev_commit_hash = match &prev_commit_id {
            Some(id) => {
                let path = CatalogPaths::commit(CatalogDomain::Catalog, id);
                match self.storage.get_raw(&path).await {
                    Ok(bytes) => {
                        let record: CommitRecord = serde_json::from_slice(&bytes).map_err(|e| {
                            CatalogError::Serialization {
                                message: format!("deserialize commit record '{path}': {e}"),
                            }
                        })?;
                        Some(record.compute_hash())
                    }
                    Err(arco_core::Error::NotFound(_)) => {
                        // First commit or missing record - acceptable edge case
                        None
                    }
                    Err(e) => return Err(CatalogError::from(e)),
                }
            }
            None => None,
        };

        Ok(CommitRecord {
            commit_id: commit_id.to_string(),
            prev_commit_id,
            prev_commit_hash,
            operation: "Update".into(),
            payload_hash,
            created_at: Utc::now(),
        })
    }

    async fn persist_commit_record(&self, commit: &CommitRecord) -> Result<()> {
        let bytes = json_bytes(commit)?;
        let key = CommitKey::record(CatalogDomain::Catalog, &commit.commit_id);
        match self.storage.put_commit(&key, bytes).await? {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(CatalogError::PreconditionFailed {
                message: format!("commit already exists: {}", commit.commit_id),
            }),
        }
    }
}

fn next_commit_ulid(previous: Option<&str>) -> Result<String> {
    let candidate = Ulid::new();

    let Some(previous) = previous else {
        return Ok(candidate.to_string());
    };

    let previous = Ulid::from_string(previous).map_err(|e| CatalogError::InvariantViolation {
        message: format!("invalid previous commit_ulid '{previous}': {e}"),
    })?;

    if candidate > previous {
        return Ok(candidate.to_string());
    }

    let next = previous
        .increment()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: "commit_ulid overflow while generating monotonic successor".to_string(),
        })?;
    Ok(next.to_string())
}

fn json_vec<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| CatalogError::Serialization {
        message: format!("serialize JSON: {e}"),
    })
}

fn json_bytes<T: serde::Serialize>(value: &T) -> Result<Bytes> {
    Ok(Bytes::from(json_vec(value)?))
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arco_core::Result as CoreResult;
    use arco_core::storage::{MemoryBackend, ObjectMeta};
    use serde::de::DeserializeOwned;
    use std::ops::Range;

    fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse json: {e}"),
        })
    }

    #[derive(Debug)]
    struct HookedBackend {
        inner: MemoryBackend,
        inject_once: AtomicBool,
    }

    impl HookedBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                inject_once: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for HookedBackend {
        async fn get(&self, path: &str) -> CoreResult<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> CoreResult<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> CoreResult<WriteResult> {
            // Inject a no-op write once to force a CAS conflict.
            if matches!(&precondition, WritePrecondition::MatchesVersion(_))
                && path.ends_with(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
                && self.inject_once.swap(false, Ordering::SeqCst)
            {
                let current = self.inner.get(path).await?;
                let _ = self
                    .inner
                    .put(path, current, WritePrecondition::None)
                    .await?;
            }

            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> CoreResult<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> CoreResult<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> CoreResult<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> CoreResult<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn test_initialize_catalog_creates_required_files() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        let root_bytes = storage.get_raw(CatalogPaths::ROOT_MANIFEST).await?;
        let mut root: RootManifest = parse_json(&root_bytes)?;
        root.normalize_paths();
        assert_eq!(root.version, 1);

        let catalog_bytes = storage.get_raw(&root.catalog_manifest_path).await?;
        let catalog: CatalogDomainManifest = parse_json(&catalog_bytes)?;
        assert_eq!(catalog.snapshot_version, 0);

        let lineage_bytes = storage.get_raw(&root.lineage_manifest_path).await?;
        let lineage: LineageManifest = parse_json(&lineage_bytes)?;
        assert_eq!(lineage.snapshot_version, 0);

        let exec_bytes = storage.get_raw(&root.executions_manifest_path).await?;
        let exec: ExecutionsManifest = parse_json(&exec_bytes)?;
        assert_eq!(exec.watermark_version, 0);

        let search_bytes = storage.get_raw(&root.search_manifest_path).await?;
        let search: SearchManifest = parse_json(&search_bytes)?;
        assert_eq!(search.snapshot_version, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_initialize_idempotent() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage);

        writer.initialize().await?;
        writer.initialize().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_update_with_cas() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        let commit = writer
            .update(|manifest| {
                manifest.snapshot_version = 1;
                manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
                Ok(())
            })
            .await?;

        assert_eq!(commit.operation, "Update");

        let core_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
            .await?;
        let core: CatalogDomainManifest = parse_json(&core_bytes)?;
        assert_eq!(core.snapshot_version, 1);
        assert_eq!(
            core.snapshot_path,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_conflict_retries_and_succeeds() -> Result<()> {
        let backend = Arc::new(HookedBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone()).with_cas_retries(5);

        writer.initialize().await?;

        writer
            .update(|manifest| {
                manifest.snapshot_version += 1;
                Ok(())
            })
            .await?;

        let core_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
            .await?;
        let core: CatalogDomainManifest = parse_json(&core_bytes)?;
        assert_eq!(core.snapshot_version, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_writes_pointer_and_snapshot_manifest() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        writer
            .update(|manifest| {
                manifest.snapshot_version = 1;
                manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
                Ok(())
            })
            .await?;

        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_bytes = storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        assert_eq!(pointer.manifest_id, "00000000000000000001");

        let snapshot_bytes = storage.get_raw(&pointer.manifest_path).await?;
        let snapshot: CatalogDomainManifest = parse_json(&snapshot_bytes)?;
        assert_eq!(snapshot.manifest_id, "00000000000000000001");
        assert_eq!(snapshot.snapshot_version, 1);

        // Compatibility shim: legacy mutable manifest path remains readable.
        let legacy_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
            .await?;
        let legacy: CatalogDomainManifest = parse_json(&legacy_bytes)?;
        assert_eq!(legacy.snapshot_version, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_chain_has_prev_hash() -> Result<()> {
        // Verifies the tamper-evident audit chain links commits together.
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        // First update - no previous commit to link
        let commit1 = writer
            .update(|manifest| {
                manifest.snapshot_version = 1;
                Ok(())
            })
            .await?;
        assert!(commit1.prev_commit_id.is_none());
        assert!(commit1.prev_commit_hash.is_none());

        // Second update - should link to first commit
        let commit2 = writer
            .update(|manifest| {
                manifest.snapshot_version = 2;
                Ok(())
            })
            .await?;
        assert_eq!(commit2.prev_commit_id, Some(commit1.commit_id.clone()));
        assert!(
            commit2.prev_commit_hash.is_some(),
            "second commit must have prev_commit_hash"
        );

        // Third update - should link to second commit
        let commit3 = writer
            .update(|manifest| {
                manifest.snapshot_version = 3;
                Ok(())
            })
            .await?;
        assert_eq!(commit3.prev_commit_id, Some(commit2.commit_id.clone()));
        assert!(
            commit3.prev_commit_hash.is_some(),
            "third commit must have prev_commit_hash"
        );

        // Verify chain integrity: commit3.prev_commit_hash should be SHA256 of commit2
        let expected_hash = commit2.compute_hash();
        assert_eq!(commit3.prev_commit_hash, Some(expected_hash));

        Ok(())
    }
}
