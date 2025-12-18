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

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

use crate::error::{CatalogError, Result};
use crate::lock::LockGuard;
use crate::lock::{DEFAULT_LOCK_TTL, DEFAULT_MAX_RETRIES, DistributedLock};
use crate::manifest::{
    CatalogDomainManifest, CatalogManifest, CommitRecord, ExecutionsManifest, LineageManifest,
    RootManifest, SearchManifest, SnapshotFile, SnapshotInfo,
};
use crate::parquet_util;
use crate::state::{CatalogState, LineageState};

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

        let result = self.update_inner(&mut update_fn).await;

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
        _guard: &LockGuard<dyn StorageBackend>,
        mut update_fn: F,
    ) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        self.update_inner(&mut update_fn).await
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

    /// Writes a new catalog snapshot (namespaces/tables/columns) for the given version.
    ///
    /// Snapshot files are written before the manifest is updated (atomic publish).
    /// Snapshot paths are versioned; overwriting is allowed for crash recovery when the
    /// manifest CAS fails (the snapshot is not visible until publish succeeds).
    ///
    /// # Errors
    ///
    /// Returns an error if serializing the snapshot or writing to storage fails.
    pub async fn write_catalog_snapshot(
        &self,
        _guard: &LockGuard<dyn StorageBackend>,
        version: u64,
        state: &CatalogState,
    ) -> Result<SnapshotInfo> {
        let snapshot_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, version);

        let namespaces_bytes = parquet_util::write_namespaces(&state.namespaces)?;
        let tables_bytes = parquet_util::write_tables(&state.tables)?;
        let columns_bytes = parquet_util::write_columns(&state.columns)?;

        let ns_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "namespaces.parquet");
        let tables_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "tables.parquet");
        let cols_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "columns.parquet");

        let _ = self
            .storage
            .put_raw(&ns_path, namespaces_bytes.clone(), WritePrecondition::None)
            .await?;
        let _ = self
            .storage
            .put_raw(&tables_path, tables_bytes.clone(), WritePrecondition::None)
            .await?;
        let _ = self
            .storage
            .put_raw(&cols_path, columns_bytes.clone(), WritePrecondition::None)
            .await?;

        let mut info = SnapshotInfo::new(version, snapshot_dir);
        info.add_file(SnapshotFile {
            path: "namespaces.parquet".to_string(),
            checksum_sha256: sha256_hex(&namespaces_bytes),
            byte_size: namespaces_bytes.len() as u64,
            row_count: state.namespaces.len() as u64,
            position_range: None,
        });
        info.add_file(SnapshotFile {
            path: "tables.parquet".to_string(),
            checksum_sha256: sha256_hex(&tables_bytes),
            byte_size: tables_bytes.len() as u64,
            row_count: state.tables.len() as u64,
            position_range: None,
        });
        info.add_file(SnapshotFile {
            path: "columns.parquet".to_string(),
            checksum_sha256: sha256_hex(&columns_bytes),
            byte_size: columns_bytes.len() as u64,
            row_count: state.columns.len() as u64,
            position_range: None,
        });

        Ok(info)
    }

    /// Writes a new lineage snapshot (`lineage_edges.parquet`) for the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if serializing the snapshot or writing to storage fails.
    pub async fn write_lineage_snapshot(
        &self,
        _guard: &LockGuard<dyn StorageBackend>,
        version: u64,
        state: &LineageState,
    ) -> Result<SnapshotInfo> {
        let snapshot_dir = CatalogPaths::snapshot_dir(CatalogDomain::Lineage, version);
        let bytes = parquet_util::write_lineage_edges(&state.edges)?;
        let path =
            CatalogPaths::snapshot_file(CatalogDomain::Lineage, version, "lineage_edges.parquet");

        let _ = self
            .storage
            .put_raw(&path, bytes.clone(), WritePrecondition::None)
            .await?;

        let mut info = SnapshotInfo::new(version, snapshot_dir);
        info.add_file(SnapshotFile {
            path: "lineage_edges.parquet".to_string(),
            checksum_sha256: sha256_hex(&bytes),
            byte_size: bytes.len() as u64,
            row_count: state.edges.len() as u64,
            position_range: None,
        });
        Ok(info)
    }

    async fn update_inner<F>(&self, update_fn: &mut F) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        for attempt in 1..=self.cas_max_retries {
            let mut root: RootManifest = self.read_json(CatalogPaths::ROOT_MANIFEST).await?;
            root.normalize_paths();

            let (mut catalog, catalog_version): (CatalogDomainManifest, String) = self
                .read_json_with_version(&root.catalog_manifest_path)
                .await?;
            let prev_catalog = catalog.clone();

            update_fn(&mut catalog)?;

            catalog.updated_at = Utc::now();

            let commit = self.build_commit_record(&prev_catalog, &catalog).await?;
            catalog.last_commit_id = Some(commit.commit_id.clone());

            let catalog_bytes = json_bytes(&catalog)?;
            match self
                .storage
                .put_raw(
                    &root.catalog_manifest_path,
                    catalog_bytes,
                    WritePrecondition::MatchesVersion(catalog_version),
                )
                .await?
            {
                WriteResult::Success { .. } => {
                    self.persist_commit_record(&commit).await?;
                    return Ok(commit);
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(CatalogError::PreconditionFailed {
                            message: "manifest update lost CAS race after max retries".into(),
                        });
                    }

                    // Another writer updated the manifest between read and write.
                    // Retry from fresh state.
                    continue;
                }
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

    async fn read_json_with_version<T>(&self, path: &str) -> Result<(T, String)>
    where
        T: serde::de::DeserializeOwned,
    {
        let meta = self
            .storage
            .head_raw(path)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                entity: "manifest".to_string(),
                name: path.to_string(),
            })?;

        let value = self.read_json(path).await?;
        Ok((value, meta.version))
    }

    /// Builds a commit record for an update operation.
    ///
    /// If the previous manifest has a `last_commit_id`, this method loads that
    /// commit record from storage and computes its hash for the tamper-evident chain.
    async fn build_commit_record(
        &self,
        prev: &CatalogDomainManifest,
        next: &CatalogDomainManifest,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&json_vec(next)?);
        let prev_commit_id = prev.last_commit_id.clone();

        // Load and hash previous commit for tamper-evident chain
        let prev_commit_hash = match &prev_commit_id {
            Some(id) => {
                let path = CatalogPaths::commit(CatalogDomain::Catalog, id);
                match self.storage.get_raw(&path).await {
                    Ok(bytes) => Some(sha256_prefixed(&bytes)),
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
            commit_id: Ulid::new().to_string(),
            prev_commit_id,
            prev_commit_hash,
            operation: "Update".into(),
            payload_hash,
            created_at: Utc::now(),
        })
    }

    async fn persist_commit_record(&self, commit: &CommitRecord) -> Result<()> {
        let path = CatalogPaths::commit(CatalogDomain::Catalog, &commit.commit_id);
        let bytes = json_bytes(commit)?;
        match self
            .storage
            .put_raw(&path, bytes, WritePrecondition::DoesNotExist)
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(CatalogError::PreconditionFailed {
                message: format!("commit already exists: {}", commit.commit_id),
            }),
        }
    }
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

fn sha256_hex(bytes: &Bytes) -> String {
    let hash = Sha256::digest(bytes);
    hex::encode(hash)
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
        let commit2_path = CatalogPaths::commit(CatalogDomain::Catalog, &commit2.commit_id);
        let commit2_bytes = storage.get_raw(&commit2_path).await?;
        let expected_hash = format!("sha256:{}", hex::encode(Sha256::digest(&commit2_bytes)));
        assert_eq!(commit3.prev_commit_hash, Some(expected_hash));

        Ok(())
    }
}
