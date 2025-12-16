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

use arco_core::ScopedStorage;
use arco_core::error::{Error, Result};
use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::lock::{DEFAULT_LOCK_TTL, DEFAULT_MAX_RETRIES, DistributedLock};
use crate::manifest::{
    CatalogManifest, CommitRecord, CoreManifest, ExecutionManifest, GovernanceManifest,
    LineageManifest, RootManifest, paths,
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
    /// The lock path is derived from [`ScopedStorage::core_lock_path`].
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        let backend = storage.backend().clone();
        let lock_path = storage.core_lock_path();
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
    /// - `manifests/root.manifest.json`
    /// - `manifests/core.manifest.json`
    /// - `manifests/execution.manifest.json`
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
            .await?;

        let root = RootManifest::new();
        self.ensure_json_exists(paths::ROOT_MANIFEST, &root).await?;

        self.ensure_json_exists(paths::CORE_MANIFEST, &CoreManifest::new())
            .await?;
        self.ensure_json_exists(paths::EXECUTION_MANIFEST, &ExecutionManifest::new())
            .await?;

        guard.release().await
    }

    /// Reads the current catalog manifest by loading domain manifests.
    ///
    /// # Errors
    ///
    /// Returns an error if any required manifest is missing or cannot be parsed.
    pub async fn read_manifest(&self) -> Result<CatalogManifest> {
        let root: RootManifest = self.read_json(paths::ROOT_MANIFEST).await?;
        let core: CoreManifest = self.read_json(&root.core_manifest_path).await?;
        let execution: ExecutionManifest = self.read_json(&root.execution_manifest_path).await?;

        let lineage: Option<LineageManifest> = match root.lineage_manifest_path.as_deref() {
            Some(path) => Some(self.read_json(path).await?),
            None => None,
        };

        let governance: Option<GovernanceManifest> = match root.governance_manifest_path.as_deref()
        {
            Some(path) => Some(self.read_json(path).await?),
            None => None,
        };

        Ok(CatalogManifest {
            version: root.version,
            core,
            execution,
            lineage,
            governance,
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
        F: FnMut(&mut CatalogManifest) -> Result<()>,
    {
        let guard = self
            .lock
            .acquire_with_operation(self.lock_ttl, self.lock_max_retries, Some("Update".into()))
            .await?;

        let result = self.update_inner(&mut update_fn).await;

        match result {
            Ok(commit) => {
                guard.release().await?;
                Ok(commit)
            }
            Err(e) => Err(e),
        }
    }

    async fn update_inner<F>(&self, update_fn: &mut F) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogManifest) -> Result<()>,
    {
        for attempt in 1..=self.cas_max_retries {
            let root: RootManifest = self.read_json(paths::ROOT_MANIFEST).await?;

            let (core, core_version): (CoreManifest, String) = self
                .read_json_with_version(&root.core_manifest_path)
                .await?;
            let execution: ExecutionManifest =
                self.read_json(&root.execution_manifest_path).await?;

            let prev_core = core.clone();
            let mut manifest = CatalogManifest {
                version: root.version,
                core,
                execution,
                lineage: None,
                governance: None,
                created_at: root.updated_at,
                updated_at: Utc::now(),
            };

            update_fn(&mut manifest)?;

            let now = Utc::now();
            manifest.core.updated_at = now;
            manifest.updated_at = now;

            let commit = self.build_commit_record(&prev_core, &manifest.core).await?;
            manifest.core.last_commit_id = Some(commit.commit_id.clone());

            let core_bytes = json_bytes(&manifest.core)?;
            match self
                .storage
                .put_raw(
                    &root.core_manifest_path,
                    core_bytes,
                    WritePrecondition::MatchesVersion(core_version),
                )
                .await?
            {
                WriteResult::Success { .. } => {
                    self.persist_commit_record(&commit).await?;
                    return Ok(commit);
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Error::PreconditionFailed {
                            message: "manifest update lost CAS race after max retries".into(),
                        });
                    }

                    // Another writer updated the manifest between read and write.
                    // Retry from fresh state.
                    continue;
                }
            }
        }

        Err(Error::Internal {
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
        serde_json::from_slice(&bytes).map_err(|e| Error::Serialization {
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
            .ok_or_else(|| Error::NotFound(format!("manifest not found: {path}")))?;

        let value = self.read_json(path).await?;
        Ok((value, meta.version))
    }

    /// Builds a commit record for an update operation.
    ///
    /// If the previous manifest has a `last_commit_id`, this method loads that
    /// commit record from storage and computes its hash for the tamper-evident chain.
    async fn build_commit_record(
        &self,
        prev: &CoreManifest,
        next: &CoreManifest,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&json_vec(next)?);
        let prev_commit_id = prev.last_commit_id.clone();

        // Load and hash previous commit for tamper-evident chain
        let prev_commit_hash = match &prev_commit_id {
            Some(id) => {
                let path = format!("core/commits/{id}.json");
                match self.storage.get_raw(&path).await {
                    Ok(bytes) => Some(sha256_prefixed(&bytes)),
                    Err(Error::NotFound(_)) => {
                        // First commit or missing record - acceptable edge case
                        None
                    }
                    Err(e) => return Err(e),
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
        let path = format!("core/commits/{}.json", commit.commit_id);
        let bytes = json_bytes(commit)?;
        match self
            .storage
            .put_raw(&path, bytes, WritePrecondition::DoesNotExist)
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(Error::PreconditionFailed {
                message: format!("commit already exists: {}", commit.commit_id),
            }),
        }
    }
}

fn json_vec<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::Serialization {
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

    use arco_core::storage::{MemoryBackend, ObjectMeta};
    use std::ops::Range;
    use serde::de::DeserializeOwned;

    fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| Error::Serialization {
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
        async fn get(&self, path: &str) -> Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> Result<WriteResult> {
            // Inject a no-op write once to force a CAS conflict.
            if matches!(&precondition, WritePrecondition::MatchesVersion(_))
                && path.ends_with(paths::CORE_MANIFEST)
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

        async fn delete(&self, path: &str) -> Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn test_initialize_catalog_creates_required_files() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        let root_bytes = storage.get_raw(paths::ROOT_MANIFEST).await?;
        let root: RootManifest = parse_json(&root_bytes)?;
        assert_eq!(root.version, 1);

        let core_bytes = storage.get_raw(paths::CORE_MANIFEST).await?;
        let core: CoreManifest = parse_json(&core_bytes)?;
        assert_eq!(core.snapshot_version, 0);

        let exec_bytes = storage.get_raw(paths::EXECUTION_MANIFEST).await?;
        let exec: ExecutionManifest = parse_json(&exec_bytes)?;
        assert_eq!(exec.watermark_version, 0);

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
                manifest.core.snapshot_version = 1;
                manifest.core.snapshot_path = "core/snapshots/v1/".into();
                Ok(())
            })
            .await
            ?;

        assert_eq!(commit.operation, "Update");

        let core_bytes = storage.get_raw(paths::CORE_MANIFEST).await?;
        let core: CoreManifest = parse_json(&core_bytes)?;
        assert_eq!(core.snapshot_version, 1);
        assert_eq!(core.snapshot_path, "core/snapshots/v1/");

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
                manifest.core.snapshot_version += 1;
                Ok(())
            })
            .await
            ?;

        let core_bytes = storage.get_raw(paths::CORE_MANIFEST).await?;
        let core: CoreManifest = parse_json(&core_bytes)?;
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
                manifest.core.snapshot_version = 1;
                Ok(())
            })
            .await
            ?;
        assert!(commit1.prev_commit_id.is_none());
        assert!(commit1.prev_commit_hash.is_none());

        // Second update - should link to first commit
        let commit2 = writer
            .update(|manifest| {
                manifest.core.snapshot_version = 2;
                Ok(())
            })
            .await
            ?;
        assert_eq!(commit2.prev_commit_id, Some(commit1.commit_id.clone()));
        assert!(
            commit2.prev_commit_hash.is_some(),
            "second commit must have prev_commit_hash"
        );

        // Third update - should link to second commit
        let commit3 = writer
            .update(|manifest| {
                manifest.core.snapshot_version = 3;
                Ok(())
            })
            .await
            ?;
        assert_eq!(commit3.prev_commit_id, Some(commit2.commit_id.clone()));
        assert!(
            commit3.prev_commit_hash.is_some(),
            "third commit must have prev_commit_hash"
        );

        // Verify chain integrity: commit3.prev_commit_hash should be SHA256 of commit2
        let commit2_path = format!("core/commits/{}.json", commit2.commit_id);
        let commit2_bytes = storage.get_raw(&commit2_path).await?;
        let expected_hash = format!("sha256:{}", hex::encode(Sha256::digest(&commit2_bytes)));
        assert_eq!(commit3.prev_commit_hash, Some(expected_hash));

        Ok(())
    }
}
