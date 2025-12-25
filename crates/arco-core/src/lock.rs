//! Distributed lock implementation for Tier 1 catalog operations.
//!
//! This module provides a distributed lock using cloud object storage
//! as the coordination point. It uses:
//! - **CAS (Compare-and-Swap)**: Atomic acquisition via preconditioned writes
//! - **TTL (Time-to-Live)**: Automatic expiry to prevent deadlocks
//! - **Retry with backoff**: Handles transient conflicts gracefully
//!
//! # How It Works
//!
//! 1. Lock acquisition writes a lock file with the holder's ID and expiry time
//! 2. The write uses `DoesNotExist` precondition - only one writer can succeed
//! 3. If lock exists, check if expired - if so, take it over
//! 4. Lock release deletes the lock file (or leaves for TTL cleanup)
//!
//! # Example
//!
//! ```rust,ignore
//! let lock = DistributedLock::new(storage.clone(), "catalog.lock");
//!
//! // Acquire lock with 30s TTL and 5 retries
//! let guard = lock.acquire(Duration::from_secs(30), 5).await?;
//!
//! // Critical section - only one holder at a time
//! // ... update catalog ...
//!
//! // Release lock (or drop guard for automatic release)
//! guard.release().await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::publish::{FencingToken, PermitIssuer};
use crate::storage::{StorageBackend, WritePrecondition, WriteResult};

/// Default lock TTL (30 seconds).
pub const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(30);

/// Default maximum retry attempts for lock acquisition.
pub const DEFAULT_MAX_RETRIES: u32 = 5;

/// Base backoff duration for retries.
const BACKOFF_BASE: Duration = Duration::from_millis(100);

/// Maximum backoff duration.
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Lock file contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LockInfo {
    /// Unique lock holder ID.
    pub holder_id: String,

    /// When the lock expires.
    pub expires_at: DateTime<Utc>,

    /// When the lock was acquired.
    pub acquired_at: DateTime<Utc>,

    /// Monotonically increasing sequence number for distributed fencing.
    ///
    /// This value is incremented on each lock acquisition and provides the
    /// fencing token that allows detection of stale lock holders.
    #[serde(default)]
    pub sequence_number: u64,

    /// Optional description of the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

impl LockInfo {
    /// Creates a new lock info with the given holder ID, TTL, and sequence number.
    #[must_use]
    pub fn new(holder_id: impl Into<String>, ttl: Duration, sequence_number: u64) -> Self {
        let now = Utc::now();
        Self {
            holder_id: holder_id.into(),
            expires_at: now
                + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::seconds(30)),
            acquired_at: now,
            sequence_number,
            operation: None,
        }
    }

    /// Returns whether this lock has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    /// Returns the remaining TTL, or zero if expired.
    #[must_use]
    pub fn remaining_ttl(&self) -> Duration {
        let remaining = self.expires_at - Utc::now();
        let millis = remaining.num_milliseconds();
        if millis <= 0 {
            Duration::ZERO
        } else {
            Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
        }
    }
}

/// A distributed lock backed by object storage.
///
/// Uses CAS operations to ensure only one writer can hold the lock at a time.
pub struct DistributedLock<S: StorageBackend + ?Sized> {
    storage: Arc<S>,
    lock_path: String,
    holder_id: String,
}

// Manual Clone implementation to avoid requiring S: Clone
// (Arc<S> can be cloned regardless of whether S is Clone)
impl<S: StorageBackend + ?Sized> Clone for DistributedLock<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            lock_path: self.lock_path.clone(),
            holder_id: self.holder_id.clone(),
        }
    }
}

impl<S: StorageBackend + ?Sized> DistributedLock<S> {
    /// Creates a new distributed lock.
    ///
    /// Each lock instance gets a unique holder ID for identification.
    #[must_use]
    pub fn new(storage: Arc<S>, lock_path: impl Into<String>) -> Self {
        Self {
            storage,
            lock_path: lock_path.into(),
            holder_id: Ulid::new().to_string(),
        }
    }

    /// Returns the holder ID for this lock instance.
    #[must_use]
    pub fn holder_id(&self) -> &str {
        &self.holder_id
    }

    /// Attempts to acquire the lock with the given TTL.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock could not be acquired after all retries.
    pub async fn acquire(&self, ttl: Duration, max_retries: u32) -> Result<LockGuard<S>> {
        self.acquire_with_operation(ttl, max_retries, None).await
    }

    /// Attempts to acquire the lock with operation description.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock could not be acquired after all retries.
    pub async fn acquire_with_operation(
        &self,
        ttl: Duration,
        max_retries: u32,
        operation: Option<String>,
    ) -> Result<LockGuard<S>> {
        let mut attempts = 0;
        let mut backoff = BACKOFF_BASE;

        loop {
            match self.try_acquire(ttl, operation.clone()).await {
                Ok(guard) => return Ok(guard),
                Err(LockError::AlreadyHeld(holder)) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(Error::PreconditionFailed {
                            message: format!("lock held by {holder} after {max_retries} retries",),
                        });
                    }

                    // Exponential backoff with jitter
                    let jitter = Duration::from_millis(rand_jitter());
                    let delay = backoff.min(BACKOFF_MAX) + jitter;
                    tokio::time::sleep(delay).await;
                    backoff = backoff.saturating_mul(2);
                }
                Err(LockError::Storage(e)) => return Err(e),
            }
        }
    }

    /// Attempts to acquire the lock once (no retries).
    async fn try_acquire(
        &self,
        ttl: Duration,
        operation: Option<String>,
    ) -> std::result::Result<LockGuard<S>, LockError> {
        // First, try to create lock with DoesNotExist precondition
        // New locks start with sequence_number = 1
        let mut lock_info = LockInfo::new(&self.holder_id, ttl, 1);
        lock_info.operation.clone_from(&operation);

        let lock_bytes = Bytes::from(serde_json::to_vec(&lock_info).map_err(|e| {
            LockError::Storage(Error::Internal {
                message: format!("serialize lock: {e}"),
            })
        })?);

        match self
            .storage
            .put(&self.lock_path, lock_bytes, WritePrecondition::DoesNotExist)
            .await
            .map_err(LockError::Storage)?
        {
            WriteResult::Success { version } => {
                return Ok(LockGuard {
                    storage: self.storage.clone(),
                    lock_path: self.lock_path.clone(),
                    holder_id: self.holder_id.clone(),
                    version,
                    fencing_token: FencingToken::new(1),
                    released: false,
                });
            }
            WriteResult::PreconditionFailed { .. } => {
                // Lock exists - check if expired
            }
        }

        // Lock exists, check if we can take it over.
        // CRITICAL: Get version FIRST, then read contents. This ensures the
        // expiry decision is bound to the same version used for CAS.
        // If another writer takes over between HEAD and GET, we'll either:
        // - See their non-expired lock → retry normally
        // - CAS will fail (version changed) → retry
        let meta = self
            .storage
            .head(&self.lock_path)
            .await
            .map_err(LockError::Storage)?;

        let Some(meta) = meta else {
            // Lock disappeared between our DoesNotExist check and now - retry
            return Err(LockError::AlreadyHeld("race".into()));
        };

        let existing = self.read_lock().await.map_err(LockError::Storage)?;

        match existing {
            Some(info) if info.is_expired() => {
                // Expired lock - try to take it over using version from HEAD above.
                // Increment the sequence number for distributed fencing.
                let new_sequence = info.sequence_number.saturating_add(1);
                let mut new_lock_info = LockInfo::new(&self.holder_id, ttl, new_sequence);
                new_lock_info.operation = operation;

                let new_lock_bytes =
                    Bytes::from(serde_json::to_vec(&new_lock_info).map_err(|e| {
                        LockError::Storage(Error::Internal {
                            message: format!("serialize lock: {e}"),
                        })
                    })?);

                // This ensures atomicity: if another writer took over after our HEAD,
                // the CAS will fail and we'll retry.
                match self
                    .storage
                    .put(
                        &self.lock_path,
                        new_lock_bytes,
                        WritePrecondition::MatchesVersion(meta.version),
                    )
                    .await
                    .map_err(LockError::Storage)?
                {
                    WriteResult::Success { version } => Ok(LockGuard {
                        storage: self.storage.clone(),
                        lock_path: self.lock_path.clone(),
                        holder_id: self.holder_id.clone(),
                        version,
                        fencing_token: FencingToken::new(new_sequence),
                        released: false,
                    }),
                    WriteResult::PreconditionFailed { .. } => {
                        // Someone else took it or lock changed - retry
                        Err(LockError::AlreadyHeld("unknown".into()))
                    }
                }
            }
            Some(info) => {
                // Lock is held and not expired
                Err(LockError::AlreadyHeld(info.holder_id))
            }
            None => {
                // Lock disappeared after HEAD - retry from start
                Err(LockError::AlreadyHeld("race".into()))
            }
        }
    }

    /// Reads the current lock info, if any.
    async fn read_lock(&self) -> Result<Option<LockInfo>> {
        match self.storage.get(&self.lock_path).await {
            Ok(data) => {
                let info: LockInfo =
                    serde_json::from_slice(&data).map_err(|e| Error::Internal {
                        message: format!("parse lock: {e}"),
                    })?;
                Ok(Some(info))
            }
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Forcefully breaks an existing lock (admin operation).
    ///
    /// # Warning
    ///
    /// This should only be used for recovery when a lock is known to be stale
    /// but hasn't expired (e.g., crashed holder with long TTL).
    ///
    /// # Errors
    ///
    /// Returns an error if the lock could not be broken.
    pub async fn force_break(&self) -> Result<()> {
        self.storage.delete(&self.lock_path).await
    }

    /// Checks if the lock is currently held (regardless of holder).
    ///
    /// # Errors
    ///
    /// Returns an error if the lock state could not be read.
    pub async fn is_locked(&self) -> Result<bool> {
        Ok(self
            .read_lock()
            .await?
            .is_some_and(|info| !info.is_expired()))
    }

    /// Reads the current lock info without acquiring the lock.
    ///
    /// Intended for validation by infrastructure components (e.g., sync compactor).
    ///
    /// # Errors
    ///
    /// Returns an error if the lock state could not be read.
    pub async fn read_lock_info(&self) -> Result<Option<LockInfo>> {
        self.read_lock().await
    }
}

#[cfg(feature = "sync-compaction")]
/// Creates a permit issuer from a validated fencing token for sync compaction.
///
/// # Safety
///
/// Callers MUST validate `sequence_number` against the current lock state before
/// calling this function. This API is intentionally feature-gated to limit its use.
#[must_use]
pub fn sync_compact_permit_issuer(lock_path: &str, sequence_number: u64) -> PermitIssuer {
    PermitIssuer::from_validated_token(FencingToken::new(sequence_number), lock_path)
}

/// RAII guard for a held lock.
///
/// The lock is automatically released when the guard is dropped.
pub struct LockGuard<S: StorageBackend + ?Sized> {
    storage: Arc<S>,
    lock_path: String,
    holder_id: String,
    /// Opaque version token for CAS operations (multi-cloud compatible).
    version: String,
    /// Fencing token from lock acquisition (for distributed fencing).
    fencing_token: FencingToken,
    released: bool,
}

impl<S: StorageBackend + ?Sized> LockGuard<S> {
    /// Returns the holder ID for this lock.
    #[must_use]
    pub fn holder_id(&self) -> &str {
        &self.holder_id
    }

    /// Returns the version at which the lock was acquired.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the fencing token for this lock acquisition.
    ///
    /// The fencing token is a monotonically increasing value that can be used
    /// to detect stale lock holders. A higher fencing token always takes
    /// precedence over a lower one.
    #[must_use]
    pub fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns a permit issuer for this lock.
    ///
    /// The issuer can be used to create publish permits that carry the
    /// fencing token from this lock acquisition.
    #[must_use]
    pub fn permit_issuer(&self) -> PermitIssuer {
        PermitIssuer::from_validated_token(self.fencing_token, &self.lock_path)
    }

    /// Explicitly releases the lock.
    ///
    /// This is called automatically on drop, but calling explicitly
    /// allows handling release errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock could not be released.
    pub async fn release(mut self) -> Result<()> {
        self.do_release().await
    }

    /// Internal release implementation.
    ///
    /// Uses CAS to write an expired lock record instead of unconditional delete.
    /// This prevents deleting a new holder's lock if takeover happened between
    /// our ownership check and the release operation.
    async fn do_release(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }

        // Read current lock to verify ownership
        if let Some(info) = self.read_lock().await? {
            if info.holder_id == self.holder_id {
                // Create an expired lock record (releases the lock)
                // Preserve sequence_number so next acquisition can increment it
                let expired_info = LockInfo {
                    holder_id: self.holder_id.clone(),
                    expires_at: Utc::now() - chrono::Duration::seconds(1),
                    acquired_at: info.acquired_at,
                    sequence_number: info.sequence_number,
                    operation: None,
                };

                let expired_bytes =
                    Bytes::from(serde_json::to_vec(&expired_info).map_err(|e| {
                        Error::Internal {
                            message: format!("serialize expired lock: {e}"),
                        }
                    })?);

                // CAS write with our version - if another holder took over,
                // this fails and we leave their lock intact.
                //
                // On Success: Leave the expired record in place - next acquire
                // will overwrite it. Deleting here would race: new holder could
                // acquire between our CAS and delete.
                //
                // On PreconditionFailed: Another holder took over - don't touch
                // their lock. This is expected in takeover scenarios.
                let _ = self
                    .storage
                    .put(
                        &self.lock_path,
                        expired_bytes,
                        WritePrecondition::MatchesVersion(self.version.clone()),
                    )
                    .await?;
            }
        }

        self.released = true;
        Ok(())
    }

    /// Reads the current lock info.
    async fn read_lock(&self) -> Result<Option<LockInfo>> {
        match self.storage.get(&self.lock_path).await {
            Ok(data) => {
                let info: LockInfo =
                    serde_json::from_slice(&data).map_err(|e| Error::Internal {
                        message: format!("parse lock: {e}"),
                    })?;
                Ok(Some(info))
            }
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Extends the lock TTL.
    ///
    /// This is useful for long-running operations that need to hold
    /// the lock longer than initially expected.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is no longer held by this guard.
    pub async fn extend(&mut self, additional_ttl: Duration) -> Result<()> {
        let current = self.read_lock().await?;

        match current {
            Some(info) if info.holder_id == self.holder_id => {
                let mut new_info = info;
                new_info.expires_at = Utc::now()
                    + chrono::Duration::from_std(additional_ttl)
                        .unwrap_or(chrono::Duration::seconds(30));

                let lock_bytes =
                    Bytes::from(serde_json::to_vec(&new_info).map_err(|e| Error::Internal {
                        message: format!("serialize lock: {e}"),
                    })?);

                // Use CAS to ensure we still own it
                let meta = self
                    .storage
                    .head(&self.lock_path)
                    .await?
                    .ok_or_else(|| Error::NotFound(self.lock_path.clone()))?;

                match self
                    .storage
                    .put(
                        &self.lock_path,
                        lock_bytes,
                        WritePrecondition::MatchesVersion(meta.version),
                    )
                    .await?
                {
                    WriteResult::Success { version } => {
                        self.version = version;
                        Ok(())
                    }
                    WriteResult::PreconditionFailed { .. } => Err(Error::PreconditionFailed {
                        message: "lock modified by another holder".into(),
                    }),
                }
            }
            Some(_) => Err(Error::PreconditionFailed {
                message: "lock held by different holder".into(),
            }),
            None => Err(Error::NotFound(self.lock_path.clone())),
        }
    }
}

impl<S: StorageBackend + ?Sized> Drop for LockGuard<S> {
    fn drop(&mut self) {
        if !self.released {
            // Best-effort async release in destructor.
            // In practice, prefer calling release() explicitly.
            //
            // Guard against panic when dropped outside a Tokio runtime
            // (e.g., during shutdown or in non-async contexts).
            // If no runtime, TTL will handle eventual cleanup.
            let Ok(handle) = tokio::runtime::Handle::try_current() else {
                // No runtime available - rely on TTL for cleanup
                return;
            };

            let storage = self.storage.clone();
            let path = self.lock_path.clone();
            let holder = self.holder_id.clone();
            let version = self.version.clone();

            handle.spawn(async move {
                // Write expired record via CAS - same approach as do_release().
                // Avoids race where delete could remove a new holder's lock.
                if let Ok(Some(meta)) = storage.head(&path).await {
                    if meta.version == version {
                        if let Ok(data) = storage.get(&path).await {
                            if let Ok(info) = serde_json::from_slice::<LockInfo>(&data) {
                                if info.holder_id == holder {
                                    let expired = LockInfo {
                                        holder_id: holder,
                                        expires_at: Utc::now() - chrono::Duration::seconds(1),
                                        acquired_at: info.acquired_at,
                                        sequence_number: info.sequence_number,
                                        operation: None,
                                    };
                                    if let Ok(bytes) = serde_json::to_vec(&expired) {
                                        let _ = storage
                                            .put(
                                                &path,
                                                Bytes::from(bytes),
                                                WritePrecondition::MatchesVersion(version),
                                            )
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }
}

/// Internal lock acquisition errors.
enum LockError {
    AlreadyHeld(String),
    Storage(Error),
}

/// Generates random jitter for backoff (0-50ms).
fn rand_jitter() -> u64 {
    // Simple linear congruential generator for jitter
    // (avoids full rand dependency for this simple case)
    use std::time::SystemTime;
    let seed = u64::from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
    );
    seed % 50
}

/// Path constants for lock files.
///
/// These align with [`ScopedStorage`](arco_core::ScopedStorage) path helpers.
pub mod paths {
    /// Lock file for the catalog domain (Tier 1) operations.
    pub const CATALOG_LOCK: &str = "locks/catalog.lock.json";

    /// Lock file prefix for asset-level locks (future use).
    pub const ASSET_LOCK_PREFIX: &str = "locks/assets/";
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    #[tokio::test]
    async fn test_acquire_and_release() {
        let backend = Arc::new(MemoryBackend::new());
        let lock = DistributedLock::new(backend.clone(), "test.lock");

        let guard = lock
            .acquire(Duration::from_secs(30), 5)
            .await
            .expect("acquire");
        assert!(!guard.holder_id().is_empty());

        guard.release().await.expect("release");

        // Lock should be gone
        assert!(!lock.is_locked().await.expect("check"));
    }

    #[tokio::test]
    async fn test_lock_prevents_second_acquisition() {
        let backend = Arc::new(MemoryBackend::new());
        let lock1 = DistributedLock::new(backend.clone(), "test.lock");
        let lock2 = DistributedLock::new(backend.clone(), "test.lock");

        // First lock succeeds
        let _guard1 = lock1
            .acquire(Duration::from_secs(30), 1)
            .await
            .expect("acquire1");

        // Second lock fails (only 1 retry, short timeout)
        let result = lock2.acquire(Duration::from_millis(100), 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_expired_lock_can_be_taken_over() {
        let backend = Arc::new(MemoryBackend::new());
        let lock1 = DistributedLock::new(backend.clone(), "test.lock");
        let lock2 = DistributedLock::new(backend.clone(), "test.lock");

        // Acquire with very short TTL
        let guard1 = lock1
            .acquire(Duration::from_millis(1), 1)
            .await
            .expect("acquire1");

        // First acquisition has fencing token 1
        assert_eq!(guard1.fencing_token().sequence(), 1);

        // Let it expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second lock should succeed by taking over expired lock
        let guard2 = lock2
            .acquire(Duration::from_secs(30), 1)
            .await
            .expect("acquire2");

        // Different holders
        assert_ne!(guard1.holder_id(), guard2.holder_id());

        // Fencing token should be incremented
        assert_eq!(guard2.fencing_token().sequence(), 2);
        assert!(guard2.fencing_token() > guard1.fencing_token());

        guard2.release().await.expect("release2");
    }

    #[tokio::test]
    async fn test_fencing_token_increments_on_takeover() {
        let backend = Arc::new(MemoryBackend::new());

        // First holder
        let lock1 = DistributedLock::new(backend.clone(), "test.lock");
        let guard1 = lock1
            .acquire(Duration::from_millis(1), 1)
            .await
            .expect("acquire1");
        assert_eq!(guard1.fencing_token().sequence(), 1);

        // Simulate delay and expiry
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second holder takes over
        let lock2 = DistributedLock::new(backend.clone(), "test.lock");
        let guard2 = lock2
            .acquire(Duration::from_secs(1), 1)
            .await
            .expect("acquire2");
        assert_eq!(guard2.fencing_token().sequence(), 2);

        // Release and acquire again
        guard2.release().await.expect("release2");
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Third holder takes over expired lock
        let lock3 = DistributedLock::new(backend.clone(), "test.lock");
        let guard3 = lock3
            .acquire(Duration::from_secs(30), 1)
            .await
            .expect("acquire3");

        // Fencing token should be 3
        assert_eq!(guard3.fencing_token().sequence(), 3);

        guard3.release().await.expect("release3");
    }

    #[tokio::test]
    async fn test_permit_issuer_from_lock_guard() {
        let backend = Arc::new(MemoryBackend::new());
        let lock = DistributedLock::new(backend.clone(), "test.lock");

        let guard = lock
            .acquire(Duration::from_secs(30), 1)
            .await
            .expect("acquire");

        // Get permit issuer from guard
        let issuer = guard.permit_issuer();

        // Issue a permit
        let permit = issuer.issue_permit("catalog", "v1".into());

        // Permit should have the same fencing token
        assert_eq!(permit.fencing_token(), guard.fencing_token());
        assert_eq!(permit.fencing_token().sequence(), 1);

        guard.release().await.expect("release");
    }

    #[tokio::test]
    async fn test_lock_info_expiry() {
        let info = LockInfo::new("holder-1", Duration::from_secs(1), 1);
        assert!(!info.is_expired());
        assert!(info.remaining_ttl() > Duration::ZERO);

        // Create expired lock
        let expired = LockInfo {
            holder_id: "holder-2".into(),
            expires_at: Utc::now() - chrono::Duration::seconds(10),
            acquired_at: Utc::now() - chrono::Duration::seconds(20),
            sequence_number: 5,
            operation: None,
        };
        assert!(expired.is_expired());
        assert_eq!(expired.remaining_ttl(), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_force_break() {
        let backend = Arc::new(MemoryBackend::new());
        let lock = DistributedLock::new(backend.clone(), "test.lock");

        let _guard = lock
            .acquire(Duration::from_secs(30), 1)
            .await
            .expect("acquire");
        assert!(lock.is_locked().await.expect("check"));

        lock.force_break().await.expect("break");
        assert!(!lock.is_locked().await.expect("check2"));
    }

    #[tokio::test]
    async fn test_lock_with_operation() {
        let backend = Arc::new(MemoryBackend::new());
        let lock = DistributedLock::new(backend.clone(), "test.lock");

        let guard = lock
            .acquire_with_operation(Duration::from_secs(30), 5, Some("CreateAsset".into()))
            .await
            .expect("acquire");

        // Verify operation is stored
        let data = backend.get("test.lock").await.expect("get");
        let info: LockInfo = serde_json::from_slice(&data).expect("parse");
        assert_eq!(info.operation, Some("CreateAsset".into()));

        guard.release().await.expect("release");
    }

    #[tokio::test]
    async fn test_extend_ttl() {
        let backend = Arc::new(MemoryBackend::new());
        let lock = DistributedLock::new(backend.clone(), "test.lock");

        let mut guard = lock
            .acquire(Duration::from_secs(1), 1)
            .await
            .expect("acquire");

        // Extend by 30 seconds
        guard.extend(Duration::from_secs(30)).await.expect("extend");

        // Verify new expiry is later
        let data = backend.get("test.lock").await.expect("get");
        let info: LockInfo = serde_json::from_slice(&data).expect("parse");
        assert!(info.remaining_ttl() > Duration::from_secs(20));

        guard.release().await.expect("release");
    }

    #[test]
    fn test_paths() {
        assert_eq!(paths::CATALOG_LOCK, "locks/catalog.lock.json");
        assert!(paths::ASSET_LOCK_PREFIX.starts_with("locks/"));
    }
}
