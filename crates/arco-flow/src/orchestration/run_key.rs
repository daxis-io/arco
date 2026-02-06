//! Run key reservation for idempotent `TriggerRun`.
//!
//! Implements strong idempotency for `run_key` using a reservation blob pattern.
//! When a run is triggered with a `run_key`, we first attempt to reserve it by
//! writing a small JSON blob with `WritePrecondition::DoesNotExist`. If the write
//! fails, we read the existing reservation and return the previously created run.
//!
//! ## Storage Path
//!
//! Reservations are stored at:
//! ```text
//! run_keys/{sha256(run_key)[0..40]}.json
//! ```
//!
//! The hash prevents `run_key` values with special characters from causing path issues
//! while maintaining a flat namespace for efficient lookup.
//!
//! ## Collision Handling
//!
//! SHA-256 collisions are astronomically unlikely (2^-128 collision probability).
//! As a defense-in-depth measure, we store the original `run_key` in the reservation
//! and verify it on read. A mismatch returns an error rather than silently returning
//! the wrong run.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

use arco_core::{ScopedStorage, WritePrecondition, WriteResult};

use crate::error::{Error, Result};

/// Fingerprint validation policy for `run_key` reservations.
#[derive(Debug, Clone, Copy, Default)]
pub struct FingerprintPolicy {
    /// Reservations created before this cutoff are treated as lenient for missing fingerprints.
    pub cutoff: Option<DateTime<Utc>>,
}

impl FingerprintPolicy {
    /// Always allow missing fingerprints (legacy behavior).
    #[must_use]
    pub fn lenient() -> Self {
        Self { cutoff: None }
    }

    /// Enforce strict matching for reservations created on/after the cutoff.
    #[must_use]
    pub fn with_cutoff(cutoff: DateTime<Utc>) -> Self {
        Self {
            cutoff: Some(cutoff),
        }
    }

    /// Derive policy from an optional cutoff timestamp.
    #[must_use]
    pub fn from_cutoff(cutoff: Option<DateTime<Utc>>) -> Self {
        Self { cutoff }
    }

    fn allows_missing(&self, existing_created_at: DateTime<Utc>) -> bool {
        self.cutoff
            .is_none_or(|cutoff| existing_created_at < cutoff)
    }
}

/// Reservation blob stored for `run_key` idempotency.
///
/// Contains all information needed to return a consistent response for duplicate
/// `TriggerRun` requests, even before the compactor has processed the `RunTriggered` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunKeyReservation {
    /// Original `run_key` value (for collision verification).
    pub run_key: String,
    /// Reserved run ID.
    pub run_id: String,
    /// Associated plan ID.
    pub plan_id: String,
    /// Event ID of the `RunTriggered` event.
    pub event_id: String,
    /// Event ID of the `PlanCreated` event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_event_id: Option<String>,
    /// Fingerprint of the trigger request payload (selection/partitions/labels).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_fingerprint: Option<String>,
    /// When the reservation was created.
    pub created_at: DateTime<Utc>,
}

/// Result of attempting to reserve a `run_key`.
#[derive(Debug)]
pub enum ReservationResult {
    /// Successfully reserved - caller should proceed to emit `RunTriggered`.
    Reserved,
    /// Already reserved with matching fingerprint - caller should return existing run.
    AlreadyExists(RunKeyReservation),
    /// Already reserved but with different fingerprint - payload consistency violation.
    /// Caller should either reject the request or log a warning.
    FingerprintMismatch {
        /// The existing reservation (with the original fingerprint).
        existing: RunKeyReservation,
        /// The fingerprint from the new request that didn't match.
        requested_fingerprint: Option<String>,
    },
}

/// Generates the storage path for a `run_key` reservation.
///
/// Format: `run_keys/{sha256(run_key)[0..40]}.json`
#[must_use]
pub fn reservation_path(run_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_key.as_bytes());
    let hash = hasher.finalize();

    // Take first 20 bytes = 40 hex chars = 160 bits of entropy
    let hex_encoded = hex::encode(hash.get(..20).unwrap_or(&hash));

    format!("run_keys/{hex_encoded}.json")
}

/// Attempts to reserve a `run_key` for a new run.
///
/// Uses `WritePrecondition::DoesNotExist` to ensure only one caller wins the race.
///
/// `fingerprint_policy` controls whether missing fingerprints are treated as
/// legacy-compatible or strict based on reservation creation time.
///
/// # Returns
///
/// - `Ok(ReservationResult::Reserved)` - Caller won the race, should emit `RunTriggered`.
/// - `Ok(ReservationResult::AlreadyExists(reservation))` - Another run already claimed this key.
///
/// # Errors
///
/// Returns an error if:
/// - Serialization fails
/// - Storage write fails (non-precondition error)
/// - Existing reservation has a hash collision (different `run_key`)
pub async fn reserve_run_key(
    storage: &ScopedStorage,
    reservation: &RunKeyReservation,
    fingerprint_policy: FingerprintPolicy,
) -> Result<ReservationResult> {
    let path = reservation_path(&reservation.run_key);

    let json = serde_json::to_string(reservation).map_err(|e| Error::Serialization {
        message: format!("failed to serialize run_key reservation: {e}"),
    })?;

    let result = storage
        .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
        .await?;

    match result {
        WriteResult::Success { .. } => {
            tracing::info!(
                run_key = %reservation.run_key,
                run_id = %reservation.run_id,
                path = %path,
                "Run key reserved"
            );
            Ok(ReservationResult::Reserved)
        }
        WriteResult::PreconditionFailed { .. } => {
            // Read existing reservation.
            //
            // Some object stores can exhibit a brief read-after-write lag in rare cases.
            // If the write precondition fails (meaning *some* object exists) but the
            // subsequent read returns NotFound, treat it as a retryable race and
            // retry the read a handful of times with tight backoff.
            let mut existing = get_reservation(storage, &reservation.run_key).await?;
            if existing.is_none() {
                // Total worst-case sleep ~= 155ms
                const READ_RETRIES_MS: [u64; 5] = [5, 10, 20, 40, 80];
                for (attempt, delay_ms) in READ_RETRIES_MS.into_iter().enumerate() {
                    tracing::debug!(
                        run_key = %reservation.run_key,
                        attempt = attempt + 1,
                        delay_ms,
                        "run_key reservation not yet visible after precondition failure; retrying read"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    existing = get_reservation(storage, &reservation.run_key).await?;
                    if existing.is_some() {
                        break;
                    }
                }
            }
            match existing {
                Some(existing) => {
                    // Validate fingerprint consistency with optional cutoff for legacy reservations.
                    let fingerprints_match = match (
                        &existing.request_fingerprint,
                        &reservation.request_fingerprint,
                    ) {
                        (Some(a), Some(b)) => a == b,
                        (None, None) => true,
                        (None, Some(_)) => fingerprint_policy.allows_missing(existing.created_at),
                        (Some(_), None) => false,
                    };

                    if fingerprints_match {
                        tracing::info!(
                            run_key = %reservation.run_key,
                            existing_run_id = %existing.run_id,
                            "Run key already reserved with matching fingerprint"
                        );
                        Ok(ReservationResult::AlreadyExists(existing))
                    } else {
                        tracing::warn!(
                            run_key = %reservation.run_key,
                            existing_run_id = %existing.run_id,
                            existing_fingerprint = ?existing.request_fingerprint,
                            requested_fingerprint = ?reservation.request_fingerprint,
                            existing_created_at = %existing.created_at,
                            fingerprint_cutoff = ?fingerprint_policy.cutoff,
                            "Run key already reserved with DIFFERENT fingerprint - payload consistency violation"
                        );
                        Ok(ReservationResult::FingerprintMismatch {
                            existing,
                            requested_fingerprint: reservation.request_fingerprint.clone(),
                        })
                    }
                }
                None => {
                    // Extremely unlikely: precondition failed but read returned None.
                    // Could be a deeper storage inconsistency or deletion race.
                    Err(Error::storage(format!(
                        "run_key reservation race: precondition failed but reservation not found: {}",
                        reservation.run_key
                    )))
                }
            }
        }
    }
}

/// Retrieves an existing `run_key` reservation.
///
/// # Returns
///
/// - `Ok(Some(reservation))` - Reservation exists
/// - `Ok(None)` - No reservation for this `run_key`
///
/// # Errors
///
/// Returns an error if:
/// - Storage read fails
/// - Deserialization fails
/// - Hash collision detected (stored `run_key` differs from requested)
pub async fn get_reservation(
    storage: &ScopedStorage,
    run_key: &str,
) -> Result<Option<RunKeyReservation>> {
    let path = reservation_path(run_key);

    let data = match storage.get_raw(&path).await {
        Ok(data) => data,
        Err(arco_core::Error::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let reservation: RunKeyReservation =
        serde_json::from_slice(&data).map_err(|e| Error::Serialization {
            message: format!("failed to deserialize run_key reservation: {e}"),
        })?;

    // Verify no hash collision
    if reservation.run_key != run_key {
        return Err(Error::Configuration {
            message: format!(
                "run_key hash collision detected: requested='{}', stored='{}'",
                run_key, reservation.run_key
            ),
        });
    }

    Ok(Some(reservation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::{
        Error as CoreError, MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition,
    };
    use async_trait::async_trait;
    use chrono::Duration as ChronoDuration;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn make_reservation(run_key: &str) -> RunKeyReservation {
        RunKeyReservation {
            run_key: run_key.to_string(),
            run_id: format!("run_{}", ulid::Ulid::new()),
            plan_id: format!("plan_{}", ulid::Ulid::new()),
            event_id: ulid::Ulid::new().to_string(),
            plan_event_id: Some(ulid::Ulid::new().to_string()),
            request_fingerprint: Some(format!("fingerprint:{run_key}")),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_reservation_path_is_deterministic() {
        let path1 = reservation_path("daily-etl:2025-01-15");
        let path2 = reservation_path("daily-etl:2025-01-15");
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_reservation_path_differs_for_different_keys() {
        let path1 = reservation_path("daily-etl:2025-01-15");
        let path2 = reservation_path("daily-etl:2025-01-16");
        assert_ne!(path1, path2);
    }

    #[test]
    fn test_reservation_path_format() {
        let path = reservation_path("test-key");
        assert!(path.starts_with("run_keys/"));
        assert!(path.ends_with(".json"));
        // 40 hex chars between prefix and suffix
        let hash_part = path
            .strip_prefix("run_keys/")
            .unwrap()
            .strip_suffix(".json")
            .unwrap();
        assert_eq!(hash_part.len(), 40);
    }

    #[tokio::test]
    async fn test_reserve_run_key_success() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        let reservation = make_reservation("test-run-key");
        let result = reserve_run_key(&storage, &reservation, FingerprintPolicy::lenient()).await?;

        assert!(matches!(result, ReservationResult::Reserved));
        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_run_key_duplicate_returns_existing() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        let reservation1 = make_reservation("test-run-key");
        let result1 =
            reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        // Second reservation with same key should return existing
        let reservation2 = make_reservation("test-run-key");
        let result2 =
            reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;

        match result2 {
            ReservationResult::AlreadyExists(existing) => {
                assert_eq!(existing.run_id, reservation1.run_id);
                assert_eq!(existing.plan_id, reservation1.plan_id);
                assert_eq!(existing.plan_event_id, reservation1.plan_event_id);
                assert_eq!(
                    existing.request_fingerprint,
                    reservation1.request_fingerprint
                );
            }
            ReservationResult::Reserved => {
                panic!("expected AlreadyExists, got Reserved");
            }
            ReservationResult::FingerprintMismatch { .. } => {
                panic!("expected AlreadyExists, got FingerprintMismatch");
            }
        }

        Ok(())
    }

    #[derive(Debug)]
    struct FlakyReadBackend {
        inner: Arc<MemoryBackend>,
        flaky_suffix: String,
        remaining_misses: AtomicUsize,
    }

    impl FlakyReadBackend {
        fn new(inner: Arc<MemoryBackend>, flaky_suffix: String, misses: usize) -> Self {
            Self {
                inner,
                flaky_suffix,
                remaining_misses: AtomicUsize::new(misses),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for FlakyReadBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            if path.ends_with(&self.flaky_suffix) {
                let mut current = self.remaining_misses.load(Ordering::Relaxed);
                while current > 0 {
                    match self.remaining_misses.compare_exchange_weak(
                        current,
                        current - 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Err(CoreError::NotFound(path.to_string())),
                        Err(next) => current = next,
                    }
                }
            }
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn test_reserve_run_key_precondition_failed_read_retry() -> Result<()> {
        let inner = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn StorageBackend> = Arc::new(FlakyReadBackend::new(
            inner,
            reservation_path("test-run-key"),
            2,
        ));
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        let reservation1 = make_reservation("test-run-key");
        let result1 =
            reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        let reservation2 = make_reservation("test-run-key");
        let result2 =
            reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result2, ReservationResult::AlreadyExists(_)));

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_run_key_precondition_failed_read_retry_is_bounded() -> Result<()> {
        let inner = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn StorageBackend> = Arc::new(FlakyReadBackend::new(
            inner,
            reservation_path("test-run-key"),
            100,
        ));
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        let reservation1 = make_reservation("test-run-key");
        let result1 =
            reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        let reservation2 = make_reservation("test-run-key");
        let err = reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient())
            .await
            .expect_err("expected error");
        assert!(err.to_string().contains("precondition failed"));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_reservation_not_found() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        let result = get_reservation(&storage, "nonexistent-key").await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_reservation_found() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        let reservation = make_reservation("test-key");
        reserve_run_key(&storage, &reservation, FingerprintPolicy::lenient()).await?;

        let retrieved = get_reservation(&storage, "test-key").await?.unwrap();
        assert_eq!(retrieved.run_id, reservation.run_id);
        assert_eq!(retrieved.run_key, "test-key");
        assert_eq!(retrieved.plan_event_id, reservation.plan_event_id);
        assert_eq!(
            retrieved.request_fingerprint,
            reservation.request_fingerprint
        );
        Ok(())
    }

    /// P2: Duplicate run_key requests with different fingerprints should be detected.
    /// This prevents semantic drift when the same run_key is reserved multiple times
    /// with different payloads (e.g., different asset selection or partitions).
    #[tokio::test]
    async fn test_reserve_run_key_detects_fingerprint_mismatch() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        // First reservation with fingerprint "A"
        let mut reservation1 = make_reservation("test-run-key");
        reservation1.request_fingerprint = Some("fingerprint-A".to_string());
        let result1 =
            reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        // Second reservation with DIFFERENT fingerprint "B"
        let mut reservation2 = make_reservation("test-run-key");
        reservation2.request_fingerprint = Some("fingerprint-B".to_string());
        let result2 =
            reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;

        // Should detect fingerprint mismatch
        match result2 {
            ReservationResult::FingerprintMismatch {
                existing,
                requested_fingerprint,
            } => {
                assert_eq!(
                    existing.request_fingerprint,
                    Some("fingerprint-A".to_string())
                );
                assert_eq!(requested_fingerprint, Some("fingerprint-B".to_string()));
            }
            ReservationResult::AlreadyExists(_) => {
                panic!("expected FingerprintMismatch, got AlreadyExists");
            }
            ReservationResult::Reserved => {
                panic!("expected FingerprintMismatch, got Reserved");
            }
        }

        Ok(())
    }

    /// Duplicate reservation with SAME fingerprint should return AlreadyExists (not mismatch).
    #[tokio::test]
    async fn test_reserve_run_key_same_fingerprint_is_not_mismatch() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        // First reservation with fingerprint "A"
        let mut reservation1 = make_reservation("test-run-key");
        reservation1.request_fingerprint = Some("fingerprint-A".to_string());
        let result1 =
            reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        // Second reservation with SAME fingerprint "A"
        let mut reservation2 = make_reservation("test-run-key");
        reservation2.request_fingerprint = Some("fingerprint-A".to_string());
        let result2 =
            reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;

        // Should return AlreadyExists (not mismatch)
        match result2 {
            ReservationResult::AlreadyExists(existing) => {
                assert_eq!(existing.run_id, reservation1.run_id);
            }
            ReservationResult::FingerprintMismatch { .. } => {
                panic!("expected AlreadyExists, got FingerprintMismatch");
            }
            ReservationResult::Reserved => {
                panic!("expected AlreadyExists, got Reserved");
            }
        }

        Ok(())
    }

    /// Both fingerprints are None should be treated as matching.
    #[tokio::test]
    async fn test_reserve_run_key_both_none_fingerprints_match() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

        // First reservation with no fingerprint
        let mut reservation1 = make_reservation("test-run-key");
        reservation1.request_fingerprint = None;
        let result1 =
            reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        // Second reservation also with no fingerprint
        let mut reservation2 = make_reservation("test-run-key");
        reservation2.request_fingerprint = None;
        let result2 =
            reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;

        // Should return AlreadyExists (both None is a match)
        assert!(matches!(result2, ReservationResult::AlreadyExists(_)));

        Ok(())
    }

    /// If existing has no fingerprint but new request does, be lenient (backward compat).
    #[tokio::test]
    async fn test_reserve_run_key_missing_fingerprint_is_lenient() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let cutoff = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let policy = FingerprintPolicy::with_cutoff(cutoff);

        // First reservation with no fingerprint (simulates old reservation)
        let mut reservation1 = make_reservation("test-run-key");
        reservation1.request_fingerprint = None;
        reservation1.created_at = cutoff - ChronoDuration::hours(1);
        let result1 = reserve_run_key(&storage, &reservation1, policy).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        // Second reservation HAS a fingerprint
        let mut reservation2 = make_reservation("test-run-key");
        reservation2.request_fingerprint = Some("new-fingerprint".to_string());
        let result2 = reserve_run_key(&storage, &reservation2, policy).await?;

        // Should be lenient and return AlreadyExists (not mismatch)
        // This maintains backward compatibility with old reservations
        assert!(matches!(result2, ReservationResult::AlreadyExists(_)));

        Ok(())
    }

    /// If existing reservation is post-cutoff and missing a fingerprint, treat as mismatch.
    #[tokio::test]
    async fn test_reserve_run_key_missing_fingerprint_is_strict_after_cutoff() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let cutoff = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let policy = FingerprintPolicy::with_cutoff(cutoff);

        // First reservation with no fingerprint created AFTER cutoff
        let mut reservation1 = make_reservation("test-run-key");
        reservation1.request_fingerprint = None;
        reservation1.created_at = cutoff + ChronoDuration::hours(1);
        let result1 = reserve_run_key(&storage, &reservation1, policy).await?;
        assert!(matches!(result1, ReservationResult::Reserved));

        // Second reservation HAS a fingerprint
        let mut reservation2 = make_reservation("test-run-key");
        reservation2.request_fingerprint = Some("new-fingerprint".to_string());
        let result2 = reserve_run_key(&storage, &reservation2, policy).await?;

        match result2 {
            ReservationResult::FingerprintMismatch {
                existing,
                requested_fingerprint,
            } => {
                assert_eq!(existing.request_fingerprint, None);
                assert_eq!(requested_fingerprint, Some("new-fingerprint".to_string()));
            }
            ReservationResult::AlreadyExists(_) => {
                panic!("expected FingerprintMismatch, got AlreadyExists");
            }
            ReservationResult::Reserved => {
                panic!("expected FingerprintMismatch, got Reserved");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reservation_serialization_roundtrip() {
        let reservation = RunKeyReservation {
            run_key: "daily-etl:2025-01-15".to_string(),
            run_id: "run_01ABCDEF".to_string(),
            plan_id: "plan_01GHIJKL".to_string(),
            event_id: "01MNOPQR".to_string(),
            plan_event_id: Some("01PLANEVENT".to_string()),
            request_fingerprint: Some("fingerprint-123".to_string()),
            created_at: Utc::now(),
        };

        let json = serde_json::to_string(&reservation).expect("serialize");
        let parsed: RunKeyReservation = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.run_key, reservation.run_key);
        assert_eq!(parsed.run_id, reservation.run_id);
        assert_eq!(parsed.plan_id, reservation.plan_id);
        assert_eq!(parsed.event_id, reservation.event_id);
        assert_eq!(parsed.plan_event_id, reservation.plan_event_id);
        assert_eq!(parsed.request_fingerprint, reservation.request_fingerprint);
    }
}
