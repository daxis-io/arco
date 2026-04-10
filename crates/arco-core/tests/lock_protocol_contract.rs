#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "support/barrier_backend.rs"]
mod barrier_backend;

use std::sync::Arc;
use std::time::Duration;

use arco_core::lock::{DistributedLock, LockInfo};
use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use barrier_backend::{BarrierBackend, BarrierMatch};
use bytes::Bytes;

#[tokio::test]
async fn expired_lock_takeover_race_has_single_winner_and_monotonic_fencing() {
    let inner: Arc<dyn StorageBackend> = Arc::new(arco_core::MemoryBackend::new());
    let backend: Arc<dyn StorageBackend> = Arc::new(BarrierBackend::new(
        inner.clone(),
        2,
        BarrierMatch::matches_version("locks/catalog.lock.json"),
    ));

    let expired = LockInfo {
        holder_id: "expired-holder".to_string(),
        expires_at: chrono::Utc::now() - chrono::Duration::seconds(5),
        acquired_at: chrono::Utc::now() - chrono::Duration::seconds(10),
        sequence_number: 1,
        operation: None,
    };
    inner
        .put(
            "locks/catalog.lock.json",
            Bytes::from(serde_json::to_vec(&expired).expect("serialize expired lock")),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed expired lock");

    let left_lock = DistributedLock::new(backend.clone(), "locks/catalog.lock.json");
    let right_lock = DistributedLock::new(backend, "locks/catalog.lock.json");

    let left = tokio::spawn(async move { left_lock.acquire(Duration::from_secs(30), 1).await });
    let right = tokio::spawn(async move { right_lock.acquire(Duration::from_secs(30), 1).await });

    let left = left.await.expect("left task");
    let right = right.await.expect("right task");

    let winners = [&left, &right]
        .iter()
        .filter(|result| result.is_ok())
        .count();
    assert_eq!(winners, 1, "exactly one expired-lock takeover must win");

    let winner = left
        .as_ref()
        .ok()
        .or_else(|| right.as_ref().ok())
        .expect("winner");
    assert_eq!(
        winner.fencing_token().sequence(),
        2,
        "expired lock takeover must increment the fencing token exactly once"
    );

    let current = inner
        .get("locks/catalog.lock.json")
        .await
        .expect("current lock bytes");
    let current: LockInfo = serde_json::from_slice(&current).expect("parse current lock");
    assert_eq!(current.sequence_number, 2);
    assert_eq!(current.holder_id, winner.holder_id());
}

#[tokio::test]
async fn stale_guard_release_never_clobbers_newer_lock_holder() {
    let backend: Arc<dyn StorageBackend> = Arc::new(arco_core::MemoryBackend::new());
    let lock = DistributedLock::new(backend.clone(), "locks/catalog.lock.json");
    let guard = lock
        .acquire(Duration::from_secs(30), 1)
        .await
        .expect("initial acquire");

    let replacement = LockInfo::new("new-holder", Duration::from_secs(30), 2);
    let replacement_write = backend
        .put(
            "locks/catalog.lock.json",
            Bytes::from(serde_json::to_vec(&replacement).expect("serialize replacement lock")),
            WritePrecondition::MatchesVersion(guard.version().to_string()),
        )
        .await
        .expect("replacement write");
    assert!(
        matches!(replacement_write, WriteResult::Success { .. }),
        "replacement lock must take over the stale holder version"
    );

    guard.release().await.expect("stale guard release");

    let current = backend
        .get("locks/catalog.lock.json")
        .await
        .expect("current lock bytes");
    let current: LockInfo = serde_json::from_slice(&current).expect("parse current lock");
    assert_eq!(current.holder_id, "new-holder");
    assert_eq!(current.sequence_number, 2);
}
