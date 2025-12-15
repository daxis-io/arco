//! Integration tests for concurrent writer safety.
//!
//! These tests verify the catalog's distributed locking and CAS mechanisms work
//! correctly under contention.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arco_catalog::tier1_writer::Tier1Writer;
use arco_core::ScopedStorage;
use arco_core::storage::MemoryBackend;

/// Two writers race to initialize - at least one must succeed.
#[tokio::test]
async fn test_two_writers_initialize_race() {
    let backend = Arc::new(MemoryBackend::new());
    let init_success_count = Arc::new(AtomicU32::new(0));
    let init_failure_count = Arc::new(AtomicU32::new(0));

    let handles: Vec<_> = (0..2)
        .map(|_| {
            let backend = backend.clone();
            let success = init_success_count.clone();
            let failure = init_failure_count.clone();

            tokio::spawn(async move {
                let storage = ScopedStorage::new(backend, "acme", "production").unwrap();
                let writer = Tier1Writer::new(storage);

                match writer.initialize().await {
                    Ok(()) => {
                        success.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        failure.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    let successes = init_success_count.load(Ordering::SeqCst);
    let failures = init_failure_count.load(Ordering::SeqCst);

    assert!(successes >= 1, "at least one writer should succeed");
    assert_eq!(successes + failures, 2, "all writers should complete");
}

/// Many concurrent updates - all should eventually succeed (with retries if needed).
#[tokio::test]
async fn test_many_concurrent_updates() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
    let writer = Tier1Writer::new(storage.clone());

    writer.initialize().await.unwrap();

    let update_count = Arc::new(AtomicU32::new(0));
    let num_writers = 5_u32;

    let handles: Vec<_> = (0..num_writers)
        .map(|_| {
            let backend = backend.clone();
            let count = update_count.clone();

            tokio::spawn(async move {
                let storage = ScopedStorage::new(backend, "acme", "production").unwrap();
                let writer = Tier1Writer::new(storage);

                writer
                    .update(|manifest| {
                        manifest.core.snapshot_version += 1;
                        Ok(())
                    })
                    .await
                    .unwrap();

                count.fetch_add(1, Ordering::SeqCst);
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(
        update_count.load(Ordering::SeqCst),
        num_writers,
        "all updates should complete"
    );

    let final_manifest = writer.read_manifest().await.unwrap();
    assert_eq!(
        final_manifest.core.snapshot_version,
        u64::from(num_writers),
        "version should equal number of updates"
    );
}

/// A second writer can initialize (idempotent) and update after a first writer.
#[tokio::test]
async fn test_loser_can_retry() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production").unwrap();
    let writer1 = Tier1Writer::new(storage.clone());
    let writer2 = Tier1Writer::new(storage.clone());

    writer1.initialize().await.unwrap();

    // Idempotent initialization: a "loser" should not wedge.
    let _ = writer2.initialize().await;

    writer1
        .update(|m| {
            m.core.snapshot_version = 1;
            Ok(())
        })
        .await
        .unwrap();

    writer2
        .update(|m| {
            m.core.snapshot_version = 2;
            Ok(())
        })
        .await
        .unwrap();

    let final_manifest = writer1.read_manifest().await.unwrap();
    assert_eq!(final_manifest.core.snapshot_version, 2);
}
