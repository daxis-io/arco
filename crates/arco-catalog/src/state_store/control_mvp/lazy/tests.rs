#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines
)]
use super::super::super::ControlMvpOutboxTrimTarget;
use super::*;
use crate::state_store::ScanContinuationKey;
use crate::state_store::{ArcoStateAdmin, ArcoStateReader, StateScope};
use arco_core::storage::WritePrecondition;
use arco_core::{MemoryBackend, ScopedStorage};
use std::{num::NonZeroU64, sync::Arc};

fn fixture() -> (ScopedStorage, ControlMvpStateStore) {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let store = ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap()
    .with_test_segment_sizing(64, 8192)
    .unwrap();
    (storage, store)
}
async fn seed(store: &ControlMvpStateStore) -> String {
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let id = tx.tx_id().to_string();
    for n in 0..100_u32 {
        tx.put(&n.to_be_bytes(), Bytes::from(vec![42; 1024]))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    id
}
#[tokio::test]
async fn unrelated_corruption_is_deferred_but_commit_always_revalidates() {
    for after_memo in [false, true] {
        let (storage, store) = fixture();
        let id = seed(&store).await;
        let before = storage
            .get_raw(&store.paths.current_pointer())
            .await
            .unwrap();
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if after_memo {
            tx.get(&99_u32.to_be_bytes()).await.unwrap();
        }
        let path = store.paths.l0_segment_object(&id);
        let mut bytes = storage.get_raw(&path).await.unwrap().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        storage
            .put_raw(&path, Bytes::from(bytes), WritePrecondition::None)
            .await
            .unwrap();
        if after_memo {
            assert!(tx.get(&99_u32.to_be_bytes()).await.unwrap().is_some());
        } else {
            // Beginning after corruption still authenticates only metadata.
            tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            assert!(tx.get(&0_u32.to_be_bytes()).await.unwrap().is_some());
            assert!(tx.get(&99_u32.to_be_bytes()).await.is_err());
        }
        let candidate = store.paths.tx_object(tx.tx_id());
        assert!(tx.commit().await.is_err());
        assert!(storage.head_raw(&candidate).await.unwrap().is_none());
        assert_eq!(
            before,
            storage
                .get_raw(&store.paths.current_pointer())
                .await
                .unwrap()
        );
    }
}
#[tokio::test]
async fn missing_pinned_directory_is_never_negative_evidence() {
    let (storage, store) = fixture();
    let id = seed(&store).await;
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    storage
        .delete(&store.paths.segment_index(&id))
        .await
        .unwrap();
    assert!(tx.get(b"never-present").await.is_err());
    assert!(tx.assert_absent(b"never-present").await.is_err());
    assert!(
        tx.assert_range_empty(KeyRange::new(vec![], vec![255]))
            .await
            .is_err()
    );
    assert!(tx.commit().await.is_err());
}
#[tokio::test]
async fn all_transaction_cursors_reject_wire_readers_and_other_transactions() {
    for genesis in [false, true] {
        let (_, store) = fixture();
        if !genesis {
            seed(&store).await;
        }
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        for key in [b"x", b"y"] {
            tx.put(key, Bytes::new()).await.unwrap();
        }
        let page = tx
            .scan(ScanRequest::new(b"").with_limits(1, 4096, 64))
            .await
            .unwrap();
        let cursor = page.continuation().unwrap().clone();
        assert!(
            cursor
                .encode_opaque(&ScanContinuationKey::generate().unwrap())
                .is_err()
        );
        assert!(
            store
                .scan(ScanRequest::new(b"").with_token(cursor.clone()))
                .await
                .is_err()
        );
        if !genesis {
            assert!(
                store
                    .read_at(store.current_state_token().await.unwrap())
                    .await
                    .unwrap()
                    .scan(ScanRequest::new(b"").with_token(cursor.clone()))
                    .await
                    .is_err()
            );
        }
        let mut other = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        assert!(
            other
                .scan(ScanRequest::new(b"").with_token(cursor.clone()))
                .await
                .is_err()
        );
        tx.scan(ScanRequest::new(b"").with_token(cursor))
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn point_assertions_always_use_pinned_base_and_preserve_tombstones() {
    let (_, store) = fixture();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"", Bytes::new()).await.unwrap();
    tx.delete(b"deleted").await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.delete(b"").await.unwrap();
    assert!(tx.assert_absent(b"").await.is_err());
    tx.assert_generation(b"", 1).await.unwrap();
    tx.put(b"deleted", Bytes::new()).await.unwrap();
    tx.assert_absent(b"deleted").await.unwrap();
    assert!(tx.assert_generation(b"deleted", 1).await.is_err());
    tx.assert_absent(b"never").await.unwrap();
    assert_eq!(
        tx.reads.points[b"deleted".as_slice()],
        PointWitness::Tombstone(1)
    );
    assert_eq!(tx.reads.points[b"never".as_slice()], PointWitness::Absent);
    assert!(
        tx.assert_range_empty(KeyRange::new(b"deleted".to_vec(), b"deletee".to_vec()))
            .await
            .is_err()
    );
    tx.assert_range_empty(KeyRange::new(b"z".to_vec(), b"a".to_vec()))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
#[tokio::test]
async fn request_budget_evicts_optional_memo_and_preserves_failed_staging() {
    let (_, store) = fixture();
    seed(&store).await;
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.get(&0_u32.to_be_bytes()).await.unwrap();
    assert!(tx.reads.memo_bytes > 0);
    let spare = tx.reads.memo_bytes;
    tx.reads.bytes = MAX_REQUEST_BYTES - spare;
    tx.put(b"small", Bytes::new()).await.unwrap();
    assert_eq!(tx.reads.memo_bytes, 0);
    let before = tx.writes.len();
    assert!(matches!(
        tx.put(b"too-large", Bytes::from(vec![0; spare + 1])).await,
        Err(CatalogError::MaintenanceBackpressure { .. })
    ));
    assert_eq!(tx.writes.len(), before);
    tx.reads.entries = MAX_REQUEST_ENTRIES;
    assert!(matches!(
        tx.delete(b"new-entry").await,
        Err(CatalogError::MaintenanceBackpressure { .. })
    ));
    assert_eq!(tx.writes.len(), before);
}
#[tokio::test]
async fn pinned_reads_survive_head_transitions_but_attempts_cannot_rebase() {
    for claim in [false, true] {
        let (_, store) = fixture();
        seed(&store).await;
        let mut pinned = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        let old = pinned.get(&0_u32.to_be_bytes()).await.unwrap().unwrap();
        if claim {
            store.clone().claim_writer_authority().await.unwrap();
        } else {
            let mut changed = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            changed.delete(&0_u32.to_be_bytes()).await.unwrap();
            changed.commit().await.unwrap();
        }
        assert_eq!(
            pinned.get(&0_u32.to_be_bytes()).await.unwrap().unwrap(),
            old
        );
        assert!(pinned.get(&1_u32.to_be_bytes()).await.unwrap().is_some());
        assert!(pinned.commit().await.is_err());
    }
}
#[tokio::test]
async fn selected_outbox_lookup_finds_keyless_l1_and_exact_l0_incarnations() {
    let (_, store) = fixture();
    let store = store.with_checkpoint_interval(NonZeroU64::new(1).unwrap());
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for id in ["z-last", "a-first"] {
        tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(id, Bytes::new()))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    assert!(
        tx.stage_projection_intent("z-last", "test", Bytes::new())
            .await
            .is_err()
    );
    tx.trim_projection_outbox([ControlMvpOutboxTrimTarget::new("a-first", 1)])
        .await
        .unwrap();
    assert!(
        tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "a-first",
            Bytes::new()
        ))
        .await
        .is_err()
    );
    tx.commit().await.unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "a-first",
        Bytes::from_static(b"new"),
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let records = store.current_projection_outbox().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(ControlMvpProjectionOutboxRecord::record_id)
            .collect::<Vec<_>>(),
        ["z-last", "a-first"]
    );
    assert_eq!(records[1].origin_sequence(), Some(4));
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    assert!(
        tx.trim_projection_outbox([ControlMvpOutboxTrimTarget::new("a-first", 1)])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn commit_requires_the_pinned_manifest_after_memoized_access() {
    let (storage, store) = fixture();
    seed(&store).await;
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.get(&0_u32.to_be_bytes()).await.unwrap();
    let path = store
        .paths
        .manifest_object(&tx.base.manifest().unwrap().manifest_id);
    storage.delete(&path).await.unwrap();
    assert!(tx.get(&0_u32.to_be_bytes()).await.unwrap().is_some());
    assert!(tx.commit().await.is_err());
}

#[tokio::test]
async fn empty_transaction_pages_advance_over_staged_deletes() {
    let (_, store) = fixture();
    let store = store
        .with_test_segment_sizing(4, 8192)
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(1).unwrap());
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for n in 0..10 {
        tx.put(format!("a{n}").as_bytes(), Bytes::new())
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for n in 0..4 {
        tx.delete(format!("a{n}").as_bytes()).await.unwrap();
    }
    let page = tx
        .scan(ScanRequest::new(b"a").with_limits(10, 4096, 1))
        .await
        .unwrap();
    assert!(page.entries().is_empty());
    let cursor = page.continuation().unwrap().clone();
    assert_eq!(cursor.exclusive_last_key, b"a3");
    tx.put(b"a2", Bytes::from_static(b"past")).await.unwrap();
    tx.put(b"a4", Bytes::from_static(b"changed")).await.unwrap();
    let page = tx
        .scan(ScanRequest::new(b"a").with_token(cursor))
        .await
        .unwrap();
    assert_eq!(page.entries().first().unwrap().key(), b"a4");
    assert_eq!(page.entries().first().unwrap().value().generation(), None);
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn transaction_pins_do_not_renew_retention_or_cross_reclamation_fences() {
    use super::super::super::ControlMvpMaintenanceWorker;
    for expired in [false, true] {
        let (storage, store) = fixture();
        let store = store.with_checkpoint_interval(NonZeroU64::new(1).unwrap());
        let id = seed(&store).await;
        let mut pinned = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        pinned.get(&0_u32.to_be_bytes()).await.unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        storage
            .put_raw(
                &store.paths.tx_object("orphan"),
                Bytes::from_static(b"orphan"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let worker =
            ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone()).unwrap();
        let now = chrono::Utc::now() + chrono::Duration::days(if expired { 31 } else { 8 });
        let mut cursor = None;
        loop {
            let outcome = worker
                .collect_gc_page_at(now, Vec::new(), cursor.as_deref())
                .await
                .unwrap();
            cursor = outcome.continuation().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        assert!(
            pinned.get(&0_u32.to_be_bytes()).await.unwrap().is_some(),
            "successful memo may remain available"
        );
        if expired {
            assert!(
                storage
                    .head_raw(&store.paths.l0_segment_object(&id))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(pinned.get(&1_u32.to_be_bytes()).await.is_err());
        } else {
            assert!(pinned.get(&1_u32.to_be_bytes()).await.unwrap().is_some());
        }
        assert!(pinned.commit().await.is_err());
    }
}

#[tokio::test]
async fn maintenance_preserves_pinned_reads_but_consumes_old_cas_attempt() {
    use super::super::super::ControlMvpMaintenanceWorker;
    let (storage, store) = fixture();
    seed(&store).await;
    for _ in 1..16 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.get(&0_u32.to_be_bytes()).await.unwrap();
    let before = store.current_state_token().await.unwrap();
    let worker = ControlMvpMaintenanceWorker::new(storage, store.scope.clone()).unwrap();
    worker.consolidate_pending().await.unwrap().unwrap();
    let after = store.current_state_token().await.unwrap();
    assert_eq!(before.logical_sequence(), after.logical_sequence());
    assert_ne!(
        before.authority_manifest_id(),
        after.authority_manifest_id()
    );
    assert!(tx.get(&1_u32.to_be_bytes()).await.unwrap().is_some());
    assert!(matches!(
        tx.commit().await,
        Err(CatalogError::CasFailed { .. })
    ));
}

#[tokio::test]
async fn same_value_and_delete_reinsert_aba_change_range_and_predicate_witnesses() {
    let (_, store) = fixture();
    let key = b"a".to_vec();
    let range = KeyRange::new(key.clone(), b"b".to_vec());
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(&key, Bytes::new()).await.unwrap();
    tx.commit().await.unwrap();
    for delete_first in [false, true] {
        let mut old = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        let inputs = old
            .read_set(std::slice::from_ref(&key), std::slice::from_ref(&range))
            .await
            .unwrap();
        let witness = old.range_witness(&range).await.unwrap();
        if delete_first {
            let mut changed = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            changed.delete(&key).await.unwrap();
            changed.commit().await.unwrap();
        }
        let mut changed = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        changed.put(&key, Bytes::new()).await.unwrap();
        changed.commit().await.unwrap();
        let mut fresh = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        assert!(fresh.assert_inputs_unchanged(inputs.clone()).await.is_err());
        assert!(
            fresh
                .assert_range_unchanged(range.clone(), witness)
                .await
                .is_err()
        );
        old.assert_inputs_unchanged(inputs).await.unwrap();
        assert!(matches!(
            old.commit().await,
            Err(CatalogError::CasFailed { .. })
        ));
    }
}
