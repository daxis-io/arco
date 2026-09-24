//! Synthetic construction and V1 entry-point isolation.
#![allow(clippy::unwrap_used)]
use super::*;

fn store() -> ControlMvpStateStore {
    ControlMvpStateStore::new_synthetic_bounded(
        ScopedStorage::new(
            Arc::new(arco_core::MemoryBackend::new()),
            "tenant",
            "workspace",
        )
        .unwrap(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap()
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn authority8_narrow_delivery_page_stays_bounded_above_sixteen_blocks() {
    let store = store();
    // The streaming constructor flushes at 256 rows, so this forces 17 leaves
    // in both indexes while the requested page needs only their first blocks.
    let count = 17 * 256;
    let intents = (0..count).map(|ordinal| {
        ProjectionIntentV2::new(
            format!("intent-{ordinal:08}"),
            "catalog",
            store.scope.clone(),
            1,
            "a1".repeat(32),
            ordinal,
            b"payload",
        )
        .unwrap()
    });
    let token = store
        .install_synthetic_genesis("wide-delivery", 1, 0, std::iter::empty(), count, intents)
        .await
        .unwrap();
    cost::take_bounded_work();
    let first = store
        .projection_outbox_page_v2(Some(token), None, 1)
        .await
        .unwrap();
    assert_eq!(first.records().len(), 1);
    assert_eq!(first.records()[0].intent_id(), "intent-00000000");
    assert_eq!(
        cost::take_bounded_work()
            .values()
            .map(|work| work.selected_blocks)
            .sum::<u64>(),
        2,
        "delivery and active blocks must both enter narrow-read accounting"
    );
    let second = store
        .projection_outbox_page_v2(None, first.continuation().cloned(), 1)
        .await
        .unwrap();
    assert_eq!(second.records()[0].intent_id(), "intent-00000001");
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn authority8_replacements_are_separate_from_the_old_block_selection_ceiling() {
    let store = store();
    let rows = (0..9 * 256).map(|index| SyntheticKvEntry {
        key: format!("key-{index:08}").into_bytes(),
        generation: 1,
        value: Some(vec![b'v'; 128]),
    });
    store
        .install_synthetic_genesis("nine-old-blocks", 1, 9 * 256, rows, 0, std::iter::empty())
        .await
        .unwrap();
    let mut txn = transaction(&store, "nine-replacements").await;
    for block in 0..9 {
        txn.put(
            format!("key-{:08}", block * 256).as_bytes(),
            Bytes::from_static(b"changed"),
        )
        .await
        .unwrap();
    }
    let committed = txn.commit_v2().await.unwrap();
    assert_eq!(committed.token().logical_sequence(), 2);
    for block in 0..9 {
        assert_eq!(
            store
                .get(format!("key-{:08}", block * 256).as_bytes())
                .await
                .unwrap(),
            Some(Bytes::from_static(b"changed"))
        );
    }
}

#[tokio::test]
async fn authority8_active_id_absence_authenticates_its_boundary_index() {
    let store = store();
    let mut seed = transaction(&store, "seed-boundary").await;
    seed.stage_projection_intent_v2("middle", "catalog", Bytes::from_static(b"payload"))
        .await
        .unwrap();
    let committed = seed.commit_v2().await.unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(
        &store
            .retention
            .get_raw(
                &store
                    .paths
                    .manifest_object(committed.token().authority_manifest_id()),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let root_hex = manifest
        .get("active_id_root")
        .unwrap()
        .get("directory_root_hex")
        .unwrap()
        .as_str()
        .unwrap();
    let directory = directory::Directory::new(store.retention.clone(), &store.scope).unwrap();
    let root = directory
        .decode_root(&hex::decode(root_hex).unwrap())
        .unwrap();
    let leaf = directory
        .lookup(&root, b"middle", &mut directory::ReadBudget::default())
        .await
        .unwrap()
        .unwrap();
    let descriptor: physical::Descriptor = serde_json::from_slice(
        &store
            .retention
            .get_raw(&format!(
                "{}/physical/descriptors/{}.json",
                store.paths.base_prefix(),
                hex::encode(leaf.digest)
            ))
            .await
            .unwrap(),
    )
    .unwrap();
    store
        .retention
        .put_raw(
            &store.paths.segment_index(&descriptor.segment.segment_id),
            Bytes::from_static(b"corrupt boundary index"),
            arco_core::storage::WritePrecondition::None,
        )
        .await
        .unwrap();
    let mut txn = transaction(&store, "outside-boundary").await;
    assert!(
        txn.stage_projection_intent_v2("outside", "catalog", Bytes::from_static(b"payload"))
            .await
            .is_err(),
        "ID absence must authenticate the adjacent physical index"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn authority8_predicate_and_rewrite_blocks_share_one_transaction_ceiling() {
    let store = store();
    let rows = (0..17 * 256).map(|index| SyntheticKvEntry {
        key: format!("key-{index:08}").into_bytes(),
        generation: 1,
        value: Some(vec![b'v'; 128]),
    });
    let original = store
        .install_synthetic_genesis(
            "combined-selection",
            1,
            17 * 256,
            rows,
            0,
            std::iter::empty(),
        )
        .await
        .unwrap();
    let mut txn = transaction(&store, "combined-selection").await;
    for block in 0..9 {
        assert!(
            txn.get(format!("key-{:08}", block * 256).as_bytes())
                .await
                .unwrap()
                .is_some()
        );
    }
    for block in 9..17 {
        txn.put(
            format!("key-{:08}", block * 256).as_bytes(),
            Bytes::from_static(b"changed"),
        )
        .await
        .unwrap();
    }
    assert!(
        matches!(
            txn.commit_v2().await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ),
        "nine predicate blocks plus eight other rewrite blocks exceed the shared sixteen-block bound"
    );
    assert_eq!(store.current_state_token().await.unwrap(), original);
}

#[tokio::test]
async fn authority8_rejects_v1_projection_staging_before_mutation() {
    let store = store();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        txn.stage_projection_intent("old", "catalog", Bytes::new())
            .await,
        Err(CatalogError::UnsupportedAuthorityFormat { .. })
    ));
    assert!(matches!(
        txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new("old", Bytes::new()))
            .await,
        Err(CatalogError::UnsupportedAuthorityFormat { .. })
    ));
    assert!(txn.projection_intents.is_empty() && txn.outbox.is_empty());
}

#[tokio::test]
async fn authority8_rejects_checkpoint_and_old_outbox_before_genesis() {
    let store = store();
    assert!(matches!(
        store.checkpoint(CheckpointOptions::default()).await,
        Err(CatalogError::UnsupportedAuthorityFormat { .. })
    ));
    assert!(matches!(
        store.current_projection_outbox().await,
        Err(CatalogError::UnsupportedAuthorityFormat { .. })
    ));
}

#[tokio::test]
async fn authority8_requires_explicit_current_selection_but_retained_reads_dispatch_by_witness() {
    let store = store();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.set_logical_operation("isolation", "test", &"a1".repeat(32))
        .unwrap();
    let predicted = txn.predicted_state_token().unwrap();
    assert!(store.read_at(predicted).await.is_err());
    txn.put(b"key", Bytes::from_static(b"value")).await.unwrap();
    let committed = txn.commit_v2().await.unwrap();
    let production =
        ControlMvpStateStore::new(store.retention.clone(), store.scope.clone()).unwrap();
    assert!(matches!(
        production.begin_control_txn(TxnOptions::default()).await,
        Err(CatalogError::UnsupportedAuthorityFormat { .. })
    ));
    let retained = production.read_at(committed.token().clone()).await.unwrap();
    assert_eq!(
        retained.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );
}

async fn transaction(store: &ControlMvpStateStore, operation: &str) -> ControlMvpTxn {
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.set_logical_operation(operation, "test", &"a1".repeat(32))
        .unwrap();
    txn
}

#[tokio::test]
async fn authority8_competing_writers_preserve_point_and_empty_range_observations() {
    let store = store();
    let mut seed = transaction(&store, "seed").await;
    seed.put(b"left", Bytes::from_static(b"left"))
        .await
        .unwrap();
    seed.put(b"right", Bytes::from_static(b"right"))
        .await
        .unwrap();
    seed.commit_v2().await.unwrap();
    let mut first = transaction(&store, "first").await;
    let mut second = transaction(&store, "second").await;
    assert!(first.get(b"middle").await.unwrap().is_none());
    first
        .assert_range_empty(KeyRange::new(b"middle".to_vec(), b"n".to_vec()))
        .await
        .unwrap();
    first
        .put(b"winner", Bytes::from_static(b"first"))
        .await
        .unwrap();
    second
        .put(b"middle", Bytes::from_static(b"inserted"))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let other = barrier.clone();
    let (a, b) = tokio::join!(
        async {
            barrier.wait().await;
            first.commit_v2().await
        },
        async {
            other.wait().await;
            second.commit_v2().await
        },
    );
    assert_ne!(
        a.is_ok(),
        b.is_ok(),
        "only one pinned candidate may publish"
    );
    assert!(matches!(
        a.as_ref().err().or_else(|| b.as_ref().err()),
        Some(CatalogError::CasFailed { .. })
    ));
    let observed = store.get(b"middle").await.unwrap();
    assert_eq!(observed.is_some(), b.is_ok());
}

#[tokio::test]
async fn authority8_range_empty_ignores_tombstones_but_witnesses_resurrection() {
    let store = store();
    let range = KeyRange::new(b"a/".to_vec(), b"a0".to_vec());
    let mut seed = transaction(&store, "seed").await;
    seed.put(b"a/b", Bytes::from_static(b"v1")).await.unwrap();
    seed.commit_v2().await.unwrap();
    let mut delete = transaction(&store, "delete").await;
    delete.delete(b"a/b").await.unwrap();
    delete.commit_v2().await.unwrap();

    // The bounded range branch must not count the retained tombstone.
    let mut empty = transaction(&store, "empty").await;
    empty.assert_range_empty(range.clone()).await.unwrap();
    empty.put(b"c", Bytes::from_static(b"after")).await.unwrap();
    empty.commit_v2().await.unwrap();
    assert!(store.get(b"a/b").await.unwrap().is_none());
    assert_eq!(
        store.get(b"c").await.unwrap(),
        Some(Bytes::from_static(b"after"))
    );

    // The bounded witness still covers the tombstone, so a concurrent
    // resurrection conflicts at commit.
    let mut stale = transaction(&store, "stale").await;
    stale.assert_range_empty(range).await.unwrap();
    stale.put(b"d", Bytes::from_static(b"stale")).await.unwrap();
    let mut resurrect = transaction(&store, "resurrect").await;
    resurrect
        .put(b"a/b", Bytes::from_static(b"v2"))
        .await
        .unwrap();
    resurrect.commit_v2().await.unwrap();
    assert!(matches!(
        stale.commit_v2().await,
        Err(CatalogError::CasFailed { .. } | CatalogError::PreconditionFailed { .. })
    ));
    assert!(store.get(b"d").await.unwrap().is_none());
}

#[tokio::test]
async fn authority8_deleted_generations_and_retained_values_survive_recreation() {
    let store = store();
    let mut seed = transaction(&store, "seed").await;
    seed.put(b"key", Bytes::from_static(b"first"))
        .await
        .unwrap();
    let original = seed.commit_v2().await.unwrap();
    let mut delete = transaction(&store, "delete").await;
    delete.assert_generation(b"key", 1).await.unwrap();
    delete.delete(b"key").await.unwrap();
    delete.commit_v2().await.unwrap();
    let mut recreate = transaction(&store, "recreate").await;
    assert!(recreate.get(b"key").await.unwrap().is_none());
    assert!(recreate.assert_generation(b"key", 1).await.is_err());
    recreate.assert_absent(b"key").await.unwrap();
    if let TransactionBase::Bounded(base) = &recreate.base {
        let deleted = base.get(&store, b"key").await.unwrap().unwrap();
        assert!(deleted.tombstone);
        assert_eq!(deleted.generation, 2);
        assert!(base.get(&store, b"never-present").await.unwrap().is_none());
    } else {
        panic!("expected bounded base");
    }
    recreate
        .put(b"key", Bytes::from_static(b"second"))
        .await
        .unwrap();
    recreate.commit_v2().await.unwrap();
    let retained = store.read_at(original.token().clone()).await.unwrap();
    assert_eq!(
        retained.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"first"))
    );
    assert_eq!(
        transaction(&store, "observe")
            .await
            .get(b"key")
            .await
            .unwrap()
            .unwrap()
            .generation(),
        Some(3)
    );
}

#[tokio::test]
async fn authority8_rejects_stale_and_unclaimed_writer_epochs() {
    let store = store();
    transaction(&store, "seed").await.commit_v2().await.unwrap();
    let advanced = store.clone().with_writer_epoch(1).unwrap();
    assert!(matches!(
        advanced.begin_control_txn(TxnOptions::default()).await,
        Err(CatalogError::PreconditionFailed { .. })
    ));

    let mut pointer = store.load_pointer().await.unwrap();
    pointer.writer_epoch = 1;
    store
        .retention
        .put_raw(
            &store.paths.current_pointer(),
            encode_json(&pointer, "test fenced pointer").unwrap(),
            arco_core::storage::WritePrecondition::None,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.begin_control_txn(TxnOptions::default()).await,
        Err(CatalogError::StaleWriterEpoch { .. })
    ));
    transaction(&advanced, "adopted")
        .await
        .commit_v2()
        .await
        .unwrap();
    assert_eq!(store.load_pointer().await.unwrap().writer_epoch, 1);
}

#[tokio::test]
async fn authority8_external_reclamation_fence_rejects_a_previously_pinned_writer() {
    let store = store();
    transaction(&store, "seed-fence")
        .await
        .commit_v2()
        .await
        .unwrap();
    let mut stale = transaction(&store, "stale-reclamation").await;
    stale
        .put(b"stale", Bytes::from_static(b"must not publish"))
        .await
        .unwrap();
    let mut pointer = store.load_pointer().await.unwrap();
    pointer.reclamation_generation = 1;
    store
        .retention
        .put_raw(
            &store.paths.current_pointer(),
            encode_json(&pointer, "external reclamation fence").unwrap(),
            arco_core::storage::WritePrecondition::None,
        )
        .await
        .unwrap();
    assert!(matches!(
        stale.commit_v2().await,
        Err(CatalogError::CasFailed { .. })
    ));
    assert!(store.get(b"stale").await.unwrap().is_none());
    transaction(&store, "fresh-reclamation")
        .await
        .commit_v2()
        .await
        .unwrap();
    assert_eq!(
        store.load_pointer().await.unwrap().reclamation_generation,
        1
    );
}

#[tokio::test]
async fn authority8_fence_claim_authenticates_the_selected_manifest_before_publication() {
    let store = store();
    let committed = transaction(&store, "seed").await.commit_v2().await.unwrap();
    let original = store
        .retention
        .get_raw(&store.paths.current_pointer())
        .await
        .unwrap();
    let manifest = store
        .paths
        .manifest_object(committed.token().authority_manifest_id());
    store
        .retention
        .put_raw(
            &manifest,
            Bytes::from_static(b"corrupt manifest"),
            arco_core::storage::WritePrecondition::None,
        )
        .await
        .unwrap();
    assert!(store.clone().claim_writer_authority().await.is_err());
    assert_eq!(
        store
            .retention
            .get_raw(&store.paths.current_pointer())
            .await
            .unwrap(),
        original
    );
}

#[tokio::test]
async fn authority8_delivery_continuation_retains_exact_incarnations_across_trim_and_restage() {
    let store = store();
    let mut seed = transaction(&store, "outbox-seed").await;
    for id in ["a", "b", "c"] {
        seed.stage_projection_intent_v2(id, "catalog", Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    let committed = seed.commit_v2().await.unwrap();
    let old_b = committed.projection_intents()[1].clone();
    let first = store
        .projection_outbox_page_v2(Some(committed.token().clone()), None, 1)
        .await
        .unwrap();
    assert_eq!(first.records()[0].intent_id(), "a");
    let continuation = first.continuation().unwrap().clone();

    let mut trim = transaction(&store, "trim-b").await;
    trim.trim_projection_intents_v2(std::slice::from_ref(&old_b))
        .await
        .unwrap();
    assert!(
        trim.stage_projection_intent_v2("b", "catalog", Bytes::from_static(b"payload"))
            .await
            .is_err()
    );
    trim.commit_v2().await.unwrap();
    let mut restage = transaction(&store, "restage-b").await;
    restage
        .stage_projection_intent_v2("b", "catalog", Bytes::from_static(b"new payload"))
        .await
        .unwrap();
    let restaged = restage.commit_v2().await.unwrap();

    let second = store
        .projection_outbox_page_v2(None, Some(continuation.clone()), 1)
        .await
        .unwrap();
    assert_eq!(second.records(), std::slice::from_ref(&old_b));
    let current = store
        .projection_outbox_page_v2(None, None, 10)
        .await
        .unwrap();
    let order = current
        .records()
        .iter()
        .map(|intent| {
            (
                intent.intent_id(),
                intent.source_logical_sequence(),
                intent.ordinal(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(order, [("a", 1, 0), ("c", 1, 2), ("b", 3, 0)]);
    assert!(
        store
            .projection_outbox_page_v2(Some(restaged.token().clone()), Some(continuation), 1)
            .await
            .is_err()
    );
    let mut stale_trim = transaction(&store, "stale-trim").await;
    assert!(
        stale_trim
            .trim_projection_intents_v2(&[old_b])
            .await
            .is_err()
    );
}
