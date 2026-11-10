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

#[tokio::test]
async fn authority8_gc_dry_run_bounds_reads_across_historical_manifests() {
    let store = store();
    for ordinal in 0_u64..512 {
        let mut txn = transaction(&store, &format!("gc-inventory-{ordinal}")).await;
        txn.put(b"live", Bytes::copy_from_slice(&ordinal.to_be_bytes()))
            .await
            .unwrap();
        txn.commit_v2().await.unwrap();
    }
    let orphan = store.paths.state_object(&"ee".repeat(32));
    store
        .retention
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    assert!(
        matches!(
            worker
                .plan_gc_at(Utc::now() + ChronoDuration::days(40), Vec::new())
                .await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ),
        "one GC page must share its read ceiling across every protected manifest"
    );
    assert!(store.retention.head_raw(&orphan).await.unwrap().is_some());
    assert_eq!(
        store.get(b"live").await.unwrap(),
        Some(Bytes::copy_from_slice(&511_u64.to_be_bytes()))
    );
}

#[tokio::test]
async fn authority8_gc_dry_run_protects_current_directory_closure() {
    let store = store();
    store
        .install_synthetic_genesis(
            "gc-current-closure",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(b"value".to_vec()),
            }],
            0,
            std::iter::empty(),
        )
        .await
        .expect("synthetic authority");
    let orphans = [
        format!(
            "control/directory/v1/domains/{}/pages/{}",
            store.scope.domain(),
            "aa".repeat(32)
        ),
        format!(
            "{}/physical/descriptors/{}.json",
            store.paths.base_prefix(),
            "bb".repeat(32)
        ),
        store.paths.state_object(&"cc".repeat(32)),
        store.paths.segment_index(&"dd".repeat(32)),
    ];
    for path in &orphans {
        store
            .retention
            .put_raw(
                path,
                Bytes::from_static(b"orphan"),
                arco_core::storage::WritePrecondition::DoesNotExist,
            )
            .await
            .expect("orphan object");
    }
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let mut found = Vec::new();
    let mut cursor = None;
    loop {
        let plan = worker
            .plan_gc_page_at(
                Utc::now() + ChronoDuration::days(40),
                Vec::new(),
                cursor.as_deref(),
            )
            .await
            .expect("authority-8 dry run");
        found.extend(
            plan.candidates()
                .iter()
                .map(|candidate| candidate.path().to_owned()),
        );
        cursor = plan.continuation().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    let mut expected = orphans.to_vec();
    expected.sort();
    found.sort();
    assert_eq!(
        found, expected,
        "current closure is protected and both orphan prefixes are eligible"
    );
    assert_eq!(
        store.get(b"live").await.expect("current value"),
        Some(Bytes::from_static(b"value"))
    );
    assert!(matches!(
        worker
            .collect_gc_at(Utc::now() + ChronoDuration::days(40), Vec::new())
            .await,
        Err(CatalogError::UnsupportedAuthorityFormat { .. })
    ));
    for path in &orphans {
        assert!(
            store
                .retention
                .head_raw(path)
                .await
                .expect("orphan HEAD")
                .is_some(),
            "dry-run must not authorize deletion"
        );
    }
}

#[tokio::test]
async fn authority8_gc_dry_run_fails_closed_on_missing_live_descriptor() {
    let store = store();
    store
        .install_synthetic_genesis(
            "gc-missing-descriptor",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(b"value".to_vec()),
            }],
            0,
            std::iter::empty(),
        )
        .await
        .expect("synthetic authority");
    let descriptor_prefix = format!("{}/physical/descriptors/", store.paths.base_prefix());
    let page = store
        .retention
        .list_page_meta(&descriptor_prefix, None, 1)
        .await
        .expect("descriptor inventory");
    let descriptor = page
        .objects
        .first()
        .expect("live descriptor")
        .path
        .to_string();
    store
        .retention
        .delete(&descriptor)
        .await
        .expect("remove descriptor");
    let orphan = store.paths.state_object(&"ee".repeat(32));
    store
        .retention
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("orphan object");
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    assert!(
        worker
            .plan_gc_at(Utc::now() + ChronoDuration::days(40), Vec::new())
            .await
            .is_err()
    );
    assert!(
        store
            .retention
            .head_raw(&orphan)
            .await
            .expect("orphan HEAD")
            .is_some()
    );
}

#[tokio::test]
async fn authority8_gc_dry_run_rejects_corrupt_retained_selector() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([41; 32]));
    store
        .install_synthetic_genesis("gc-corrupt-retained", 1, 0, [], 0, [])
        .await
        .expect("synthetic authority");
    let orphan = store.paths.state_object(&"ee".repeat(32));
    store
        .retention
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("orphan object");
    store
        .retention
        .put_raw(
            &format!("{}/retained/v1/current.json", store.paths.base_prefix()),
            Bytes::from_static(b"{"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("corrupt retained selector");
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    assert!(
        worker
            .plan_gc_at(Utc::now() + ChronoDuration::days(40), Vec::new())
            .await
            .is_err(),
        "a corrupt retained selector cannot authorize an orphan candidate"
    );
    assert!(
        store
            .retention
            .head_raw(&orphan)
            .await
            .expect("orphan HEAD")
            .is_some()
    );
}

#[tokio::test]
async fn authority8_gc_dry_run_rejects_corrupt_workspace_pin() {
    let store = store();
    store
        .install_synthetic_genesis("gc-corrupt-pin", 1, 0, [], 0, [])
        .await
        .expect("synthetic authority");
    let orphan = store.paths.state_object(&"ef".repeat(32));
    store
        .retention
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("orphan object");
    store
        .retention
        .put_raw(
            "retention/pins/corrupt/latest.json",
            Bytes::from_static(b"{"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("corrupt pin selector");
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    assert!(
        worker
            .plan_gc_at(Utc::now() + ChronoDuration::days(40), Vec::new())
            .await
            .is_err(),
        "an invalid workspace pin cannot authorize a GC candidate"
    );
    assert!(
        store
            .retention
            .head_raw(&orphan)
            .await
            .expect("orphan HEAD")
            .is_some()
    );
}

#[tokio::test]
async fn authority8_gc_dry_run_rejects_unresolved_armed_restore_without_prepared_bytes() {
    use crate::retention_coordination::{
        ArmedRestorePublicationIntent, RetainedPointerIntentPrecondition, RetentionMutationEpoch,
        RetentionMutationKind,
    };
    use crate::workspace_io_budget::WorkspaceIoBudget;

    let store = store();
    store
        .install_synthetic_genesis("gc-armed-restore", 1, 0, [], 0, [])
        .await
        .expect("synthetic authority");
    let orphan = store.paths.state_object(&"ef".repeat(32));
    store
        .retention
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("old orphan");
    let scoped = store
        .retention
        .as_legacy_scoped()
        .expect("workspace root")
        .clone();
    let mut budget = WorkspaceIoBudget::new();
    let mut guard = budget
        .acquire_retention_lock(scoped.clone(), "gc-armed-restore")
        .await
        .expect("retention lock");
    let mut epoch = RetentionMutationEpoch::claim_bounded(
        scoped,
        &mut guard,
        RetentionMutationKind::WorkspaceRestoreApply,
        "gc-armed-restore",
        &mut budget,
    )
    .await
    .expect("restore epoch");
    let candidate = "22".repeat(32);
    let mut mutation = epoch.begin_bounded_mutation();
    mutation
        .arm_restore_publication(
            ArmedRestorePublicationIntent {
                intent_type: "arco.restore.publication-intent".into(),
                version: 1,
                domain: store.scope.domain().into(),
                plan_sha256: "11".repeat(32),
                owner_generation: 1,
                candidate_id: candidate.clone(),
                prepared_path: format!(
                    "{}/restore/v7/{candidate}/prepared.json",
                    store.paths.base_prefix()
                ),
                prepared_sha256: "33".repeat(32),
                precondition: RetainedPointerIntentPrecondition::DoesNotExist {},
            },
            &mut guard,
            &mut budget,
        )
        .await
        .expect("arm restore publication");
    mutation
        .finish(Ok(()))
        .expect("leave armed epoch in flight");

    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    assert!(
        worker
            .plan_gc_at(Utc::now() + ChronoDuration::days(40), Vec::new())
            .await
            .is_err(),
        "an unresolved restore publication must block GC even without prepared bytes"
    );
    assert!(store.retention.head_raw(&orphan).await.unwrap().is_some());
}

#[tokio::test]
async fn authority8_commit_waits_for_retention_gc_lock_before_staging_reusable_pages() {
    let store = store();
    store
        .install_synthetic_genesis(
            "gc-writer-lock",
            1,
            0,
            std::iter::empty(),
            0,
            std::iter::empty(),
        )
        .await
        .expect("synthetic authority");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.set_logical_operation_v2("gc-lock-op", "kv", &"11".repeat(32))
        .expect("operation");
    txn.put(b"live", Bytes::from_static(b"value"))
        .await
        .expect("stage write");
    let guard = DistributedLock::new(Arc::new(store.retention.clone()), RETENTION_GC_LOCK_PATH)
        .acquire_with_operation(
            RETENTION_GC_LOCK_TTL,
            RETENTION_GC_LOCK_MAX_RETRIES,
            Some("authority8-gc-lock-test".into()),
        )
        .await
        .expect("hold GC lock");
    let mut commit = tokio::spawn(async move { txn.commit_v2().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut commit)
            .await
            .is_err(),
        "bounded commit must wait while GC owns the retention lock"
    );
    guard.release().await.expect("release GC lock");
    commit
        .await
        .expect("join commit")
        .expect("commit after lock release");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the post-fence trim and reusable-object inventory are one regression"
)]
async fn authority8_gc_refuses_reusable_empty_pages_and_key_objects() {
    let store = store();
    let intent = ProjectionIntentV2::new(
        "gc-notice",
        "catalog",
        store.scope.clone(),
        1,
        "a1".repeat(32),
        0,
        b"notice",
    )
    .expect("intent");
    store
        .install_synthetic_genesis(
            "gc-reused-empty-page",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(b"value".to_vec()),
            }],
            1,
            [intent.clone()],
        )
        .await
        .expect("synthetic authority");
    let directory =
        directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
    let empty = directory.empty_root().await.expect("unattached empty page");
    let encoded = empty.encode();
    let orphan = format!(
        "control/directory/v1/domains/{}/pages/{}",
        store.scope.domain(),
        hex::encode(&encoded[encoded.len() - 32..])
    );
    let key_object = format!(
        "control/directory/v1/domains/{}/keys/{}",
        store.scope.domain(),
        "cc".repeat(32)
    );
    store
        .retention
        .put_raw(
            &key_object,
            Bytes::from_static(b"unattached long key"),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .expect("unattached key object");
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let now = Utc::now() + ChronoDuration::days(40);
    let mut cursor = None;
    let mut planned = Vec::new();
    loop {
        let page = worker
            .plan_gc_page_at(now, Vec::new(), cursor.as_deref())
            .await
            .expect("dry-run page");
        planned.extend(
            page.candidates()
                .iter()
                .map(|candidate| candidate.path().to_owned()),
        );
        cursor = page.continuation().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        !planned.contains(&orphan),
        "empty page can be reused after a GC fence"
    );
    assert!(
        !planned.contains(&key_object),
        "long-key object can be reused after a GC fence"
    );
    let head_version = store
        .retention
        .head_raw(&store.paths.current_pointer())
        .await
        .expect("HEAD")
        .expect("selected HEAD")
        .version;
    worker
        .fence_reclamation(&head_version)
        .await
        .expect("GC fence");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin after fence");
    txn.set_logical_operation_v2("gc-trim-notice", "projection_trim", &"12".repeat(32))
        .expect("operation");
    txn.trim_projection_intents_v2(&[intent])
        .await
        .expect("stage trim");
    txn.commit_v2().await.expect("publish post-fence successor");
    assert!(
        store
            .projection_outbox_page_v2(None, None, 1)
            .await
            .expect("post-fence outbox read")
            .records()
            .is_empty()
    );
}

#[tokio::test]
async fn authority8_post_fence_successor_survives_delayed_empty_page_delete() {
    let store = store();
    let intent = ProjectionIntentV2::new(
        "gc-notice",
        "catalog",
        store.scope.clone(),
        1,
        "a1".repeat(32),
        0,
        b"notice",
    )
    .expect("intent");
    store
        .install_synthetic_genesis(
            "gc-delayed-empty-page",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(b"value".to_vec()),
            }],
            1,
            [intent.clone()],
        )
        .await
        .expect("synthetic authority");
    let directory =
        directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
    let empty = directory
        .empty_root()
        .await
        .expect("old unattached empty page");
    let encoded = empty.encode();
    let orphan = format!(
        "control/directory/v1/domains/{}/pages/{}",
        store.scope.domain(),
        hex::encode(encoded.get(encoded.len() - 32..).expect("root digest"))
    );
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let head_version = store
        .retention
        .head_raw(&store.paths.current_pointer())
        .await
        .expect("HEAD")
        .expect("current HEAD")
        .version;
    worker
        .fence_reclamation(&head_version)
        .await
        .expect("GC fence");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin after fence");
    txn.set_logical_operation_v2("gc-trim-notice", "projection_trim", &"12".repeat(32))
        .expect("operation");
    txn.trim_projection_intents_v2(&[intent])
        .await
        .expect("stage trim");
    txn.commit_v2().await.expect("post-fence successor");
    store
        .retention
        .delete(&orphan)
        .await
        .expect("delayed DELETE");
    assert!(
        store
            .projection_outbox_page_v2(None, None, 1)
            .await
            .expect("current read after delayed DELETE")
            .records()
            .is_empty()
    );
}

#[tokio::test]
async fn authority8_transaction_manifest_binds_v2_directory_objects() {
    let store = store();
    store
        .install_synthetic_genesis("directory-version-binding", 1, 0, [], 0, [])
        .await
        .expect("synthetic authority");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("transaction");
    txn.set_logical_operation_v2("directory-version", "kv", &"11".repeat(32))
        .expect("operation");
    txn.put(b"live", Bytes::from_static(b"value"))
        .await
        .expect("staging");
    let candidate_id = txn.candidate_manifest_id().to_owned();
    txn.commit_v2().await.expect("publish");
    let bytes = store
        .storage
        .get(&store.paths.manifest_object(&candidate_id))
        .await
        .expect("manifest");
    let wire: serde_json::Value = serde_json::from_slice(&bytes).expect("manifest JSON");
    assert_eq!(wire["directory_object_version"], 2);
}

#[tokio::test]
async fn authority8_synthetic_genesis_manifest_binds_v2_directory_objects() {
    let store = store();
    store
        .install_synthetic_genesis("genesis-directory-version", 1, 0, [], 0, [])
        .await
        .expect("synthetic authority");
    let bytes = store
        .storage
        .get(
            &store
                .paths
                .manifest_object("synthetic-genesis-directory-version"),
        )
        .await
        .expect("manifest");
    let wire: serde_json::Value = serde_json::from_slice(&bytes).expect("manifest JSON");
    assert_eq!(wire["directory_object_version"], 2);
}

#[tokio::test]
async fn authority8_post_fence_successor_survives_delayed_long_key_delete() {
    let store = store();
    store
        .install_synthetic_genesis("gc-delayed-long-key", 1, 0, [], 0, [])
        .await
        .expect("synthetic authority");
    let key = vec![b'k'; 100];
    let directory =
        directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
    let mut orphan_builder = directory.builder();
    orphan_builder
        .push(directory::Leaf {
            first: key.clone(),
            last: key.clone(),
            rows: 1,
            bytes: 100,
            digest: [7; 32],
        })
        .await
        .expect("unattached leaf");
    orphan_builder.finish().await.expect("unattached old root");
    let prefix = format!(
        "control/directory/v1/domains/{}/keys/",
        store.scope.domain()
    );
    let page = store
        .retention
        .list_page_meta(&prefix, None, 2)
        .await
        .expect("old key object");
    let [old_key] = page.objects.as_slice() else {
        panic!("expected one unattached V1 key object");
    };
    let orphan = old_key.path.to_string();
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let head_version = store
        .retention
        .head_raw(&store.paths.current_pointer())
        .await
        .expect("HEAD")
        .expect("current HEAD")
        .version;
    worker
        .fence_reclamation(&head_version)
        .await
        .expect("GC fence");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin after fence");
    txn.set_logical_operation_v2("gc-put-long-key", "kv", &"13".repeat(32))
        .expect("operation");
    txn.put(&key, Bytes::from_static(b"value"))
        .await
        .expect("stage put");
    txn.commit_v2().await.expect("post-fence successor");
    store
        .retention
        .delete(&orphan)
        .await
        .expect("delayed DELETE");
    assert_eq!(
        store
            .get(&key)
            .await
            .expect("current read after delayed DELETE"),
        Some(Bytes::from_static(b"value"))
    );
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
async fn authority8_forged_attachment_cannot_make_an_unpublished_candidate_readable() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([8; 32]));
    transaction(&store, "seed").await.commit_v2().await.unwrap();
    let mut first = transaction(&store, "first-candidate").await;
    first
        .put(b"first", Bytes::from_static(b"first"))
        .await
        .unwrap();
    let first_token = first.predicted_state_token().unwrap();
    let mut second = transaction(&store, "second-candidate").await;
    second
        .put(b"second", Bytes::from_static(b"second"))
        .await
        .unwrap();
    let second_token = second.predicted_state_token().unwrap();
    let (first_result, second_result) = tokio::join!(first.commit_v2(), second.commit_v2());
    let candidate = match (first_result.is_err(), second_result.is_err()) {
        (true, false) => first_token,
        (false, true) => second_token,
        _ => panic!("exactly one candidate must lose HEAD publication"),
    };
    let manifest_path = store
        .paths
        .manifest_object(candidate.authority_manifest_id());
    let manifest = store.retention.get_raw(&manifest_path).await.unwrap();
    let reference = PersistedAuthorityReference::new(
        IMPLEMENTATION,
        store.scope.clone(),
        PersistedAuthorityKind::StateToken,
        candidate.authority_manifest_id(),
        candidate.logical_sequence(),
        manifest_path,
        prefixed_sha256(&manifest),
        None,
        None,
        Utc::now() + ChronoDuration::hours(1),
    )
    .unwrap();
    let attachment_path = format!(
        "{}/retained-authority8/{}.json",
        store.paths.base_prefix(),
        reference.manifest_id()
    );
    store
        .retention
        .put_raw(
            &attachment_path,
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "record_type": "control_mvp_retained_authority8",
                    "version": 1,
                    "reference": reference,
                    "binding": vec![8_u8; 32],
                }))
                .unwrap(),
            ),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    assert!(
        store.resolve_persisted_reference(&reference).await.is_err(),
        "a forged attachment must not make a detached candidate retained authority"
    );
}

#[tokio::test]
async fn authority8_selected_retained_source_survives_more_than_thirty_two_head_advances() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([9; 32]));
    let mut source = transaction(&store, "retained-source").await;
    source
        .put(b"retained", Bytes::from_static(b"source"))
        .await
        .unwrap();
    let source = source.commit_v2().await.unwrap().token().clone();
    let reference = store
        .persist_state_reference(&source, Utc::now() + ChronoDuration::days(1))
        .await
        .unwrap();
    store
        .install_test_retained_source(&reference)
        .await
        .unwrap();
    for advance in 0..33 {
        let mut txn = transaction(&store, &format!("advance-{advance}")).await;
        txn.put(
            format!("advance-{advance}").as_bytes(),
            Bytes::from_static(b"advanced"),
        )
        .await
        .unwrap();
        txn.commit_v2().await.unwrap();
    }
    let reader = store.resolve_persisted_reference(&reference).await.unwrap();
    assert_eq!(
        reader.get(b"retained").await.unwrap(),
        Some(Bytes::from_static(b"source"))
    );
}

#[tokio::test]
async fn authority8_selected_retained_source_rejects_a_covering_singleton_gap() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([10; 32]));
    let source = transaction(&store, "retained-gap")
        .await
        .commit_v2()
        .await
        .unwrap();
    let reference = store
        .persist_state_reference(source.token(), Utc::now() + ChronoDuration::days(1))
        .await
        .unwrap();
    store
        .install_test_retained_singleton_gap(&reference)
        .await
        .unwrap();
    transaction(&store, "retained-gap-advance")
        .await
        .commit_v2()
        .await
        .unwrap();
    assert!(
        store.resolve_persisted_reference(&reference).await.is_err(),
        "a covering retained-directory leaf must not prove exact membership"
    );
}

#[tokio::test]
async fn authority8_selected_retained_source_rejects_a_different_configured_binding() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([11; 32]));
    let source = transaction(&store, "retained-binding")
        .await
        .commit_v2()
        .await
        .unwrap();
    let reference = store
        .persist_state_reference(source.token(), Utc::now() + ChronoDuration::days(1))
        .await
        .unwrap();
    store
        .install_test_retained_source(&reference)
        .await
        .unwrap();
    transaction(&store, "retained-binding-advance")
        .await
        .commit_v2()
        .await
        .unwrap();
    let other =
        ControlMvpStateStore::new_synthetic_bounded(store.retention.clone(), store.scope.clone())
            .unwrap()
            .with_durable_authority_binding(DurableAuthorityBinding::new([12; 32]));
    assert!(other.resolve_persisted_reference(&reference).await.is_err());
}

#[tokio::test]
async fn authority8_selected_retained_source_rejects_deadline_equality() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([13; 32]));
    let source = transaction(&store, "retained-deadline")
        .await
        .commit_v2()
        .await
        .unwrap();
    let deadline = Utc::now() + ChronoDuration::hours(1);
    let reference = store
        .persist_state_reference(source.token(), deadline)
        .await
        .unwrap();
    store
        .install_test_retained_source(&reference)
        .await
        .unwrap();
    assert!(
        store
            .resolve_persisted_reference_at(&reference, deadline)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn authority8_selected_retained_source_rejects_a_generation_jump() {
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([14; 32]));
    let source = transaction(&store, "retained-generation")
        .await
        .commit_v2()
        .await
        .unwrap();
    let reference = store
        .persist_state_reference(source.token(), Utc::now() + ChronoDuration::days(1))
        .await
        .unwrap();
    store
        .install_test_retained_generation_jump(&reference)
        .await
        .unwrap();
    transaction(&store, "retained-generation-advance")
        .await
        .commit_v2()
        .await
        .unwrap();
    assert!(store.resolve_persisted_reference(&reference).await.is_err());
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

#[tokio::test]
async fn authority8_rejects_put_with_expiry_before_staging() {
    let store = store();
    let mut seed = transaction(&store, "seed").await;
    seed.put(b"key", Bytes::from_static(b"first"))
        .await
        .unwrap();
    seed.commit_v2().await.unwrap();

    let mut txn = transaction(&store, "expiring").await;
    let error = txn
        .put_with_expiry(b"key", Bytes::from_static(b"second"), 1_900_000_000_000)
        .await
        .unwrap_err();
    assert!(
        matches!(error, CatalogError::Validation { .. }),
        "bounded authority must reject expiring writes with Validation, got {error:?}"
    );
    // Nothing was staged: the overlay still shows the committed value and a
    // following commit changes no key.
    let observed = txn.get(b"key").await.unwrap().unwrap();
    assert_eq!(observed.bytes(), &Bytes::from_static(b"first"));
    assert_eq!(observed.generation(), Some(1));
    assert!(
        txn.writes.is_empty(),
        "rejected expiry must not stage a write"
    );
    txn.commit_v2().await.unwrap();
    let after = transaction(&store, "observe")
        .await
        .get(b"key")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.bytes(), &Bytes::from_static(b"first"));
    assert_eq!(after.generation(), Some(1));
}

/// Bounded authority-8 planning cannot honour a restore key policy, so it
/// refuses a participant whose policy excludes anything rather than planning
/// a restore that would silently restore the excluded keys.
#[tokio::test]
async fn authority8_bounded_restore_refuses_a_restore_key_policy() {
    use crate::state_store::RestorePlanningContext;
    use crate::workspace_io_budget::{WorkspaceCaptureIo, WorkspaceIoBudget};
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([36; 32]));
    let mut txn = transaction(&store, "restore-key-policy-source").await;
    txn.put(b"\x03receipt", Bytes::from_static(b"receipt"))
        .await
        .unwrap();
    let token = txn.commit_v2().await.unwrap().token().clone();
    let now = Utc::now();
    let source = store
        .persist_state_reference(&token, now + ChronoDuration::days(2))
        .await
        .unwrap();
    let identity =
        RestoreAttemptIdentity::new(format!("rst_{}", Ulid::from(706_u128)), 1, "catalog").unwrap();
    let mut budget = WorkspaceIoBudget::new();
    let mut context = RestorePlanningContext::new(
        prefixed_sha256(b"workspace-request"),
        now,
        now + ChronoDuration::hours(24),
        now,
        WorkspaceCaptureIo::new(
            store.retention.as_legacy_scoped().expect("workspace root"),
            &mut budget,
        ),
    );
    let error = ControlMvpRestoreParticipant::new(store.clone())
        .with_key_policy(RestoreKeyPolicy::excluding([[0x03_u8]]).unwrap())
        .plan_restore_bounded(&source, &identity, &mut context)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            CatalogError::UnsupportedOperation { message } if message.contains("restore key policy")
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn authority8_bounded_restore_plans_the_authenticated_source_without_writes() {
    use crate::state_store::RestorePlanningContext;
    use crate::workspace_io_budget::{WorkspaceCaptureIo, WorkspaceIoBudget};
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([35; 32]));
    let mut txn = transaction(&store, "restore-plan7-source").await;
    txn.put(b"key", Bytes::from_static(b"source"))
        .await
        .unwrap();
    let token = txn.commit_v2().await.unwrap().token().clone();
    let now = Utc::now();
    let source = store
        .persist_state_reference(&token, now + ChronoDuration::days(2))
        .await
        .unwrap();
    let identity =
        RestoreAttemptIdentity::new(format!("rst_{}", Ulid::from(705_u128)), 1, "catalog").unwrap();
    let inventory = |objects: Vec<arco_core::storage::ObjectMeta>| {
        objects
            .into_iter()
            .map(|o| (o.path, o.version, o.size))
            .collect::<BTreeSet<_>>()
    };
    let before = inventory(
        store
            .retention
            .as_legacy_scoped()
            .expect("workspace root")
            .backend()
            .list("")
            .await
            .unwrap(),
    );
    let mut budget = WorkspaceIoBudget::new();
    let mut context = RestorePlanningContext::new(
        prefixed_sha256(b"workspace-request"),
        now,
        now + ChronoDuration::hours(24),
        now,
        WorkspaceCaptureIo::new(
            store.retention.as_legacy_scoped().expect("workspace root"),
            &mut budget,
        ),
    );
    let plan = ControlMvpRestoreParticipant::new(store.clone())
        .plan_restore_bounded(&source, &identity, &mut context)
        .await
        .expect("authenticated authority-8 source must produce Plan7");
    let wire = serde_json::to_value(&plan).unwrap();
    assert_eq!(wire["plan_kind"], "control_mvp_v7");
    assert_eq!(wire["version"], 7);
    assert_eq!(wire["owner_generation"], 1);
    assert_eq!(wire["source"], serde_json::to_value(&source).unwrap());
    assert_eq!(wire["identity"], serde_json::to_value(&identity).unwrap());
    let mut inspection = crate::state_store::RestoreBoundedInspectionContext::new(
        identity.restore_id().into(),
        identity.attempt(),
        identity.domain().into(),
        prefixed_sha256(&serde_jcs::to_vec(&plan).unwrap()),
        WorkspaceCaptureIo::new(
            store.retention.as_legacy_scoped().expect("workspace root"),
            &mut budget,
        ),
    );
    assert!(matches!(
        ControlMvpRestoreParticipant::new(store.clone())
            .inspect_restore_bounded(&plan, &mut inspection)
            .await
            .expect("unstarted Plan7 must have bounded read-only inspection"),
        RestoreParticipantInspection::Ready
    ));
    assert_eq!(
        before,
        inventory(
            store
                .retention
                .as_legacy_scoped()
                .expect("workspace root")
                .backend()
                .list("")
                .await
                .unwrap()
        ),
        "planning wrote artifacts"
    );
}

#[tokio::test]
async fn authority8_plan7_retries_keep_original_logical_identity_and_reject_tampering() {
    use crate::state_store::RestorePlanningContext;
    use crate::workspace_io_budget::{WorkspaceCaptureIo, WorkspaceIoBudget};
    let store = store().with_durable_authority_binding(DurableAuthorityBinding::new([37; 32]));
    let mut txn = transaction(&store, "plan7-source-past").await;
    txn.put(b"key", Bytes::from_static(b"source"))
        .await
        .unwrap();
    let token = txn.commit_v2().await.unwrap().token().clone();
    let now = Utc::now();
    let source = store
        .persist_state_reference(&token, now + ChronoDuration::days(2))
        .await
        .unwrap();
    store.install_test_retained_source(&source).await.unwrap();
    let mut txn = transaction(&store, "plan7-current").await;
    txn.put(b"key", Bytes::from_static(b"target"))
        .await
        .unwrap();
    let current = txn.commit_v2().await.unwrap().token().clone();
    let restore_id = format!("rst_{}", Ulid::from(707_u128));
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let mut wires = Vec::new();
    for (attempt, elapsed) in [(1, 0), (1, 1), (2, 2)] {
        let identity = RestoreAttemptIdentity::new(&restore_id, attempt, "catalog").unwrap();
        let mut budget = WorkspaceIoBudget::new();
        let mut context = RestorePlanningContext::new(
            prefixed_sha256(b"workspace-request"),
            now,
            now + ChronoDuration::hours(24),
            now + ChronoDuration::seconds(elapsed),
            WorkspaceCaptureIo::new(
                store.retention.as_legacy_scoped().expect("workspace root"),
                &mut budget,
            ),
        );
        let plan = adapter
            .plan_restore_bounded(&source, &identity, &mut context)
            .await
            .unwrap();
        assert!(matches!(
            adapter.inspect_restore(&plan).await,
            Err(CatalogError::UnsupportedOperation { .. })
        ));
        assert!(matches!(
            adapter.apply_restore(&plan, now).await,
            Err(CatalogError::UnsupportedOperation { .. })
        ));
        let wire = serde_json::to_value(&plan).unwrap();
        let decoded: PersistedRestoreParticipantPlan =
            serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(decoded, plan);
        assert_eq!(wire["source_logical_sequence"], source.logical_sequence());
        assert_eq!(wire["base_logical_sequence"], current.logical_sequence());
        assert_eq!(
            wire["result_logical_sequence"],
            current.logical_sequence() + 1
        );
        wires.push(wire);
    }
    let [first, retry, replacement] = wires.as_slice() else {
        panic!("three attempts")
    };
    assert_eq!(
        first, retry,
        "live retry time must not change immutable bytes"
    );
    assert_eq!(first["logical_commit_id"], replacement["logical_commit_id"]);
    assert_eq!(
        first["restore_request_digest"],
        replacement["restore_request_digest"]
    );
    assert_ne!(first["candidate_id"], replacement["candidate_id"]);
    for (field, value) in [
        ("owner_generation", serde_json::json!(2)),
        ("version", serde_json::json!(6)),
        (
            "workspace_request_sha256",
            serde_json::json!(prefixed_sha256(b"different-request")),
        ),
        ("source_kv_root_b64", serde_json::json!("YQ==")),
        ("restore_notice_payload_b64", serde_json::json!("YQ")),
        (
            "logical_commit_id",
            serde_json::json!(prefixed_sha256(b"forged-logical")),
        ),
        ("candidate_id", serde_json::json!("aa".repeat(32))),
        ("unknown_field", serde_json::json!(true)),
    ] {
        let mut changed = first.clone();
        changed[field] = value;
        assert!(
            serde_json::from_value::<PersistedRestoreParticipantPlan>(changed).is_err(),
            "accepted changed {field}"
        );
    }
}

#[tokio::test]
async fn authority8_genesis_waits_for_retention_gc_lock_before_staging() {
    use arco_core::StorageBackend as _;

    let store = store();
    let guard = DistributedLock::new(Arc::new(store.retention.clone()), RETENTION_GC_LOCK_PATH)
        .acquire_with_operation(
            RETENTION_GC_LOCK_TTL,
            RETENTION_GC_LOCK_MAX_RETRIES,
            Some("authority8-genesis-gc-lock-test".into()),
        )
        .await
        .expect("hold GC lock");
    let writer = store.clone();
    let mut genesis = tokio::spawn(async move {
        writer
            .install_synthetic_genesis("gc-genesis-lock", 1, 0, [], 0, [])
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut genesis)
            .await
            .is_err(),
        "synthetic genesis must wait while GC owns the retention lock"
    );
    for prefix in [
        format!("{}/", store.paths.base_prefix()),
        format!("control/directory/v1/domains/{}/", store.scope.domain()),
    ] {
        assert!(
            store
                .retention
                .list_page(&prefix, None, 256)
                .await
                .unwrap()
                .objects
                .is_empty(),
            "synthetic genesis must not stage authority or directory artifacts under the GC lock"
        );
    }
    assert!(
        store
            .retention
            .head_raw(&store.paths.current_pointer())
            .await
            .unwrap()
            .is_none()
    );
    guard.release().await.expect("release GC lock");
    genesis
        .await
        .expect("join genesis")
        .expect("genesis after lock release");
    assert!(
        store
            .retention
            .head_raw(&store.paths.current_pointer())
            .await
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn authority8_genesis_renews_retention_lease_during_streamed_construction() {
    use arco_core::test_inputs::FixedInputs;

    let store = store();
    let started = Utc::now();
    let mut clock = Some(FixedInputs::at(started));
    let rows = (0_u64..768).map(|ordinal| {
        if ordinal % 256 == 0 {
            drop(clock.take());
            clock = Some(FixedInputs::at(
                started + ChronoDuration::seconds(i64::try_from(ordinal / 256).unwrap() * 20),
            ));
        }
        SyntheticKvEntry {
            key: format!("receipt/{ordinal:04}").into_bytes(),
            generation: 1,
            value: Some(b"live".to_vec()),
        }
    });
    let result = store
        .install_synthetic_genesis("gc-genesis-renewal", 1, 768, rows, 0, [])
        .await;
    assert!(
        result.is_ok(),
        "streamed genesis must renew its 30-second retention lease: {result:?}"
    );
    assert_eq!(
        store.get(b"receipt/0767").await.unwrap(),
        Some(Bytes::from_static(b"live"))
    );
    drop(clock.take());
}
