#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::super::*;
use super::*;
use arco_core::{MemoryBackend, storage::WritePrecondition};

fn fixture() -> (ScopedStorage, ControlMvpStateStore) {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let store = ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    (storage, store)
}

async fn manifest(store: &ControlMvpStateStore) -> ControlMvpManifest {
    store
        .load_manifest_for_pointer(&store.load_pointer().await.unwrap())
        .await
        .unwrap()
}

#[test]
fn canonical_hashes_match_independent_binary_vectors() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/reports/2026-09-06-gate3-canonical-vectors.json"
    )))
    .unwrap();
    let expected = |name: &str| {
        vectors["vectors"][name]["sha256"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let scope = StateScope::new("tenant", "workspace", "catalog");
    assert_eq!(genesis(&scope).unwrap().root, expected("genesis"));
    let mut tx = ControlMvpTxObject {
        history: HistoryLink::default(),
        reclamation_generation: 0,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.clone(),
        tx_id: "physical-tx".to_string(),
        base_manifest_id: None,
        sequence: 1,
        writer_epoch: 0,
        request_id: None,
        l0_segment: unwritten_l0_segment_ref("physical-tx", 1),
        writes: Vec::new(),
        outbox: Vec::new(),
        outbox_trim: Vec::new(),
    };
    assert_eq!(mutation_digest(&tx).unwrap(), expected("empty_mutation"));
    assert_eq!(
        HistoryLink::new(&tx, &genesis(&scope).unwrap().root)
            .unwrap()
            .resulting_root,
        expected("empty_commit_history")
    );
    tx.request_id = Some("request".to_string());
    tx.writes = vec![
        ControlMvpWriteEntry {
            key: vec![0, 255],
            generation: 1,
            value: Some(Vec::new()),
        },
        ControlMvpWriteEntry {
            key: Vec::new(),
            generation: 1,
            value: None,
        },
    ];
    tx.outbox = vec![ControlMvpOutboxEntry {
        record_id: "event".to_string(),
        payload: vec![255],
    }];
    assert_eq!(mutation_digest(&tx).unwrap(), expected("one_mutation"));
    assert_eq!(
        HistoryLink::new(&tx, &genesis(&scope).unwrap().root)
            .unwrap()
            .resulting_root,
        expected("one_commit_history")
    );
    tx.tx_id = "other-physical-tx".to_string();
    tx.writer_epoch = 12;
    tx.reclamation_generation = 32;
    assert_eq!(mutation_digest(&tx).unwrap(), expected("one_mutation"));
    assert_eq!(
        checkpoint_physical_digest(&scope, &[]).unwrap(),
        expected("empty_checkpoint_layout")
    );
    let other = StateScope::new("other", "workspace", "catalog");
    assert_ne!(genesis(&scope).unwrap(), genesis(&other).unwrap());
}

#[test]
fn workspace_and_metastore_digests_diverge_for_equal_textual_ids() {
    let wks = StateScope::new("acme", "prod", "catalog");
    let mts = StateScope::metastore("acme", "prod", "catalog");

    assert_ne!(
        genesis(&wks).unwrap(),
        genesis(&mts).unwrap(),
        "equal textual ids must not share a genesis root"
    );
    assert_ne!(
        checkpoint_physical_digest(&wks, &[]).unwrap(),
        checkpoint_physical_digest(&mts, &[]).unwrap(),
        "equal textual ids must not share a checkpoint layout digest"
    );

    let tx_for = |scope: &StateScope| ControlMvpTxObject {
        history: HistoryLink::default(),
        reclamation_generation: 0,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.clone(),
        tx_id: "physical-tx".to_string(),
        base_manifest_id: None,
        sequence: 1,
        writer_epoch: 0,
        request_id: None,
        l0_segment: unwritten_l0_segment_ref("physical-tx", 1),
        writes: Vec::new(),
        outbox: Vec::new(),
        outbox_trim: Vec::new(),
    };
    assert_ne!(
        mutation_digest(&tx_for(&wks)).unwrap(),
        mutation_digest(&tx_for(&mts)).unwrap(),
        "equal textual ids must not share a mutation digest"
    );
}

#[test]
fn control_mvp_envelope_accepts_legacy_workspace_scope() {
    let value = serde_json::json!({
        "reclamation_generation": 0,
        "format_version": CONTROL_MVP_FORMAT_VERSION,
        "implementation": IMPLEMENTATION,
        "scope": { "tenant_id": "acme", "workspace_id": "prod", "domain": "catalog" },
        "manifest_id": "manifest-1",
        "logical_sequence": 1,
        "manifest_checksum_sha256": "0".repeat(64),
        "writer_epoch": 0
    });

    let pointer: ControlMvpPointer = serde_json::from_value(value).unwrap();
    assert!(matches!(
        pointer.scope.root(),
        AuthorityRoot::Workspace { .. }
    ));
    assert_eq!(pointer.scope.workspace_id(), Some("prod"));
}

#[test]
fn control_mvp_envelope_scope_is_versioned_and_round_trips() {
    let scope = StateScope::new("acme", "prod", "catalog");
    let pointer = ControlMvpPointer {
        reclamation_generation: 0,
        format_version: CONTROL_MVP_FORMAT_VERSION,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.clone(),
        manifest_id: "manifest-1".to_string(),
        logical_sequence: 1,
        manifest_checksum_sha256: "0".repeat(64),
        writer_epoch: 0,
    };

    let value = serde_json::to_value(&pointer).unwrap();
    assert_eq!(value["scope"]["scope_version"].as_u64(), Some(2));
    assert_eq!(value["scope"]["root_kind"].as_str(), Some("workspace"));
    assert_eq!(value["scope"]["workspace_id"].as_str(), Some("prod"));
    assert!(value["scope"].get("metastore_id").is_none());

    let decoded: ControlMvpPointer = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.scope, scope);
}

#[test]
fn control_mvp_envelope_rejects_cross_root_scope() {
    let wks = StateScope::new("acme", "prod", "catalog");
    let mts = StateScope::metastore("acme", "prod", "catalog");

    let pointer = ControlMvpPointer {
        reclamation_generation: 0,
        format_version: CONTROL_MVP_FORMAT_VERSION,
        implementation: IMPLEMENTATION.to_string(),
        scope: wks,
        manifest_id: "manifest-1".to_string(),
        logical_sequence: 1,
        manifest_checksum_sha256: "0".repeat(64),
        writer_epoch: 0,
    };
    assert!(
        pointer.validate(&mts).is_err(),
        "a workspace envelope must not validate against a metastore root"
    );
}

#[tokio::test]
async fn nonempty_physical_layouts_match_independent_vectors() {
    let (_, store) = fixture();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut layout = manifest(&store).await;
    let state = ControlMvpStateRef {
        state_id: "state-vector".into(),
        logical_sequence: 9,
        segment_size_bytes: 12345,
        index_size_bytes: 678,
        min_key_hex: Some("00ff".into()),
        max_key_hex: Some("ff00".into()),
        checksum_sha256: "11".repeat(32),
        index_checksum_sha256: "22".repeat(32),
    };
    layout.base_states = vec![state.clone()];
    layout.anchor_states.clear();
    layout.tx_refs[0].tx_id = "tx-vector".into();
    layout.tx_refs[0].sequence = 10;
    layout.tx_refs[0].size_bytes = 901;
    layout.tx_refs[0].checksum_sha256 = "33".repeat(32);
    let vectors: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/reports/2026-09-06-gate3-canonical-vectors.json"
    )))
    .unwrap();
    assert_eq!(
        layout.physical_digest().unwrap(),
        vectors["vectors"]["nonempty_manifest_layout"]["sha256"]
    );
    assert_eq!(
        checkpoint_physical_digest(&layout.scope, &[state]).unwrap(),
        vectors["vectors"]["nonempty_checkpoint_layout"]["sha256"]
    );
}

#[tokio::test]
async fn local_manifest_validation_rejects_duplicate_transaction_ids() {
    let (_, store) = fixture();
    for _ in 0..2 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let original = manifest(&store).await;
    for id in [
        original.tx_refs[0].tx_id.clone(),
        String::new(),
        "../outside".to_string(),
        "bad%id".to_string(),
    ] {
        let mut forged = original.clone();
        forged.tx_refs[1].tx_id = id;
        forged.physical_root = forged.physical_digest().unwrap();
        assert!(forged.validate(&store.scope, &forged.manifest_id).is_err());
    }
}

#[tokio::test]
async fn local_manifest_validation_rejects_invalid_owning_metadata() {
    let (storage, store) = fixture();
    for _ in 0..16 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let original = manifest(&store).await;
    let mut bad_checksum = original.clone();
    bad_checksum.state_checksum_sha256 = "not-a-digest".to_string();
    assert!(
        bad_checksum
            .validate(&store.scope, &bad_checksum.manifest_id)
            .is_err()
    );
    let mut bad_parent = original;
    bad_parent.base_manifest_id = Some("../parent".to_string());
    assert!(
        bad_parent
            .validate(&store.scope, &bad_parent.manifest_id)
            .is_err()
    );
    ControlMvpMaintenanceWorker::new(storage, store.scope.clone())
        .unwrap()
        .consolidate_pending()
        .await
        .unwrap()
        .unwrap();
    let original = manifest(&store).await;
    for id in ["", "../other", "bad\nidentity", "bad%id"] {
        let mut forged = original.clone();
        forged.base_states[0].state_id = id.to_string();
        let result = forged.physical_digest().and_then(|digest| {
            forged.physical_root = digest;
            forged.validate(&store.scope, &forged.manifest_id)
        });
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn pointer_and_checkpoint_reject_unfollowable_immutable_ids() {
    let (_, store) = fixture();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut pointer = store.load_pointer().await.unwrap();
    pointer.manifest_id = "bad%id".to_string();
    assert!(pointer.validate(&store.scope).is_err());
    let token = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let mut checkpoint = store.load_checkpoint(&token).await.unwrap();
    checkpoint.checkpoint_id = "bad%id".to_string();
    assert!(checkpoint.validate(&store.scope, "bad%id").is_err());
    checkpoint.checkpoint_id = token.checkpoint_id().to_string();
    checkpoint.manifest_id = "bad%id".to_string();
    assert!(
        checkpoint
            .validate(&store.scope, token.checkpoint_id())
            .is_err()
    );
}

#[tokio::test]
async fn nonempty_restore_behind_source_continues_destination_history() {
    let (storage, store) = fixture();
    let store = store.with_checkpoint_interval(NonZeroU64::new(1).unwrap());
    let mut first = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    first
        .put(b"key", Bytes::from_static(b"destination"))
        .await
        .unwrap();
    first.commit().await.unwrap();
    let destination = manifest(&store).await;
    let destination_head = storage
        .get_raw(&store.paths.current_pointer())
        .await
        .unwrap();
    for _ in 0..3 {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"source")).await.unwrap();
        tx.commit().await.unwrap();
    }
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let source = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::days(1))
        .await
        .unwrap();
    storage
        .put_raw(
            &store.paths.current_pointer(),
            destination_head,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let participant = ControlMvpRestoreParticipant::new(store.clone());
    let identity =
        RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog").unwrap();
    let plan = participant
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("valid restore from a later retained source");
    participant.apply_restore(&plan, Utc::now()).await.unwrap();
    let after = manifest(&store).await;
    assert_eq!(after.logical_sequence, destination.logical_sequence + 1);
    assert_eq!(
        after.tx_refs.last().unwrap().history.preceding_root,
        destination.history_root
    );
    assert_eq!(
        store.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"source"))
    );
}

#[tokio::test]
async fn corrupt_redundant_anchor_cannot_be_promoted_by_a_successor() {
    for begin_before_corruption in [false, true] {
        let (storage, store) = fixture();
        let store = store.with_checkpoint_interval(NonZeroU64::new(1).unwrap());
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        let token = tx.commit().await.unwrap().into_state_token();
        let parent = manifest(&store).await;
        let before = storage
            .get_raw(&store.paths.current_pointer())
            .await
            .unwrap();
        let pending = if begin_before_corruption {
            Some(
                store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        storage
            .delete(&store.paths.state_object(&parent.anchor_states[0].state_id))
            .await
            .unwrap();
        if let Some(tx) = pending {
            assert!(tx.commit().await.is_err());
        } else {
            assert!(
                store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap()
                    .commit()
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            storage
                .get_raw(&store.paths.current_pointer())
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .read_at(token)
                .await
                .unwrap()
                .get(b"key")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"value"))
        );
    }
}

#[tokio::test]
async fn maintenance_rejects_corrupt_redundant_inline_anchor() {
    let (storage, store) = fixture();
    let store = store.with_checkpoint_interval(NonZeroU64::new(16).unwrap());
    for _ in 0..16 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let source = manifest(&store).await;
    assert!(source.maintenance_intent.is_some());
    assert!(!source.anchor_states.is_empty());
    let before = storage
        .get_raw(&store.paths.current_pointer())
        .await
        .unwrap();
    storage
        .delete(&store.paths.state_object(&source.anchor_states[0].state_id))
        .await
        .unwrap();
    let worker = ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone()).unwrap();
    assert!(
        worker.consolidate_pending().await.is_err(),
        "maintenance must verify every owning source state"
    );
    assert_eq!(
        storage
            .get_raw(&store.paths.current_pointer())
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn convergent_state_preserves_distinct_history_and_equivalent_layouts_preserve_history() {
    let mut converged = Vec::new();
    for first in [b"a", b"b"] {
        let (_, store) = fixture();
        for value in [first.as_slice(), b"final"] {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            tx.put(b"key", Bytes::copy_from_slice(value)).await.unwrap();
            tx.commit().await.unwrap();
        }
        converged.push(manifest(&store).await);
    }
    assert_eq!(
        converged[0].state_checksum_sha256,
        converged[1].state_checksum_sha256
    );
    assert_ne!(converged[0].history_root, converged[1].history_root);
    let mut layouts = Vec::new();
    for (rows, target) in [(512, 32 * 1024), (64, 256 * 1024)] {
        let (storage, store) = fixture();
        for sequence in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            if sequence == 0 {
                for key in 0_u64..512 {
                    tx.put(&key.to_be_bytes(), Bytes::from(vec![42; 1024]))
                        .await
                        .unwrap();
                }
            }
            tx.commit().await.unwrap();
        }
        let before = manifest(&store).await;
        let mut empty_layout = before.clone();
        empty_layout.base_states.clear();
        empty_layout.anchor_states.clear();
        empty_layout.tx_refs.clear();
        assert_eq!(
            empty_layout.physical_digest().unwrap(),
            "522c3a4cf152512cda0649ad4ffb23eec78515b519176f7ce3e279aeb0159bb1"
        );
        let worker = ControlMvpMaintenanceWorker::new(storage, store.scope.clone())
            .unwrap()
            .with_test_segment_sizing(rows, target)
            .unwrap();
        worker.consolidate_pending().await.unwrap().unwrap();
        let after = manifest(&store).await;
        assert_eq!(before.history_root, after.history_root);
        assert_eq!(before.state_checksum_sha256, after.state_checksum_sha256);
        assert_ne!(before.physical_root, after.physical_root);
        assert_eq!(before.history_root, after.history_anchor.root);
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let checkpoint = store.load_checkpoint(&checkpoint).await.unwrap();
        checkpoint.validate_source(&after).unwrap();
        assert_ne!(
            checkpoint.validation.checkpoint_physical_root,
            after.physical_root
        );
        let claimed = store.clone().claim_writer_authority().await.unwrap();
        let fenced = manifest(&claimed).await;
        assert_eq!(fenced.history_root, after.history_root);
        assert_eq!(fenced.physical_root, after.physical_root);
        layouts.push(after);
    }
    assert_eq!(layouts[0].history_root, layouts[1].history_root);
    assert_eq!(
        layouts[0].state_checksum_sha256,
        layouts[1].state_checksum_sha256
    );
    assert_ne!(layouts[0].physical_root, layouts[1].physical_root);
}

#[tokio::test]
async fn forged_equivalence_and_checkpoint_layout_evidence_is_rejected() {
    let (storage, store) = fixture();
    for sequence in 0..16 {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if sequence == 0 {
            tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        }
        tx.commit().await.unwrap();
    }
    let source = manifest(&store).await;
    ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone())
        .unwrap()
        .consolidate_pending()
        .await
        .unwrap()
        .unwrap();
    let mut after = manifest(&store).await;
    let checkpoint_token = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let checkpoint = store.load_checkpoint(&checkpoint_token).await.unwrap();
    let mut wrong_layout = checkpoint.clone();
    wrong_layout
        .validation
        .checkpoint_physical_root
        .clone_from(&after.physical_root);
    assert!(
        wrong_layout
            .validate(&store.scope, &wrong_layout.checkpoint_id)
            .is_err()
    );
    for which in 0..3 {
        let mut forged = checkpoint.clone();
        match which {
            0 => forged.validation.source_history_root = "0".repeat(64),
            1 => forged.validation.source_physical_root = "0".repeat(64),
            _ => forged.validation.state_checksum_sha256 = "0".repeat(64),
        }
        assert!(forged.validate_source(&after).is_err());
    }
    let mut invalid = after.clone();
    invalid.equivalence.as_mut().unwrap().source_history_root = "0".repeat(64);
    assert!(
        invalid
            .validate(&store.scope, &invalid.manifest_id)
            .is_err()
    );
    after.equivalence.as_mut().unwrap().source_physical_root = "0".repeat(64);
    let bytes = encode_envelope("control-mvp-manifest", &after).unwrap();
    let digest = sha256_hex(&bytes);
    storage
        .put_raw(
            &store.paths.manifest_object(&after.manifest_id),
            bytes,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let result = store
        .resolve_ancestor(&after.manifest_id, &digest, |candidate, _| {
            (candidate.manifest_id == source.manifest_id).then_some(())
        })
        .await;
    assert!(matches!(
        result,
        Err(CatalogError::InvariantViolation { .. })
    ));
    assert!(
        store.read_checkpoint(checkpoint_token).await.is_err(),
        "checkpoint retains its exact source witness"
    );
}

#[tokio::test]
async fn rendered_outbox_order_and_incarnations_are_part_of_equivalence() {
    let (_, store) = fixture();
    let mut expected = ReplayState::empty(&store.scope).unwrap();
    expected.logical_sequence = 4;
    expected.outbox = vec![
        ControlMvpOutboxEntry {
            record_id: "b".to_string(),
            payload: vec![1],
        }
        .to_record_with_sequence(2),
        ControlMvpOutboxEntry {
            record_id: "a".to_string(),
            payload: vec![2],
        }
        .to_record_with_sequence(3),
    ];
    let rendered = store
        .render_state_snapshots(&expected, "outbox-equivalence")
        .unwrap();
    let mut reordered = expected.clone();
    reordered.outbox.swap(0, 1);
    assert!(
        store
            .validate_rendered_state(&reordered, &rendered)
            .is_err()
    );
    let mut incarnation = expected;
    incarnation.outbox[0].origin_sequence = Some(1);
    assert!(
        store
            .validate_rendered_state(&incarnation, &rendered)
            .is_err()
    );
}

#[tokio::test]
async fn coherent_data_and_semantic_checksum_replacement_cannot_rewrite_history() {
    let (storage, store) = fixture();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"key", Bytes::from_static(b"original"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut selected = manifest(&store).await;
    let mut tx = store.load_tx(&selected.tx_refs[0]).await.unwrap();
    tx.writes[0].value = Some(b"replaced".to_vec());
    let (bytes, index, reference) = encode_segment(
        &tx.tx_id,
        ControlMvpSegmentLevel::L0,
        1,
        &store.scope,
        &segment_rows_for_tx(&tx),
        PRODUCTION_SEGMENT_LIMITS,
    )
    .unwrap();
    tx.l0_segment = reference;
    storage
        .put_raw(
            &store.paths.l0_segment_object(&tx.tx_id),
            bytes,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    storage
        .put_raw(
            &store.paths.segment_index(&tx.tx_id),
            index,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let bytes = encode_envelope("control-mvp-tx", &tx).unwrap();
    selected.tx_refs[0].size_bytes = bytes.len() as u64;
    selected.tx_refs[0].checksum_sha256 = sha256_hex(&bytes);
    storage
        .put_raw(
            &store.paths.tx_object(&tx.tx_id),
            bytes,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let mut expected = ReplayState::empty(&store.scope).unwrap();
    let mut altered = tx.clone();
    altered.history = HistoryLink::new(&altered, &expected.history_root).unwrap();
    expected.apply_tx(&altered).unwrap();
    selected.state_checksum_sha256 = expected.checksum().unwrap();
    selected.physical_root = selected.physical_digest().unwrap();
    selected
        .validate(&store.scope, &selected.manifest_id)
        .unwrap();
    assert!(store.replay_manifest(&selected).await.is_err());
}

#[tokio::test]
async fn rendered_rewrites_reject_lost_altered_and_duplicated_rows() {
    let (_, store) = fixture();
    let mut expected = ReplayState::empty(&store.scope).unwrap();
    expected.logical_sequence = 2;
    for key in [b"a", b"b"] {
        expected.kv.insert(
            key.to_vec(),
            StoredValue {
                bytes: Bytes::from_static(b"value"),
                generation: 1,
                tombstone: false,
            },
        );
    }
    for mutation in 0..4 {
        let mut rendered = store
            .render_state_snapshots(&expected, "render-test")
            .unwrap();
        let reference = state_segment_reference(&rendered[0].reference);
        let mut rows = decode_segment_rows(
            &rendered[0].bytes,
            &rendered[0].index_bytes,
            &reference,
            &store.scope,
        )
        .unwrap();
        match mutation {
            0 => {
                rows.pop();
            }
            1 => rows[0].value = Some(b"other".to_vec()),
            2 => rows[1] = rows[0].clone(),
            _ => rows.swap(0, 1),
        }
        let (bytes, index, reference) = encode_segment(
            &reference.segment_id,
            reference.level,
            reference.logical_sequence,
            &store.scope,
            &rows,
            PRODUCTION_SEGMENT_LIMITS,
        )
        .unwrap();
        rendered[0].bytes = bytes;
        rendered[0].index_bytes = index;
        rendered[0].reference.segment_size_bytes = reference.segment_size_bytes;
        rendered[0].reference.index_size_bytes = reference.index_size_bytes;
        rendered[0].reference.checksum_sha256 = reference.checksum_sha256;
        rendered[0].reference.index_checksum_sha256 = reference.index_checksum_sha256;
        assert!(
            store.validate_rendered_state(&expected, &rendered).is_err(),
            "mutation {mutation}"
        );
    }
}
