//! Executable checks for the capacity architecture; no production protocol change.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
use super::*;
use crate::{CatalogPatch, ControlCatalogAuthority, WriteOptions};
use arco_core::MemoryBackend;
use std::time::Instant;

#[tokio::test]
async fn restore_values_are_referenced_by_compact_transaction_metadata() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage, scope)
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(1).unwrap());
    // Values live in Arrow L0. JSON transaction metadata does not embed them.
    let value = Bytes::from(vec![120; 192 * 1024]);
    for n in 0_u64..8 {
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        txn.put(&n.to_be_bytes(), value.clone()).await.unwrap();
        txn.commit().await.unwrap();
    }
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let source = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .unwrap();
    let source_values = store
        .restore_source_values(&source, &RestoreKeyPolicy::none(), Utc::now())
        .await
        .unwrap();
    assert_eq!(source_values.rows.len(), 8);
    let decoded_bytes = source_values
        .rows
        .iter()
        .map(|(k, v)| k.len() + v.bytes.len())
        .sum::<usize>();
    assert!(decoded_bytes < MAX_SEGMENT_BYTES);
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for n in 0_u64..8 {
        txn.delete(&n.to_be_bytes()).await.unwrap();
    }
    txn.commit().await.unwrap();
    let before = store.current_state_token().await.unwrap();
    let identity =
        RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog").unwrap();
    let stable = store.load_stable_restore_base(&source).await.unwrap();
    let rendered = store
        .render_restore_candidate(
            &source,
            &source_values,
            &identity,
            &stable,
            1,
            Utc::now().timestamp_millis(),
        )
        .unwrap();
    assert!(rendered.transaction_bytes.len() < 16 * 1024);
    assert!(rendered.l0_segment_bytes.len() > decoded_bytes);
    let participant = ControlMvpRestoreParticipant::new(store.clone());
    let plan = participant
        .plan_restore(&source, &identity, Utc::now())
        .await
        .unwrap();
    assert!(matches!(
        participant.apply_restore(&plan, Utc::now()).await.unwrap(),
        RestoreParticipantInspection::Visible { .. }
    ));
    assert!(
        store
            .current_state_token()
            .await
            .unwrap()
            .logical_sequence()
            > before.logical_sequence()
    );
    for (key, expected) in &source_values.rows {
        assert_eq!(
            store.get(key).await.unwrap().as_ref(),
            Some(&expected.bytes)
        );
    }
    println!(
        "{}",
        serde_json::json!({"case":"restore-payload-is-referenced", "rows":8,
        "decoded_bytes":decoded_bytes, "transaction_bytes":rendered.transaction_bytes.len(),
        "l0_bytes":rendered.l0_segment_bytes.len(), "restore_applied":true})
    );
}

#[tokio::test]
async fn disjoint_restore_diff_exceeds_one_l0_with_small_injected_limits() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage, scope)
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(1).unwrap())
        .with_segment_limits(SegmentLimits {
            rows: 32,
            ..PRODUCTION_SEGMENT_LIMITS
        });
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for n in 0_u64..24 {
        txn.put(&n.to_be_bytes(), Bytes::from_static(b"value"))
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let source = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .unwrap();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for n in 0_u64..24 {
        txn.delete(&n.to_be_bytes()).await.unwrap();
    }
    txn.commit().await.unwrap();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for n in 24_u64..48 {
        txn.put(&n.to_be_bytes(), Bytes::from_static(b"value"))
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();
    assert_eq!(
        store
            .restore_source_values(&source, &RestoreKeyPolicy::none(), Utc::now())
            .await
            .unwrap()
            .rows
            .len(),
        24
    );
    let before = store.current_state_token().await.unwrap();
    let identity =
        RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog").unwrap();
    let participant = ControlMvpRestoreParticipant::new(store.clone());
    let error = participant
        .plan_restore(&source, &identity, Utc::now())
        .await
        .unwrap_err();
    assert!(matches!(&error, CatalogError::Validation { .. }));
    assert!(
        error
            .to_string()
            .contains("segment exceeds the supported row limit"),
        "{error}"
    );
    assert_eq!(store.current_state_token().await.unwrap(), before);
    println!(
        "{}",
        serde_json::json!({"case":"single-l0-restoration-boundary","source_rows":24,
        "current_visible_rows":24,"restore_kv_writes":48,"restore_notice_rows":1,
        "injected_l0_rows":32,"production_limits_changed":false,"head_unchanged":true,"error":error.to_string()})
    );
}

/// Receipt-like rows identical in the source and the current state need no
/// write under the plain rules, but a receipt key policy deletes every one of
/// them, so the excluded-key deletes alone can overflow the restore's single
/// L0. The restore then fails `Validation`, naming the restore and its write
/// counts, and HEAD does not move.
#[tokio::test]
async fn excluded_key_deletes_alone_can_exceed_one_restore_l0_with_small_injected_limits() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage, scope)
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(1).unwrap())
        .with_segment_limits(SegmentLimits {
            rows: 32,
            ..PRODUCTION_SEGMENT_LIMITS
        });
    // Forty receipts in two commits, so no commit's own L0 exceeds 32 rows.
    for batch in 0_u8..2 {
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        for n in 0_u8..20 {
            txn.put_with_expiry(&[0x03, batch, n], Bytes::from_static(b"receipt"), 5)
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
    }
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.put(b"plain", Bytes::from_static(b"v1")).await.unwrap();
    txn.commit().await.unwrap();
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let source = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .unwrap();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.put(b"plain", Bytes::from_static(b"v2")).await.unwrap();
    txn.commit().await.unwrap();
    let before = store.current_state_token().await.unwrap();
    let identity =
        RestoreAttemptIdentity::new("rst_00000000000000000000000041", 1, "catalog").unwrap();

    // Positive control: without a policy the identical receipts need no
    // write and the one-put restore fits.
    ControlMvpRestoreParticipant::new(store.clone())
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("the plain restore fits one L0");
    let error = ControlMvpRestoreParticipant::new(store.clone())
        .with_key_policy(RestoreKeyPolicy::excluding([[0x03_u8]]).unwrap())
        .plan_restore(&source, &identity, Utc::now())
        .await
        .unwrap_err();
    let CatalogError::Validation { message } = &error else {
        panic!("an L0 overflow stays Validation: {error:?}");
    };
    assert!(
        message.starts_with(
            "Control MVP restore rst_00000000000000000000000041 attempt 1 of domain catalog \
             does not fit one L0 segment (puts: 1, deletes: 0, excluded-key deletes: 40, plus \
             one restore notice): "
        ),
        "{message}"
    );
    assert!(
        message.ends_with("segment exceeds the supported row limit"),
        "{message}"
    );
    assert_eq!(store.current_state_token().await.unwrap(), before);
    println!(
        "{}",
        serde_json::json!({"case":"excluded-key-deletes-overflow-one-restore-l0",
        "receipt_rows":40,"plain_puts":1,"injected_l0_rows":32,
        "production_limits_changed":false,"head_unchanged":true,"error":error.to_string()})
    );
}

#[tokio::test]
#[ignore = "explicit full-size capacity encoding experiment; not a pilot workload"]
async fn pilot_inventory_round_trips_through_bounded_production_l1_shards() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let authority = ControlCatalogAuthority::new(storage.clone(), scope.clone()).unwrap();
    authority
        .create_catalog("capacity", None, WriteOptions::with_idempotency("create"))
        .await
        .unwrap();
    authority
        .patch_catalog(
            "capacity",
            CatalogPatch {
                description: Some(Some("capacity sample".into())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("update"),
        )
        .await
        .unwrap();
    let store = ControlMvpStateStore::new(storage, scope.clone()).unwrap();
    let sample = scan_all_entries_bounded(&store, b"", MAX_SEGMENT_ROWS, MAX_SEGMENT_BYTES)
        .await
        .unwrap();
    // Receipts are the only per-mutation KV row: audit records are
    // projection-only since retention step 3.
    let receipt = sample.iter().find(|r| r.key().first() == Some(&3)).unwrap();
    assert!(sample.iter().all(|r| r.key().first() != Some(&4)));
    let mutations = 1_209_600_u64;
    // Fixed synthetic acceptance clock: receipt `ordinal` is accepted 500 ms
    // after the previous one and carries the production 24 h expiry hint, so
    // the nullable expiry column is populated (monotone) like real receipts.
    let base_occurred_at_ms: i64 = 1_800_000_000_000;
    let limits = half_segment_limits(PRODUCTION_SEGMENT_LIMITS);
    let started = Instant::now();
    let mut batch = Vec::new();
    let mut batch_bytes = 0_usize;
    let mut peak_input_bytes = 0_usize;
    let mut total_decoded_bytes = 0_u64;
    let mut previous_key = None;
    let mut shards = Vec::new();
    let mut input_hash = Sha256::new();
    let mut output_hash = Sha256::new();
    for index in 0..mutations {
        let ordinal = index + 1;
        let (key, value) =
            crate::catalog_authority::capacity_fixture_record(receipt.value().bytes(), ordinal)
                .unwrap();
        assert!(previous_key.as_ref().is_none_or(|previous| previous < &key));
        previous_key = Some(key.clone());
        let decoded_bytes = key.len() + value.len();
        if !batch.is_empty() && batch_bytes + decoded_bytes + 128 > 16 * 1024 * 1024 {
            shards.push(round_trip_batch(
                &scope,
                &batch,
                &mut output_hash,
                shards.len(),
                limits,
            ));
            batch.clear();
            batch_bytes = 0;
        }
        hash_bytes(&mut input_hash, &key);
        hash_bytes(&mut input_hash, &value);
        batch_bytes += decoded_bytes + 128;
        peak_input_bytes = peak_input_bytes.max(batch_bytes);
        total_decoded_bytes += decoded_bytes as u64;
        batch.push(ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key,
            value: Some(value),
            generation: ordinal,
            tombstone: false,
            logical_sequence: mutations,
            logical_ordinal: index,
            origin_sequence: None,
            expires_at_ms: Some(
                base_occurred_at_ms
                    + i64::try_from(ordinal).unwrap() * 500
                    + crate::catalog_authority::CATALOG_RECEIPT_RETENTION_MS,
            ),
        });
    }
    if !batch.is_empty() {
        shards.push(round_trip_batch(
            &scope,
            &batch,
            &mut output_hash,
            shards.len(),
            limits,
        ));
    }
    assert_eq!(input_hash.finalize(), output_hash.finalize());
    assert_eq!(
        shards
            .iter()
            .map(|s| s["rows"].as_u64().unwrap())
            .sum::<u64>(),
        mutations
    );
    let report = serde_json::json!({"status":"physical-encoding-feasible-only",
        "semantic_mutations_executed":2, "synthetic_rows":mutations,
        "sample_receipt_hex":hex::encode(receipt.value().bytes()),
        "sample_receipt_key_hex":hex::encode(receipt.key()),
        "decoded_key_value_bytes":total_decoded_bytes,
        "encoded_segment_bytes":shards.iter().map(|s|s["segment_bytes"].as_u64().unwrap()).sum::<u64>(),
        "encoded_index_bytes":shards.iter().map(|s|s["index_bytes"].as_u64().unwrap()).sum::<u64>(),
        "peak_input_batch_accounted_bytes":peak_input_bytes,
        "peak_input_accounting_scope":"key/value lengths plus128bytes per row; not heap/RSS ownership proof",
        "elapsed_seconds":started.elapsed().as_secs_f64(), "shards":shards,
        "all_rows_round_trip":true, "all_output_hashes_match":true,
        "full_root_restore_replay_maintenance":false, "pilot_qualified":false,
        "scope":"synthetic inventory from the production typed receipt encoder and production L1 codec (receipts only: audit records are projection-only since retention step 3); no whole pilot root, table mutation mix, history, concurrency or provider proof"});
    let path =
        std::env::var("ARCO_GATE7_CAPACITY_DESIGN_REPORT").expect("explicit report destination");
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!(
        "physical encoding: {} rows, {} shards, {} decoded bytes",
        mutations,
        report["shards"].as_array().unwrap().len(),
        total_decoded_bytes
    );
}

fn round_trip_batch(
    scope: &StateScope,
    rows: &[ControlMvpSegmentRow],
    hash: &mut Sha256,
    ordinal: usize,
    limits: SegmentLimits,
) -> serde_json::Value {
    let started = Instant::now();
    let (bytes, index, reference) = encode_segment(
        &format!("capacity-{ordinal:06}"),
        ControlMvpSegmentLevel::L1,
        1_209_600,
        scope,
        rows,
        limits,
    )
    .unwrap();
    assert!(
        bytes.len() <= limits.bytes
            && index.len() <= limits.index_bytes
            && rows.len() <= limits.rows
    );
    let decoded = decode_segment_rows(&bytes, &index, &reference, scope).unwrap();
    assert_eq!(decoded, rows);
    for row in &decoded {
        hash_bytes(hash, &row.key);
        hash_bytes(hash, row.value.as_ref().unwrap());
    }
    // Actual corrupted bytes must fail the production authenticated decoder.
    if ordinal == 0 {
        let mut corrupt = bytes.to_vec();
        corrupt[0] ^= 1;
        assert!(decode_segment_rows(&corrupt, &index, &reference, scope).is_err());
        let mut corrupt = index.to_vec();
        corrupt[0] ^= 1;
        assert!(decode_segment_rows(&bytes, &corrupt, &reference, scope).is_err());
    }
    serde_json::json!({"ordinal":ordinal, "rows":rows.len(), "segment_bytes":bytes.len(),
        "index_bytes":index.len(), "segment_sha256":reference.checksum_sha256,
        "index_sha256":reference.index_checksum_sha256,
        "first_key_hex":hex::encode(&rows.first().unwrap().key),
        "last_key_hex":hex::encode(&rows.last().unwrap().key),
        "encode_decode_seconds":started.elapsed().as_secs_f64()})
}

/// Retention smoke at pilot shape: 2,000 receipt-like rows with a 24 h expiry
/// hint and 500 deletes of other keys, consolidated, then one
/// `RetentionHorizon` cycle 31 days later. Every receipt and tombstone is
/// purged, the live non-receipt rows survive, the certificate counts match,
/// and the logical sequence is unchanged. The fixture clock stamps every
/// render at the first instant, so the age bound is the head itself.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn retention_horizon_purges_expired_receipts_and_tombstones_at_pilot_shape() {
    use crate::{
        DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceStatus, PreparedMaintenance,
    };
    use arco_core::test_inputs::FixedInputs;

    const RECEIPTS: usize = 2_000;
    const RECEIPT_COMMITS: usize = 16;
    const DOOMED: usize = 500;
    const LIVE: usize = 100;

    async fn publish(
        worker: &DurableMaintenanceWorker,
        plan: PreparedMaintenance,
        now: DateTime<Utc>,
    ) -> ControlMvpMaintenanceOutcome {
        let id = plan.job_id().clone();
        let mut progress = worker.start_at(&plan, now).await.unwrap();
        while progress.status == MaintenanceStatus::Active {
            progress = worker.advance_at(&id, now).await.unwrap();
        }
        assert_eq!(progress.status, MaintenanceStatus::ReadyToPublish);
        worker
            .publish_at(&id, now)
            .await
            .unwrap()
            .expect("the only writer publishes on its first attempt")
    }

    // 2030-01-01T00:00:00Z, the instant `FixedInputs::scoped` also uses.
    let start = DateTime::from_timestamp(1_893_456_000, 0).unwrap();
    let clock = FixedInputs::at(start);
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
    let worker =
        DurableMaintenanceWorker::new(storage, scope, DurableAuthorityBinding::new([5; 32]))
            .unwrap();

    let live_keys = (0..LIVE)
        .map(|n| format!("catalog/{n:04}").into_bytes())
        .collect::<BTreeSet<_>>();
    let doomed_keys = (0..DOOMED)
        .map(|n| format!("doomed/{n:04}").into_bytes())
        .collect::<Vec<_>>();
    let expires_at_ms = (start + ChronoDuration::hours(24)).timestamp_millis();

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for key in &live_keys {
        txn.put(key, Bytes::from_static(b"catalog row"))
            .await
            .unwrap();
    }
    for key in &doomed_keys {
        txn.put(key, Bytes::from_static(b"doomed row"))
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();
    let per_commit = RECEIPTS / RECEIPT_COMMITS;
    for commit in 0..RECEIPT_COMMITS {
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        for n in commit * per_commit..(commit + 1) * per_commit {
            txn.put_with_expiry(
                format!("receipt/{n:05}").as_bytes(),
                Bytes::from_static(b"receipt"),
                expires_at_ms,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
    }
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for key in &doomed_keys {
        txn.delete(key).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Eighteen L0 segments carry a maintenance intent; consolidate them so the
    // horizon renders from L1 like a pilot root would.
    let plan = worker
        .prepare_at(start)
        .await
        .unwrap()
        .expect("the head selects a maintenance intent");
    publish(&worker, plan, start).await;
    let pointer = store.load_pointer().await.unwrap();
    let before = store.load_manifest_for_pointer(&pointer).await.unwrap();
    assert!(before.tx_refs.is_empty() && before.retention_horizon.is_none());
    let before_state = store.replay_for_successor(&before).await.unwrap();
    assert_eq!(before_state.kv.len(), LIVE + DOOMED + RECEIPTS);

    // Thirty-one days later the floor (now - 30 d - 1 h) is past every stamp,
    // so the age bound is the head and every tombstone is at or below it, and
    // every receipt expired a month before the purge cutoff.
    drop(clock);
    let later = start + ChronoDuration::days(31);
    let _clock = FixedInputs::at(later);
    let plan = worker
        .prepare_horizon_at(later)
        .await
        .unwrap()
        .expect("expired receipts and tombstones are eligible");
    let outcome = publish(&worker, plan, later).await;

    let pointer = store.load_pointer().await.unwrap();
    let after = store.load_manifest_for_pointer(&pointer).await.unwrap();
    assert_eq!(
        outcome.selected_token().authority_manifest_id(),
        after.manifest_id
    );
    assert_eq!(after.logical_sequence, before.logical_sequence);
    assert_eq!(after.history_root, before.history_root);
    assert_eq!(after.layout_generation, before.layout_generation + 1);
    let certificate = after.retention_horizon.as_ref().expect("certificate");
    assert_eq!(certificate.horizon_sequence, before.logical_sequence);
    assert_eq!(
        certificate.pinned_evidence,
        vec![PinnedSequenceV1 {
            kind: "manifest_age".into(),
            id: before.manifest_id.clone(),
            sequence: before.logical_sequence,
        }]
    );
    assert_eq!(
        certificate.purged_counts,
        PurgedCountsV1 {
            expired_rows: u64::try_from(RECEIPTS).unwrap(),
            tombstones: u64::try_from(DOOMED).unwrap(),
        }
    );
    assert_eq!(
        certificate.parent_state_checksum_sha256,
        before.state_checksum_sha256
    );

    let state = store.replay_for_successor(&after).await.unwrap();
    assert_eq!(state.kv.keys().cloned().collect::<BTreeSet<_>>(), live_keys);
    assert!(
        state
            .kv
            .values()
            .all(|value| !value.tombstone && value.expires_at_ms.is_none()),
        "only live rows without a hint remain"
    );
    assert_eq!(state.checksum().unwrap(), after.state_checksum_sha256);
    assert_eq!(
        store.get(b"catalog/0000").await.unwrap(),
        Some(Bytes::from_static(b"catalog row"))
    );
    assert_eq!(store.get(b"receipt/00000").await.unwrap(), None);
    assert_eq!(store.get(b"doomed/0000").await.unwrap(), None);
    println!(
        "{}",
        serde_json::json!({"case":"retention-horizon-pilot-shape", "receipts":RECEIPTS,
        "deletes":DOOMED, "live":LIVE, "retained_rows":state.kv.len(),
        "logical_sequence":after.logical_sequence, "layout_generation":after.layout_generation})
    );
}
