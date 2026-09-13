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
        .restore_source_values(&source, Utc::now())
        .await
        .unwrap();
    assert_eq!(source_values.len(), 8);
    let decoded_bytes = source_values
        .iter()
        .map(|(k, v)| k.len() + v.len())
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
        .render_restore_candidate(&source, &source_values, &identity, &stable, 1)
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
    for (key, expected) in &source_values {
        assert_eq!(store.get(key).await.unwrap().as_ref(), Some(expected));
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
            .restore_source_values(&source, Utc::now())
            .await
            .unwrap()
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
    let receipt = sample.iter().find(|r| r.key().first() == Some(&3)).unwrap();
    let audit = sample.iter().find(|r| r.key().first() == Some(&4)).unwrap();
    let mutations = 1_209_600_u64;
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
    for index in 0..mutations * 2 {
        let is_receipt = index < mutations;
        let ordinal = index % mutations + 1;
        let template = if is_receipt { receipt } else { audit };
        let (key, value) = crate::catalog_authority::capacity_fixture_record(
            template.value().bytes(),
            is_receipt,
            ordinal,
        )
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
        mutations * 2
    );
    let report = serde_json::json!({"status":"physical-encoding-feasible-only",
        "semantic_mutations_executed":2, "synthetic_rows":mutations * 2,
        "sample_receipt_hex":hex::encode(receipt.value().bytes()),
        "sample_audit_hex":hex::encode(audit.value().bytes()),
        "sample_receipt_key_hex":hex::encode(receipt.key()), "sample_audit_key_hex":hex::encode(audit.key()),
        "decoded_key_value_bytes":total_decoded_bytes,
        "encoded_segment_bytes":shards.iter().map(|s|s["segment_bytes"].as_u64().unwrap()).sum::<u64>(),
        "encoded_index_bytes":shards.iter().map(|s|s["index_bytes"].as_u64().unwrap()).sum::<u64>(),
        "peak_input_batch_accounted_bytes":peak_input_bytes,
        "peak_input_accounting_scope":"key/value lengths plus128bytes per row; not heap/RSS ownership proof",
        "elapsed_seconds":started.elapsed().as_secs_f64(), "shards":shards,
        "all_rows_round_trip":true, "all_output_hashes_match":true,
        "full_root_restore_replay_maintenance":false, "pilot_qualified":false,
        "scope":"synthetic inventory from production typed receipt/audit encoders and production L1 codec; no whole pilot root, table mutation mix, history, concurrency or provider proof"});
    let path =
        std::env::var("ARCO_GATE7_CAPACITY_DESIGN_REPORT").expect("explicit report destination");
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!(
        "physical encoding: {} rows, {} shards, {} decoded bytes",
        mutations * 2,
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
