//! Indexed Arrow IPC segment and `control/v1` layout contract tests.

#![allow(clippy::expect_used)]

use std::num::NonZeroU64;
use std::sync::Arc;

use arco_catalog::{
    ArcoStateReader, ArcoStateTxn, ControlMvpPaths, ControlMvpProjectionOutboxRecord,
    ControlMvpStateStore, StateScope, TxnOptions,
};
use arco_core::storage::{WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use bytes::Bytes;

fn storage() -> ScopedStorage {
    ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
        .expect("scoped storage")
}

#[tokio::test]
async fn transaction_envelope_is_metadata_only_and_l0_drives_replay() {
    let storage = storage();
    let store = store(storage.clone());
    let value = Bytes::from(vec![0xa5; 16 * 1024]);
    let outbox_payload = Bytes::from(vec![0x5a; 16 * 1024]);
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    let tx_id = txn.tx_id().to_string();
    txn.put(b"catalogs/large", value.clone())
        .await
        .expect("stage write");
    txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "large-projection",
        outbox_payload.clone(),
    ))
    .expect("stage outbox");

    txn.commit().await.expect("commit");

    let transaction_bytes = storage
        .get_raw(&store.paths().tx_object(&tx_id))
        .await
        .expect("transaction envelope");
    let transaction: serde_json::Value =
        serde_json::from_slice(&transaction_bytes).expect("transaction json");
    let payload = transaction["payload"]
        .as_object()
        .expect("transaction payload");
    for duplicated_field in ["writes", "outbox", "outbox_trim"] {
        assert!(
            !payload.contains_key(duplicated_field),
            "{duplicated_field} belongs only in the authoritative L0 segment"
        );
    }
    assert!(
        transaction_bytes.len() < 4 * 1024,
        "metadata envelope unexpectedly contains staged payload bytes"
    );

    assert_eq!(
        Some(value),
        store.get(b"catalogs/large").await.expect("replay")
    );
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("replay outbox");
    assert_eq!(1, outbox.len());
    assert_eq!(outbox_payload, outbox[0].payload());
}

fn store(storage: ScopedStorage) -> ControlMvpStateStore {
    ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
        .expect("control store")
        .with_checkpoint_interval(NonZeroU64::new(1).expect("nonzero"))
}

#[test]
fn control_v1_paths_separate_head_transactions_segments_and_indexes() {
    let paths = ControlMvpPaths::new("catalog");

    assert_eq!("control/v1/domains/catalog", paths.base_prefix());
    assert_eq!(
        "control/v1/domains/catalog/head/current.json",
        paths.current_pointer()
    );
    assert_eq!(
        "control/v1/domains/catalog/transactions/tx-1.json",
        paths.tx_object("tx-1")
    );
    assert_eq!(
        "control/v1/domains/catalog/segments/l0/tx-1.arrow",
        paths.l0_segment_object("tx-1")
    );
    assert_eq!(
        "control/v1/domains/catalog/segments/l1/state-1.arrow",
        paths.state_object("state-1")
    );
    assert_eq!(
        "control/v1/domains/catalog/indexes/tx-1.idx",
        paths.segment_index("tx-1")
    );
}

#[tokio::test]
async fn commit_persists_indexed_l0_and_l1_arrow_segments() {
    let storage = storage();
    let store = store(storage.clone());
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    let tx_id = txn.tx_id().to_string();
    let state_id = format!(
        "state-{}",
        txn.candidate_manifest_id()
            .strip_prefix("manifest-")
            .expect("manifest prefix")
    );
    txn.put(b"catalogs/sales", Bytes::from_static(b"active"))
        .await
        .expect("stage write");

    txn.commit().await.expect("commit");

    let paths = store.paths();
    let l0 = storage
        .get_raw(&paths.l0_segment_object(&tx_id))
        .await
        .expect("read l0 segment");
    let l1 = storage
        .get_raw(&paths.state_object(&state_id))
        .await
        .expect("read l1 segment");
    assert!(l0.starts_with(b"ARROW1"));
    assert!(l1.starts_with(b"ARROW1"));

    let l0_index: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&paths.segment_index(&tx_id))
            .await
            .expect("read l0 index"),
    )
    .expect("decode l0 index");
    let l1_index: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&paths.segment_index(&state_id))
            .await
            .expect("read l1 index"),
    )
    .expect("decode l1 index");
    assert_eq!("l0", l0_index["level"]);
    assert_eq!("l1", l1_index["level"]);
    assert_eq!(1, l0_index["rowCount"]);
    assert_eq!(1, l1_index["rowCount"]);
    for (index, segment_len) in [(&l0_index, l0.len()), (&l1_index, l1.len())] {
        let offsets = index["recordBatchOffsets"]
            .as_array()
            .expect("record batch offsets");
        assert_eq!(1, offsets.len());
        let offset = offsets[0].as_u64().expect("record batch offset");
        assert!(
            offset > 0,
            "Arrow record-batch offset is not the file start"
        );
        assert!(
            offset < u64::try_from(segment_len).expect("segment length"),
            "Arrow record-batch offset must be within the segment"
        );
    }
    assert_eq!("catalogs/sales", l1_index["minKeyUtf8"]);
    assert_eq!("catalogs/sales", l1_index["maxKeyUtf8"]);
}

#[tokio::test]
async fn corrupted_segment_index_fails_closed_before_state_is_returned() {
    let storage = storage();
    let store = store(storage.clone());
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    let state_id = format!(
        "state-{}",
        txn.candidate_manifest_id()
            .strip_prefix("manifest-")
            .expect("manifest prefix")
    );
    txn.put(b"catalogs/sales", Bytes::from_static(b"active"))
        .await
        .expect("stage write");
    txn.commit().await.expect("commit");
    let mut successor = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin successor");
    successor
        .put(b"catalogs/finance", Bytes::from_static(b"active"))
        .await
        .expect("stage successor write");
    successor.commit().await.expect("commit successor");

    let index_path = store.paths().segment_index(&state_id);
    let meta = storage
        .head_raw(&index_path)
        .await
        .expect("head index")
        .expect("index exists");
    let result = storage
        .put_raw(
            &index_path,
            Bytes::from_static(br#"{"formatVersion":3}"#),
            WritePrecondition::MatchesVersion(meta.version),
        )
        .await
        .expect("replace index");
    assert!(matches!(result, WriteResult::Success { .. }));

    let error = store
        .get(b"catalogs/sales")
        .await
        .expect_err("corrupt index must fail closed");
    assert!(error.to_string().contains("segment index"));
}
