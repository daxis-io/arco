//! Indexed Arrow IPC segment and `control/v1` layout contract tests.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::num::NonZeroU64;
use std::sync::Arc;

use arco_catalog::{
    ArcoStateReader, ArcoStateTxn, CatalogError, ControlMvpOutboxTrimTarget, ControlMvpPaths,
    ControlMvpProjectionOutboxRecord, ControlMvpStateStore, StateScope, TxnOptions,
};
use arco_core::storage::{WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use arrow::array::{BinaryArray, BooleanArray, UInt8Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::ipc::{Block, Footer, FooterArgs};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn storage() -> ScopedStorage {
    ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
        .expect("scoped storage")
}

fn segment_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("record_kind", DataType::UInt8, false),
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("generation", DataType::UInt64, false),
        Field::new("tombstone", DataType::Boolean, false),
        Field::new("logical_sequence", DataType::UInt64, false),
        Field::new("logical_ordinal", DataType::UInt64, false),
        Field::new("origin_sequence", DataType::UInt64, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn segment_batch(
    record_kinds: Vec<u8>,
    keys: Vec<&'static [u8]>,
    values: Vec<Option<&'static [u8]>>,
    generations: Vec<u64>,
    tombstones: Vec<bool>,
    sequences: Vec<u64>,
    ordinals: Vec<u64>,
    origins: Vec<Option<u64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        segment_schema(),
        vec![
            Arc::new(UInt8Array::from(record_kinds)),
            Arc::new(BinaryArray::from(keys)),
            Arc::new(BinaryArray::from(values)),
            Arc::new(UInt64Array::from(generations)),
            Arc::new(BooleanArray::from(tombstones)),
            Arc::new(UInt64Array::from(sequences)),
            Arc::new(UInt64Array::from(ordinals)),
            Arc::new(UInt64Array::from(origins)),
        ],
    )
    .expect("test segment batch")
}

fn arrow_file(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new(&mut bytes, schema).expect("Arrow writer");
    for batch in batches {
        writer.write(batch).expect("write test batch");
    }
    writer.finish().expect("finish test segment");
    bytes
}

fn footer_bounds(bytes: &[u8]) -> (usize, usize) {
    let trailer_start = bytes.len() - 10;
    let trailer: [u8; 10] = bytes[trailer_start..].try_into().expect("Arrow trailer");
    let footer_len = arrow::ipc::reader::read_footer_length(trailer).expect("footer length");
    (trailer_start - footer_len, trailer_start)
}

fn footer_blocks(bytes: &[u8]) -> Vec<(u64, i32, i64)> {
    let (footer_start, trailer_start) = footer_bounds(bytes);
    arrow::ipc::root_as_footer(&bytes[footer_start..trailer_start])
        .expect("Arrow footer")
        .recordBatches()
        .expect("record batches")
        .iter()
        .map(|block| {
            (
                u64::try_from(block.offset()).expect("positive offset"),
                block.metaDataLength(),
                block.bodyLength(),
            )
        })
        .collect()
}

fn footer_without_schema(bytes: &[u8]) -> Vec<u8> {
    let (footer_start, trailer_start) = footer_bounds(bytes);
    let footer =
        arrow::ipc::root_as_footer(&bytes[footer_start..trailer_start]).expect("original footer");
    let block = *footer.recordBatches().expect("record batches").get(0);
    let mut builder = FlatBufferBuilder::new();
    let batches = builder.create_vector(&[block]);
    let replacement = Footer::create(
        &mut builder,
        &FooterArgs {
            version: footer.version(),
            schema: None,
            dictionaries: None,
            recordBatches: Some(batches),
            custom_metadata: None,
        },
    );
    builder.finish(replacement, None);
    let replacement = builder.finished_data();
    let mut malformed = bytes[..footer_start].to_vec();
    malformed.extend_from_slice(replacement);
    malformed.extend_from_slice(
        &u32::try_from(replacement.len())
            .expect("footer length")
            .to_le_bytes(),
    );
    malformed.extend_from_slice(b"ARROW1");
    malformed
}

fn footer_with_body_length(bytes: &[u8], body_length: i64) -> Vec<u8> {
    let (footer_start, trailer_start) = footer_bounds(bytes);
    let footer =
        arrow::ipc::root_as_footer(&bytes[footer_start..trailer_start]).expect("original footer");
    let original = footer.recordBatches().expect("record batches").get(0);
    let replacement = Block::new(original.offset(), original.metaDataLength(), body_length);
    let block_offset = bytes[footer_start..trailer_start]
        .windows(original.0.len())
        .position(|window| window == original.0)
        .expect("record-batch block bytes");
    let mut malformed = bytes.to_vec();
    malformed[footer_start + block_offset..footer_start + block_offset + replacement.0.len()]
        .copy_from_slice(&replacement.0);
    malformed
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn bloom_bits_hex(keys: &[&[u8]]) -> String {
    let mut bits = [0_u8; 32];
    for key in keys {
        let digest = Sha256::digest(key);
        for byte in digest.iter().take(3) {
            let bit = usize::from(*byte) % (bits.len() * 8);
            bits[bit / 8] |= 1 << (bit % 8);
        }
    }
    hex::encode(bits)
}

#[derive(Serialize, Deserialize)]
struct MirrorScope {
    tenant_id: String,
    workspace_id: String,
    domain: String,
}

#[derive(Serialize, Deserialize)]
struct MirrorStateRef {
    state_id: String,
    logical_sequence: u64,
    checksum_sha256: String,
    index_checksum_sha256: String,
}

#[derive(Serialize, Deserialize)]
struct MirrorTxRef {
    tx_id: String,
    sequence: u64,
    checksum_sha256: String,
}

#[derive(Serialize, Deserialize)]
struct MirrorSegmentRef {
    segment_id: String,
    level: String,
    logical_sequence: u64,
    checksum_sha256: String,
    index_checksum_sha256: String,
}

#[derive(Serialize, Deserialize)]
struct MirrorManifest {
    format_version: u32,
    implementation: String,
    scope: MirrorScope,
    manifest_id: String,
    logical_sequence: u64,
    base_manifest_id: Option<String>,
    writer_epoch: u64,
    base_state: Option<MirrorStateRef>,
    anchor_state: Option<MirrorStateRef>,
    tx_refs: Vec<MirrorTxRef>,
    state_checksum_sha256: String,
}

#[derive(Serialize, Deserialize)]
struct MirrorTransaction {
    implementation: String,
    scope: MirrorScope,
    tx_id: String,
    base_manifest_id: Option<String>,
    sequence: u64,
    writer_epoch: u64,
    request_id: Option<String>,
    l0_segment: MirrorSegmentRef,
}

fn reseal_envelope(value: &mut Value) -> Bytes {
    let artifact_type = value["artifact_type"].as_str().expect("artifact type");
    let payload = if artifact_type == "control-mvp-manifest" {
        serde_json::to_vec(
            &serde_json::from_value::<MirrorManifest>(value["payload"].clone())
                .expect("manifest payload"),
        )
        .expect("manifest payload bytes")
    } else {
        assert_eq!(artifact_type, "control-mvp-tx", "test envelope type");
        serde_json::to_vec(
            &serde_json::from_value::<MirrorTransaction>(value["payload"].clone())
                .expect("transaction payload"),
        )
        .expect("transaction payload bytes")
    };
    value["checksum_sha256"] = Value::String(sha256(&payload));
    Bytes::from(serde_json::to_vec(value).expect("sealed envelope"))
}

async fn overwrite(storage: &ScopedStorage, path: &str, bytes: Bytes) {
    assert!(matches!(
        storage
            .put_raw(path, bytes, WritePrecondition::None)
            .await
            .expect("overwrite test artifact"),
        WriteResult::Success { .. }
    ));
}

#[allow(clippy::too_many_arguments)]
async fn install_malformed_l1(
    storage: &ScopedStorage,
    store: &ControlMvpStateStore,
    state_id: &str,
    selected_manifest_id: &str,
    malformed: Vec<u8>,
    row_count: usize,
    index_keys: &[&[u8]],
    state_checksum_sha256: Option<&str>,
) {
    let paths = store.paths();
    let index_path = paths.segment_index(state_id);
    let mut index: Value =
        serde_json::from_slice(&storage.get_raw(&index_path).await.expect("state index"))
            .expect("state index JSON");
    index["segmentChecksumSha256"] = Value::String(sha256(&malformed));
    index["rowCount"] = Value::from(row_count);
    let min_key = index_keys.iter().copied().min();
    let max_key = index_keys.iter().copied().max();
    index["minKeyHex"] = min_key.map_or(Value::Null, |key| Value::String(hex::encode(key)));
    index["maxKeyHex"] = max_key.map_or(Value::Null, |key| Value::String(hex::encode(key)));
    index["minKeyUtf8"] = min_key
        .and_then(|key| std::str::from_utf8(key).ok())
        .map_or(Value::Null, |key| Value::String(key.to_string()));
    index["maxKeyUtf8"] = max_key
        .and_then(|key| std::str::from_utf8(key).ok())
        .map_or(Value::Null, |key| Value::String(key.to_string()));
    index["bloomBitsHex"] = Value::String(bloom_bits_hex(index_keys));
    index["recordBatchOffsets"] = Value::Array(
        footer_blocks(&malformed)
            .into_iter()
            .map(|(offset, _, _)| Value::from(offset))
            .collect(),
    );
    let index_bytes = Bytes::from(serde_json::to_vec(&index).expect("state index bytes"));

    let manifest_path = paths.manifest_object(selected_manifest_id);
    let mut manifest: Value = serde_json::from_slice(
        &storage
            .get_raw(&manifest_path)
            .await
            .expect("selected manifest"),
    )
    .expect("manifest JSON");
    manifest["payload"]["base_state"]["checksum_sha256"] = Value::String(sha256(&malformed));
    manifest["payload"]["base_state"]["index_checksum_sha256"] =
        Value::String(sha256(&index_bytes));
    if let Some(state_checksum_sha256) = state_checksum_sha256 {
        *manifest["payload"]
            .get_mut("state_checksum_sha256")
            .expect("manifest state checksum field") =
            Value::String(state_checksum_sha256.to_string());
    }
    let manifest_bytes = reseal_envelope(&mut manifest);

    let pointer_path = paths.current_pointer();
    let mut pointer: Value =
        serde_json::from_slice(&storage.get_raw(&pointer_path).await.expect("pointer"))
            .expect("pointer JSON");
    pointer["manifest_checksum_sha256"] = Value::String(sha256(&manifest_bytes));

    overwrite(
        storage,
        &paths.state_object(state_id),
        Bytes::from(malformed),
    )
    .await;
    overwrite(storage, &index_path, index_bytes).await;
    overwrite(storage, &manifest_path, manifest_bytes).await;
    overwrite(
        storage,
        &pointer_path,
        Bytes::from(serde_json::to_vec(&pointer).expect("pointer bytes")),
    )
    .await;
}

async fn assert_typed_read_error(store: ControlMvpStateStore) -> CatalogError {
    let joined = tokio::spawn(async move { store.get(b"catalogs/seed").await }).await;
    let result = joined.expect("malformed segment read must not panic");
    assert!(
        matches!(result, Err(CatalogError::InvariantViolation { .. })),
        "malformed checksum-coherent segment returned {result:?}"
    );
    result.expect_err("malformed segment must fail")
}

async fn anchored_store() -> (ScopedStorage, ControlMvpStateStore, String, String) {
    let storage = storage();
    let store = store(storage.clone());
    let mut base = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin base transaction");
    let state_id = base.candidate_manifest_id().replace("manifest-", "state-");
    base.put(b"catalogs/seed", Bytes::from_static(b"seed"))
        .await
        .expect("stage base write");
    base.commit().await.expect("commit anchored base");

    let mut successor = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin successor");
    let selected_manifest_id = successor.candidate_manifest_id().to_string();
    successor
        .put(b"catalogs/successor", Bytes::from_static(b"successor"))
        .await
        .expect("stage successor");
    successor.commit().await.expect("commit successor");
    (storage, store, state_id, selected_manifest_id)
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
    let transaction: Value = serde_json::from_slice(&transaction_bytes).expect("transaction json");
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

    let l0_index: Value = serde_json::from_slice(
        &storage
            .get_raw(&paths.segment_index(&tx_id))
            .await
            .expect("read l0 index"),
    )
    .expect("decode l0 index");
    let l1_index: Value = serde_json::from_slice(
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
    for (index, segment) in [(&l0_index, &l0), (&l1_index, &l1)] {
        let offsets = index["recordBatchOffsets"]
            .as_array()
            .expect("record batch offsets");
        let blocks = footer_blocks(segment);
        assert_eq!(blocks.len(), offsets.len());
        for (stored, (actual, metadata_length, body_length)) in offsets.iter().zip(blocks) {
            assert_eq!(stored.as_u64().expect("stored batch offset"), actual);
            assert!(
                actual > 0,
                "Arrow record-batch offset is not the file start"
            );
            assert!(
                metadata_length > 0,
                "Arrow metadata length must be positive"
            );
            assert!(body_length >= 0, "Arrow body length must be nonnegative");
            assert!(
                actual < u64::try_from(segment.len()).expect("segment length"),
                "Arrow record-batch offset must be within the segment"
            );
        }
    }
    assert_eq!("catalogs/sales", l1_index["minKeyUtf8"]);
    assert_eq!("catalogs/sales", l1_index["maxKeyUtf8"]);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn checksum_coherent_malformed_arrow_cases_fail_closed_without_panics() {
    #[derive(Clone, Copy)]
    enum Case {
        WrongSchema,
        UnknownKind,
        InvalidPolarity,
        DuplicateKv,
        DuplicateOutbox,
        NonContiguousOrdinals,
        NullOutboxOrigin,
        ZeroOutboxOrigin,
        FutureOutboxOrigin,
        InvalidOutboxGeneration,
        MissingSchema,
        NegativeBodyLength,
        HugeBodyLength,
        MultipleBatches,
    }

    let valid_batch = || {
        segment_batch(
            vec![0],
            vec![b"catalogs/seed"],
            vec![Some(b"seed")],
            vec![1],
            vec![false],
            vec![1],
            vec![0],
            vec![None],
        )
    };
    let cases = [
        Case::WrongSchema,
        Case::UnknownKind,
        Case::InvalidPolarity,
        Case::DuplicateKv,
        Case::DuplicateOutbox,
        Case::NonContiguousOrdinals,
        Case::NullOutboxOrigin,
        Case::ZeroOutboxOrigin,
        Case::FutureOutboxOrigin,
        Case::InvalidOutboxGeneration,
        Case::MissingSchema,
        Case::NegativeBodyLength,
        Case::HugeBodyLength,
        Case::MultipleBatches,
    ];

    for case in cases {
        let (storage, store, state_id, selected_manifest_id) = anchored_store().await;
        let (malformed, row_count, label) = match case {
            Case::WrongSchema => {
                let schema = Schema::empty();
                let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
                (arrow_file(&schema, &[batch]), 0, "wrong schema")
            }
            Case::UnknownKind => {
                let batch = segment_batch(
                    vec![99],
                    vec![b"catalogs/seed"],
                    vec![Some(b"seed")],
                    vec![1],
                    vec![false],
                    vec![1],
                    vec![0],
                    vec![None],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    1,
                    "unknown kind",
                )
            }
            Case::InvalidPolarity => {
                let batch = segment_batch(
                    vec![0],
                    vec![b"catalogs/seed"],
                    vec![None],
                    vec![1],
                    vec![false],
                    vec![1],
                    vec![0],
                    vec![None],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    1,
                    "invalid polarity",
                )
            }
            Case::DuplicateKv => {
                let batch = segment_batch(
                    vec![0, 0],
                    vec![b"catalogs/seed", b"catalogs/seed"],
                    vec![Some(b"first"), Some(b"second")],
                    vec![1, 1],
                    vec![false, false],
                    vec![1, 1],
                    vec![0, 1],
                    vec![None, None],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    2,
                    "duplicate KV key",
                )
            }
            Case::DuplicateOutbox => {
                let batch = segment_batch(
                    vec![1, 1],
                    vec![b"outbox", b"outbox"],
                    vec![Some(b"first"), Some(b"second")],
                    vec![0, 0],
                    vec![false, false],
                    vec![1, 1],
                    vec![0, 1],
                    vec![Some(1), Some(1)],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    2,
                    "duplicate outbox id",
                )
            }
            Case::NonContiguousOrdinals => {
                let batch = segment_batch(
                    vec![1, 1],
                    vec![b"outbox-a", b"outbox-b"],
                    vec![Some(b"first"), Some(b"second")],
                    vec![0, 0],
                    vec![false, false],
                    vec![1, 1],
                    vec![0, 2],
                    vec![Some(1), Some(1)],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    2,
                    "non-contiguous ordinals",
                )
            }
            Case::NullOutboxOrigin => {
                let batch = segment_batch(
                    vec![1],
                    vec![b"outbox"],
                    vec![Some(b"payload")],
                    vec![0],
                    vec![false],
                    vec![1],
                    vec![0],
                    vec![None],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    1,
                    "null L1 outbox origin",
                )
            }
            Case::ZeroOutboxOrigin => {
                let batch = segment_batch(
                    vec![1],
                    vec![b"outbox"],
                    vec![Some(b"payload")],
                    vec![0],
                    vec![false],
                    vec![1],
                    vec![0],
                    vec![Some(0)],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    1,
                    "zero L1 outbox origin",
                )
            }
            Case::FutureOutboxOrigin => {
                let batch = segment_batch(
                    vec![1],
                    vec![b"outbox"],
                    vec![Some(b"payload")],
                    vec![0],
                    vec![false],
                    vec![1],
                    vec![0],
                    vec![Some(2)],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    1,
                    "future L1 outbox origin",
                )
            }
            Case::InvalidOutboxGeneration => {
                let batch = segment_batch(
                    vec![1],
                    vec![b"outbox"],
                    vec![Some(b"payload")],
                    vec![1],
                    vec![false],
                    vec![1],
                    vec![0],
                    vec![Some(1)],
                );
                (
                    arrow_file(segment_schema().as_ref(), &[batch]),
                    1,
                    "nonzero L1 outbox generation",
                )
            }
            Case::MissingSchema => {
                let valid = arrow_file(segment_schema().as_ref(), &[valid_batch()]);
                (footer_without_schema(&valid), 1, "missing schema")
            }
            Case::NegativeBodyLength => {
                let valid = arrow_file(segment_schema().as_ref(), &[valid_batch()]);
                (
                    footer_with_body_length(&valid, -1),
                    1,
                    "negative body length",
                )
            }
            Case::HugeBodyLength => {
                let valid = arrow_file(segment_schema().as_ref(), &[valid_batch()]);
                (
                    footer_with_body_length(&valid, 1_i64 << 40),
                    1,
                    "huge body length",
                )
            }
            Case::MultipleBatches => {
                let batch = valid_batch();
                (
                    arrow_file(segment_schema().as_ref(), &[batch.clone(), batch]),
                    2,
                    "multiple batches",
                )
            }
        };
        let outbox_origin = match case {
            Case::NullOutboxOrigin => Some(None),
            Case::ZeroOutboxOrigin => Some(Some(0)),
            Case::FutureOutboxOrigin => Some(Some(2)),
            Case::InvalidOutboxGeneration => Some(Some(1)),
            _ => None,
        };
        let state_checksum_sha256 = outbox_origin.map(|origin_sequence| {
            #[derive(Serialize)]
            struct DigestEntry {
                key: Vec<u8>,
                generation: u64,
                value: Option<Vec<u8>>,
            }
            #[derive(Serialize)]
            struct DigestOutbox {
                record_id: String,
                payload: Vec<u8>,
                origin_sequence: Option<u64>,
            }
            #[derive(Serialize)]
            struct ReplayDigest {
                logical_sequence: u64,
                entries: Vec<DigestEntry>,
                outbox: Vec<DigestOutbox>,
            }
            let digest = ReplayDigest {
                logical_sequence: 2,
                entries: vec![DigestEntry {
                    key: b"catalogs/successor".to_vec(),
                    generation: 2,
                    value: Some(b"successor".to_vec()),
                }],
                outbox: vec![DigestOutbox {
                    record_id: "outbox".to_string(),
                    payload: b"payload".to_vec(),
                    origin_sequence,
                }],
            };
            sha256(&serde_json::to_vec(&digest).expect("logical state digest"))
        });
        let index_keys: Vec<&[u8]> = match case {
            Case::WrongSchema
            | Case::UnknownKind
            | Case::DuplicateOutbox
            | Case::NonContiguousOrdinals
            | Case::NullOutboxOrigin
            | Case::ZeroOutboxOrigin
            | Case::FutureOutboxOrigin
            | Case::InvalidOutboxGeneration => Vec::new(),
            Case::DuplicateKv => vec![b"catalogs/seed", b"catalogs/seed"],
            _ => vec![b"catalogs/seed"],
        };
        install_malformed_l1(
            &storage,
            &store,
            &state_id,
            &selected_manifest_id,
            malformed,
            row_count,
            &index_keys,
            state_checksum_sha256.as_deref(),
        )
        .await;
        let error = assert_typed_read_error(store).await;
        if outbox_origin.is_some() {
            assert!(
                error.to_string().contains("origin"),
                "null L1 origin failed for the wrong reason: {error:?}"
            );
        }
        eprintln!("verified malformed Arrow case: {label}");
    }
}

#[tokio::test]
async fn checksum_coherent_null_origin_l0_trim_fails_closed_without_a_panic() {
    let storage = storage();
    let store = store(storage.clone());
    let mut seed = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin seed");
    seed.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "record-r",
        Bytes::from_static(b"payload"),
    ))
    .expect("stage retained outbox record");
    seed.commit().await.expect("commit seed");

    let mut trim = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin trim");
    let tx_id = trim.tx_id().to_string();
    let manifest_id = trim.candidate_manifest_id().to_string();
    trim.trim_projection_outbox([ControlMvpOutboxTrimTarget::new("record-r", 1)])
        .expect("stage trim");
    trim.commit().await.expect("commit trim");

    let batch = segment_batch(
        vec![2],
        vec![b"record-r"],
        vec![None],
        vec![0],
        vec![true],
        vec![2],
        vec![0],
        vec![None],
    );
    let malformed = arrow_file(segment_schema().as_ref(), &[batch]);
    let paths = store.paths();
    let index_path = paths.segment_index(&tx_id);
    let mut index: Value =
        serde_json::from_slice(&storage.get_raw(&index_path).await.expect("trim index"))
            .expect("trim index JSON");
    index["segmentChecksumSha256"] = Value::String(sha256(&malformed));
    index["recordBatchOffsets"] = Value::Array(
        footer_blocks(&malformed)
            .into_iter()
            .map(|(offset, _, _)| Value::from(offset))
            .collect(),
    );
    let index_bytes = Bytes::from(serde_json::to_vec(&index).expect("index bytes"));

    let tx_path = paths.tx_object(&tx_id);
    let mut transaction: Value =
        serde_json::from_slice(&storage.get_raw(&tx_path).await.expect("trim transaction"))
            .expect("trim transaction JSON");
    transaction["payload"]["l0_segment"]["checksum_sha256"] = Value::String(sha256(&malformed));
    transaction["payload"]["l0_segment"]["index_checksum_sha256"] =
        Value::String(sha256(&index_bytes));
    let transaction_bytes = reseal_envelope(&mut transaction);

    let manifest_path = paths.manifest_object(&manifest_id);
    let mut manifest: Value = serde_json::from_slice(
        &storage
            .get_raw(&manifest_path)
            .await
            .expect("trim manifest"),
    )
    .expect("trim manifest JSON");
    manifest["payload"]["tx_refs"][0]["checksum_sha256"] =
        Value::String(sha256(&transaction_bytes));
    let manifest_bytes = reseal_envelope(&mut manifest);

    let pointer_path = paths.current_pointer();
    let mut pointer: Value =
        serde_json::from_slice(&storage.get_raw(&pointer_path).await.expect("pointer"))
            .expect("pointer JSON");
    pointer["manifest_checksum_sha256"] = Value::String(sha256(&manifest_bytes));

    overwrite(
        &storage,
        &paths.l0_segment_object(&tx_id),
        Bytes::from(malformed),
    )
    .await;
    overwrite(&storage, &index_path, index_bytes).await;
    overwrite(&storage, &tx_path, transaction_bytes).await;
    overwrite(&storage, &manifest_path, manifest_bytes).await;
    overwrite(
        &storage,
        &pointer_path,
        Bytes::from(serde_json::to_vec(&pointer).expect("pointer bytes")),
    )
    .await;

    let _ = assert_typed_read_error(store).await;
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
