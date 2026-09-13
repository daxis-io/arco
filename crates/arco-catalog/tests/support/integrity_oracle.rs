//! Independent logical oracle. It encodes the documented format directly and
//! never calls production replay, hashing, partitioning or maintenance helpers.
#![allow(clippy::indexing_slicing, clippy::panic)] // Oracle assertions intentionally fail on malformed fixtures.
use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct LogicalOracle {
    pub sequence: u64,
    pub root: String,
    last_mutation: String,
    kv: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    pub outbox: Vec<(String, Bytes, u64)>,
}

fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}
fn prefix(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    field(&mut out, format!("arco/control-v1/{name}").as_bytes());
    out.extend_from_slice(&1_u32.to_be_bytes());
    field(&mut out, b"arco-state-control-mvp");
    out.extend_from_slice(&7_u32.to_be_bytes());
    for scope in [b"tenant".as_slice(), b"workspace", b"catalog"] {
        field(&mut out, scope);
    }
    out
}
fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

impl LogicalOracle {
    pub fn new() -> Self {
        Self {
            sequence: 0,
            root: digest(&prefix("history-genesis")),
            last_mutation: String::new(),
            kv: BTreeMap::new(),
            outbox: Vec::new(),
        }
    }

    pub fn commit(
        &mut self,
        writes: Vec<(Vec<u8>, Option<Bytes>)>,
        additions: Vec<(String, Bytes)>,
        trims: Vec<(String, u64)>,
    ) {
        self.commit_with_request(writes, additions, trims, None);
    }

    pub fn commit_with_request(
        &mut self,
        mut writes: Vec<(Vec<u8>, Option<Bytes>)>,
        additions: Vec<(String, Bytes)>,
        mut trims: Vec<(String, u64)>,
        request: Option<&str>,
    ) {
        assert!(
            trims
                .iter()
                .all(|(id, _)| !additions.iter().any(|(added, _)| added == id))
        );
        self.sequence += 1;
        let mut mutation = prefix("mutation");
        mutation.push(u8::from(request.is_some()));
        if let Some(request) = request {
            field(&mut mutation, request.as_bytes());
        }
        writes.sort_by(|a, b| a.0.cmp(&b.0));
        mutation.extend_from_slice(&(writes.len() as u64).to_be_bytes());
        for (key, value) in writes {
            field(&mut mutation, &key);
            mutation.extend_from_slice(&self.sequence.to_be_bytes());
            mutation.push(u8::from(value.is_some()));
            if let Some(value) = &value {
                field(&mut mutation, value);
            }
            self.kv
                .insert(key, (self.sequence, value.map(|value| value.to_vec())));
        }
        mutation.extend_from_slice(&(additions.len() as u64).to_be_bytes());
        for (id, payload) in &additions {
            field(&mut mutation, id.as_bytes());
            field(&mut mutation, payload);
            mutation.extend_from_slice(&self.sequence.to_be_bytes());
        }
        trims.sort();
        mutation.extend_from_slice(&(trims.len() as u64).to_be_bytes());
        for (id, origin) in trims {
            field(&mut mutation, id.as_bytes());
            mutation.extend_from_slice(&origin.to_be_bytes());
            let position = self
                .outbox
                .iter()
                .position(|entry| entry.0 == id && entry.2 == origin)
                .unwrap();
            self.outbox.remove(position);
        }
        for (id, payload) in additions {
            assert!(!self.outbox.iter().any(|entry| entry.0 == id));
            self.outbox.push((id, payload, self.sequence));
        }
        let mut history = prefix("history-step");
        history.extend_from_slice(&hex::decode(&self.root).unwrap());
        history.extend_from_slice(&self.sequence.to_be_bytes());
        history.extend_from_slice(&Sha256::digest(&mutation));
        self.last_mutation = digest(&mutation);
        self.root = digest(&history);
    }

    pub async fn assert_manifest(&self, storage: &ScopedStorage, id: &str) {
        #[derive(Serialize)]
        struct Entry<'a> {
            key: &'a Vec<u8>,
            generation: u64,
            value: &'a Option<Vec<u8>>,
        }
        #[derive(Serialize)]
        struct Outbox<'a> {
            record_id: &'a str,
            payload: Vec<u8>,
            origin_sequence: Option<u64>,
        }
        #[derive(Serialize)]
        struct State<'a> {
            logical_sequence: u64,
            entries: Vec<Entry<'a>>,
            outbox: Vec<Outbox<'a>>,
        }
        let state = State {
            logical_sequence: self.sequence,
            entries: self
                .kv
                .iter()
                .map(|(key, (generation, value))| Entry {
                    key,
                    generation: *generation,
                    value,
                })
                .collect(),
            outbox: self
                .outbox
                .iter()
                .map(|(id, payload, origin)| Outbox {
                    record_id: id,
                    payload: payload.to_vec(),
                    origin_sequence: Some(*origin),
                })
                .collect(),
        };
        let bytes = storage
            .get_raw(&format!("control/v1/domains/catalog/manifests/{id}.json"))
            .await
            .unwrap();
        let doc = assert_envelope(&bytes, "control-mvp-manifest");
        assert_eq!(doc["payload"]["logical_sequence"], self.sequence);
        assert_eq!(doc["payload"]["history_root"], self.root);
        assert_eq!(
            doc["payload"]["state_checksum_sha256"],
            digest(&serde_json::to_vec(&state).unwrap())
        );
        self.assert_physical(storage, &doc["payload"]).await;
    }

    /// Independently decodes every owning artifact and recomputes both histories
    /// and physical ownership. No production state-store helper is called.
    #[allow(clippy::cognitive_complexity)] // Explicit independent ownership and replay assertions.
    async fn assert_physical(&self, storage: &ScopedStorage, manifest: &serde_json::Value) {
        let mut physical = prefix("manifest-layout");
        for (role, name) in [(1, "base_states"), (2, "anchor_states")] {
            let refs = manifest[name].as_array().unwrap();
            physical.push(role);
            physical.extend_from_slice(&(refs.len() as u64).to_be_bytes());
            for r in refs {
                field(&mut physical, r["state_id"].as_str().unwrap().as_bytes());
                for name in ["logical_sequence", "segment_size_bytes", "index_size_bytes"] {
                    physical.extend_from_slice(&r[name].as_u64().unwrap().to_be_bytes());
                }
                physical.extend_from_slice(&1_u32.to_be_bytes());
                physical.extend_from_slice(&1_u32.to_be_bytes());
                for name in ["min_key_hex", "max_key_hex"] {
                    physical.push(u8::from(!r[name].is_null()));
                    if let Some(value) = r[name].as_str() {
                        field(&mut physical, &hex::decode(value).unwrap());
                    }
                }
                for name in ["checksum_sha256", "index_checksum_sha256"] {
                    physical.extend_from_slice(&hex::decode(r[name].as_str().unwrap()).unwrap());
                }
            }
        }
        let refs = manifest["tx_refs"].as_array().unwrap();
        physical.push(3);
        physical.extend_from_slice(&(refs.len() as u64).to_be_bytes());
        for r in refs {
            field(&mut physical, r["tx_id"].as_str().unwrap().as_bytes());
            physical.extend_from_slice(&r["sequence"].as_u64().unwrap().to_be_bytes());
            physical.extend_from_slice(&r["size_bytes"].as_u64().unwrap().to_be_bytes());
            physical.extend_from_slice(&7_u32.to_be_bytes());
            physical
                .extend_from_slice(&hex::decode(r["checksum_sha256"].as_str().unwrap()).unwrap());
        }
        assert_eq!(manifest["physical_root"], digest(&physical));
        let mut actual = Self::new();
        actual.sequence = manifest["history_anchor"]["sequence"].as_u64().unwrap();
        manifest["history_anchor"]["root"]
            .as_str()
            .unwrap()
            .clone_into(&mut actual.root);
        actual
            .read_base(storage, manifest["base_states"].as_array().unwrap())
            .await;
        for r in refs {
            let path = format!(
                "control/v1/domains/catalog/transactions/{}.json",
                r["tx_id"].as_str().unwrap()
            );
            let bytes = storage.get_raw(&path).await.unwrap();
            assert_eq!(bytes.len() as u64, r["size_bytes"].as_u64().unwrap());
            assert_eq!(digest(&bytes), r["checksum_sha256"]);
            let doc = assert_envelope(&bytes, "control-mvp-tx");
            let tx = &doc["payload"];
            assert_transaction_owner(tx, r);
            let rows = physical_rows(storage, &tx["l0_segment"], false).await;
            let mut writes = Vec::new();
            let mut additions = Vec::new();
            let mut trims = Vec::new();
            for row in rows {
                assert_eq!(row.sequence, actual.sequence + 1);
                match row.kind {
                    0 => {
                        assert_eq!(row.generation, row.sequence);
                        assert_eq!(row.tombstone, row.value.is_none());
                        writes.push((row.key, row.value.map(Bytes::from)));
                    }
                    1 => additions.push((
                        row.ordinal,
                        String::from_utf8(row.key).unwrap(),
                        Bytes::from(row.value.unwrap()),
                    )),
                    2 => trims.push((String::from_utf8(row.key).unwrap(), row.origin.unwrap())),
                    _ => panic!("unknown independent physical row kind"),
                }
            }
            additions.sort_by_key(|r| r.0);
            let preceding = actual.root.clone();
            actual.commit_with_request(
                writes,
                additions
                    .into_iter()
                    .map(|(_, id, value)| (id, value))
                    .collect(),
                trims,
                tx["request_id"].as_str(),
            );
            for declared in [&tx["history"], &r["history"]] {
                assert_history_link(declared, &preceding, &actual.last_mutation, &actual.root);
            }
        }
        assert_eq!(actual.sequence, self.sequence);
        assert_eq!(actual.root, self.root);
        assert_eq!(actual.kv, self.kv);
        assert_eq!(actual.outbox, self.outbox);
        let anchors = manifest["anchor_states"].as_array().unwrap();
        if !anchors.is_empty() {
            let mut anchored = Self::new();
            anchored.sequence = self.sequence;
            anchored.read_base(storage, anchors).await;
            assert_eq!(anchored.kv, self.kv);
            assert_eq!(anchored.outbox, self.outbox);
        }
    }

    async fn read_base(&mut self, storage: &ScopedStorage, refs: &[serde_json::Value]) {
        for r in refs {
            let mut outbox = Vec::new();
            for row in physical_rows(storage, r, true).await {
                assert_eq!(row.sequence, self.sequence);
                match row.kind {
                    0 => {
                        assert_eq!(row.ordinal, self.kv.len() as u64);
                        assert_eq!(row.tombstone, row.value.is_none());
                        assert!(
                            self.kv
                                .insert(row.key, (row.generation, row.value))
                                .is_none()
                        );
                    }
                    1 => outbox.push(row),
                    _ => panic!("non-state row in L1"),
                }
            }
            outbox.sort_by_key(|row| row.ordinal);
            for row in outbox {
                assert_eq!(row.ordinal, self.outbox.len() as u64);
                self.outbox.push((
                    String::from_utf8(row.key).unwrap(),
                    Bytes::from(row.value.unwrap()),
                    row.origin.unwrap(),
                ));
            }
        }
    }
}

#[derive(Clone)]
struct PhysicalRow {
    kind: u8,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    generation: u64,
    tombstone: bool,
    sequence: u64,
    ordinal: u64,
    origin: Option<u64>,
}

fn assert_physical_row(row: &PhysicalRow, l1: bool) {
    assert_eq!(row.tombstone, row.value.is_none());
    match row.kind {
        0 => {
            assert!(row.origin.is_none());
            assert!(row.generation > 0 && row.generation <= row.sequence);
            if !l1 {
                assert_eq!(row.generation, row.sequence);
            }
        }
        1 => {
            assert!(!row.tombstone);
            assert_eq!(row.generation, 0);
            assert!(row.origin.is_some_and(|n| n > 0 && n <= row.sequence));
            if !l1 {
                assert_eq!(row.origin, Some(row.sequence));
            }
        }
        2 => {
            assert!(!l1 && row.tombstone);
            assert_eq!(row.generation, 0);
            assert!(row.origin.is_some_and(|n| n > 0 && n <= row.sequence));
        }
        _ => panic!("unknown oracle row kind"),
    }
}

fn assert_physical_schema(schema: &arrow::datatypes::Schema) {
    use arrow::datatypes::{DataType, Field, Schema};
    assert_eq!(
        schema,
        &Schema::new(vec![
            Field::new("record_kind", DataType::UInt8, false),
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("generation", DataType::UInt64, false),
            Field::new("tombstone", DataType::Boolean, false),
            Field::new("logical_sequence", DataType::UInt64, false),
            Field::new("logical_ordinal", DataType::UInt64, false),
            Field::new("origin_sequence", DataType::UInt64, true),
        ])
    );
}

fn assert_history_link(link: &serde_json::Value, preceding: &str, mutation: &str, resulting: &str) {
    assert_eq!(link["preceding_root"], preceding);
    assert_eq!(link["mutation_sha256"], mutation);
    assert_eq!(link["resulting_root"], resulting);
}

fn assert_transaction_owner(tx: &serde_json::Value, owner: &serde_json::Value) {
    assert_eq!(tx["implementation"], "arco-state-control-mvp");
    assert_eq!(
        tx["scope"],
        serde_json::json!({"tenant_id":"tenant", "workspace_id":"workspace", "domain":"catalog"})
    );
    assert_eq!(tx["tx_id"], owner["tx_id"]);
    assert_eq!(tx["sequence"], owner["sequence"]);
    assert_eq!(tx["l0_segment"]["segment_id"], tx["tx_id"]);
    assert_eq!(tx["l0_segment"]["level"], "l0");
    assert_eq!(tx["l0_segment"]["logical_sequence"], tx["sequence"]);
}

/// The independent oracle requires the canonical envelope emitted by writers.
/// Checking the original payload slice avoids `serde_json::Value`'s map sorting.
fn assert_envelope(bytes: &[u8], artifact: &str) -> serde_json::Value {
    assert!(
        bytes.len()
            <= if artifact == "control-mvp-tx" {
                4 * 1024 * 1024
            } else {
                1024 * 1024
            }
    );
    let doc: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(doc["format_version"], 7);
    assert_eq!(doc["artifact_type"], artifact);
    let checksum = doc["checksum_sha256"].as_str().unwrap();
    assert!(
        checksum.len() == 64
            && checksum
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    );
    let prefix = format!(
        "{{\"format_version\":7,\"artifact_type\":\"{artifact}\",\"checksum_sha256\":\"{checksum}\",\"payload\":"
    );
    let payload = bytes
        .strip_prefix(prefix.as_bytes())
        .unwrap()
        .strip_suffix(b"}")
        .unwrap();
    assert_eq!(digest(payload), checksum);
    doc
}

async fn physical_rows(
    storage: &ScopedStorage,
    r: &serde_json::Value,
    l1: bool,
) -> Vec<PhysicalRow> {
    let id = r[if l1 { "state_id" } else { "segment_id" }]
        .as_str()
        .unwrap();
    let base = "control/v1/domains/catalog";
    let data = storage
        .get_raw(&format!(
            "{base}/segments/{}/{id}.arrow",
            if l1 { "l1" } else { "l0" }
        ))
        .await
        .unwrap();
    let directory = storage
        .get_raw(&format!("{base}/indexes/{id}.idx"))
        .await
        .unwrap();
    decode_physical_rows(r, l1, &data, &directory)
}

#[allow(clippy::cognitive_complexity)] // Independent wire-format assertions.
fn decode_physical_rows(
    r: &serde_json::Value,
    l1: bool,
    data: &Bytes,
    directory: &Bytes,
) -> Vec<PhysicalRow> {
    use arrow::array::{Array, BinaryArray, BooleanArray, UInt8Array, UInt64Array};
    use arrow::ipc::reader::FileReader;
    assert!(data.len() <= 64 * 1024 * 1024);
    assert!(directory.len() <= 512 * 1024);
    assert_eq!(data.len() as u64, r["segment_size_bytes"].as_u64().unwrap());
    assert_eq!(
        directory.len() as u64,
        r["index_size_bytes"].as_u64().unwrap()
    );
    assert_eq!(digest(data), r["checksum_sha256"]);
    assert_eq!(digest(directory), r["index_checksum_sha256"]);
    let index: serde_json::Value = serde_json::from_slice(directory).unwrap();
    assert_eq!(index["formatVersion"], 1);
    assert_eq!(index["implementation"], "arco-state-control-mvp");
    assert_eq!(
        index["scope"],
        serde_json::json!({"tenant_id":"tenant", "workspace_id":"workspace", "domain":"catalog"})
    );
    assert_eq!(
        index["segmentId"],
        r[if l1 { "state_id" } else { "segment_id" }]
    );
    assert_eq!(index["level"], if l1 { "l1" } else { "l0" });
    assert_eq!(index["logicalSequence"], r["logical_sequence"]);
    assert_eq!(index["segmentSizeBytes"], data.len());
    assert_eq!(index["segmentChecksumSha256"], r["checksum_sha256"]);
    let blocks = index["blocks"].as_array().unwrap();
    assert!(!blocks.is_empty() && blocks.len() <= 4096);
    assert_eq!(
        index["recordBatchOffsets"],
        serde_json::Value::Array(blocks.iter().map(|b| b["offset"].clone()).collect())
    );
    let mut rows = Vec::new();
    let mut end = 0;
    for block in blocks {
        let first_row = rows.len();
        let offset = usize::try_from(block["offset"].as_u64().unwrap()).unwrap();
        let len = usize::try_from(block["length"].as_u64().unwrap()).unwrap();
        assert_eq!(offset, end);
        assert!(len > 0);
        end = offset.checked_add(len).unwrap();
        let bytes = data.slice(offset..end);
        assert_eq!(digest(&bytes), block["checksumSha256"]);
        let reader = FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.num_batches(), 1);
        for batch in reader {
            let batch = batch.unwrap();
            assert_physical_schema(batch.schema().as_ref());
            let column = |name| batch.column_by_name(name).unwrap();
            let kind = column("record_kind")
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();
            let key = column("key")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let value = column("value")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let tombstone = column("tombstone")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let number = |name, ordinal| {
                let values = column(name).as_any().downcast_ref::<UInt64Array>().unwrap();
                (!values.is_null(ordinal)).then(|| values.value(ordinal))
            };
            for ordinal in 0..batch.num_rows() {
                assert!(
                    !kind.is_null(ordinal) && !key.is_null(ordinal) && !tombstone.is_null(ordinal)
                );
                rows.push(PhysicalRow {
                    kind: kind.value(ordinal),
                    key: key.value(ordinal).to_vec(),
                    value: (!value.is_null(ordinal)).then(|| value.value(ordinal).to_vec()),
                    generation: number("generation", ordinal).unwrap(),
                    tombstone: tombstone.value(ordinal),
                    sequence: number("logical_sequence", ordinal).unwrap(),
                    ordinal: number("logical_ordinal", ordinal).unwrap(),
                    origin: number("origin_sequence", ordinal),
                });
            }
        }
        let selected = &rows[first_row..];
        assert_eq!(block["rowCount"], selected.len());
        assert_eq!(
            block["recordKind"],
            serde_json::json!(selected.first().map(|r| r.kind))
        );
        assert_eq!(
            block["minKeyHex"],
            serde_json::json!(selected.first().map(|r| hex::encode(&r.key)))
        );
        assert_eq!(
            block["maxKeyHex"],
            serde_json::json!(selected.last().map(|r| hex::encode(&r.key)))
        );
        assert_eq!(
            block["minOrdinal"],
            serde_json::json!(selected.iter().map(|r| r.ordinal).min())
        );
        assert_eq!(
            block["maxOrdinal"],
            serde_json::json!(selected.iter().map(|r| r.ordinal).max())
        );
        assert!(selected.iter().all(|r| r.kind == selected[0].kind));
        assert!(!selected.is_empty() || blocks.len() == 1);
    }
    assert_eq!(end, data.len());
    assert!(rows.len() <= 1_000_000);
    assert!(
        rows.windows(2)
            .all(|r| (r[0].kind, &r[0].key) < (r[1].kind, &r[1].key))
    );
    assert!(rows.iter().all(|row| row.kind <= if l1 { 1 } else { 2 }
        && serde_json::json!(row.sequence) == index["logicalSequence"]));
    assert_eq!(rows.len() as u64, index["rowCount"].as_u64().unwrap());
    assert_key_directory(&index, r, l1, &rows);
    for row in &rows {
        assert_physical_row(row, l1);
    }
    if !l1 {
        for kind in 0..3 {
            let mut ordinals: Vec<_> = rows
                .iter()
                .filter(|r| r.kind == kind)
                .map(|r| r.ordinal)
                .collect();
            ordinals.sort_unstable();
            assert!(ordinals.into_iter().enumerate().all(|(i, n)| i as u64 == n));
        }
    }
    rows
}

#[test]
fn oracle_rejects_row_metadata_and_schema_drift() {
    use arrow::datatypes::{DataType, Field, Schema};
    let row = PhysicalRow {
        kind: 1,
        key: b"event".to_vec(),
        value: Some(vec![]),
        generation: 0,
        tombstone: false,
        sequence: 2,
        ordinal: 0,
        origin: Some(2),
    };
    assert_physical_row(&row, false);
    for (generation, origin, tombstone) in [
        (1, Some(2), false),
        (0, Some(1), false),
        (0, None, false),
        (0, Some(2), true),
    ] {
        let forged = PhysicalRow {
            generation,
            tombstone,
            origin,
            ..row.clone()
        };
        assert!(std::panic::catch_unwind(|| assert_physical_row(&forged, false)).is_err());
    }
    let trim = PhysicalRow {
        kind: 2,
        value: None,
        tombstone: true,
        ..row
    };
    assert_physical_row(&trim, false);
    for (generation, origin, value) in [
        (1, Some(2), None),
        (0, None, None),
        (0, Some(2), Some(vec![])),
    ] {
        let forged = PhysicalRow {
            value,
            generation,
            origin,
            ..trim.clone()
        };
        assert!(std::panic::catch_unwind(|| assert_physical_row(&forged, false)).is_err());
    }
    let schema = Schema::new(vec![Field::new("record_kind", DataType::UInt8, true)]);
    assert!(std::panic::catch_unwind(|| assert_physical_schema(&schema)).is_err());
}

fn assert_key_directory(
    index: &serde_json::Value,
    reference: &serde_json::Value,
    l1: bool,
    rows: &[PhysicalRow],
) {
    let keys: Vec<_> = rows
        .iter()
        .filter(|r| r.kind == 0)
        .map(|r| r.key.as_slice())
        .collect();
    assert_eq!(index["distinctKvKeys"], keys.len());
    for (hex_name, utf8_name, key) in [
        ("minKeyHex", "minKeyUtf8", keys.first()),
        ("maxKeyHex", "maxKeyUtf8", keys.last()),
    ] {
        assert_eq!(index[hex_name], serde_json::json!(key.map(hex::encode)));
        assert_eq!(
            index[utf8_name],
            serde_json::json!(key.and_then(|k| std::str::from_utf8(k).ok()))
        );
    }
    if l1 {
        assert_eq!(reference["min_key_hex"], index["minKeyHex"]);
        assert_eq!(reference["max_key_hex"], index["maxKeyHex"]);
    }
    assert_eq!(index["bloomHashVersion"], 1);
    let bits = hex::decode(index["bloomBitsHex"].as_str().unwrap()).unwrap();
    assert!(bits.len() <= 128 * 1024);
    match index["bloomMode"].as_str().unwrap() {
        "Empty" => {
            assert!(keys.is_empty() && bits.is_empty());
            assert_eq!(index["bloomProbes"], 0);
        }
        "Disabled" => {
            assert!(bits.is_empty());
            assert_eq!(index["bloomProbes"], 0);
        }
        "Enabled" => {
            assert!(!keys.is_empty());
            assert_eq!(index["bloomProbes"], 7);
            let mut expected = vec![0_u8; (keys.len() * 10).div_ceil(8)];
            for key in keys {
                let hash = Sha256::digest(key);
                let a = u64::from_be_bytes(hash[..8].try_into().unwrap());
                let b = u64::from_be_bytes(hash[8..16].try_into().unwrap()) | 1;
                for probe in 0_u64..7 {
                    let bit = usize::try_from(
                        a.wrapping_add(probe.wrapping_mul(b)) % (expected.len() as u64 * 8),
                    )
                    .unwrap();
                    expected[bit / 8] |= 1 << (bit % 8);
                }
            }
            assert_eq!(bits, expected);
        }
        _ => panic!("unknown oracle Bloom mode"),
    }
}

#[test]
fn oracle_applies_exact_trims_before_later_same_id_additions() {
    let mut oracle = LogicalOracle::new();
    oracle.commit(
        Vec::new(),
        vec![("event".to_string(), Bytes::from_static(b"old"))],
        Vec::new(),
    );
    let prior = oracle.root.clone();
    oracle.commit(Vec::new(), Vec::new(), vec![("event".to_string(), 1)]);
    oracle.commit(
        Vec::new(),
        vec![("event".to_string(), Bytes::from_static(b"new"))],
        Vec::new(),
    );
    assert_eq!(
        oracle.outbox,
        vec![("event".to_string(), Bytes::from_static(b"new"), 3)]
    );
    assert_ne!(oracle.root, prior);
}

#[tokio::test]
async fn oracle_rejects_checksum_coherent_directory_and_history_corruption() {
    use arco_catalog::{ArcoStateTxn as _, ControlMvpStateStore, StateScope, TxnOptions};
    let storage = ScopedStorage::new(
        std::sync::Arc::new(arco_core::MemoryBackend::new()),
        "tenant",
        "workspace",
    )
    .unwrap();
    let store = ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"", Bytes::from_static(b"value")).await.unwrap();
    let receipt = tx.commit().await.unwrap();
    let base = "control/v1/domains/catalog";
    let raw = storage
        .get_raw(&format!(
            "{base}/manifests/{}.json",
            receipt.state_token().authority_manifest_id()
        ))
        .await
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let txid = manifest["payload"]["tx_refs"][0]["tx_id"].as_str().unwrap();
    let raw = storage
        .get_raw(&format!("{base}/transactions/{txid}.json"))
        .await
        .unwrap();
    let tx: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_envelope(&raw, "control-mvp-tx");
    for (field, value) in [
        ("format_version", serde_json::json!(8)),
        ("artifact_type", serde_json::json!("control-mvp-manifest")),
        ("checksum_sha256", serde_json::json!("0".repeat(64))),
    ] {
        let mut forged = tx.clone();
        forged[field] = value;
        let bytes = serde_json::to_vec(&forged).unwrap();
        assert!(std::panic::catch_unwind(|| assert_envelope(&bytes, "control-mvp-tx")).is_err());
    }
    let owner = &manifest["payload"]["tx_refs"][0];
    assert_transaction_owner(&tx["payload"], owner);
    for (field, value) in [
        ("tx_id", serde_json::json!("wrong-id")),
        ("sequence", serde_json::json!(2)),
        ("implementation", serde_json::json!("wrong-implementation")),
        (
            "scope",
            serde_json::json!({"tenant_id":"other", "workspace_id":"workspace", "domain":"catalog"}),
        ),
        (
            "l0_segment",
            serde_json::json!({"segment_id":"wrong", "level":"l1", "logical_sequence":2}),
        ),
    ] {
        let mut forged = tx["payload"].clone();
        forged[field] = value;
        assert!(
            std::panic::catch_unwind(|| assert_transaction_owner(&forged, owner)).is_err(),
            "oracle accepted transaction {field}"
        );
    }
    let reference = &tx["payload"]["l0_segment"];
    let id = reference["segment_id"].as_str().unwrap();
    let data = storage
        .get_raw(&format!("{base}/segments/l0/{id}.arrow"))
        .await
        .unwrap();
    let directory = storage
        .get_raw(&format!("{base}/indexes/{id}.idx"))
        .await
        .unwrap();
    assert_eq!(
        decode_physical_rows(reference, false, &data, &directory).len(),
        1
    );
    for (field, value) in [
        ("recordBatchOffsets", serde_json::json!([1])),
        ("segmentId", serde_json::json!("wrong-id")),
        ("formatVersion", serde_json::json!(2)),
        ("logicalSequence", serde_json::json!(2)),
        ("bloomBitsHex", serde_json::json!("0000")),
        ("minKeyHex", serde_json::json!("ff")),
    ] {
        let mut index: serde_json::Value = serde_json::from_slice(&directory).unwrap();
        index[field] = value;
        let forged = Bytes::from(serde_json::to_vec(&index).unwrap());
        let mut owner = reference.clone();
        owner["index_size_bytes"] = serde_json::json!(forged.len());
        owner["index_checksum_sha256"] = serde_json::json!(digest(&forged));
        assert!(
            std::panic::catch_unwind(|| decode_physical_rows(&owner, false, &data, &forged))
                .is_err(),
            "oracle accepted forged {field}"
        );
    }
    for field in ["preceding_root", "mutation_sha256", "resulting_root"] {
        let valid = &tx["payload"]["history"];
        let mut forged = valid.clone();
        forged[field] = serde_json::json!("0".repeat(64));
        assert!(
            std::panic::catch_unwind(|| assert_history_link(
                &forged,
                valid["preceding_root"].as_str().unwrap(),
                valid["mutation_sha256"].as_str().unwrap(),
                valid["resulting_root"].as_str().unwrap()
            ))
            .is_err()
        );
    }
}

type VersionObservation = Option<(u64, Option<Vec<u8>>)>;

/// Independent pinned transaction model; no production readers or witnesses.
#[allow(dead_code)] // Shared by several independently compiled contract suites.
#[derive(Clone)]
pub struct LogicalTransaction {
    pub pinned: LogicalOracle,
    pub captured_head: u64,
    pub writes: BTreeMap<Vec<u8>, Option<Bytes>>,
    pub observations: BTreeMap<Vec<u8>, VersionObservation>,
    pub overlay_reads: Vec<Vec<u8>>,
}
#[allow(dead_code)]
impl LogicalTransaction {
    pub fn pin(state: &LogicalOracle, head: u64) -> Self {
        Self {
            pinned: state.clone(),
            captured_head: head,
            writes: BTreeMap::new(),
            observations: BTreeMap::new(),
            overlay_reads: Vec::new(),
        }
    }
    pub fn get(&mut self, key: &[u8]) -> Option<(Bytes, Option<u64>)> {
        if let Some(value) = self.writes.get(key) {
            self.overlay_reads.push(key.to_vec());
            return value.clone().map(|v| (v, None));
        }
        let value = self.pinned.kv.get(key).cloned();
        self.observations.insert(key.to_vec(), value.clone());
        value.and_then(|(g, v)| v.map(|v| (Bytes::from(v), Some(g))))
    }
    pub fn assert_absent(&self, key: &[u8]) -> bool {
        self.pinned.kv.get(key).is_none_or(|(_, v)| v.is_none())
    }
    pub fn assert_generation(&self, key: &[u8], generation: u64) -> bool {
        self.pinned
            .kv
            .get(key)
            .is_some_and(|(g, v)| *g == generation && v.is_some())
    }
    pub fn range_empty(&self, start: &[u8], end: &[u8]) -> bool {
        !self
            .pinned
            .kv
            .keys()
            .any(|k| k.as_slice() >= start && k.as_slice() < end)
    }
    pub fn range_witness(&self, start: &[u8], end: &[u8]) -> u64 {
        let mut encoded = Vec::new();
        field(&mut encoded, start);
        field(&mut encoded, end);
        for (key, (generation, value)) in &self.pinned.kv {
            if key.as_slice() >= start && key.as_slice() < end {
                field(&mut encoded, key);
                encoded.extend_from_slice(&generation.to_be_bytes());
                encoded.push(u8::from(value.is_none()));
            }
        }
        u64::from_be_bytes(Sha256::digest(encoded)[..8].try_into().unwrap())
    }
    pub fn scan(&self, prefix: &[u8], after: Option<&[u8]>) -> Vec<(Vec<u8>, Bytes, Option<u64>)> {
        let mut values = self
            .pinned
            .kv
            .iter()
            .map(|(k, (g, v))| (k.clone(), v.clone().map(|v| (Bytes::from(v), Some(*g)))))
            .collect::<BTreeMap<_, _>>();
        for (k, v) in &self.writes {
            values.insert(k.clone(), v.clone().map(|v| (v, None)));
        }
        values
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix) && after.is_none_or(|a| k.as_slice() > a))
            .filter_map(|(k, v)| v.map(|(v, g)| (k, v, g)))
            .collect()
    }
    pub fn commit(self, state: &mut LogicalOracle, head: &mut u64) -> bool {
        if self.captured_head != *head {
            return false;
        }
        assert_eq!(self.pinned.root, state.root);
        state.commit(self.writes.into_iter().collect(), Vec::new(), Vec::new());
        *head += 1;
        true
    }
}
