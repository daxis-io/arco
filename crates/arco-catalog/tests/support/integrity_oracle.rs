//! Independent logical oracle. It encodes the documented format directly and
//! never calls production replay, hashing, partitioning or maintenance helpers.
#![allow(clippy::indexing_slicing)] // Oracle assertions intentionally fail on malformed fixtures.
use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct LogicalOracle {
    pub sequence: u64,
    pub root: String,
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
        let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(doc["payload"]["logical_sequence"], self.sequence);
        assert_eq!(doc["payload"]["history_root"], self.root);
        assert_eq!(
            doc["payload"]["state_checksum_sha256"],
            digest(&serde_json::to_vec(&state).unwrap())
        );
    }
}

#[test]
fn oracle_applies_exact_trims_before_same_id_additions() {
    let mut oracle = LogicalOracle::new();
    oracle.commit(
        Vec::new(),
        vec![("event".to_string(), Bytes::from_static(b"old"))],
        Vec::new(),
    );
    let prior = oracle.root.clone();
    oracle.commit(
        Vec::new(),
        vec![("event".to_string(), Bytes::from_static(b"new"))],
        vec![("event".to_string(), 1)],
    );
    assert_eq!(
        oracle.outbox,
        vec![("event".to_string(), Bytes::from_static(b"new"), 2)]
    );
    assert_ne!(oracle.root, prior);
}
