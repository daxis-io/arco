//! Deterministic reference model for the state-store seam.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateStore, ArcoStateTxn, CheckpointOptions,
    CheckpointToken, KeyRange, KvPair, PredicateInputSet, StateScope, StateStoreCapabilities,
    StateToken, TxnOptions, VersionedValue,
};
use crate::error::{CatalogError, Result};

/// Deterministic in-memory reference implementation of the state-store seam.
#[derive(Debug, Clone)]
pub struct ModelStateStore {
    scope: StateScope,
    inner: Arc<Mutex<ModelState>>,
}

impl ModelStateStore {
    /// Stable implementation identifier for the deterministic model.
    pub const IMPLEMENTATION: &'static str = "arco-state-model";

    /// Creates an empty deterministic model store for an authority scope.
    #[must_use]
    pub fn new(scope: StateScope) -> Self {
        Self {
            scope,
            inner: Arc::new(Mutex::new(ModelState::default())),
        }
    }

    /// Returns committed records in deterministic commit order.
    #[must_use]
    pub fn committed_records(&self) -> Vec<ModelCommitRecord> {
        lock_model_state(&self.inner).log.clone()
    }

    /// Returns deterministic folded entries, including tombstoned keys.
    #[must_use]
    pub fn folded_entries(&self) -> Vec<(Vec<u8>, Option<Bytes>, u64)> {
        lock_model_state(&self.inner).folded_entries()
    }

    /// Returns deterministic transition explanations for committed records.
    #[must_use]
    pub fn explain_transitions(&self) -> Vec<String> {
        self.committed_records()
            .iter()
            .map(ModelCommitRecord::explain)
            .collect()
    }

    /// Returns a stable SHA-256-derived witness for a half-open key range.
    #[must_use]
    pub fn range_witness(&self, range: &KeyRange) -> u64 {
        lock_model_state(&self.inner).range_witness(range)
    }

    /// Replays committed model records into a folded model store.
    ///
    /// # Errors
    ///
    /// Returns an invariant violation when the record stream has a gap,
    /// duplicate sequence with different content, or write generation mismatch.
    pub fn replay_from_committed_records(
        scope: StateScope,
        records: Vec<ModelCommitRecord>,
    ) -> Result<Self> {
        let mut state = ModelState::default();
        let mut seen = BTreeMap::new();

        for record in records {
            if let Some(existing) = seen.get(&record.sequence) {
                if existing == &record {
                    continue;
                }
                return Err(invariant_violation(format!(
                    "conflicting model record for sequence {}",
                    record.sequence
                )));
            }
            if record.sequence != state.logical_sequence + 1 {
                return Err(invariant_violation(format!(
                    "model replay expected sequence {}, got {}",
                    state.logical_sequence + 1,
                    record.sequence
                )));
            }
            state.apply_record(&record)?;
            seen.insert(record.sequence, record.clone());
            state.log.push(record);
        }

        Ok(Self {
            scope,
            inner: Arc::new(Mutex::new(state)),
        })
    }

    fn token(&self, logical_sequence: u64) -> StateToken {
        StateToken {
            scope: self.scope.clone(),
            logical_sequence,
            authority_manifest_id: format!("model-state-{logical_sequence:020}"),
        }
    }
}

/// Deterministic committed model record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCommitRecord {
    sequence: u64,
    request_id: Option<String>,
    logical_events: Vec<String>,
    writes: Vec<ModelWrite>,
}

impl ModelCommitRecord {
    /// Returns the committed logical sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the request identifier attached to the transaction, if any.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Returns deterministic logical events represented by this commit.
    #[must_use]
    pub fn logical_events(&self) -> &[String] {
        &self.logical_events
    }

    /// Returns deterministic staged writes folded by key.
    #[must_use]
    pub fn writes(&self) -> &[ModelWrite] {
        &self.writes
    }

    fn explain(&self) -> String {
        let request_id = self.request_id.as_deref().unwrap_or("<none>");
        let writes = self
            .writes
            .iter()
            .map(ModelWrite::explain)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "sequence={} request_id={} writes={}",
            self.sequence, request_id, writes
        )
    }
}

/// Deterministic write folded into a model commit record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelWrite {
    key: Vec<u8>,
    generation: u64,
    value: Option<Bytes>,
}

impl ModelWrite {
    /// Returns the key written by this record.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the key generation assigned by the accepted commit.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the stored value, or `None` for a tombstone.
    #[must_use]
    pub const fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    fn explain(&self) -> String {
        let key = String::from_utf8_lossy(&self.key);
        if self.value.is_some() {
            format!("put({key}@{})", self.generation)
        } else {
            format!("delete({key}@{})", self.generation)
        }
    }
}

#[derive(Debug, Default)]
struct ModelState {
    logical_sequence: u64,
    kv: BTreeMap<Vec<u8>, StoredValue>,
    log: Vec<ModelCommitRecord>,
}

#[derive(Debug, Clone)]
struct StoredValue {
    bytes: Bytes,
    generation: u64,
    tombstone: bool,
}

#[derive(Debug)]
struct ModelTxn {
    store: ModelStateStore,
    request_id: Option<String>,
    preconditions: Vec<Precondition>,
    writes: BTreeMap<Vec<u8>, StagedWrite>,
}

#[derive(Debug)]
enum StagedWrite {
    Put(Bytes),
    Delete,
}

#[derive(Debug)]
enum Precondition {
    Absent {
        key: Vec<u8>,
        witness: PointWitness,
    },
    Generation {
        key: Vec<u8>,
        expected: u64,
    },
    RangeEmpty {
        range: KeyRange,
        witness: u64,
    },
    RangeUnchanged {
        range: KeyRange,
        witness: u64,
    },
    Predicate {
        inputs: PredicateInputSet,
        witness: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PointWitness {
    Absent,
    Present(u64),
    Tombstone(u64),
}

impl ModelState {
    fn folded_entries(&self) -> Vec<(Vec<u8>, Option<Bytes>, u64)> {
        self.kv
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    (!value.tombstone).then(|| value.bytes.clone()),
                    value.generation,
                )
            })
            .collect()
    }

    fn apply_record(&mut self, record: &ModelCommitRecord) -> Result<()> {
        let expected_events = logical_events_for_writes(&record.writes);
        if record.logical_events != expected_events {
            return Err(invariant_violation(format!(
                "model record {} had non-deterministic logical events",
                record.sequence
            )));
        }
        for write in &record.writes {
            if write.generation != record.sequence {
                return Err(invariant_violation(format!(
                    "model write generation {} did not match record sequence {}",
                    write.generation, record.sequence
                )));
            }
            self.kv.insert(
                write.key.clone(),
                StoredValue {
                    bytes: write.value.clone().unwrap_or_default(),
                    generation: write.generation,
                    tombstone: write.value.is_none(),
                },
            );
        }
        self.logical_sequence = record.sequence;
        Ok(())
    }

    fn point_witness(&self, key: &[u8]) -> PointWitness {
        self.kv.get(key).map_or(PointWitness::Absent, |value| {
            if value.tombstone {
                PointWitness::Tombstone(value.generation)
            } else {
                PointWitness::Present(value.generation)
            }
        })
    }

    fn validate_precondition(&self, precondition: &Precondition) -> Result<()> {
        match precondition {
            Precondition::Absent { key, witness } => {
                if matches!(witness, PointWitness::Present(_)) {
                    return Err(precondition_failed(
                        "key was present when absence was asserted",
                    ));
                }
                let current = self.point_witness(key);
                if &current == witness && !matches!(current, PointWitness::Present(_)) {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "absent key witness changed before model commit",
                    ))
                }
            }
            Precondition::Generation { key, expected } => {
                if self.point_witness(key) == PointWitness::Present(*expected) {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "point generation witness changed before model commit",
                    ))
                }
            }
            Precondition::RangeEmpty { range, witness } => {
                if self.range_witness(range) == *witness && !self.range_has_entries(range) {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "empty range witness changed before model commit",
                    ))
                }
            }
            Precondition::RangeUnchanged { range, witness } => {
                if self.range_witness(range) == *witness {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "unchanged range witness changed before model commit",
                    ))
                }
            }
            Precondition::Predicate { inputs, witness } => {
                if self.predicate_witness(inputs.point_keys(), inputs.ranges()) == *witness {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "predicate input witness changed before model commit",
                    ))
                }
            }
        }
    }

    fn range_has_entries(&self, range: &KeyRange) -> bool {
        self.kv
            .keys()
            .any(|key| key_in_range(key.as_slice(), range))
    }

    fn range_witness(&self, range: &KeyRange) -> u64 {
        let mut hasher = Sha256::new();
        hash_bytes(&mut hasher, range.start());
        hash_bytes(&mut hasher, range.end());
        for (key, value) in self
            .kv
            .iter()
            .filter(|(key, _value)| key_in_range(key.as_slice(), range))
        {
            hash_bytes(&mut hasher, key);
            hash_u64(&mut hasher, value.generation);
            hasher.update([u8::from(value.tombstone)]);
        }
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 8];
        for (target, source) in bytes.iter_mut().zip(digest.iter()) {
            *target = *source;
        }
        u64::from_be_bytes(bytes)
    }

    fn predicate_witness(&self, keys: &[Vec<u8>], ranges: &[KeyRange]) -> u64 {
        let mut hasher = Sha256::new();

        let mut sorted_keys = keys.iter().collect::<Vec<_>>();
        sorted_keys.sort();
        for key in sorted_keys {
            hash_bytes(&mut hasher, key);
            match self.point_witness(key) {
                PointWitness::Absent => hasher.update([0]),
                PointWitness::Present(generation) => {
                    hasher.update([1]);
                    hash_u64(&mut hasher, generation);
                }
                PointWitness::Tombstone(generation) => {
                    hasher.update([2]);
                    hash_u64(&mut hasher, generation);
                }
            }
        }

        let mut sorted_ranges = ranges.iter().collect::<Vec<_>>();
        sorted_ranges.sort_by(|left, right| {
            left.start()
                .cmp(right.start())
                .then_with(|| left.end().cmp(right.end()))
        });
        for range in sorted_ranges {
            hash_bytes(&mut hasher, range.start());
            hash_bytes(&mut hasher, range.end());
            hash_u64(&mut hasher, self.range_witness(range));
        }

        let digest = hasher.finalize();
        let mut bytes = [0_u8; 8];
        for (target, source) in bytes.iter_mut().zip(digest.iter()) {
            *target = *source;
        }
        u64::from_be_bytes(bytes)
    }
}

#[async_trait]
impl ArcoStateReader for ModelStateStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = lock_model_state(&self.inner);
        Ok(inner
            .kv
            .get(key)
            .filter(|value| !value.tombstone)
            .map(|value| value.bytes.clone()))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>> {
        let inner = lock_model_state(&self.inner);
        Ok(inner
            .kv
            .iter()
            .filter(|(key, value)| key.starts_with(prefix) && !value.tombstone)
            .map(|(key, value)| {
                KvPair::new(
                    key.clone(),
                    VersionedValue::new(value.bytes.clone(), Some(value.generation)),
                )
            })
            .collect())
    }

    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "StateToken scope does not match model store",
            ));
        }
        if token.authority_manifest_id()
            != self.token(token.logical_sequence()).authority_manifest_id()
        {
            return Err(validation_failed(
                "StateToken authority manifest does not match model token format",
            ));
        }

        let records = {
            let inner = lock_model_state(&self.inner);
            if token.logical_sequence() > inner.logical_sequence {
                return Err(precondition_failed(
                    "StateToken is ahead of the current model sequence",
                ));
            }
            inner
                .log
                .iter()
                .filter(|record| record.sequence <= token.logical_sequence())
                .cloned()
                .collect::<Vec<_>>()
        };

        Ok(Box::new(Self::replay_from_committed_records(
            self.scope.clone(),
            records,
        )?))
    }

    async fn read_checkpoint(&self, _token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported("model checkpoint reads"))
    }
}

#[async_trait]
impl ArcoStateAdmin for ModelStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities::deterministic_model(Self::IMPLEMENTATION)
    }

    async fn current_state_token(&self) -> Result<StateToken> {
        let inner = lock_model_state(&self.inner);
        Ok(self.token(inner.logical_sequence))
    }

    async fn checkpoint(&self, _opts: CheckpointOptions) -> Result<CheckpointToken> {
        Err(unsupported("model checkpoints"))
    }
}

#[async_trait]
impl ArcoStateStore for ModelStateStore {
    async fn begin_txn(&self, opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>> {
        if let Some(scope) = opts.scope()
            && scope != &self.scope
        {
            return Err(validation_failed(
                "transaction scope does not match model store",
            ));
        }

        Ok(Box::new(ModelTxn {
            store: self.clone(),
            request_id: opts.request_id().map(ToOwned::to_owned),
            preconditions: Vec::new(),
            writes: BTreeMap::new(),
        }))
    }
}

#[async_trait]
impl ArcoStateTxn for ModelTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        if let Some(write) = self.writes.get(key) {
            return Ok(match write {
                StagedWrite::Put(bytes) => Some(VersionedValue::new(bytes.clone(), None)),
                StagedWrite::Delete => None,
            });
        }

        let inner = lock_model_state(&self.store.inner);
        Ok(inner
            .kv
            .get(key)
            .filter(|value| !value.tombstone)
            .map(|value| VersionedValue::new(value.bytes.clone(), Some(value.generation))))
    }

    async fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<KvPair>> {
        let mut entries = {
            let inner = lock_model_state(&self.store.inner);
            inner
                .kv
                .iter()
                .filter(|(key, value)| key.starts_with(prefix) && !value.tombstone)
                .map(|(key, value)| {
                    (
                        key.clone(),
                        VersionedValue::new(value.bytes.clone(), Some(value.generation)),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };

        for (key, write) in &self.writes {
            if key.starts_with(prefix) {
                match write {
                    StagedWrite::Put(bytes) => {
                        entries.insert(key.clone(), VersionedValue::new(bytes.clone(), None));
                    }
                    StagedWrite::Delete => {
                        entries.remove(key);
                    }
                }
            }
        }

        Ok(entries
            .into_iter()
            .map(|(key, value)| KvPair::new(key, value))
            .collect())
    }

    async fn put(&mut self, key: &[u8], value: Bytes) -> Result<()> {
        self.writes.insert(key.to_vec(), StagedWrite::Put(value));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.writes.insert(key.to_vec(), StagedWrite::Delete);
        Ok(())
    }

    async fn assert_absent(&mut self, key: &[u8]) -> Result<()> {
        let witness = {
            let inner = lock_model_state(&self.store.inner);
            inner.point_witness(key)
        };
        if matches!(witness, PointWitness::Present(_)) {
            return Err(precondition_failed(
                "cannot assert absence for a present model key",
            ));
        }
        self.preconditions.push(Precondition::Absent {
            key: key.to_vec(),
            witness,
        });
        Ok(())
    }

    async fn assert_generation(&mut self, key: &[u8], generation: u64) -> Result<()> {
        let current = {
            let inner = lock_model_state(&self.store.inner);
            inner.point_witness(key)
        };
        if current != PointWitness::Present(generation) {
            return Err(precondition_failed(
                "cannot assert a model key generation that is not currently present",
            ));
        }
        self.preconditions.push(Precondition::Generation {
            key: key.to_vec(),
            expected: generation,
        });
        Ok(())
    }

    async fn assert_range_empty(&mut self, range: KeyRange) -> Result<()> {
        let (range_has_entries, witness) = {
            let inner = lock_model_state(&self.store.inner);
            (inner.range_has_entries(&range), inner.range_witness(&range))
        };
        if range_has_entries {
            return Err(precondition_failed("cannot assert a non-empty model range"));
        }
        self.preconditions
            .push(Precondition::RangeEmpty { range, witness });
        Ok(())
    }

    async fn assert_range_unchanged(
        &mut self,
        range: KeyRange,
        observed_generation: u64,
    ) -> Result<()> {
        let current = {
            let inner = lock_model_state(&self.store.inner);
            inner.range_witness(&range)
        };
        if current != observed_generation {
            return Err(precondition_failed(
                "cannot assert a stale model range witness",
            ));
        }
        self.preconditions.push(Precondition::RangeUnchanged {
            range,
            witness: observed_generation,
        });
        Ok(())
    }

    async fn read_set(
        &mut self,
        keys: &[Vec<u8>],
        ranges: &[KeyRange],
    ) -> Result<PredicateInputSet> {
        let witness = {
            let inner = lock_model_state(&self.store.inner);
            inner.predicate_witness(keys, ranges)
        };
        Ok(PredicateInputSet::with_model_witness(
            keys.to_vec(),
            ranges.to_vec(),
            witness,
        ))
    }

    async fn assert_inputs_unchanged(&mut self, inputs: PredicateInputSet) -> Result<()> {
        let witness = inputs
            .model_witness()
            .ok_or_else(|| precondition_failed("predicate input set has no model witness"))?;
        let current = {
            let inner = lock_model_state(&self.store.inner);
            inner.predicate_witness(inputs.point_keys(), inputs.ranges())
        };
        if current != witness {
            return Err(precondition_failed("cannot assert stale predicate inputs"));
        }
        self.preconditions
            .push(Precondition::Predicate { inputs, witness });
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<StateToken> {
        let store = self.store.clone();
        let next_sequence = {
            let mut inner = lock_model_state(&store.inner);
            for precondition in &self.preconditions {
                inner.validate_precondition(precondition)?;
            }
            let next_sequence = inner.logical_sequence + 1;

            let mut writes = Vec::with_capacity(self.writes.len());
            for (key, write) in self.writes {
                match write {
                    StagedWrite::Put(bytes) => {
                        inner.kv.insert(
                            key.clone(),
                            StoredValue {
                                bytes: bytes.clone(),
                                generation: next_sequence,
                                tombstone: false,
                            },
                        );
                        writes.push(ModelWrite {
                            key,
                            generation: next_sequence,
                            value: Some(bytes),
                        });
                    }
                    StagedWrite::Delete => {
                        inner.kv.insert(
                            key.clone(),
                            StoredValue {
                                bytes: Bytes::new(),
                                generation: next_sequence,
                                tombstone: true,
                            },
                        );
                        writes.push(ModelWrite {
                            key,
                            generation: next_sequence,
                            value: None,
                        });
                    }
                }
            }

            inner.logical_sequence = next_sequence;
            let logical_events = logical_events_for_writes(&writes);
            inner.log.push(ModelCommitRecord {
                sequence: next_sequence,
                request_id: self.request_id,
                logical_events,
                writes,
            });
            next_sequence
        };

        Ok(store.token(next_sequence))
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn unsupported(operation: &str) -> CatalogError {
    CatalogError::UnsupportedOperation {
        message: format!("{operation} are not supported by arco-state-model yet"),
    }
}

fn precondition_failed(message: &str) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: message.to_string(),
    }
}

fn validation_failed(message: &str) -> CatalogError {
    CatalogError::Validation {
        message: message.to_string(),
    }
}

fn invariant_violation(message: impl Into<String>) -> CatalogError {
    CatalogError::InvariantViolation {
        message: message.into(),
    }
}

fn lock_model_state(inner: &Mutex<ModelState>) -> MutexGuard<'_, ModelState> {
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}

fn logical_events_for_writes(writes: &[ModelWrite]) -> Vec<String> {
    writes
        .iter()
        .map(|write| {
            let key = String::from_utf8_lossy(&write.key);
            write.value.as_ref().map_or_else(
                || format!("delete {key} generation={}", write.generation),
                |bytes| {
                    format!(
                        "put {key} generation={} bytes={}",
                        write.generation,
                        bytes.len()
                    )
                },
            )
        })
        .collect()
}

fn key_in_range(key: &[u8], range: &KeyRange) -> bool {
    key >= range.start() && key < range.end()
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hash_u64(hasher, bytes.len() as u64);
    hasher.update(bytes);
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> StateScope {
        StateScope::new("tenant", "workspace", "catalog")
    }

    fn put_record(key: &[u8], bytes: &'static [u8]) -> ModelCommitRecord {
        let write = ModelWrite {
            key: key.to_vec(),
            generation: 1,
            value: Some(Bytes::from_static(bytes)),
        };
        ModelCommitRecord {
            sequence: 1,
            request_id: None,
            logical_events: logical_events_for_writes(std::slice::from_ref(&write)),
            writes: vec![write],
        }
    }

    #[test]
    fn replay_rejects_conflicting_duplicate_records() {
        let first = put_record(b"catalog/default", b"v1");
        let conflicting = put_record(b"catalog/default", b"v2");

        let result =
            ModelStateStore::replay_from_committed_records(scope(), vec![first, conflicting]);

        assert!(matches!(
            result,
            Err(CatalogError::InvariantViolation { .. })
        ));
    }

    #[test]
    fn replay_rejects_non_deterministic_logical_events() {
        let write = ModelWrite {
            key: b"catalog/default".to_vec(),
            generation: 1,
            value: Some(Bytes::from_static(b"v1")),
        };
        let record = ModelCommitRecord {
            sequence: 1,
            request_id: None,
            logical_events: vec!["unexpected event".to_string()],
            writes: vec![write],
        };

        let result = ModelStateStore::replay_from_committed_records(scope(), vec![record]);

        assert!(matches!(
            result,
            Err(CatalogError::InvariantViolation { .. })
        ));
    }
}
