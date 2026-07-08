//! Object-store-backed control-state MVP.

use std::collections::BTreeMap;

use arco_core::ScopedStorage;
use arco_core::storage::{WritePrecondition, WriteResult};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateStore, ArcoStateTxn, CheckpointOptions,
    CheckpointToken, KeyRange, KvPair, PredicateInputSet, StateScope, StateStoreCapabilities,
    StateToken, TxnOptions, VersionedValue,
};
use crate::error::{CatalogError, Result};

const IMPLEMENTATION: &str = "arco-state-control-mvp";

/// Object-store-backed state-store MVP for validating control-manifest authority.
#[derive(Clone)]
pub struct ControlMvpStateStore {
    storage: ScopedStorage,
    scope: StateScope,
    paths: ControlMvpPaths,
}

impl ControlMvpStateStore {
    /// Stable implementation identifier for this MVP backend.
    pub const IMPLEMENTATION: &'static str = IMPLEMENTATION;

    /// Creates a control-state MVP store over workspace-scoped storage.
    ///
    /// # Errors
    ///
    /// Returns validation errors when the storage scope does not match the state
    /// scope or when the domain cannot be represented as a safe object path.
    pub fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        if storage.tenant_id() != scope.tenant_id()
            || storage.workspace_id() != scope.workspace_id()
        {
            return Err(validation_failed(
                "control MVP storage scope does not match StateScope",
            ));
        }

        let paths = ControlMvpPaths::new(scope.domain());
        ScopedStorage::validate_path(&paths.current_pointer())?;

        Ok(Self {
            storage,
            scope,
            paths,
        })
    }

    /// Returns the scope-relative paths used by this store.
    #[must_use]
    pub fn paths(&self) -> ControlMvpPaths {
        self.paths.clone()
    }

    /// Begins a concrete control-MVP transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested transaction scope does not match or
    /// the current pointer-selected manifest cannot be loaded.
    pub async fn begin_control_txn(&self, opts: TxnOptions) -> Result<ControlMvpTxn> {
        if let Some(scope) = opts.scope()
            && scope != &self.scope
        {
            return Err(validation_failed(
                "transaction scope does not match control MVP store",
            ));
        }

        let base = self.load_current_base_state().await?;
        let next_sequence = base.state.logical_sequence + 1;
        let request_id = opts.request_id().map(ToOwned::to_owned);
        let suffix = Ulid::new().to_string().to_ascii_lowercase();
        let tx_id = request_id.clone().map_or_else(
            || format!("tx-{next_sequence:020}-{suffix}"),
            |request_id| format!("tx-{next_sequence:020}-{request_id}-{suffix}"),
        );
        let manifest_id = format!("manifest-{next_sequence:020}-{suffix}");

        Ok(ControlMvpTxn {
            store: self.clone(),
            base,
            request_id,
            tx_id,
            manifest_id,
            preconditions: Vec::new(),
            writes: BTreeMap::new(),
            outbox: Vec::new(),
        })
    }

    /// Reads projection outbox records selected by the current visible manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when visible artifacts are corrupt or unavailable.
    pub async fn current_projection_outbox(&self) -> Result<Vec<ControlMvpProjectionOutboxRecord>> {
        Ok(self.load_current_state().await?.outbox)
    }

    /// Reads projection outbox records selected by the manifest named by a token.
    ///
    /// # Errors
    ///
    /// Returns an error when the token scope mismatches or retained artifacts are
    /// corrupt or unavailable.
    pub async fn projection_outbox_at(
        &self,
        token: StateToken,
    ) -> Result<Vec<ControlMvpProjectionOutboxRecord>> {
        Ok(self.load_state_at_token(&token).await?.outbox)
    }

    async fn load_current_base_state(&self) -> Result<ControlMvpBase> {
        let pointer_meta = self.storage.head_raw(&self.paths.current_pointer()).await?;
        let Some(pointer_meta) = pointer_meta else {
            return Ok(ControlMvpBase {
                pointer_version: None,
                manifest_id: None,
                state: ReplayState::default(),
                tx_refs: Vec::new(),
            });
        };

        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        let state = self.replay_manifest(&manifest).await?;

        Ok(ControlMvpBase {
            pointer_version: Some(pointer_meta.version),
            manifest_id: Some(pointer.manifest_id),
            tx_refs: manifest.tx_refs,
            state,
        })
    }

    async fn load_current_state(&self) -> Result<ReplayState> {
        let Some(_pointer_meta) = self.storage.head_raw(&self.paths.current_pointer()).await?
        else {
            return Ok(ReplayState::default());
        };
        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        self.replay_manifest(&manifest).await
    }

    async fn load_state_at_token(&self, token: &StateToken) -> Result<ReplayState> {
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "StateToken scope does not match control MVP store",
            ));
        }
        let manifest = self.load_manifest(token.authority_manifest_id()).await?;
        if manifest.logical_sequence != token.logical_sequence() {
            return Err(invariant_violation(
                "StateToken logical sequence does not match manifest",
            ));
        }
        self.replay_manifest(&manifest).await
    }

    async fn load_pointer(&self) -> Result<ControlMvpPointer> {
        let bytes = self.storage.get_raw(&self.paths.current_pointer()).await?;
        let pointer: ControlMvpPointer = decode_json(&bytes, "control MVP pointer")?;
        pointer.validate(&self.scope)?;
        Ok(pointer)
    }

    async fn load_manifest_for_pointer(
        &self,
        pointer: &ControlMvpPointer,
    ) -> Result<ControlMvpManifest> {
        self.load_manifest_with_expected_checksum(
            &pointer.manifest_id,
            Some(&pointer.manifest_checksum_sha256),
        )
        .await
    }

    async fn load_manifest(&self, manifest_id: &str) -> Result<ControlMvpManifest> {
        self.load_manifest_with_expected_checksum(manifest_id, None)
            .await
    }

    async fn load_manifest_with_expected_checksum(
        &self,
        manifest_id: &str,
        expected_checksum: Option<&str>,
    ) -> Result<ControlMvpManifest> {
        let bytes = self
            .storage
            .get_raw(&self.paths.manifest_object(manifest_id))
            .await?;
        validate_raw_checksum(
            &bytes,
            expected_checksum,
            "control MVP manifest reference checksum",
        )?;
        let manifest: ControlMvpManifest =
            decode_envelope(&bytes, "control-mvp-manifest", "control MVP manifest")?;
        manifest.validate(&self.scope, manifest_id)?;
        Ok(manifest)
    }

    async fn replay_manifest(&self, manifest: &ControlMvpManifest) -> Result<ReplayState> {
        let mut state = ReplayState::default();
        for tx_ref in &manifest.tx_refs {
            let tx = self.load_tx(tx_ref).await?;
            state.apply_tx(&tx)?;
        }

        let checksum = state.checksum()?;
        if checksum != manifest.state_checksum_sha256 {
            return Err(invariant_violation(
                "control MVP manifest state checksum does not match replay",
            ));
        }
        Ok(state)
    }

    async fn load_tx(&self, tx_ref: &ControlMvpTxRef) -> Result<ControlMvpTxObject> {
        let bytes = self
            .storage
            .get_raw(&self.paths.tx_object(&tx_ref.tx_id))
            .await?;
        validate_raw_checksum(
            &bytes,
            Some(&tx_ref.checksum_sha256),
            "control MVP transaction reference checksum",
        )?;
        let tx: ControlMvpTxObject =
            decode_envelope(&bytes, "control-mvp-tx", "control MVP transaction")?;
        tx.validate(&self.scope, tx_ref)?;
        Ok(tx)
    }

    async fn write_checkpoint(&self, checkpoint: &ControlMvpCheckpoint) -> Result<()> {
        let bytes = encode_envelope("control-mvp-checkpoint", checkpoint)?;
        match self
            .storage
            .put_raw(
                &self.paths.checkpoint_object(&checkpoint.checkpoint_id),
                bytes,
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(precondition_failed(
                "control MVP checkpoint object already exists",
            )),
        }
    }

    async fn load_checkpoint(&self, checkpoint_id: &str) -> Result<ControlMvpCheckpoint> {
        let bytes = self
            .storage
            .get_raw(&self.paths.checkpoint_object(checkpoint_id))
            .await?;
        let checkpoint: ControlMvpCheckpoint =
            decode_envelope(&bytes, "control-mvp-checkpoint", "control MVP checkpoint")?;
        checkpoint.validate(&self.scope, checkpoint_id)?;
        Ok(checkpoint)
    }

    fn token(&self, manifest_id: String, logical_sequence: u64) -> StateToken {
        StateToken {
            scope: self.scope.clone(),
            logical_sequence,
            authority_manifest_id: manifest_id,
        }
    }

    fn checkpoint_token(&self, checkpoint_id: String) -> CheckpointToken {
        CheckpointToken {
            scope: self.scope.clone(),
            checkpoint_id,
        }
    }
}

/// Path helper for the control-state MVP object layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpPaths {
    domain: String,
}

impl ControlMvpPaths {
    /// Creates path helpers for a state-store domain.
    #[must_use]
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
        }
    }

    /// Returns the base prefix for all MVP control-state artifacts.
    #[must_use]
    pub fn base_prefix(&self) -> String {
        format!("state-store/control-mvp/{}", self.domain)
    }

    /// Returns the immutable transaction object path.
    #[must_use]
    pub fn tx_object(&self, tx_id: &str) -> String {
        format!("{}/txlog/{tx_id}.json", self.base_prefix())
    }

    /// Returns the immutable manifest object path.
    #[must_use]
    pub fn manifest_object(&self, manifest_id: &str) -> String {
        format!("{}/manifests/{manifest_id}.json", self.base_prefix())
    }

    /// Returns the current pointer path.
    #[must_use]
    pub fn current_pointer(&self) -> String {
        format!("{}/current.pointer.json", self.base_prefix())
    }

    /// Returns the immutable checkpoint object path.
    #[must_use]
    pub fn checkpoint_object(&self, checkpoint_id: &str) -> String {
        format!("{}/checkpoints/{checkpoint_id}.json", self.base_prefix())
    }
}

/// MVP projection outbox record staged inside a control transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpProjectionOutboxRecord {
    record_id: String,
    payload: Bytes,
}

impl ControlMvpProjectionOutboxRecord {
    /// Creates a projection outbox record.
    #[must_use]
    pub fn new(record_id: impl Into<String>, payload: Bytes) -> Self {
        Self {
            record_id: record_id.into(),
            payload,
        }
    }

    /// Returns the outbox record identifier.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    /// Returns the outbox payload.
    #[must_use]
    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// Concrete control-MVP transaction with MVP-only staging helpers.
pub struct ControlMvpTxn {
    store: ControlMvpStateStore,
    base: ControlMvpBase,
    request_id: Option<String>,
    tx_id: String,
    manifest_id: String,
    preconditions: Vec<Precondition>,
    writes: BTreeMap<Vec<u8>, StagedWrite>,
    outbox: Vec<ControlMvpProjectionOutboxRecord>,
}

impl ControlMvpTxn {
    /// Returns the immutable transaction object identifier this transaction will write.
    #[must_use]
    pub fn tx_id(&self) -> &str {
        &self.tx_id
    }

    /// Returns the candidate manifest identifier this transaction will write.
    #[must_use]
    pub fn candidate_manifest_id(&self) -> &str {
        &self.manifest_id
    }

    /// Stages a projection outbox record in this MVP transaction.
    pub fn stage_projection_outbox(&mut self, record: ControlMvpProjectionOutboxRecord) {
        self.outbox.push(record);
    }

    /// Commits the transaction and returns the resulting state token.
    ///
    /// # Errors
    ///
    /// Returns an error when artifact writes fail, preconditions are not met, or
    /// pointer CAS publication loses to another writer.
    pub async fn commit(self) -> Result<StateToken> {
        self.commit_inner().await
    }

    fn get_inner(&self, key: &[u8]) -> Option<VersionedValue> {
        if let Some(write) = self.writes.get(key) {
            return match write {
                StagedWrite::Put(bytes) => Some(VersionedValue::new(bytes.clone(), None)),
                StagedWrite::Delete => None,
            };
        }
        self.base
            .state
            .kv
            .get(key)
            .filter(|value| !value.tombstone)
            .map(|value| VersionedValue::new(value.bytes.clone(), Some(value.generation)))
    }

    fn scan_prefix_inner(&self, prefix: &[u8]) -> Vec<KvPair> {
        let mut entries = self
            .base
            .state
            .kv
            .iter()
            .filter(|(key, value)| key.starts_with(prefix) && !value.tombstone)
            .map(|(key, value)| {
                (
                    key.clone(),
                    VersionedValue::new(value.bytes.clone(), Some(value.generation)),
                )
            })
            .collect::<BTreeMap<_, _>>();

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

        entries
            .into_iter()
            .map(|(key, value)| KvPair::new(key, value))
            .collect()
    }

    fn put_inner(&mut self, key: &[u8], value: Bytes) {
        self.writes.insert(key.to_vec(), StagedWrite::Put(value));
    }

    fn delete_inner(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), StagedWrite::Delete);
    }

    fn assert_absent_inner(&mut self, key: &[u8]) -> Result<()> {
        let witness = self.base.state.point_witness(key);
        if matches!(witness, PointWitness::Present(_)) {
            return Err(precondition_failed(
                "cannot assert absence for a present control MVP key",
            ));
        }
        self.preconditions.push(Precondition::Absent {
            key: key.to_vec(),
            witness,
        });
        Ok(())
    }

    fn assert_generation_inner(&mut self, key: &[u8], generation: u64) -> Result<()> {
        if self.base.state.point_witness(key) != PointWitness::Present(generation) {
            return Err(precondition_failed(
                "cannot assert a control MVP key generation that is not currently present",
            ));
        }
        self.preconditions.push(Precondition::Generation {
            key: key.to_vec(),
            expected: generation,
        });
        Ok(())
    }

    /// Returns the current transaction-base witness for a key range.
    #[must_use]
    pub(crate) fn range_witness(&self, range: &KeyRange) -> u64 {
        self.base.state.range_witness(range)
    }

    async fn commit_inner(self) -> Result<StateToken> {
        for precondition in &self.preconditions {
            self.base.state.validate_precondition(precondition)?;
        }

        let next_sequence = self.base.state.logical_sequence + 1;
        let tx = ControlMvpTxObject {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            tx_id: self.tx_id.clone(),
            base_manifest_id: self.base.manifest_id.clone(),
            sequence: next_sequence,
            request_id: self.request_id.clone(),
            writes: self
                .writes
                .into_iter()
                .map(|(key, write)| ControlMvpWriteEntry::from_staged(key, next_sequence, write))
                .collect(),
            outbox: self
                .outbox
                .iter()
                .map(ControlMvpOutboxEntry::from_record)
                .collect(),
        };
        let tx_bytes = encode_envelope("control-mvp-tx", &tx)?;
        let tx_checksum = sha256_hex(&tx_bytes);
        put_immutable(
            &self.store.storage,
            &self.store.paths.tx_object(&self.tx_id),
            tx_bytes,
            "control MVP transaction object already exists",
        )
        .await?;

        let mut candidate_state = self.base.state.clone();
        candidate_state.apply_tx(&tx)?;

        let mut tx_refs = self.base.tx_refs.clone();
        tx_refs.push(ControlMvpTxRef {
            tx_id: self.tx_id.clone(),
            sequence: next_sequence,
            checksum_sha256: tx_checksum,
        });

        let manifest = ControlMvpManifest {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            manifest_id: self.manifest_id.clone(),
            logical_sequence: next_sequence,
            base_manifest_id: self.base.manifest_id,
            tx_refs,
            state_checksum_sha256: candidate_state.checksum()?,
        };
        let manifest_bytes = encode_envelope("control-mvp-manifest", &manifest)?;
        let manifest_checksum = sha256_hex(&manifest_bytes);
        put_immutable(
            &self.store.storage,
            &self.store.paths.manifest_object(&self.manifest_id),
            manifest_bytes,
            "control MVP manifest object already exists",
        )
        .await?;

        let pointer = ControlMvpPointer {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            manifest_id: self.manifest_id.clone(),
            logical_sequence: next_sequence,
            manifest_checksum_sha256: manifest_checksum,
        };
        let pointer_bytes = encode_json(&pointer, "control MVP pointer")?;
        let precondition = self.base.pointer_version.map_or(
            WritePrecondition::DoesNotExist,
            WritePrecondition::MatchesVersion,
        );
        match self
            .store
            .storage
            .put_raw(
                &self.store.paths.current_pointer(),
                pointer_bytes,
                precondition,
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(self.store.token(self.manifest_id, next_sequence)),
            WriteResult::PreconditionFailed { .. } => Err(CatalogError::CasFailed {
                message: "control MVP pointer CAS lost to a newer manifest".to_string(),
            }),
        }
    }
}

#[async_trait]
impl ArcoStateReader for ControlMvpStateStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self
            .load_current_state()
            .await?
            .kv
            .get(key)
            .filter(|value| !value.tombstone)
            .map(|value| value.bytes.clone()))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>> {
        Ok(self.load_current_state().await?.scan_prefix(prefix))
    }

    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        Ok(Box::new(ControlMvpRetainedReader {
            state: self.load_state_at_token(&token).await?,
        }))
    }

    async fn read_checkpoint(&self, token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "CheckpointToken scope does not match control MVP store",
            ));
        }
        let checkpoint = self.load_checkpoint(token.checkpoint_id()).await?;
        let manifest = self
            .load_manifest_with_expected_checksum(
                &checkpoint.manifest_id,
                Some(&checkpoint.manifest_checksum_sha256),
            )
            .await?;
        Ok(Box::new(ControlMvpRetainedReader {
            state: self.replay_manifest(&manifest).await?,
        }))
    }
}

#[async_trait]
impl ArcoStateAdmin for ControlMvpStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities::control_mvp(Self::IMPLEMENTATION)
    }

    async fn current_state_token(&self) -> Result<StateToken> {
        let pointer = self.load_pointer().await?;
        Ok(self.token(pointer.manifest_id, pointer.logical_sequence))
    }

    async fn checkpoint(&self, opts: CheckpointOptions) -> Result<CheckpointToken> {
        if let Some(scope) = opts.scope()
            && scope != &self.scope
        {
            return Err(validation_failed(
                "checkpoint scope does not match control MVP store",
            ));
        }
        let pointer = self.load_pointer().await?;
        let checkpoint_id = format!(
            "checkpoint-{:020}-{}",
            pointer.logical_sequence,
            Ulid::new().to_string().to_ascii_lowercase()
        );
        let checkpoint = ControlMvpCheckpoint {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.scope),
            checkpoint_id: checkpoint_id.clone(),
            manifest_id: pointer.manifest_id,
            logical_sequence: pointer.logical_sequence,
            manifest_checksum_sha256: pointer.manifest_checksum_sha256,
            min_retention_seconds: opts.min_retention_seconds(),
        };
        self.write_checkpoint(&checkpoint).await?;
        Ok(self.checkpoint_token(checkpoint_id))
    }
}

#[async_trait]
impl ArcoStateStore for ControlMvpStateStore {
    async fn begin_txn(&self, opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>> {
        Ok(Box::new(self.begin_control_txn(opts).await?))
    }
}

#[async_trait]
impl ArcoStateTxn for ControlMvpTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        Ok(self.get_inner(key))
    }

    async fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<KvPair>> {
        Ok(self.scan_prefix_inner(prefix))
    }

    async fn put(&mut self, key: &[u8], value: Bytes) -> Result<()> {
        self.put_inner(key, value);
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_inner(key);
        Ok(())
    }

    async fn assert_absent(&mut self, key: &[u8]) -> Result<()> {
        self.assert_absent_inner(key)
    }

    async fn assert_generation(&mut self, key: &[u8], generation: u64) -> Result<()> {
        self.assert_generation_inner(key, generation)
    }

    async fn assert_range_empty(&mut self, range: KeyRange) -> Result<()> {
        if self.base.state.range_has_entries(&range) {
            return Err(precondition_failed(
                "cannot assert a non-empty control MVP range",
            ));
        }
        let witness = self.base.state.range_witness(&range);
        self.preconditions
            .push(Precondition::RangeEmpty { range, witness });
        Ok(())
    }

    async fn assert_range_unchanged(
        &mut self,
        range: KeyRange,
        observed_generation: u64,
    ) -> Result<()> {
        if self.base.state.range_witness(&range) != observed_generation {
            return Err(precondition_failed(
                "cannot assert a stale control MVP range witness",
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
        let witness = self.base.state.predicate_witness(keys, ranges);
        Ok(PredicateInputSet::with_model_witness(
            keys.to_vec(),
            ranges.to_vec(),
            witness,
        ))
    }

    async fn assert_inputs_unchanged(&mut self, inputs: PredicateInputSet) -> Result<()> {
        let witness = inputs
            .model_witness()
            .ok_or_else(|| precondition_failed("predicate input set has no control MVP witness"))?;
        if self
            .base
            .state
            .predicate_witness(inputs.point_keys(), inputs.ranges())
            != witness
        {
            return Err(precondition_failed(
                "cannot assert stale control MVP predicate inputs",
            ));
        }
        self.preconditions
            .push(Precondition::Predicate { inputs, witness });
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<StateToken> {
        (*self).commit_inner().await
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ControlMvpBase {
    pointer_version: Option<String>,
    manifest_id: Option<String>,
    state: ReplayState,
    tx_refs: Vec<ControlMvpTxRef>,
}

#[derive(Debug, Clone, Default)]
struct ReplayState {
    logical_sequence: u64,
    kv: BTreeMap<Vec<u8>, StoredValue>,
    outbox: Vec<ControlMvpProjectionOutboxRecord>,
}

impl ReplayState {
    fn apply_tx(&mut self, tx: &ControlMvpTxObject) -> Result<()> {
        let expected = self.logical_sequence + 1;
        if tx.sequence != expected {
            return Err(invariant_violation(format!(
                "control MVP replay expected sequence {expected}, got {}",
                tx.sequence
            )));
        }

        for write in &tx.writes {
            if write.generation != tx.sequence {
                return Err(invariant_violation(
                    "control MVP write generation does not match transaction sequence",
                ));
            }
            self.kv.insert(
                write.key.clone(),
                StoredValue {
                    bytes: Bytes::from(write.value.clone().unwrap_or_default()),
                    generation: write.generation,
                    tombstone: write.value.is_none(),
                },
            );
        }
        self.outbox
            .extend(tx.outbox.iter().map(ControlMvpOutboxEntry::to_record));
        self.logical_sequence = tx.sequence;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<KvPair> {
        self.kv
            .iter()
            .filter(|(key, value)| key.starts_with(prefix) && !value.tombstone)
            .map(|(key, value)| {
                KvPair::new(
                    key.clone(),
                    VersionedValue::new(value.bytes.clone(), Some(value.generation)),
                )
            })
            .collect()
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
                if self.point_witness(key) == *witness
                    && !matches!(witness, PointWitness::Present(_))
                {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "absent key witness changed before control MVP commit",
                    ))
                }
            }
            Precondition::Generation { key, expected } => {
                if self.point_witness(key) == PointWitness::Present(*expected) {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "point generation witness changed before control MVP commit",
                    ))
                }
            }
            Precondition::RangeEmpty { range, witness } => {
                if self.range_witness(range) == *witness && !self.range_has_entries(range) {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "empty range witness changed before control MVP commit",
                    ))
                }
            }
            Precondition::RangeUnchanged { range, witness } => {
                if self.range_witness(range) == *witness {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "unchanged range witness changed before control MVP commit",
                    ))
                }
            }
            Precondition::Predicate { inputs, witness } => {
                if self.predicate_witness(inputs.point_keys(), inputs.ranges()) == *witness {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "predicate input witness changed before control MVP commit",
                    ))
                }
            }
        }
    }

    fn range_has_entries(&self, range: &KeyRange) -> bool {
        self.kv.keys().any(|key| key_in_range(key, range))
    }

    fn range_witness(&self, range: &KeyRange) -> u64 {
        let mut hasher = Sha256::new();
        hash_bytes(&mut hasher, range.start());
        hash_bytes(&mut hasher, range.end());
        for (key, value) in self
            .kv
            .iter()
            .filter(|(key, _value)| key_in_range(key, range))
        {
            hash_bytes(&mut hasher, key);
            hash_u64(&mut hasher, value.generation);
            hasher.update([u8::from(value.tombstone)]);
        }
        digest_u64(hasher)
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

        digest_u64(hasher)
    }

    fn checksum(&self) -> Result<String> {
        let digest = ReplayStateDigest {
            logical_sequence: self.logical_sequence,
            entries: self
                .kv
                .iter()
                .map(|(key, value)| ReplayStateDigestEntry {
                    key: key.clone(),
                    generation: value.generation,
                    value: (!value.tombstone).then(|| value.bytes.to_vec()),
                })
                .collect(),
            outbox: self
                .outbox
                .iter()
                .map(ControlMvpOutboxEntry::from_record)
                .collect(),
        };
        let bytes = encode_json_vec(&digest, "control MVP replay digest")?;
        Ok(sha256_hex(&bytes))
    }
}

#[derive(Debug, Clone)]
struct StoredValue {
    bytes: Bytes,
    generation: u64,
    tombstone: bool,
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

#[derive(Debug, Serialize, Deserialize)]
struct ChecksumEnvelope<T> {
    artifact_type: String,
    checksum_sha256: String,
    payload: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpScopeDoc {
    tenant_id: String,
    workspace_id: String,
    domain: String,
}

impl ControlMvpScopeDoc {
    fn matches_scope(&self, scope: &StateScope) -> bool {
        self.tenant_id == scope.tenant_id()
            && self.workspace_id == scope.workspace_id()
            && self.domain == scope.domain()
    }
}

impl From<&StateScope> for ControlMvpScopeDoc {
    fn from(value: &StateScope) -> Self {
        Self {
            tenant_id: value.tenant_id().to_string(),
            workspace_id: value.workspace_id().to_string(),
            domain: value.domain().to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlMvpPointer {
    implementation: String,
    scope: ControlMvpScopeDoc,
    manifest_id: String,
    logical_sequence: u64,
    manifest_checksum_sha256: String,
}

impl ControlMvpPointer {
    fn validate(&self, scope: &StateScope) -> Result<()> {
        if self.implementation != IMPLEMENTATION {
            return Err(invariant_violation(
                "control MVP pointer implementation mismatch",
            ));
        }
        if !self.scope.matches_scope(scope) {
            return Err(validation_failed("control MVP pointer scope mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlMvpManifest {
    implementation: String,
    scope: ControlMvpScopeDoc,
    manifest_id: String,
    logical_sequence: u64,
    base_manifest_id: Option<String>,
    tx_refs: Vec<ControlMvpTxRef>,
    state_checksum_sha256: String,
}

impl ControlMvpManifest {
    fn validate(&self, scope: &StateScope, expected_manifest_id: &str) -> Result<()> {
        if self.implementation != IMPLEMENTATION {
            return Err(invariant_violation(
                "control MVP manifest implementation mismatch",
            ));
        }
        if !self.scope.matches_scope(scope) {
            return Err(validation_failed("control MVP manifest scope mismatch"));
        }
        if self.manifest_id != expected_manifest_id {
            return Err(invariant_violation(
                "control MVP manifest id does not match requested path",
            ));
        }
        if self.tx_refs.last().map_or(0, |tx_ref| tx_ref.sequence) != self.logical_sequence {
            return Err(invariant_violation(
                "control MVP manifest sequence does not match selected tx refs",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpTxRef {
    tx_id: String,
    sequence: u64,
    checksum_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlMvpTxObject {
    implementation: String,
    scope: ControlMvpScopeDoc,
    tx_id: String,
    base_manifest_id: Option<String>,
    sequence: u64,
    request_id: Option<String>,
    writes: Vec<ControlMvpWriteEntry>,
    outbox: Vec<ControlMvpOutboxEntry>,
}

impl ControlMvpTxObject {
    fn validate(&self, scope: &StateScope, tx_ref: &ControlMvpTxRef) -> Result<()> {
        if self.implementation != IMPLEMENTATION {
            return Err(invariant_violation(
                "control MVP transaction implementation mismatch",
            ));
        }
        if !self.scope.matches_scope(scope) {
            return Err(validation_failed("control MVP transaction scope mismatch"));
        }
        if self.tx_id != tx_ref.tx_id || self.sequence != tx_ref.sequence {
            return Err(invariant_violation(
                "control MVP transaction ref does not match transaction payload",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlMvpWriteEntry {
    key: Vec<u8>,
    generation: u64,
    value: Option<Vec<u8>>,
}

impl ControlMvpWriteEntry {
    fn from_staged(key: Vec<u8>, generation: u64, write: StagedWrite) -> Self {
        match write {
            StagedWrite::Put(bytes) => Self {
                key,
                generation,
                value: Some(bytes.to_vec()),
            },
            StagedWrite::Delete => Self {
                key,
                generation,
                value: None,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpOutboxEntry {
    record_id: String,
    payload: Vec<u8>,
}

impl ControlMvpOutboxEntry {
    fn from_record(record: &ControlMvpProjectionOutboxRecord) -> Self {
        Self {
            record_id: record.record_id.clone(),
            payload: record.payload.to_vec(),
        }
    }

    fn to_record(&self) -> ControlMvpProjectionOutboxRecord {
        ControlMvpProjectionOutboxRecord::new(
            self.record_id.clone(),
            Bytes::from(self.payload.clone()),
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayStateDigest {
    logical_sequence: u64,
    entries: Vec<ReplayStateDigestEntry>,
    outbox: Vec<ControlMvpOutboxEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayStateDigestEntry {
    key: Vec<u8>,
    generation: u64,
    value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlMvpCheckpoint {
    implementation: String,
    scope: ControlMvpScopeDoc,
    checkpoint_id: String,
    manifest_id: String,
    logical_sequence: u64,
    manifest_checksum_sha256: String,
    min_retention_seconds: Option<u64>,
}

impl ControlMvpCheckpoint {
    fn validate(&self, scope: &StateScope, expected_checkpoint_id: &str) -> Result<()> {
        if self.implementation != IMPLEMENTATION {
            return Err(invariant_violation(
                "control MVP checkpoint implementation mismatch",
            ));
        }
        if !self.scope.matches_scope(scope) {
            return Err(validation_failed("control MVP checkpoint scope mismatch"));
        }
        if self.checkpoint_id != expected_checkpoint_id {
            return Err(invariant_violation(
                "control MVP checkpoint id does not match requested path",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ControlMvpRetainedReader {
    state: ReplayState,
}

#[async_trait]
impl ArcoStateReader for ControlMvpRetainedReader {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self
            .state
            .kv
            .get(key)
            .filter(|value| !value.tombstone)
            .map(|value| value.bytes.clone()))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>> {
        Ok(self.state.scan_prefix(prefix))
    }

    async fn read_at(&self, _token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported(
            "nested StateToken reads on control MVP retained readers",
        ))
    }

    async fn read_checkpoint(&self, _token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported(
            "nested CheckpointToken reads on control MVP retained readers",
        ))
    }
}

async fn put_immutable(
    storage: &ScopedStorage,
    path: &str,
    bytes: Bytes,
    precondition_message: &str,
) -> Result<()> {
    match storage
        .put_raw(path, bytes, WritePrecondition::DoesNotExist)
        .await?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => Err(precondition_failed(precondition_message)),
    }
}

fn encode_envelope<T: Serialize>(artifact_type: &str, payload: &T) -> Result<Bytes> {
    let payload_bytes = encode_json_vec(payload, artifact_type)?;
    let envelope = ChecksumEnvelope {
        artifact_type: artifact_type.to_string(),
        checksum_sha256: sha256_hex(&payload_bytes),
        payload,
    };
    Ok(Bytes::from(encode_json_vec(&envelope, artifact_type)?))
}

fn decode_envelope<T>(bytes: &[u8], artifact_type: &str, context: &str) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let envelope: ChecksumEnvelope<T> = decode_json(bytes, context)?;
    if envelope.artifact_type != artifact_type {
        return Err(invariant_violation(format!(
            "{context} artifact type mismatch"
        )));
    }
    let payload_bytes = encode_json_vec(&envelope.payload, context)?;
    let checksum = sha256_hex(&payload_bytes);
    if checksum != envelope.checksum_sha256 {
        return Err(invariant_violation(format!("{context} checksum mismatch")));
    }
    Ok(envelope.payload)
}

fn validate_raw_checksum(bytes: &[u8], expected: Option<&str>, context: &str) -> Result<()> {
    if let Some(expected) = expected {
        let actual = sha256_hex(bytes);
        if actual != expected {
            return Err(invariant_violation(format!("{context} mismatch")));
        }
    }
    Ok(())
}

fn encode_json<T: Serialize>(value: &T, context: &str) -> Result<Bytes> {
    Ok(Bytes::from(encode_json_vec(value, context)?))
}

fn encode_json_vec<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| CatalogError::Serialization {
        message: format!("failed to serialize {context}: {error}"),
    })
}

fn decode_json<T>(bytes: &[u8], context: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(bytes).map_err(|error| CatalogError::Serialization {
        message: format!("failed to deserialize {context}: {error}"),
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn digest_u64(hasher: Sha256) -> u64 {
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    for (target, source) in bytes.iter_mut().zip(digest.iter()) {
        *target = *source;
    }
    u64::from_be_bytes(bytes)
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

fn unsupported(operation: &str) -> CatalogError {
    CatalogError::UnsupportedOperation {
        message: format!("{operation} are not supported by arco-state-control-mvp"),
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
