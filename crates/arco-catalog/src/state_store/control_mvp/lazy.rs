//! Request-local authenticated access. Full replay remains the commit boundary.
#[cfg(any(test, feature = "test-utils"))]
use super::build_scan_page;
use super::hash_tag;
use super::{
    ArcoStateTxn, BTreeMap, BTreeSet, BlockScanBudget, BlockScanCursor, Bytes, CatalogError,
    CommitOutcome, ControlMvpBase, ControlMvpManifest, ControlMvpProjectionOutboxRecord,
    ControlMvpScopeDoc, ControlMvpSegmentIndex, ControlMvpSegmentLevel, ControlMvpSegmentRef,
    ControlMvpSegmentRow, ControlMvpStateStore, ControlMvpTxn, Digest, KeyRange, KvPair,
    MAX_SCAN_ARROW_BYTES, MAX_SEGMENT_ROWS, PointWitness, Precondition, PredicateInputSet,
    ReplayState, Result, SEGMENT_RECORD_OUTBOX, SEGMENT_RECORD_OUTBOX_TRIM, ScanPage, ScanRequest,
    Sha256, StagedWrite, StateToken, StoredValue, VersionedValue, async_trait, block_key_bounds,
    digest_u64, hash_bytes, hash_u64, index_key_bounds, integrity, invariant_violation,
    key_bounds_overlap_prefix, precondition_failed, state_reference_key_bounds,
    state_segment_reference, stored_row_value, validation_failed,
};
#[cfg(feature = "test-utils")]
use super::{TxnOptions, cost};
use crate::state_store::{ScanContinuation, ScanContinuationOrigin};

const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_REQUEST_ENTRIES: usize = 1_000_000;
const ENTRY_BYTES: usize = 96;

/// A pin contains authenticated metadata, never a partially populated replay.
#[derive(Debug)]
pub(super) enum TransactionBase {
    Genesis,
    Manifest {
        manifest: Box<ControlMvpManifest>,
        digest: String,
        head_version: String,
        writer_epoch: u64,
        reclamation_generation: u64,
    },
}

impl TransactionBase {
    pub(super) fn manifest(&self) -> Option<&ControlMvpManifest> {
        match self {
            Self::Genesis => None,
            Self::Manifest { manifest, .. } => Some(manifest),
        }
    }
    pub(super) fn logical_sequence(&self) -> u64 {
        self.manifest()
            .map_or(0, |manifest| manifest.logical_sequence)
    }
    pub(super) const fn writer_epoch(&self) -> u64 {
        match self {
            Self::Genesis => 0,
            Self::Manifest { writer_epoch, .. } => *writer_epoch,
        }
    }
    pub(super) const fn reclamation_generation(&self) -> u64 {
        match self {
            Self::Genesis => 0,
            Self::Manifest {
                reclamation_generation,
                ..
            } => *reclamation_generation,
        }
    }
    pub(super) fn pointer_version(&self) -> Option<&str> {
        match self {
            Self::Genesis => None,
            Self::Manifest { head_version, .. } => Some(head_version),
        }
    }
    fn token(&self, store: &ControlMvpStateStore) -> Option<StateToken> {
        match self {
            Self::Genesis => None,
            Self::Manifest {
                manifest, digest, ..
            } => Some(
                store
                    .token(manifest.manifest_id.clone(), manifest.logical_sequence)
                    .with_manifest_witness(digest.clone()),
            ),
        }
    }
    pub(super) async fn materialize_for_commit(
        &self,
        store: &ControlMvpStateStore,
    ) -> Result<ControlMvpBase> {
        if let Self::Manifest {
            manifest, digest, ..
        } = self
        {
            // Pins are nondurable. The selected authority itself must still be
            // available and match its raw digest at the publication boundary.
            store
                .load_manifest_with_expected_checksum(&manifest.manifest_id, Some(digest))
                .await?;
        }
        self.materialize(store).await
    }

    pub(super) async fn materialize(&self, store: &ControlMvpStateStore) -> Result<ControlMvpBase> {
        let (state, history_anchor, base_states, tx_refs) = match self.manifest() {
            None => (
                ReplayState::empty(&store.scope),
                integrity::genesis(&ControlMvpScopeDoc::from(&store.scope)),
                Vec::new(),
                Vec::new(),
            ),
            Some(manifest) => {
                let state = store.replay_for_successor(manifest).await?;
                let (base_states, tx_refs) = manifest.successor_anchor();
                (
                    state,
                    manifest.successor_history_anchor(),
                    base_states,
                    tx_refs,
                )
            }
        };
        Ok(ControlMvpBase {
            history_anchor,
            manifest_checksum_sha256: match self {
                Self::Genesis => None,
                Self::Manifest { digest, .. } => Some(digest.clone()),
            },
            reclamation_generation: self.reclamation_generation(),
            pointer_version: self.pointer_version().map(str::to_owned),
            manifest_id: self.manifest().map(|m| m.manifest_id.clone()),
            writer_epoch: self.writer_epoch(),
            layout_generation: self.manifest().map_or(0, |m| m.layout_generation),
            state,
            base_states,
            tx_refs,
        })
    }
}

impl ControlMvpStateStore {
    pub(super) async fn pin_transaction_base(&self) -> Result<TransactionBase> {
        let Some(meta) = self.storage.head(&self.paths.current_pointer()).await? else {
            return Ok(TransactionBase::Genesis);
        };
        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        Ok(TransactionBase::Manifest {
            manifest: Box::new(manifest),
            digest: pointer.manifest_checksum_sha256,
            head_version: meta.version,
            writer_epoch: pointer.writer_epoch,
            reclamation_generation: pointer.reclamation_generation,
        })
    }

    /// Current local operation phase, for test backend accounting.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[must_use]
    pub fn test_cost_phase() -> &'static str {
        cost::current()
    }
    /// Drains local work counters partitioned by phase. Nested counts are
    /// already included in request totals.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[must_use]
    pub fn take_test_phase_work() -> BTreeMap<&'static str, [u64; 20]> {
        cost::take()
    }

    /// Test-only pre-Gate-4 eager transaction reference. Begin reconstructs the
    /// complete base; its reads and assertions use that snapshot. Commit retains
    /// the historical promoted-base check and candidate validation.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub async fn begin_eager_reference(&self, opts: TxnOptions) -> Result<ControlMvpTxn> {
        let mut txn = self.begin_control_txn(opts).await?;
        txn.eager_base = Some(txn.base.materialize(self).await?);
        Ok(txn)
    }

    async fn selected_outbox_rows(
        &self,
        reference: &ControlMvpSegmentRef,
        index: &ControlMvpSegmentIndex,
        id: &str,
        kind: u8,
    ) -> Result<Vec<ControlMvpSegmentRow>> {
        let mut selected = Vec::new();
        for block in &index.blocks {
            if block.record_kind != Some(kind)
                || !block_key_bounds(block)?.is_some_and(|(min, max)| {
                    id.as_bytes() >= min.as_slice() && id.as_bytes() <= max.as_slice()
                })
            {
                continue;
            }
            for row in self.load_block(reference, block).await? {
                validate_outbox_row(reference, &row)?;
                if row.key == id.as_bytes() {
                    selected.push(row);
                }
            }
        }
        if selected.len() > 1 {
            return Err(invariant_violation("duplicate selected outbox ID"));
        }
        Ok(selected)
    }

    async fn outbox_record_from_manifest(
        &self,
        manifest: &ControlMvpManifest,
        id: &str,
    ) -> Result<Option<ControlMvpProjectionOutboxRecord>> {
        let mut selected = None;
        // Manifest KV bounds and Bloom filters cannot exclude an outbox ID.
        // Keyless L1 shards are owners too.
        for state in &manifest.base_states {
            let reference = state_segment_reference(state);
            let (_, index) = self.load_segment_index(&reference).await?;
            if index_key_bounds(&index)? != state_reference_key_bounds(state)? {
                return Err(invariant_violation(
                    "outbox directory differs from owning L1 bounds",
                ));
            }
            for row in self
                .selected_outbox_rows(&reference, &index, id, SEGMENT_RECORD_OUTBOX)
                .await?
            {
                if selected.is_some() {
                    return Err(invariant_violation("duplicate L1 outbox ID"));
                }
                selected = Some(outbox_row_record(row)?);
            }
        }
        for tx_ref in &manifest.tx_refs {
            let tx = self.load_tx_metadata(tx_ref).await?;
            let (_, index) = self.load_segment_index(&tx.l0_segment).await?;
            for row in self
                .selected_outbox_rows(&tx.l0_segment, &index, id, SEGMENT_RECORD_OUTBOX_TRIM)
                .await?
            {
                if selected
                    .as_ref()
                    .is_none_or(|record: &ControlMvpProjectionOutboxRecord| {
                        record.origin_sequence != row.origin_sequence
                    })
                {
                    return Err(invariant_violation(
                        "selected outbox trim names an absent or different incarnation",
                    ));
                }
                selected = None;
            }
            for row in self
                .selected_outbox_rows(&tx.l0_segment, &index, id, SEGMENT_RECORD_OUTBOX)
                .await?
            {
                if selected.is_some() {
                    return Err(invariant_violation("duplicate L0 outbox ID"));
                }
                selected = Some(outbox_row_record(row)?);
            }
        }
        Ok(selected)
    }
}

fn validate_outbox_row(reference: &ControlMvpSegmentRef, row: &ControlMvpSegmentRow) -> Result<()> {
    let origin = row
        .origin_sequence
        .ok_or_else(|| invariant_violation("selected outbox row lacks origin"))?;
    if row.generation != 0
        || origin == 0
        || origin > reference.logical_sequence
        || std::str::from_utf8(&row.key).is_err()
    {
        return Err(invariant_violation("invalid selected outbox row metadata"));
    }
    match row.record_kind {
        SEGMENT_RECORD_OUTBOX
            if !row.tombstone
                && row.value.is_some()
                && (reference.level == ControlMvpSegmentLevel::L1
                    || origin == reference.logical_sequence) =>
        {
            Ok(())
        }
        SEGMENT_RECORD_OUTBOX_TRIM
            if reference.level == ControlMvpSegmentLevel::L0
                && row.tombstone
                && row.value.is_none() =>
        {
            Ok(())
        }
        _ => Err(invariant_violation(
            "invalid selected outbox row kind or payload",
        )),
    }
}
fn outbox_row_record(row: ControlMvpSegmentRow) -> Result<ControlMvpProjectionOutboxRecord> {
    Ok(ControlMvpProjectionOutboxRecord {
        record_id: String::from_utf8(row.key)
            .map_err(|_| invariant_violation("invalid outbox ID"))?,
        payload: Bytes::from(
            row.value
                .ok_or_else(|| invariant_violation("outbox payload absent"))?,
        ),
        origin_sequence: row.origin_sequence,
        observed_root: None,
    })
}

/// Essential state is charged before mutation. Optional memo payloads may be
/// evicted to admit observations or writes. Accounting is deliberately not RSS.
#[derive(Default)]
pub(super) struct TransactionReads {
    bytes: usize,
    entries: usize,
    memo_bytes: usize,
    memo_entries: usize,
    points: BTreeMap<Vec<u8>, PointWitness>,
    overlay_reads: BTreeSet<Vec<u8>>,
    values: BTreeMap<Vec<u8>, Option<StoredValue>>,
    ranges: BTreeMap<(Vec<u8>, Vec<u8>), (u64, bool)>,
    scans: Vec<ScanObservation>,
}
struct ScanObservation {
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    through: Option<Vec<u8>>,
    witness: u64,
}
impl TransactionReads {
    pub(super) fn check_essential(&self, bytes: usize, entries: usize) -> Result<()> {
        if self.bytes.saturating_add(bytes) > MAX_REQUEST_BYTES
            || self.entries.saturating_add(entries) > MAX_REQUEST_ENTRIES
        {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "transaction retained state exceeds 64 MiB or one million entries"
                    .to_string(),
            });
        }
        Ok(())
    }
    pub(super) fn reserve(&mut self, bytes: usize, entries: usize) -> Result<()> {
        self.check_essential(bytes, entries)?;
        if !self.memo_fits(bytes, entries) {
            self.values.clear();
            self.memo_bytes = 0;
            self.memo_entries = 0;
        }
        self.bytes += bytes;
        self.entries += entries;
        Ok(())
    }
    fn memo_fits(&self, bytes: usize, entries: usize) -> bool {
        self.bytes
            .saturating_add(self.memo_bytes)
            .saturating_add(bytes)
            <= MAX_REQUEST_BYTES
            && self
                .entries
                .saturating_add(self.memo_entries)
                .saturating_add(entries)
                <= MAX_REQUEST_ENTRIES
    }
    fn point(&mut self, key: &[u8], value: Option<StoredValue>) -> Result<Option<StoredValue>> {
        if !self.points.contains_key(key) {
            self.reserve(key.len() + ENTRY_BYTES, 1)?;
            self.points
                .insert(key.to_vec(), point_witness(value.as_ref()));
        }
        let bytes = key.len() + ENTRY_BYTES + value.as_ref().map_or(0, |v| v.bytes.len());
        if !self.values.contains_key(key) && self.memo_fits(bytes, 1) {
            self.memo_bytes += bytes;
            self.memo_entries += 1;
            self.values.insert(key.to_vec(), value.clone());
        }
        Ok(value)
    }
    fn overlay(&mut self, key: &[u8]) -> Result<()> {
        if !self.overlay_reads.contains(key) {
            self.reserve(key.len() + ENTRY_BYTES, 1)?;
            self.overlay_reads.insert(key.to_vec());
        }
        Ok(())
    }
    pub(super) fn validate(&self, state: &ReplayState) -> Result<()> {
        for (key, observed) in &self.points {
            if state.point_witness(key) != *observed {
                return Err(precondition_failed(
                    "authenticated point observation changed",
                ));
            }
        }
        for ((start, end), (witness, _)) in &self.ranges {
            if state.range_witness(&KeyRange::new(start.clone(), end.clone())) != *witness {
                return Err(precondition_failed(
                    "authenticated range observation changed",
                ));
            }
        }
        for scan in &self.scans {
            let mut hasher = Sha256::new();
            for (key, value) in &state.kv {
                if key.starts_with(&scan.prefix)
                    && scan
                        .after
                        .as_deref()
                        .is_none_or(|after| key.as_slice() > after)
                    && scan
                        .through
                        .as_deref()
                        .is_none_or(|through| key.as_slice() <= through)
                {
                    hash_version(&mut hasher, key, value);
                }
            }
            if digest_u64(hasher) != scan.witness {
                return Err(precondition_failed(
                    "authenticated scan observation changed",
                ));
            }
        }
        Ok(())
    }
}
fn point_witness(value: Option<&StoredValue>) -> PointWitness {
    value.map_or(PointWitness::Absent, |v| {
        if v.tombstone {
            PointWitness::Tombstone(v.generation)
        } else {
            PointWitness::Present(v.generation)
        }
    })
}
fn hash_version(hasher: &mut Sha256, key: &[u8], value: &StoredValue) {
    hash_bytes(hasher, key);
    hash_u64(hasher, value.generation);
    hash_tag(hasher, u8::from(value.tombstone));
}

/// A resolved stream keeps at most one block per L1/L0 input. Tombstones remain
/// in this stream; public filtering and overlay application happen above it.
pub(super) struct ResolvedRows {
    cursors: Vec<BlockScanCursor>,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
}
impl ResolvedRows {
    pub(super) async fn new(
        store: &ControlMvpStateStore,
        manifest: Option<&ControlMvpManifest>,
        prefix: &[u8],
        after: Option<&[u8]>,
        range: Option<&KeyRange>,
    ) -> Result<Self> {
        let mut cursors = Vec::new();
        if let Some(manifest) = manifest {
            let mut base = BlockScanCursor::new(std::collections::VecDeque::new());
            base.range = range.cloned();
            for reference in &manifest.base_states {
                let bounds = state_reference_key_bounds(reference)?;
                if key_bounds_overlap_prefix(bounds.as_ref(), prefix)
                    && bounds.as_ref().is_some_and(|(min, max)| {
                        after.is_none_or(|after| max.as_slice() > after)
                            && range.is_none_or(|range| {
                                max.as_slice() >= range.start() && min.as_slice() < range.end()
                            })
                    })
                {
                    base.expected_bounds
                        .insert(reference.state_id.clone(), bounds);
                    base.references
                        .push_back(state_segment_reference(reference));
                }
            }
            cursors.push(base);
            for reference in &manifest.tx_refs {
                let tx = store.load_tx_metadata(reference).await?;
                let mut cursor =
                    BlockScanCursor::new(std::collections::VecDeque::from([tx.l0_segment]));
                cursor.range = range.cloned();
                cursors.push(cursor);
            }
        }
        Ok(Self {
            cursors,
            prefix: prefix.to_vec(),
            after: after.map(<[u8]>::to_vec),
        })
    }
    pub(super) async fn fill(
        &mut self,
        store: &ControlMvpStateStore,
        budget: &mut BlockScanBudget,
    ) -> Result<bool> {
        for cursor in &mut self.cursors {
            if !cursor
                .fill(store, &self.prefix, self.after.as_deref(), budget)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub(super) fn key(&self) -> Option<&[u8]> {
        self.cursors
            .iter()
            .filter_map(|c| c.rows.front().map(|r| r.key.as_slice()))
            .min()
    }
    pub(super) fn take(&mut self, key: &[u8]) -> Option<StoredValue> {
        let mut row = None;
        for cursor in &mut self.cursors {
            if cursor.rows.front().is_some_and(|row| row.key == key) {
                row = cursor.rows.pop_front();
            }
        }
        row.map(stored_row_value)
    }
    pub(super) fn may_have_more(&self) -> bool {
        self.cursors.iter().any(BlockScanCursor::may_have_more)
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != u8::MAX {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}

impl ControlMvpTxn {
    async fn base_value(&mut self, key: &[u8]) -> Result<Option<StoredValue>> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(base) = &self.eager_base {
            return Ok(base.state.kv.get(key).cloned());
        }
        if let Some(value) = self.reads.values.get(key) {
            return Ok(value.clone());
        }
        let value = match self.base.manifest() {
            None => None,
            Some(manifest) => {
                self.store
                    .get_versioned_from_manifest(manifest, key)
                    .await?
            }
        };
        self.reads.point(key, value)
    }
    pub(super) async fn base_outbox_record(
        &self,
        id: &str,
    ) -> Result<Option<ControlMvpProjectionOutboxRecord>> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(base) = &self.eager_base {
            return Ok(base
                .state
                .outbox
                .iter()
                .find(|r| r.record_id == id)
                .cloned());
        }
        match self.base.manifest() {
            None => Ok(None),
            Some(manifest) => self.store.outbox_record_from_manifest(manifest, id).await,
        }
    }
    pub(super) async fn ensure_outbox_id_available(&self, id: &str, entity: &str) -> Result<()> {
        if self.outbox.iter().any(|record| record.record_id == id)
            || self
                .projection_intents
                .iter()
                .any(|intent| intent.intent_id == id)
            || self.outbox_trim.iter().any(|trim| trim.record_id == id)
            || self.base_outbox_record(id).await?.is_some()
        {
            return Err(CatalogError::AlreadyExists {
                entity: entity.to_string(),
                name: id.to_string(),
            });
        }
        Ok(())
    }
    fn add_precondition(&mut self, precondition: Precondition) -> Result<()> {
        let bytes = match &precondition {
            Precondition::Absent { key, .. } | Precondition::Generation { key, .. } => key.len(),
            Precondition::RangeEmpty { range, .. } | Precondition::RangeUnchanged { range, .. } => {
                range.start().len() + range.end().len()
            }
            Precondition::Predicate { inputs, .. } => {
                inputs
                    .point_keys()
                    .iter()
                    .map(|key| key.len() + ENTRY_BYTES)
                    .sum::<usize>()
                    + inputs
                        .ranges()
                        .iter()
                        .map(|r| r.start().len() + r.end().len() + ENTRY_BYTES)
                        .sum::<usize>()
            }
        };
        let entries = match &precondition {
            Precondition::Predicate { inputs, .. } => {
                1 + inputs.point_keys().len() + inputs.ranges().len()
            }
            _ => 1,
        };
        self.reads.reserve(bytes + ENTRY_BYTES, entries)?;
        self.preconditions.push(precondition);
        Ok(())
    }
    fn stage_write(&mut self, key: &[u8], write: StagedWrite) -> Result<()> {
        let size = |write: &StagedWrite| {
            key.len()
                + ENTRY_BYTES
                + match write {
                    StagedWrite::Put(value) => value.len(),
                    StagedWrite::Delete => 0,
                }
        };
        let prior = self.writes.get(key).map(size);
        let bytes = size(&write);
        self.reads.reserve(
            bytes.saturating_sub(prior.unwrap_or(0)),
            usize::from(prior.is_none()),
        )?;
        if let Some(prior) = prior {
            self.reads.bytes -= prior.saturating_sub(bytes);
        }
        self.writes.insert(key.to_vec(), write);
        Ok(())
    }
    async fn range_evidence(&mut self, range: &KeyRange) -> Result<(u64, bool)> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(base) = &self.eager_base {
            return Ok((
                base.state.range_witness(range),
                base.state.range_has_entries(range),
            ));
        }
        let cache_key = (range.start().to_vec(), range.end().to_vec());
        if let Some(result) = self.reads.ranges.get(&cache_key) {
            return Ok(*result);
        }
        let mut hasher = Sha256::new();
        hash_bytes(&mut hasher, range.start());
        hash_bytes(&mut hasher, range.end());
        let mut present = false;
        if range.start() < range.end() {
            let mut stream =
                ResolvedRows::new(&self.store, self.base.manifest(), b"", None, Some(range))
                    .await?;
            loop {
                // A new chunk replenishes only transient I/O budgets. Resolved
                // rows are hashed immediately; no total-range row cap exists.
                let mut budget = BlockScanBudget {
                    blocks: 64,
                    segments: 64,
                    bytes: MAX_SCAN_ARROW_BYTES,
                };
                let mut progressed = false;
                loop {
                    if !stream.fill(&self.store, &mut budget).await? {
                        break;
                    }
                    let Some(key) = stream.key().map(<[u8]>::to_vec) else {
                        let result = (digest_u64(hasher), present);
                        self.memo_range(cache_key, result)?;
                        return Ok(result);
                    };
                    let value = stream
                        .take(&key)
                        .ok_or_else(|| invariant_violation("range stream lost row"))?;
                    hash_version(&mut hasher, &key, &value);
                    present = true;
                    progressed = true;
                }
                if !progressed && budget.blocks == 64 {
                    return Err(CatalogError::MaintenanceBackpressure {
                        message: "range cannot resolve a row within raw Arrow budget".to_string(),
                    });
                }
                tokio::task::yield_now().await;
            }
        }
        let result = (digest_u64(hasher), present);
        self.memo_range(cache_key, result)?;
        Ok(result)
    }
    fn memo_range(&mut self, key: (Vec<u8>, Vec<u8>), result: (u64, bool)) -> Result<()> {
        // The completed fingerprint is also the mandatory range observation.
        self.reads
            .reserve(key.0.len() + key.1.len() + ENTRY_BYTES, 1)?;
        self.reads.ranges.insert(key, result);
        Ok(())
    }
    pub(crate) async fn range_witness(&mut self, range: &KeyRange) -> Result<u64> {
        Ok(self.range_evidence(range).await?.0)
    }
    async fn predicate_witness(&mut self, keys: &[Vec<u8>], ranges: &[KeyRange]) -> Result<u64> {
        let mut hasher = Sha256::new();
        let mut keys = keys.iter().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            hash_bytes(&mut hasher, key);
            match point_witness(self.base_value(key).await?.as_ref()) {
                PointWitness::Absent => hash_tag(&mut hasher, 0),
                PointWitness::Present(g) => {
                    hash_tag(&mut hasher, 1);
                    hash_u64(&mut hasher, g);
                }
                PointWitness::Tombstone(g) => {
                    hash_tag(&mut hasher, 2);
                    hash_u64(&mut hasher, g);
                }
            }
        }
        let mut ranges = ranges.iter().collect::<Vec<_>>();
        ranges.sort_by(|a, b| a.start().cmp(b.start()).then_with(|| a.end().cmp(b.end())));
        for range in ranges {
            hash_bytes(&mut hasher, range.start());
            hash_bytes(&mut hasher, range.end());
            hash_u64(&mut hasher, self.range_witness(range).await?);
        }
        Ok(digest_u64(hasher))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one ordered merge and evidence boundary precede pagination"
    )]
    async fn scan_lazy(&mut self, request: ScanRequest) -> Result<ScanPage> {
        request.validate_for_origin(&self.store.scope, Some(self.nonce))?;
        let token = self.base.token(&self.store);
        if let Some(cursor) = &request.token {
            let ScanContinuationOrigin::Transaction { base, .. } = &cursor.origin else {
                return Err(validation_failed("foreign transaction cursor"));
            };
            if base != &token {
                return Err(validation_failed("transaction cursor base mismatch"));
            }
        }
        let after = request.effective_start_after();
        let mut stream = ResolvedRows::new(
            &self.store,
            self.base.manifest(),
            request.prefix(),
            after,
            None,
        )
        .await?;
        let lower = after.map_or_else(
            || std::ops::Bound::Included(request.prefix().to_vec()),
            |after| std::ops::Bound::Excluded(after.to_vec()),
        );
        let upper = prefix_end(request.prefix())
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut overlay = self.writes.range((lower, upper)).peekable();
        let mut budget = BlockScanBudget {
            blocks: 64,
            segments: request.max_segments(),
            bytes: MAX_SCAN_ARROW_BYTES,
        };
        let mut entries = Vec::new();
        let mut bytes = 0_usize;
        let mut boundary: Option<Vec<u8>> = None;
        let mut has_more = false;
        let mut hasher = Sha256::new();
        let mut resolved = 0_usize;
        loop {
            if !stream.fill(&self.store, &mut budget).await? {
                if boundary.is_none() {
                    return Err(CatalogError::MaintenanceBackpressure {
                        message: "scan cannot resolve a row within block/segment/byte budget"
                            .to_string(),
                    });
                }
                has_more = true;
                break;
            }
            let Some(key) = stream
                .key()
                .into_iter()
                .chain(overlay.peek().map(|(key, _)| key.as_slice()))
                .min()
                .map(<[u8]>::to_vec)
            else {
                break;
            };
            let base = stream.take(&key);
            let staged = if overlay.peek().is_some_and(|(k, _)| **k == key) {
                overlay.next().map(|(_, v)| v)
            } else {
                None
            };
            let value = match staged {
                Some(StagedWrite::Put(value)) => Some(VersionedValue::new(value.clone(), None)),
                Some(StagedWrite::Delete) => None,
                None => base
                    .as_ref()
                    .filter(|v| !v.tombstone)
                    .map(|v| VersionedValue::new(v.bytes.clone(), Some(v.generation))),
            };
            if let Some(value) = value {
                let size = key.len().saturating_add(value.bytes().len());
                if bytes.saturating_add(size) > request.max_bytes() {
                    if boundary.is_none() {
                        return Err(validation_failed(
                            "scan entry exceeds requested page byte budget",
                        ));
                    }
                    has_more = true;
                    break;
                }
                bytes += size;
                entries.push(KvPair::new(key.clone(), value));
            }
            if let Some(base) = base {
                hash_version(&mut hasher, &key, &base);
            }
            boundary = Some(key);
            resolved += 1;
            if entries.len() >= request.max_rows()
                || bytes >= request.max_bytes()
                || resolved >= MAX_SEGMENT_ROWS
            {
                has_more = stream.may_have_more() || overlay.peek().is_some();
                break;
            }
        }
        let through = if has_more { boundary.clone() } else { None };
        let observation = ScanObservation {
            prefix: request.prefix().to_vec(),
            after: after.map(<[u8]>::to_vec),
            through,
            witness: digest_u64(hasher),
        };
        let size = observation.prefix.len()
            + observation.after.as_ref().map_or(0, Vec::len)
            + observation.through.as_ref().map_or(0, Vec::len)
            + ENTRY_BYTES;
        self.reads.reserve(size, 1)?;
        self.reads.scans.push(observation);
        let continuation = if has_more {
            Some(ScanContinuation {
                scope: self.store.scope.clone(),
                prefix: request.prefix().to_vec(),
                origin: ScanContinuationOrigin::Transaction {
                    nonce: self.nonce,
                    base: token.clone(),
                },
                exclusive_last_key: boundary
                    .ok_or_else(|| invariant_violation("transaction scan made no progress"))?,
                query_binding: None,
            })
        } else {
            None
        };
        Ok(ScanPage {
            entries,
            continuation,
            observed_token: token,
        })
    }
}

#[async_trait]
impl ArcoStateTxn for ControlMvpTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        if let Some(write) = self.writes.get(key) {
            let value = match write {
                StagedWrite::Put(value) => Some(VersionedValue::new(value.clone(), None)),
                StagedWrite::Delete => None,
            };
            self.reads.overlay(key)?;
            return Ok(value);
        }
        Ok(self
            .base_value(key)
            .await?
            .filter(|v| !v.tombstone)
            .map(|v| VersionedValue::new(v.bytes, Some(v.generation))))
    }
    async fn scan(&mut self, request: ScanRequest) -> Result<ScanPage> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(base) = &self.eager_base {
            let mut entries = base
                .state
                .scan_prefix(request.prefix())
                .into_iter()
                .map(|entry| (entry.key().to_vec(), entry.value().clone()))
                .collect::<BTreeMap<_, _>>();
            for (key, write) in &self.writes {
                if key.starts_with(request.prefix()) {
                    match write {
                        StagedWrite::Put(value) => {
                            entries.insert(key.clone(), VersionedValue::new(value.clone(), None));
                        }
                        StagedWrite::Delete => {
                            entries.remove(key);
                        }
                    }
                }
            }
            return build_scan_page(
                &self.store.scope,
                request,
                self.base.token(&self.store),
                entries.into_iter().map(|(k, v)| KvPair::new(k, v)),
            );
        }
        self.scan_lazy(request).await
    }
    async fn put(&mut self, key: &[u8], value: Bytes) -> Result<()> {
        self.stage_write(key, StagedWrite::Put(value))
    }
    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.stage_write(key, StagedWrite::Delete)
    }
    async fn assert_absent(&mut self, key: &[u8]) -> Result<()> {
        let witness = point_witness(self.base_value(key).await?.as_ref());
        if matches!(witness, PointWitness::Present(_)) {
            return Err(precondition_failed(
                "cannot assert absence for a present control MVP key",
            ));
        }
        self.add_precondition(Precondition::Absent {
            key: key.to_vec(),
            witness,
        })
    }
    async fn assert_generation(&mut self, key: &[u8], generation: u64) -> Result<()> {
        if point_witness(self.base_value(key).await?.as_ref()) != PointWitness::Present(generation)
        {
            return Err(precondition_failed(
                "cannot assert a control MVP key generation that is not currently present",
            ));
        }
        self.add_precondition(Precondition::Generation {
            key: key.to_vec(),
            expected: generation,
        })
    }
    async fn assert_range_empty(&mut self, range: KeyRange) -> Result<()> {
        let (witness, present) = self.range_evidence(&range).await?;
        if present {
            return Err(precondition_failed(
                "cannot assert a non-empty control MVP range",
            ));
        }
        self.add_precondition(Precondition::RangeEmpty { range, witness })
    }
    async fn assert_range_unchanged(
        &mut self,
        range: KeyRange,
        observed_generation: u64,
    ) -> Result<()> {
        if self.range_witness(&range).await? != observed_generation {
            return Err(precondition_failed(
                "cannot assert a stale control MVP range witness",
            ));
        }
        self.add_precondition(Precondition::RangeUnchanged {
            range,
            witness: observed_generation,
        })
    }
    async fn read_set(
        &mut self,
        keys: &[Vec<u8>],
        ranges: &[KeyRange],
    ) -> Result<PredicateInputSet> {
        let witness = self.predicate_witness(keys, ranges).await?;
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
            .predicate_witness(inputs.point_keys(), inputs.ranges())
            .await?
            != witness
        {
            return Err(precondition_failed(
                "cannot assert stale control MVP predicate inputs",
            ));
        }
        self.add_precondition(Precondition::Predicate { inputs, witness })
    }
    async fn commit(self: Box<Self>) -> Result<CommitOutcome> {
        Box::pin((*self).commit_inner()).await
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests;
