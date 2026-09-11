//! Durable-maintenance planning and execution. Authority, continuation and
//! maintenance wire versions are deliberately independent.
use super::{
    Arc, AuthorityWritePrecondition, BTreeMap, BTreeSet, BlockScanBudget, Bytes,
    CONTROL_MVP_FORMAT_VERSION, CatalogError, ChronoDuration, ControlMvpBlock,
    ControlMvpGcCandidate, ControlMvpGcPlan, ControlMvpMaintenanceOutcome,
    ControlMvpMaintenanceWorker, ControlMvpManifest, ControlMvpPointer, ControlMvpScopeDoc,
    ControlMvpSegmentIndex, ControlMvpSegmentLevel, ControlMvpSegmentRef, ControlMvpSegmentRow,
    ControlMvpStateRef, ControlMvpStateStore, DateTime, Deserialize, Digest, DistributedLock,
    HistoryAnchor, IMPLEMENTATION, KeyRange, MAX_BLOCK_BYTES, MAX_CONTROL_JSON_BYTES,
    MAX_HEAD_JSON_BYTES, MAX_SCAN_ARROW_BYTES, MAX_SEGMENT_BYTES, MAX_SEGMENT_ROWS,
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL,
    RenderedControlMvpStateSegment, ReplayState, Result, RetainedAuthorityRoots,
    RetentionMutationEpoch, RewriteEquivalence, SEGMENT_FORMAT_VERSION, SEGMENT_RECORD_KV,
    SEGMENT_RECORD_OUTBOX, ScopedStorage, Serialize, Sha256, StateScope, Ulid, Utc, WriteResult,
    ambiguous_authority_outcome, block_key_bounds, cost, decode_json, decode_json_limited,
    decode_segment_rows, encode_envelope_limited, encode_json, encode_json_limited, encode_segment,
    half_segment_limits, hash_bytes, hash_tag, hash_u64, integrity, invariant_violation,
    layout_maintenance_intent_for_manifest, lazy, precondition_failed, put_immutable_matching,
    segment_row_key_bounds_hex, sha256_hex, sort_segment_rows, state_segment_reference,
    valid_raw_digest, validate_raw_checksum, validation_failed,
};

const MAINTENANCE_VERSION: u32 = 1;
const MAINTENANCE_POLICY_VERSION: u32 = 1;
const MAX_UNITS: usize = 256;
const MAX_PLAN_PAGE_BYTES: usize = 64 * 1024;
const MAX_PLAN_BYTES: usize = 8 * 1024 * 1024;

/// Stable identity assigned by the trusted durable-location composition layer.
///
/// It must change with provider location, root or authority instance, and remain
/// stable across replicas, credentials and restarts. It has no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableAuthorityBinding([u8; 32]);

impl DurableAuthorityBinding {
    /// Binds an independently assigned identity. Never derive this value from a
    /// persisted job or a process-local state-store binding.
    #[must_use]
    pub const fn new(identity: [u8; 32]) -> Self {
        Self(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxLocator {
    id: String,
    origin: u64,
    ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Unit {
    Kv {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        ordinal: u64,
    },
    Outbox {
        records: Vec<OutboxLocator>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedBlock {
    source: ControlMvpSegmentRef,
    block: ControlMvpBlock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanPage {
    version: u32,
    seed: String,
    ordinal: usize,
    unit: Unit,
    selected: Vec<SelectedBlock>,
    logical_digest: String,
    rows: usize,
    output: ControlMvpStateRef,
}

struct PreparedPlan {
    pages: Vec<Bytes>,
    hashes: Vec<String>,
    selected_bytes: u64,
    source_bytes: u64,
    sequence: u64,
}

fn maintenance_capacity(message: &str) -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: message.into(),
    }
}

fn unit_digest(rows: &[ControlMvpSegmentRow]) -> String {
    #[cfg(feature = "test-utils")]
    cost::record(10, 1);
    let mut hash = Sha256::new();
    hash_bytes(&mut hash, b"arco/control-v1/maintenance-unit-v1");
    hash_u64(&mut hash, rows.len() as u64);
    for row in rows {
        hash_tag(&mut hash, row.record_kind);
        hash_bytes(&mut hash, &row.key);
        hash_tag(&mut hash, u8::from(row.value.is_some()));
        if let Some(value) = &row.value {
            hash_bytes(&mut hash, value);
        }
        hash_u64(&mut hash, row.generation);
        hash_tag(&mut hash, u8::from(row.tombstone));
        hash_u64(&mut hash, row.logical_sequence);
        hash_u64(&mut hash, row.logical_ordinal);
        hash_tag(&mut hash, u8::from(row.origin_sequence.is_some()));
        if let Some(origin) = row.origin_sequence {
            hash_u64(&mut hash, origin);
        }
    }
    hex::encode(hash.finalize())
}

impl Unit {
    fn from_rows(rows: &[ControlMvpSegmentRow]) -> Result<Self> {
        if rows
            .first()
            .is_none_or(|r| r.record_kind == SEGMENT_RECORD_KV)
        {
            let end = rows.last().map(|row| {
                let mut end = row.key.clone();
                end.push(0);
                end
            });
            return Ok(Self::Kv {
                start: rows.first().map(|r| r.key.clone()),
                end,
                ordinal: rows.first().map_or(0, |r| r.logical_ordinal),
            });
        }
        let records = rows
            .iter()
            .map(|r| {
                Ok(OutboxLocator {
                    id: String::from_utf8(r.key.clone())
                        .map_err(|e| invariant_violation(e.to_string()))?,
                    origin: r
                        .origin_sequence
                        .ok_or_else(|| invariant_violation("outbox source incarnation missing"))?,
                    ordinal: r.logical_ordinal,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Outbox { records })
    }

    fn selects(&self, source: &ControlMvpSegmentRef, block: &ControlMvpBlock) -> Result<bool> {
        let Some((minimum, maximum)) = block_key_bounds(block)? else {
            return Ok(false);
        };
        Ok(match self {
            Self::Kv {
                start: Some(start),
                end: Some(end),
                ..
            } => {
                block.record_kind == Some(SEGMENT_RECORD_KV) && maximum >= *start && minimum < *end
            }
            Self::Kv { .. } => false,
            Self::Outbox { records } => {
                block.record_kind == Some(SEGMENT_RECORD_OUTBOX)
                    && records.iter().any(|r| {
                        (source.level == ControlMvpSegmentLevel::L1
                            || source.logical_sequence == r.origin)
                            && r.id.as_bytes() >= minimum.as_slice()
                            && r.id.as_bytes() <= maximum.as_slice()
                    })
            }
        })
    }
}

impl PreparedPlan {
    async fn build(
        store: &ControlMvpStateStore,
        source: &ControlMvpManifest,
        seed: &str,
    ) -> Result<Self> {
        let state = cost::phase(
            "maintenance-preflight-reconstruction",
            store.replay_for_successor(source),
        )
        .await?;
        let (sources, source_bytes) = cost::phase("maintenance-source-metadata", async {
            let mut sources = Vec::new();
            let mut source_bytes = source
                .owning_states()
                .map(|r| r.segment_size_bytes)
                .sum::<u64>();
            for reference in &source.base_states {
                let reference = state_segment_reference(reference);
                let (_, index) = store.load_segment_index(&reference).await?;
                sources.push((reference, index));
            }
            for tx in &source.tx_refs {
                let tx = store.load_tx_metadata(tx).await?;
                source_bytes = source_bytes
                    .checked_add(tx.l0_segment.segment_size_bytes)
                    .ok_or_else(|| maintenance_capacity("source byte overflow"))?;
                let (_, index) = store.load_segment_index(&tx.l0_segment).await?;
                sources.push((tx.l0_segment, index));
            }
            Ok::<_, CatalogError>((sources, source_bytes))
        })
        .await?;
        let _selection = cost::PhaseGuard::enter("maintenance-selection");
        let mut plan = Self {
            pages: Vec::new(),
            hashes: Vec::new(),
            selected_bytes: 0,
            source_bytes,
            sequence: source.logical_sequence,
        };
        let mut candidate = Vec::new();
        let row_limit = store
            .l1_test_rows
            .unwrap_or(MAX_SEGMENT_ROWS / 2)
            .min(MAX_SEGMENT_ROWS / 2);
        let mut candidate_bytes = 0_usize;
        for (ordinal, (key, value)) in state.kv.iter().enumerate() {
            candidate_bytes = candidate_bytes
                .saturating_add(key.len())
                .saturating_add(value.bytes.len())
                .saturating_add(128);
            candidate.push(ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_KV,
                key: key.clone(),
                value: (!value.tombstone).then(|| value.bytes.to_vec()),
                generation: value.generation,
                tombstone: value.tombstone,
                logical_sequence: state.logical_sequence,
                logical_ordinal: ordinal as u64,
                origin_sequence: None,
            });
            if candidate.len() >= row_limit || candidate_bytes >= MAX_SEGMENT_BYTES / 2 {
                plan.add_chunk(store, seed, &sources, &mut candidate)?;
                candidate.clear();
                candidate_bytes = 0;
            }
        }
        if !candidate.is_empty() {
            plan.add_chunk(store, seed, &sources, &mut candidate)?;
            candidate.clear();
        }
        candidate_bytes = 0;
        for (ordinal, record) in state.outbox.iter().enumerate() {
            candidate_bytes = candidate_bytes
                .saturating_add(record.record_id.len())
                .saturating_add(record.payload.len())
                .saturating_add(128);
            candidate.push(ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_OUTBOX,
                key: record.record_id.as_bytes().to_vec(),
                value: Some(record.payload.to_vec()),
                generation: 0,
                tombstone: false,
                logical_sequence: state.logical_sequence,
                logical_ordinal: ordinal as u64,
                origin_sequence: record.origin_sequence,
            });
            if candidate.len() >= row_limit || candidate_bytes >= MAX_SEGMENT_BYTES / 2 {
                plan.add_chunk(store, seed, &sources, &mut candidate)?;
                candidate.clear();
                candidate_bytes = 0;
            }
        }
        if !candidate.is_empty() || plan.pages.is_empty() {
            plan.add_chunk(store, seed, &sources, &mut candidate)?;
        }
        if plan.selected_bytes > plan.source_bytes.saturating_mul(2) {
            return Err(maintenance_capacity(
                "planned construction exceeds two source reads",
            ));
        }
        Ok(plan)
    }

    fn add_chunk(
        &mut self,
        store: &ControlMvpStateStore,
        seed: &str,
        sources: &[(ControlMvpSegmentRef, ControlMvpSegmentIndex)],
        rows: &mut [ControlMvpSegmentRow],
    ) -> Result<()> {
        let _phase = cost::PhaseGuard::enter("maintenance-preflight-sizing");
        let unit = Unit::from_rows(rows)?;
        let selected = sources
            .iter()
            .flat_map(|(source, index)| index.blocks.iter().map(move |block| (source, block)))
            .filter_map(|(source, block)| match unit.selects(source, block) {
                Ok(true) => Some(Ok(SelectedBlock {
                    source: source.clone(),
                    block: block.clone(),
                })),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>>>()?;
        let selected_bytes = selected.iter().map(|b| b.block.length).sum::<u64>();
        let fits_input = selected.len() <= 64
            && selected
                .iter()
                .map(|b| &b.source.segment_id)
                .collect::<BTreeSet<_>>()
                .len()
                <= 64
            && selected_bytes <= MAX_SCAN_ARROW_BYTES as u64;
        sort_segment_rows(rows);
        let sequence = self.sequence;
        let rendered = render_unit(store, seed, self.pages.len(), sequence, rows);
        let encoded = if fits_input {
            rendered.and_then(|output| {
                let plan = PlanPage {
                    version: MAINTENANCE_VERSION,
                    seed: seed.into(),
                    ordinal: self.pages.len(),
                    unit,
                    selected,
                    logical_digest: unit_digest(rows),
                    rows: rows.len(),
                    output: output.reference,
                };
                encode_json_limited(&plan, MAX_PLAN_PAGE_BYTES, "maintenance plan page")
            })
        } else {
            Err(maintenance_capacity("unit selected-input budget exceeded"))
        };
        match encoded {
            Ok(page) => {
                if self.pages.len() >= MAX_UNITS
                    || self
                        .pages
                        .iter()
                        .map(Bytes::len)
                        .sum::<usize>()
                        .saturating_add(page.len())
                        > MAX_PLAN_BYTES
                {
                    return Err(maintenance_capacity(
                        "maintenance plan exceeds job capacity",
                    ));
                }
                self.selected_bytes = self
                    .selected_bytes
                    .checked_add(selected_bytes)
                    .ok_or_else(|| maintenance_capacity("selected byte overflow"))?;
                self.hashes.push(sha256_hex(&page));
                self.pages.push(page);
                Ok(())
            }
            Err(CatalogError::MaintenanceBackpressure { .. }) if rows.len() > 1 => {
                // Outbox split points are logical ordinals, never physical ID order.
                rows.sort_by_key(|r| r.logical_ordinal);
                let middle = rows.len() / 2;
                let (left, right) = rows.split_at_mut(middle);
                self.add_chunk(store, seed, sources, left)?;
                self.add_chunk(store, seed, sources, right)
            }
            Err(error) => Err(error),
        }
    }
}

fn render_unit(
    store: &ControlMvpStateStore,
    seed: &str,
    ordinal: usize,
    sequence: u64,
    rows: &[ControlMvpSegmentRow],
) -> Result<RenderedControlMvpStateSegment> {
    let _phase = cost::PhaseGuard::enter("maintenance-output-rendering-validation");
    #[cfg(feature = "test-utils")]
    cost::record(19, rows.len());
    let id = format!("maintenance-{seed}-{ordinal:03}");
    let limits = if rows.len() == 1 {
        store.segment_limits
    } else {
        half_segment_limits(store.segment_limits)
    };
    let (bytes, index_bytes, reference) = encode_segment(
        &id,
        ControlMvpSegmentLevel::L1,
        sequence,
        &store.scope,
        rows,
        limits,
    )?;
    if decode_segment_rows(&bytes, &index_bytes, &reference, &store.scope)? != rows {
        return Err(invariant_violation(
            "maintenance normal render validation mismatch",
        ));
    }
    let (min_key_hex, max_key_hex) = segment_row_key_bounds_hex(rows);
    Ok(RenderedControlMvpStateSegment {
        reference: ControlMvpStateRef {
            state_id: id,
            logical_sequence: sequence,
            segment_size_bytes: reference.segment_size_bytes,
            index_size_bytes: reference.index_size_bytes,
            checksum_sha256: reference.checksum_sha256,
            index_checksum_sha256: reference.index_checksum_sha256,
            min_key_hex,
            max_key_hex,
        },
        bytes,
        index_bytes,
    })
}

impl PlanPage {
    #[allow(clippy::too_many_lines)] // Keep the ordered validation boundary together.
    async fn construct(
        &self,
        store: &ControlMvpStateStore,
        source: &ControlMvpManifest,
    ) -> Result<RenderedControlMvpStateSegment> {
        cost::phase("maintenance-selection", async {
            if self.version != MAINTENANCE_VERSION
                || self.ordinal >= MAX_UNITS
                || self.selected.len() > 64
                || self.rows > MAX_SEGMENT_ROWS
            {
                return Err(invariant_violation(
                    "unsupported or oversized maintenance unit",
                ));
            }
            let mut rows = match &self.unit {
                Unit::Kv {
                    start: None,
                    end: None,
                    ..
                } if self.rows == 0 => Vec::new(),
                Unit::Kv {
                    start: Some(start),
                    end: Some(end),
                    ordinal,
                } => {
                    if start >= end {
                        return Err(invariant_violation("invalid maintenance key range"));
                    }
                    let range = KeyRange::new(start.clone(), end.clone());
                    let mut merge = cost::phase(
                        "maintenance-source-metadata",
                        lazy::ResolvedRows::new(store, Some(source), b"", None, Some(&range)),
                    )
                    .await?;
                    let mut budget = BlockScanBudget {
                        blocks: 64,
                        segments: 64,
                        bytes: MAX_SCAN_ARROW_BYTES,
                    };
                    let mut rows = Vec::new();
                    loop {
                        if !merge.fill(store, &mut budget).await? {
                            return Err(maintenance_capacity(
                                "maintenance selected KV input exceeded admission",
                            ));
                        }
                        let Some(key) = merge.key().map(<[u8]>::to_vec) else {
                            break;
                        };
                        let value = merge.take(&key).ok_or_else(|| {
                            invariant_violation("selected maintenance row absent")
                        })?;
                        let logical_ordinal = ordinal
                            .checked_add(rows.len() as u64)
                            .ok_or_else(|| invariant_violation("maintenance ordinal overflow"))?;
                        rows.push(ControlMvpSegmentRow {
                            record_kind: SEGMENT_RECORD_KV,
                            key,
                            value: (!value.tombstone).then(|| value.bytes.to_vec()),
                            generation: value.generation,
                            tombstone: value.tombstone,
                            logical_sequence: source.logical_sequence,
                            logical_ordinal,
                            origin_sequence: None,
                        });
                        if rows.len() > self.rows {
                            return Err(invariant_violation(
                                "maintenance range contains excess rows",
                            ));
                        }
                    }
                    rows
                }
                Unit::Kv { .. } => {
                    return Err(invariant_violation("invalid empty maintenance key range"));
                }
                Unit::Outbox { records } => {
                    if records.len() != self.rows {
                        return Err(invariant_violation("outbox locator count mismatch"));
                    }
                    let locators: BTreeMap<_, _> = records
                        .iter()
                        .map(|record| ((record.id.as_bytes(), record.origin), record))
                        .collect();
                    if locators.len() != records.len() {
                        return Err(invariant_violation("duplicate maintenance outbox locator"));
                    }
                    let mut selected = BTreeMap::new();
                    let mut bytes = 0_u64;
                    let mut seen = BTreeSet::new();
                    let mut owners = BTreeSet::new();
                    for locator in &self.selected {
                        bytes = bytes
                            .checked_add(locator.block.length)
                            .ok_or_else(|| maintenance_capacity("outbox input overflow"))?;
                        owners.insert(&locator.source.segment_id);
                        if bytes > MAX_SCAN_ARROW_BYTES as u64
                            || owners.len() > 64
                            || !seen.insert((&locator.source.segment_id, locator.block.offset))
                        {
                            return Err(invariant_violation(
                                "invalid selected outbox block budget",
                            ));
                        }
                        let (_, index) = store.load_segment_index(&locator.source).await?;
                        if !index.blocks.contains(&locator.block) {
                            return Err(invariant_violation(
                                "outbox locator is not in authenticated directory",
                            ));
                        }
                        for mut row in store.load_block(&locator.source, &locator.block).await? {
                            if row.record_kind != SEGMENT_RECORD_OUTBOX {
                                return Err(invariant_violation(
                                    "outbox unit selected a different row kind",
                                ));
                            }
                            if let Some(record) = row
                                .origin_sequence
                                .and_then(|origin| locators.get(&(row.key.as_slice(), origin)))
                            {
                                row.logical_sequence = source.logical_sequence;
                                row.logical_ordinal = record.ordinal;
                                if selected.insert(record.ordinal, row).is_some() {
                                    return Err(invariant_violation(
                                        "duplicate selected outbox incarnation",
                                    ));
                                }
                            }
                        }
                    }
                    selected.into_values().collect()
                }
            };
            sort_segment_rows(&mut rows);
            if rows.len() != self.rows || unit_digest(&rows) != self.logical_digest {
                return Err(invariant_violation(
                    "constructed maintenance logical unit mismatch",
                ));
            }
            let rendered = render_unit(
                store,
                &self.seed,
                self.ordinal,
                source.logical_sequence,
                &rows,
            )?;
            if rendered.reference != self.output {
                return Err(invariant_violation(
                    "constructed maintenance output differs from admitted bytes",
                ));
            }
            Ok(rendered)
        })
        .await
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests;

/// Content-addressed immutable maintenance descriptor identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceJobId(String);

impl MaintenanceJobId {
    /// Parses an identity supplied independently of stored job bytes.
    ///
    /// # Errors
    /// Rejects noncanonical SHA-256 identities.
    pub fn parse(identity: impl Into<String>) -> Result<Self> {
        let identity = identity.into();
        if !valid_raw_digest(&identity) {
            return Err(validation_failed("invalid maintenance job identity"));
        }
        Ok(Self(identity))
    }
    /// Returns the exact descriptor digest.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    version: u32,
    policy_version: u32,
    authority_version: u32,
    segment_version: u32,
    directory_version: u32,
    encoder_version: u32,
    scope: ControlMvpScopeDoc,
    binding: DurableAuthorityBinding,
    source_id: String,
    source_digest: String,
    source_sequence: u64,
    source_history: String,
    source_physical: String,
    source_checksum: String,
    layout_generation: u64,
    reclamation_generation: u64,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    nonce: String,
    seed: String,
    block_target: usize,
    pages: Vec<String>,
}

impl Descriptor {
    fn validate(&self, scope: &StateScope, binding: DurableAuthorityBinding) -> Result<()> {
        if self.version != MAINTENANCE_VERSION
            || self.policy_version != MAINTENANCE_POLICY_VERSION
            || self.authority_version != CONTROL_MVP_FORMAT_VERSION
            || self.segment_version != SEGMENT_FORMAT_VERSION
            || self.directory_version != SEGMENT_FORMAT_VERSION
            || self.encoder_version != 1
            || !self.scope.matches_scope(scope)
            || self.binding != binding
        {
            return Err(validation_failed(
                "maintenance authority binding or format mismatch",
            ));
        }
        if self.pages.is_empty()
            || self.pages.len() > MAX_UNITS
            || self.layout_generation == u64::MAX
            || !integrity::valid_immutable_id(&self.source_id)
            || [
                &self.source_digest,
                &self.source_history,
                &self.source_physical,
                &self.source_checksum,
                &self.seed,
            ]
            .into_iter()
            .chain(&self.pages)
            .any(|digest| !valid_raw_digest(digest))
            || self.expires_at
                != self
                    .created_at
                    .checked_add_signed(ChronoDuration::hours(24))
                    .ok_or_else(|| invariant_violation("maintenance expiry overflow"))?
            || self.retained_until
                != self
                    .created_at
                    .checked_add_signed(ChronoDuration::days(8))
                    .ok_or_else(|| invariant_violation("maintenance retention overflow"))?
            || Ulid::from_string(&self.nonce).is_err()
            || !(8 * 1024..=MAX_BLOCK_BYTES).contains(&self.block_target)
        {
            return Err(invariant_violation("invalid maintenance descriptor"));
        }
        if self.seed != self.render_seed()? {
            return Err(invariant_violation(
                "maintenance render seed is not scope bound",
            ));
        }
        Ok(())
    }

    #[allow(clippy::suspicious_operation_groupings)] // Render-source names deliberately differ from manifest names.
    fn validate_source(&self, source: &ControlMvpManifest) -> Result<()> {
        if source.manifest_id != self.source_id
            || source.scope != self.scope
            || source.logical_sequence != self.source_sequence
            || source.history_root != self.source_history
            || source.physical_root != self.source_physical
            || source.state_checksum_sha256 != self.source_checksum
            || source.layout_generation != self.layout_generation
        {
            return Err(invariant_violation(
                "maintenance render source differs from descriptor",
            ));
        }
        Ok(())
    }

    fn live(&self, now: DateTime<Utc>) -> Result<()> {
        if now < self.created_at || now.max(cost::now()) >= self.expires_at {
            return Err(precondition_failed(
                "maintenance execution lifetime expired or clock precedes creation",
            ));
        }
        Ok(())
    }

    fn pin_id(&self) -> String {
        format!("pin_{}", self.nonce)
    }
}

/// Durable state selected by a maintenance progress CAS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MaintenanceStatus {
    /// An inactive descriptor and plan have been admitted.
    Planned,
    /// Retention protection is active and construction may advance.
    Active,
    /// Every output has an authenticated selected receipt.
    ReadyToPublish,
    /// A publication attempt may have reached HEAD.
    Publishing,
    /// Exact publication evidence confirms selection.
    Published,
    /// Source compatibility was consumed by a different publication.
    Superseded,
    /// The job was abandoned before any unresolved publication.
    Abandoned,
    /// A terminal failure was proven.
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    ordinal: usize,
    plan_digest: String,
    logical_digest: String,
    rows: usize,
    output: ControlMvpStateRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)] // The wire field names the monotonic revision ordinal.
struct Revision {
    version: u32,
    job: String,
    revision: u32,
    completed: usize,
    status: MaintenanceStatus,
    predecessor: Option<String>,
    receipt: Option<Receipt>,
    attempt: Option<String>,
    submissions: u8,
    submission_nonce: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selector {
    version: u32,
    job: String,
    revision: u32,
    digest: String,
}

struct LoadedJob {
    descriptor: Descriptor,
    pages: Vec<PlanPage>,
    revisions: Vec<(String, Revision)>,
    selector_version: String,
    attempts: BTreeMap<String, PublicationAttempt>,
}

fn job_prefix(store: &ControlMvpStateStore, id: &str) -> String {
    format!("{}/maintenance/{id}", store.paths.base_prefix())
}
fn descriptor_path(store: &ControlMvpStateStore, id: &str) -> String {
    format!("{}/descriptor.json", job_prefix(store, id))
}
fn page_path(store: &ControlMvpStateStore, id: &str, ordinal: usize, digest: &str) -> String {
    format!("{}/plans/{ordinal:03}-{digest}.json", job_prefix(store, id))
}
fn revision_path(store: &ControlMvpStateStore, id: &str, digest: &str) -> String {
    format!("{}/revisions/{digest}.json", job_prefix(store, id))
}
fn selector_path(store: &ControlMvpStateStore, id: &str) -> String {
    format!("{}/selected.json", job_prefix(store, id))
}

async fn load_descriptor(
    store: &ControlMvpStateStore,
    id: &MaintenanceJobId,
    binding: DurableAuthorityBinding,
) -> Result<Descriptor> {
    let bytes = store
        .get_json(&descriptor_path(store, id.as_str()), MAX_PLAN_PAGE_BYTES)
        .await?;
    validate_raw_checksum(&bytes, Some(id.as_str()), "maintenance descriptor identity")?;
    let descriptor: Descriptor =
        decode_json_limited(&bytes, MAX_PLAN_PAGE_BYTES, "maintenance descriptor")?;
    descriptor.validate(&store.scope, binding)?;
    Ok(descriptor)
}

async fn load_pages(
    store: &ControlMvpStateStore,
    id: &MaintenanceJobId,
    descriptor: &Descriptor,
) -> Result<Vec<PlanPage>> {
    cost::phase("maintenance-plan-authentication", async {
        let mut pages: Vec<PlanPage> = Vec::new();
        let mut bytes_total = 0_usize;
        let mut kv_ordinal = 0_u64;
        let mut outbox_ordinal = 0_u64;
        let mut key_end: Option<Vec<u8>> = None;
        let mut keyless = false;
        for (ordinal, digest) in descriptor.pages.iter().enumerate() {
            let bytes = store
                .get_json(
                    &page_path(store, id.as_str(), ordinal, digest),
                    MAX_PLAN_PAGE_BYTES,
                )
                .await?;
            bytes_total = bytes_total
                .checked_add(bytes.len())
                .ok_or_else(|| maintenance_capacity("plan byte overflow"))?;
            if bytes_total > MAX_PLAN_BYTES {
                return Err(maintenance_capacity("maintenance plan aggregate limit"));
            }
            validate_raw_checksum(&bytes, Some(digest), "maintenance plan page")?;
            let page: PlanPage = decode_json(&bytes, "maintenance plan page")?;
            if page.version != MAINTENANCE_VERSION
                || page.ordinal != ordinal
                || page.seed != descriptor.seed
                || page.output.state_id != format!("maintenance-{}-{ordinal:03}", descriptor.seed)
                || page.output.logical_sequence != descriptor.source_sequence
                || page.rows > MAX_SEGMENT_ROWS
                || page.selected.len() > 64
                || page
                    .selected
                    .iter()
                    .try_fold(0_u64, |n, s| n.checked_add(s.block.length))
                    .is_none_or(|n| n > MAX_SCAN_ARROW_BYTES as u64)
                || !valid_raw_digest(&page.logical_digest)
            {
                return Err(invariant_violation(
                    "invalid maintenance page binding or budget",
                ));
            }
            integrity::validate_state_refs(std::slice::from_ref(&page.output))?;
            match &page.unit {
                Unit::Kv {
                    start: None,
                    end: None,
                    ordinal: 0,
                } if descriptor.pages.len() == 1 && page.rows == 0 => {}
                Unit::Kv {
                    start: Some(start),
                    end: Some(end),
                    ordinal,
                } if !keyless
                    && start < end
                    && *ordinal == kv_ordinal
                    && key_end.as_ref().is_none_or(|prior| prior <= start) =>
                {
                    kv_ordinal = kv_ordinal
                        .checked_add(page.rows as u64)
                        .ok_or_else(|| invariant_violation("KV plan ordinal overflow"))?;
                    key_end = Some(end.clone());
                }
                Unit::Outbox { records } if !records.is_empty() && records.len() == page.rows => {
                    keyless = true;
                    for record in records {
                        if record.ordinal != outbox_ordinal
                            || record.origin == 0
                            || record.origin > descriptor.source_sequence
                        {
                            return Err(invariant_violation(
                                "discontinuous maintenance outbox slice",
                            ));
                        }
                        outbox_ordinal = outbox_ordinal
                            .checked_add(1)
                            .ok_or_else(|| invariant_violation("outbox plan ordinal overflow"))?;
                    }
                }
                _ => return Err(invariant_violation("unordered maintenance shard plan")),
            }
            pages.push(page);
        }
        Ok(pages)
    })
    .await
}

impl LoadedJob {
    #[allow(clippy::too_many_lines)] // Keep the ordered validation boundary together.
    async fn load(
        store: &ControlMvpStateStore,
        id: &MaintenanceJobId,
        binding: DurableAuthorityBinding,
    ) -> Result<Self> {
        cost::phase("maintenance-progress-authentication", async {
            let descriptor = load_descriptor(store, id, binding).await?;
            let pages = load_pages(store, id, &descriptor).await?;
            let selector_meta = store
                .storage
                .head(&selector_path(store, id.as_str()))
                .await?
                .ok_or_else(|| precondition_failed("maintenance job is not active"))?;
            let bytes = store
                .get_json(&selector_path(store, id.as_str()), 8 * 1024)
                .await?;
            let selector: Selector = decode_json(&bytes, "maintenance selector")?;
            if selector.version != MAINTENANCE_VERSION
                || selector.job != id.as_str()
                || selector.revision >= 320
                || !valid_raw_digest(&selector.digest)
            {
                return Err(invariant_violation("invalid maintenance selector"));
            }
            let mut revisions = Vec::new();
            let mut next = Some(selector.digest);
            for expected in (0..=selector.revision).rev() {
                let digest = next
                    .take()
                    .ok_or_else(|| invariant_violation("missing progress predecessor"))?;
                let bytes = store
                    .get_json(&revision_path(store, id.as_str(), &digest), 8 * 1024)
                    .await?;
                validate_raw_checksum(&bytes, Some(&digest), "maintenance revision")?;
                let revision: Revision = decode_json(&bytes, "maintenance revision")?;
                if revision.version != MAINTENANCE_VERSION
                    || revision.job != id.as_str()
                    || revision.revision != expected
                    || revision.completed > pages.len()
                    || revision
                        .predecessor
                        .as_ref()
                        .is_some_and(|d| !valid_raw_digest(d))
                {
                    return Err(invariant_violation("invalid maintenance progress revision"));
                }
                next.clone_from(&revision.predecessor);
                revisions.push((digest, revision));
            }
            if next.is_some() {
                return Err(invariant_violation("excess progress predecessors"));
            }
            revisions.reverse();
            let mut completed = 0;
            let mut attempts = BTreeMap::new();
            for (index, (_, revision)) in revisions.iter().enumerate() {
                if let Some((_, previous)) = index.checked_sub(1).and_then(|i| revisions.get(i)) {
                    validate_progress_transition(previous, revision, pages.len())?;
                }
                if index == 0
                    && (revision.status != MaintenanceStatus::Active
                        || revision.receipt.is_some()
                        || revision.completed != 0
                        || revision.attempt.is_some()
                        || revision.submissions != 0
                        || revision.submission_nonce.is_some())
                {
                    return Err(invariant_violation("invalid maintenance activation"));
                }
                if let Some(digest) = &revision.attempt {
                    if !attempts.contains_key(digest) {
                        if revision.status != MaintenanceStatus::Publishing || attempts.len() >= 16
                        {
                            return Err(invariant_violation(
                                "unselected or excessive maintenance attempts",
                            ));
                        }
                        let attempt = read_attempt(store, id, &descriptor, digest).await?;
                        if attempt.ordinal != attempts.len() {
                            return Err(invariant_violation(
                                "discontinuous maintenance attempt ordinal",
                            ));
                        }
                        attempts.insert(digest.clone(), attempt);
                    }
                }
                if let Some(receipt) = &revision.receipt {
                    let page = pages
                        .get(completed)
                        .ok_or_else(|| invariant_violation("repeated maintenance receipt"))?;
                    if receipt.ordinal != completed
                        || descriptor.pages.get(completed) != Some(&receipt.plan_digest)
                        || receipt.logical_digest != page.logical_digest
                        || receipt.rows != page.rows
                        || receipt.output != page.output
                    {
                        return Err(invariant_violation("maintenance receipt differs from plan"));
                    }
                    for (path, length) in [
                        (
                            store.paths.state_object(&receipt.output.state_id),
                            receipt.output.segment_size_bytes,
                        ),
                        (
                            store.paths.segment_index(&receipt.output.state_id),
                            receipt.output.index_size_bytes,
                        ),
                    ] {
                        let meta = store.storage.head(&path).await?.ok_or_else(|| {
                            invariant_violation("completed maintenance object missing")
                        })?;
                        if meta.size != length {
                            return Err(invariant_violation(
                                "completed maintenance object length mismatch",
                            ));
                        }
                    }
                    completed += 1;
                }
                if revision.completed != completed {
                    return Err(invariant_violation(
                        "nonmonotonic maintenance completion count",
                    ));
                }
            }
            Ok(Self {
                descriptor,
                pages,
                revisions,
                selector_version: selector_meta.version,
                attempts,
            })
        })
        .await
    }
}

fn expected_pin(
    descriptor: &Descriptor,
    id: &str,
) -> Result<crate::workspace_snapshot::RetentionPinRevision> {
    crate::workspace_snapshot::RetentionPinRevision::new(
        descriptor.pin_id(),
        1,
        crate::workspace_snapshot::RetentionTarget::Maintenance(format!(
            "{}/{id}",
            descriptor.scope.domain
        )),
        descriptor.created_at,
        descriptor.retained_until,
        None,
    )
}

/// GC-only interpretation of a maintenance pin. A persisted binding is checked
/// here for structural consistency; it never grants an execution capability or
/// public retained-cut authority. Workers separately require configured binding.
pub async fn retention_root(
    storage: &ScopedStorage,
    target: &str,
    selected: &crate::gc::reachability::SelectedRetentionPin,
) -> Result<crate::gc::reachability::RetainedAuthorityRoot> {
    cost::phase("maintenance-GC-root", async {
        use crate::workspace_snapshot::{retention_pin_latest_path, retention_pin_revision_path};
        crate::workspace_snapshot::RetentionTarget::Maintenance(target.into()).validate()?;
        let (domain, id) = target
            .split_once('/')
            .ok_or_else(|| validation_failed("invalid maintenance GC target"))?;
        let id = MaintenanceJobId::parse(id)?;
        // GC has no independently configured worker binding; interpret pins directly.
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new(storage.tenant_id(), storage.workspace_id(), domain),
        )?
        .without_read_cache();
        let bytes = store
            .get_json(&descriptor_path(&store, id.as_str()), MAX_PLAN_PAGE_BYTES)
            .await?;
        validate_raw_checksum(&bytes, Some(id.as_str()), "maintenance GC descriptor")?;
        let descriptor: Descriptor = decode_json(&bytes, "maintenance GC descriptor")?;
        descriptor.validate(&store.scope, descriptor.binding)?;
        let pin = expected_pin(&descriptor, id.as_str())?;
        if selected.initial_revision()? != &pin || selected.latest_revision()? != &pin {
            return Err(invariant_violation(
                "maintenance pin identity or fixed retention differs from job",
            ));
        }
        // Validate every selected version, including progress versions unknown to
        // this binary. Receipts are not a substitute for publication byte readback.
        LoadedJob::load(&store, &id, descriptor.binding).await?;
        let source = store
            .load_manifest_with_expected_checksum(
                &descriptor.source_id,
                Some(&descriptor.source_digest),
            )
            .await?;
        descriptor.validate_source(&source)?;
        let worker = ControlMvpMaintenanceWorker {
            store,
            lifecycle: storage.clone(),
        };
        let mut required_paths = BTreeSet::new();
        worker
            .protect_manifest_closure(
                &descriptor.source_id,
                Some(&descriptor.source_digest),
                &mut required_paths,
            )
            .await?;
        for ordinal in 0..descriptor.pages.len() {
            let name = format!("maintenance-{}-{ordinal:03}", descriptor.seed);
            required_paths.insert(worker.store.paths.state_object(&name));
            required_paths.insert(worker.store.paths.segment_index(&name));
        }
        for ordinal in 0..16 {
            required_paths.insert(worker.store.paths.manifest_object(&format!(
                "maintenance-{}-publication-{ordinal:02}",
                descriptor.seed
            )));
        }
        required_paths.insert(retention_pin_latest_path(&descriptor.pin_id())?);
        required_paths.insert(retention_pin_revision_path(&descriptor.pin_id(), 1)?);
        Ok(crate::gc::reachability::RetainedAuthorityRoot {
            authorities: Vec::new(),
            required_paths,
            protected_prefixes: vec![format!("{}/", job_prefix(&worker.store, id.as_str()))],
        })
    })
    .await
}

/// An admitted, read-only maintenance plan. Its identity is available before any
/// durable write, so the caller can persist it for crash recovery.
pub struct PreparedMaintenance {
    id: MaintenanceJobId,
    descriptor: Descriptor,
    bytes: Bytes,
    pages: Vec<Bytes>,
}

impl PreparedMaintenance {
    /// Exact identity to retain before starting the job.
    #[must_use]
    pub fn job_id(&self) -> &MaintenanceJobId {
        &self.id
    }
}

/// Explicit durable maintenance capability. Trusted composition must provide
/// the same location binding on every replica and after restart.
pub struct DurableMaintenanceWorker {
    worker: ControlMvpMaintenanceWorker,
    binding: DurableAuthorityBinding,
}

/// Authenticated selected progress. Publication status still requires exact
/// attempt reconciliation before it can yield a publication outcome.
#[derive(Debug, Clone)]
pub struct MaintenanceProgress {
    /// Descriptor identity supplied to later invocations.
    pub job: MaintenanceJobId,
    /// Selected durable state.
    pub status: MaintenanceStatus,
    /// Contiguous completed output units.
    pub completed: usize,
    /// Admitted output units.
    pub total: usize,
}

impl LoadedJob {
    fn last(&self) -> Result<&(String, Revision)> {
        self.revisions
            .last()
            .ok_or_else(|| invariant_violation("missing selected progress"))
    }
    fn progress(&self, id: &MaintenanceJobId) -> Result<MaintenanceProgress> {
        let last = &self.last()?.1;
        Ok(MaintenanceProgress {
            job: id.clone(),
            status: last.status,
            completed: last.completed,
            total: self.pages.len(),
        })
    }
}

impl Descriptor {
    fn render_seed(&self) -> Result<String> {
        Ok(sha256_hex(&encode_json(
            &(
                "arco/control-v1/maintenance-render-seed-v1",
                &self.scope,
                self.binding,
                &self.source_id,
                &self.source_digest,
                self.reclamation_generation,
                self.layout_generation,
                self.created_at,
                &self.nonce,
                self.block_target,
                self.policy_version,
                self.encoder_version,
            ),
            "maintenance render seed",
        )?))
    }
}

impl DurableMaintenanceWorker {
    /// Creates a worker with independently configured durable location identity.
    ///
    /// # Errors
    /// Rejects invalid or mismatched storage scopes.
    pub fn new(
        storage: ScopedStorage,
        scope: StateScope,
        binding: DurableAuthorityBinding,
    ) -> Result<Self> {
        let mut worker = ControlMvpMaintenanceWorker::new(storage, scope)?;
        worker.store.cache_namespace = Some(binding);
        worker.store = worker
            .store
            .with_read_cache_config(super::ControlMvpReadCacheConfig::default())?;
        Ok(Self { worker, binding })
    }

    /// Configures an empty cache for authenticated maintenance reads.
    ///
    /// # Errors
    /// Rejects nonzero capacities that cannot fund handle administration.
    pub fn with_read_cache_config(
        mut self,
        config: super::ControlMvpReadCacheConfig,
    ) -> Result<Self> {
        self.worker.store = self.worker.store.with_read_cache_config(config)?;
        Ok(self)
    }

    /// Uses direct reads while retaining all durable authority checks.
    #[must_use]
    pub fn without_read_cache(mut self) -> Self {
        self.worker.store = self.worker.store.without_read_cache();
        self
    }

    /// Returns the shared read cache for statistics and compatible store reuse.
    #[must_use]
    pub fn read_cache(&self) -> Option<super::ControlMvpReadCache> {
        self.worker.store.read_cache()
    }

    /// Configures deterministic local fixture sizing.
    ///
    /// # Errors
    /// Rejects sizes outside existing reader and writer limits.
    #[cfg(feature = "test-utils")]
    pub fn with_test_segment_sizing(mut self, rows: usize, target: usize) -> Result<Self> {
        self.worker = self.worker.with_test_segment_sizing(rows, target)?;
        Ok(self)
    }

    async fn compatible(
        &self,
        descriptor: &Descriptor,
    ) -> Result<(
        String,
        ControlMvpPointer,
        ControlMvpManifest,
        ControlMvpManifest,
    )> {
        cost::phase("maintenance-ancestry-compatibility", async {
            let store = &self.worker.store;
            let before = store
                .storage
                .head(&store.paths.current_pointer())
                .await?
                .ok_or_else(|| invariant_violation("maintenance HEAD missing"))?;
            let pointer = store.load_pointer().await?;
            let current = store.load_manifest_for_pointer(&pointer).await?;
            let source = store
                .load_manifest_with_expected_checksum(
                    &descriptor.source_id,
                    Some(&descriptor.source_digest),
                )
                .await?;
            descriptor.validate_source(&source)?;
            if pointer.reclamation_generation != descriptor.reclamation_generation
                || current.layout_generation != descriptor.layout_generation
                || current.base_states != source.base_states
                || current.anchor_states != source.anchor_states
                || !current.tx_refs.starts_with(&source.tx_refs)
                || current
                    .tx_refs
                    .iter()
                    .skip(source.tx_refs.len())
                    .any(|tx| tx.tx_id.starts_with("tx-restore-"))
            {
                return Err(precondition_failed(
                    "maintenance source ownership or reclamation generation was consumed",
                ));
            }
            let found = store
                .resolve_ancestor_bounded(
                    &current.manifest_id,
                    &pointer.manifest_checksum_sha256,
                    |manifest, digest| {
                        (manifest.manifest_id == source.manifest_id
                            && digest == descriptor.source_digest)
                            .then_some(())
                    },
                    32,
                    64 * 1024 * 1024,
                )
                .await?;
            if found.is_none() {
                return Err(precondition_failed(
                    "maintenance source is not an authenticated ancestor",
                ));
            }
            let after = store
                .storage
                .head(&store.paths.current_pointer())
                .await?
                .ok_or_else(|| invariant_violation("maintenance HEAD disappeared"))?;
            if before.version != after.version {
                return Err(CatalogError::CasFailed {
                    message: "maintenance HEAD changed during compatibility validation".into(),
                });
            }
            Ok((before.version, pointer, current, source))
        })
        .await
    }

    /// Performs full source preflight without writing durable objects.
    /// Persist the returned job identity before calling `start_at`; every later
    /// interruption can then be addressed by that exact identity.
    /// Returns no plan when ordinary admission has no pending maintenance intent.
    ///
    /// # Errors
    /// Returns typed admission, storage, integrity, coordination or fencing errors.
    pub async fn prepare_at(&self, now: DateTime<Utc>) -> Result<Option<PreparedMaintenance>> {
        self.prepare_inner(now, false).await
    }

    /// Forced admission for comparison with the separately preserved eager reference.
    ///
    /// # Errors
    /// Returns the same preflight and admission failures as normal preparation.
    #[doc(hidden)]
    #[cfg(feature = "test-utils")]
    pub async fn test_prepare_forced_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<PreparedMaintenance>> {
        self.prepare_inner(now, true).await
    }

    async fn prepare_inner(
        &self,
        now: DateTime<Utc>,
        force: bool,
    ) -> Result<Option<PreparedMaintenance>> {
        cost::phase("maintenance-start-preflight", async {
            let store = &self.worker.store;
            if store
                .storage
                .head(&store.paths.current_pointer())
                .await?
                .is_none()
            {
                return Ok(None);
            }
            let pointer = store.load_pointer().await?;
            let source = store.load_manifest_for_pointer(&pointer).await?;
            if source.maintenance_intent.is_none() && !force {
                return Ok(None);
            }
            let mut descriptor = Descriptor {
                version: MAINTENANCE_VERSION,
                policy_version: MAINTENANCE_POLICY_VERSION,
                authority_version: CONTROL_MVP_FORMAT_VERSION,
                segment_version: SEGMENT_FORMAT_VERSION,
                directory_version: SEGMENT_FORMAT_VERSION,
                encoder_version: 1,
                scope: (&store.scope).into(),
                binding: self.binding,
                source_id: source.manifest_id.clone(),
                source_digest: pointer.manifest_checksum_sha256,
                source_sequence: source.logical_sequence,
                source_history: source.history_root.clone(),
                source_physical: source.physical_root.clone(),
                source_checksum: source.state_checksum_sha256.clone(),
                layout_generation: source.layout_generation,
                reclamation_generation: pointer.reclamation_generation,
                created_at: now,
                expires_at: now
                    .checked_add_signed(ChronoDuration::hours(24))
                    .ok_or_else(|| invariant_violation("expiry overflow"))?,
                retained_until: now
                    .checked_add_signed(ChronoDuration::days(8))
                    .ok_or_else(|| invariant_violation("retention overflow"))?,
                nonce: cost::nonce().to_string(),
                seed: String::new(),
                block_target: store.segment_limits.block_target,
                pages: Vec::new(),
            };
            descriptor.seed = descriptor.render_seed()?;
            let plan = PreparedPlan::build(store, &source, &descriptor.seed).await?;
            descriptor.pages.clone_from(&plan.hashes);
            descriptor.validate(&store.scope, self.binding)?;
            let bytes =
                encode_json_limited(&descriptor, MAX_PLAN_PAGE_BYTES, "maintenance descriptor")?;
            let id = MaintenanceJobId::parse(sha256_hex(&bytes))?;
            // All admission serialization is checked while prepare remains read-only.
            activation_bytes(&descriptor, &id)?;
            Ok(Some(PreparedMaintenance {
                id,
                descriptor,
                bytes,
                pages: plan.pages,
            }))
        })
        .await
    }

    /// Publishes and activates a prepared job whose identity the caller already knows.
    /// Retry interrupted activation with `recover_activation_at` and that identity.
    ///
    /// # Errors
    /// Rejects expired, incompatible or differently bound plans and unresolved writes.
    pub async fn start_at(
        &self,
        plan: &PreparedMaintenance,
        now: DateTime<Utc>,
    ) -> Result<MaintenanceProgress> {
        cost::phase("maintenance-start-pinning", async {
            let store = &self.worker.store;
            let descriptor = &plan.descriptor;
            let id = &plan.id;
            descriptor.validate(&store.scope, self.binding)?;
            descriptor.live(now.max(cost::now()))?;
            let activation = activation_bytes(descriptor, id)?;
            self.compatible(descriptor).await?;
            immutable_reconciled(
                store,
                &descriptor_path(store, id.as_str()),
                plan.bytes.clone(),
            )
            .await?;
            for (ordinal, bytes) in plan.pages.iter().enumerate() {
                let digest = descriptor
                    .pages
                    .get(ordinal)
                    .ok_or_else(|| invariant_violation("missing admitted page digest"))?;
                immutable_reconciled(
                    store,
                    &page_path(store, id.as_str(), ordinal, digest),
                    bytes.clone(),
                )
                .await?;
            }
            descriptor.live(now.max(cost::now()))?;
            Box::pin(self.activate(id, descriptor, activation, now)).await?;
            LoadedJob::load(store, id, self.binding).await?.progress(id)
        })
        .await
    }

    /// Whole-start fixture convenience. Production callers persist the identity
    /// returned by `prepare_at` before invoking `start_at`.
    ///
    /// # Errors
    /// Returns preparation or activation failures.
    #[doc(hidden)]
    #[cfg(all(test, feature = "test-utils"))]
    async fn test_start_at(&self, now: DateTime<Utc>) -> Result<Option<MaintenanceJobId>> {
        let Some(plan) = self.prepare_at(now).await? else {
            return Ok(None);
        };
        self.start_at(&plan, now).await?;
        Ok(Some(plan.id))
    }

    #[allow(clippy::too_many_lines)] // Keep admission, exact repair and epoch settlement together.
    async fn activate(
        &self,
        id: &MaintenanceJobId,
        descriptor: &Descriptor,
        bytes: (Bytes, Bytes, Bytes, Bytes),
        now: DateTime<Utc>,
    ) -> Result<()> {
        use crate::workspace_snapshot::{retention_pin_latest_path, retention_pin_revision_path};
        let storage = &self.worker.lifecycle;
        let store = &self.worker.store;
        load_pages(store, id, descriptor).await?;
        let mut guard = DistributedLock::new(Arc::new(storage.clone()), RETENTION_GC_LOCK_PATH)
            .acquire_with_operation(
                RETENTION_GC_LOCK_TTL,
                RETENTION_GC_LOCK_MAX_RETRIES,
                Some(format!("maintenance-root:{}", id.as_str())),
            )
            .await
            .map_err(CatalogError::from)?;
        // Authenticate still-durable evidence after waiting for coordination,
        // before a fresh epoch could manufacture its own recovery authority.
        let admission = async {
            let protected = load_descriptor(store, id, self.binding).await?;
            load_pages(store, id, &protected).await?;
            let claimed = RetentionMutationEpoch::maintenance_root_submitted_before(
                storage,
                id.as_str(),
                descriptor.created_at,
                descriptor.expires_at,
            )
            .await?;
            let pin_path = retention_pin_revision_path(&descriptor.pin_id(), 1)?;
            let published = if storage.head_raw(&pin_path).await?.is_some() {
                if storage.get_raw(&pin_path).await? != bytes.0 {
                    return Err(invariant_violation(
                        "maintenance activation pin conflicts with descriptor",
                    ));
                }
                true
            } else {
                false
            };
            let submitted = claimed || published;
            if !submitted {
                descriptor.live(now)?;
                self.compatible(descriptor).await?;
                descriptor.live(now)?;
            }
            Ok::<_, CatalogError>(submitted)
        }
        .await;
        let submitted = match admission {
            Ok(submitted) => submitted,
            Err(error) => {
                let _ = guard.release().await;
                return Err(error);
            }
        };
        let mut epoch = match RetentionMutationEpoch::claim_maintenance_root(
            storage.clone(),
            &mut guard,
            id.as_str(),
            (!submitted).then_some((now, descriptor.expires_at)),
        )
        .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                let _ = guard.release().await;
                return Err(error);
            }
        };
        let result = Box::pin(async {
            // GC may collect an expired job while recovery waits for this lock.
            // Both reads authenticate the independently supplied job identity;
            // validate the entire still-durable plan before publishing any root.
            let protected_descriptor = load_descriptor(store, id, self.binding).await?;
            load_pages(store, id, &protected_descriptor).await?;
            if submitted {
                // Exact recovery repairs the immutable root even if the current
                // layout consumed this job. Final compatibility is still checked.
                let source = store
                    .load_manifest_with_expected_checksum(
                        &descriptor.source_id,
                        Some(&descriptor.source_digest),
                    )
                    .await?;
                descriptor.validate_source(&source)?;
            } else {
                descriptor.live(now)?;
                self.compatible(descriptor).await?;
            }
            if !submitted {
                descriptor.live(now)?;
            }
            epoch
                .put_immutable_reconciled(
                    &retention_pin_revision_path(&descriptor.pin_id(), 1)?,
                    bytes.0,
                )
                .await?;
            epoch
                .put_immutable_reconciled(
                    &retention_pin_latest_path(&descriptor.pin_id())?,
                    bytes.1,
                )
                .await?;
            let digest = sha256_hex(&bytes.2);
            epoch
                .put_immutable_reconciled(&revision_path(store, id.as_str(), &digest), bytes.2)
                .await?;
            if store
                .storage
                .head(&selector_path(store, id.as_str()))
                .await?
                .is_some()
            {
                LoadedJob::load(store, id, self.binding).await?;
            } else {
                epoch
                    .put_immutable_reconciled(&selector_path(store, id.as_str()), bytes.3)
                    .await?;
            }
            self.compatible(descriptor).await?;
            Ok(())
        })
        .await;
        let settled = epoch.settle().await;
        let released = guard.release().await.map_err(CatalogError::from);
        if let Err(error) = settled {
            return Err(ambiguous_authority_outcome(format!(
                "maintenance root epoch settlement failed: {error}; activation: {result:?}"
            )));
        }
        released?;
        result
    }

    /// Authenticates selected metadata without reconstructing source data.
    ///
    /// # Errors
    /// Rejects expired, copied, missing, corrupt or incompatible job evidence.
    pub async fn resume_at(
        &self,
        id: &MaintenanceJobId,
        now: DateTime<Utc>,
    ) -> Result<MaintenanceProgress> {
        cost::phase("maintenance-resume", async {
            let job = LoadedJob::load(&self.worker.store, id, self.binding).await?;
            job.descriptor.live(now)?;
            self.verify_pin(&job.descriptor, id, now).await?;
            if matches!(
                job.last()?.1.status,
                MaintenanceStatus::Publishing | MaintenanceStatus::Published
            ) {
                let attempt = Self::load_attempt(&job)?;
                self.observe_attempt(&attempt).await?;
            } else {
                self.compatible(&job.descriptor).await?;
            }
            job.progress(id)
        })
        .await
    }

    async fn verify_pin(
        &self,
        descriptor: &Descriptor,
        id: &MaintenanceJobId,
        now: DateTime<Utc>,
    ) -> Result<()> {
        use crate::workspace_snapshot::{retention_pin_latest_path, retention_pin_revision_path};
        use arco_core::storage_traits::ReadStore as _;
        let pin = expected_pin(descriptor, id.as_str())?;
        if pin.status_at(now)? != crate::workspace_snapshot::RetentionStatus::Active {
            return Err(precondition_failed(
                "maintenance source protection is unavailable",
            ));
        }
        let (revision, selector, _, _) = activation_bytes(descriptor, id)?;
        for (path, bytes) in [
            (
                retention_pin_revision_path(&descriptor.pin_id(), 1)?,
                revision,
            ),
            (retention_pin_latest_path(&descriptor.pin_id())?, selector),
        ] {
            let stored = self
                .worker
                .lifecycle
                .get_range(&path, 0..(bytes.len() as u64).saturating_add(1))
                .await?;
            if stored != bytes {
                return Err(invariant_violation(
                    "maintenance pin differs from admitted immutable bytes",
                ));
            }
        }
        Ok(())
    }
}

fn activation_bytes(
    descriptor: &Descriptor,
    id: &MaintenanceJobId,
) -> Result<(Bytes, Bytes, Bytes, Bytes)> {
    use crate::workspace_snapshot::{
        RetentionPinLatest, encode_retention_pin_latest, encode_retention_pin_revision,
        retention_pin_revision_path,
    };
    let pin = expected_pin(descriptor, id.as_str())?;
    let pin_bytes = Bytes::from(encode_retention_pin_revision(&pin)?);
    let pin_selector = RetentionPinLatest::new(
        descriptor.pin_id(),
        1,
        retention_pin_revision_path(&descriptor.pin_id(), 1)?,
        format!("sha256:{}", sha256_hex(&pin_bytes)),
    )?;
    let pin_selector = Bytes::from(encode_retention_pin_latest(&pin_selector)?);
    let initial = Revision {
        version: MAINTENANCE_VERSION,
        job: id.as_str().into(),
        revision: 0,
        completed: 0,
        status: MaintenanceStatus::Active,
        predecessor: None,
        receipt: None,
        attempt: None,
        submissions: 0,
        submission_nonce: None,
    };
    let initial = encode_json_limited(&initial, 8 * 1024, "maintenance activation")?;
    let selector = Selector {
        version: MAINTENANCE_VERSION,
        job: id.as_str().into(),
        revision: 0,
        digest: sha256_hex(&initial),
    };
    let selector = encode_json_limited(&selector, 8 * 1024, "maintenance activation selector")?;
    Ok((pin_bytes, pin_selector, initial, selector))
}

async fn immutable_reconciled(
    store: &ControlMvpStateStore,
    path: &str,
    bytes: Bytes,
) -> Result<()> {
    cost::phase("maintenance-immutable-write", async {
        match put_immutable_matching(
            &store.storage,
            path,
            bytes.clone(),
            "conflicting maintenance immutable bytes",
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(error) => match store
                .storage
                .get_range(path, 0..(bytes.len() as u64).saturating_add(1))
                .await
            {
                Ok(visible) if visible == bytes => Ok(()),
                Ok(_) => Err(precondition_failed(
                    "maintenance immutable winner has conflicting bytes",
                )),
                Err(_) => Err(ambiguous_authority_outcome(format!(
                    "maintenance immutable write could not be reconciled: {error}"
                ))),
            },
        }
    })
    .await
}

impl DurableMaintenanceWorker {
    /// Constructs and selects at most one output shard and its receipt.
    ///
    /// # Errors
    /// Returns source, budget, immutable reconciliation or progress CAS failures.
    pub async fn advance_at(
        &self,
        id: &MaintenanceJobId,
        now: DateTime<Utc>,
    ) -> Result<MaintenanceProgress> {
        Box::pin(cost::phase("maintenance-advance", async {
            let job = LoadedJob::load(&self.worker.store, id, self.binding).await?;
            job.descriptor.live(now)?;
            self.verify_pin(&job.descriptor, id, now).await?;
            let (_, _, _, source) = self.compatible(&job.descriptor).await?;
            let (predecessor, last) = job.last()?;
            if last.status == MaintenanceStatus::ReadyToPublish {
                return job.progress(id);
            }
            if last.status != MaintenanceStatus::Active {
                return Err(precondition_failed("maintenance job is not constructible"));
            }
            let page = job
                .pages
                .get(last.completed)
                .ok_or_else(|| invariant_violation("maintenance next unit absent"))?;
            let revision = Revision {
                version: MAINTENANCE_VERSION,
                job: id.as_str().into(),
                revision: last
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| maintenance_capacity("maintenance revision overflow"))?,
                completed: last.completed + 1,
                status: if last.completed + 1 == job.pages.len() {
                    MaintenanceStatus::ReadyToPublish
                } else {
                    MaintenanceStatus::Active
                },
                predecessor: Some(predecessor.clone()),
                receipt: Some(Receipt {
                    ordinal: last.completed,
                    plan_digest: job
                        .descriptor
                        .pages
                        .get(last.completed)
                        .ok_or_else(|| invariant_violation("unit digest missing"))?
                        .clone(),
                    logical_digest: page.logical_digest.clone(),
                    rows: page.rows,
                    output: page.output.clone(),
                }),
                attempt: None,
                submissions: last.submissions,
                submission_nonce: None,
            };
            let (bytes, selector) = revision_bytes(&revision)?;
            let mut render_store = self.worker.store.clone();
            render_store.segment_limits.block_target = job.descriptor.block_target;
            let rendered = cost::phase(
                "maintenance-construction",
                page.construct(&render_store, &source),
            )
            .await?;
            job.descriptor.live(now.max(cost::now()))?;
            immutable_reconciled(
                &render_store,
                &render_store
                    .paths
                    .state_object(&rendered.reference.state_id),
                rendered.bytes,
            )
            .await?;
            job.descriptor.live(now.max(cost::now()))?;
            immutable_reconciled(
                &render_store,
                &render_store
                    .paths
                    .segment_index(&rendered.reference.state_id),
                rendered.index_bytes,
            )
            .await?;
            job.descriptor.live(now.max(cost::now()))?;
            self.compatible(&job.descriptor).await?;
            self.select_revision(
                id,
                &job.selector_version,
                bytes,
                selector,
                Some(job.descriptor.expires_at),
            )
            .await?;
            Ok(MaintenanceProgress {
                job: id.clone(),
                status: revision.status,
                completed: revision.completed,
                total: job.pages.len(),
            })
        }))
        .await
    }

    async fn select_revision(
        &self,
        id: &MaintenanceJobId,
        expected: &str,
        revision: Bytes,
        selector: Bytes,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let check_deadline = || {
            if deadline.is_some_and(|expiry| cost::now() >= expiry) {
                Err(precondition_failed(
                    "maintenance progress execution deadline passed",
                ))
            } else {
                Ok(())
            }
        };
        check_deadline()?;
        let store = &self.worker.store;
        immutable_reconciled(
            store,
            &revision_path(store, id.as_str(), &sha256_hex(&revision)),
            revision,
        )
        .await?;
        let path = selector_path(store, id.as_str());
        check_deadline()?;
        let result = cost::phase(
            "maintenance-progress-CAS",
            store.storage.put(
                &path,
                selector.clone(),
                AuthorityWritePrecondition::MatchesVersion(expected.into()),
            ),
        )
        .await;
        match result {
            Ok(WriteResult::Success { .. }) => Ok(()),
            result => {
                if store
                    .storage
                    .get_range(&path, 0..(selector.len() as u64).saturating_add(1))
                    .await
                    .is_ok_and(|visible| visible == selector)
                {
                    return Ok(());
                }
                match result {
                    Ok(_) => Err(CatalogError::CasFailed {
                        message: "maintenance progress CAS lost".into(),
                    }),
                    Err(error) => Err(ambiguous_authority_outcome(format!(
                        "maintenance progress CAS could not be reconciled: {error}"
                    ))),
                }
            }
        }
    }

    /// Records abandonment only when no publication can be in flight.
    /// Fixed retention remains active until the original eight-day deadline.
    ///
    /// # Errors
    /// Rejects expired jobs, unresolved publication and progress CAS conflicts.
    pub async fn abandon_at(
        &self,
        id: &MaintenanceJobId,
        now: DateTime<Utc>,
    ) -> Result<MaintenanceProgress> {
        cost::phase("maintenance-abandonment", async {
            let job = LoadedJob::load(&self.worker.store, id, self.binding).await?;
            job.descriptor.live(now)?;
            self.verify_pin(&job.descriptor, id, now).await?;
            let (predecessor, last) = job.last()?;
            if last.status == MaintenanceStatus::Abandoned {
                return job.progress(id);
            }
            if !matches!(
                last.status,
                MaintenanceStatus::Active | MaintenanceStatus::ReadyToPublish
            ) {
                return Err(precondition_failed(
                    "maintenance publication is unresolved or terminal",
                ));
            }
            let revision = Revision {
                version: MAINTENANCE_VERSION,
                job: id.as_str().into(),
                revision: last
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| maintenance_capacity("revision overflow"))?,
                completed: last.completed,
                status: MaintenanceStatus::Abandoned,
                predecessor: Some(predecessor.clone()),
                receipt: None,
                attempt: None,
                submissions: last.submissions,
                submission_nonce: None,
            };
            let (bytes, selector) = revision_bytes(&revision)?;
            self.select_revision(
                id,
                &job.selector_version,
                bytes,
                selector,
                Some(job.descriptor.expires_at),
            )
            .await?;
            Ok(MaintenanceProgress {
                job: id.clone(),
                status: revision.status,
                completed: revision.completed,
                total: job.pages.len(),
            })
        })
        .await
    }
}

fn prepare_submission(
    id: &MaintenanceJobId,
    job: &LoadedJob,
    attempt: &str,
) -> Result<(Revision, Bytes, Bytes)> {
    let (predecessor, last) = job.last()?;
    if last.submissions >= 16 || last.revision > 317 {
        return Err(maintenance_capacity(
            "maintenance publication submission limit",
        ));
    }
    let revision = Revision {
        version: MAINTENANCE_VERSION,
        job: id.as_str().into(),
        revision: last.revision + 1,
        completed: last.completed,
        status: MaintenanceStatus::Publishing,
        predecessor: Some(predecessor.clone()),
        receipt: None,
        attempt: Some(attempt.into()),
        submissions: last.submissions + 1,
        submission_nonce: Some(cost::nonce().to_string()),
    };
    let (bytes, selector) = revision_bytes(&revision)?;
    Ok((revision, bytes, selector))
}

fn revision_bytes(revision: &Revision) -> Result<(Bytes, Bytes)> {
    if revision.revision >= 320 {
        return Err(maintenance_capacity("maintenance progress history limit"));
    }
    let bytes = encode_json_limited(revision, 8 * 1024, "maintenance revision")?;
    let selector = Selector {
        version: MAINTENANCE_VERSION,
        job: revision.job.clone(),
        revision: revision.revision,
        digest: sha256_hex(&bytes),
    };
    let selector = encode_json_limited(&selector, 8 * 1024, "maintenance selector")?;
    Ok((bytes, selector))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationAttempt {
    version: u32,
    job: String,
    ordinal: usize,
    source_id: String,
    source_digest: String,
    source_sequence: u64,
    head_version: String,
    writer_epoch: u64,
    reclamation_generation: u64,
    candidate_id: String,
    candidate_digest: String,
    pointer: Vec<u8>,
}

struct PublicationCandidate {
    attempt: PublicationAttempt,
    manifest: Bytes,
}

async fn read_attempt(
    store: &ControlMvpStateStore,
    id: &MaintenanceJobId,
    descriptor: &Descriptor,
    digest: &str,
) -> Result<PublicationAttempt> {
    cost::phase("maintenance-attempt-authentication", async {
        if !valid_raw_digest(digest) {
            return Err(invariant_violation("invalid maintenance attempt reference"));
        }
        let bytes = store
            .get_json(
                &attempt_path(store, id.as_str(), digest),
                MAX_PLAN_PAGE_BYTES,
            )
            .await?;
        validate_raw_checksum(&bytes, Some(digest), "maintenance publication attempt")?;
        let attempt: PublicationAttempt = decode_json(&bytes, "maintenance publication attempt")?;
        if attempt.version != MAINTENANCE_VERSION
            || attempt.job != id.as_str()
            || attempt.ordinal >= 16
            || attempt.candidate_id
                != format!(
                    "maintenance-{}-publication-{:02}",
                    descriptor.seed, attempt.ordinal
                )
            || !valid_raw_digest(&attempt.candidate_digest)
            || !valid_raw_digest(&attempt.source_digest)
            || !integrity::valid_immutable_id(&attempt.source_id)
            || attempt.head_version.is_empty()
            || attempt.reclamation_generation != descriptor.reclamation_generation
        {
            return Err(invariant_violation(
                "invalid maintenance publication attempt identity",
            ));
        }
        let pointer: ControlMvpPointer = decode_json_limited(
            &attempt.pointer,
            MAX_HEAD_JSON_BYTES,
            "maintenance pending HEAD",
        )?;
        pointer.validate(&store.scope)?;
        if pointer.manifest_id != attempt.candidate_id
            || pointer.manifest_checksum_sha256 != attempt.candidate_digest
            || pointer.logical_sequence != attempt.source_sequence
            || pointer.writer_epoch != attempt.writer_epoch
            || pointer.reclamation_generation != attempt.reclamation_generation
        {
            return Err(invariant_violation(
                "maintenance attempt HEAD differs from candidate",
            ));
        }
        Ok(attempt)
    })
    .await
}

enum PublicationObservation {
    Selected,
    Consumed,
    Pending,
}

fn attempt_path(store: &ControlMvpStateStore, id: &str, digest: &str) -> String {
    format!("{}/attempts/{digest}.json", job_prefix(store, id))
}

impl DurableMaintenanceWorker {
    #[allow(clippy::too_many_lines)] // Keep the ordered validation boundary together.
    async fn prepare_publication(
        &self,
        id: &MaintenanceJobId,
        job: &LoadedJob,
        ordinal: usize,
    ) -> Result<PublicationCandidate> {
        if ordinal >= 16 {
            return Err(maintenance_capacity(
                "maintenance publication attempt limit",
            ));
        }
        if job.last()?.1.completed != job.pages.len() {
            return Err(precondition_failed(
                "maintenance output units remain unfinished",
            ));
        }
        let store = &self.worker.store;
        let (head_version, pointer, current, render) = self.compatible(&job.descriptor).await?;
        let expected = cost::phase(
            "maintenance-publish-source-reconstruction",
            store.replay_for_successor(&current),
        )
        .await?;
        let (candidate_state, suffix) =
            cost::phase("maintenance-candidate-reconstruction", async {
                let mut candidate_state = ReplayState {
                    logical_sequence: render.logical_sequence,
                    history_root: render.history_root.clone(),
                    ..ReplayState::default()
                };
                // Read and normally decode each completed object once. Accumulate the
                // candidate reconstruction, not a second collection of rendered objects.
                cost::phase("maintenance-completed-output-reuse", async {
                    for page in &job.pages {
                        let (index, directory) = store
                            .load_segment_index(&state_segment_reference(&page.output))
                            .await?;
                        let snapshot = store
                            .load_state_snapshot_from_index(&page.output, &index, &directory)
                            .await?;
                        candidate_state.append_snapshot(snapshot)?;
                    }
                    Ok::<_, CatalogError>(())
                })
                .await?;
                if candidate_state.checksum()? != render.state_checksum_sha256
                    || candidate_state.logical_sequence != render.logical_sequence
                {
                    return Err(invariant_violation(
                        "completed outputs differ from authenticated render cut",
                    ));
                }
                let suffix = current
                    .tx_refs
                    .get(render.tx_refs.len()..)
                    .ok_or_else(|| invariant_violation("maintenance source prefix missing"))?
                    .to_vec();
                for reference in &suffix {
                    candidate_state.apply_tx(&store.load_tx(reference).await?)?;
                }
                Ok::<_, CatalogError>((candidate_state, suffix))
            })
            .await?;
        {
            let _phase = cost::PhaseGuard::enter("maintenance-final-equivalence");
            if candidate_state != expected
                || candidate_state.checksum()? != current.state_checksum_sha256
            {
                return Err(invariant_violation(
                    "complete maintenance candidate is not semantically equivalent",
                ));
            }
        }
        let _phase = cost::PhaseGuard::enter("maintenance-manifest-HEAD-rendering");
        let candidate_id = format!(
            "maintenance-{}-publication-{ordinal:02}",
            job.descriptor.seed
        );
        let mut candidate = current.clone();
        candidate.manifest_id.clone_from(&candidate_id);
        candidate.base_manifest_id = Some(current.manifest_id.clone());
        candidate.parent_manifest_sha256 = Some(pointer.manifest_checksum_sha256.clone());
        candidate.reclamation_generation = pointer.reclamation_generation;
        candidate.writer_epoch = pointer.writer_epoch;
        candidate.layout_generation = current
            .layout_generation
            .checked_add(1)
            .ok_or_else(|| invariant_violation("maintenance layout generation overflow"))?;
        candidate.base_states = job.pages.iter().map(|page| page.output.clone()).collect();
        candidate.anchor_states.clear();
        candidate.tx_refs = suffix;
        candidate.history_anchor = HistoryAnchor {
            sequence: render.logical_sequence,
            root: render.history_root.clone(),
        };
        // Every render attestation field comes from the job-bound, normally
        // validated raw source. No persisted progress field supplies this cut.
        candidate.equivalence = Some(RewriteEquivalence {
            encoding_version: 2,
            source_manifest_id: current.manifest_id.clone(),
            source_manifest_sha256: pointer.manifest_checksum_sha256.clone(),
            source_history_root: current.history_root.clone(),
            source_physical_root: current.physical_root.clone(),
            logical_sequence: current.logical_sequence,
            state_checksum_sha256: current.state_checksum_sha256.clone(),
            render_source: Some(integrity::RenderSource {
                manifest_id: render.manifest_id.clone(),
                manifest_sha256: job.descriptor.source_digest.clone(),
                logical_sequence: render.logical_sequence,
                history_anchor: render.history_anchor.clone(),
                history_root: render.history_root.clone(),
                physical_root: render.physical_root.clone(),
                state_checksum_sha256: render.state_checksum_sha256.clone(),
                base_states: render.base_states.clone(),
                anchor_states: render.anchor_states.clone(),
                tx_refs: render.tx_refs.clone(),
            }),
        });
        candidate.maintenance_intent = layout_maintenance_intent_for_manifest(
            &store.scope,
            &candidate_id,
            candidate.logical_sequence,
            candidate.layout_generation,
            candidate.tx_refs.len(),
        )?;
        candidate.physical_root = candidate.physical_digest()?;
        candidate.validate(&store.scope, &candidate_id)?;
        let manifest = encode_envelope_limited(
            "control-mvp-manifest",
            &candidate,
            MAX_CONTROL_JSON_BYTES,
            "maintenance publication manifest",
        )?;
        let candidate_digest = sha256_hex(&manifest);
        let candidate_pointer = ControlMvpPointer {
            reclamation_generation: pointer.reclamation_generation,
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.into(),
            scope: (&store.scope).into(),
            manifest_id: candidate_id.clone(),
            logical_sequence: current.logical_sequence,
            manifest_checksum_sha256: candidate_digest.clone(),
            writer_epoch: pointer.writer_epoch,
        };
        let pointer_bytes = encode_json_limited(
            &candidate_pointer,
            MAX_HEAD_JSON_BYTES,
            "maintenance publication HEAD",
        )?;
        let attempt = PublicationAttempt {
            version: MAINTENANCE_VERSION,
            job: id.as_str().into(),
            ordinal,
            source_id: current.manifest_id,
            source_digest: pointer.manifest_checksum_sha256,
            source_sequence: current.logical_sequence,
            head_version,
            writer_epoch: pointer.writer_epoch,
            reclamation_generation: pointer.reclamation_generation,
            candidate_id,
            candidate_digest,
            pointer: pointer_bytes.to_vec(),
        };
        Ok(PublicationCandidate { attempt, manifest })
    }

    fn load_attempt(job: &LoadedJob) -> Result<PublicationAttempt> {
        let digest = job
            .last()?
            .1
            .attempt
            .as_ref()
            .ok_or_else(|| invariant_violation("maintenance publication attempt missing"))?;
        job.attempts
            .get(digest)
            .cloned()
            .ok_or_else(|| invariant_violation("selected attempt was not authenticated"))
    }

    async fn observe_attempt(
        &self,
        attempt: &PublicationAttempt,
    ) -> Result<PublicationObservation> {
        cost::phase("maintenance-publication-reconciliation", async {
            let store = &self.worker.store;
            let before = store
                .storage
                .head(&store.paths.current_pointer())
                .await?
                .ok_or_else(|| ambiguous_authority_outcome("maintenance HEAD unavailable"))?;
            let pointer = store.load_pointer().await?;
            let observation = store
                .resolve_ancestor_bounded(
                    &pointer.manifest_id,
                    &pointer.manifest_checksum_sha256,
                    |manifest, digest| {
                        if manifest.manifest_id == attempt.candidate_id
                            && digest == attempt.candidate_digest
                        {
                            Some(PublicationObservation::Selected)
                        } else if manifest.manifest_id == attempt.source_id
                            && digest == attempt.source_digest
                        {
                            Some(if before.version == attempt.head_version {
                                PublicationObservation::Pending
                            } else {
                                PublicationObservation::Consumed
                            })
                        } else {
                            None
                        }
                    },
                    64,
                    64 * 1024 * 1024,
                )
                .await?
                .ok_or_else(|| {
                    ambiguous_authority_outcome("maintenance publication ancestry unavailable")
                })?;
            let after = store
                .storage
                .head(&store.paths.current_pointer())
                .await?
                .ok_or_else(|| ambiguous_authority_outcome("maintenance HEAD disappeared"))?;
            if before.version != after.version {
                return Err(ambiguous_authority_outcome(
                    "maintenance HEAD changed during reconciliation",
                ));
            }
            Ok(observation)
        })
        .await
    }

    /// Fully validates and submits at most one fenced HEAD CAS. A selected
    /// pending attempt is reconciled or identically resubmitted; completed L1
    /// objects are never rendered or PUT again.
    ///
    /// # Errors
    /// Returns typed validation, capacity, fencing, CAS or ambiguous outcomes.
    pub async fn publish_at(
        &self,
        id: &MaintenanceJobId,
        now: DateTime<Utc>,
    ) -> Result<Option<ControlMvpMaintenanceOutcome>> {
        Box::pin(cost::phase("maintenance-publication", async {
            let mut job = LoadedJob::load(&self.worker.store, id, self.binding).await?;
            let last = &job.last()?.1;
            if matches!(
                last.status,
                MaintenanceStatus::Publishing | MaintenanceStatus::Published
            ) {
                let attempt = Self::load_attempt(&job)?;
                match self.observe_attempt(&attempt).await? {
                    PublicationObservation::Selected => {
                        if last.status == MaintenanceStatus::Publishing {
                            self.finish_publication(id, &job, MaintenanceStatus::Published)
                                .await?;
                        }
                        return Ok(Some(self.publication_outcome(&job, &attempt)));
                    }
                    PublicationObservation::Consumed => {
                        if last.status == MaintenanceStatus::Published {
                            return Err(invariant_violation(
                                "published maintenance evidence disappeared",
                            ));
                        }
                        let status = if now.max(cost::now()) >= job.descriptor.expires_at {
                            MaintenanceStatus::Superseded
                        } else {
                            MaintenanceStatus::ReadyToPublish
                        };
                        self.finish_publication(id, &job, status).await?;
                        return Ok(None);
                    }
                    PublicationObservation::Pending => {
                        if last.status == MaintenanceStatus::Published {
                            return Err(invariant_violation(
                                "published maintenance HEAD is still at its source",
                            ));
                        }
                        job.descriptor.live(now)?;
                        self.verify_pin(&job.descriptor, id, now).await?;
                        let candidate = self.prepare_publication(id, &job, attempt.ordinal).await?;
                        if candidate.attempt != attempt {
                            return Err(ambiguous_authority_outcome(
                                "pending maintenance candidate cannot be identically reconstructed",
                            ));
                        }
                        job.descriptor.live(now.max(cost::now()))?;
                        let digest = job
                            .last()?
                            .1
                            .attempt
                            .clone()
                            .ok_or_else(|| invariant_violation("pending attempt missing"))?;
                        let submission = prepare_submission(id, &job, &digest)?;
                        job = self.begin_submission(id, &job, submission).await?;
                        return self.submit_publication(id, &job, candidate, now).await;
                    }
                }
            }
            if last.status != MaintenanceStatus::ReadyToPublish {
                return Err(precondition_failed(
                    "maintenance job is not ready to publish",
                ));
            }
            job.descriptor.live(now)?;
            self.verify_pin(&job.descriptor, id, now).await?;
            if last.revision > 317 || last.submissions >= 16 {
                return Err(maintenance_capacity(
                    "publication requires two progress revisions",
                ));
            }
            let ordinal = job.attempts.len();
            let candidate = self.prepare_publication(id, &job, ordinal).await?;
            let attempt_bytes = encode_json_limited(
                &candidate.attempt,
                MAX_PLAN_PAGE_BYTES,
                "maintenance publication attempt",
            )?;
            let attempt_digest = sha256_hex(&attempt_bytes);
            let submission = prepare_submission(id, &job, &attempt_digest)?;
            // Admission is complete before any immutable publication artifact PUT.
            job.descriptor.live(now.max(cost::now()))?;
            immutable_reconciled(
                &self.worker.store,
                &attempt_path(&self.worker.store, id.as_str(), &attempt_digest),
                attempt_bytes,
            )
            .await?;
            job = self.begin_submission(id, &job, submission).await?;
            self.submit_publication(id, &job, candidate, now).await
        }))
        .await
    }

    async fn begin_submission(
        &self,
        id: &MaintenanceJobId,
        job: &LoadedJob,
        prepared: (Revision, Bytes, Bytes),
    ) -> Result<LoadedJob> {
        let (revision, bytes, selector) = prepared;
        self.select_revision(
            id,
            &job.selector_version,
            bytes,
            selector,
            Some(job.descriptor.expires_at),
        )
        .await?;
        let selected = LoadedJob::load(&self.worker.store, id, self.binding).await?;
        if selected.last()?.1.submission_nonce != revision.submission_nonce {
            return Err(ambiguous_authority_outcome(
                "publication submission grant changed",
            ));
        }
        Ok(selected)
    }

    async fn submit_publication(
        &self,
        id: &MaintenanceJobId,
        job: &LoadedJob,
        candidate: PublicationCandidate,
        now: DateTime<Utc>,
    ) -> Result<Option<ControlMvpMaintenanceOutcome>> {
        job.descriptor.live(now.max(cost::now()))?;
        let store = &self.worker.store;
        immutable_reconciled(
            store,
            &store.paths.manifest_object(&candidate.attempt.candidate_id),
            candidate.manifest,
        )
        .await?;
        job.descriptor.live(now.max(cost::now()))?;
        let result = cost::phase(
            "maintenance-HEAD-CAS",
            store.storage.put(
                &store.paths.current_pointer(),
                Bytes::from(candidate.attempt.pointer.clone()),
                AuthorityWritePrecondition::MatchesVersion(candidate.attempt.head_version.clone()),
            ),
        )
        .await;
        match result {
            Ok(WriteResult::Success { .. }) => {
                self.finish_publication(id, job, MaintenanceStatus::Published)
                    .await?;
                Ok(Some(self.publication_outcome(job, &candidate.attempt)))
            }
            result => match self.observe_attempt(&candidate.attempt).await {
                Ok(PublicationObservation::Selected) => {
                    self.finish_publication(id, job, MaintenanceStatus::Published)
                        .await?;
                    Ok(Some(self.publication_outcome(job, &candidate.attempt)))
                }
                Ok(PublicationObservation::Consumed) => {
                    let status = if now.max(cost::now()) >= job.descriptor.expires_at {
                        MaintenanceStatus::Superseded
                    } else {
                        MaintenanceStatus::ReadyToPublish
                    };
                    self.finish_publication(id, job, status).await?;
                    Ok(None)
                }
                _ => Err(ambiguous_authority_outcome(format!(
                    "maintenance HEAD CAS is unresolved: {result:?}"
                ))),
            },
        }
    }

    async fn finish_publication(
        &self,
        id: &MaintenanceJobId,
        job: &LoadedJob,
        status: MaintenanceStatus,
    ) -> Result<()> {
        let (digest, last) = job.last()?;
        if last.status != MaintenanceStatus::Publishing {
            return Err(invariant_violation("maintenance attempt is not selected"));
        }
        let revision = Revision {
            version: MAINTENANCE_VERSION,
            job: id.as_str().into(),
            revision: last
                .revision
                .checked_add(1)
                .ok_or_else(|| maintenance_capacity("revision overflow"))?,
            completed: last.completed,
            status,
            predecessor: Some(digest.clone()),
            receipt: None,
            attempt: last.attempt.clone(),
            submissions: last.submissions,
            submission_nonce: None,
        };
        let (bytes, selector) = revision_bytes(&revision)?;
        self.select_revision(id, &job.selector_version, bytes, selector, None)
            .await
    }

    fn publication_outcome(
        &self,
        job: &LoadedJob,
        attempt: &PublicationAttempt,
    ) -> ControlMvpMaintenanceOutcome {
        ControlMvpMaintenanceOutcome {
            source_token: self
                .worker
                .store
                .token(attempt.source_id.clone(), attempt.source_sequence)
                .with_manifest_witness(attempt.source_digest.clone()),
            selected_token: self
                .worker
                .store
                .token(attempt.candidate_id.clone(), attempt.source_sequence)
                .with_manifest_witness(attempt.candidate_digest.clone()),
            layout_generation: job.descriptor.layout_generation + 1,
        }
    }
}

fn validate_progress_transition(
    previous: &Revision,
    current: &Revision,
    total: usize,
) -> Result<()> {
    let submission_valid = if current.status == MaintenanceStatus::Publishing {
        current.submissions <= 16
            && current.submissions == previous.submissions.saturating_add(1)
            && current
                .submission_nonce
                .as_ref()
                .is_some_and(|nonce| Ulid::from_string(nonce).is_ok())
            && current.submission_nonce != previous.submission_nonce
    } else {
        current.submissions == previous.submissions && current.submission_nonce.is_none()
    };
    if !submission_valid {
        return Err(invariant_violation(
            "invalid maintenance submission reservation",
        ));
    }
    let valid = if current.receipt.is_some() {
        previous.status == MaintenanceStatus::Active
            && current.completed == previous.completed + 1
            && current.attempt.is_none()
            && current.status
                == if current.completed == total {
                    MaintenanceStatus::ReadyToPublish
                } else {
                    MaintenanceStatus::Active
                }
    } else if current.completed != previous.completed {
        false
    } else {
        match (previous.status, current.status) {
            (
                MaintenanceStatus::Active | MaintenanceStatus::ReadyToPublish,
                MaintenanceStatus::Abandoned
                | MaintenanceStatus::Failed
                | MaintenanceStatus::Superseded,
            ) => current.attempt.is_none(),
            (MaintenanceStatus::Publishing, MaintenanceStatus::Publishing) => {
                current.completed == total
                    && current.attempt.is_some()
                    && current.attempt == previous.attempt
            }
            (MaintenanceStatus::ReadyToPublish, MaintenanceStatus::Publishing) => {
                current.completed == total
                    && current
                        .attempt
                        .as_ref()
                        .is_some_and(|id| valid_raw_digest(id))
                    && current.attempt != previous.attempt
            }
            (
                MaintenanceStatus::Publishing,
                MaintenanceStatus::Published
                | MaintenanceStatus::ReadyToPublish
                | MaintenanceStatus::Superseded,
            ) => current.attempt.is_some() && current.attempt == previous.attempt,
            _ => false,
        }
    };
    if !valid {
        return Err(invariant_violation(
            "invalid maintenance progress state transition",
        ));
    }
    Ok(())
}

impl DurableMaintenanceWorker {
    /// Replays an interrupted activation using its independently supplied job ID.
    /// The original pin deadline is preserved. No output construction is performed.
    ///
    /// # Errors
    /// Rejects mismatched bindings, incompatible sources and unresolved foreign epochs.
    pub async fn recover_activation_at(
        &self,
        id: &MaintenanceJobId,
        now: DateTime<Utc>,
    ) -> Result<MaintenanceProgress> {
        cost::phase("maintenance-root-recovery", async {
            let descriptor = load_descriptor(&self.worker.store, id, self.binding).await?;
            let bytes = activation_bytes(&descriptor, id)?;
            Box::pin(self.activate(id, &descriptor, bytes, now)).await?;
            let job = LoadedJob::load(&self.worker.store, id, self.binding).await?;
            if now.max(cost::now()) < descriptor.expires_at {
                self.verify_pin(&descriptor, id, now).await?;
            }
            job.progress(id)
        })
        .await
    }
}

impl DurableMaintenanceWorker {
    #[cfg(test)]
    pub(super) fn with_fixture_store(mut self, mut store: ControlMvpStateStore) -> Self {
        store.cache_namespace = Some(self.binding);
        self.worker.store = store;
        self
    }
}

const PIN_GC_CURSOR: &str = "maintenance-pins:";

pub(super) fn pin_gc_cursor() -> &'static str {
    PIN_GC_CURSOR
}

#[allow(clippy::too_many_lines)] // One bounded candidate page, including interrupted pin publication.
pub(super) async fn expired_pin_page(
    worker: &ControlMvpMaintenanceWorker,
    now: DateTime<Utc>,
    cursor: &str,
) -> Result<Option<ControlMvpGcPlan>> {
    Box::pin(cost::phase("maintenance-GC-pins", async {
        use crate::workspace_snapshot::{RetentionTarget, retention_pin_revision_path};
        let Some(cursor) = cursor.strip_prefix(PIN_GC_CURSOR) else {
            return Ok(None);
        };
        if !cursor.is_empty() && !cursor.starts_with("retention/pins/") {
            return Err(validation_failed("invalid maintenance pin GC cursor"));
        }
        let page = worker
            .lifecycle
            .list_page_meta(
                "retention/pins/",
                (!cursor.is_empty()).then_some(cursor),
                128,
            )
            .await?;
        let continuation = page
            .next_start_after
            .map(|next| format!("{PIN_GC_CURSOR}{next}"));
        let Some(head) = worker
            .store
            .storage
            .head(&worker.store.paths.current_pointer())
            .await?
        else {
            return Ok(Some(ControlMvpGcPlan {
                head_version: String::new(),
                candidates: Vec::new(),
                continuation,
            }));
        };
        let mut candidates = Vec::new();
        for object in page.objects {
            let Some(relative) = object.path.as_str().strip_prefix("retention/pins/") else {
                continue;
            };
            let Some(pin) = relative.strip_suffix("/latest.json") else {
                // An interrupted activation can leave revision 1 without a selector.
                // It is still an immutable, fixed-deadline record; collect it under
                // the same reclamation and exact object-version fences as the pair.
                if let Some((pin, _)) = relative.split_once("/revisions/") {
                    use arco_core::storage_traits::ReadStore as _;
                    let selector = crate::workspace_snapshot::retention_pin_latest_path(pin)?;
                    if worker.lifecycle.head_raw(&selector).await?.is_none() {
                        let bytes = worker
                            .lifecycle
                            .get_range(object.path.as_str(), 0..8193)
                            .await?;
                        if bytes.len() > 8192 {
                            return Err(invariant_violation("oversized orphan retention revision"));
                        }
                        let revision =
                            crate::workspace_snapshot::decode_retention_pin_revision(&bytes)?;
                        if let RetentionTarget::Maintenance(target) = revision.target() {
                            if retention_pin_revision_path(pin, 1)? != object.path.as_str()
                                || revision.pin_id() != pin
                            {
                                return Err(invariant_violation(
                                    "orphan maintenance pin path mismatch",
                                ));
                            }
                            let (domain, _) = target.split_once('/').ok_or_else(|| {
                                invariant_violation("invalid orphan maintenance target")
                            })?;
                            if domain == worker.store.scope.domain()
                                && revision.retained_until() <= now
                                && object.last_modified.is_some_and(|modified| {
                                    modified <= now - ChronoDuration::days(7)
                                })
                            {
                                candidates.push(ControlMvpGcCandidate {
                                    path: object.path.to_string(),
                                    version: object.version,
                                    size: object.size,
                                });
                            }
                        }
                    }
                }
                continue;
            };
            let selected =
                crate::gc::reachability::load_selected_retention_pin(&worker.lifecycle, pin)
                    .await?;
            let revision = selected.latest_revision()?;
            let RetentionTarget::Maintenance(target) = revision.target() else {
                continue;
            };
            let (domain, _) = target
                .split_once('/')
                .ok_or_else(|| invariant_violation("invalid maintenance GC target"))?;
            if domain != worker.store.scope.domain() || revision.retained_until() > now {
                continue;
            }
            if selected.initial_revision()? != revision {
                return Err(invariant_violation("maintenance pin has mutable retention"));
            }
            let revision_path = retention_pin_revision_path(pin, 1)?;
            let revision_meta = worker
                .lifecycle
                .head_raw(&revision_path)
                .await?
                .ok_or_else(|| invariant_violation("maintenance pin revision disappeared"))?;
            // Never leave a selector pointing at a deleted revision. Both members
            // must age before either is selected, and deletion orders selector first.
            let cutoff = now - ChronoDuration::days(7);
            if object
                .last_modified
                .is_some_and(|modified| modified <= cutoff)
                && revision_meta
                    .last_modified
                    .is_some_and(|modified| modified <= cutoff)
            {
                candidates.push(ControlMvpGcCandidate {
                    path: object.path.to_string(),
                    version: object.version,
                    size: object.size,
                });
                candidates.push(ControlMvpGcCandidate {
                    path: revision_path,
                    version: revision_meta.version,
                    size: revision_meta.size,
                });
            }
        }
        let mut roots = RetainedAuthorityRoots::new(&worker.lifecycle, now);
        while let Some(root) = roots.next().await? {
            candidates.retain(|candidate| {
                !root.required_paths.contains(&candidate.path)
                    && !root
                        .protected_prefixes
                        .iter()
                        .any(|prefix| candidate.path.starts_with(prefix))
            });
        }
        Ok(Some(ControlMvpGcPlan {
            head_version: head.version,
            candidates,
            continuation,
        }))
    }))
    .await
}
