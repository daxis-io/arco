//! Object-store-backed control-state MVP.
//!
//! # Replay model (format version 4)
//!
//! Every manifest anchors replay on an ordered set of checksummed, non-overlapping
//! immutable Arrow IPC L1 segments and carries only the transaction suffix
//! committed since that anchor. Point and prefix reads consult segment indexes
//! before fetching Arrow data. Production commits request asynchronous layout
//! maintenance at 16 reachable L0 segments and fail closed at 32; the worker
//! renders L1 shards at half the hard segment capacity without advancing the
//! logical sequence. Explicit non-production checkpoint intervals may render
//! anchors inline for deterministic restore tests. Checkpoints materialize (or
//! reuse) the same segment set so `read_checkpoint` never replays history.
//! Segments and their indexes are bound by raw-byte checksums in every
//! reference; corrupt or substituted data fails closed.
//!
//! # Writer fencing
//!
//! Publication uses the strategy's two-condition protocol: the current-pointer
//! CAS must succeed **and** the writer's fencing epoch must be **exactly** the
//! epoch recorded in the current pointer. Only [the CAS-protected
//! claim][`ControlMvpStateStore::claim_writer_authority`] advances the epoch,
//! so an arbitrary future epoch supplied from outside can never publish and
//! can never drag the pointer epoch forward without a claim. A writer whose
//! epoch has been superseded fails closed with
//! [`CatalogError::StaleWriterEpoch`]; a writer holding an unclaimed future
//! epoch fails closed with [`CatalogError::PreconditionFailed`]. Both happen
//! before any state becomes visible. Store-maintenance writers that must
//! survive epoch claims (rather than fence competitors) adopt the published
//! epoch via [`ControlMvpStateStore::at_current_writer_epoch`].
//!
//! [`u64::MAX`] is never an acceptable epoch: accepting it (or saturating an
//! out-of-range input down to it) would make the next claim's increment
//! overflow and wedge the domain permanently, so it is rejected at every
//! entry point instead.
//!
//! Rejecting it on *input* is not enough, because a cooperative writer does
//! not supply an epoch at all — it copies the published one — and so does
//! restore-candidate generation. A pointer that already carries `u64::MAX`
//! (which no claim, publication, or restore can produce, so only corruption or
//! forgery can install one) is therefore rejected while the pointer is
//! *validated*, which is the single choke point every one of those paths goes
//! through: `at_current_writer_epoch`, `begin_control_txn`, publication,
//! `claim_writer_authority`, current-state reads, and stable restore-base
//! resolution.
//!
//! ## Repair policy for an already-published terminal epoch
//!
//! Such a domain is terminal *in place*, deliberately: nothing rewrites the
//! pointer, because a repair that silently lowered the published epoch would
//! un-fence writers the pointer says are fenced. The retained history stays
//! fully readable, because checkpoint and [`StateToken`] reads resolve through
//! manifest identity and never consult the pointer. Recovery is therefore to
//! read the retained authority through a checkpoint or state token and restore
//! it into a fresh domain scope, which begins at epoch 0 with an intact claim
//! protocol.
//!
//! # Format versioning
//!
//! Format version 4 is the only supported on-disk format and is rooted beneath
//! the fresh `control/v1/` prefix. There is deliberately no migration path from
//! the version 3 shadow format or the older JSON-anchor formats: unknown and old
//! `format_version` values fail closed instead of being migrated.
//!
//! Restore *plans* are versioned separately from on-disk state artifacts,
//! because an in-flight restore attempt written by an older revision must
//! still be readable by the recovery path that has to supersede it. Plan
//! version 1 (which predates `observed_writer_epoch`) and version 2 (which
//! names the retired object layout) are therefore decoded as legacy plans
//! that can be inspected and superseded but can never be applied. See
//! [`ControlMvpRestorePlan`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Cursor;
use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arco_core::lock::DistributedLock;
use arco_core::storage::WriteResult;
use arco_core::{AuthorityWritePrecondition, ScopedAuthorityStore, ScopedStorage};
use arrow::array::{
    Array, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, UInt8Array, UInt8Builder,
    UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::MetadataVersion;
use arrow::ipc::reader::FileReaderBuilder;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use flatbuffers::VerifierOptions;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateStore, ArcoStateTxn, CheckpointOptions,
    CheckpointToken, CommitOutcome, KeyRange, KvPair, LayoutMaintenanceIntentV1,
    LayoutMaintenanceReason, PersistedAuthorityAdapter, PersistedAuthorityKind,
    PersistedAuthorityReference, PersistedRestoreParticipantPlan, PredicateInputSet,
    ProjectionIntentV1, RestoreAttemptIdentity, RestoreParticipantInspection,
    RestoredAuthorityEvidence, ScanPage, ScanRequest, StateRestoreParticipant, StateScope,
    StateStoreBindingIdentity, StateStoreCapabilities, StateToken, TxnOptions, VersionedValue,
    build_scan_page, build_scan_page_with_backend_boundary, scan_all_entries_bounded,
};
use crate::error::{CatalogError, Result};
use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL,
};

const IMPLEMENTATION: &str = "arco-state-control-mvp";
const RESTORE_PLAN_RECORD_TYPE: &str = "control_mvp_restore_plan";
const RESTORE_PLAN_VERSION: u32 = 3;
/// Restore-plan versions that predate the `control/v1/` authority layout.
/// They remain decodable only so recovery can safely supersede them without
/// dereferencing paths from the retired layout.
const RESTORE_PLAN_VERSION_V1: u32 = 1;
const RESTORE_PLAN_VERSION_V2: u32 = 2;
const CONTROL_MVP_FORMAT_VERSION: u32 = 4;
const MAX_SEGMENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_SCAN_ARROW_BYTES: usize = MAX_SEGMENT_BYTES;
const MAX_SEGMENT_INDEX_BYTES: usize = 512 * 1024;
const MAX_SEGMENT_ROWS: usize = 1_000_000;
const MAX_SEGMENT_FOOTER_TABLES: usize = 64;
const MAX_SEGMENT_FOOTER_DEPTH: usize = 16;
const MAX_SEGMENT_FOOTER_APPARENT_BYTES: usize = 1024 * 1024;
const MAX_HEAD_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTROL_JSON_BYTES: usize = 1024 * 1024;
const MAX_TRANSACTION_JSON_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROJECTION_INTENT_AGGREGATE_BYTES: usize = 4 * 1024 * 1024;
const L0_MAINTENANCE_INTENT_THRESHOLD: usize = 16;
const L0_MAINTENANCE_BACKPRESSURE_THRESHOLD: usize = 32;
const EMPTY_CURRENT_BASE_MARKER: &[u8] =
    br#"{"record_type":"control_mvp_empty_current_base","version":1}"#;

#[derive(Debug, Clone, Copy)]
struct SegmentLimits {
    bytes: usize,
    index_bytes: usize,
    rows: usize,
}

const PRODUCTION_SEGMENT_LIMITS: SegmentLimits = SegmentLimits {
    bytes: MAX_SEGMENT_BYTES,
    index_bytes: MAX_SEGMENT_INDEX_BYTES,
    rows: MAX_SEGMENT_ROWS,
};

/// Object-store-backed state-store MVP for validating control-manifest authority.
#[derive(Clone)]
pub struct ControlMvpStateStore {
    storage: ScopedAuthorityStore,
    retention: ScopedStorage,
    binding_identity: StateStoreBindingIdentity,
    scope: StateScope,
    paths: ControlMvpPaths,
    checkpoint_interval: u64,
    writer_epoch: u64,
    segment_limits: SegmentLimits,
}

impl ControlMvpStateStore {
    /// Stable implementation identifier for this MVP backend.
    pub const IMPLEMENTATION: &'static str = IMPLEMENTATION;

    /// Default number of committed transactions between automatic replay anchors.
    pub const DEFAULT_CHECKPOINT_INTERVAL: u64 = 32;

    /// Creates a control-state MVP store over workspace-scoped storage.
    ///
    /// # Errors
    ///
    /// Returns validation errors when the storage scope does not match the state
    /// scope or when the domain cannot be represented as a safe object path.
    pub fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        scope.validate()?;
        if storage.tenant_id() != scope.tenant_id()
            || storage.workspace_id() != scope.workspace_id()
        {
            return Err(validation_failed(
                "control MVP storage scope does not match StateScope",
            ));
        }

        let paths = ControlMvpPaths::new(scope.domain());
        ScopedStorage::validate_path(&paths.current_pointer())?;
        let binding_identity = StateStoreBindingIdentity::from_scoped_storage(&storage);

        Ok(Self {
            storage: ScopedAuthorityStore::new(storage.clone()),
            retention: storage,
            binding_identity,
            scope,
            paths,
            checkpoint_interval: Self::DEFAULT_CHECKPOINT_INTERVAL,
            writer_epoch: 0,
            segment_limits: PRODUCTION_SEGMENT_LIMITS,
        })
    }

    /// Sets the automatic replay-anchor interval in committed transactions.
    #[must_use]
    pub const fn with_checkpoint_interval(mut self, interval: NonZeroU64) -> Self {
        self.checkpoint_interval = interval.get();
        self
    }

    #[cfg(test)]
    const fn with_segment_limits(mut self, limits: SegmentLimits) -> Self {
        self.segment_limits = limits;
        self
    }

    /// Pins the writer fencing epoch this store publishes with.
    ///
    /// The epoch is *not* an authority grant: publication additionally
    /// requires it to equal the epoch recorded in the published pointer, so a
    /// pinned future epoch fails closed instead of advancing the pointer
    /// without a [`Self::claim_writer_authority`] call.
    ///
    /// # Errors
    ///
    /// Returns a validation error for [`u64::MAX`], which is never an
    /// acceptable epoch: publishing it would make the next claim's increment
    /// overflow and wedge the domain permanently. Out-of-range input is
    /// rejected rather than saturated, because saturating `u64::MAX` to
    /// `u64::MAX - 1` publishes the one epoch after which exactly one further
    /// claim is possible, which is the same wedge one step later.
    pub fn with_writer_epoch(mut self, writer_epoch: u64) -> Result<Self> {
        if writer_epoch == u64::MAX {
            return Err(unclaimable_writer_epoch());
        }
        self.writer_epoch = writer_epoch;
        Ok(self)
    }

    /// Returns this store bound to the writer epoch currently recorded in the
    /// published pointer (cooperative fencing), or unchanged when the domain
    /// has no published state yet.
    ///
    /// Store-maintenance writers (for example the projection outbox worker)
    /// use this to keep functioning after another writer advanced the epoch
    /// through [`Self::claim_writer_authority`]: they adopt the published
    /// epoch instead of failing [`CatalogError::StaleWriterEpoch`] forever.
    /// The adopted epoch equals the published one, so cooperative writers can
    /// never regress fencing nor fence other writers out.
    ///
    /// # Errors
    ///
    /// Returns storage or corrupt-pointer errors other than a missing pointer.
    pub async fn at_current_writer_epoch(mut self) -> Result<Self> {
        match self.load_pointer().await {
            Ok(pointer) => {
                self.writer_epoch = pointer.writer_epoch;
                Ok(self)
            }
            Err(CatalogError::NotFound { .. }) => Ok(self),
            Err(error) => Err(error),
        }
    }

    /// Returns the writer fencing epoch this store publishes with.
    #[must_use]
    pub const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Claims the next writer fencing epoch and returns a store bound to it.
    ///
    /// The claim is durably published through the current-pointer CAS, so
    /// every writer holding an older epoch fails closed on its next begin or
    /// publish attempt. This is the **only** operation that advances the
    /// published epoch: ordinary publication requires exact equality with it.
    ///
    /// # Errors
    ///
    /// Returns a validation error before any state exists (there is no
    /// authority to fence yet), a validation error when the claim would
    /// publish [`u64::MAX`] (which no later claim could supersede), and a CAS
    /// error when another writer moved the pointer concurrently.
    pub async fn claim_writer_authority(mut self) -> Result<Self> {
        let pointer_meta = self.storage.head(&self.paths.current_pointer()).await?;
        let Some(pointer_meta) = pointer_meta else {
            return Err(validation_failed(
                "cannot claim a control MVP writer epoch before the first commit",
            ));
        };
        let pointer = self.load_pointer().await?;
        let claimed_epoch = pointer
            .writer_epoch
            .checked_add(1)
            .ok_or_else(|| validation_failed("control MVP writer epoch overflow during claim"))?;
        if claimed_epoch == u64::MAX {
            return Err(unclaimable_writer_epoch());
        }
        let claimed = ControlMvpPointer {
            writer_epoch: claimed_epoch,
            ..pointer
        };
        let claimed_bytes = encode_json_limited(
            &claimed,
            MAX_HEAD_JSON_BYTES,
            "control MVP epoch-claim head",
        )?;
        let pointer_write = self
            .storage
            .put(
                &self.paths.current_pointer(),
                claimed_bytes.clone(),
                AuthorityWritePrecondition::MatchesVersion(pointer_meta.version),
            )
            .await;
        match pointer_write {
            Ok(WriteResult::Success { .. }) => {
                self.writer_epoch = claimed_epoch;
                Ok(self)
            }
            Ok(WriteResult::PreconditionFailed { .. }) => Err(CatalogError::CasFailed {
                message: "control MVP writer epoch claim lost a pointer race".to_string(),
            }),
            Err(error) => {
                if self
                    .storage
                    .get(&self.paths.current_pointer())
                    .await
                    .is_ok_and(|current| current == claimed_bytes)
                {
                    self.writer_epoch = claimed_epoch;
                    Ok(self)
                } else {
                    Err(ambiguous_authority_outcome(format!(
                        "control MVP writer epoch claim could not be reconciled after storage failure: {error}"
                    )))
                }
            }
        }
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
        opts.validate()?;
        if let Some(scope) = opts.scope()
            && scope != &self.scope
        {
            return Err(validation_failed(
                "transaction scope does not match control MVP store",
            ));
        }

        let base = self.load_current_base_state().await?;
        validate_publication_epoch(self.writer_epoch, base.writer_epoch)?;
        let next_sequence = next_logical_sequence(
            base.state.logical_sequence,
            "beginning a control MVP transaction",
        )?;
        let request_id = opts.request_id().map(ToOwned::to_owned);
        let suffix = opts.operation_id().map_or_else(
            || Ulid::new().to_string().to_ascii_lowercase(),
            ToOwned::to_owned,
        );
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
            outbox_trim: Vec::new(),
            projection_intents: Vec::new(),
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
        let pointer_meta = self.storage.head(&self.paths.current_pointer()).await?;
        let Some(pointer_meta) = pointer_meta else {
            return Ok(ControlMvpBase {
                pointer_version: None,
                manifest_id: None,
                writer_epoch: 0,
                layout_generation: 0,
                state: ReplayState::default(),
                base_states: Vec::new(),
                tx_refs: Vec::new(),
            });
        };

        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        let state = self.replay_manifest(&manifest).await?;
        let (base_states, tx_refs) = manifest.successor_anchor();

        Ok(ControlMvpBase {
            pointer_version: Some(pointer_meta.version),
            manifest_id: Some(pointer.manifest_id),
            writer_epoch: pointer.writer_epoch,
            layout_generation: manifest.layout_generation,
            state,
            base_states,
            tx_refs,
        })
    }

    async fn load_current_state(&self) -> Result<ReplayState> {
        let Some(_pointer_meta) = self.storage.head(&self.paths.current_pointer()).await? else {
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
        let bytes = self.storage.get(&self.paths.current_pointer()).await?;
        let pointer: ControlMvpPointer =
            decode_json_limited(&bytes, MAX_HEAD_JSON_BYTES, "control MVP mutable head")?;
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
            .get(&self.paths.manifest_object(manifest_id))
            .await?;
        validate_raw_checksum(
            &bytes,
            expected_checksum,
            "control MVP manifest reference checksum",
        )?;
        let manifest: ControlMvpManifest = decode_envelope_limited(
            &bytes,
            "control-mvp-manifest",
            MAX_CONTROL_JSON_BYTES,
            "control MVP manifest",
        )?;
        manifest.validate(&self.scope, manifest_id)?;
        Ok(manifest)
    }

    async fn replay_manifest(&self, manifest: &ControlMvpManifest) -> Result<ReplayState> {
        let mut state = self.load_state_snapshots(&manifest.base_states).await?;
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

    async fn load_state_snapshots(&self, references: &[ControlMvpStateRef]) -> Result<ReplayState> {
        let Some(first) = references.first() else {
            return Ok(ReplayState::default());
        };
        let mut combined = ReplayState {
            logical_sequence: first.logical_sequence,
            ..ReplayState::default()
        };
        let mut expected_outbox_ordinal = 0_u64;
        let indexes = self.load_l1_indexes(references).await?;
        for (reference, (index_bytes, _index)) in references.iter().zip(indexes) {
            if reference.logical_sequence != combined.logical_sequence {
                return Err(invariant_violation(
                    "control MVP L1 segment set mixes logical sequences",
                ));
            }
            let shard = self
                .load_state_snapshot_from_index(reference, &index_bytes)
                .await?;
            for entry in shard.entries {
                let tombstone = entry.value.is_none();
                let value = StoredValue {
                    bytes: Bytes::from(entry.value.unwrap_or_default()),
                    generation: entry.generation,
                    tombstone,
                };
                if combined.kv.insert(entry.key, value).is_some() {
                    return Err(invariant_violation(
                        "control MVP L1 segment set contains duplicate keys",
                    ));
                }
            }
            if let Some(start) = shard.outbox_start_ordinal {
                if start != expected_outbox_ordinal {
                    return Err(invariant_violation(
                        "control MVP L1 segment set has discontinuous outbox ordinals",
                    ));
                }
                expected_outbox_ordinal = expected_outbox_ordinal
                    .checked_add(u64::try_from(shard.outbox.len()).map_err(|error| {
                        segment_serialization_error("count L1 outbox rows", error)
                    })?)
                    .ok_or_else(|| invariant_violation("control MVP L1 outbox ordinal overflow"))?;
                combined.outbox.extend(
                    shard
                        .outbox
                        .iter()
                        .map(ControlMvpOutboxStateEntry::to_record),
                );
            }
        }
        Ok(combined)
    }

    async fn load_state_snapshot_from_index(
        &self,
        reference: &ControlMvpStateRef,
        index_bytes: &[u8],
    ) -> Result<ControlMvpStateObject> {
        let segment_reference = state_segment_reference(reference);
        let bytes = self
            .storage
            .get(&self.paths.state_object(&reference.state_id))
            .await?;
        let rows = decode_segment_rows(&bytes, index_bytes, &segment_reference, &self.scope)?;
        let snapshot = state_object_from_segment_rows(reference, rows, &self.scope)?;
        snapshot.validate(&self.scope, reference)?;
        Ok(snapshot)
    }

    async fn load_segment_index(
        &self,
        reference: &ControlMvpSegmentRef,
    ) -> Result<(Bytes, ControlMvpSegmentIndex)> {
        let index_bytes = self
            .storage
            .get(&self.paths.segment_index(&reference.segment_id))
            .await?;
        if index_bytes.len() > MAX_SEGMENT_INDEX_BYTES {
            return Err(invariant_violation(
                "control MVP segment index exceeds the supported byte limit",
            ));
        }
        validate_raw_checksum(
            &index_bytes,
            Some(&reference.index_checksum_sha256),
            "control MVP segment index reference checksum",
        )?;
        let index: ControlMvpSegmentIndex = decode_json(&index_bytes, "control MVP segment index")?;
        validate_segment_index_identity(&index, reference, &self.scope)?;
        validate_segment_index_key_metadata(&index)?;
        Ok((index_bytes, index))
    }

    async fn load_l1_indexes(
        &self,
        references: &[ControlMvpStateRef],
    ) -> Result<Vec<(Bytes, ControlMvpSegmentIndex)>> {
        let mut indexes = Vec::with_capacity(references.len());
        let mut prior_max_key: Option<Vec<u8>> = None;
        let mut saw_keyless_segment = false;
        let mut segment_ids = BTreeSet::new();
        for reference in references {
            if !segment_ids.insert(reference.state_id.as_str()) {
                return Err(invariant_violation(
                    "control MVP L1 segment set repeats a segment id",
                ));
            }
            let loaded = self
                .load_segment_index(&state_segment_reference(reference))
                .await?;
            match index_key_bounds(&loaded.1)? {
                Some((min_key, max_key)) => {
                    if saw_keyless_segment
                        || prior_max_key
                            .as_ref()
                            .is_some_and(|prior| prior.as_slice() >= min_key.as_slice())
                    {
                        return Err(invariant_violation(
                            "control MVP L1 segment key bounds overlap or are out of order",
                        ));
                    }
                    prior_max_key = Some(max_key);
                }
                None => saw_keyless_segment = true,
            }
            indexes.push(loaded);
        }
        Ok(indexes)
    }

    async fn load_state_value_from_index(
        &self,
        reference: &ControlMvpStateRef,
        index_bytes: &[u8],
        index: &ControlMvpSegmentIndex,
        key: &[u8],
    ) -> Result<Option<StoredValue>> {
        let segment_reference = state_segment_reference(reference);
        if !segment_index_might_contain_key(index, key)? {
            return Ok(None);
        }
        let bytes = self
            .storage
            .get(&self.paths.state_object(&reference.state_id))
            .await?;
        let rows = decode_segment_rows(&bytes, index_bytes, &segment_reference, &self.scope)?;
        let snapshot = state_object_from_segment_rows(reference, rows, &self.scope)?;
        snapshot.validate(&self.scope, reference)?;
        Ok(snapshot
            .entries
            .into_iter()
            .find(|entry| entry.key == key)
            .map(|entry| StoredValue {
                bytes: Bytes::from(entry.value.clone().unwrap_or_default()),
                generation: entry.generation,
                tombstone: entry.value.is_none(),
            }))
    }

    async fn write_rendered_state_snapshots(
        &self,
        rendered: &[RenderedControlMvpStateSegment],
    ) -> Result<()> {
        for segment in rendered {
            put_immutable_matching(
                &self.storage,
                &self.paths.state_object(&segment.reference.state_id),
                segment.bytes.clone(),
                "control MVP L1 segment already exists with different bytes",
            )
            .await?;
            put_immutable_matching(
                &self.storage,
                &self.paths.segment_index(&segment.reference.state_id),
                segment.index_bytes.clone(),
                "control MVP L1 segment index already exists with different bytes",
            )
            .await?;
        }
        Ok(())
    }

    fn render_state_snapshots(
        &self,
        state: &ReplayState,
        manifest_id: &str,
    ) -> Result<Vec<RenderedControlMvpStateSegment>> {
        let snapshot = ControlMvpStateObject::from_replay(
            state,
            state_segment_id_for_manifest(manifest_id, 0),
            &self.scope,
        );
        let rows = segment_rows_for_state(&snapshot);
        let target_limits = half_segment_limits(self.segment_limits);
        let row_shards = partition_state_rows(
            &rows,
            state.logical_sequence,
            manifest_id,
            &self.scope,
            target_limits,
        )?;
        row_shards
            .into_iter()
            .enumerate()
            .map(|(ordinal, rows)| {
                let state_id = state_segment_id_for_manifest(manifest_id, ordinal);
                let (bytes, index_bytes, segment_reference) = encode_segment(
                    &state_id,
                    ControlMvpSegmentLevel::L1,
                    state.logical_sequence,
                    &self.scope,
                    &rows,
                    target_limits,
                )?;
                let (min_key_hex, max_key_hex) = segment_row_key_bounds_hex(&rows);
                Ok(RenderedControlMvpStateSegment {
                    reference: ControlMvpStateRef {
                        state_id,
                        logical_sequence: state.logical_sequence,
                        checksum_sha256: segment_reference.checksum_sha256,
                        index_checksum_sha256: segment_reference.index_checksum_sha256,
                        min_key_hex,
                        max_key_hex,
                    },
                    bytes,
                    index_bytes,
                })
            })
            .collect()
    }

    async fn write_l0_segment(
        &self,
        reference: &ControlMvpSegmentRef,
        bytes: Bytes,
        index_bytes: Bytes,
    ) -> Result<()> {
        put_immutable_matching(
            &self.storage,
            &self.paths.l0_segment_object(&reference.segment_id),
            bytes,
            "control MVP L0 segment already exists with different bytes",
        )
        .await?;
        put_immutable_matching(
            &self.storage,
            &self.paths.segment_index(&reference.segment_id),
            index_bytes,
            "control MVP L0 segment index already exists with different bytes",
        )
        .await
    }

    async fn load_l0_segment_rows(
        &self,
        reference: &ControlMvpSegmentRef,
    ) -> Result<Vec<ControlMvpSegmentRow>> {
        let bytes = self
            .storage
            .get(&self.paths.l0_segment_object(&reference.segment_id))
            .await?;
        let index_bytes = self
            .storage
            .get(&self.paths.segment_index(&reference.segment_id))
            .await?;
        decode_segment_rows(&bytes, &index_bytes, reference, &self.scope)
    }

    async fn load_tx(&self, tx_ref: &ControlMvpTxRef) -> Result<ControlMvpTxObject> {
        let mut tx = self.load_tx_metadata(tx_ref).await?;
        let rows = self.load_l0_segment_rows(&tx.l0_segment).await?;
        tx.hydrate_from_segment_rows(rows)?;
        Ok(tx)
    }

    async fn load_tx_metadata(&self, tx_ref: &ControlMvpTxRef) -> Result<ControlMvpTxObject> {
        let bytes = self
            .storage
            .get(&self.paths.tx_object(&tx_ref.tx_id))
            .await?;
        validate_raw_checksum(
            &bytes,
            Some(&tx_ref.checksum_sha256),
            "control MVP transaction reference checksum",
        )?;
        let tx: ControlMvpTxObject = decode_envelope_limited(
            &bytes,
            "control-mvp-tx",
            MAX_TRANSACTION_JSON_BYTES,
            "control MVP transaction",
        )?;
        tx.validate(&self.scope, tx_ref)?;
        Ok(tx)
    }

    async fn load_tx_write_if_indexed(
        &self,
        tx_ref: &ControlMvpTxRef,
        key: &[u8],
    ) -> Result<Option<ControlMvpWriteEntry>> {
        let mut tx = self.load_tx_metadata(tx_ref).await?;
        let (index_bytes, index) = self.load_segment_index(&tx.l0_segment).await?;
        if !segment_index_might_contain_key(&index, key)? {
            return Ok(None);
        }
        let bytes = self
            .storage
            .get(&self.paths.l0_segment_object(&tx.l0_segment.segment_id))
            .await?;
        let rows = decode_segment_rows(&bytes, &index_bytes, &tx.l0_segment, &self.scope)?;
        tx.hydrate_from_segment_rows(rows)?;
        Ok(tx.writes.into_iter().find(|write| write.key == key))
    }

    async fn get_from_manifest(
        &self,
        manifest: &ControlMvpManifest,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        let mut selected = None;
        let indexes = self.load_l1_indexes(&manifest.base_states).await?;
        for (reference, (index_bytes, index)) in manifest.base_states.iter().zip(indexes) {
            if let Some(value) = self
                .load_state_value_from_index(reference, &index_bytes, &index, key)
                .await?
            {
                if selected.replace(value).is_some() {
                    return Err(invariant_violation(
                        "control MVP L1 segment set contains overlapping keys",
                    ));
                }
            }
        }
        for tx_ref in &manifest.tx_refs {
            if let Some(write) = self.load_tx_write_if_indexed(tx_ref, key).await? {
                selected = Some(StoredValue {
                    bytes: Bytes::from(write.value.clone().unwrap_or_default()),
                    generation: write.generation,
                    tombstone: write.value.is_none(),
                });
            }
        }
        Ok(selected
            .filter(|value| !value.tombstone)
            .map(|value| value.bytes))
    }

    async fn validate_manifest_read_metadata(&self, manifest: &ControlMvpManifest) -> Result<()> {
        self.load_l1_indexes(&manifest.base_states).await?;
        for tx_ref in &manifest.tx_refs {
            let tx = self.load_tx_metadata(tx_ref).await?;
            self.load_segment_index(&tx.l0_segment).await?;
        }
        Ok(())
    }

    async fn load_state_prefix_from_index(
        &self,
        reference: &ControlMvpStateRef,
        index_bytes: &[u8],
        index: &ControlMvpSegmentIndex,
        prefix: &[u8],
        remaining_segments: &mut usize,
    ) -> Result<Vec<(Vec<u8>, StoredValue)>> {
        let segment_reference = state_segment_reference(reference);
        if !segment_index_overlaps_prefix(index, prefix)? {
            return Ok(Vec::new());
        }
        *remaining_segments = remaining_segments.checked_sub(1).ok_or_else(|| {
            CatalogError::MaintenanceBackpressure {
                message: "prefix scan requires more immutable segments than its page budget"
                    .to_string(),
            }
        })?;
        let bytes = self
            .storage
            .get(&self.paths.state_object(&reference.state_id))
            .await?;
        let rows = decode_segment_rows(&bytes, index_bytes, &segment_reference, &self.scope)?;
        let snapshot = state_object_from_segment_rows(reference, rows, &self.scope)?;
        snapshot.validate(&self.scope, reference)?;
        Ok(snapshot
            .entries
            .into_iter()
            .filter(|entry| entry.key.starts_with(prefix))
            .map(|entry| {
                let tombstone = entry.value.is_none();
                (
                    entry.key,
                    StoredValue {
                        bytes: Bytes::from(entry.value.unwrap_or_default()),
                        generation: entry.generation,
                        tombstone,
                    },
                )
            })
            .collect())
    }

    async fn load_tx_prefix_if_indexed(
        &self,
        tx_ref: &ControlMvpTxRef,
        prefix: &[u8],
        remaining_segments: &mut usize,
        remaining_arrow_bytes: &mut usize,
    ) -> Result<Vec<ControlMvpWriteEntry>> {
        let mut tx = self.load_tx_metadata(tx_ref).await?;
        let (index_bytes, index) = self.load_segment_index(&tx.l0_segment).await?;
        if !segment_index_overlaps_prefix(&index, prefix)? {
            return Ok(Vec::new());
        }
        let segment_size = usize::try_from(index.segment_size_bytes).map_err(|error| {
            segment_serialization_error("convert indexed segment byte size", error)
        })?;
        if segment_size > *remaining_arrow_bytes {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "prefix scan L0 overlay exceeds its physical Arrow read budget; consolidation is required"
                    .to_string(),
            });
        }
        *remaining_segments = remaining_segments.checked_sub(1).ok_or_else(|| {
            CatalogError::MaintenanceBackpressure {
                message: "prefix scan requires more immutable segments than its page budget"
                    .to_string(),
            }
        })?;
        *remaining_arrow_bytes -= segment_size;
        let bytes = self
            .storage
            .get(&self.paths.l0_segment_object(&tx.l0_segment.segment_id))
            .await?;
        let rows = decode_segment_rows(&bytes, &index_bytes, &tx.l0_segment, &self.scope)?;
        tx.hydrate_from_segment_rows(rows)?;
        Ok(tx
            .writes
            .into_iter()
            .filter(|write| write.key.starts_with(prefix))
            .collect())
    }

    async fn load_scan_overlay(
        &self,
        manifest: &ControlMvpManifest,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        remaining_segments: &mut usize,
        remaining_arrow_bytes: &mut usize,
    ) -> Result<BTreeMap<Vec<u8>, StoredValue>> {
        let mut overlay = BTreeMap::new();
        for tx_ref in &manifest.tx_refs {
            for write in self
                .load_tx_prefix_if_indexed(
                    tx_ref,
                    prefix,
                    remaining_segments,
                    remaining_arrow_bytes,
                )
                .await?
            {
                if start_after.is_none_or(|start| write.key.as_slice() > start) {
                    overlay.insert(
                        write.key,
                        StoredValue {
                            bytes: Bytes::from(write.value.clone().unwrap_or_default()),
                            generation: write.generation,
                            tombstone: write.value.is_none(),
                        },
                    );
                }
            }
        }
        Ok(overlay)
    }

    async fn scan_manifest_page(
        &self,
        manifest: &ControlMvpManifest,
        request: ScanRequest,
        observed_token: StateToken,
    ) -> Result<ScanPage> {
        self.scan_manifest_page_with_arrow_budget(
            manifest,
            request,
            observed_token,
            MAX_SCAN_ARROW_BYTES,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn scan_manifest_page_with_arrow_budget(
        &self,
        manifest: &ControlMvpManifest,
        request: ScanRequest,
        observed_token: StateToken,
        raw_arrow_budget: usize,
    ) -> Result<ScanPage> {
        request.validate_for_scope(&self.scope)?;
        let prefix = request.prefix();
        let start_after = request.effective_start_after();
        let mut remaining_segments = request.max_segments();
        let mut remaining_arrow_bytes = raw_arrow_budget;
        let overlay = self
            .load_scan_overlay(
                manifest,
                prefix,
                start_after,
                &mut remaining_segments,
                &mut remaining_arrow_bytes,
            )
            .await?;

        let mut base = BTreeMap::new();
        let mut last_loaded_max = None;
        let mut backend_resume_after = None;
        for reference in &manifest.base_states {
            let bounds = state_reference_key_bounds(reference)?;
            let Some((_min_key, max_key)) = bounds.as_ref() else {
                continue;
            };
            if !key_bounds_overlap_prefix(bounds.as_ref(), prefix) {
                continue;
            }
            if start_after.is_some_and(|start| max_key.as_slice() <= start) {
                continue;
            }
            if remaining_segments == 0 {
                backend_resume_after = last_loaded_max;
                if backend_resume_after.is_none() {
                    return Err(CatalogError::MaintenanceBackpressure {
                        message: "prefix scan segment budget is consumed by the L0 overlay before any L1 progress"
                            .to_string(),
                    });
                }
                break;
            }
            let (index_bytes, index) = self
                .load_segment_index(&state_segment_reference(reference))
                .await?;
            if index_key_bounds(&index)?.as_ref() != bounds.as_ref() {
                return Err(invariant_violation(
                    "control MVP manifest L1 bounds do not match its checksummed segment index",
                ));
            }
            let segment_size = usize::try_from(index.segment_size_bytes).map_err(|error| {
                segment_serialization_error("convert indexed segment byte size", error)
            })?;
            if segment_size > remaining_arrow_bytes {
                if last_loaded_max.is_none() {
                    return Err(CatalogError::MaintenanceBackpressure {
                        message: "prefix scan L0 overlay leaves no physical Arrow read budget for required L1 state; consolidation is required"
                            .to_string(),
                    });
                }
                backend_resume_after = last_loaded_max;
                break;
            }
            for (key, value) in self
                .load_state_prefix_from_index(
                    reference,
                    &index_bytes,
                    &index,
                    prefix,
                    &mut remaining_segments,
                )
                .await?
            {
                if start_after.is_none_or(|start| key.as_slice() > start) {
                    base.insert(key, value);
                }
            }
            remaining_arrow_bytes -= segment_size;
            last_loaded_max = Some(max_key.clone());
            if scan_page_budget_exceeded(
                &base,
                &overlay,
                max_key.as_slice(),
                request.max_rows(),
                request.max_bytes(),
            )? {
                break;
            }
        }

        let merge_through = backend_resume_after.as_deref();
        for (key, value) in overlay {
            if merge_through.is_none_or(|boundary| key.as_slice() <= boundary) {
                base.insert(key, value);
            }
        }
        let entries = base
            .into_iter()
            .filter(|(_key, value)| !value.tombstone)
            .map(|(key, value)| {
                KvPair::new(
                    key,
                    VersionedValue::new(value.bytes, Some(value.generation)),
                )
            })
            .collect::<Vec<_>>();
        build_scan_page_with_backend_boundary(
            &self.scope,
            request,
            Some(observed_token),
            entries,
            backend_resume_after,
        )
    }

    async fn write_checkpoint(&self, checkpoint: &ControlMvpCheckpoint) -> Result<()> {
        let bytes = encode_envelope_limited(
            "control-mvp-checkpoint",
            checkpoint,
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
        match self
            .storage
            .put(
                &self.paths.checkpoint_object(&checkpoint.checkpoint_id),
                bytes,
                AuthorityWritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(precondition_failed(
                "control MVP checkpoint object already exists",
            )),
        }
    }

    /// Publishes immutable checkpoint artifacts without acquiring retention
    /// coordination. The caller must already own a durable retention epoch and
    /// must treat any returned error as an uncertain external mutation.
    async fn publish_checkpoint_under_retention(
        &self,
        opts: &CheckpointOptions,
    ) -> Result<CheckpointToken> {
        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        // Reuse the manifest's own anchored snapshot when it has one;
        // otherwise materialize the replay state as a new immutable snapshot
        // so checkpoint reads never replay history.
        let (state_refs, rendered) = if !manifest.anchor_states.is_empty() {
            (manifest.anchor_states.clone(), Vec::new())
        } else if manifest.tx_refs.is_empty() && !manifest.base_states.is_empty() {
            (manifest.base_states.clone(), Vec::new())
        } else {
            let state = self.replay_manifest(&manifest).await?;
            let rendered = self.render_state_snapshots(&state, &manifest.manifest_id)?;
            let state_refs = rendered
                .iter()
                .map(|segment| segment.reference.clone())
                .collect();
            (state_refs, rendered)
        };
        let checkpoint_id = format!(
            "checkpoint-{:020}-{}",
            pointer.logical_sequence,
            Ulid::new().to_string().to_ascii_lowercase()
        );
        let checkpoint = ControlMvpCheckpoint {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.scope),
            checkpoint_id: checkpoint_id.clone(),
            manifest_id: pointer.manifest_id,
            logical_sequence: pointer.logical_sequence,
            manifest_checksum_sha256: pointer.manifest_checksum_sha256,
            states: state_refs,
            min_retention_seconds: opts.min_retention_seconds(),
        };
        encode_envelope_limited(
            "control-mvp-checkpoint",
            &checkpoint,
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
        self.write_rendered_state_snapshots(&rendered).await?;
        self.write_checkpoint(&checkpoint).await?;
        Ok(self.checkpoint_token(checkpoint_id))
    }

    async fn load_checkpoint(&self, checkpoint_id: &str) -> Result<ControlMvpCheckpoint> {
        let bytes = self
            .storage
            .get(&self.paths.checkpoint_object(checkpoint_id))
            .await?;
        let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
            &bytes,
            "control-mvp-checkpoint",
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
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

    /// Determines whether an exact transaction reference is part of the
    /// visible lineage, independent of how many replay anchors have been
    /// committed since it published.
    ///
    /// The bounded transaction suffix resets at every anchor, so a suffix-only
    /// scan would misreport an applied restore as absent (and therefore
    /// Superseded) once `checkpoint_interval` commits pass. This walk starts
    /// at the current suffix and follows the anchor chain backwards — each
    /// ordered `base_states` set names shards written by exactly one producing
    /// manifest, whose own `anchor_states` must byte-match the followed set
    /// (binding every shard's raw checksum) — until the
    /// referenced sequence is covered or genesis is reached. Every hop loads an
    /// envelope-checksummed manifest and the anchor sequence strictly
    /// decreases, so the walk is deterministic, fail-closed, and bounded by
    /// the number of anchors, not by history length.
    async fn tx_in_lineage(
        &self,
        parent: &ControlMvpBase,
        planned: &ControlMvpTxRef,
    ) -> Result<bool> {
        if planned.sequence > parent.state.logical_sequence {
            return Ok(false);
        }
        let mut tx_refs = parent.tx_refs.clone();
        let mut base_states = parent.base_states.clone();
        loop {
            if let Some(found) = tx_refs
                .iter()
                .find(|reference| reference.sequence == planned.sequence)
            {
                return Ok(found == planned);
            }
            let Some(anchor) = base_states.first() else {
                // Genesis reached without covering the transaction sequence.
                return Ok(false);
            };
            if planned.sequence > anchor.logical_sequence {
                // The suffix covered the planned sequence's range but a
                // different, contiguous set of transactions occupies it.
                return Ok(false);
            }
            let producing_manifest_id =
                manifest_id_for_anchor_state(&anchor.state_id).ok_or_else(|| {
                    invariant_violation(
                        "control MVP anchor snapshot id does not name a producing manifest",
                    )
                })?;
            let manifest = self.load_manifest(&producing_manifest_id).await?;
            if manifest.anchor_states == base_states {
                tx_refs = manifest.tx_refs;
                base_states = manifest.base_states;
                continue;
            }
            if manifest.base_states == base_states && manifest.tx_refs.is_empty() {
                let source_manifest_id = manifest.base_manifest_id.as_deref().ok_or_else(|| {
                    invariant_violation(
                        "control MVP maintenance manifest has no logical source manifest",
                    )
                })?;
                let source = self.load_manifest(source_manifest_id).await?;
                if source.logical_sequence != manifest.logical_sequence
                    || source.state_checksum_sha256 != manifest.state_checksum_sha256
                    || source.layout_generation >= manifest.layout_generation
                {
                    return Err(invariant_violation(
                        "control MVP maintenance lineage does not preserve equivalent logical state",
                    ));
                }
                tx_refs = source.tx_refs;
                base_states = source.base_states;
                continue;
            }
            return Err(invariant_violation(
                "control MVP anchor chain producing manifest does not carry the referenced L1 segments",
            ));
        }
    }

    async fn load_restore_source_lineage(
        &self,
        source: &PersistedAuthorityReference,
    ) -> Result<ControlMvpBase> {
        self.validate_restore_authority_format(source)?;
        let manifest_bytes = self.storage.get(source.manifest_path()).await?;
        if prefixed_sha256(&manifest_bytes) != source.manifest_sha256() {
            return Err(invariant_violation(
                "Control MVP restore source manifest checksum mismatch",
            ));
        }
        let manifest: ControlMvpManifest = decode_envelope_limited(
            &manifest_bytes,
            "control-mvp-manifest",
            MAX_CONTROL_JSON_BYTES,
            "Control MVP restore source manifest",
        )?;
        manifest.validate(&self.scope, source.manifest_id())?;
        if manifest.logical_sequence != source.logical_sequence() {
            return Err(invariant_violation(
                "Control MVP restore source manifest sequence mismatch",
            ));
        }
        let state = self.replay_manifest(&manifest).await?;
        let (base_states, tx_refs) = manifest.successor_anchor();
        Ok(ControlMvpBase {
            pointer_version: None,
            manifest_id: Some(manifest.manifest_id),
            writer_epoch: 0,
            layout_generation: manifest.layout_generation,
            state,
            base_states,
            tx_refs,
        })
    }

    async fn load_stable_restore_base(
        &self,
        source: &PersistedAuthorityReference,
    ) -> Result<StableRestoreBase> {
        for _ in 0..4 {
            let before = self.storage.head(&self.paths.current_pointer()).await?;
            let Some(before) = before else {
                if self
                    .storage
                    .head(&self.paths.current_pointer())
                    .await?
                    .is_some()
                {
                    continue;
                }
                let candidate_parent = self.load_restore_source_lineage(source).await?;
                return Ok(StableRestoreBase {
                    current: ControlMvpBase {
                        pointer_version: None,
                        manifest_id: None,
                        writer_epoch: 0,
                        layout_generation: 0,
                        state: ReplayState::default(),
                        base_states: Vec::new(),
                        tx_refs: Vec::new(),
                    },
                    candidate_parent,
                    current_base_kind: ControlMvpRestoreCurrentBaseKind::Empty,
                    writer_epoch: 0,
                    pointer_bytes: Bytes::from_static(EMPTY_CURRENT_BASE_MARKER),
                });
            };
            let pointer_bytes = self.storage.get(&self.paths.current_pointer()).await?;
            let Some(after) = self.storage.head(&self.paths.current_pointer()).await? else {
                continue;
            };
            if before.version != after.version {
                continue;
            }
            let pointer: ControlMvpPointer =
                decode_json(&pointer_bytes, "Control MVP restore base pointer")?;
            pointer.validate(&self.scope)?;
            let manifest = self.load_manifest_for_pointer(&pointer).await?;
            if manifest.logical_sequence != pointer.logical_sequence {
                return Err(invariant_violation(
                    "Control MVP restore base pointer sequence mismatch",
                ));
            }
            let state = self.replay_manifest(&manifest).await?;
            let (base_states, tx_refs) = manifest.successor_anchor();
            let current = ControlMvpBase {
                pointer_version: Some(before.version),
                manifest_id: Some(pointer.manifest_id),
                writer_epoch: pointer.writer_epoch,
                layout_generation: manifest.layout_generation,
                state,
                base_states,
                tx_refs,
            };
            return Ok(StableRestoreBase {
                candidate_parent: current.clone(),
                current,
                current_base_kind: ControlMvpRestoreCurrentBaseKind::Pointer,
                writer_epoch: pointer.writer_epoch,
                pointer_bytes,
            });
        }
        Err(CatalogError::CasFailed {
            message: "Control MVP current pointer was unstable during restore planning".to_string(),
        })
    }

    async fn restore_source_values(
        &self,
        source: &PersistedAuthorityReference,
        now: DateTime<Utc>,
    ) -> Result<BTreeMap<Vec<u8>, Bytes>> {
        self.validate_restore_authority_format(source)?;
        if source.reference_kind() != PersistedAuthorityKind::Checkpoint
            || source.checkpoint_path().is_none()
            || source.checkpoint_sha256().is_none()
        {
            return Err(validation_failed(
                "Control MVP restore requires checkpoint authority evidence",
            ));
        }
        let reader = self.resolve_persisted_reference_at(source, now).await?;
        Ok(
            scan_all_entries_bounded(reader.as_ref(), b"", MAX_SEGMENT_ROWS, MAX_SEGMENT_BYTES)
                .await?
                .into_iter()
                .map(|entry| (entry.key().to_vec(), entry.value().bytes().clone()))
                .collect(),
        )
    }

    fn validate_restore_authority_format(
        &self,
        source: &PersistedAuthorityReference,
    ) -> Result<()> {
        validate_control_mvp_authority_format(&self.paths, source)
    }

    fn restore_writes(
        source_values: &BTreeMap<Vec<u8>, Bytes>,
        current: &ReplayState,
    ) -> BTreeMap<Vec<u8>, StagedWrite> {
        let mut writes = BTreeMap::new();
        for (key, current) in current.kv.iter().filter(|(_key, value)| !value.tombstone) {
            match source_values.get(key) {
                Some(source_value) if source_value == &current.bytes => {}
                Some(source_value) => {
                    writes.insert(key.clone(), StagedWrite::Put(source_value.clone()));
                }
                None => {
                    writes.insert(key.clone(), StagedWrite::Delete);
                }
            }
        }
        for (key, source_value) in source_values {
            if current.kv.get(key).is_none_or(|current| current.tombstone) {
                writes.insert(key.clone(), StagedWrite::Put(source_value.clone()));
            }
        }
        writes
    }

    #[allow(clippy::too_many_lines)]
    fn render_restore_candidate(
        &self,
        source: &PersistedAuthorityReference,
        source_values: &BTreeMap<Vec<u8>, Bytes>,
        identity: &RestoreAttemptIdentity,
        stable: &StableRestoreBase,
        checkpoint_interval: u64,
    ) -> Result<RenderedControlMvpRestore> {
        let base_manifest_id = stable
            .candidate_parent
            .manifest_id
            .as_deref()
            .ok_or_else(|| validation_failed("Control MVP restore lineage has no manifest"))?;
        let result_sequence = stable
            .candidate_parent
            .state
            .logical_sequence
            .checked_add(1)
            .ok_or_else(|| validation_failed("Control MVP restore sequence overflow"))?;
        if result_sequence <= source.logical_sequence() {
            return Err(validation_failed(
                "Control MVP restore result must be newer than source authority",
            ));
        }

        let suffix = restore_identity_suffix(
            &self.scope,
            identity,
            source,
            stable.current_base_kind,
            base_manifest_id,
            stable.current.pointer_version.as_deref(),
            &prefixed_sha256(&stable.pointer_bytes),
            result_sequence,
            Some(checkpoint_interval),
        );
        let transaction_id = format!("tx-restore-{result_sequence:020}-{suffix}");
        let candidate_manifest_id = format!("manifest-{result_sequence:020}-restore-{suffix}");
        let outbox_record_id = format!(
            "restore:{}:{}:{}",
            identity.restore_id(),
            identity.attempt(),
            identity.domain()
        );

        let writes = Self::restore_writes(source_values, &stable.current.state);

        let notice = ControlMvpRestoreNotice {
            restore_id: identity.restore_id().to_string(),
            participant_attempt: identity.attempt(),
            domain: identity.domain().to_string(),
            source_logical_sequence: source.logical_sequence(),
            result_logical_sequence: result_sequence,
        };
        let mut tx = ControlMvpTxObject {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.scope),
            tx_id: transaction_id.clone(),
            base_manifest_id: Some(base_manifest_id.to_string()),
            sequence: result_sequence,
            writer_epoch: stable.writer_epoch,
            request_id: Some(format!(
                "restore:{}:{}:{}",
                identity.restore_id(),
                identity.attempt(),
                identity.domain()
            )),
            l0_segment: unwritten_l0_segment_ref(&transaction_id, result_sequence),
            writes: writes
                .into_iter()
                .map(|(key, write)| ControlMvpWriteEntry::from_staged(key, result_sequence, write))
                .collect(),
            outbox: vec![ControlMvpOutboxEntry {
                record_id: outbox_record_id.clone(),
                payload: encode_json_vec(&notice, "Control MVP restore notice")?,
            }],
            outbox_trim: Vec::new(),
        };
        let l0_rows = segment_rows_for_tx(&tx);
        let (l0_segment_bytes, l0_index_bytes, l0_reference) = encode_segment(
            &transaction_id,
            ControlMvpSegmentLevel::L0,
            result_sequence,
            &self.scope,
            &l0_rows,
            self.segment_limits,
        )?;
        tx.l0_segment = l0_reference;
        let transaction_bytes = encode_envelope_limited(
            "control-mvp-tx",
            &tx,
            MAX_TRANSACTION_JSON_BYTES,
            "Control MVP restore transaction",
        )?;
        let transaction_checksum = sha256_hex(&transaction_bytes);
        let mut candidate_state = stable.candidate_parent.state.clone();
        candidate_state.apply_tx(&tx)?;
        let mut tx_refs = stable.candidate_parent.tx_refs.clone();
        tx_refs.push(ControlMvpTxRef {
            tx_id: transaction_id.clone(),
            sequence: result_sequence,
            checksum_sha256: transaction_checksum,
        });
        let production_async_layout = checkpoint_interval == Self::DEFAULT_CHECKPOINT_INTERVAL;
        if production_async_layout && tx_refs.len() >= L0_MAINTENANCE_BACKPRESSURE_THRESHOLD {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "control MVP reached 32 L0 segments before layout maintenance completed"
                    .to_string(),
            });
        }
        let rendered_l1 = if !production_async_layout
            && u64::try_from(tx_refs.len()).unwrap_or(u64::MAX) >= checkpoint_interval
        {
            self.render_state_snapshots(&candidate_state, &candidate_manifest_id)?
        } else {
            Vec::new()
        };
        let maintenance_intent = layout_maintenance_intent_for_manifest(
            &self.scope,
            &candidate_manifest_id,
            result_sequence,
            stable.candidate_parent.layout_generation,
            tx_refs.len(),
        )?;
        let manifest = ControlMvpManifest {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.scope),
            manifest_id: candidate_manifest_id.clone(),
            logical_sequence: result_sequence,
            base_manifest_id: Some(base_manifest_id.to_string()),
            writer_epoch: stable.writer_epoch,
            layout_generation: stable.candidate_parent.layout_generation,
            base_states: stable.candidate_parent.base_states.clone(),
            anchor_states: rendered_l1
                .iter()
                .map(|segment| segment.reference.clone())
                .collect(),
            tx_refs,
            state_checksum_sha256: candidate_state.checksum()?,
            maintenance_intent,
        };
        let manifest_bytes = encode_envelope_limited(
            "control-mvp-manifest",
            &manifest,
            MAX_CONTROL_JSON_BYTES,
            "Control MVP restore manifest",
        )?;
        let manifest_checksum = sha256_hex(&manifest_bytes);
        let pointer = ControlMvpPointer {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.scope),
            manifest_id: candidate_manifest_id.clone(),
            logical_sequence: result_sequence,
            manifest_checksum_sha256: manifest_checksum,
            writer_epoch: stable.writer_epoch,
        };
        let pointer_bytes =
            encode_json_limited(&pointer, MAX_HEAD_JSON_BYTES, "Control MVP restore head")?;

        Ok(RenderedControlMvpRestore {
            transaction_id,
            transaction_bytes,
            l0_segment_bytes,
            l0_index_bytes,
            l1_segments: rendered_l1,
            candidate_manifest_id,
            manifest_bytes,
            pointer_bytes,
            outbox_record_id,
            result_sequence,
        })
    }

    async fn build_restore_plan(
        &self,
        source: &PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: DateTime<Utc>,
    ) -> Result<ControlMvpRestorePlan> {
        if identity.domain() != self.scope.domain() {
            return Err(validation_failed("restore identity domain mismatch"));
        }
        let source_values = self.restore_source_values(source, now).await?;
        let stable = self.load_stable_restore_base(source).await?;
        let rendered = self.render_restore_candidate(
            source,
            &source_values,
            identity,
            &stable,
            self.checkpoint_interval,
        )?;
        let plan = ControlMvpRestorePlan {
            record_type: RESTORE_PLAN_RECORD_TYPE.to_string(),
            version: RESTORE_PLAN_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: self.scope.clone(),
            identity: identity.clone(),
            source: source.clone(),
            current_base_kind: stable.current_base_kind,
            base_pointer_version: stable.current.pointer_version.clone(),
            observed_base_pointer_sha256: prefixed_sha256(&stable.pointer_bytes),
            observed_writer_epoch: stable.writer_epoch,
            checkpoint_interval: Some(self.checkpoint_interval),
            base_manifest_id: stable
                .candidate_parent
                .manifest_id
                .clone()
                .ok_or_else(|| validation_failed("restore lineage manifest missing"))?,
            base_logical_sequence: stable.candidate_parent.state.logical_sequence,
            transaction_id: rendered.transaction_id.clone(),
            transaction_path: self.paths.tx_object(&rendered.transaction_id),
            transaction_sha256: prefixed_sha256(&rendered.transaction_bytes),
            candidate_manifest_id: rendered.candidate_manifest_id.clone(),
            candidate_manifest_path: self.paths.manifest_object(&rendered.candidate_manifest_id),
            candidate_manifest_sha256: prefixed_sha256(&rendered.manifest_bytes),
            candidate_pointer_sha256: prefixed_sha256(&rendered.pointer_bytes),
            result_logical_sequence: rendered.result_sequence,
            restore_outbox_record_id: rendered.outbox_record_id,
        };
        plan.validate(self)?;
        let plan_bytes = encode_json(&plan, "Control MVP restore plan")?;
        validate_candidate_json_size(
            &plan_bytes,
            MAX_CONTROL_JSON_BYTES,
            "Control MVP restore plan",
        )?;
        Ok(plan)
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

    /// Returns the version-one base prefix for all control-state artifacts.
    #[must_use]
    pub fn base_prefix(&self) -> String {
        format!("control/v1/domains/{}", self.domain)
    }

    /// Returns the immutable transaction object path.
    #[must_use]
    pub fn tx_object(&self, tx_id: &str) -> String {
        format!("{}/transactions/{tx_id}.json", self.base_prefix())
    }

    /// Returns the immutable manifest object path.
    #[must_use]
    pub fn manifest_object(&self, manifest_id: &str) -> String {
        format!("{}/manifests/{manifest_id}.json", self.base_prefix())
    }

    /// Returns the current pointer path.
    #[must_use]
    pub fn current_pointer(&self) -> String {
        format!("{}/head/current.json", self.base_prefix())
    }

    /// Returns the immutable checkpoint object path.
    #[must_use]
    pub fn checkpoint_object(&self, checkpoint_id: &str) -> String {
        format!("{}/checkpoints/{checkpoint_id}.json", self.base_prefix())
    }

    /// Returns the immutable consolidated L1 state-segment object path.
    #[must_use]
    pub fn state_object(&self, state_id: &str) -> String {
        format!("{}/segments/l1/{state_id}.arrow", self.base_prefix())
    }

    /// Returns the immutable level-zero transaction-segment object path.
    #[must_use]
    pub fn l0_segment_object(&self, segment_id: &str) -> String {
        format!("{}/segments/l0/{segment_id}.arrow", self.base_prefix())
    }

    /// Returns the immutable index sidecar path for any segment level.
    #[must_use]
    pub fn segment_index(&self, segment_id: &str) -> String {
        format!("{}/indexes/{segment_id}.idx", self.base_prefix())
    }
}

fn validate_control_mvp_authority_format(
    paths: &ControlMvpPaths,
    source: &PersistedAuthorityReference,
) -> Result<()> {
    let canonical_manifest = paths.manifest_object(source.manifest_id());
    let checkpoint_prefix = format!("{}/checkpoints/", paths.base_prefix());
    let canonical_checkpoint = source.checkpoint_path().is_none_or(|path| {
        path.strip_prefix(&checkpoint_prefix)
            .and_then(|suffix| suffix.strip_suffix(".json"))
            .is_some_and(|checkpoint_id| {
                super::validate_scope_component(checkpoint_id, "checkpoint_id").is_ok()
                    && paths.checkpoint_object(checkpoint_id) == path
            })
    });
    if source.manifest_path() == canonical_manifest && canonical_checkpoint {
        return Ok(());
    }
    Err(CatalogError::UnsupportedAuthorityFormat {
        message: format!(
            "the control/v1 hard cut rejects authority reference manifest {} checkpoint {:?}; old layouts are not migrated; recover from a retained control/v1 authority source",
            source.manifest_path(),
            source.checkpoint_path(),
        ),
    })
}

/// Returns the deterministic state-snapshot id anchored to a manifest.
fn state_id_for_manifest(manifest_id: &str) -> String {
    format!(
        "state-{}",
        manifest_id.strip_prefix("manifest-").unwrap_or(manifest_id)
    )
}

fn state_segment_id_for_manifest(manifest_id: &str, ordinal: usize) -> String {
    let state_id = state_id_for_manifest(manifest_id);
    if ordinal == 0 {
        state_id
    } else {
        format!("{state_id}-part-{ordinal:06}")
    }
}

fn layout_maintenance_intent_for_manifest(
    scope: &StateScope,
    manifest_id: &str,
    logical_sequence: u64,
    layout_generation: u64,
    l0_count: usize,
) -> Result<Option<LayoutMaintenanceIntentV1>> {
    if l0_count < L0_MAINTENANCE_INTENT_THRESHOLD {
        return Ok(None);
    }
    let next_generation = layout_generation.checked_add(1).ok_or_else(|| {
        invariant_violation("control MVP layout generation cannot advance beyond u64::MAX")
    })?;
    let token = StateToken {
        scope: scope.clone(),
        logical_sequence,
        authority_manifest_id: manifest_id.to_string(),
    };
    LayoutMaintenanceIntentV1::new(
        format!("maintain-{manifest_id}"),
        &token,
        next_generation,
        LayoutMaintenanceReason::L0SegmentCount,
    )
    .map(Some)
}

/// Returns the manifest id that produced a deterministic anchor snapshot.
///
/// Inverse of [`state_id_for_manifest`] for anchor snapshots, which are only
/// ever written under the producing manifest's identity.
fn manifest_id_for_anchor_state(state_id: &str) -> Option<String> {
    state_id
        .split_once("-part-")
        .map_or(state_id, |(base, _ordinal)| base)
        .strip_prefix("state-")
        .map(|suffix| format!("manifest-{suffix}"))
}

/// Separately constructed capability for asynchronous physical-layout maintenance.
///
/// API request code receives [`ControlMvpStateStore`], not this type. Operators
/// construct the worker explicitly so consolidation has an independently
/// auditable identity and never becomes an implicit mutation fallback.
pub struct ControlMvpMaintenanceWorker {
    store: ControlMvpStateStore,
    lifecycle: ScopedStorage,
}

const CONTROL_MVP_GC_PAGE_SIZE: usize = 256;
const CONTROL_MVP_ORPHAN_MIN_AGE_DAYS: i64 = 7;
const CONTROL_MVP_TOKEN_RETENTION_DAYS: i64 = 30;

/// One immutable `control/v1` artifact proven eligible for deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpGcCandidate {
    path: String,
    version: String,
    size: u64,
}

impl ControlMvpGcCandidate {
    /// Returns the scope-relative object path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the size observed during the coordinated inventory.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}

/// Conservative dry-run plan for one `control/v1` garbage-collection pass.
#[derive(Debug, Clone)]
pub struct ControlMvpGcPlan {
    head_version: String,
    candidates: Vec<ControlMvpGcCandidate>,
    continuation: Option<String>,
}

impl ControlMvpGcPlan {
    /// Returns candidates in deterministic path order.
    #[must_use]
    pub fn candidates(&self) -> &[ControlMvpGcCandidate] {
        &self.candidates
    }

    /// Returns the total candidate bytes observed by the plan.
    #[must_use]
    pub fn candidate_bytes(&self) -> u64 {
        self.candidates.iter().fold(0_u64, |total, candidate| {
            total.saturating_add(candidate.size)
        })
    }

    /// Returns the exclusive inventory cursor for the next bounded pass.
    #[must_use]
    pub fn continuation(&self) -> Option<&str> {
        self.continuation.as_deref()
    }
}

/// Result of one coordinated active `control/v1` garbage-collection pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpGcOutcome {
    objects_deleted: u64,
    bytes_reclaimed: u64,
    continuation: Option<String>,
}

impl ControlMvpGcOutcome {
    /// Returns the number of immutable objects deleted.
    #[must_use]
    pub const fn objects_deleted(&self) -> u64 {
        self.objects_deleted
    }

    /// Returns the bytes represented by deleted inventory entries.
    #[must_use]
    pub const fn bytes_reclaimed(&self) -> u64 {
        self.bytes_reclaimed
    }

    /// Returns the exclusive inventory cursor for the next bounded pass.
    #[must_use]
    pub fn continuation(&self) -> Option<&str> {
        self.continuation.as_deref()
    }
}

/// Successful publication of an equivalent-state physical layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpMaintenanceOutcome {
    source_token: StateToken,
    selected_token: StateToken,
    layout_generation: u64,
}

impl ControlMvpMaintenanceOutcome {
    /// Returns the exact logical authority cut the worker consolidated.
    #[must_use]
    pub const fn source_token(&self) -> &StateToken {
        &self.source_token
    }

    /// Returns the equivalent-state manifest selected by exact head CAS.
    #[must_use]
    pub const fn selected_token(&self) -> &StateToken {
        &self.selected_token
    }

    /// Returns the physical layout generation selected by this publication.
    #[must_use]
    pub const fn layout_generation(&self) -> u64 {
        self.layout_generation
    }
}

impl ControlMvpMaintenanceWorker {
    /// Creates a maintenance capability over the exact scoped authority root.
    ///
    /// # Errors
    ///
    /// Returns validation errors when storage and state scope differ.
    pub fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        Ok(Self {
            store: ControlMvpStateStore::new(storage.clone(), scope)?,
            lifecycle: storage,
        })
    }

    /// Builds a bounded, read-only GC plan at the supplied clock cut.
    ///
    /// All manifests and checkpoints younger than 30 days are treated as
    /// retained token pins. The current head closure is retained regardless
    /// of age. Only immutable artifacts older than seven days are considered;
    /// missing object timestamps fail closed by retaining the object.
    ///
    /// `additional_protected_paths` carries snapshot/export/restore-journal
    /// closure discovered by the operator. Every path must be inside this
    /// exact authority domain.
    ///
    /// # Errors
    ///
    /// Returns storage, corruption, invalid-protection-path, or bounded-list
    /// capacity errors.
    pub async fn plan_gc_at(
        &self,
        now: DateTime<Utc>,
        additional_protected_paths: impl IntoIterator<Item = String>,
    ) -> Result<ControlMvpGcPlan> {
        self.plan_gc_page_at(now, additional_protected_paths, None)
            .await
    }

    /// Builds one bounded, resumable GC plan page.
    ///
    /// `continuation` is the exclusive cursor returned by the preceding page.
    /// The cursor is validated against this exact authority prefix. Each page
    /// inventories at most `CONTROL_MVP_GC_PAGE_SIZE` candidate objects while
    /// streaming all retained roots through a page-sized working set.
    ///
    /// # Errors
    ///
    /// Returns storage, corruption, or invalid-cursor errors.
    pub async fn plan_gc_page_at(
        &self,
        now: DateTime<Utc>,
        additional_protected_paths: impl IntoIterator<Item = String>,
        continuation: Option<&str>,
    ) -> Result<ControlMvpGcPlan> {
        self.plan_gc_page_inner(now, additional_protected_paths, continuation)
            .await
    }

    /// Executes conservative active GC under the workspace retention lock and
    /// durable mutation epoch.
    ///
    /// The inventory is rebuilt after the epoch is claimed, before any delete.
    /// The authority head and each candidate version are rechecked during the
    /// sweep. A concurrent head advance aborts the remaining pass.
    ///
    /// # Errors
    ///
    /// Returns coordination, storage, corruption, or revalidation errors.
    pub async fn collect_gc_at(
        &self,
        now: DateTime<Utc>,
        additional_protected_paths: impl IntoIterator<Item = String>,
    ) -> Result<ControlMvpGcOutcome> {
        self.collect_gc_page_at(now, additional_protected_paths, None)
            .await
    }

    /// Executes one bounded, resumable GC page under retention coordination.
    ///
    /// # Errors
    ///
    /// Returns coordination, storage, corruption, invalid-cursor, or
    /// revalidation errors.
    pub async fn collect_gc_page_at(
        &self,
        now: DateTime<Utc>,
        additional_protected_paths: impl IntoIterator<Item = String>,
        continuation: Option<&str>,
    ) -> Result<ControlMvpGcOutcome> {
        let protected = additional_protected_paths.into_iter().collect::<Vec<_>>();
        let mut guard =
            DistributedLock::new(Arc::new(self.lifecycle.clone()), RETENTION_GC_LOCK_PATH)
                .acquire_with_operation(
                    RETENTION_GC_LOCK_TTL,
                    RETENTION_GC_LOCK_MAX_RETRIES,
                    Some(format!("control-v1-gc:{}", self.store.scope.domain())),
                )
                .await
                .map_err(CatalogError::from)?;
        let operation_id = guard.holder_id().to_string();
        let mut epoch = match RetentionMutationEpoch::claim(
            self.lifecycle.clone(),
            &mut guard,
            RetentionMutationKind::CatalogGc,
            operation_id,
        )
        .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                let _ = guard.release().await;
                return Err(error);
            }
        };
        let collection = async {
            let plan = self
                .plan_gc_page_inner(now, protected, continuation)
                .await?;
            let mut outcome = ControlMvpGcOutcome {
                objects_deleted: 0,
                bytes_reclaimed: 0,
                continuation: plan.continuation.clone(),
            };
            for candidate in &plan.candidates {
                let head = self
                    .lifecycle
                    .head_raw(&self.store.paths.current_pointer())
                    .await?
                    .ok_or_else(|| invariant_violation("control MVP head disappeared during GC"))?;
                if head.version != plan.head_version {
                    return Err(CatalogError::CasFailed {
                        message: "control MVP head advanced during GC revalidation".to_string(),
                    });
                }
                let Some(selected) = self.lifecycle.head_raw(&candidate.path).await? else {
                    continue;
                };
                if selected.version != candidate.version {
                    return Err(CatalogError::CasFailed {
                        message: format!(
                            "control MVP GC candidate changed after inventory: {}",
                            candidate.path
                        ),
                    });
                }
                epoch.delete_reclaimable(&candidate.path).await?;
                outcome.objects_deleted = outcome.objects_deleted.saturating_add(1);
                outcome.bytes_reclaimed = outcome.bytes_reclaimed.saturating_add(candidate.size);
            }
            Ok(outcome)
        }
        .await;
        let settlement = epoch.settle().await;
        let release = guard.release().await.map_err(CatalogError::from);
        match (collection, settlement, release) {
            (Ok(outcome), Ok(()), Ok(())) => Ok(outcome),
            (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn plan_gc_page_inner(
        &self,
        now: DateTime<Utc>,
        additional_protected_paths: impl IntoIterator<Item = String>,
        continuation: Option<&str>,
    ) -> Result<ControlMvpGcPlan> {
        let base_prefix = format!("{}/", self.store.paths.base_prefix());
        let mut protected = BTreeSet::new();
        for path in additional_protected_paths {
            ScopedStorage::validate_path(&path)?;
            if !path.starts_with(&base_prefix) {
                return Err(validation_failed(&format!(
                    "control MVP GC protected path is outside authority domain: {path}"
                )));
            }
            protected.insert(path);
        }

        let inventory_page = self
            .lifecycle
            .list_page_meta(&base_prefix, continuation, CONTROL_MVP_GC_PAGE_SIZE)
            .await?;
        let next_continuation = inventory_page.next_start_after.clone();

        let Some(head) = self
            .lifecycle
            .head_raw(&self.store.paths.current_pointer())
            .await?
        else {
            return Ok(ControlMvpGcPlan {
                head_version: String::new(),
                candidates: Vec::new(),
                continuation: next_continuation,
            });
        };

        let token_cutoff = now - ChronoDuration::days(CONTROL_MVP_TOKEN_RETENTION_DAYS);
        let orphan_cutoff = now - ChronoDuration::days(CONTROL_MVP_ORPHAN_MIN_AGE_DAYS);
        let manifests_prefix = format!("{base_prefix}manifests/");
        let checkpoints_prefix = format!("{base_prefix}checkpoints/");
        let mut candidates = inventory_page
            .objects
            .into_iter()
            .filter_map(|object| {
                let path = object.path.to_string();
                if protected.contains(&path) || !Self::is_gc_managed_immutable(&base_prefix, &path)
                {
                    return None;
                }
                let cutoff = if path.starts_with(&manifests_prefix)
                    || path.starts_with(&checkpoints_prefix)
                {
                    token_cutoff
                } else {
                    orphan_cutoff
                };
                object
                    .last_modified
                    .is_some_and(|last_modified| last_modified <= cutoff)
                    .then_some(ControlMvpGcCandidate {
                        path,
                        version: object.version,
                        size: object.size,
                    })
            })
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            return Ok(ControlMvpGcPlan {
                head_version: head.version,
                candidates,
                continuation: next_continuation,
            });
        }

        candidates.retain(|candidate| !protected.contains(&candidate.path));
        let pointer = self.store.load_pointer().await?;
        let mut current_closure = BTreeSet::from([self.store.paths.current_pointer()]);
        self.protect_manifest_closure(&pointer.manifest_id, &mut current_closure)
            .await?;
        candidates.retain(|candidate| !current_closure.contains(&candidate.path));

        // Stream every retained root through a page-sized inventory and a
        // single-root closure. Only the bounded candidate page survives across
        // iterations, so total authority inventory can grow without increasing
        // process memory or wedging reclamation.
        let mut root_cursor = None;
        while !candidates.is_empty() {
            let root_page = self
                .lifecycle
                .list_page_meta(
                    &base_prefix,
                    root_cursor.as_deref(),
                    CONTROL_MVP_GC_PAGE_SIZE,
                )
                .await?;
            for object in &root_page.objects {
                let path = object.path.to_string();
                let retained_by_age = object
                    .last_modified
                    .is_none_or(|last_modified| last_modified >= token_cutoff);
                let mut retained_closure = BTreeSet::new();
                if retained_by_age && path.starts_with(&manifests_prefix) {
                    let manifest_id = path
                        .strip_prefix(&manifests_prefix)
                        .and_then(|suffix| suffix.strip_suffix(".json"))
                        .filter(|id| !id.is_empty() && !id.contains('/'))
                        .ok_or_else(|| {
                            invariant_violation("noncanonical control MVP manifest path")
                        })?;
                    self.protect_manifest_closure(manifest_id, &mut retained_closure)
                        .await?;
                } else if path.starts_with(&checkpoints_prefix) {
                    let checkpoint_id = path
                        .strip_prefix(&checkpoints_prefix)
                        .and_then(|suffix| suffix.strip_suffix(".json"))
                        .filter(|id| !id.is_empty() && !id.contains('/'))
                        .ok_or_else(|| {
                            invariant_violation("noncanonical control MVP checkpoint path")
                        })?;
                    let bytes = self.lifecycle.get_raw(&path).await?;
                    let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
                        &bytes,
                        "control-mvp-checkpoint",
                        MAX_CONTROL_JSON_BYTES,
                        "control MVP checkpoint",
                    )?;
                    checkpoint.validate(&self.store.scope, checkpoint_id)?;
                    let minimum_seconds = u64::try_from(
                        ChronoDuration::days(CONTROL_MVP_TOKEN_RETENTION_DAYS).num_seconds(),
                    )
                    .map_err(|error| {
                        invariant_violation(format!("convert checkpoint retention floor: {error}"))
                    })?;
                    let retention_seconds = checkpoint
                        .min_retention_seconds
                        .unwrap_or(0)
                        .max(minimum_seconds);
                    let retention_seconds = i64::try_from(retention_seconds).unwrap_or(i64::MAX);
                    let retained_checkpoint = object.last_modified.is_none_or(|last_modified| {
                        last_modified
                            .checked_add_signed(ChronoDuration::seconds(retention_seconds))
                            .is_none_or(|deadline| deadline >= now)
                    });
                    if retained_checkpoint {
                        retained_closure.insert(path);
                        self.protect_manifest_closure(
                            &checkpoint.manifest_id,
                            &mut retained_closure,
                        )
                        .await?;
                        for reference in &checkpoint.states {
                            Self::protect_segment(
                                &self.store.paths,
                                reference,
                                &mut retained_closure,
                            );
                        }
                    }
                }
                if !retained_closure.is_empty() {
                    candidates.retain(|candidate| !retained_closure.contains(&candidate.path));
                }
            }
            match root_page.next_start_after {
                Some(next) => root_cursor = Some(next),
                None => break,
            }
        }

        Ok(ControlMvpGcPlan {
            head_version: head.version,
            candidates,
            continuation: next_continuation,
        })
    }

    async fn protect_manifest_closure(
        &self,
        manifest_id: &str,
        protected: &mut BTreeSet<String>,
    ) -> Result<()> {
        let manifest_path = self.store.paths.manifest_object(manifest_id);
        if !protected.insert(manifest_path) {
            return Ok(());
        }
        let manifest = self.store.load_manifest(manifest_id).await?;
        for reference in manifest
            .base_states
            .iter()
            .chain(manifest.anchor_states.iter())
        {
            Self::protect_segment(&self.store.paths, reference, protected);
        }
        for tx_ref in &manifest.tx_refs {
            protected.insert(self.store.paths.tx_object(&tx_ref.tx_id));
            let tx = self.store.load_tx_metadata(tx_ref).await?;
            protected.insert(
                self.store
                    .paths
                    .l0_segment_object(&tx.l0_segment.segment_id),
            );
            protected.insert(self.store.paths.segment_index(&tx.l0_segment.segment_id));
        }
        Ok(())
    }

    fn protect_segment(
        paths: &ControlMvpPaths,
        reference: &ControlMvpStateRef,
        protected: &mut BTreeSet<String>,
    ) {
        protected.insert(paths.state_object(&reference.state_id));
        protected.insert(paths.segment_index(&reference.state_id));
    }

    fn is_gc_managed_immutable(base_prefix: &str, path: &str) -> bool {
        [
            "manifests/",
            "transactions/",
            "segments/l0/",
            "segments/l1/",
            "indexes/",
            "checkpoints/",
        ]
        .iter()
        .any(|suffix| path.starts_with(&format!("{base_prefix}{suffix}")))
    }

    /// Returns the durable intent selected by the current authority manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when the current head or manifest is unavailable or corrupt.
    pub async fn pending_intent(&self) -> Result<Option<LayoutMaintenanceIntentV1>> {
        let Some(_) = self
            .store
            .storage
            .head(&self.store.paths.current_pointer())
            .await?
        else {
            return Ok(None);
        };
        let pointer = self.store.load_pointer().await?;
        let manifest = self.store.load_manifest_for_pointer(&pointer).await?;
        Ok(manifest.maintenance_intent)
    }

    /// Consolidates the currently selected suffix into a checksummed L1 shard set.
    ///
    /// Publication uses the exact observed head version. A conflict triggers a
    /// fresh read and replan; the worker never overwrites a newer logical state.
    /// The returned token has the same logical sequence as the source token.
    ///
    /// # Errors
    ///
    /// Returns typed corruption, storage, ambiguity, or repeated-CAS errors.
    #[allow(clippy::too_many_lines)]
    pub async fn consolidate_pending(&self) -> Result<Option<ControlMvpMaintenanceOutcome>> {
        for _attempt in 0..4 {
            let Some(head) = self
                .store
                .storage
                .head(&self.store.paths.current_pointer())
                .await?
            else {
                return Ok(None);
            };
            let pointer = self.store.load_pointer().await?;
            let source_manifest = self.store.load_manifest_for_pointer(&pointer).await?;
            let Some(intent) = source_manifest.maintenance_intent.clone() else {
                return Ok(None);
            };
            let source_token = self.store.token(
                source_manifest.manifest_id.clone(),
                source_manifest.logical_sequence,
            );
            let state = self.store.replay_manifest(&source_manifest).await?;
            let candidate_manifest_id = format!(
                "manifest-{:020}-layout-{:020}-{}",
                source_manifest.logical_sequence,
                intent.layout_generation(),
                Ulid::new().to_string().to_ascii_lowercase()
            );
            let rendered = self
                .store
                .render_state_snapshots(&state, &candidate_manifest_id)?;
            let base_states = rendered
                .iter()
                .map(|segment| segment.reference.clone())
                .collect::<Vec<_>>();
            let candidate_manifest = ControlMvpManifest {
                format_version: CONTROL_MVP_FORMAT_VERSION,
                implementation: IMPLEMENTATION.to_string(),
                scope: ControlMvpScopeDoc::from(&self.store.scope),
                manifest_id: candidate_manifest_id.clone(),
                logical_sequence: source_manifest.logical_sequence,
                base_manifest_id: Some(source_manifest.manifest_id.clone()),
                writer_epoch: pointer.writer_epoch,
                layout_generation: intent.layout_generation(),
                base_states,
                anchor_states: Vec::new(),
                tx_refs: Vec::new(),
                state_checksum_sha256: source_manifest.state_checksum_sha256.clone(),
                maintenance_intent: None,
            };
            candidate_manifest.validate(&self.store.scope, &candidate_manifest_id)?;
            let manifest_bytes = encode_envelope_limited(
                "control-mvp-manifest",
                &candidate_manifest,
                MAX_CONTROL_JSON_BYTES,
                "control MVP maintenance manifest",
            )?;
            let candidate_pointer = ControlMvpPointer {
                format_version: CONTROL_MVP_FORMAT_VERSION,
                implementation: IMPLEMENTATION.to_string(),
                scope: ControlMvpScopeDoc::from(&self.store.scope),
                manifest_id: candidate_manifest_id.clone(),
                logical_sequence: source_manifest.logical_sequence,
                manifest_checksum_sha256: sha256_hex(&manifest_bytes),
                writer_epoch: pointer.writer_epoch,
            };
            let pointer_bytes = encode_json_limited(
                &candidate_pointer,
                MAX_HEAD_JSON_BYTES,
                "control MVP maintenance head",
            )?;

            for segment in &rendered {
                put_immutable_matching(
                    &self.store.storage,
                    &self.store.paths.state_object(&segment.reference.state_id),
                    segment.bytes.clone(),
                    "control MVP maintenance L1 segment already exists with different bytes",
                )
                .await?;
                put_immutable_matching(
                    &self.store.storage,
                    &self.store.paths.segment_index(&segment.reference.state_id),
                    segment.index_bytes.clone(),
                    "control MVP maintenance L1 index already exists with different bytes",
                )
                .await?;
            }
            put_immutable_matching(
                &self.store.storage,
                &self.store.paths.manifest_object(&candidate_manifest_id),
                manifest_bytes,
                "control MVP maintenance manifest already exists with different bytes",
            )
            .await?;
            let publish = self
                .store
                .storage
                .put(
                    &self.store.paths.current_pointer(),
                    pointer_bytes.clone(),
                    AuthorityWritePrecondition::MatchesVersion(head.version),
                )
                .await;
            match publish {
                Ok(WriteResult::Success { .. }) => {
                    return Ok(Some(ControlMvpMaintenanceOutcome {
                        source_token,
                        selected_token: self
                            .store
                            .token(candidate_manifest_id, source_manifest.logical_sequence),
                        layout_generation: intent.layout_generation(),
                    }));
                }
                Ok(WriteResult::PreconditionFailed { .. }) => {}
                Err(error) => {
                    if self
                        .store
                        .storage
                        .get(&self.store.paths.current_pointer())
                        .await
                        .is_ok_and(|visible| visible == pointer_bytes)
                    {
                        return Ok(Some(ControlMvpMaintenanceOutcome {
                            source_token,
                            selected_token: self
                                .store
                                .token(candidate_manifest_id, source_manifest.logical_sequence),
                            layout_generation: intent.layout_generation(),
                        }));
                    }
                    return Err(ambiguous_authority_outcome(format!(
                        "control MVP maintenance head CAS outcome is unknown: {error}"
                    )));
                }
            }
        }
        Err(CatalogError::CasFailed {
            message: "control MVP maintenance head changed during four replans".to_string(),
        })
    }
}

fn stale_writer_epoch(held: u64, current: u64) -> CatalogError {
    CatalogError::StaleWriterEpoch {
        message: format!(
            "control MVP writer epoch {held} is superseded by published epoch {current}; \
             retry with an explicit epoch of exactly {current}, or resolve the published \
             epoch cooperatively before writing"
        ),
    }
}

fn unclaimed_writer_epoch(held: u64, current: u64) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: format!(
            "control MVP writer epoch {held} is ahead of published epoch {current} but was \
             never claimed; publication requires exact equality with the published epoch and \
             only claim_writer_authority may advance it, so a future epoch supplied from \
             outside is refused instead of silently becoming authority"
        ),
    }
}

fn unclaimable_writer_epoch() -> CatalogError {
    CatalogError::Validation {
        message: format!(
            "control MVP writer epoch {} is not an acceptable epoch: publishing it would make \
             every later claim_writer_authority increment overflow and wedge the domain \
             permanently, so it is rejected rather than saturated",
            u64::MAX
        ),
    }
}

/// Returns the fail-closed error for a *published* pointer already carrying the
/// terminal epoch.
///
/// No claim, publication, or restore can produce this pointer, so observing one
/// means the pointer was corrupted or forged. Refusing it here is what stops
/// the paths that never supply an epoch — cooperative adoption through
/// [`ControlMvpStateStore::at_current_writer_epoch`] and restore-candidate
/// generation, which both copy the published epoch — from spreading a terminal
/// epoch into transactions, manifests, and new pointers.
fn terminal_published_writer_epoch() -> CatalogError {
    CatalogError::InvariantViolation {
        message: format!(
            "control MVP pointer publishes writer epoch {}, which no claim, publication, or \
             restore can produce; the domain is refused in place rather than repaired, because \
             lowering a published epoch would un-fence writers the pointer says are fenced. \
             Retained history stays readable through checkpoint and state-token reads, which \
             resolve by manifest identity and never consult the pointer; recover by restoring \
             that retained authority into a fresh domain scope",
            u64::MAX
        ),
    }
}

/// Enforces the publication epoch condition: the held epoch must equal the
/// epoch recorded in the published pointer. A lower epoch has been fenced out;
/// a higher one was never claimed and must not become authority by publishing.
fn validate_publication_epoch(held: u64, published: u64) -> Result<()> {
    if held < published {
        return Err(stale_writer_epoch(held, published));
    }
    if held > published {
        return Err(unclaimed_writer_epoch(held, published));
    }
    Ok(())
}

/// MVP projection outbox record staged inside a control transaction.
///
/// A staged record has no `origin_sequence`; replay stamps the committing
/// transaction's logical sequence so consumers can acknowledge and derive
/// projection watermarks from the record's provenance.
///
/// # Delivery identity
///
/// The `record_id` is a **business** identifier and is deliberately reusable:
/// once a record has been trimmed, a producer may stage the same id again.
/// Delivery identity is therefore [`Self::event_id`] — the immutable
/// *incarnation* of one staging, derived from the committing transaction's
/// logical sequence plus the record id. Record ids are unique across the
/// retained outbox and the logical sequence increases strictly, so every
/// staging that ever happens in a domain has a distinct event id, and a
/// re-staged record id can never be mistaken for the incarnation that was
/// consumed before it. The derivation is a pure function of committed
/// transaction data, so replay is deterministic and the state-checksum chain
/// is unchanged (event ids are derived, never stored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpProjectionOutboxRecord {
    record_id: String,
    payload: Bytes,
    origin_sequence: Option<u64>,
}

/// Returns the immutable outbox-event id for one staging incarnation.
///
/// Deterministic from committed transaction data only: `origin_sequence` is
/// the committing transaction's logical sequence (fixed width, so the
/// encoding is unambiguous) and `record_id` is unique within it.
#[must_use]
pub fn control_mvp_outbox_event_id(origin_sequence: u64, record_id: &str) -> String {
    format!("evt-{origin_sequence:020}-{record_id}")
}

/// Exact outbox event one trim removes from the source domain.
///
/// Trims are conditional on this identity, not on the reusable record id
/// alone: an observation captured before a concurrent trim/re-stage cycle
/// names an incarnation that no longer exists, and staging it fails closed
/// with [`CatalogError::PreconditionFailed`] instead of deleting whatever
/// record currently happens to carry that id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpOutboxTrimTarget {
    record_id: String,
    origin_sequence: u64,
}

impl ControlMvpOutboxTrimTarget {
    /// Names the exact outbox event incarnation to remove.
    #[must_use]
    pub fn new(record_id: impl Into<String>, origin_sequence: u64) -> Self {
        Self {
            record_id: record_id.into(),
            origin_sequence,
        }
    }

    /// Returns the business record id.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    /// Returns the observed origin sequence.
    #[must_use]
    pub const fn origin_sequence(&self) -> u64 {
        self.origin_sequence
    }

    /// Returns the immutable event id this target names.
    #[must_use]
    pub fn event_id(&self) -> String {
        control_mvp_outbox_event_id(self.origin_sequence, &self.record_id)
    }
}

/// Durable deterministic evidence for one Control MVP restore participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ControlMvpRestoreCurrentBaseKind {
    Empty,
    Pointer,
}

impl ControlMvpRestoreCurrentBaseKind {
    const fn identity_label(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Pointer => "pointer",
        }
    }
}

/// Durable deterministic evidence for one Control MVP restore participant.
///
/// # Plan versioning
///
/// Version 3 is the version this revision plans in. Versions 1 and 2 carry
/// paths from the retired authority layout and are still **decodable** so an
/// in-flight attempt can be inspected, superseded, and safely replanned.
///
/// The migration is fail-closed. A decoded v1 plan records
/// `observed_writer_epoch = 0` as "not observed". Both old versions reach
/// exactly one terminal outcome at inspection:
/// [`RestoreParticipantInspection::Superseded`]. They are never Ready and
/// their object paths are never read or written. The driver replans them as a
/// version 3 plan under the current layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ControlMvpRestorePlan {
    record_type: String,
    version: u32,
    implementation: String,
    scope: StateScope,
    identity: RestoreAttemptIdentity,
    source: PersistedAuthorityReference,
    current_base_kind: ControlMvpRestoreCurrentBaseKind,
    base_pointer_version: Option<String>,
    observed_base_pointer_sha256: String,
    observed_writer_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_interval: Option<u64>,
    base_manifest_id: String,
    base_logical_sequence: u64,
    transaction_id: String,
    transaction_path: String,
    transaction_sha256: String,
    candidate_manifest_id: String,
    candidate_manifest_path: String,
    candidate_manifest_sha256: String,
    candidate_pointer_sha256: String,
    result_logical_sequence: u64,
    restore_outbox_record_id: String,
}

/// Wire shape used to decode any supported restore-plan version.
///
/// `observed_writer_epoch` is optional here **only** so a version 1 record can
/// be read at all; the migration below decides what its absence means per
/// version instead of letting Serde silently default it.
#[derive(Deserialize)]
struct ControlMvpRestorePlanWire {
    record_type: String,
    version: u32,
    implementation: String,
    scope: StateScope,
    identity: RestoreAttemptIdentity,
    source: PersistedAuthorityReference,
    current_base_kind: ControlMvpRestoreCurrentBaseKind,
    base_pointer_version: Option<String>,
    observed_base_pointer_sha256: String,
    #[serde(default)]
    observed_writer_epoch: Option<u64>,
    #[serde(default)]
    checkpoint_interval: Option<u64>,
    base_manifest_id: String,
    base_logical_sequence: u64,
    transaction_id: String,
    transaction_path: String,
    transaction_sha256: String,
    candidate_manifest_id: String,
    candidate_manifest_path: String,
    candidate_manifest_sha256: String,
    candidate_pointer_sha256: String,
    result_logical_sequence: u64,
    restore_outbox_record_id: String,
}

impl<'de> Deserialize<'de> for ControlMvpRestorePlan {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ControlMvpRestorePlanWire::deserialize(deserializer)?;
        let observed_writer_epoch = if wire.version == RESTORE_PLAN_VERSION_V1 {
            // Version 1 never carried the field. Present means the record is
            // malformed for its declared version, absent means "not observed",
            // which apply-time handling treats as fail-closed (never Ready).
            match wire.observed_writer_epoch {
                None => 0,
                Some(_) => {
                    return Err(serde::de::Error::custom(
                        "control MVP restore plan version 1 must not carry observed_writer_epoch",
                    ));
                }
            }
        } else {
            wire.observed_writer_epoch.ok_or_else(|| {
                serde::de::Error::custom(
                    "control MVP restore plan is missing observed_writer_epoch",
                )
            })?
        };
        let checkpoint_interval = match wire.version {
            RESTORE_PLAN_VERSION_V1 | RESTORE_PLAN_VERSION_V2 => {
                if wire.checkpoint_interval.is_some() {
                    return Err(serde::de::Error::custom(
                        "legacy Control MVP restore plans must not carry checkpoint_interval",
                    ));
                }
                None
            }
            RESTORE_PLAN_VERSION => match wire.checkpoint_interval {
                Some(interval) if interval > 0 => Some(interval),
                Some(_) => {
                    return Err(serde::de::Error::custom(
                        "Control MVP restore plan checkpoint_interval must be positive",
                    ));
                }
                None => {
                    return Err(serde::de::Error::custom(
                        "Control MVP restore plan is missing checkpoint_interval",
                    ));
                }
            },
            _ => wire.checkpoint_interval,
        };
        Ok(Self {
            record_type: wire.record_type,
            version: wire.version,
            implementation: wire.implementation,
            scope: wire.scope,
            identity: wire.identity,
            source: wire.source,
            current_base_kind: wire.current_base_kind,
            base_pointer_version: wire.base_pointer_version,
            observed_base_pointer_sha256: wire.observed_base_pointer_sha256,
            observed_writer_epoch,
            checkpoint_interval,
            base_manifest_id: wire.base_manifest_id,
            base_logical_sequence: wire.base_logical_sequence,
            transaction_id: wire.transaction_id,
            transaction_path: wire.transaction_path,
            transaction_sha256: wire.transaction_sha256,
            candidate_manifest_id: wire.candidate_manifest_id,
            candidate_manifest_path: wire.candidate_manifest_path,
            candidate_manifest_sha256: wire.candidate_manifest_sha256,
            candidate_pointer_sha256: wire.candidate_pointer_sha256,
            result_logical_sequence: wire.result_logical_sequence,
            restore_outbox_record_id: wire.restore_outbox_record_id,
        })
    }
}

impl ControlMvpRestorePlan {
    /// Returns the durable plan version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns whether this plan names the retired authority layout and
    /// therefore may only be superseded, never applied.
    #[must_use]
    pub const fn is_legacy_version(&self) -> bool {
        matches!(
            self.version,
            RESTORE_PLAN_VERSION_V1 | RESTORE_PLAN_VERSION_V2
        )
    }

    /// Returns the exact source authority reference.
    #[must_use]
    pub const fn source(&self) -> &PersistedAuthorityReference {
        &self.source
    }

    pub(crate) fn validate_source_authority_format(&self) -> Result<()> {
        validate_control_mvp_authority_format(
            &ControlMvpPaths::new(self.scope.domain()),
            &self.source,
        )
    }

    /// Returns the originating participant attempt identity.
    #[must_use]
    pub const fn identity(&self) -> &RestoreAttemptIdentity {
        &self.identity
    }

    /// Returns the exact base-pointer object version.
    #[must_use]
    pub fn base_pointer_version(&self) -> Option<&str> {
        self.base_pointer_version.as_deref()
    }

    /// Returns the digest of raw base-pointer bytes bound to the observed version.
    #[must_use]
    pub fn observed_base_pointer_sha256(&self) -> &str {
        &self.observed_base_pointer_sha256
    }

    /// Returns the deterministic candidate-pointer payload digest.
    #[must_use]
    pub fn candidate_pointer_sha256(&self) -> &str {
        &self.candidate_pointer_sha256
    }

    /// Returns the deterministic transaction ID.
    #[must_use]
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    /// Returns the exact immutable transaction path.
    #[must_use]
    pub fn transaction_path(&self) -> &str {
        &self.transaction_path
    }

    /// Returns the exact planned transaction digest.
    #[must_use]
    pub fn transaction_sha256(&self) -> &str {
        &self.transaction_sha256
    }

    /// Returns the deterministic candidate manifest ID.
    #[must_use]
    pub fn candidate_manifest_id(&self) -> &str {
        &self.candidate_manifest_id
    }

    /// Returns the exact immutable candidate manifest path.
    #[must_use]
    pub fn candidate_manifest_path(&self) -> &str {
        &self.candidate_manifest_path
    }

    /// Returns the exact candidate manifest digest.
    #[must_use]
    pub fn candidate_manifest_sha256(&self) -> &str {
        &self.candidate_manifest_sha256
    }

    /// Returns the strictly newer planned logical sequence.
    #[must_use]
    pub const fn result_logical_sequence(&self) -> u64 {
        self.result_logical_sequence
    }

    fn required_checkpoint_interval(&self) -> Result<u64> {
        self.checkpoint_interval
            .filter(|interval| *interval > 0)
            .ok_or_else(|| {
                validation_failed("Control MVP restore plan checkpoint_interval is missing or zero")
            })
    }

    fn validate(&self, store: &ControlMvpStateStore) -> Result<()> {
        self.scope.validate()?;
        self.source.validate()?;
        let validated_identity = RestoreAttemptIdentity::new(
            self.identity.restore_id(),
            self.identity.attempt(),
            self.identity.domain(),
        )?;
        let current_base_valid = match self.current_base_kind {
            ControlMvpRestoreCurrentBaseKind::Empty => {
                self.base_pointer_version.is_none()
                    && self.observed_base_pointer_sha256
                        == prefixed_sha256(EMPTY_CURRENT_BASE_MARKER)
                    && self.observed_writer_epoch == 0
                    && self.base_manifest_id == self.source.manifest_id()
                    && self.base_logical_sequence == self.source.logical_sequence()
            }
            ControlMvpRestoreCurrentBaseKind::Pointer => self
                .base_pointer_version
                .as_ref()
                .is_some_and(|version| !version.is_empty()),
        };
        let checkpoint_interval = self.required_checkpoint_interval()?;
        if self.record_type != RESTORE_PLAN_RECORD_TYPE
            || self.version != RESTORE_PLAN_VERSION
            || self.implementation != IMPLEMENTATION
            || self.scope != store.scope
            || self.identity != validated_identity
            || self.identity.domain() != store.scope.domain()
            || self.source.implementation() != IMPLEMENTATION
            || self.source.scope() != &store.scope
            || self.source.reference_kind() != PersistedAuthorityKind::Checkpoint
            || self.source.checkpoint_path().is_none()
            || self.source.checkpoint_sha256().is_none()
            || !current_base_valid
            || self.base_manifest_id.is_empty()
            || self.base_logical_sequence == 0
            || self.result_logical_sequence
                != self.base_logical_sequence.checked_add(1).unwrap_or(0)
            || self.result_logical_sequence <= self.source.logical_sequence()
        {
            return Err(validation_failed("invalid Control MVP restore plan"));
        }
        let suffix = restore_identity_suffix(
            &self.scope,
            &self.identity,
            &self.source,
            self.current_base_kind,
            &self.base_manifest_id,
            self.base_pointer_version.as_deref(),
            &self.observed_base_pointer_sha256,
            self.result_logical_sequence,
            Some(checkpoint_interval),
        );
        let expected_transaction_id =
            format!("tx-restore-{:020}-{suffix}", self.result_logical_sequence);
        let expected_manifest_id = format!(
            "manifest-{:020}-restore-{suffix}",
            self.result_logical_sequence
        );
        let expected_outbox_id = format!(
            "restore:{}:{}:{}",
            self.identity.restore_id(),
            self.identity.attempt(),
            self.identity.domain()
        );
        if self.transaction_id != expected_transaction_id
            || self.transaction_path != store.paths.tx_object(&expected_transaction_id)
            || self.candidate_manifest_id != expected_manifest_id
            || self.candidate_manifest_path != store.paths.manifest_object(&expected_manifest_id)
            || self.restore_outbox_record_id != expected_outbox_id
        {
            return Err(validation_failed(
                "Control MVP restore plan deterministic identity mismatch",
            ));
        }
        for digest in [
            &self.observed_base_pointer_sha256,
            &self.transaction_sha256,
            &self.candidate_manifest_sha256,
            &self.candidate_pointer_sha256,
        ] {
            validate_prefixed_digest(digest, "Control MVP restore digest")?;
        }
        Ok(())
    }

    /// Validates only the inert evidence needed to classify a retired plan.
    ///
    /// Old transaction, manifest, and checkpoint paths intentionally are not
    /// compared with the current layout and must never be dereferenced. The
    /// remaining checks prevent a malformed or cross-scope record from being
    /// silently accepted as a legitimate recovery artifact.
    fn validate_legacy_for_supersession(&self, store: &ControlMvpStateStore) -> Result<()> {
        self.scope.validate()?;
        self.source.validate()?;
        let validated_identity = RestoreAttemptIdentity::new(
            self.identity.restore_id(),
            self.identity.attempt(),
            self.identity.domain(),
        )?;
        let current_base_valid = match self.current_base_kind {
            ControlMvpRestoreCurrentBaseKind::Empty => {
                self.base_pointer_version.is_none()
                    && self.observed_base_pointer_sha256
                        == prefixed_sha256(EMPTY_CURRENT_BASE_MARKER)
                    && self.observed_writer_epoch == 0
                    && self.base_manifest_id == self.source.manifest_id()
                    && self.base_logical_sequence == self.source.logical_sequence()
            }
            ControlMvpRestoreCurrentBaseKind::Pointer => self
                .base_pointer_version
                .as_ref()
                .is_some_and(|version| !version.is_empty()),
        };
        if self.record_type != RESTORE_PLAN_RECORD_TYPE
            || !self.is_legacy_version()
            || (self.version == RESTORE_PLAN_VERSION_V1 && self.observed_writer_epoch != 0)
            || self.checkpoint_interval.is_some()
            || self.implementation != IMPLEMENTATION
            || self.scope != store.scope
            || self.identity != validated_identity
            || self.identity.domain() != store.scope.domain()
            || self.source.implementation() != IMPLEMENTATION
            || self.source.scope() != &store.scope
            || self.source.reference_kind() != PersistedAuthorityKind::Checkpoint
            || self.source.checkpoint_path().is_none()
            || self.source.checkpoint_sha256().is_none()
            || !current_base_valid
            || self.base_manifest_id.is_empty()
            || self.base_logical_sequence == 0
            || self.result_logical_sequence
                != self.base_logical_sequence.checked_add(1).unwrap_or(0)
            || self.result_logical_sequence <= self.source.logical_sequence()
        {
            return Err(validation_failed("invalid legacy Control MVP restore plan"));
        }
        let expected_outbox_id = format!(
            "restore:{}:{}:{}",
            self.identity.restore_id(),
            self.identity.attempt(),
            self.identity.domain()
        );
        if self.restore_outbox_record_id != expected_outbox_id {
            return Err(validation_failed(
                "legacy Control MVP restore plan deterministic identity mismatch",
            ));
        }
        for path in [&self.transaction_path, &self.candidate_manifest_path] {
            ScopedStorage::validate_path(path)?;
        }
        for digest in [
            &self.observed_base_pointer_sha256,
            &self.transaction_sha256,
            &self.candidate_manifest_sha256,
            &self.candidate_pointer_sha256,
        ] {
            validate_prefixed_digest(digest, "legacy Control MVP restore digest")?;
        }
        Ok(())
    }
}

/// Explicit deterministic roll-forward adapter for [`ControlMvpStateStore`].
#[derive(Clone)]
pub struct ControlMvpRestoreParticipant {
    store: ControlMvpStateStore,
}

impl ControlMvpRestoreParticipant {
    /// Creates an explicitly configured restore participant.
    #[must_use]
    pub const fn new(store: ControlMvpStateStore) -> Self {
        Self { store }
    }

    async fn write_restore_immutable_artifacts(
        &self,
        plan: &ControlMvpRestorePlan,
        rendered: &RenderedControlMvpRestore,
    ) -> Result<()> {
        put_restore_immutable(
            &self.store.storage,
            &plan.transaction_path,
            rendered.transaction_bytes.clone(),
        )
        .await?;
        put_restore_immutable(
            &self.store.storage,
            &self.store.paths.l0_segment_object(&rendered.transaction_id),
            rendered.l0_segment_bytes.clone(),
        )
        .await?;
        put_restore_immutable(
            &self.store.storage,
            &self.store.paths.segment_index(&rendered.transaction_id),
            rendered.l0_index_bytes.clone(),
        )
        .await?;
        for l1_segment in &rendered.l1_segments {
            put_restore_immutable(
                &self.store.storage,
                &self
                    .store
                    .paths
                    .state_object(&l1_segment.reference.state_id),
                l1_segment.bytes.clone(),
            )
            .await?;
            put_restore_immutable(
                &self.store.storage,
                &self
                    .store
                    .paths
                    .segment_index(&l1_segment.reference.state_id),
                l1_segment.index_bytes.clone(),
            )
            .await?;
        }
        put_restore_immutable(
            &self.store.storage,
            &plan.candidate_manifest_path,
            rendered.manifest_bytes.clone(),
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn inspect_visible_restore(
        &self,
        plan: &ControlMvpRestorePlan,
        planned_checksum: &str,
    ) -> Result<RestoreParticipantInspection> {
        let planned_tx_ref = ControlMvpTxRef {
            tx_id: plan.transaction_id.clone(),
            sequence: plan.result_logical_sequence,
            checksum_sha256: planned_checksum.to_string(),
        };
        let tx = self.store.load_tx(&planned_tx_ref).await?;
        let expected_request_id = format!(
            "restore:{}:{}:{}",
            plan.identity.restore_id(),
            plan.identity.attempt(),
            plan.identity.domain()
        );
        let [restore_notice] = tx.outbox.as_slice() else {
            return Err(invariant_violation(
                "visible restore transaction does not contain exactly one outbox notice",
            ));
        };
        if tx.base_manifest_id.as_deref() != Some(plan.base_manifest_id.as_str())
            || tx.request_id.as_deref() != Some(expected_request_id.as_str())
            || restore_notice.record_id != plan.restore_outbox_record_id
        {
            return Err(invariant_violation(
                "visible restore transaction does not match planned restore metadata",
            ));
        }
        let notice: ControlMvpRestoreNotice = decode_json(
            &restore_notice.payload,
            "Control MVP visible restore outbox notice",
        )?;
        if notice.restore_id != plan.identity.restore_id()
            || notice.participant_attempt != plan.identity.attempt()
            || notice.domain != plan.identity.domain()
            || notice.source_logical_sequence != plan.source.logical_sequence()
            || notice.result_logical_sequence != plan.result_logical_sequence
        {
            return Err(invariant_violation(
                "visible restore outbox notice does not match planned restore",
            ));
        }
        let manifest_bytes = self
            .store
            .storage
            .get(&plan.candidate_manifest_path)
            .await?;
        if prefixed_sha256(&manifest_bytes) != plan.candidate_manifest_sha256 {
            return Err(invariant_violation(
                "visible restore manifest checksum mismatch",
            ));
        }
        let manifest: ControlMvpManifest = decode_envelope_limited(
            &manifest_bytes,
            "control-mvp-manifest",
            MAX_CONTROL_JSON_BYTES,
            "Control MVP restore candidate manifest",
        )?;
        manifest.validate(&self.store.scope, &plan.candidate_manifest_id)?;
        let base_manifest = self.store.load_manifest(&plan.base_manifest_id).await?;
        let (expected_base_states, expected_prefix) = base_manifest.successor_anchor();
        let checkpoint_interval = plan.required_checkpoint_interval()?;
        let should_anchor =
            u64::try_from(expected_prefix.len() + 1).unwrap_or(u64::MAX) >= checkpoint_interval;
        if manifest.logical_sequence != plan.result_logical_sequence
            || manifest.base_manifest_id.as_deref() != Some(plan.base_manifest_id.as_str())
            || manifest.base_states != expected_base_states
            || manifest.anchor_states.is_empty() == should_anchor
            || manifest.tx_refs.len() != expected_prefix.len() + 1
            || manifest.tx_refs.get(..expected_prefix.len()) != Some(expected_prefix.as_slice())
            || manifest.tx_refs.last() != Some(&planned_tx_ref)
        {
            return Err(invariant_violation(
                "visible restore candidate manifest does not extend the planned base",
            ));
        }
        let replayed = self.store.replay_manifest(&manifest).await?;
        if !manifest.anchor_states.is_empty() {
            let anchored = self
                .store
                .load_state_snapshots(&manifest.anchor_states)
                .await?;
            if anchored.checksum()? != replayed.checksum()? {
                return Err(invariant_violation(
                    "visible restore L1 anchor does not match replayed state",
                ));
            }
        }
        let candidate_pointer = ControlMvpPointer {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            manifest_id: plan.candidate_manifest_id.clone(),
            logical_sequence: plan.result_logical_sequence,
            manifest_checksum_sha256: sha256_hex(&manifest_bytes),
            writer_epoch: plan.observed_writer_epoch,
        };
        if prefixed_sha256(&encode_json(
            &candidate_pointer,
            "Control MVP visible restore candidate pointer",
        )?) != plan.candidate_pointer_sha256
        {
            return Err(invariant_violation(
                "visible restore candidate pointer digest mismatch",
            ));
        }
        let evidence = RestoredAuthorityEvidence::new(
            IMPLEMENTATION,
            self.store.scope.clone(),
            &plan.transaction_id,
            &plan.candidate_manifest_id,
            &plan.candidate_manifest_path,
            &plan.candidate_manifest_sha256,
            plan.result_logical_sequence,
            plan.identity.attempt(),
        )?;
        Ok(RestoreParticipantInspection::Visible {
            token: self.store.token(
                plan.candidate_manifest_id.clone(),
                plan.result_logical_sequence,
            ),
            evidence,
        })
    }
}

impl ControlMvpProjectionOutboxRecord {
    /// Creates a projection outbox record for staging in a transaction.
    #[must_use]
    pub fn new(record_id: impl Into<String>, payload: Bytes) -> Self {
        Self {
            record_id: record_id.into(),
            payload,
            origin_sequence: None,
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

    /// Returns the logical sequence of the commit that produced this record.
    ///
    /// `None` only for records that have been staged but not yet committed.
    #[must_use]
    pub const fn origin_sequence(&self) -> Option<u64> {
        self.origin_sequence
    }

    /// Returns this staging incarnation's immutable event id — the delivery
    /// identity consumers acknowledge and trim by.
    ///
    /// `None` only for records that have been staged but not yet committed,
    /// because the event id is derived from the committing sequence.
    #[must_use]
    pub fn event_id(&self) -> Option<String> {
        self.origin_sequence
            .map(|sequence| control_mvp_outbox_event_id(sequence, &self.record_id))
    }

    /// Returns the trim target naming exactly this staging incarnation.
    ///
    /// `None` only for records that have been staged but not yet committed.
    #[must_use]
    pub fn trim_target(&self) -> Option<ControlMvpOutboxTrimTarget> {
        self.origin_sequence
            .map(|sequence| ControlMvpOutboxTrimTarget::new(self.record_id.clone(), sequence))
    }

    /// Creates a record as it appears when read back from committed state,
    /// carrying the producing commit's logical sequence.
    #[must_use]
    pub fn with_origin_sequence(
        record_id: impl Into<String>,
        payload: Bytes,
        origin_sequence: u64,
    ) -> Self {
        Self {
            record_id: record_id.into(),
            payload,
            origin_sequence: Some(origin_sequence),
        }
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
    outbox_trim: Vec<ControlMvpOutboxTrimEntry>,
    projection_intents: Vec<StagedProjectionIntent>,
}

struct StagedProjectionIntent {
    intent_id: String,
    projection_kind: String,
    payload: Bytes,
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

    pub(crate) fn predicted_state_token(&self) -> Result<StateToken> {
        let predicted_sequence = next_logical_sequence(
            self.base.state.logical_sequence,
            "predicting a control MVP transaction token",
        )?;
        Ok(self
            .store
            .token(self.manifest_id.clone(), predicted_sequence))
    }

    /// Stages a projection outbox record in this MVP transaction.
    ///
    /// Record ids are unique across the retained outbox: staging an id that
    /// is currently retained, staged for trimming, or already staged in this
    /// transaction fails with a typed duplicate-id error, so a duplicate can
    /// never wedge acknowledgement or trimming of the original record. Ack
    /// retirement is ordered before source trims (see the projection outbox
    /// worker), so an id absent from the retained outbox has no live
    /// acknowledgement bound to it and re-staging it produces a fresh record.
    /// Concurrent staging of the same id is resolved by the single-writer
    /// pointer CAS: the losing commit fails and any retry begins from the
    /// winning state, where this validation rejects the duplicate.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::AlreadyExists`] when the record id is already
    /// retained in the transaction's base outbox or staged in this
    /// transaction.
    pub fn stage_projection_outbox(
        &mut self,
        record: ControlMvpProjectionOutboxRecord,
    ) -> Result<()> {
        let duplicate = self
            .base
            .state
            .outbox
            .iter()
            .map(|existing| existing.record_id.as_str())
            .chain(self.outbox.iter().map(|staged| staged.record_id.as_str()))
            .chain(
                self.projection_intents
                    .iter()
                    .map(|intent| intent.intent_id.as_str()),
            )
            .any(|existing| existing == record.record_id);
        if duplicate {
            return Err(CatalogError::AlreadyExists {
                entity: "projection outbox record".to_string(),
                name: record.record_id,
            });
        }
        self.outbox.push(record);
        Ok(())
    }

    /// Stages a version-one projection intent in the authority transaction.
    ///
    /// The committed envelope is bound to the successful transaction's
    /// [`StateToken`], persisted in the source outbox, and returned in the
    /// [`CommitOutcome`]. Queue delivery happens only after this method's
    /// transaction commits.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an invalid version-one envelope or an
    /// already-retained/staged intent identifier.
    pub fn stage_projection_intent(
        &mut self,
        intent_id: impl Into<String>,
        projection_kind: impl Into<String>,
        payload: Bytes,
    ) -> Result<()> {
        let staged = StagedProjectionIntent {
            intent_id: intent_id.into(),
            projection_kind: projection_kind.into(),
            payload,
        };
        let predicted_sequence = next_logical_sequence(
            self.base.state.logical_sequence,
            "predicting a control MVP projection token",
        )?;
        let predicted_token = self
            .store
            .token(self.manifest_id.clone(), predicted_sequence);
        ProjectionIntentV1::new(
            staged.intent_id.clone(),
            staged.projection_kind.clone(),
            &predicted_token,
            staged.payload.clone(),
        )?;
        let duplicate = self
            .base
            .state
            .outbox
            .iter()
            .map(|record| record.record_id.as_str())
            .chain(self.outbox.iter().map(|record| record.record_id.as_str()))
            .chain(
                self.projection_intents
                    .iter()
                    .map(|intent| intent.intent_id.as_str()),
            )
            .any(|existing| existing == staged.intent_id);
        if duplicate {
            return Err(CatalogError::AlreadyExists {
                entity: "projection intent".to_string(),
                name: staged.intent_id,
            });
        }
        self.projection_intents.push(staged);
        Ok(())
    }

    /// Stages removal of already-consumed projection outbox events.
    ///
    /// Trimming bounds outbox growth through snapshots: trimmed records leave
    /// the replayed state from this commit forward, while token-pinned reads of
    /// retained history still observe them. Callers must trim only events the
    /// consuming projection has durably acknowledged — the store enforces that
    /// each named *event incarnation* currently exists, not that it was
    /// acknowledged.
    ///
    /// Targets name `(record_id, origin_sequence)`, and that identity is
    /// validated against the transaction's base state — i.e. inside the
    /// transaction that will publish the trim. A caller whose observation
    /// predates a concurrent trim-and-re-stage cycle therefore fails closed
    /// instead of deleting the fresh incarnation that inherited the id.
    ///
    /// # Errors
    ///
    /// Returns a precondition failure when a record id is not present in the
    /// transaction's base outbox, when it is present under a different origin
    /// sequence than the target observed, or when it is trimmed twice.
    pub fn trim_projection_outbox(
        &mut self,
        targets: impl IntoIterator<Item = ControlMvpOutboxTrimTarget>,
    ) -> Result<()> {
        for target in targets {
            let Some(present) = self
                .base
                .state
                .outbox
                .iter()
                .find(|record| record.record_id == target.record_id)
            else {
                return Err(precondition_failed(&format!(
                    "cannot trim projection outbox record {}: not present in current state",
                    target.record_id
                )));
            };
            if present.origin_sequence != Some(target.origin_sequence) {
                return Err(precondition_failed(&format!(
                    "cannot trim projection outbox event {}: record {} is currently retained as \
                     event {} (a different incarnation of the same record id)",
                    target.event_id(),
                    target.record_id,
                    present
                        .event_id()
                        .unwrap_or_else(|| "<uncommitted>".to_string()),
                )));
            }
            if self
                .outbox_trim
                .iter()
                .any(|staged| staged.record_id() == target.record_id)
            {
                return Err(precondition_failed(&format!(
                    "projection outbox record {} is already staged for trimming",
                    target.record_id
                )));
            }
            self.outbox_trim.push(ControlMvpOutboxTrimEntry {
                record_id: target.record_id,
                origin_sequence: target.origin_sequence,
            });
        }
        Ok(())
    }

    /// Commits the transaction and returns the resulting state token.
    ///
    /// # Errors
    ///
    /// Returns an error when artifact writes fail, preconditions are not met, or
    /// pointer CAS publication loses to another writer.
    pub async fn commit(self) -> Result<CommitOutcome> {
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

    #[allow(clippy::too_many_lines)]
    async fn commit_inner(self) -> Result<CommitOutcome> {
        for precondition in &self.preconditions {
            self.base.state.validate_precondition(precondition)?;
        }
        validate_publication_epoch(self.store.writer_epoch, self.base.writer_epoch)?;

        let next_sequence = next_logical_sequence(
            self.base.state.logical_sequence,
            "committing a control MVP transaction",
        )?;
        let committed_token = self.store.token(self.manifest_id.clone(), next_sequence);
        let projection_intents = self
            .projection_intents
            .iter()
            .map(|intent| {
                ProjectionIntentV1::new(
                    intent.intent_id.clone(),
                    intent.projection_kind.clone(),
                    &committed_token,
                    intent.payload.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let encoded_projection_intents = projection_intents
            .iter()
            .map(|intent| encode_json(intent, "projection intent"))
            .collect::<Result<Vec<_>>>()?;
        let aggregate_intent_bytes =
            encoded_projection_intents
                .iter()
                .try_fold(0_usize, |total, intent| {
                    total.checked_add(intent.len()).ok_or_else(|| {
                        CatalogError::MaintenanceBackpressure {
                            message: "projection intent aggregate byte count overflow".to_string(),
                        }
                    })
                })?;
        if aggregate_intent_bytes > MAX_PROJECTION_INTENT_AGGREGATE_BYTES {
            return Err(CatalogError::MaintenanceBackpressure {
                message: format!(
                    "projection intents require {aggregate_intent_bytes} JSON bytes, above the {MAX_PROJECTION_INTENT_AGGREGATE_BYTES}-byte transaction budget"
                ),
            });
        }
        let mut committed_outbox = self.outbox;
        for (intent, encoded) in projection_intents.iter().zip(encoded_projection_intents) {
            committed_outbox.push(ControlMvpProjectionOutboxRecord::new(
                intent.intent_id(),
                encoded,
            ));
        }
        let mut tx = ControlMvpTxObject {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            tx_id: self.tx_id.clone(),
            base_manifest_id: self.base.manifest_id.clone(),
            sequence: next_sequence,
            writer_epoch: self.store.writer_epoch,
            request_id: self.request_id.clone(),
            l0_segment: unwritten_l0_segment_ref(&self.tx_id, next_sequence),
            writes: self
                .writes
                .into_iter()
                .map(|(key, write)| ControlMvpWriteEntry::from_staged(key, next_sequence, write))
                .collect(),
            outbox: committed_outbox
                .iter()
                .map(ControlMvpOutboxEntry::from_record)
                .collect(),
            outbox_trim: self.outbox_trim,
        };
        let l0_rows = segment_rows_for_tx(&tx);
        let (l0_bytes, l0_index_bytes, l0_reference) = encode_segment(
            &self.tx_id,
            ControlMvpSegmentLevel::L0,
            next_sequence,
            &self.store.scope,
            &l0_rows,
            self.store.segment_limits,
        )?;
        tx.l0_segment = l0_reference.clone();
        let tx_bytes = encode_envelope_limited(
            "control-mvp-tx",
            &tx,
            MAX_TRANSACTION_JSON_BYTES,
            "control MVP transaction",
        )?;
        let tx_checksum = sha256_hex(&tx_bytes);
        let candidate_tx_ref = ControlMvpTxRef {
            tx_id: self.tx_id.clone(),
            sequence: next_sequence,
            checksum_sha256: tx_checksum.clone(),
        };
        let mut candidate_state = self.base.state.clone();
        candidate_state.apply_tx(&tx)?;

        let mut tx_refs = self.base.tx_refs.clone();
        tx_refs.push(candidate_tx_ref.clone());

        let production_async_layout =
            self.store.checkpoint_interval == ControlMvpStateStore::DEFAULT_CHECKPOINT_INTERVAL;
        if production_async_layout && tx_refs.len() >= L0_MAINTENANCE_BACKPRESSURE_THRESHOLD {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "control MVP reached 32 L0 segments before layout maintenance completed"
                    .to_string(),
            });
        }

        // Explicit non-production intervals retain the deterministic inline
        // anchor path used by restore tests. Production uses maintenance intent.
        let rendered_anchor =
            if !production_async_layout && tx_refs.len() as u64 >= self.store.checkpoint_interval {
                self.store
                    .render_state_snapshots(&candidate_state, &self.manifest_id)?
            } else {
                Vec::new()
            };
        let anchor_states = rendered_anchor
            .iter()
            .map(|rendered| rendered.reference.clone())
            .collect();
        let maintenance_intent = layout_maintenance_intent_for_manifest(
            &self.store.scope,
            &self.manifest_id,
            next_sequence,
            self.base.layout_generation,
            tx_refs.len(),
        )?;

        let manifest = ControlMvpManifest {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            manifest_id: self.manifest_id.clone(),
            logical_sequence: next_sequence,
            base_manifest_id: self.base.manifest_id,
            writer_epoch: self.store.writer_epoch,
            layout_generation: self.base.layout_generation,
            base_states: self.base.base_states,
            anchor_states,
            tx_refs,
            state_checksum_sha256: candidate_state.checksum()?,
            maintenance_intent,
        };
        let manifest_bytes = encode_envelope_limited(
            "control-mvp-manifest",
            &manifest,
            MAX_CONTROL_JSON_BYTES,
            "control MVP manifest",
        )?;
        let manifest_checksum = sha256_hex(&manifest_bytes);
        let pointer = ControlMvpPointer {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            manifest_id: self.manifest_id.clone(),
            logical_sequence: next_sequence,
            manifest_checksum_sha256: manifest_checksum,
            writer_epoch: self.store.writer_epoch,
        };
        let pointer_bytes =
            encode_json_limited(&pointer, MAX_HEAD_JSON_BYTES, "control MVP mutable head")?;
        let precondition = self.base.pointer_version.map_or(
            AuthorityWritePrecondition::DoesNotExist,
            AuthorityWritePrecondition::MatchesVersion,
        );

        // Candidate replay, required-anchor rendering, manifest encoding, and
        // head encoding all complete before the first immutable artifact is
        // published. A capacity failure therefore leaves no orphan candidate.
        put_immutable(
            &self.store.storage,
            &self.store.paths.tx_object(&self.tx_id),
            tx_bytes,
            "control MVP transaction object already exists",
        )
        .await?;
        self.store
            .write_l0_segment(&l0_reference, l0_bytes, l0_index_bytes)
            .await?;
        for rendered in rendered_anchor {
            put_immutable_matching(
                &self.store.storage,
                &self.store.paths.state_object(&rendered.reference.state_id),
                rendered.bytes,
                "control MVP L1 segment already exists with different bytes",
            )
            .await?;
            put_immutable_matching(
                &self.store.storage,
                &self.store.paths.segment_index(&rendered.reference.state_id),
                rendered.index_bytes,
                "control MVP L1 segment index already exists with different bytes",
            )
            .await?;
        }
        put_immutable(
            &self.store.storage,
            &self.store.paths.manifest_object(&self.manifest_id),
            manifest_bytes,
            "control MVP manifest object already exists",
        )
        .await?;
        let pointer_write = self
            .store
            .storage
            .put(
                &self.store.paths.current_pointer(),
                pointer_bytes.clone(),
                precondition,
            )
            .await;
        match pointer_write {
            Err(error) => {
                // S3 may accept a conditional PUT and lose the response. The
                // exact canonical pointer bytes prove direct publication. A
                // successor may advance the head before readback, so the same
                // exact transaction reference in visible lineage also proves
                // commitment. Anything else remains genuinely ambiguous.
                let exact_pointer_match = self
                    .store
                    .storage
                    .get(&self.store.paths.current_pointer())
                    .await
                    .is_ok_and(|current| current == pointer_bytes);
                if exact_pointer_match {
                    Ok(CommitOutcome::new(committed_token, projection_intents))
                } else {
                    let visible_lineage_contains_candidate =
                        match self.store.load_current_base_state().await {
                            Ok(visible) => self
                                .store
                                .tx_in_lineage(&visible, &candidate_tx_ref)
                                .await
                                .unwrap_or(false),
                            Err(_) => false,
                        };
                    if visible_lineage_contains_candidate {
                        Ok(CommitOutcome::new(committed_token, projection_intents))
                    } else {
                        Err(ambiguous_authority_outcome(format!(
                            "control MVP commit {} could not be reconciled after storage failure: {error}",
                            candidate_tx_ref.tx_id
                        )))
                    }
                }
            }
            Ok(WriteResult::Success { .. }) => {
                Ok(CommitOutcome::new(committed_token, projection_intents))
            }
            Ok(WriteResult::PreconditionFailed { .. }) => {
                // Distinguish an epoch supersession from an ordinary CAS race
                // so fenced-out writers get the typed fail-closed error.
                if let Ok(current) = self.store.load_pointer().await
                    && current.writer_epoch > self.store.writer_epoch
                {
                    return Err(stale_writer_epoch(
                        self.store.writer_epoch,
                        current.writer_epoch,
                    ));
                }
                Err(CatalogError::CasFailed {
                    message: "control MVP pointer CAS lost to a newer manifest".to_string(),
                })
            }
        }
    }
}

#[async_trait]
impl ArcoStateReader for ControlMvpStateStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let Some(_pointer_meta) = self.storage.head(&self.paths.current_pointer()).await? else {
            return Ok(None);
        };
        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        self.get_from_manifest(&manifest, key).await
    }

    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        request.validate_for_scope(&self.scope)?;
        let (manifest, observed_token) = if let Some(token) = request.continuation_token() {
            let manifest = self.load_manifest(token.authority_manifest_id()).await?;
            if manifest.logical_sequence != token.logical_sequence() {
                return Err(invariant_violation(
                    "scan continuation logical sequence does not match authority manifest",
                ));
            }
            (manifest, Some(token.clone()))
        } else {
            let Some(_pointer_meta) = self.storage.head(&self.paths.current_pointer()).await?
            else {
                return build_scan_page(&self.scope, request, None, Vec::new());
            };
            let pointer = self.load_pointer().await?;
            let manifest = self.load_manifest_for_pointer(&pointer).await?;
            (
                manifest,
                Some(self.token(pointer.manifest_id, pointer.logical_sequence)),
            )
        };
        let observed_token = observed_token.ok_or_else(|| {
            invariant_violation("control MVP manifest scan has no observed authority token")
        })?;
        self.scan_manifest_page(&manifest, request, observed_token)
            .await
    }

    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
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
        self.validate_manifest_read_metadata(&manifest).await?;
        Ok(Box::new(ControlMvpRetainedReader {
            scope: self.scope.clone(),
            token,
            source: ControlMvpRetainedSource::Manifest {
                store: Box::new(self.clone()),
                manifest: Box::new(manifest),
            },
        }))
    }

    async fn read_checkpoint(&self, token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "CheckpointToken scope does not match control MVP store",
            ));
        }
        let checkpoint = self.load_checkpoint(token.checkpoint_id()).await?;
        if checkpoint.states.is_empty()
            || checkpoint
                .states
                .iter()
                .any(|state| state.logical_sequence != checkpoint.logical_sequence)
        {
            return Err(invariant_violation(
                "control MVP checkpoint state segments do not match checkpoint sequence",
            ));
        }
        // Sequence agreement alone does not prove the snapshot is the state
        // the checkpoint's authority manifest names. Concurrent losing anchor
        // commits leave valid, same-scope, same-sequence orphan snapshots
        // behind, so a coherently substituted state reference would otherwise
        // select a losing fork. Load the named manifest under the
        // checkpoint's own manifest checksum and require the snapshot's
        // semantic state checksum to equal the manifest's. This stays bounded
        // (checkpoint + manifest + snapshot) and never replays history.
        let manifest = self
            .load_manifest_with_expected_checksum(
                &checkpoint.manifest_id,
                Some(&checkpoint.manifest_checksum_sha256),
            )
            .await?;
        if manifest.logical_sequence != checkpoint.logical_sequence {
            return Err(invariant_violation(
                "control MVP checkpoint sequence does not match its authority manifest",
            ));
        }
        let state = self.load_state_snapshots(&checkpoint.states).await?;
        if state.checksum()? != manifest.state_checksum_sha256 {
            return Err(invariant_violation(
                "control MVP checkpoint snapshot is not the state named by its authority manifest",
            ));
        }
        Ok(Box::new(ControlMvpRetainedReader {
            scope: self.scope.clone(),
            token: self.token(checkpoint.manifest_id, checkpoint.logical_sequence),
            source: ControlMvpRetainedSource::Materialized(state),
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
        if opts.is_externally_retention_coordinated() {
            return self.publish_checkpoint_under_retention(&opts).await;
        }
        let mut guard =
            DistributedLock::new(Arc::new(self.retention.clone()), RETENTION_GC_LOCK_PATH)
                .acquire_with_operation(
                    RETENTION_GC_LOCK_TTL,
                    RETENTION_GC_LOCK_MAX_RETRIES,
                    Some(format!("control-v1-checkpoint:{}", self.scope.domain())),
                )
                .await
                .map_err(CatalogError::from)?;
        let operation_id = format!(
            "control-v1-checkpoint-{}",
            Ulid::new().to_string().to_ascii_lowercase()
        );
        let lifecycle = self.retention.clone();
        let terminal_operation_id = operation_id.clone();
        let mut epoch = match RetentionMutationEpoch::claim(
            lifecycle,
            &mut guard,
            RetentionMutationKind::CatalogCheckpointPublish,
            operation_id,
        )
        .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                let _ = guard.release().await;
                return Err(error);
            }
        };
        let publication = epoch
            .run_external_mutation(self.publish_checkpoint_under_retention(&opts))
            .await;
        let settlement = if publication.is_ok() {
            epoch.settle().await
        } else {
            // Every checkpoint write above has finished before this branch is
            // entered. Checkpoint publication is additive: a selected final
            // checkpoint record is a retained root, while snapshots without
            // that record are merely unreachable candidates. It is therefore
            // safe to prove this exact operation terminal after a returned
            // error. A process crash cannot execute this reconciliation and
            // deliberately leaves the epoch in flight for operator recovery.
            drop(epoch);
            let terminal_operation_ids = BTreeSet::from([terminal_operation_id]);
            match RetentionMutationEpoch::settle_terminal_matching(
                self.retention.clone(),
                &mut guard,
                RetentionMutationKind::CatalogCheckpointPublish,
                &terminal_operation_ids,
            )
            .await
            {
                Ok(true) => Ok(()),
                Ok(false) => Err(CatalogError::CasFailed {
                    message: "checkpoint retention epoch changed before terminal settlement"
                        .to_string(),
                }),
                Err(error) => Err(error),
            }
        };
        let release = guard.release().await.map_err(CatalogError::from);
        match (publication, settlement, release) {
            (Ok(token), Ok(()), Ok(())) => Ok(token),
            (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        }
    }
}

#[async_trait]
impl PersistedAuthorityAdapter for ControlMvpStateStore {
    async fn persist_state_reference(
        &self,
        token: &StateToken,
        retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        if retention_deadline <= Utc::now() {
            return Err(validation_failed(
                "persisted authority retention deadline must be in the future",
            ));
        }
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "StateToken scope does not match control MVP store",
            ));
        }
        let manifest_path = self.paths.manifest_object(token.authority_manifest_id());
        let bytes = self.storage.get(&manifest_path).await?;
        let manifest: ControlMvpManifest = decode_envelope_limited(
            &bytes,
            "control-mvp-manifest",
            MAX_CONTROL_JSON_BYTES,
            "control MVP manifest",
        )?;
        manifest.validate(&self.scope, token.authority_manifest_id())?;
        if manifest.logical_sequence != token.logical_sequence() {
            return Err(invariant_violation(
                "StateToken logical sequence does not match manifest",
            ));
        }
        PersistedAuthorityReference::new(
            IMPLEMENTATION,
            self.scope.clone(),
            PersistedAuthorityKind::StateToken,
            token.authority_manifest_id(),
            token.logical_sequence(),
            manifest_path,
            prefixed_sha256(&bytes),
            None,
            None,
            retention_deadline,
        )
    }

    async fn persist_checkpoint_reference(
        &self,
        token: &CheckpointToken,
        retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        if retention_deadline <= Utc::now() {
            return Err(validation_failed(
                "persisted authority retention deadline must be in the future",
            ));
        }
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "CheckpointToken scope does not match control MVP store",
            ));
        }
        let checkpoint_path = self.paths.checkpoint_object(token.checkpoint_id());
        let checkpoint_bytes = self.storage.get(&checkpoint_path).await?;
        let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
            &checkpoint_bytes,
            "control-mvp-checkpoint",
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
        checkpoint.validate(&self.scope, token.checkpoint_id())?;

        let manifest_path = self.paths.manifest_object(&checkpoint.manifest_id);
        let manifest_bytes = self.storage.get(&manifest_path).await?;
        validate_raw_checksum(
            &manifest_bytes,
            Some(&checkpoint.manifest_checksum_sha256),
            "control MVP checkpoint manifest checksum",
        )?;
        let manifest: ControlMvpManifest = decode_envelope_limited(
            &manifest_bytes,
            "control-mvp-manifest",
            MAX_CONTROL_JSON_BYTES,
            "control MVP manifest",
        )?;
        manifest.validate(&self.scope, &checkpoint.manifest_id)?;
        if manifest.logical_sequence != checkpoint.logical_sequence {
            return Err(invariant_violation(
                "checkpoint logical sequence does not match manifest",
            ));
        }

        PersistedAuthorityReference::new(
            IMPLEMENTATION,
            self.scope.clone(),
            PersistedAuthorityKind::Checkpoint,
            checkpoint.manifest_id,
            checkpoint.logical_sequence,
            manifest_path,
            prefixed_sha256(&manifest_bytes),
            Some(checkpoint_path),
            Some(prefixed_sha256(&checkpoint_bytes)),
            retention_deadline,
        )
    }

    async fn resolve_persisted_reference_at(
        &self,
        reference: &PersistedAuthorityReference,
        now: DateTime<Utc>,
    ) -> Result<Box<dyn ArcoStateReader>> {
        reference.validate()?;
        if reference.implementation() != IMPLEMENTATION {
            return Err(validation_failed(
                "persisted authority implementation does not match control MVP",
            ));
        }
        if reference.scope() != &self.scope {
            return Err(validation_failed(
                "persisted authority scope does not match control MVP store",
            ));
        }
        if reference.retention_deadline() <= now {
            return Err(validation_failed(
                "persisted authority reference is expired",
            ));
        }

        let manifest_path = self.paths.manifest_object(reference.manifest_id());
        if reference.manifest_path() != manifest_path {
            return Err(validation_failed(
                "persisted authority manifest path is not canonical for this store",
            ));
        }
        let manifest_bytes = self.storage.get(&manifest_path).await?;
        if prefixed_sha256(&manifest_bytes) != reference.manifest_sha256() {
            return Err(invariant_violation(
                "persisted authority manifest checksum mismatch",
            ));
        }
        let manifest: ControlMvpManifest = decode_envelope_limited(
            &manifest_bytes,
            "control-mvp-manifest",
            MAX_CONTROL_JSON_BYTES,
            "control MVP manifest",
        )?;
        manifest.validate(&self.scope, reference.manifest_id())?;
        if manifest.logical_sequence != reference.logical_sequence() {
            return Err(invariant_violation(
                "persisted authority sequence does not match manifest",
            ));
        }

        match reference.reference_kind() {
            PersistedAuthorityKind::StateToken => {
                self.read_at(self.token(
                    reference.manifest_id().to_string(),
                    reference.logical_sequence(),
                ))
                .await
            }
            PersistedAuthorityKind::Checkpoint => {
                let checkpoint_path = reference
                    .checkpoint_path()
                    .ok_or_else(|| validation_failed("checkpoint path is missing"))?;
                let prefix = format!("{}/checkpoints/", self.paths.base_prefix());
                let checkpoint_id = checkpoint_path
                    .strip_prefix(&prefix)
                    .and_then(|path| path.strip_suffix(".json"))
                    .filter(|id| !id.is_empty() && !id.contains('/'))
                    .ok_or_else(|| {
                        validation_failed(
                            "persisted checkpoint path is not canonical for this store",
                        )
                    })?;
                if self.paths.checkpoint_object(checkpoint_id) != checkpoint_path {
                    return Err(validation_failed(
                        "persisted checkpoint path is not canonical for this store",
                    ));
                }
                let checkpoint_bytes = self.storage.get(checkpoint_path).await?;
                if prefixed_sha256(&checkpoint_bytes) != reference.checkpoint_sha256().unwrap_or("")
                {
                    return Err(invariant_violation(
                        "persisted checkpoint checksum mismatch",
                    ));
                }
                let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
                    &checkpoint_bytes,
                    "control-mvp-checkpoint",
                    MAX_CONTROL_JSON_BYTES,
                    "control MVP checkpoint",
                )?;
                checkpoint.validate(&self.scope, checkpoint_id)?;
                if checkpoint.manifest_id != reference.manifest_id()
                    || checkpoint.logical_sequence != reference.logical_sequence()
                    || checkpoint.manifest_checksum_sha256 != sha256_hex(&manifest_bytes)
                {
                    return Err(invariant_violation(
                        "persisted checkpoint does not match authority manifest",
                    ));
                }
                self.read_checkpoint(self.checkpoint_token(checkpoint_id.to_string()))
                    .await
            }
        }
    }
}

#[async_trait]
impl StateRestoreParticipant for ControlMvpRestoreParticipant {
    fn implementation(&self) -> &'static str {
        IMPLEMENTATION
    }

    fn scope(&self) -> &StateScope {
        &self.store.scope
    }

    fn restore_binding_identity(&self) -> StateStoreBindingIdentity {
        self.store.binding_identity.clone()
    }

    async fn plan_restore(
        &self,
        source: &PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: DateTime<Utc>,
    ) -> Result<PersistedRestoreParticipantPlan> {
        Ok(PersistedRestoreParticipantPlan::ControlMvp(
            self.store.build_restore_plan(source, identity, now).await?,
        ))
    }

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> Result<RestoreParticipantInspection> {
        let PersistedRestoreParticipantPlan::ControlMvp(plan) = plan;
        if plan.is_legacy_version() {
            plan.validate_legacy_for_supersession(&self.store)?;
            return Ok(RestoreParticipantInspection::Superseded);
        }
        self.store
            .validate_restore_authority_format(plan.source())?;
        plan.validate(&self.store)?;
        let stable = self.store.load_stable_restore_base(&plan.source).await?;
        let planned_checksum = plan
            .transaction_sha256
            .strip_prefix("sha256:")
            .ok_or_else(|| validation_failed("restore transaction digest is malformed"))?;
        let planned_tx_ref = ControlMvpTxRef {
            tx_id: plan.transaction_id.clone(),
            sequence: plan.result_logical_sequence,
            checksum_sha256: planned_checksum.to_string(),
        };
        let in_lineage = self
            .store
            .tx_in_lineage(&stable.candidate_parent, &planned_tx_ref)
            .await?;
        if in_lineage {
            return self.inspect_visible_restore(plan, planned_checksum).await;
        }

        let version_matches = stable.current_base_kind == plan.current_base_kind
            && stable.current.pointer_version.as_deref() == plan.base_pointer_version.as_deref()
            && stable.writer_epoch == plan.observed_writer_epoch;
        let bytes_match =
            prefixed_sha256(&stable.pointer_bytes) == plan.observed_base_pointer_sha256;
        let manifest_matches =
            stable.candidate_parent.manifest_id.as_deref() == Some(plan.base_manifest_id.as_str());
        if version_matches && bytes_match && manifest_matches {
            let source_values = self
                .store
                .restore_source_values(&plan.source, Utc::now())
                .await?;
            let rendered = self.store.render_restore_candidate(
                &plan.source,
                &source_values,
                &plan.identity,
                &stable,
                plan.required_checkpoint_interval()?,
            )?;
            if plan.base_logical_sequence != stable.candidate_parent.state.logical_sequence
                || rendered.transaction_id != plan.transaction_id
                || prefixed_sha256(&rendered.transaction_bytes) != plan.transaction_sha256
                || rendered.candidate_manifest_id != plan.candidate_manifest_id
                || prefixed_sha256(&rendered.manifest_bytes) != plan.candidate_manifest_sha256
                || prefixed_sha256(&rendered.pointer_bytes) != plan.candidate_pointer_sha256
                || rendered.outbox_record_id != plan.restore_outbox_record_id
                || rendered.result_sequence != plan.result_logical_sequence
            {
                return Err(invariant_violation(
                    "Control MVP Ready restore plan cannot reproduce deterministic candidate bytes",
                ));
            }
            Ok(RestoreParticipantInspection::Ready)
        } else {
            Ok(RestoreParticipantInspection::Superseded)
        }
    }

    async fn apply_restore(
        &self,
        persisted: &PersistedRestoreParticipantPlan,
        now: DateTime<Utc>,
    ) -> Result<RestoreParticipantInspection> {
        let PersistedRestoreParticipantPlan::ControlMvp(plan) = persisted;
        if plan.is_legacy_version() {
            plan.validate_legacy_for_supersession(&self.store)?;
            return Ok(RestoreParticipantInspection::Superseded);
        }
        self.store
            .validate_restore_authority_format(plan.source())?;
        plan.validate(&self.store)?;
        match self.inspect_restore(persisted).await? {
            RestoreParticipantInspection::Ready => {}
            other => return Ok(other),
        }

        let source_values = self.store.restore_source_values(&plan.source, now).await?;
        let stable = self.store.load_stable_restore_base(&plan.source).await?;
        if stable.current_base_kind != plan.current_base_kind
            || stable.current.pointer_version.as_deref() != plan.base_pointer_version.as_deref()
            || prefixed_sha256(&stable.pointer_bytes) != plan.observed_base_pointer_sha256
        {
            return self.inspect_restore(persisted).await;
        }
        let rendered = self.store.render_restore_candidate(
            &plan.source,
            &source_values,
            &plan.identity,
            &stable,
            plan.required_checkpoint_interval()?,
        )?;
        if rendered.transaction_id != plan.transaction_id
            || prefixed_sha256(&rendered.transaction_bytes) != plan.transaction_sha256
            || rendered.candidate_manifest_id != plan.candidate_manifest_id
            || prefixed_sha256(&rendered.manifest_bytes) != plan.candidate_manifest_sha256
            || prefixed_sha256(&rendered.pointer_bytes) != plan.candidate_pointer_sha256
            || rendered.result_sequence != plan.result_logical_sequence
        {
            return Err(invariant_violation(
                "Control MVP restore plan cannot reproduce deterministic candidate bytes",
            ));
        }

        self.write_restore_immutable_artifacts(plan, &rendered)
            .await?;
        let pointer_precondition = match plan.current_base_kind {
            ControlMvpRestoreCurrentBaseKind::Empty => AuthorityWritePrecondition::DoesNotExist,
            ControlMvpRestoreCurrentBaseKind::Pointer => {
                AuthorityWritePrecondition::MatchesVersion(
                    plan.base_pointer_version
                        .clone()
                        .ok_or_else(|| validation_failed("restore base pointer version missing"))?,
                )
            }
        };
        let pointer_write = self
            .store
            .storage
            .put(
                &self.store.paths.current_pointer(),
                rendered.pointer_bytes,
                pointer_precondition,
            )
            .await;
        let inspection = self.inspect_restore(persisted).await;
        match (pointer_write, inspection) {
            (_, Ok(RestoreParticipantInspection::Visible { token, evidence })) => {
                Ok(RestoreParticipantInspection::Visible { token, evidence })
            }
            (_, Ok(RestoreParticipantInspection::Superseded)) => {
                Ok(RestoreParticipantInspection::Superseded)
            }
            (Ok(_), Ok(RestoreParticipantInspection::Ready)) => Err(invariant_violation(
                "Control MVP pointer CAS reported success but restore is not visible",
            )),
            (Err(error), Ok(RestoreParticipantInspection::Ready)) => Err(error.into()),
            (Err(write_error), Err(inspection_error)) => Err(ambiguous_authority_outcome(format!(
                "control MVP restore pointer write could not be reconciled after storage failure: {write_error}; reconciliation inspection failed: {inspection_error}"
            ))),
            (_, Err(error)) => Err(error),
        }
    }
}

#[async_trait]
impl ArcoStateStore for ControlMvpStateStore {
    fn restore_binding_identity(&self) -> Option<StateStoreBindingIdentity> {
        Some(self.binding_identity.clone())
    }

    async fn begin_txn(&self, opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>> {
        Ok(Box::new(self.begin_control_txn(opts).await?))
    }
}

#[async_trait]
impl ArcoStateTxn for ControlMvpTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        Ok(self.get_inner(key))
    }

    async fn scan(&mut self, request: ScanRequest) -> Result<ScanPage> {
        let observed_token = self.base.manifest_id.as_ref().map(|manifest_id| {
            self.store
                .token(manifest_id.clone(), self.base.state.logical_sequence)
        });
        let entries = self.scan_prefix_inner(request.prefix());
        build_scan_page(&self.store.scope, request, observed_token, entries)
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

    async fn commit(self: Box<Self>) -> Result<CommitOutcome> {
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
    writer_epoch: u64,
    layout_generation: u64,
    state: ReplayState,
    base_states: Vec<ControlMvpStateRef>,
    tx_refs: Vec<ControlMvpTxRef>,
}

struct StableRestoreBase {
    current: ControlMvpBase,
    candidate_parent: ControlMvpBase,
    current_base_kind: ControlMvpRestoreCurrentBaseKind,
    writer_epoch: u64,
    pointer_bytes: Bytes,
}

struct RenderedControlMvpRestore {
    transaction_id: String,
    transaction_bytes: Bytes,
    l0_segment_bytes: Bytes,
    l0_index_bytes: Bytes,
    l1_segments: Vec<RenderedControlMvpStateSegment>,
    candidate_manifest_id: String,
    manifest_bytes: Bytes,
    pointer_bytes: Bytes,
    outbox_record_id: String,
    result_sequence: u64,
}

struct RenderedControlMvpStateSegment {
    reference: ControlMvpStateRef,
    bytes: Bytes,
    index_bytes: Bytes,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ControlMvpRestoreNotice {
    restore_id: String,
    participant_attempt: u64,
    domain: String,
    source_logical_sequence: u64,
    result_logical_sequence: u64,
}

#[derive(Debug, Clone, Default)]
struct ReplayState {
    logical_sequence: u64,
    kv: BTreeMap<Vec<u8>, StoredValue>,
    outbox: Vec<ControlMvpProjectionOutboxRecord>,
}

impl ReplayState {
    fn apply_tx(&mut self, tx: &ControlMvpTxObject) -> Result<()> {
        let expected =
            next_logical_sequence(self.logical_sequence, "replaying a control MVP transaction")?;
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
        for trimmed in &tx.outbox_trim {
            let record_id = trimmed.record_id();
            let retained = self
                .outbox
                .iter()
                .enumerate()
                .find(|(_, record)| record.record_id == record_id)
                .map(|(position, record)| (position, record.origin_sequence, record.event_id()));
            let Some((position, retained_sequence, retained_event)) = retained else {
                return Err(invariant_violation(format!(
                    "control MVP outbox trim names record {record_id} that is not present in replayed state"
                )));
            };
            // Identified trims are conditional on the exact event
            // incarnation, so a forged or stale trim cannot delete a record id
            // that was re-staged after the observation it was built from.
            let expected = trimmed.origin_sequence();
            if retained_sequence != Some(expected) {
                return Err(invariant_violation(format!(
                    "control MVP outbox trim names event {} but record {record_id} is retained as \
                     event {}",
                    control_mvp_outbox_event_id(expected, record_id),
                    retained_event.unwrap_or_else(|| "<uncommitted>".to_string()),
                )));
            }
            self.outbox.remove(position);
        }
        for entry in &tx.outbox {
            // Mirror of the stage-time uniqueness validation: honestly
            // produced histories can never contain a duplicate id, so a
            // duplicate observed at replay is a corrupt or forged artifact.
            if self
                .outbox
                .iter()
                .any(|record| record.record_id == entry.record_id)
            {
                return Err(invariant_violation(format!(
                    "control MVP outbox stages record {} that is already present in replayed state",
                    entry.record_id
                )));
            }
            self.outbox.push(entry.to_record_with_sequence(tx.sequence));
        }
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
                .map(ControlMvpOutboxStateEntry::from_record)
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

fn scan_page_budget_exceeded(
    base: &BTreeMap<Vec<u8>, StoredValue>,
    overlay: &BTreeMap<Vec<u8>, StoredValue>,
    through: &[u8],
    max_rows: usize,
    max_bytes: usize,
) -> Result<bool> {
    let keys = base
        .keys()
        .chain(overlay.keys())
        .filter(|key| key.as_slice() <= through)
        .collect::<BTreeSet<_>>();
    let mut rows = 0_usize;
    let mut bytes = 0_usize;
    for key in keys {
        let value = overlay.get(key).or_else(|| base.get(key)).ok_or_else(|| {
            invariant_violation("control MVP scan merge key lost its selected value")
        })?;
        if value.tombstone {
            continue;
        }
        rows = rows.saturating_add(1);
        bytes = bytes
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.bytes.len()))
            .ok_or_else(|| validation_failed("control MVP scan decoded byte count overflow"))?;
        if rows > max_rows || bytes > max_bytes {
            return Ok(true);
        }
    }
    Ok(false)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    format_version: u32,
    implementation: String,
    scope: ControlMvpScopeDoc,
    manifest_id: String,
    logical_sequence: u64,
    manifest_checksum_sha256: String,
    writer_epoch: u64,
}

impl ControlMvpPointer {
    fn validate(&self, scope: &StateScope) -> Result<()> {
        if self.format_version != CONTROL_MVP_FORMAT_VERSION {
            return Err(invariant_violation(
                "control MVP pointer format version mismatch",
            ));
        }
        if self.implementation != IMPLEMENTATION {
            return Err(invariant_violation(
                "control MVP pointer implementation mismatch",
            ));
        }
        if !self.scope.matches_scope(scope) {
            return Err(validation_failed("control MVP pointer scope mismatch"));
        }
        if self.writer_epoch == u64::MAX {
            return Err(terminal_published_writer_epoch());
        }
        Ok(())
    }
}

/// Reference to an immutable state-snapshot object, bound by raw-byte checksum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ControlMvpStateRef {
    state_id: String,
    logical_sequence: u64,
    checksum_sha256: String,
    index_checksum_sha256: String,
    min_key_hex: Option<String>,
    max_key_hex: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ControlMvpSegmentLevel {
    L0,
    L1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ControlMvpSegmentRef {
    segment_id: String,
    level: ControlMvpSegmentLevel,
    logical_sequence: u64,
    checksum_sha256: String,
    index_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlMvpSegmentIndex {
    format_version: u32,
    implementation: String,
    scope: ControlMvpScopeDoc,
    segment_id: String,
    level: ControlMvpSegmentLevel,
    logical_sequence: u64,
    row_count: u64,
    segment_size_bytes: u64,
    min_key_hex: Option<String>,
    max_key_hex: Option<String>,
    min_key_utf8: Option<String>,
    max_key_utf8: Option<String>,
    record_batch_offsets: Vec<u64>,
    bloom_bits_hex: String,
    segment_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlMvpSegmentRow {
    record_kind: u8,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    generation: u64,
    tombstone: bool,
    logical_sequence: u64,
    logical_ordinal: u64,
    origin_sequence: Option<u64>,
}

/// Immutable materialized replay state anchored to one manifest.
#[derive(Debug)]
struct ControlMvpStateObject {
    format_version: u32,
    implementation: String,
    scope: ControlMvpScopeDoc,
    state_id: String,
    logical_sequence: u64,
    entries: Vec<ReplayStateDigestEntry>,
    outbox_start_ordinal: Option<u64>,
    outbox: Vec<ControlMvpOutboxStateEntry>,
}

impl ControlMvpStateObject {
    fn from_replay(state: &ReplayState, state_id: String, scope: &StateScope) -> Self {
        Self {
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(scope),
            state_id,
            logical_sequence: state.logical_sequence,
            entries: state
                .kv
                .iter()
                .map(|(key, value)| ReplayStateDigestEntry {
                    key: key.clone(),
                    generation: value.generation,
                    value: (!value.tombstone).then(|| value.bytes.to_vec()),
                })
                .collect(),
            outbox_start_ordinal: (!state.outbox.is_empty()).then_some(0),
            outbox: state
                .outbox
                .iter()
                .map(ControlMvpOutboxStateEntry::from_record)
                .collect(),
        }
    }

    fn validate(&self, scope: &StateScope, reference: &ControlMvpStateRef) -> Result<()> {
        if self.format_version != CONTROL_MVP_FORMAT_VERSION {
            return Err(invariant_violation(
                "control MVP state snapshot format version mismatch",
            ));
        }
        if self.implementation != IMPLEMENTATION {
            return Err(invariant_violation(
                "control MVP state snapshot implementation mismatch",
            ));
        }
        if !self.scope.matches_scope(scope) {
            return Err(validation_failed(
                "control MVP state snapshot scope mismatch",
            ));
        }
        if self.state_id != reference.state_id {
            return Err(invariant_violation(
                "control MVP state snapshot id does not match reference",
            ));
        }
        if self.logical_sequence != reference.logical_sequence {
            return Err(invariant_violation(
                "control MVP state snapshot sequence does not match reference",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpManifest {
    format_version: u32,
    implementation: String,
    scope: ControlMvpScopeDoc,
    manifest_id: String,
    logical_sequence: u64,
    base_manifest_id: Option<String>,
    writer_epoch: u64,
    layout_generation: u64,
    base_states: Vec<ControlMvpStateRef>,
    anchor_states: Vec<ControlMvpStateRef>,
    tx_refs: Vec<ControlMvpTxRef>,
    state_checksum_sha256: String,
    maintenance_intent: Option<LayoutMaintenanceIntentV1>,
}

impl ControlMvpManifest {
    fn validate(&self, scope: &StateScope, expected_manifest_id: &str) -> Result<()> {
        if self.format_version != CONTROL_MVP_FORMAT_VERSION {
            return Err(invariant_violation(
                "control MVP manifest format version mismatch",
            ));
        }
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
        let base_sequence = self.validate_base_states()?;
        self.validate_replay_suffix(base_sequence)?;
        self.validate_anchor_states()?;
        self.validate_maintenance_intent(scope)?;
        Ok(())
    }

    fn validate_base_states(&self) -> Result<Option<u64>> {
        let base_sequence = self
            .base_states
            .first()
            .map(|reference| reference.logical_sequence);
        if self
            .base_states
            .iter()
            .any(|reference| Some(reference.logical_sequence) != base_sequence)
        {
            return Err(invariant_violation(
                "control MVP manifest base L1 segments mix logical sequences",
            ));
        }
        let mut prior_max = None;
        for reference in &self.base_states {
            match state_reference_key_bounds(reference)? {
                Some((minimum, maximum)) => {
                    if prior_max
                        .as_ref()
                        .is_some_and(|prior: &Vec<u8>| prior.as_slice() >= minimum.as_slice())
                    {
                        return Err(invariant_violation(
                            "control MVP manifest L1 key bounds overlap or are out of order",
                        ));
                    }
                    prior_max = Some(maximum);
                }
                None if self.base_states.len() == 1 => {}
                None => {
                    return Err(invariant_violation(
                        "control MVP manifest has a keyless L1 shard in a multi-shard layout",
                    ));
                }
            }
        }
        Ok(base_sequence)
    }

    fn validate_replay_suffix(&self, base_sequence: Option<u64>) -> Result<()> {
        if self.tx_refs.is_empty() {
            if base_sequence != Some(self.logical_sequence) {
                return Err(invariant_violation(
                    "control MVP suffix-free manifest must select an equivalent L1 state",
                ));
            }
        } else {
            let expected_first = match base_sequence {
                Some(sequence) => {
                    next_logical_sequence(sequence, "validating a control MVP manifest suffix")?
                }
                None => 1,
            };
            if self.tx_refs.first().map_or(0, |tx_ref| tx_ref.sequence) != expected_first {
                return Err(invariant_violation(
                    "control MVP manifest suffix does not start at its replay anchor",
                ));
            }
            if self.tx_refs.last().map_or(0, |tx_ref| tx_ref.sequence) != self.logical_sequence {
                return Err(invariant_violation(
                    "control MVP manifest sequence does not match selected tx refs",
                ));
            }
        }
        Ok(())
    }

    fn validate_anchor_states(&self) -> Result<()> {
        for (ordinal, anchor) in self.anchor_states.iter().enumerate() {
            if anchor.logical_sequence != self.logical_sequence
                || anchor.state_id != state_segment_id_for_manifest(&self.manifest_id, ordinal)
            {
                return Err(invariant_violation(
                    "control MVP manifest anchor segment does not match manifest identity",
                ));
            }
        }
        Ok(())
    }

    fn validate_maintenance_intent(&self, scope: &StateScope) -> Result<()> {
        match &self.maintenance_intent {
            Some(intent)
                if self.tx_refs.len() >= L0_MAINTENANCE_INTENT_THRESHOLD
                    && intent.source_scope() == scope
                    && intent.source_logical_sequence() == self.logical_sequence
                    && intent.source_authority_manifest_id() == self.manifest_id
                    && self
                        .layout_generation
                        .checked_add(1)
                        .is_some_and(|next| next == intent.layout_generation()) => {}
            Some(_) => {
                return Err(invariant_violation(
                    "control MVP layout-maintenance intent does not match its source manifest",
                ));
            }
            None if self.tx_refs.len() >= L0_MAINTENANCE_INTENT_THRESHOLD => {
                return Err(invariant_violation(
                    "control MVP manifest above the L0 maintenance threshold has no intent",
                ));
            }
            None => {}
        }
        Ok(())
    }

    /// Returns the replay anchor and transaction suffix a successor manifest
    /// must extend: a fresh suffix on this manifest's own snapshot when one
    /// was anchored, otherwise this manifest's anchor and suffix.
    fn successor_anchor(&self) -> (Vec<ControlMvpStateRef>, Vec<ControlMvpTxRef>) {
        if self.anchor_states.is_empty() {
            (self.base_states.clone(), self.tx_refs.clone())
        } else {
            (self.anchor_states.clone(), Vec::new())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    writer_epoch: u64,
    request_id: Option<String>,
    l0_segment: ControlMvpSegmentRef,
    /// Hydrated only from the checksummed Arrow L0 segment. Transaction JSON
    /// contains metadata and immutable segment references, never state data.
    #[serde(skip)]
    writes: Vec<ControlMvpWriteEntry>,
    #[serde(skip)]
    outbox: Vec<ControlMvpOutboxEntry>,
    /// Outbox events removed from replayed state by this transaction.
    /// Consumers trim only events they have durably acknowledged; the store
    /// enforces that every trimmed event incarnation exists at apply time and
    /// fails closed otherwise.
    #[serde(skip)]
    outbox_trim: Vec<ControlMvpOutboxTrimEntry>,
}

/// Exact event incarnation removed by a v4 L0 transaction row.
#[derive(Debug, Clone)]
struct ControlMvpOutboxTrimEntry {
    record_id: String,
    origin_sequence: u64,
}

impl ControlMvpOutboxTrimEntry {
    fn record_id(&self) -> &str {
        &self.record_id
    }

    const fn origin_sequence(&self) -> u64 {
        self.origin_sequence
    }
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
        if self.l0_segment.segment_id != self.tx_id
            || self.l0_segment.level != ControlMvpSegmentLevel::L0
            || self.l0_segment.logical_sequence != self.sequence
            || self.l0_segment.checksum_sha256.len() != 64
            || self.l0_segment.index_checksum_sha256.len() != 64
        {
            return Err(invariant_violation(
                "control MVP transaction L0 segment reference is invalid",
            ));
        }
        Ok(())
    }

    fn hydrate_from_segment_rows(&mut self, rows: Vec<ControlMvpSegmentRow>) -> Result<()> {
        let mut writes = Vec::new();
        let mut outbox = Vec::new();
        let mut outbox_trim = Vec::new();
        for row in rows {
            if row.logical_sequence != self.sequence {
                return Err(invariant_violation(
                    "control MVP L0 row sequence does not match transaction sequence",
                ));
            }
            match row.record_kind {
                SEGMENT_RECORD_KV => {
                    if row.origin_sequence.is_some() || row.generation != self.sequence {
                        return Err(invariant_violation(
                            "control MVP L0 key/value row metadata is invalid",
                        ));
                    }
                    writes.push((
                        row.logical_ordinal,
                        ControlMvpWriteEntry {
                            key: row.key,
                            generation: row.generation,
                            value: row.value,
                        },
                    ));
                }
                SEGMENT_RECORD_OUTBOX => {
                    if row.tombstone
                        || row.generation != 0
                        || row.origin_sequence != Some(self.sequence)
                    {
                        return Err(invariant_violation(
                            "control MVP L0 outbox row metadata is invalid",
                        ));
                    }
                    let record_id = String::from_utf8(row.key).map_err(|error| {
                        segment_serialization_error("decode L0 outbox record id", error)
                    })?;
                    let payload = row.value.ok_or_else(|| {
                        invariant_violation("control MVP L0 outbox row has no payload")
                    })?;
                    outbox.push((
                        row.logical_ordinal,
                        ControlMvpOutboxEntry { record_id, payload },
                    ));
                }
                SEGMENT_RECORD_OUTBOX_TRIM => {
                    if !row.tombstone
                        || row.generation != 0
                        || row.value.is_some()
                        || row.origin_sequence.is_none()
                    {
                        return Err(invariant_violation(
                            "control MVP L0 outbox-trim row metadata is invalid",
                        ));
                    }
                    let record_id = String::from_utf8(row.key).map_err(|error| {
                        segment_serialization_error("decode L0 outbox trim record id", error)
                    })?;
                    let entry = ControlMvpOutboxTrimEntry {
                        record_id,
                        origin_sequence: row.origin_sequence.ok_or_else(|| {
                            invariant_violation(
                                "control MVP L0 outbox-trim row is missing origin sequence",
                            )
                        })?,
                    };
                    outbox_trim.push((row.logical_ordinal, entry));
                }
                _ => {
                    return Err(invariant_violation(
                        "control MVP L0 segment contains an unknown row kind",
                    ));
                }
            }
        }
        sort_and_validate_segment_ordinals(&mut writes, "key/value")?;
        sort_and_validate_segment_ordinals(&mut outbox, "outbox")?;
        sort_and_validate_segment_ordinals(&mut outbox_trim, "outbox trim")?;

        validate_unique_hydrated_rows(&writes, &outbox, &outbox_trim)?;

        self.writes = writes.into_iter().map(|(_, entry)| entry).collect();
        self.outbox = outbox.into_iter().map(|(_, entry)| entry).collect();
        self.outbox_trim = outbox_trim.into_iter().map(|(_, entry)| entry).collect();
        Ok(())
    }
}

fn validate_unique_hydrated_rows(
    writes: &[(u64, ControlMvpWriteEntry)],
    outbox: &[(u64, ControlMvpOutboxEntry)],
    outbox_trim: &[(u64, ControlMvpOutboxTrimEntry)],
) -> Result<()> {
    let write_keys = writes
        .iter()
        .map(|(_, entry)| entry.key.as_slice())
        .collect::<BTreeSet<_>>();
    if write_keys.len() != writes.len() {
        return Err(invariant_violation(
            "control MVP L0 segment contains duplicate key/value rows",
        ));
    }
    let outbox_ids = outbox
        .iter()
        .map(|(_, entry)| entry.record_id.as_str())
        .collect::<BTreeSet<_>>();
    if outbox_ids.len() != outbox.len() {
        return Err(invariant_violation(
            "control MVP L0 segment contains duplicate outbox rows",
        ));
    }
    let trim_ids = outbox_trim
        .iter()
        .map(|(_, entry)| entry.record_id())
        .collect::<BTreeSet<_>>();
    if trim_ids.len() != outbox_trim.len() {
        return Err(invariant_violation(
            "control MVP L0 segment contains duplicate outbox-trim rows",
        ));
    }
    Ok(())
}

fn sort_and_validate_segment_ordinals<T>(
    entries: &mut [(u64, T)],
    record_kind: &str,
) -> Result<()> {
    entries.sort_by_key(|(ordinal, _)| *ordinal);
    for (expected, (actual, _)) in entries.iter().enumerate() {
        let expected = u64::try_from(expected)
            .map_err(|error| segment_serialization_error("convert L0 logical ordinal", error))?;
        if *actual != expected {
            return Err(invariant_violation(format!(
                "control MVP L0 {record_kind} logical ordinals are not contiguous"
            )));
        }
    }
    Ok(())
}

fn unwritten_l0_segment_ref(segment_id: &str, logical_sequence: u64) -> ControlMvpSegmentRef {
    ControlMvpSegmentRef {
        segment_id: segment_id.to_string(),
        level: ControlMvpSegmentLevel::L0,
        logical_sequence,
        checksum_sha256: String::new(),
        index_checksum_sha256: String::new(),
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

    fn to_record_with_sequence(&self, origin_sequence: u64) -> ControlMvpProjectionOutboxRecord {
        ControlMvpProjectionOutboxRecord {
            record_id: self.record_id.clone(),
            payload: Bytes::from(self.payload.clone()),
            origin_sequence: Some(origin_sequence),
        }
    }
}

/// Sequenced outbox entry as persisted in state snapshots and hashed into
/// replay-state digests. Unlike the transaction wire entry, it carries the
/// provenance sequence stamped at replay time.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpOutboxStateEntry {
    record_id: String,
    payload: Vec<u8>,
    origin_sequence: Option<u64>,
}

impl ControlMvpOutboxStateEntry {
    fn from_record(record: &ControlMvpProjectionOutboxRecord) -> Self {
        Self {
            record_id: record.record_id.clone(),
            payload: record.payload.to_vec(),
            origin_sequence: record.origin_sequence,
        }
    }

    fn to_record(&self) -> ControlMvpProjectionOutboxRecord {
        ControlMvpProjectionOutboxRecord {
            record_id: self.record_id.clone(),
            payload: Bytes::from(self.payload.clone()),
            origin_sequence: self.origin_sequence,
        }
    }
}

const SEGMENT_RECORD_KV: u8 = 0;
const SEGMENT_RECORD_OUTBOX: u8 = 1;
const SEGMENT_RECORD_OUTBOX_TRIM: u8 = 2;
const SEGMENT_BLOOM_BYTES: usize = 32;

fn segment_rows_for_state(state: &ControlMvpStateObject) -> Vec<ControlMvpSegmentRow> {
    let mut rows = state
        .entries
        .iter()
        .enumerate()
        .map(|(ordinal, entry)| ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: entry.key.clone(),
            value: entry.value.clone(),
            generation: entry.generation,
            tombstone: entry.value.is_none(),
            logical_sequence: state.logical_sequence,
            logical_ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
            origin_sequence: None,
        })
        .chain(state.outbox.iter().enumerate().map(|(ordinal, entry)| {
            ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_OUTBOX,
                key: entry.record_id.as_bytes().to_vec(),
                value: Some(entry.payload.clone()),
                generation: 0,
                tombstone: false,
                logical_sequence: state.logical_sequence,
                logical_ordinal: state
                    .outbox_start_ordinal
                    .unwrap_or(0)
                    .checked_add(u64::try_from(ordinal).unwrap_or(u64::MAX))
                    .unwrap_or(u64::MAX),
                origin_sequence: entry.origin_sequence,
            }
        }))
        .collect::<Vec<_>>();
    sort_segment_rows(&mut rows);
    rows
}

fn state_object_from_segment_rows(
    reference: &ControlMvpStateRef,
    rows: Vec<ControlMvpSegmentRow>,
    scope: &StateScope,
) -> Result<ControlMvpStateObject> {
    let mut entries = Vec::new();
    let mut outbox = Vec::new();
    for row in rows {
        if row.logical_sequence != reference.logical_sequence {
            return Err(invariant_violation(
                "control MVP L1 segment row sequence does not match its reference",
            ));
        }
        match row.record_kind {
            SEGMENT_RECORD_KV => {
                if row.generation == 0 || row.generation > reference.logical_sequence {
                    return Err(invariant_violation(
                        "control MVP L1 segment contains an invalid key generation",
                    ));
                }
                entries.push(ReplayStateDigestEntry {
                    key: row.key,
                    generation: row.generation,
                    value: row.value,
                });
            }
            SEGMENT_RECORD_OUTBOX => {
                let origin_sequence = row.origin_sequence.ok_or_else(|| {
                    invariant_violation("control MVP L1 outbox row is missing origin sequence")
                })?;
                if row.generation != 0
                    || origin_sequence == 0
                    || origin_sequence > reference.logical_sequence
                {
                    return Err(invariant_violation(
                        "control MVP L1 outbox row origin metadata is invalid",
                    ));
                }
                let record_id = String::from_utf8(row.key).map_err(|error| {
                    segment_serialization_error("decode L1 outbox record id", error)
                })?;
                let payload = row.value.ok_or_else(|| {
                    invariant_violation("control MVP L1 outbox row has no payload")
                })?;
                outbox.push((
                    row.logical_ordinal,
                    ControlMvpOutboxStateEntry {
                        record_id,
                        payload,
                        origin_sequence: Some(origin_sequence),
                    },
                ));
            }
            SEGMENT_RECORD_OUTBOX_TRIM => {
                return Err(invariant_violation(
                    "control MVP consolidated L1 state contains an outbox trim row",
                ));
            }
            _ => {
                return Err(invariant_violation(
                    "control MVP consolidated L1 state contains an unknown row kind",
                ));
            }
        }
    }
    outbox.sort_by_key(|(ordinal, _)| *ordinal);
    let outbox_start_ordinal = outbox.first().map(|(ordinal, _)| *ordinal);
    for (expected, (actual, _)) in outbox.iter().enumerate() {
        let expected =
            outbox_start_ordinal
                .unwrap_or(0)
                .checked_add(u64::try_from(expected).map_err(|error| {
                    segment_serialization_error("convert L1 outbox ordinal", error)
                })?)
                .ok_or_else(|| invariant_violation("control MVP L1 outbox ordinal overflow"))?;
        if *actual != expected {
            return Err(invariant_violation(
                "control MVP L1 outbox logical ordinals are not contiguous",
            ));
        }
    }
    Ok(ControlMvpStateObject {
        format_version: CONTROL_MVP_FORMAT_VERSION,
        implementation: IMPLEMENTATION.to_string(),
        scope: ControlMvpScopeDoc::from(scope),
        state_id: reference.state_id.clone(),
        logical_sequence: reference.logical_sequence,
        entries,
        outbox_start_ordinal,
        outbox: outbox.into_iter().map(|(_, entry)| entry).collect(),
    })
}

fn segment_rows_for_tx(tx: &ControlMvpTxObject) -> Vec<ControlMvpSegmentRow> {
    let mut rows = tx
        .writes
        .iter()
        .enumerate()
        .map(|(ordinal, write)| ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: write.key.clone(),
            value: write.value.clone(),
            generation: write.generation,
            tombstone: write.value.is_none(),
            logical_sequence: tx.sequence,
            logical_ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
            origin_sequence: None,
        })
        .chain(
            tx.outbox
                .iter()
                .enumerate()
                .map(|(ordinal, entry)| ControlMvpSegmentRow {
                    record_kind: SEGMENT_RECORD_OUTBOX,
                    key: entry.record_id.as_bytes().to_vec(),
                    value: Some(entry.payload.clone()),
                    generation: 0,
                    tombstone: false,
                    logical_sequence: tx.sequence,
                    logical_ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
                    origin_sequence: Some(tx.sequence),
                }),
        )
        .chain(
            tx.outbox_trim
                .iter()
                .enumerate()
                .map(|(ordinal, entry)| ControlMvpSegmentRow {
                    record_kind: SEGMENT_RECORD_OUTBOX_TRIM,
                    key: entry.record_id().as_bytes().to_vec(),
                    value: None,
                    generation: 0,
                    tombstone: true,
                    logical_sequence: tx.sequence,
                    logical_ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
                    origin_sequence: Some(entry.origin_sequence()),
                }),
        )
        .collect::<Vec<_>>();
    sort_segment_rows(&mut rows);
    rows
}

fn sort_segment_rows(rows: &mut [ControlMvpSegmentRow]) {
    rows.sort_by(|left, right| {
        (left.record_kind, left.key.as_slice()).cmp(&(right.record_kind, right.key.as_slice()))
    });
}

const fn half_segment_limits(limits: SegmentLimits) -> SegmentLimits {
    SegmentLimits {
        bytes: if limits.bytes < 2 {
            1
        } else {
            limits.bytes / 2
        },
        index_bytes: if limits.index_bytes < 2 {
            1
        } else {
            limits.index_bytes / 2
        },
        rows: if limits.rows < 2 { 1 } else { limits.rows / 2 },
    }
}

fn partition_state_rows(
    rows: &[ControlMvpSegmentRow],
    logical_sequence: u64,
    manifest_id: &str,
    scope: &StateScope,
    target_limits: SegmentLimits,
) -> Result<Vec<Vec<ControlMvpSegmentRow>>> {
    let initial = if rows.is_empty() {
        vec![Vec::new()]
    } else {
        rows.chunks(target_limits.rows)
            .map(<[ControlMvpSegmentRow]>::to_vec)
            .collect()
    };
    let probe_id = state_segment_id_for_manifest(manifest_id, 999_999);
    let mut shards = Vec::new();
    for rows in initial {
        partition_state_row_chunk(
            rows,
            logical_sequence,
            &probe_id,
            scope,
            target_limits,
            &mut shards,
        )?;
    }
    Ok(shards)
}

fn partition_state_row_chunk(
    mut rows: Vec<ControlMvpSegmentRow>,
    logical_sequence: u64,
    probe_id: &str,
    scope: &StateScope,
    target_limits: SegmentLimits,
    shards: &mut Vec<Vec<ControlMvpSegmentRow>>,
) -> Result<()> {
    match encode_segment(
        probe_id,
        ControlMvpSegmentLevel::L1,
        logical_sequence,
        scope,
        &rows,
        target_limits,
    ) {
        Ok(_) => {
            shards.push(rows);
            Ok(())
        }
        Err(CatalogError::MaintenanceBackpressure { .. }) if rows.len() > 1 => {
            let right = rows.split_off(rows.len() / 2);
            partition_state_row_chunk(
                rows,
                logical_sequence,
                probe_id,
                scope,
                target_limits,
                shards,
            )?;
            partition_state_row_chunk(
                right,
                logical_sequence,
                probe_id,
                scope,
                target_limits,
                shards,
            )
        }
        Err(error) => Err(error),
    }
}

fn encode_segment(
    segment_id: &str,
    level: ControlMvpSegmentLevel,
    logical_sequence: u64,
    scope: &StateScope,
    rows: &[ControlMvpSegmentRow],
    limits: SegmentLimits,
) -> Result<(Bytes, Bytes, ControlMvpSegmentRef)> {
    if rows.len() > limits.rows {
        return Err(segment_capacity_error(
            level,
            "control MVP segment exceeds the supported row limit",
        ));
    }
    // Keep the IPC schema metadata-free. Arrow stores schema metadata in a
    // hash map, whose iteration order is not a stable serialization contract.
    // Authority identity is checksum-bound in the deterministic JSON index.
    let schema = Arc::new(control_mvp_segment_schema());

    let mut record_kinds = UInt8Builder::new();
    let mut keys = BinaryBuilder::new();
    let mut values = BinaryBuilder::new();
    let mut generations = UInt64Builder::new();
    let mut tombstones = BooleanBuilder::new();
    let mut logical_sequences = UInt64Builder::new();
    let mut logical_ordinals = UInt64Builder::new();
    let mut origin_sequences = UInt64Builder::new();
    for row in rows {
        record_kinds.append_value(row.record_kind);
        keys.append_value(&row.key);
        if let Some(value) = &row.value {
            values.append_value(value);
        } else {
            values.append_null();
        }
        generations.append_value(row.generation);
        tombstones.append_value(row.tombstone);
        logical_sequences.append_value(row.logical_sequence);
        logical_ordinals.append_value(row.logical_ordinal);
        if let Some(origin_sequence) = row.origin_sequence {
            origin_sequences.append_value(origin_sequence);
        } else {
            origin_sequences.append_null();
        }
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(record_kinds.finish()),
            Arc::new(keys.finish()),
            Arc::new(values.finish()),
            Arc::new(generations.finish()),
            Arc::new(tombstones.finish()),
            Arc::new(logical_sequences.finish()),
            Arc::new(logical_ordinals.finish()),
            Arc::new(origin_sequences.finish()),
        ],
    )
    .map_err(|error| segment_serialization_error("build Arrow record batch", error))?;
    let mut output = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut output, schema.as_ref())
            .map_err(|error| segment_serialization_error("create Arrow IPC writer", error))?;
        writer
            .write(&batch)
            .map_err(|error| segment_serialization_error("write Arrow IPC batch", error))?;
        writer
            .finish()
            .map_err(|error| segment_serialization_error("finish Arrow IPC segment", error))?;
    }
    if output.len() > limits.bytes {
        return Err(segment_capacity_error(
            level,
            "control MVP segment exceeds the supported byte limit",
        ));
    }
    let segment_bytes = Bytes::from(output);
    let segment_checksum_sha256 = sha256_hex(&segment_bytes);
    let index = build_segment_index(
        segment_id,
        level,
        logical_sequence,
        scope,
        rows,
        &segment_bytes,
        segment_checksum_sha256.clone(),
    )?;
    let index_bytes = encode_json(&index, "control MVP segment index")?;
    if index_bytes.len() > limits.index_bytes {
        return Err(segment_capacity_error(
            level,
            "control MVP segment index exceeds the supported byte limit",
        ));
    }
    let reference = ControlMvpSegmentRef {
        segment_id: segment_id.to_string(),
        level,
        logical_sequence,
        checksum_sha256: segment_checksum_sha256,
        index_checksum_sha256: sha256_hex(&index_bytes),
    };
    Ok((segment_bytes, index_bytes, reference))
}

fn control_mvp_segment_schema() -> Schema {
    Schema::new(vec![
        Field::new("record_kind", DataType::UInt8, false),
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("generation", DataType::UInt64, false),
        Field::new("tombstone", DataType::Boolean, false),
        Field::new("logical_sequence", DataType::UInt64, false),
        Field::new("logical_ordinal", DataType::UInt64, false),
        Field::new("origin_sequence", DataType::UInt64, true),
    ])
}

fn build_segment_index(
    segment_id: &str,
    level: ControlMvpSegmentLevel,
    logical_sequence: u64,
    scope: &StateScope,
    rows: &[ControlMvpSegmentRow],
    segment_bytes: &[u8],
    segment_checksum_sha256: String,
) -> Result<ControlMvpSegmentIndex> {
    let keys = rows
        .iter()
        .filter(|row| row.record_kind == SEGMENT_RECORD_KV)
        .map(|row| row.key.as_slice())
        .collect::<Vec<_>>();
    let min_key = keys.first().copied();
    let max_key = keys.last().copied();
    Ok(ControlMvpSegmentIndex {
        format_version: CONTROL_MVP_FORMAT_VERSION,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.into(),
        segment_id: segment_id.to_string(),
        level,
        logical_sequence,
        row_count: u64::try_from(rows.len())
            .map_err(|error| segment_serialization_error("convert segment row count", error))?,
        segment_size_bytes: u64::try_from(segment_bytes.len())
            .map_err(|error| segment_serialization_error("convert segment byte size", error))?,
        min_key_hex: min_key.map(hex::encode),
        max_key_hex: max_key.map(hex::encode),
        min_key_utf8: min_key.and_then(|key| std::str::from_utf8(key).ok().map(str::to_string)),
        max_key_utf8: max_key.and_then(|key| std::str::from_utf8(key).ok().map(str::to_string)),
        record_batch_offsets: arrow_record_batch_offsets(segment_bytes)?,
        bloom_bits_hex: bloom_bits_hex(&keys),
        segment_checksum_sha256,
    })
}

fn arrow_record_batch_offsets(bytes: &[u8]) -> Result<Vec<u64>> {
    Ok(preflight_arrow_segment(bytes)?.record_batch_offsets)
}

#[derive(Debug)]
struct ArrowSegmentPreflight {
    record_batch_offsets: Vec<u64>,
}

#[allow(clippy::too_many_lines)]
fn preflight_arrow_segment(bytes: &[u8]) -> Result<ArrowSegmentPreflight> {
    if bytes.len() > MAX_SEGMENT_BYTES {
        return Err(invariant_violation(
            "control MVP Arrow segment exceeds the supported byte limit",
        ));
    }
    let trailer_start = bytes
        .len()
        .checked_sub(10)
        .ok_or_else(|| invariant_violation("control MVP Arrow segment trailer is missing"))?;
    let trailer_bytes = bytes
        .get(trailer_start..)
        .ok_or_else(|| invariant_violation("control MVP Arrow segment trailer is missing"))?;
    let trailer: [u8; 10] = trailer_bytes
        .try_into()
        .map_err(|error| segment_serialization_error("read Arrow IPC trailer", error))?;
    let footer_len = arrow::ipc::reader::read_footer_length(trailer)
        .map_err(|error| segment_serialization_error("read Arrow IPC footer length", error))?;
    let footer_start = trailer_start
        .checked_sub(footer_len)
        .ok_or_else(|| invariant_violation("control MVP Arrow segment footer is out of bounds"))?;
    let footer_bytes = bytes
        .get(footer_start..trailer_start)
        .ok_or_else(|| invariant_violation("control MVP Arrow segment footer is out of bounds"))?;
    let verifier_options = segment_verifier_options();
    let footer =
        arrow::ipc::root_as_footer_with_opts(&verifier_options, footer_bytes).map_err(|error| {
            invariant_violation(format!("control MVP Arrow footer is invalid: {error}"))
        })?;
    if footer.version() != MetadataVersion::V5 {
        return Err(invariant_violation(
            "control MVP Arrow segment metadata version is unsupported",
        ));
    }
    if footer
        .dictionaries()
        .is_some_and(|dictionaries| !dictionaries.is_empty())
    {
        return Err(invariant_violation(
            "control MVP Arrow segment dictionaries are unsupported",
        ));
    }
    if footer
        .custom_metadata()
        .is_some_and(|metadata| !metadata.is_empty())
    {
        return Err(invariant_violation(
            "control MVP Arrow footer metadata is unsupported",
        ));
    }
    let ipc_schema = footer
        .schema()
        .ok_or_else(|| invariant_violation("control MVP Arrow segment footer has no schema"))?;
    if ipc_schema
        .features()
        .is_some_and(|features| !features.is_empty())
    {
        return Err(invariant_violation(
            "control MVP Arrow schema features are unsupported",
        ));
    }
    let decoded_schema = catch_unwind(AssertUnwindSafe(|| {
        arrow::ipc::convert::fb_to_schema(ipc_schema)
    }))
    .map_err(|_| invariant_violation("control MVP Arrow schema conversion panicked"))?;
    if decoded_schema != control_mvp_segment_schema() {
        return Err(invariant_violation(
            "control MVP Arrow segment schema does not match the supported schema",
        ));
    }
    let batches = footer.recordBatches().ok_or_else(|| {
        invariant_violation("control MVP Arrow segment footer has no record batches")
    })?;
    if batches.len() != 1 {
        return Err(invariant_violation(
            "control MVP segment must contain exactly one record batch",
        ));
    }
    let mut offsets = Vec::with_capacity(1);
    let footer_start = u64::try_from(footer_start)
        .map_err(|error| segment_serialization_error("convert Arrow footer offset", error))?;
    for block in batches {
        let offset = u64::try_from(block.offset())
            .map_err(|_| invariant_violation("control MVP Arrow batch offset is negative"))?;
        let metadata_length = u64::try_from(block.metaDataLength()).map_err(|_| {
            invariant_violation("control MVP Arrow batch metadata length is negative")
        })?;
        let body_length = u64::try_from(block.bodyLength())
            .map_err(|_| invariant_violation("control MVP Arrow batch body length is negative"))?;
        let block_end = offset
            .checked_add(metadata_length)
            .and_then(|end| end.checked_add(body_length))
            .ok_or_else(|| invariant_violation("control MVP Arrow record-batch bounds overflow"))?;
        if offset == 0 || metadata_length == 0 || block_end > footer_start {
            return Err(invariant_violation(
                "control MVP Arrow record-batch block is out of bounds",
            ));
        }
        offsets.push(offset);
    }
    if offsets.is_empty() {
        return Err(invariant_violation(
            "control MVP Arrow segment footer has no record-batch offsets",
        ));
    }
    Ok(ArrowSegmentPreflight {
        record_batch_offsets: offsets,
    })
}

fn segment_verifier_options() -> VerifierOptions {
    VerifierOptions {
        max_depth: MAX_SEGMENT_FOOTER_DEPTH,
        max_tables: MAX_SEGMENT_FOOTER_TABLES,
        max_apparent_size: MAX_SEGMENT_FOOTER_APPARENT_BYTES,
        ignore_missing_null_terminator: false,
    }
}

fn bloom_bits_hex(keys: &[&[u8]]) -> String {
    let mut bits = [0_u8; SEGMENT_BLOOM_BYTES];
    for key in keys {
        let digest = Sha256::digest(key);
        for byte in digest.iter().take(3) {
            let bit = usize::from(*byte) % (SEGMENT_BLOOM_BYTES * 8);
            if let Some(slot) = bits.get_mut(bit / 8) {
                *slot |= 1 << (bit % 8);
            }
        }
    }
    hex::encode(bits)
}

fn decode_segment_rows(
    bytes: &[u8],
    index_bytes: &[u8],
    reference: &ControlMvpSegmentRef,
    scope: &StateScope,
) -> Result<Vec<ControlMvpSegmentRow>> {
    if bytes.len() > MAX_SEGMENT_BYTES {
        return Err(invariant_violation(
            "control MVP Arrow segment exceeds the supported byte limit",
        ));
    }
    if index_bytes.len() > MAX_SEGMENT_INDEX_BYTES {
        return Err(invariant_violation(
            "control MVP segment index exceeds the supported byte limit",
        ));
    }
    validate_raw_checksum(
        bytes,
        Some(&reference.checksum_sha256),
        "control MVP segment reference checksum",
    )?;
    validate_raw_checksum(
        index_bytes,
        Some(&reference.index_checksum_sha256),
        "control MVP segment index reference checksum",
    )?;
    let index: ControlMvpSegmentIndex = decode_json(index_bytes, "control MVP segment index")?;
    validate_segment_index_identity(&index, reference, scope)?;
    let preflight = preflight_arrow_segment(bytes)?;
    if index.record_batch_offsets != preflight.record_batch_offsets {
        return Err(invariant_violation(
            "control MVP segment index record-batch offsets do not match the Arrow footer",
        ));
    }
    let batches =
        catch_unwind(AssertUnwindSafe(|| -> Result<Vec<RecordBatch>> {
            let mut reader = FileReaderBuilder::new()
                .with_max_footer_fb_tables(MAX_SEGMENT_FOOTER_TABLES)
                .with_max_footer_fb_depth(MAX_SEGMENT_FOOTER_DEPTH)
                .build(Cursor::new(bytes))
                .map_err(|error| segment_serialization_error("open Arrow IPC segment", error))?;
            let mut batches = Vec::new();
            for batch in &mut reader {
                batches.push(batch.map_err(|error| {
                    segment_serialization_error("read Arrow IPC segment", error)
                })?);
            }
            Ok(batches)
        }))
        .map_err(|_| invariant_violation("control MVP Arrow reader panicked after preflight"))??;
    let [batch] = batches.as_slice() else {
        return Err(invariant_violation(
            "control MVP segment must contain exactly one record batch",
        ));
    };
    if batch.num_rows() > MAX_SEGMENT_ROWS {
        return Err(invariant_violation(
            "control MVP Arrow segment exceeds the supported row limit",
        ));
    }
    let rows = decode_segment_batch(batch)?;
    let expected_index = build_segment_index(
        &reference.segment_id,
        reference.level,
        reference.logical_sequence,
        scope,
        &rows,
        bytes,
        sha256_hex(bytes),
    )?;
    if index != expected_index {
        return Err(invariant_violation(
            "control MVP segment index does not match indexed Arrow contents",
        ));
    }
    Ok(rows)
}

#[allow(clippy::suspicious_operation_groupings)]
fn validate_segment_index_identity(
    index: &ControlMvpSegmentIndex,
    reference: &ControlMvpSegmentRef,
    scope: &StateScope,
) -> Result<()> {
    let identity_matches = index.format_version == CONTROL_MVP_FORMAT_VERSION
        && index.implementation == IMPLEMENTATION
        && index.scope.matches_scope(scope)
        && index.segment_id == reference.segment_id
        && index.level == reference.level
        && index.logical_sequence == reference.logical_sequence
        && index.segment_checksum_sha256 == reference.checksum_sha256;
    if !identity_matches {
        return Err(invariant_violation(
            "control MVP segment index identity does not match its reference",
        ));
    }
    if index.row_count
        > u64::try_from(MAX_SEGMENT_ROWS).map_err(|error| {
            segment_serialization_error("convert supported segment row limit", error)
        })?
    {
        return Err(invariant_violation(
            "control MVP segment index exceeds the supported row limit",
        ));
    }
    if index.segment_size_bytes == 0
        || index.segment_size_bytes
            > u64::try_from(MAX_SEGMENT_BYTES).map_err(|error| {
                segment_serialization_error("convert supported segment byte limit", error)
            })?
    {
        return Err(invariant_violation(
            "control MVP segment index has an invalid segment byte size",
        ));
    }
    if index.record_batch_offsets.len() != 1 {
        return Err(invariant_violation(
            "control MVP segment index must identify exactly one record batch",
        ));
    }
    if index.bloom_bits_hex.len() != SEGMENT_BLOOM_BYTES * 2
        || !index
            .bloom_bits_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invariant_violation(
            "control MVP segment index Bloom filter is malformed",
        ));
    }
    Ok(())
}

fn validate_segment_index_key_metadata(index: &ControlMvpSegmentIndex) -> Result<()> {
    let bounds = match (&index.min_key_hex, &index.max_key_hex) {
        (None, None) => None,
        (Some(minimum), Some(maximum)) => {
            let minimum = hex::decode(minimum).map_err(|error| {
                segment_serialization_error("decode segment minimum key", error)
            })?;
            let maximum = hex::decode(maximum).map_err(|error| {
                segment_serialization_error("decode segment maximum key", error)
            })?;
            if minimum > maximum {
                return Err(invariant_violation(
                    "control MVP segment index minimum key exceeds maximum key",
                ));
            }
            Some((minimum, maximum))
        }
        _ => {
            return Err(invariant_violation(
                "control MVP segment index key bounds are incomplete",
            ));
        }
    };
    if let Some((minimum, maximum)) = &bounds {
        let expected_min_utf8 = std::str::from_utf8(minimum).ok();
        let expected_max_utf8 = std::str::from_utf8(maximum).ok();
        if index.min_key_utf8.as_deref() != expected_min_utf8
            || index.max_key_utf8.as_deref() != expected_max_utf8
        {
            return Err(invariant_violation(
                "control MVP segment index UTF-8 key bounds do not match binary bounds",
            ));
        }
    } else if index.min_key_utf8.is_some() || index.max_key_utf8.is_some() {
        return Err(invariant_violation(
            "control MVP segment index has UTF-8 bounds without binary bounds",
        ));
    }
    Ok(())
}

fn index_key_bounds(index: &ControlMvpSegmentIndex) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    validate_segment_index_key_metadata(index)?;
    match (&index.min_key_hex, &index.max_key_hex) {
        (Some(minimum), Some(maximum)) => Ok(Some((
            hex::decode(minimum).map_err(|error| {
                segment_serialization_error("decode segment minimum key", error)
            })?,
            hex::decode(maximum).map_err(|error| {
                segment_serialization_error("decode segment maximum key", error)
            })?,
        ))),
        (None, None) => Ok(None),
        _ => Err(invariant_violation(
            "control MVP segment index key bounds are incomplete",
        )),
    }
}

fn state_reference_key_bounds(
    reference: &ControlMvpStateRef,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    match (&reference.min_key_hex, &reference.max_key_hex) {
        (Some(minimum), Some(maximum)) => {
            let minimum = hex::decode(minimum).map_err(|error| {
                segment_serialization_error("decode manifest L1 minimum key", error)
            })?;
            let maximum = hex::decode(maximum).map_err(|error| {
                segment_serialization_error("decode manifest L1 maximum key", error)
            })?;
            if minimum > maximum {
                return Err(invariant_violation(
                    "control MVP manifest L1 minimum key exceeds maximum key",
                ));
            }
            Ok(Some((minimum, maximum)))
        }
        (None, None) => Ok(None),
        _ => Err(invariant_violation(
            "control MVP manifest L1 key bounds are incomplete",
        )),
    }
}

fn key_bounds_overlap_prefix(bounds: Option<&(Vec<u8>, Vec<u8>)>, prefix: &[u8]) -> bool {
    let Some((minimum, maximum)) = bounds else {
        return false;
    };
    if maximum.as_slice() < prefix {
        return false;
    }
    prefix_exclusive_end(prefix).is_none_or(|exclusive_end| minimum < &exclusive_end)
}

fn segment_row_key_bounds_hex(rows: &[ControlMvpSegmentRow]) -> (Option<String>, Option<String>) {
    let mut keys = rows
        .iter()
        .filter(|row| row.record_kind == SEGMENT_RECORD_KV)
        .map(|row| row.key.as_slice());
    let minimum = keys.next();
    let maximum = keys.next_back().or(minimum);
    (minimum.map(hex::encode), maximum.map(hex::encode))
}

fn state_segment_reference(reference: &ControlMvpStateRef) -> ControlMvpSegmentRef {
    ControlMvpSegmentRef {
        segment_id: reference.state_id.clone(),
        level: ControlMvpSegmentLevel::L1,
        logical_sequence: reference.logical_sequence,
        checksum_sha256: reference.checksum_sha256.clone(),
        index_checksum_sha256: reference.index_checksum_sha256.clone(),
    }
}

fn segment_index_might_contain_key(index: &ControlMvpSegmentIndex, key: &[u8]) -> Result<bool> {
    validate_segment_index_key_metadata(index)?;
    let (Some(minimum), Some(maximum)) = (&index.min_key_hex, &index.max_key_hex) else {
        return Ok(false);
    };
    let minimum = hex::decode(minimum)
        .map_err(|error| segment_serialization_error("decode segment minimum key", error))?;
    let maximum = hex::decode(maximum)
        .map_err(|error| segment_serialization_error("decode segment maximum key", error))?;
    if key < minimum.as_slice() || key > maximum.as_slice() {
        return Ok(false);
    }
    let bloom = hex::decode(&index.bloom_bits_hex)
        .map_err(|error| segment_serialization_error("decode segment Bloom filter", error))?;
    if bloom.len() != SEGMENT_BLOOM_BYTES {
        return Err(invariant_violation(
            "control MVP segment index Bloom filter has the wrong byte length",
        ));
    }
    let digest = Sha256::digest(key);
    for byte in digest.iter().take(3) {
        let bit = usize::from(*byte) % (SEGMENT_BLOOM_BYTES * 8);
        let Some(slot) = bloom.get(bit / 8) else {
            return Err(invariant_violation(
                "control MVP segment index Bloom bit is out of bounds",
            ));
        };
        if slot & (1 << (bit % 8)) == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn segment_index_overlaps_prefix(index: &ControlMvpSegmentIndex, prefix: &[u8]) -> Result<bool> {
    validate_segment_index_key_metadata(index)?;
    let (Some(minimum), Some(maximum)) = (&index.min_key_hex, &index.max_key_hex) else {
        return Ok(false);
    };
    let minimum = hex::decode(minimum)
        .map_err(|error| segment_serialization_error("decode segment minimum key", error))?;
    let maximum = hex::decode(maximum)
        .map_err(|error| segment_serialization_error("decode segment maximum key", error))?;
    if maximum.as_slice() < prefix {
        return Ok(false);
    }
    if let Some(exclusive_end) = prefix_exclusive_end(prefix)
        && minimum >= exclusive_end
    {
        return Ok(false);
    }
    Ok(true)
}

fn prefix_exclusive_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    let mut truncate_at = None;
    for (index, byte) in end.iter_mut().enumerate().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            truncate_at = Some(index + 1);
            break;
        }
    }
    truncate_at.map(|length| {
        end.truncate(length);
        end
    })
}

#[allow(clippy::too_many_lines)]
fn decode_segment_batch(batch: &RecordBatch) -> Result<Vec<ControlMvpSegmentRow>> {
    if batch.schema().as_ref() != &control_mvp_segment_schema() {
        return Err(invariant_violation(
            "control MVP Arrow segment schema does not match the supported schema",
        ));
    }
    let record_kinds = segment_column::<UInt8Array>(batch, 0, "record_kind")?;
    let keys = segment_column::<BinaryArray>(batch, 1, "key")?;
    let values = segment_column::<BinaryArray>(batch, 2, "value")?;
    let generations = segment_column::<UInt64Array>(batch, 3, "generation")?;
    let tombstones = segment_column::<BooleanArray>(batch, 4, "tombstone")?;
    let logical_sequences = segment_column::<UInt64Array>(batch, 5, "logical_sequence")?;
    let logical_ordinals = segment_column::<UInt64Array>(batch, 6, "logical_ordinal")?;
    let origin_sequences = segment_column::<UInt64Array>(batch, 7, "origin_sequence")?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let record_kind = record_kinds.value(row_index);
        if !matches!(
            record_kind,
            SEGMENT_RECORD_KV | SEGMENT_RECORD_OUTBOX | SEGMENT_RECORD_OUTBOX_TRIM
        ) {
            return Err(invariant_violation(
                "control MVP Arrow segment has an unknown record kind",
            ));
        }
        let tombstone = tombstones.value(row_index);
        let value = (!values.is_null(row_index)).then(|| values.value(row_index).to_vec());
        if tombstone == value.is_some() {
            return Err(invariant_violation(
                "control MVP Arrow segment tombstone/value polarity is invalid",
            ));
        }
        rows.push(ControlMvpSegmentRow {
            record_kind,
            key: keys.value(row_index).to_vec(),
            value,
            generation: generations.value(row_index),
            tombstone,
            logical_sequence: logical_sequences.value(row_index),
            logical_ordinal: logical_ordinals.value(row_index),
            origin_sequence: (!origin_sequences.is_null(row_index))
                .then(|| origin_sequences.value(row_index)),
        });
    }
    if rows.windows(2).any(|pair| match pair {
        [left, right] => {
            (left.record_kind, left.key.as_slice()) >= (right.record_kind, right.key.as_slice())
        }
        _ => false,
    }) {
        return Err(invariant_violation(
            "control MVP Arrow segment rows are not sorted",
        ));
    }
    Ok(rows)
}

fn segment_column<'a, T>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a T>
where
    T: Array + 'static,
{
    batch
        .columns()
        .get(index)
        .ok_or_else(|| {
            invariant_violation(format!(
                "control MVP Arrow segment is missing column {name}"
            ))
        })?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            invariant_violation(format!(
                "control MVP Arrow segment column {name} has the wrong type"
            ))
        })
}

fn segment_serialization_error(context: &str, error: impl fmt::Display) -> CatalogError {
    CatalogError::Serialization {
        message: format!("failed to {context}: {error}"),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayStateDigest {
    logical_sequence: u64,
    entries: Vec<ReplayStateDigestEntry>,
    outbox: Vec<ControlMvpOutboxStateEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayStateDigestEntry {
    key: Vec<u8>,
    generation: u64,
    value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlMvpCheckpoint {
    format_version: u32,
    implementation: String,
    scope: ControlMvpScopeDoc,
    checkpoint_id: String,
    manifest_id: String,
    logical_sequence: u64,
    manifest_checksum_sha256: String,
    states: Vec<ControlMvpStateRef>,
    min_retention_seconds: Option<u64>,
}

impl ControlMvpCheckpoint {
    fn validate(&self, scope: &StateScope, expected_checkpoint_id: &str) -> Result<()> {
        if self.format_version != CONTROL_MVP_FORMAT_VERSION {
            return Err(invariant_violation(
                "control MVP checkpoint format version mismatch",
            ));
        }
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

#[derive(Clone)]
struct ControlMvpRetainedReader {
    scope: StateScope,
    token: StateToken,
    source: ControlMvpRetainedSource,
}

#[derive(Clone)]
enum ControlMvpRetainedSource {
    Manifest {
        store: Box<ControlMvpStateStore>,
        manifest: Box<ControlMvpManifest>,
    },
    Materialized(ReplayState),
}

#[async_trait]
impl ArcoStateReader for ControlMvpRetainedReader {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match &self.source {
            ControlMvpRetainedSource::Manifest { store, manifest } => {
                store.get_from_manifest(manifest, key).await
            }
            ControlMvpRetainedSource::Materialized(state) => Ok(state
                .kv
                .get(key)
                .filter(|value| !value.tombstone)
                .map(|value| value.bytes.clone())),
        }
    }

    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        request.validate_for_scope(&self.scope)?;
        match &self.source {
            ControlMvpRetainedSource::Manifest { store, manifest } => {
                store
                    .scan_manifest_page(manifest, request, self.token.clone())
                    .await
            }
            ControlMvpRetainedSource::Materialized(state) => build_scan_page(
                &self.scope,
                request.clone(),
                Some(self.token.clone()),
                state.scan_prefix(request.prefix()),
            ),
        }
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
    storage: &ScopedAuthorityStore,
    path: &str,
    bytes: Bytes,
    precondition_message: &str,
) -> Result<()> {
    match storage
        .put(path, bytes, AuthorityWritePrecondition::DoesNotExist)
        .await?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => Err(precondition_failed(precondition_message)),
    }
}

async fn put_immutable_matching(
    storage: &ScopedAuthorityStore,
    path: &str,
    bytes: Bytes,
    mismatch_message: &str,
) -> Result<()> {
    match storage
        .put(
            path,
            bytes.clone(),
            AuthorityWritePrecondition::DoesNotExist,
        )
        .await?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => {
            let existing = storage.get(path).await?;
            if existing == bytes {
                Ok(())
            } else {
                Err(precondition_failed(mismatch_message))
            }
        }
    }
}

async fn put_restore_immutable(
    storage: &ScopedAuthorityStore,
    path: &str,
    bytes: Bytes,
) -> Result<()> {
    match storage
        .put(
            path,
            bytes.clone(),
            AuthorityWritePrecondition::DoesNotExist,
        )
        .await?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => {
            let existing = storage.get(path).await?;
            if existing == bytes {
                Ok(())
            } else {
                Err(precondition_failed(
                    "Control MVP restore immutable object already exists with different bytes",
                ))
            }
        }
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

fn encode_envelope_limited<T: Serialize>(
    artifact_type: &str,
    payload: &T,
    max_bytes: usize,
    context: &str,
) -> Result<Bytes> {
    let bytes = encode_envelope(artifact_type, payload)?;
    validate_candidate_json_size(&bytes, max_bytes, context)?;
    Ok(bytes)
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

fn decode_envelope_limited<T>(
    bytes: &[u8],
    artifact_type: &str,
    max_bytes: usize,
    context: &str,
) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    validate_persisted_json_size(bytes, max_bytes, context)?;
    decode_envelope(bytes, artifact_type, context)
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

fn encode_json_limited<T: Serialize>(value: &T, max_bytes: usize, context: &str) -> Result<Bytes> {
    let bytes = encode_json(value, context)?;
    validate_candidate_json_size(&bytes, max_bytes, context)?;
    Ok(bytes)
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

fn decode_json_limited<T>(bytes: &[u8], max_bytes: usize, context: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    validate_persisted_json_size(bytes, max_bytes, context)?;
    decode_json(bytes, context)
}

fn validate_candidate_json_size(bytes: &[u8], max_bytes: usize, context: &str) -> Result<()> {
    if bytes.len() > max_bytes {
        return Err(CatalogError::MaintenanceBackpressure {
            message: format!(
                "{context} requires {} JSON bytes, above the {max_bytes}-byte limit",
                bytes.len()
            ),
        });
    }
    Ok(())
}

fn validate_persisted_json_size(bytes: &[u8], max_bytes: usize, context: &str) -> Result<()> {
    if bytes.len() > max_bytes {
        return Err(invariant_violation(format!(
            "{context} is {} bytes, above the supported {max_bytes}-byte limit",
            bytes.len()
        )));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn prefixed_sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

fn validate_prefixed_digest(value: &str, context: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CatalogError::Validation {
            message: format!("{context} must use sha256: prefix"),
        });
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CatalogError::Validation {
            message: format!("{context} must contain 64 lowercase hexadecimal characters"),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn restore_identity_suffix(
    scope: &StateScope,
    identity: &RestoreAttemptIdentity,
    source: &PersistedAuthorityReference,
    current_base_kind: ControlMvpRestoreCurrentBaseKind,
    base_manifest_id: &str,
    base_pointer_version: Option<&str>,
    observed_base_pointer_sha256: &str,
    result_sequence: u64,
    checkpoint_interval: Option<u64>,
) -> String {
    let mut hasher = Sha256::new();
    for value in [
        scope.tenant_id(),
        scope.workspace_id(),
        scope.domain(),
        identity.restore_id(),
        identity.domain(),
        source.implementation(),
        source.manifest_id(),
        source.manifest_path(),
        source.manifest_sha256(),
        source.checkpoint_path().unwrap_or_default(),
        source.checkpoint_sha256().unwrap_or_default(),
        current_base_kind.identity_label(),
        base_manifest_id,
        base_pointer_version.unwrap_or_default(),
        observed_base_pointer_sha256,
    ] {
        hash_bytes(&mut hasher, value.as_bytes());
    }
    hash_u64(&mut hasher, identity.attempt());
    hash_u64(&mut hasher, source.logical_sequence());
    hash_u64(&mut hasher, result_sequence);
    if let Some(checkpoint_interval) = checkpoint_interval {
        hash_u64(&mut hasher, checkpoint_interval);
    }
    hex::encode(hasher.finalize())[..32].to_string()
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

fn next_logical_sequence(current: u64, context: &str) -> Result<u64> {
    current.checked_add(1).ok_or_else(|| {
        invariant_violation(format!(
            "control MVP logical sequence overflow while {context}"
        ))
    })
}

fn ambiguous_authority_outcome(message: impl Into<String>) -> CatalogError {
    CatalogError::AmbiguousAuthorityOutcome {
        message: message.into(),
    }
}

fn segment_capacity_error(level: ControlMvpSegmentLevel, message: &str) -> CatalogError {
    match level {
        ControlMvpSegmentLevel::L0 => validation_failed(message),
        ControlMvpSegmentLevel::L1 => CatalogError::MaintenanceBackpressure {
            message: message.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition};
    use arco_core::{MemoryBackend, WriteResult};
    use arrow::ipc::{Block, Footer, FooterArgs};
    use flatbuffers::FlatBufferBuilder;
    use tokio::sync::oneshot;

    use super::*;

    struct PauseCheckpointPutBackend {
        inner: MemoryBackend,
        gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
        arrow_gets: AtomicUsize,
    }

    impl PauseCheckpointPutBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryBackend::new(),
                gate: Mutex::new(None),
                arrow_gets: AtomicUsize::new(0),
            })
        }

        fn arm(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
            let (reached_tx, reached_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            *self.gate.lock().expect("checkpoint gate") = Some((reached_tx, release_rx));
            (reached_rx, release_tx)
        }

        fn reset_arrow_gets(&self) {
            self.arrow_gets.store(0, Ordering::SeqCst);
        }

        fn arrow_gets(&self) -> usize {
            self.arrow_gets.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl StorageBackend for PauseCheckpointPutBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            if std::path::Path::new(path)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("arrow"))
            {
                self.arrow_gets.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if path.contains("/checkpoints/") {
                let gate = self.gate.lock().expect("checkpoint gate").take();
                if let Some((reached, release)) = gate {
                    let _ = reached.send(());
                    let _ = release.await;
                }
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> arco_core::Result<ListPage> {
            self.inner.list_page(prefix, start_after, limit).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    fn one_kv_row() -> ControlMvpSegmentRow {
        ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: b"key".to_vec(),
            value: Some(b"value".to_vec()),
            generation: 1,
            tombstone: false,
            logical_sequence: 1,
            logical_ordinal: 0,
            origin_sequence: None,
        }
    }

    fn encoded_test_segment() -> (Bytes, Bytes, ControlMvpSegmentRef, StateScope) {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let (bytes, index_bytes, reference) = encode_segment(
            "test-segment",
            ControlMvpSegmentLevel::L1,
            1,
            &scope,
            &[one_kv_row()],
            PRODUCTION_SEGMENT_LIMITS,
        )
        .expect("encode test segment");
        (bytes, index_bytes, reference, scope)
    }

    #[tokio::test]
    async fn l0_arrow_is_charged_before_fetching_against_the_shared_scan_budget() {
        let backend = PauseCheckpointPutBackend::new();
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .expect("store");
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin");
        txn.put(b"catalog/a", Bytes::from_static(b"value"))
            .await
            .expect("put");
        let token = txn.commit().await.expect("commit").into_state_token();
        let manifest = store
            .load_manifest(token.authority_manifest_id())
            .await
            .expect("manifest");

        backend.reset_arrow_gets();
        let error = store
            .scan_manifest_page_with_arrow_budget(
                &manifest,
                ScanRequest::new(b"catalog/"),
                token,
                1,
            )
            .await
            .expect_err("the indexed L0 segment cannot fit the physical byte budget");
        assert!(matches!(
            error,
            CatalogError::MaintenanceBackpressure { .. }
        ));
        assert_eq!(
            0,
            backend.arrow_gets(),
            "the indexed segment size must be charged before its Arrow object is fetched"
        );
    }

    #[test]
    fn l1_row_byte_and_index_capacity_failures_are_typed_backpressure() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        for limits in [
            SegmentLimits {
                rows: 0,
                ..PRODUCTION_SEGMENT_LIMITS
            },
            SegmentLimits {
                bytes: 0,
                ..PRODUCTION_SEGMENT_LIMITS
            },
            SegmentLimits {
                index_bytes: 0,
                ..PRODUCTION_SEGMENT_LIMITS
            },
        ] {
            let error = encode_segment(
                "capacity",
                ControlMvpSegmentLevel::L1,
                1,
                &scope,
                &[one_kv_row()],
                limits,
            )
            .expect_err("required L1 capacity must fail closed");
            assert!(matches!(
                error,
                CatalogError::MaintenanceBackpressure { .. }
            ));
        }

        let l0_error = encode_segment(
            "oversized-input",
            ControlMvpSegmentLevel::L0,
            1,
            &scope,
            &[one_kv_row()],
            SegmentLimits {
                rows: 0,
                ..PRODUCTION_SEGMENT_LIMITS
            },
        )
        .expect_err("an individually oversized L0 mutation is invalid input");
        assert!(matches!(l0_error, CatalogError::Validation { .. }));
    }

    #[tokio::test]
    async fn l1_sharding_keeps_each_segment_below_half_capacity() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .expect("store")
                .with_checkpoint_interval(NonZeroU64::new(2).expect("nonzero interval"))
                .with_segment_limits(SegmentLimits {
                    rows: 4,
                    ..PRODUCTION_SEGMENT_LIMITS
                });

        let mut seed = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin seed");
        seed.put(b"catalog/a", Bytes::from_static(b"a"))
            .await
            .expect("put a");
        seed.put(b"catalog/b", Bytes::from_static(b"b"))
            .await
            .expect("put b");
        seed.commit().await.expect("seed commit below L1 boundary");
        let mut candidate = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin candidate");
        candidate
            .delete(b"catalog/a")
            .await
            .expect("retain tombstone");
        candidate
            .put(b"catalog/c", Bytes::from_static(b"c"))
            .await
            .expect("put c");
        let outcome = candidate.commit().await.expect("publish sharded L1 state");
        let manifest = store
            .load_manifest(outcome.state_token().authority_manifest_id())
            .await
            .expect("load sharded manifest");
        assert_eq!(
            2,
            manifest.anchor_states.len(),
            "three retained rows at a two-row target require two ordered shards"
        );
        assert_eq!(None, store.get(b"catalog/a").await.expect("deleted a"));
        assert_eq!(
            Some(Bytes::from_static(b"b")),
            store.get(b"catalog/b").await.expect("retained b")
        );
        assert_eq!(
            Some(Bytes::from_static(b"c")),
            store.get(b"catalog/c").await.expect("inserted c")
        );
    }

    #[tokio::test]
    async fn individually_oversized_l1_row_precedes_every_candidate_artifact_write() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let (_bytes, probe_index, _reference) = encode_segment(
            &"x".repeat(128),
            ControlMvpSegmentLevel::L0,
            1,
            &scope,
            &[one_kv_row()],
            PRODUCTION_SEGMENT_LIMITS,
        )
        .expect("probe L0 index size");
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        let store = ControlMvpStateStore::new(storage, scope)
            .expect("store")
            .with_checkpoint_interval(NonZeroU64::new(2).expect("nonzero interval"))
            .with_segment_limits(SegmentLimits {
                index_bytes: probe_index.len(),
                ..PRODUCTION_SEGMENT_LIMITS
            });

        let mut seed = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin seed");
        seed.put(b"key", Bytes::from_static(b"value"))
            .await
            .expect("stage seed");
        seed.commit().await.expect("seed below L1 boundary");
        let head_before = store.current_state_token().await.expect("head before");
        let artifacts_before = backend
            .list("")
            .await
            .expect("inventory before")
            .into_iter()
            .map(|object| object.path)
            .collect::<BTreeSet<_>>();

        let mut candidate = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin candidate");
        candidate
            .put(b"key", Bytes::from_static(b"next"))
            .await
            .expect("stage candidate");
        let error = candidate
            .commit()
            .await
            .expect_err("one row cannot be split below the L1 target index budget");
        assert!(matches!(
            error,
            CatalogError::MaintenanceBackpressure { .. }
        ));
        assert_eq!(
            head_before,
            store.current_state_token().await.expect("head")
        );
        assert_eq!(
            artifacts_before,
            backend
                .list("")
                .await
                .expect("inventory after")
                .into_iter()
                .map(|object| object.path)
                .collect::<BTreeSet<_>>(),
            "known capacity failure must precede transaction, L0, L1, manifest, and head writes"
        );
    }

    #[tokio::test]
    async fn transaction_lineage_crosses_equivalent_maintenance_manifests() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
        for sequence in 1..=16_u64 {
            let mut txn = store
                .begin_control_txn(TxnOptions::default())
                .await
                .expect("begin transaction");
            txn.put(b"catalog/hot", Bytes::from(sequence.to_be_bytes().to_vec()))
                .await
                .expect("stage write");
            txn.commit().await.expect("commit write");
        }
        let source_token = store.current_state_token().await.expect("source token");
        let source_manifest = store
            .load_manifest(source_token.authority_manifest_id())
            .await
            .expect("source manifest");
        let planned = source_manifest.tx_refs[0].clone();
        ControlMvpMaintenanceWorker::new(storage, scope)
            .expect("maintenance worker")
            .consolidate_pending()
            .await
            .expect("maintenance")
            .expect("selected layout");
        let current = store.load_current_base_state().await.expect("current base");
        assert!(
            store
                .tx_in_lineage(&current, &planned)
                .await
                .expect("lineage lookup"),
            "physical-only publication must retain logical transaction lineage"
        );
    }

    #[tokio::test]
    async fn control_gc_deletes_only_aged_unreachable_artifacts() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin transaction");
        txn.put(b"catalog/live", Bytes::from_static(b"value"))
            .await
            .expect("stage write");
        let live = txn.commit().await.expect("commit live state");
        let paths = store.paths();
        let orphan_path = paths.tx_object("tx-orphan");
        storage
            .put_raw(
                &orphan_path,
                Bytes::from_static(b"orphan"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("seed orphan");

        let worker =
            ControlMvpMaintenanceWorker::new(storage.clone(), scope).expect("maintenance worker");
        let now = Utc::now() + ChronoDuration::days(8);
        let plan = worker.plan_gc_at(now, Vec::new()).await.expect("plan GC");
        assert_eq!(
            vec![orphan_path.as_str()],
            plan.candidates()
                .iter()
                .map(ControlMvpGcCandidate::path)
                .collect::<Vec<_>>()
        );
        assert!(
            !plan.candidates().iter().any(|candidate| candidate.path()
                == paths.manifest_object(live.state_token().authority_manifest_id())),
            "current authority closure is retained regardless of age"
        );

        let outcome = worker
            .collect_gc_at(now, Vec::new())
            .await
            .expect("collect GC");
        assert_eq!(1, outcome.objects_deleted());
        assert!(
            storage
                .head_raw(&orphan_path)
                .await
                .expect("head")
                .is_none()
        );
        assert_eq!(
            Some(Bytes::from_static(b"value")),
            store.get(b"catalog/live").await.expect("read live state")
        );
    }

    #[tokio::test]
    async fn control_gc_honors_checkpoint_retention_above_thirty_day_floor() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin transaction");
        txn.put(b"catalog/live", Bytes::from_static(b"value"))
            .await
            .expect("stage write");
        txn.commit().await.expect("commit live state");
        let checkpoint = store
            .checkpoint(
                CheckpointOptions::new(Some(scope.clone()))
                    .with_min_retention_seconds(60 * 24 * 60 * 60),
            )
            .await
            .expect("checkpoint");
        let checkpoint_path = store.paths().checkpoint_object(checkpoint.checkpoint_id());

        let plan = ControlMvpMaintenanceWorker::new(storage, scope)
            .expect("maintenance worker")
            .plan_gc_at(Utc::now() + ChronoDuration::days(31), Vec::new())
            .await
            .expect("plan GC");
        assert!(
            !plan
                .candidates()
                .iter()
                .any(|candidate| candidate.path() == checkpoint_path),
            "explicit 60-day checkpoint pin must outlive the 30-day floor"
        );
    }

    #[tokio::test]
    async fn control_gc_pages_and_converges_beyond_ten_thousand_objects() {
        const ORPHANS: usize = 10_241;

        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin transaction");
        txn.put(b"catalog/live", Bytes::from_static(b"value"))
            .await
            .expect("stage write");
        txn.commit().await.expect("commit live state");
        let checkpoint = store
            .checkpoint(
                CheckpointOptions::new(Some(scope.clone()))
                    .with_min_retention_seconds(60 * 24 * 60 * 60),
            )
            .await
            .expect("checkpoint");

        let paths = store.paths();
        let explicitly_pinned = paths.tx_object("tx-orphan-00000");
        for ordinal in 0..ORPHANS {
            storage
                .put_raw(
                    &paths.tx_object(&format!("tx-orphan-{ordinal:05}")),
                    Bytes::from_static(b"orphan"),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .expect("seed orphan");
        }

        let worker =
            ControlMvpMaintenanceWorker::new(storage.clone(), scope).expect("maintenance worker");
        let now = Utc::now() + ChronoDuration::days(31);
        let first = worker
            .plan_gc_page_at(now, vec![explicitly_pinned.clone()], None)
            .await
            .expect("first bounded plan");
        assert!(first.candidates().len() <= CONTROL_MVP_GC_PAGE_SIZE);
        assert!(
            first.continuation().is_some(),
            "large inventory must continue"
        );

        let mut continuation = None;
        let mut deleted = 0_u64;
        loop {
            let outcome = worker
                .collect_gc_page_at(
                    now,
                    vec![explicitly_pinned.clone()],
                    continuation.as_deref(),
                )
                .await
                .expect("bounded GC page");
            deleted = deleted.saturating_add(outcome.objects_deleted());
            continuation = outcome.continuation().map(ToOwned::to_owned);
            if continuation.is_none() {
                break;
            }
        }
        assert_eq!(u64::try_from(ORPHANS - 1).expect("orphan count"), deleted);
        assert!(
            storage
                .head_raw(&explicitly_pinned)
                .await
                .expect("head")
                .is_some()
        );
        assert_eq!(
            Some(Bytes::from_static(b"value")),
            store
                .get(b"catalog/live")
                .await
                .expect("live authority read")
        );
        let retained = store
            .read_checkpoint(checkpoint)
            .await
            .expect("retained checkpoint remains readable");
        assert_eq!(
            Some(Bytes::from_static(b"value")),
            retained
                .get(b"catalog/live")
                .await
                .expect("checkpoint read")
        );
    }

    #[tokio::test]
    async fn checkpoint_publication_excludes_gc_until_the_retained_root_is_visible() {
        let backend = PauseCheckpointPutBackend::new();
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
        let mut seed = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin seed");
        seed.put(b"catalog/live", Bytes::from_static(b"old"))
            .await
            .expect("stage seed");
        seed.commit().await.expect("commit seed");

        let (checkpoint_reached, release_checkpoint) = backend.arm();
        let checkpoint_store = store.clone();
        let checkpoint_scope = scope.clone();
        let checkpoint_task = tokio::spawn(async move {
            checkpoint_store
                .checkpoint(
                    CheckpointOptions::new(Some(checkpoint_scope))
                        .with_min_retention_seconds(60 * 24 * 60 * 60),
                )
                .await
        });
        checkpoint_reached
            .await
            .expect("checkpoint reached publication");

        let mut advance = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin advance");
        advance
            .put(b"catalog/live", Bytes::from_static(b"new"))
            .await
            .expect("stage advance");
        advance.commit().await.expect("advance authority");

        let gc_worker =
            ControlMvpMaintenanceWorker::new(storage, scope).expect("maintenance worker");
        let gc_task = tokio::spawn(async move {
            gc_worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(31), Vec::new())
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !gc_task.is_finished(),
            "GC must wait while checkpoint publication owns retention coordination"
        );

        release_checkpoint.send(()).expect("release checkpoint");
        let checkpoint = checkpoint_task
            .await
            .expect("checkpoint task")
            .expect("checkpoint publication");
        gc_task.await.expect("GC task").expect("coordinated GC");
        let retained = store
            .read_checkpoint(checkpoint)
            .await
            .expect("successful checkpoint must remain readable");
        assert_eq!(
            Some(Bytes::from_static(b"old")),
            retained
                .get(b"catalog/live")
                .await
                .expect("checkpoint read")
        );
    }

    #[tokio::test]
    async fn prefix_scan_continues_across_more_overlapping_l1_segments_than_one_page() {
        let backend = PauseCheckpointPutBackend::new();
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage, scope)
            .expect("store")
            .with_checkpoint_interval(NonZeroU64::new(1).expect("interval"))
            .with_segment_limits(SegmentLimits {
                rows: 1,
                ..PRODUCTION_SEGMENT_LIMITS
            });
        for ordinal in 0..70_u64 {
            let mut txn = store
                .begin_control_txn(TxnOptions::default())
                .await
                .expect("begin seed");
            txn.put(
                format!("catalog/{ordinal:03}").as_bytes(),
                Bytes::from(ordinal.to_be_bytes().to_vec()),
            )
            .await
            .expect("stage seed");
            txn.commit().await.expect("commit seed");
        }
        // The successor reads the preceding 70-shard anchor plus one L0 suffix.
        let mut tail = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin tail");
        tail.put(b"catalog/070", Bytes::from_static(b"tail"))
            .await
            .expect("stage tail");
        tail.commit().await.expect("commit tail");

        let mut continuation = None;
        let mut keys = Vec::new();
        let mut pages = 0_usize;
        loop {
            backend.reset_arrow_gets();
            let mut request = ScanRequest::new(b"catalog/").with_limits(1_000, 4 * 1024 * 1024, 3);
            if let Some(token) = continuation.take() {
                request = request.with_token(token);
            }
            let page = store.scan(request).await.expect("bounded scan page");
            assert!(
                backend.arrow_gets() <= 3,
                "one page fetched more Arrow segments than its hard budget"
            );
            keys.extend(page.entries().iter().map(|entry| entry.key().to_vec()));
            pages += 1;
            continuation = page.continuation().cloned();
            if continuation.is_none() {
                break;
            }
        }
        assert!(pages > 1, "segment budget must produce resumable pages");
        assert_eq!(71, keys.len());
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    fn rebind_segment(
        bytes: Vec<u8>,
        index_bytes: &[u8],
        mut reference: ControlMvpSegmentRef,
    ) -> (Bytes, Bytes, ControlMvpSegmentRef) {
        let mut index: ControlMvpSegmentIndex =
            decode_json(index_bytes, "test segment index").expect("decode index");
        let checksum = sha256_hex(&bytes);
        index.segment_checksum_sha256.clone_from(&checksum);
        let index_bytes = encode_json(&index, "test segment index").expect("encode index");
        reference.checksum_sha256 = checksum;
        reference.index_checksum_sha256 = sha256_hex(&index_bytes);
        (Bytes::from(bytes), index_bytes, reference)
    }

    fn footer_bounds(bytes: &[u8]) -> (usize, usize) {
        let trailer_start = bytes.len() - 10;
        let trailer: [u8; 10] = bytes[trailer_start..].try_into().expect("trailer");
        let footer_len = arrow::ipc::reader::read_footer_length(trailer).expect("footer length");
        (trailer_start - footer_len, trailer_start)
    }

    fn footer_without_schema(bytes: &[u8]) -> Vec<u8> {
        let (footer_start, trailer_start) = footer_bounds(bytes);
        let footer = arrow::ipc::root_as_footer(&bytes[footer_start..trailer_start])
            .expect("decode original footer");
        let block = *footer.recordBatches().expect("record batches").get(0);
        let mut builder = FlatBufferBuilder::new();
        let batches = builder.create_vector(&[block]);
        let footer = Footer::create(
            &mut builder,
            &FooterArgs {
                version: footer.version(),
                schema: None,
                dictionaries: None,
                recordBatches: Some(batches),
                custom_metadata: None,
            },
        );
        builder.finish(footer, None);
        let new_footer = builder.finished_data();
        let mut malformed = bytes[..footer_start].to_vec();
        malformed.extend_from_slice(new_footer);
        malformed.extend_from_slice(
            &u32::try_from(new_footer.len())
                .expect("footer length")
                .to_le_bytes(),
        );
        malformed.extend_from_slice(b"ARROW1");
        malformed
    }

    fn footer_with_body_length(bytes: &[u8], body_length: i64) -> Vec<u8> {
        let (footer_start, trailer_start) = footer_bounds(bytes);
        let footer = arrow::ipc::root_as_footer(&bytes[footer_start..trailer_start])
            .expect("decode original footer");
        let original = footer.recordBatches().expect("record batches").get(0);
        let replacement = Block::new(original.offset(), original.metaDataLength(), body_length);
        let needle = original.0;
        let offset = bytes[footer_start..trailer_start]
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("footer block bytes");
        let mut malformed = bytes.to_vec();
        malformed[footer_start + offset..footer_start + offset + replacement.0.len()]
            .copy_from_slice(&replacement.0);
        malformed
    }

    fn two_duplicate_kv_rows() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(control_mvp_segment_schema()),
            vec![
                Arc::new(UInt8Array::from(vec![SEGMENT_RECORD_KV; 2])),
                Arc::new(BinaryArray::from(vec![b"duplicate".as_slice(); 2])),
                Arc::new(BinaryArray::from(vec![
                    Some(b"first".as_slice()),
                    Some(b"second".as_slice()),
                ])),
                Arc::new(UInt64Array::from(vec![1, 1])),
                Arc::new(BooleanArray::from(vec![false, false])),
                Arc::new(UInt64Array::from(vec![1, 1])),
                Arc::new(UInt64Array::from(vec![0, 1])),
                Arc::new(UInt64Array::from(vec![None, None])),
            ],
        )
        .expect("duplicate-key batch")
    }

    #[test]
    fn malformed_arrow_schema_fails_closed_without_panicking() {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let decoded = catch_unwind(AssertUnwindSafe(|| decode_segment_batch(&batch)));

        assert!(
            decoded.is_ok(),
            "malformed schemas must not reach column panics"
        );
        assert!(decoded.is_ok_and(|result| result.is_err()));
    }

    #[test]
    fn duplicate_physical_segment_keys_fail_closed() {
        let error = decode_segment_batch(&two_duplicate_kv_rows())
            .expect_err("duplicate (record_kind, key) rows must be rejected");

        assert!(matches!(error, CatalogError::InvariantViolation { .. }));
    }

    #[test]
    fn v4_l0_trim_rows_require_origin_sequence() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let mut tx = ControlMvpTxObject {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&scope),
            tx_id: "tx-1".to_string(),
            base_manifest_id: None,
            sequence: 1,
            writer_epoch: 0,
            request_id: None,
            l0_segment: unwritten_l0_segment_ref("tx-1", 1),
            writes: Vec::new(),
            outbox: Vec::new(),
            outbox_trim: Vec::new(),
        };

        let error = tx
            .hydrate_from_segment_rows(vec![ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_OUTBOX_TRIM,
                key: b"outbox-id".to_vec(),
                value: None,
                generation: 0,
                tombstone: true,
                logical_sequence: 1,
                logical_ordinal: 0,
                origin_sequence: None,
            }])
            .expect_err("v4 trim rows must identify the removed event incarnation");

        assert!(matches!(error, CatalogError::InvariantViolation { .. }));
    }

    #[test]
    fn replay_rejects_sequence_zero_after_terminal_logical_sequence_without_panicking() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let tx = ControlMvpTxObject {
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&scope),
            tx_id: "tx-zero".to_string(),
            base_manifest_id: Some("manifest-terminal".to_string()),
            sequence: 0,
            writer_epoch: 0,
            request_id: None,
            l0_segment: unwritten_l0_segment_ref("tx-zero", 0),
            writes: Vec::new(),
            outbox: Vec::new(),
            outbox_trim: Vec::new(),
        };
        let mut state = ReplayState {
            logical_sequence: u64::MAX,
            ..ReplayState::default()
        };

        let applied = catch_unwind(AssertUnwindSafe(|| state.apply_tx(&tx)));

        assert!(applied.is_ok(), "terminal replay must not panic");
        assert!(matches!(
            applied.expect("unwind boundary"),
            Err(CatalogError::InvariantViolation { .. })
        ));
        assert_eq!(u64::MAX, state.logical_sequence);
    }

    #[test]
    fn checksum_coherent_missing_schema_is_typed_not_a_panic() {
        let (bytes, index_bytes, reference, scope) = encoded_test_segment();
        let malformed = footer_without_schema(&bytes);
        let (bytes, index_bytes, reference) = rebind_segment(malformed, &index_bytes, reference);

        let decoded = catch_unwind(AssertUnwindSafe(|| {
            decode_segment_rows(&bytes, &index_bytes, &reference, &scope)
        }));

        assert!(decoded.is_ok(), "missing schema must not panic");
        assert!(matches!(
            decoded.expect("unwind boundary"),
            Err(CatalogError::InvariantViolation { .. })
        ));
    }

    #[test]
    fn checksum_coherent_negative_body_length_is_typed_not_a_panic() {
        let (bytes, index_bytes, reference, scope) = encoded_test_segment();
        let malformed = footer_with_body_length(&bytes, -1);
        let (bytes, index_bytes, reference) = rebind_segment(malformed, &index_bytes, reference);

        let decoded = catch_unwind(AssertUnwindSafe(|| {
            decode_segment_rows(&bytes, &index_bytes, &reference, &scope)
        }));

        assert!(decoded.is_ok(), "negative body length must not panic");
        assert!(matches!(
            decoded.expect("unwind boundary"),
            Err(CatalogError::InvariantViolation { .. })
        ));
    }

    #[test]
    fn checksum_coherent_huge_body_length_is_typed_without_allocation() {
        let (bytes, index_bytes, reference, scope) = encoded_test_segment();
        let malformed = footer_with_body_length(&bytes, 1_i64 << 40);
        let (bytes, index_bytes, reference) = rebind_segment(malformed, &index_bytes, reference);

        let decoded = catch_unwind(AssertUnwindSafe(|| {
            decode_segment_rows(&bytes, &index_bytes, &reference, &scope)
        }));

        assert!(decoded.is_ok(), "huge body length must not panic");
        assert!(matches!(
            decoded.expect("unwind boundary"),
            Err(CatalogError::InvariantViolation { .. })
        ));
    }
}
