//! Object-store-backed control-state MVP.
//!
//! # Replay model (format version 7)
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
//! Format version 7 is the only supported on-disk format and is rooted beneath
//! `control/v1/` on fresh roots. There is deliberately no migration path from
//! version 6 without integrity roots, version 5 without authenticated blocks,
//! unfenced version 4, or older JSON-anchor
//! formats: unknown and old `format_version` values fail closed.
//!
//! Before active GC deletes a candidate page, it advances HEAD's checked
//! reclamation generation by exact-version CAS under retention coordination.
//! Publishers pinned before that fence lose CAS and must regenerate artifacts.
//! Candidate identities include staging generation; transactions also bind the
//! exact observed HEAD version. A GC-only fence preserves the selected manifest,
//! logical sequence, writer epoch, and physical layout.
//!
//! Restore *plans* are versioned separately from on-disk state artifacts,
//! because an in-flight restore attempt written by an older revision must
//! still be readable by the recovery path that has to supersede it. Plan
//! versions 1 through 5 are therefore decoded as legacy plans
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
use crate::gc::reachability::RetainedAuthorityRoots;
use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL,
};

const IMPLEMENTATION: &str = "arco-state-control-mvp";
pub(crate) mod cost;
#[cfg(feature = "test-utils")]
mod eager_reference;
mod integrity;
mod lazy;
pub(crate) mod maintenance;
use integrity::{CheckpointValidation, HistoryAnchor, HistoryLink, RewriteEquivalence};
use lazy::{TransactionBase, TransactionReads};
pub use maintenance::{
    DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceJobId, MaintenanceProgress,
    MaintenanceStatus, PreparedMaintenance,
};
const RESTORE_PLAN_RECORD_TYPE: &str = "control_mvp_restore_plan";
const RESTORE_PLAN_VERSION: u32 = 6;
const RESTORE_PLAN_VERSION_V5: u32 = 5;
const RESTORE_PLAN_VERSION_V4: u32 = 4;
const RESTORE_PLAN_VERSION_V3: u32 = 3;
/// Restore-plan versions that predate the `control/v1/` authority layout.
/// They remain decodable only so recovery can safely supersede them without
/// dereferencing paths from the retired layout.
const RESTORE_PLAN_VERSION_V1: u32 = 1;
const RESTORE_PLAN_VERSION_V2: u32 = 2;
const CONTROL_MVP_FORMAT_VERSION: u32 = 7;
const SEGMENT_FORMAT_VERSION: u32 = 1;
const BLOCK_TARGET_BYTES: usize = 64 * 1024;
const MAX_BLOCK_BYTES: usize = 256 * 1024;
const MAX_SEGMENT_BLOCKS: usize = 4096;
const MAX_BLOOM_BYTES: usize = 128 * 1024;
const MAX_SEGMENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_SCAN_ARROW_BYTES: usize = MAX_SEGMENT_BYTES;
const MAX_SEGMENT_INDEX_BYTES: usize = 512 * 1024;
const MAX_SEGMENT_ROWS: usize = 1_000_000;
const MAX_SEGMENT_FOOTER_TABLES: usize = 64;
const MAX_SEGMENT_FOOTER_DEPTH: usize = 16;
const MAX_SEGMENT_FOOTER_APPARENT_BYTES: usize = 1024 * 1024;
const MAX_HEAD_JSON_BYTES: usize = 64 * 1024;
const MAX_CONTROL_JSON_BYTES: usize = 1024 * 1024;
const MAX_CONTROL_JSON_PROBE_BYTES: u64 = MAX_CONTROL_JSON_BYTES as u64 + 1;
const MAX_TRANSACTION_JSON_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROJECTION_INTENT_AGGREGATE_BYTES: usize = 4 * 1024 * 1024;
const L0_MAINTENANCE_INTENT_THRESHOLD: usize = 16;
const L0_MAINTENANCE_BACKPRESSURE_THRESHOLD: usize = 32;
const EMPTY_CURRENT_BASE_MARKER: &[u8] =
    br#"{"record_type":"control_mvp_empty_current_base","version":1}"#;

#[derive(Debug, Clone, Copy)]
struct SegmentLimits {
    block_target: usize,
    bytes: usize,
    index_bytes: usize,
    rows: usize,
}

const PRODUCTION_SEGMENT_LIMITS: SegmentLimits = SegmentLimits {
    block_target: BLOCK_TARGET_BYTES,
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
    l1_test_rows: Option<usize>,
}

impl ControlMvpStateStore {
    async fn get_json(&self, path: &str, limit: usize) -> Result<Bytes> {
        let end = (limit as u64)
            .checked_add(1)
            .ok_or_else(|| invariant_violation("JSON probe overflow"))?;
        let bytes = self.storage.get_range(path, 0..end).await?;
        if bytes.len() > limit {
            return Err(invariant_violation(format!(
                "authority JSON {} exceeds {limit} byte bound",
                path.to_ascii_lowercase()
            )));
        }
        Ok(bytes)
    }
    /// Stable implementation identifier for this MVP backend.
    pub const IMPLEMENTATION: &'static str = IMPLEMENTATION;

    /// Default number of committed transactions between automatic replay anchors.
    pub const DEFAULT_CHECKPOINT_INTERVAL: u64 = 32;

    /// Creates a control-state MVP store over workspace-scoped storage.
    ///
    /// # Errors
    ///
    /// Returns validation errors when the storage scope does not match the state
    /// scope, the physical root is not a workspace, or the domain cannot be
    /// represented as a safe object path. Non-workspace roots require the future
    /// versioned authority-scope format; they must not alias legacy `StateScope`.
    pub fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        scope.validate()?;
        if storage.tenant_id() != scope.tenant_id()
            || storage.scope().workspace_id() != Some(scope.workspace_id())
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
            l1_test_rows: None,
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

    /// Resolves an authenticated outbox intent for isolated local cost measurement.
    ///
    /// # Errors
    /// Returns the normal provenance, integrity, and reconciliation errors.
    #[cfg(feature = "test-utils")]
    pub async fn resolve_test_projection_source(
        &self,
        record: &ControlMvpProjectionOutboxRecord,
    ) -> Result<StateToken> {
        let intent = decode_json(record.payload(), "test projection intent")?;
        self.resolve_projection_source(record, &intent).await
    }

    /// Returns and resets thread-local SHA-256 helper calls and input bytes.
    /// Bloom probe hashing is excluded; this is local test instrumentation.
    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn take_test_authentication_work() -> (u64, u64) {
        TEST_SHA256_WORK.with(|work| work.replace((0, 0)))
    }

    /// Returns and resets canonical hash, rendered-state and rendered-transaction
    /// validation counts and input bytes, in that order. Local instrumentation only.
    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn take_test_integrity_work() -> [u64; 6] {
        TEST_INTEGRITY_WORK.with(|work| work.replace([0; 6]))
    }

    /// Sets bounded writer sizing for deterministic local scaling tests.
    ///
    /// # Errors
    /// Returns validation errors for values outside production reader caps.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_test_segment_sizing(mut self, l1_rows: usize, block_target: usize) -> Result<Self> {
        if l1_rows == 0
            || l1_rows > MAX_SEGMENT_ROWS / 2
            || !(8 * 1024..=MAX_BLOCK_BYTES).contains(&block_target)
        {
            return Err(validation_failed("invalid test writer sizing"));
        }
        self.l1_test_rows = Some(l1_rows);
        self.segment_limits.block_target = block_target;
        Ok(self)
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
                    .get_json(&self.paths.current_pointer(), MAX_HEAD_JSON_BYTES)
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

    /// Pins a concrete control-MVP transaction without reconstructing data.
    ///
    /// Reads authenticate selected immutable evidence on demand. The pin does
    /// not renew retention. Commit freshly validates the complete pinned state
    /// and every required anchor before publishing candidate artifacts.
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

        let base = self.pin_transaction_base().await?;
        validate_publication_epoch(self.writer_epoch, base.writer_epoch())?;
        let next_sequence = next_logical_sequence(
            base.logical_sequence(),
            "beginning a control MVP transaction",
        )?;
        let request_id = opts.request_id().map(ToOwned::to_owned);
        let suffix = opts.operation_id().map_or_else(
            || Ulid::new().to_string().to_ascii_lowercase(),
            ToOwned::to_owned,
        );
        let head_identity = sha256_hex(base.pointer_version().unwrap_or("").as_bytes());
        let suffix = format!(
            "{suffix}-head-{head_identity}-rg-{:020}",
            base.reclamation_generation()
        );
        let tx_id = request_id.clone().map_or_else(
            || format!("tx-{next_sequence:020}-{suffix}"),
            |request_id| format!("tx-{next_sequence:020}-{request_id}-{suffix}"),
        );
        let manifest_id = format!("manifest-{next_sequence:020}-{suffix}");

        Ok(ControlMvpTxn {
            store: self.clone(),
            base,
            reads: TransactionReads::default(),
            nonce: Ulid::new().0,
            #[cfg(any(test, feature = "test-utils"))]
            eager_base: None,
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
        let token = self.current_state_token().await;
        match token {
            Ok(token) => self.projection_outbox_at(token).await,
            Err(error)
                if self
                    .storage
                    .head(&self.paths.current_pointer())
                    .await?
                    .is_none() =>
            {
                let _ = error;
                Ok(Vec::new())
            }
            Err(error) => Err(error),
        }
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
        let mut records = self.load_state_at_token(&token).await?.outbox;
        for record in &mut records {
            record.observed_root = Some(token.clone());
        }
        Ok(records)
    }

    pub(crate) async fn resolve_projection_source(
        &self,
        record: &ControlMvpProjectionOutboxRecord,
        intent: &ProjectionIntentV1,
    ) -> Result<StateToken> {
        let observed = record.observed_root.as_ref().ok_or_else(|| {
            invariant_violation("projection record has no authenticated observed root")
        })?;
        let decoded: ProjectionIntentV1 = decode_json(record.payload(), "projection provenance")?;
        if decoded != *intent
            || encode_json(intent, "projection provenance")? != *record.payload()
            || intent.source_scope() != &self.scope
            || observed.scope() != &self.scope
            || record.record_id() != intent.intent_id()
            || record.origin_sequence() != Some(intent.source_logical_sequence())
            || intent.source_logical_sequence() > observed.logical_sequence()
        {
            return Err(invariant_violation(
                "projection provenance does not match authenticated outbox record",
            ));
        }
        self.resolve_ancestor(
            observed.authority_manifest_id(),
            observed.manifest_witness()?,
            |manifest, digest| {
                (manifest.manifest_id == intent.source_authority_manifest_id()).then(|| {
                    self.token(manifest.manifest_id.clone(), manifest.logical_sequence)
                        .with_manifest_witness(digest.to_string())
                })
            },
        )
        .await?
        .filter(|token| token.logical_sequence() == intent.source_logical_sequence())
        .ok_or_else(|| {
            invariant_violation("projection source is absent from authenticated lineage")
        })
    }

    async fn load_current_base_state(&self) -> Result<ControlMvpBase> {
        self.pin_transaction_base().await?.materialize(self).await
    }

    async fn load_state_at_token(&self, token: &StateToken) -> Result<ReplayState> {
        if token.scope() != &self.scope {
            return Err(validation_failed(
                "StateToken scope does not match control MVP store",
            ));
        }
        let manifest = self
            .load_manifest_with_expected_checksum(
                token.authority_manifest_id(),
                Some(token.manifest_witness()?),
            )
            .await?;
        if manifest.logical_sequence != token.logical_sequence() {
            return Err(invariant_violation(
                "StateToken logical sequence does not match manifest",
            ));
        }
        self.replay_manifest(&manifest).await
    }

    async fn load_pointer(&self) -> Result<ControlMvpPointer> {
        let bytes = self
            .get_json(&self.paths.current_pointer(), MAX_HEAD_JSON_BYTES)
            .await?;
        validate_persisted_json_size(&bytes, MAX_HEAD_JSON_BYTES, "control MVP mutable head")?;
        validate_version_header(&bytes, CONTROL_MVP_FORMAT_VERSION, "control MVP HEAD")?;
        let pointer: ControlMvpPointer =
            decode_json_limited(&bytes, MAX_HEAD_JSON_BYTES, "control MVP mutable head")?;
        pointer.validate(&self.scope)?;
        Ok(pointer)
    }

    async fn load_manifest_for_pointer(
        &self,
        pointer: &ControlMvpPointer,
    ) -> Result<ControlMvpManifest> {
        let manifest = self
            .load_manifest_with_expected_checksum(
                &pointer.manifest_id,
                Some(&pointer.manifest_checksum_sha256),
            )
            .await?;
        if manifest.logical_sequence != pointer.logical_sequence {
            return Err(invariant_violation(
                "HEAD logical sequence differs from authenticated manifest",
            ));
        }
        if pointer.writer_epoch < manifest.writer_epoch
            || pointer.reclamation_generation < manifest.reclamation_generation
        {
            return Err(invariant_violation(
                "HEAD fences precede authenticated manifest",
            ));
        }
        Ok(manifest)
    }

    #[cfg(test)]
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
            .get_range(
                &self.paths.manifest_object(manifest_id),
                0..MAX_CONTROL_JSON_PROBE_BYTES,
            )
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
        state.history_root.clone_from(&manifest.history_anchor.root);
        if let Some(render) = manifest
            .equivalence
            .as_ref()
            .and_then(|e| e.render_source.as_ref())
            && (state.logical_sequence != render.logical_sequence
                || state.checksum()? != render.state_checksum_sha256)
        {
            return Err(invariant_violation(
                "materialized rewrite differs from its render cut",
            ));
        }
        for tx_ref in &manifest.tx_refs {
            let tx = self.load_tx(tx_ref).await?;
            state.apply_tx(&tx)?;
        }

        let checksum = state.checksum()?;
        if checksum != manifest.state_checksum_sha256 || state.history_root != manifest.history_root
        {
            return Err(invariant_violation(
                "control MVP manifest state checksum does not match replay",
            ));
        }
        Ok(state)
    }

    async fn verify_materialized_state(
        &self,
        references: &[ControlMvpStateRef],
        expected: &ReplayState,
    ) -> Result<()> {
        let mut actual = self.load_state_snapshots(references).await?;
        actual.history_root.clone_from(&expected.history_root);
        if actual != *expected || actual.checksum()? != expected.checksum()? {
            return Err(invariant_violation(
                "materialized successor base differs from parent state",
            ));
        }
        Ok(())
    }

    async fn replay_for_successor(&self, manifest: &ControlMvpManifest) -> Result<ReplayState> {
        let state = self.replay_manifest(manifest).await?;
        if !manifest.anchor_states.is_empty() {
            self.verify_materialized_state(&manifest.anchor_states, &state)
                .await?;
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
        let indexes = self.load_l1_indexes(references).await?;
        for (reference, (index_bytes, _)) in references.iter().zip(indexes) {
            let shard = self
                .load_state_snapshot_from_index(reference, &index_bytes)
                .await?;
            combined.append_snapshot(shard)?;
        }
        Ok(combined)
    }

    async fn load_state_snapshot_from_index(
        &self,
        reference: &ControlMvpStateRef,
        index_bytes: &[u8],
    ) -> Result<ControlMvpStateObject> {
        let segment_reference = state_segment_reference(reference);
        let bytes = self.load_complete_segment(&segment_reference).await?;
        let rows = decode_segment_rows(&bytes, index_bytes, &segment_reference, &self.scope)?;
        let snapshot = state_object_from_segment_rows(reference, rows, &self.scope)?;
        snapshot.validate(&self.scope, reference)?;
        Ok(snapshot)
    }

    async fn load_segment_index(
        &self,
        reference: &ControlMvpSegmentRef,
    ) -> Result<(Bytes, ControlMvpSegmentIndex)> {
        cost::selection_read("maintenance-source-metadata", async {
            if reference.index_size_bytes == 0
                || reference.index_size_bytes > MAX_SEGMENT_INDEX_BYTES as u64
            {
                return Err(invariant_violation("invalid declared directory length"));
            }
            let probe_end = reference
                .index_size_bytes
                .checked_add(1)
                .ok_or_else(|| invariant_violation("index probe overflow"))?;
            let index_bytes = self
                .storage
                .get_range(
                    &self.paths.segment_index(&reference.segment_id),
                    0..probe_end,
                )
                .await?;
            if index_bytes.len() as u64 != reference.index_size_bytes {
                return Err(invariant_violation(
                    "directory length differs from owning reference",
                ));
            }
            validate_raw_checksum(
                &index_bytes,
                Some(&reference.index_checksum_sha256),
                "control MVP segment index reference checksum",
            )?;
            validate_version_header(&index_bytes, SEGMENT_FORMAT_VERSION, "segment directory")?;
            let index: ControlMvpSegmentIndex =
                decode_json(&index_bytes, "control MVP segment index")?;
            validate_segment_index_identity(&index, reference, &self.scope)?;
            validate_segment_index_key_metadata(&index)?;
            Ok((index_bytes, index))
        })
        .await
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
            if index_key_bounds(&loaded.1)? != state_reference_key_bounds(reference)? {
                return Err(invariant_violation(
                    "L1 directory bounds differ from owning reference",
                ));
            }
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

    async fn load_block(
        &self,
        reference: &ControlMvpSegmentRef,
        block: &ControlMvpBlock,
    ) -> Result<Vec<ControlMvpSegmentRow>> {
        cost::selection_read("maintenance-selected-data-reads", async {
            let path = match reference.level {
                ControlMvpSegmentLevel::L0 => self.paths.l0_segment_object(&reference.segment_id),
                ControlMvpSegmentLevel::L1 => self.paths.state_object(&reference.segment_id),
            };
            let end = block
                .offset
                .checked_add(block.length)
                .ok_or_else(|| invariant_violation("block range overflow"))?;
            let bytes = self.storage.get_range(&path, block.offset..end).await?;
            let rows = decode_block_rows(&bytes, block)?;
            for row in &rows {
                if row.logical_sequence != reference.logical_sequence {
                    return Err(invariant_violation("block sequence mismatch"));
                }
                if row.record_kind == SEGMENT_RECORD_KV
                    && (row.generation == 0
                        || row.generation > reference.logical_sequence
                        || (reference.level == ControlMvpSegmentLevel::L0
                            && row.generation != reference.logical_sequence)
                        || row.tombstone != row.value.is_none()
                        || row.origin_sequence.is_some())
                {
                    return Err(invariant_violation("invalid selected KV row"));
                }
            }
            Ok(rows)
        })
        .await
    }

    async fn indexed_point(
        &self,
        reference: &ControlMvpSegmentRef,
        index: &ControlMvpSegmentIndex,
        key: &[u8],
    ) -> Result<Option<StoredValue>> {
        if !segment_index_might_contain_key(index, key)? {
            return Ok(None);
        }
        for block in &index.blocks {
            if block.record_kind == Some(SEGMENT_RECORD_KV)
                && block_key_bounds(block)?
                    .is_some_and(|(min, max)| key >= min.as_slice() && key <= max.as_slice())
            {
                return Ok(self
                    .load_block(reference, block)
                    .await?
                    .into_iter()
                    .find(|row| row.key == key)
                    .map(stored_row_value));
            }
        }
        Ok(None)
    }

    async fn load_state_value_from_index(
        &self,
        reference: &ControlMvpStateRef,
        index_bytes: &[u8],
        index: &ControlMvpSegmentIndex,
        key: &[u8],
    ) -> Result<Option<StoredValue>> {
        let _ = index_bytes;
        self.indexed_point(&state_segment_reference(reference), index, key)
            .await
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
        let mut target_limits = half_segment_limits(self.segment_limits);
        if let Some(rows) = self.l1_test_rows {
            target_limits.rows = target_limits.rows.min(rows);
        }
        let row_shards = partition_state_rows(
            &rows,
            state.logical_sequence,
            manifest_id,
            &self.scope,
            target_limits,
        )?;
        let rendered = row_shards
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
                        segment_size_bytes: segment_reference.segment_size_bytes,
                        index_size_bytes: segment_reference.index_size_bytes,
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
            .collect::<Result<Vec<_>>>()?;
        self.validate_rendered_state(state, &rendered)?;
        Ok(rendered)
    }

    fn validate_rendered_state(
        &self,
        expected: &ReplayState,
        rendered: &[RenderedControlMvpStateSegment],
    ) -> Result<()> {
        #[cfg(feature = "test-utils")]
        record_integrity_work(
            1,
            rendered
                .iter()
                .map(|segment| segment.bytes.len() + segment.index_bytes.len())
                .sum(),
        );
        let mut actual = ReplayState {
            logical_sequence: expected.logical_sequence,
            history_root: expected.history_root.clone(),
            ..ReplayState::default()
        };
        for segment in rendered {
            let reference = state_segment_reference(&segment.reference);
            let rows = decode_segment_rows(
                &segment.bytes,
                &segment.index_bytes,
                &reference,
                &self.scope,
            )?;
            if segment_row_key_bounds_hex(&rows)
                != (
                    segment.reference.min_key_hex.clone(),
                    segment.reference.max_key_hex.clone(),
                )
            {
                return Err(invariant_violation(
                    "rendered L1 bounds differ from owning reference",
                ));
            }
            actual.append_snapshot(state_object_from_segment_rows(
                &segment.reference,
                rows,
                &self.scope,
            )?)?;
        }
        if actual != *expected || actual.checksum()? != expected.checksum()? {
            return Err(invariant_violation(
                "rendered rewrite is not equivalent to expected state",
            ));
        }
        Ok(())
    }

    fn validate_rendered_transaction(
        &self,
        tx: &ControlMvpTxObject,
        bytes: &[u8],
        index: &[u8],
    ) -> Result<()> {
        #[cfg(feature = "test-utils")]
        record_integrity_work(2, bytes.len() + index.len());
        let rows = decode_segment_rows(bytes, index, &tx.l0_segment, &self.scope)?;
        let mut decoded = tx.clone();
        decoded.hydrate_from_segment_rows(rows)?;
        if integrity::mutation_digest(&decoded) != tx.history.mutation_sha256 {
            return Err(invariant_violation(
                "rendered transaction differs from expected mutation",
            ));
        }
        Ok(())
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
        let bytes = self.load_complete_segment(reference).await?;
        let (index_bytes, _) = self.load_segment_index(reference).await?;
        decode_segment_rows(&bytes, &index_bytes, reference, &self.scope)
    }

    async fn load_complete_segment(&self, reference: &ControlMvpSegmentRef) -> Result<Bytes> {
        if reference.segment_size_bytes == 0
            || reference.segment_size_bytes > MAX_SEGMENT_BYTES as u64
        {
            return Err(invariant_violation("invalid declared segment length"));
        }
        let end = reference
            .segment_size_bytes
            .checked_add(1)
            .ok_or_else(|| invariant_violation("segment probe overflow"))?;
        let path = match reference.level {
            ControlMvpSegmentLevel::L0 => self.paths.l0_segment_object(&reference.segment_id),
            ControlMvpSegmentLevel::L1 => self.paths.state_object(&reference.segment_id),
        };
        let bytes = self.storage.get_range(&path, 0..end).await?;
        if bytes.len() as u64 != reference.segment_size_bytes {
            return Err(invariant_violation(
                "segment length differs from owning reference",
            ));
        }
        Ok(bytes)
    }

    async fn load_tx(&self, tx_ref: &ControlMvpTxRef) -> Result<ControlMvpTxObject> {
        let mut tx = self.load_tx_metadata(tx_ref).await?;
        let rows = self.load_l0_segment_rows(&tx.l0_segment).await?;
        tx.hydrate_from_segment_rows(rows)?;
        Ok(tx)
    }

    async fn load_tx_metadata(&self, tx_ref: &ControlMvpTxRef) -> Result<ControlMvpTxObject> {
        if tx_ref.size_bytes == 0 || tx_ref.size_bytes > MAX_TRANSACTION_JSON_BYTES as u64 {
            return Err(invariant_violation("invalid transaction reference length"));
        }
        let bytes = self
            .get_json(
                &self.paths.tx_object(&tx_ref.tx_id),
                usize::try_from(tx_ref.size_bytes)
                    .map_err(|_| invariant_violation("transaction metadata length overflow"))?,
            )
            .await?;
        if bytes.len() as u64 != tx_ref.size_bytes {
            return Err(invariant_violation(
                "transaction length differs from owning reference",
            ));
        }
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
        let tx = self.load_tx_metadata(tx_ref).await?;
        let (_, index) = self.load_segment_index(&tx.l0_segment).await?;
        Ok(self
            .indexed_point(&tx.l0_segment, &index, key)
            .await?
            .map(|value| ControlMvpWriteEntry {
                key: key.to_vec(),
                generation: value.generation,
                value: (!value.tombstone).then(|| value.bytes.to_vec()),
            }))
    }

    async fn get_from_manifest(
        &self,
        manifest: &ControlMvpManifest,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        Ok(self
            .get_versioned_from_manifest(manifest, key)
            .await?
            .filter(|value| !value.tombstone)
            .map(|value| value.bytes))
    }

    async fn get_versioned_from_manifest(
        &self,
        manifest: &ControlMvpManifest,
        key: &[u8],
    ) -> Result<Option<StoredValue>> {
        let mut selected = None;
        for reference in &manifest.base_states {
            let bounds = state_reference_key_bounds(reference)?;
            if !bounds
                .as_ref()
                .is_some_and(|(min, max)| key >= min.as_slice() && key <= max.as_slice())
            {
                continue;
            }
            let (index_bytes, index) = self
                .load_segment_index(&state_segment_reference(reference))
                .await?;
            if index_key_bounds(&index)? != bounds {
                return Err(invariant_violation(
                    "manifest and directory key bounds differ",
                ));
            }
            selected = self
                .load_state_value_from_index(reference, &index_bytes, &index, key)
                .await?;
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
        Ok(selected)
    }

    fn validate_manifest_read_metadata(&self, manifest: &ControlMvpManifest) -> Result<()> {
        manifest.validate(&self.scope, &manifest.manifest_id)
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
        let mut stream =
            lazy::ResolvedRows::new(self, Some(manifest), prefix, start_after, None).await?;
        let mut budget = BlockScanBudget {
            blocks: 64,
            segments: request.max_segments(),
            bytes: raw_arrow_budget,
        };
        let mut entries = Vec::new();
        let mut logical_bytes = 0_usize;
        let mut boundary = None;
        let mut has_more = false;
        loop {
            let ready = stream.fill(self, &mut budget).await?;
            if !ready {
                if boundary.is_none() {
                    return Err(CatalogError::MaintenanceBackpressure {
                        message:
                            "scan overlay prevents safe progress within block/segment/byte budget"
                                .to_string(),
                    });
                }
                has_more = true;
                break;
            }
            let Some(key) = stream.key().map(<[u8]>::to_vec) else {
                break;
            };
            let row = stream
                .take(&key)
                .ok_or_else(|| invariant_violation("scan merge lost selected row"))?;
            if !row.tombstone {
                let value = row.bytes;
                let size = key
                    .len()
                    .checked_add(value.len())
                    .ok_or_else(|| invariant_violation("scan byte count overflow"))?;
                if logical_bytes.saturating_add(size) > request.max_bytes() && boundary.is_some() {
                    has_more = true;
                    break;
                }
                logical_bytes = logical_bytes.saturating_add(size);
                entries.push(KvPair::new(
                    key.clone(),
                    VersionedValue::new(value, Some(row.generation)),
                ));
            }
            boundary = Some(key);
            if entries.len() >= request.max_rows() || logical_bytes >= request.max_bytes() {
                has_more = stream.may_have_more();
                break;
            }
        }
        build_scan_page_with_backend_boundary(
            &self.scope,
            request,
            Some(observed_token),
            entries,
            if has_more { boundary } else { None },
        )
    }

    async fn write_checkpoint(
        &self,
        checkpoint: &ControlMvpCheckpoint,
        bytes: Bytes,
    ) -> Result<()> {
        let path = self.paths.checkpoint_object(&checkpoint.checkpoint_id);
        match self
            .storage
            .put(
                &path,
                bytes.clone(),
                AuthorityWritePrecondition::DoesNotExist,
            )
            .await
        {
            Ok(WriteResult::Success { .. }) => Ok(()),
            Ok(WriteResult::PreconditionFailed { .. }) => Err(precondition_failed(
                "control MVP checkpoint object already exists",
            )),
            Err(error) => {
                // An exact immutable record proves publication, even if its PUT
                // response was lost. Absence cannot prove a remote PUT is terminal.
                if self
                    .get_json(&path, MAX_CONTROL_JSON_BYTES)
                    .await
                    .is_ok_and(|visible| visible == bytes)
                {
                    Ok(())
                } else {
                    Err(ambiguous_authority_outcome(format!(
                        "control MVP checkpoint publication could not be reconciled: {error}"
                    )))
                }
            }
        }
    }

    /// Publishes immutable checkpoint artifacts without acquiring retention
    /// coordination. The caller must already own a durable retention epoch and
    /// must treat any returned error as an uncertain external mutation.
    async fn publish_checkpoint_under_retention(
        &self,
        opts: &CheckpointOptions,
    ) -> Result<CheckpointToken> {
        let (checkpoint, rendered) = self.prepare_checkpoint(opts).await?;
        self.write_rendered_state_snapshots(&rendered).await?;
        let bytes = encode_envelope_limited(
            "control-mvp-checkpoint",
            &checkpoint,
            MAX_CONTROL_JSON_BYTES,
            "checkpoint",
        )?;
        let witness = sha256_hex(&bytes);
        self.write_checkpoint(&checkpoint, bytes).await?;
        Ok(self
            .checkpoint_token(checkpoint.checkpoint_id)
            .with_checkpoint_witness(witness))
    }

    async fn prepare_checkpoint(
        &self,
        opts: &CheckpointOptions,
    ) -> Result<(ControlMvpCheckpoint, Vec<RenderedControlMvpStateSegment>)> {
        let pointer = self.load_pointer().await?;
        let manifest = self.load_manifest_for_pointer(&pointer).await?;
        let expected_state = self.replay_manifest(&manifest).await?;
        let checkpoint_id = format!(
            "checkpoint-{:020}-rg-{:020}-{}",
            pointer.logical_sequence,
            pointer.reclamation_generation,
            Ulid::new().to_string().to_ascii_lowercase()
        );
        // Reuse the manifest's own anchored snapshot when it has one;
        // otherwise materialize the replay state as a new immutable snapshot
        // so checkpoint reads never replay history.
        let (state_refs, rendered) = if !manifest.anchor_states.is_empty() {
            (manifest.anchor_states.clone(), Vec::new())
        } else if manifest.tx_refs.is_empty() && !manifest.base_states.is_empty() {
            (manifest.base_states.clone(), Vec::new())
        } else {
            let rendered = self.render_state_snapshots(&expected_state, &checkpoint_id)?;
            let state_refs = rendered
                .iter()
                .map(|segment| segment.reference.clone())
                .collect();
            (state_refs, rendered)
        };
        if rendered.is_empty() {
            let mut state = self.load_state_snapshots(&state_refs).await?;
            state.history_root.clone_from(&manifest.history_root);
            if state != expected_state || state.checksum()? != manifest.state_checksum_sha256 {
                return Err(invariant_violation(
                    "checkpoint reused state differs from authority manifest",
                ));
            }
        }
        let checkpoint = ControlMvpCheckpoint {
            validation: CheckpointValidation {
                encoding_version: 1,
                source_manifest_sha256: pointer.manifest_checksum_sha256.clone(),
                source_history_root: manifest.history_root.clone(),
                source_physical_root: manifest.physical_root.clone(),
                state_checksum_sha256: manifest.state_checksum_sha256.clone(),
                checkpoint_physical_root: integrity::checkpoint_physical_digest(
                    &ControlMvpScopeDoc::from(&self.scope),
                    &state_refs,
                )?,
            },
            reclamation_generation: pointer.reclamation_generation,
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
        checkpoint.validate(&self.scope, &checkpoint.checkpoint_id)?;
        checkpoint.validate_source(&manifest)?;
        encode_envelope_limited(
            "control-mvp-checkpoint",
            &checkpoint,
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
        Ok((checkpoint, rendered))
    }

    async fn load_checkpoint(&self, token: &CheckpointToken) -> Result<ControlMvpCheckpoint> {
        let checkpoint_id = token.checkpoint_id();
        let bytes = self
            .get_json(
                &self.paths.checkpoint_object(checkpoint_id),
                MAX_CONTROL_JSON_BYTES,
            )
            .await?;
        validate_raw_checksum(
            &bytes,
            Some(token.checkpoint_witness()?),
            "checkpoint token witness",
        )?;
        let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
            &bytes,
            "control-mvp-checkpoint",
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
        checkpoint.validate(&self.scope, checkpoint_id)?;
        Ok(checkpoint)
    }

    async fn validate_checkpoint_protection(
        &self,
        checkpoint: &ControlMvpCheckpoint,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let meta = self
            .storage
            .head(&self.paths.checkpoint_object(&checkpoint.checkpoint_id))
            .await?
            .ok_or_else(|| validation_failed("checkpoint protection is missing"))?;
        let created = meta
            .last_modified
            .ok_or_else(|| validation_failed("checkpoint protection has no creation timestamp"))?;
        let floor = u64::try_from(CONTROL_MVP_TOKEN_RETENTION_DAYS * 24 * 60 * 60)
            .map_err(|_| invariant_violation("invalid checkpoint retention floor"))?;
        let seconds = i64::try_from(checkpoint.min_retention_seconds.unwrap_or(floor).max(floor))
            .map_err(|_| validation_failed("checkpoint retention interval overflow"))?;
        let deadline = created
            .checked_add_signed(ChronoDuration::seconds(seconds))
            .ok_or_else(|| validation_failed("checkpoint retention deadline overflow"))?;
        if deadline <= now {
            let checkpoint_path = self.paths.checkpoint_object(&checkpoint.checkpoint_id);
            let checksum = prefixed_sha256(
                &self
                    .get_json(&checkpoint_path, MAX_CONTROL_JSON_BYTES)
                    .await?,
            );
            if !self
                .externally_protected(now, |reference| {
                    reference.checkpoint_path() == Some(checkpoint_path.as_str())
                        && reference.checkpoint_sha256() == Some(checksum.as_str())
                        && reference.manifest_id() == checkpoint.manifest_id
                        && reference.logical_sequence() == checkpoint.logical_sequence
                        && reference.manifest_sha256()
                            == format!("sha256:{}", checkpoint.manifest_checksum_sha256)
                })
                .await?
            {
                return Err(validation_failed(
                    "checkpoint protection expired; object existence cannot renew it",
                ));
            }
        }
        Ok(())
    }

    // A caller can hold the identity of a staged manifest even when its CAS
    // never landed. Only published lineage supplies source-protection evidence.
    // The time floor keeps the historical manifest and all intervening links
    // protected while a coordinated retained-root publication validates them.
    async fn validate_state_token_protection(
        &self,
        token: &StateToken,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let pointer = self.load_pointer().await?;
        if pointer.manifest_id == token.authority_manifest_id() {
            if pointer.manifest_checksum_sha256 != token.manifest_witness()?
                || pointer.logical_sequence != token.logical_sequence()
            {
                return Err(invariant_violation(
                    "token differs from selected HEAD witness",
                ));
            }
            return Ok(());
        }
        let meta = self
            .storage
            .head(&self.paths.manifest_object(token.authority_manifest_id()))
            .await?
            .ok_or_else(|| validation_failed("state token protection is missing"))?;
        if meta.last_modified.is_none_or(|created| {
            created <= now - ChronoDuration::days(CONTROL_MVP_TOKEN_RETENTION_DAYS)
        }) {
            let checksum = prefixed_sha256(
                &self
                    .get_json(
                        &self.paths.manifest_object(token.authority_manifest_id()),
                        MAX_CONTROL_JSON_BYTES,
                    )
                    .await?,
            );
            let manifest_id = token.authority_manifest_id();
            if self
                .externally_protected(now, |reference| {
                    reference.manifest_id() == manifest_id
                        && reference.logical_sequence() == token.logical_sequence()
                        && reference.manifest_sha256() == checksum
                        && token
                            .expected_manifest_sha256
                            .as_ref()
                            .is_some_and(|witness| checksum == format!("sha256:{witness}"))
                })
                .await?
            {
                return Ok(());
            }
            return Err(validation_failed("state token protection expired"));
        }
        let witness = token.manifest_witness()?;
        let acknowledged = self
            .resolve_ancestor(
                &pointer.manifest_id,
                &pointer.manifest_checksum_sha256,
                |manifest, _digest| {
                    (manifest.manifest_id == token.authority_manifest_id())
                        .then_some(manifest.logical_sequence == token.logical_sequence())
                },
            )
            .await?;
        if acknowledged == Some(true) {
            self.load_manifest_with_expected_checksum(token.authority_manifest_id(), Some(witness))
                .await?;
            return Ok(());
        }
        Err(validation_failed(
            "state token has no acknowledged publication in authority lineage",
        ))
    }

    async fn externally_protected(
        &self,
        now: DateTime<Utc>,
        matches: impl Fn(&PersistedAuthorityReference) -> bool,
    ) -> Result<bool> {
        let mut roots = RetainedAuthorityRoots::new(&self.retention, now);
        while let Some(root) = roots.next().await? {
            for reference in root.authorities {
                if reference.scope() == &self.scope
                    && reference.implementation() == IMPLEMENTATION
                    && matches(&reference)
                {
                    validate_control_mvp_authority_format(&self.paths, &reference)?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn token(&self, manifest_id: String, logical_sequence: u64) -> StateToken {
        StateToken {
            expected_manifest_sha256: None,
            scope: self.scope.clone(),
            logical_sequence,
            authority_manifest_id: manifest_id,
        }
    }

    fn checkpoint_token(&self, checkpoint_id: String) -> CheckpointToken {
        CheckpointToken {
            expected_checkpoint_sha256: None,
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
    #[allow(clippy::option_if_let_else)]
    async fn tx_in_lineage(
        &self,
        parent: &ControlMvpBase,
        planned: &ControlMvpTxRef,
    ) -> Result<bool> {
        if planned.sequence > parent.state.logical_sequence {
            return Ok(false);
        }
        let Some(id) = &parent.manifest_id else {
            return Ok(false);
        };
        let digest = parent
            .manifest_checksum_sha256
            .as_deref()
            .ok_or_else(|| invariant_violation("missing lineage root witness"))?;
        let found = self
            .resolve_ancestor(id, digest, |manifest, _digest| {
                if let Some(found) = manifest
                    .tx_refs
                    .iter()
                    .find(|tx| tx.sequence == planned.sequence)
                {
                    Some(found == planned)
                } else if manifest.logical_sequence < planned.sequence {
                    Some(false)
                } else {
                    None
                }
            })
            .await?;
        Ok(found.unwrap_or(false))
    }

    async fn resolve_ancestor<T>(
        &self,
        id: &str,
        digest: &str,
        select: impl FnMut(&ControlMvpManifest, &str) -> Option<T>,
    ) -> Result<Option<T>> {
        self.resolve_ancestor_bounded(id, digest, select, 4096, 64 * 1024 * 1024)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn resolve_ancestor_bounded<T>(
        &self,
        id: &str,
        digest: &str,
        mut select: impl FnMut(&ControlMvpManifest, &str) -> Option<T>,
        max_manifests: usize,
        max_bytes: usize,
    ) -> Result<Option<T>> {
        let mut next = Some((id.to_string(), digest.to_string()));
        let mut visited = BTreeSet::new();
        let mut metadata_bytes = 0_usize;
        let mut child: Option<AncestorTransition> = None;
        while let Some((id, digest)) = next {
            if visited.len() >= max_manifests || metadata_bytes >= max_bytes {
                return Err(ambiguous_authority_outcome(
                    "authenticated ancestry resolution budget exhausted",
                ));
            }
            if !visited.insert(id.clone()) {
                return Err(invariant_violation("cyclic authority lineage"));
            }
            let remaining = max_bytes - metadata_bytes;
            let probe_end = MAX_CONTROL_JSON_PROBE_BYTES.min(remaining as u64);
            let bytes = self
                .storage
                .get_range(&self.paths.manifest_object(&id), 0..probe_end)
                .await
                .map_err(|error| {
                    ambiguous_authority_outcome(format!(
                        "authenticated ancestry is unavailable: {error}"
                    ))
                })?;
            metadata_bytes = metadata_bytes
                .checked_add(bytes.len())
                .ok_or_else(|| invariant_violation("ancestry metadata byte overflow"))?;
            if metadata_bytes > max_bytes
                || (bytes.len() == remaining && sha256_hex(&bytes) != digest)
            {
                return Err(ambiguous_authority_outcome(
                    "authenticated ancestry metadata budget exhausted",
                ));
            }
            validate_raw_checksum(&bytes, Some(&digest), "authenticated ancestry manifest")?;
            let manifest: ControlMvpManifest = decode_envelope_limited(
                &bytes,
                "control-mvp-manifest",
                MAX_CONTROL_JSON_BYTES,
                "authenticated ancestor",
            )?;
            manifest.validate(&self.scope, &id)?;
            if let Some(child) = child {
                let successor_digest = sha256_hex(&encode_json(
                    &manifest.successor_anchor(),
                    "lineage successor",
                )?);
                let mutation = manifest.logical_sequence.checked_add(1) == Some(child.sequence)
                    && manifest.layout_generation == child.layout
                    && child.equivalence.is_none()
                    && successor_digest == child.predecessor_digest;
                let maintenance = manifest.logical_sequence == child.sequence
                    && manifest.layout_generation.checked_add(1) == Some(child.layout)
                    && manifest.state_checksum_sha256 == child.checksum
                    && child.equivalence.as_ref().is_some_and(|evidence| {
                        evidence.source_physical_root == manifest.physical_root
                    })
                    && manifest
                        .maintenance_intent
                        .as_ref()
                        .is_some_and(|intent| intent.layout_generation() == child.layout);
                let history_matches = child.parent_history_root == manifest.history_root;
                if (!mutation && !maintenance) || !history_matches {
                    return Err(invariant_violation(
                        "invalid authenticated ancestry transition",
                    ));
                }
            }
            if let Some(found) = select(&manifest, &digest) {
                return Ok(Some(found));
            }
            let prefix = manifest
                .tx_refs
                .get(..manifest.tx_refs.len().saturating_sub(1))
                .unwrap_or_default();
            let predecessor_digest = sha256_hex(&encode_json(
                &(&manifest.base_states, prefix),
                "lineage predecessor",
            )?);
            child = Some(AncestorTransition {
                sequence: manifest.logical_sequence,
                layout: manifest.layout_generation,
                checksum: manifest.state_checksum_sha256,
                predecessor_digest,
                parent_history_root: manifest
                    .tx_refs
                    .last()
                    .filter(|_| manifest.equivalence.is_none())
                    .map_or_else(
                        || manifest.history_root.clone(),
                        |tx| tx.history.preceding_root.clone(),
                    ),
                equivalence: manifest.equivalence,
            });
            next = manifest
                .base_manifest_id
                .zip(manifest.parent_manifest_sha256);
        }
        Ok(None)
    }

    async fn load_restore_source_lineage(
        &self,
        source: &PersistedAuthorityReference,
    ) -> Result<ControlMvpBase> {
        self.validate_restore_authority_format(source)?;
        let manifest_bytes = self
            .get_json(source.manifest_path(), MAX_CONTROL_JSON_BYTES)
            .await?;
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
        let state = self.replay_for_successor(&manifest).await?;
        let (base_states, tx_refs) = manifest.successor_anchor();
        Ok(ControlMvpBase {
            history_anchor: manifest.successor_history_anchor(),
            reclamation_generation: manifest.reclamation_generation,
            pointer_version: None,
            manifest_id: Some(manifest.manifest_id),
            manifest_checksum_sha256: Some(sha256_hex(&manifest_bytes)),
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
                        history_anchor: integrity::genesis(&ControlMvpScopeDoc::from(&self.scope)),
                        reclamation_generation: 0,
                        pointer_version: None,
                        manifest_id: None,
                        manifest_checksum_sha256: None,
                        writer_epoch: 0,
                        layout_generation: 0,
                        state: ReplayState::empty(&self.scope),
                        base_states: Vec::new(),
                        tx_refs: Vec::new(),
                    },
                    candidate_parent,
                    current_base_kind: ControlMvpRestoreCurrentBaseKind::Empty,
                    writer_epoch: 0,
                    pointer_bytes: Bytes::from_static(EMPTY_CURRENT_BASE_MARKER),
                });
            };
            let pointer_bytes = self
                .get_json(&self.paths.current_pointer(), MAX_HEAD_JSON_BYTES)
                .await?;
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
            let state = self.replay_for_successor(&manifest).await?;
            let (base_states, tx_refs) = manifest.successor_anchor();
            let current = ControlMvpBase {
                history_anchor: manifest.successor_history_anchor(),
                reclamation_generation: pointer.reclamation_generation,
                pointer_version: Some(before.version),
                manifest_id: Some(pointer.manifest_id),
                manifest_checksum_sha256: Some(pointer.manifest_checksum_sha256),
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
        if stable.current_base_kind == ControlMvpRestoreCurrentBaseKind::Empty
            && result_sequence <= source.logical_sequence()
        {
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
        let suffix = format!("{suffix}-rg-{:020}", stable.current.reclamation_generation);
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
            history: HistoryLink::default(),
            reclamation_generation: stable.current.reclamation_generation,
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
        tx.history = HistoryLink::new(&tx, &stable.candidate_parent.state.history_root)?;
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
        self.validate_rendered_transaction(&tx, &l0_segment_bytes, &l0_index_bytes)?;
        let transaction_bytes = encode_envelope_limited(
            "control-mvp-tx",
            &tx,
            MAX_TRANSACTION_JSON_BYTES,
            "Control MVP restore transaction",
        )?;
        let transaction_checksum = sha256_hex(&transaction_bytes);
        let transaction_ref = ControlMvpTxRef {
            size_bytes: transaction_bytes.len() as u64,
            history: tx.history.clone(),
            tx_id: transaction_id.clone(),
            sequence: result_sequence,
            checksum_sha256: transaction_checksum,
        };
        let mut candidate_state = stable.candidate_parent.state.clone();
        candidate_state.apply_tx(&tx)?;
        let mut tx_refs = stable.candidate_parent.tx_refs.clone();
        tx_refs.push(transaction_ref.clone());
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
        let mut manifest = ControlMvpManifest {
            history_anchor: stable.candidate_parent.history_anchor.clone(),
            history_root: candidate_state.history_root.clone(),
            physical_root: String::new(),
            equivalence: None,
            parent_manifest_sha256: stable.candidate_parent.manifest_checksum_sha256.clone(),
            reclamation_generation: stable.current.reclamation_generation,
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
        manifest.physical_root = manifest.physical_digest()?;
        manifest.validate(&self.scope, &manifest.manifest_id)?;
        let manifest_bytes = encode_envelope_limited(
            "control-mvp-manifest",
            &manifest,
            MAX_CONTROL_JSON_BYTES,
            "Control MVP restore manifest",
        )?;
        let manifest_checksum = sha256_hex(&manifest_bytes);

        let pointer = ControlMvpPointer {
            reclamation_generation: stable.current.reclamation_generation,
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
            transaction_ref,
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
            transaction_ref: Some(rendered.transaction_ref.clone()),
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
            observed_reclamation_generation: stable.current.reclamation_generation,
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
        expected_manifest_sha256: None,
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

/// Separately constructed capability for asynchronous physical-layout maintenance.
///
/// API request code receives [`ControlMvpStateStore`], not this type. Operators
/// construct the worker explicitly so consolidation has an independently
/// auditable identity and never becomes an implicit mutation fallback.
///
/// ```compile_fail
/// use arco_catalog::{ControlMvpMaintenanceWorker, DurableAuthorityBinding};
/// fn fixture(worker: &ControlMvpMaintenanceWorker) {
///     let _ = worker.test_consolidate_pending(DurableAuthorityBinding::new([17; 32]));
/// }
/// ```
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
    /// Configures deterministic local maintenance writer sizing.
    ///
    /// # Errors
    /// Returns validation errors for sizing outside production reader caps.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_test_segment_sizing(mut self, rows: usize, target: usize) -> Result<Self> {
        self.store = self.store.with_test_segment_sizing(rows, target)?;
        Ok(self)
    }

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
    /// Active snapshot/export roots are loaded through the retention capability.
    /// `additional_protected_paths` may conservatively protect extra operator
    /// paths; every path must be inside this exact authority domain.
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
            RetentionMutationKind::ControlGc,
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
            // Fence the exact inventoried authority before authorizing any DELETE.
            // Even a delayed DELETE can then affect only objects which publishers
            // in the new generation cannot introduce into their closure.
            let head_version = if plan.candidates.is_empty() {
                plan.head_version.clone()
            } else {
                self.fence_reclamation(&plan.head_version).await?
            };
            for candidate in &plan.candidates {
                let head = self
                    .lifecycle
                    .head_raw(&self.store.paths.current_pointer())
                    .await?
                    .ok_or_else(|| invariant_violation("control MVP head disappeared during GC"))?;
                if head.version != head_version {
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

    async fn fence_reclamation(&self, observed_version: &str) -> Result<String> {
        let pointer = self.store.load_pointer().await?;
        let generation = pointer
            .reclamation_generation
            .checked_add(1)
            .ok_or_else(|| invariant_violation("control MVP reclamation generation exhausted"))?;
        let fenced = ControlMvpPointer {
            reclamation_generation: generation,
            ..pointer
        };
        let bytes = encode_json_limited(
            &fenced,
            MAX_HEAD_JSON_BYTES,
            "control MVP reclamation fence",
        )?;
        match self
            .store
            .storage
            .put(
                &self.store.paths.current_pointer(),
                bytes.clone(),
                AuthorityWritePrecondition::MatchesVersion(observed_version.to_string()),
            )
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(version),
            Ok(WriteResult::PreconditionFailed { .. }) => Err(CatalogError::CasFailed {
                message: "control MVP authority changed before reclamation fence".to_string(),
            }),
            Err(error) => {
                // Pair the read-back bytes with an exact version. A later authority
                // publication invalidates this plan even if its generation matches.
                let before = self
                    .store
                    .storage
                    .head(&self.store.paths.current_pointer())
                    .await?;
                let visible = self
                    .store
                    .get_json(&self.store.paths.current_pointer(), MAX_HEAD_JSON_BYTES)
                    .await?;
                let after = self
                    .store
                    .storage
                    .head(&self.store.paths.current_pointer())
                    .await?;
                if let (Some(before), Some(after)) = (before, after)
                    && before.version == after.version
                    && visible == bytes
                {
                    return Ok(after.version);
                }
                Err(ambiguous_authority_outcome(format!(
                    "control MVP reclamation fence could not be reconciled: {error}"
                )))
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn plan_gc_page_inner(
        &self,
        now: DateTime<Utc>,
        additional_protected_paths: impl IntoIterator<Item = String>,
        continuation: Option<&str>,
    ) -> Result<ControlMvpGcPlan> {
        if let Some(cursor) = continuation {
            if let Some(plan) = maintenance::expired_pin_page(self, now, cursor).await? {
                return Ok(plan);
            }
        }
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
        let next_continuation = inventory_page
            .next_start_after
            .clone()
            .or_else(|| Some(maintenance::pin_gc_cursor().to_string()));

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
        self.protect_manifest_closure(
            &pointer.manifest_id,
            Some(&pointer.manifest_checksum_sha256),
            &mut current_closure,
        )
        .await?;
        candidates.retain(|candidate| !current_closure.contains(&candidate.path));

        // Inventory active external pins inside the same retention epoch as the
        // fence. Caller-supplied paths are not a substitute for durable roots.
        let mut roots = RetainedAuthorityRoots::new(&self.lifecycle, now);
        while let Some(root) = roots.next().await? {
            // Existing retained records may include provider objects that are
            // not reachable through a domain authority. Preserve their exact
            // paths as well, including records written by older publishers.
            candidates.retain(|candidate| !root.required_paths.contains(&candidate.path));
            candidates.retain(|candidate| {
                !root
                    .protected_prefixes
                    .iter()
                    .any(|prefix| candidate.path.starts_with(prefix))
            });
            for reference in root.authorities {
                if reference.scope() != &self.store.scope {
                    continue;
                }
                let closure = self.protected_reference_closure(&reference).await?;
                candidates.retain(|candidate| !closure.contains(&candidate.path));
            }
        }

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
                    self.protect_manifest_closure(manifest_id, None, &mut retained_closure)
                        .await?;
                } else if path.starts_with(&checkpoints_prefix) {
                    let checkpoint_id = path
                        .strip_prefix(&checkpoints_prefix)
                        .and_then(|suffix| suffix.strip_suffix(".json"))
                        .filter(|id| !id.is_empty() && !id.contains('/'))
                        .ok_or_else(|| {
                            invariant_violation("noncanonical control MVP checkpoint path")
                        })?;
                    let bytes = self.store.get_json(&path, MAX_CONTROL_JSON_BYTES).await?;
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
                        let source = self
                            .store
                            .load_manifest_with_expected_checksum(
                                &checkpoint.manifest_id,
                                Some(&checkpoint.manifest_checksum_sha256),
                            )
                            .await?;
                        checkpoint.validate_source(&source)?;
                        retained_closure.insert(path);
                        self.protect_manifest_closure(
                            &checkpoint.manifest_id,
                            Some(&checkpoint.manifest_checksum_sha256),
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
        expected_digest: Option<&str>,
        protected: &mut BTreeSet<String>,
    ) -> Result<()> {
        let manifest_path = self.store.paths.manifest_object(manifest_id);
        if !protected.insert(manifest_path) {
            return Ok(());
        }
        let manifest = self
            .store
            .load_manifest_with_expected_checksum(manifest_id, expected_digest)
            .await?;
        for reference in manifest.owning_states() {
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

    async fn protected_reference_closure(
        &self,
        reference: &PersistedAuthorityReference,
    ) -> Result<BTreeSet<String>> {
        if reference.implementation() != IMPLEMENTATION {
            return Err(validation_failed(
                "retained authority implementation does not match control store",
            ));
        }
        validate_control_mvp_authority_format(&self.store.paths, reference)?;
        let checksum = reference
            .manifest_sha256()
            .strip_prefix("sha256:")
            .ok_or_else(|| validation_failed("retained manifest digest is malformed"))?;
        let manifest = self
            .store
            .load_manifest_with_expected_checksum(reference.manifest_id(), Some(checksum))
            .await?;
        if manifest.logical_sequence != reference.logical_sequence() {
            return Err(invariant_violation(
                "retained manifest logical sequence mismatch",
            ));
        }
        let mut protected = BTreeSet::new();
        self.protect_manifest_closure(reference.manifest_id(), Some(checksum), &mut protected)
            .await?;
        if let Some(path) = reference.checkpoint_path() {
            let bytes = self.store.get_json(path, MAX_CONTROL_JSON_BYTES).await?;
            if Some(prefixed_sha256(&bytes).as_str()) != reference.checkpoint_sha256() {
                return Err(invariant_violation("retained checkpoint checksum mismatch"));
            }
            let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
                &bytes,
                "control-mvp-checkpoint",
                MAX_CONTROL_JSON_BYTES,
                "retained checkpoint",
            )?;
            checkpoint.validate(&self.store.scope, &checkpoint.checkpoint_id)?;
            checkpoint.validate_source(&manifest)?;
            if self
                .store
                .paths
                .checkpoint_object(&checkpoint.checkpoint_id)
                != path
                || checkpoint.manifest_id != reference.manifest_id()
                || checkpoint.manifest_checksum_sha256 != checksum
                || checkpoint.logical_sequence != reference.logical_sequence()
            {
                return Err(invariant_violation(
                    "retained checkpoint authority binding mismatch",
                ));
            }
            protected.insert(path.to_string());
            for segment in &checkpoint.states {
                Self::protect_segment(&self.store.paths, segment, &mut protected);
            }
        }
        Ok(protected)
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
            "maintenance/",
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

    #[cfg(test)]
    async fn test_consolidate_pending(
        &self,
        binding: DurableAuthorityBinding,
    ) -> Result<Option<ControlMvpMaintenanceOutcome>> {
        let worker = DurableMaintenanceWorker::new(
            self.lifecycle.clone(),
            self.store.scope.clone(),
            binding,
        )?
        .with_fixture_store(self.store.clone());
        Box::pin(fixture_driver::consolidate_pending(&worker)).await
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
#[derive(Debug, Clone, Eq)]
pub struct ControlMvpProjectionOutboxRecord {
    observed_root: Option<StateToken>,
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
/// Version 6 binds the exact transaction/history reference, observed reclamation
/// generation and exact HEAD identity. Versions 1 through 5 are supersession-only;
/// recovery must replan them before writing any artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ControlMvpRestorePlan {
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_ref: Option<ControlMvpTxRef>,
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
    observed_reclamation_generation: u64,
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
    #[serde(default)]
    transaction_ref: Option<ControlMvpTxRef>,
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
    observed_reclamation_generation: Option<u64>,
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
            transaction_ref: if wire.version == RESTORE_PLAN_VERSION {
                Some(wire.transaction_ref.ok_or_else(|| {
                    serde::de::Error::custom("restore plan lacks transaction history witness")
                })?)
            } else {
                wire.transaction_ref
            },
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
            observed_reclamation_generation: if wire.version == RESTORE_PLAN_VERSION {
                wire.observed_reclamation_generation.ok_or_else(|| {
                    serde::de::Error::custom(
                        "control MVP restore plan is missing observed_reclamation_generation",
                    )
                })?
            } else {
                wire.observed_reclamation_generation.unwrap_or(0)
            },
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
    fn transaction_reference(&self) -> Result<&ControlMvpTxRef> {
        let reference = self
            .transaction_ref
            .as_ref()
            .ok_or_else(|| validation_failed("restore plan lacks transaction reference"))?;
        if reference.tx_id != self.transaction_id
            || reference.sequence != self.result_logical_sequence
            || format!("sha256:{}", reference.checksum_sha256) != self.transaction_sha256
            || reference.size_bytes == 0
            || reference.size_bytes > MAX_TRANSACTION_JSON_BYTES as u64
        {
            return Err(invariant_violation(
                "restore transaction reference mismatch",
            ));
        }
        reference
            .history
            .validate(&ControlMvpScopeDoc::from(&self.scope), reference.sequence)?;
        Ok(reference)
    }
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
            RESTORE_PLAN_VERSION_V1
                | RESTORE_PLAN_VERSION_V2
                | RESTORE_PLAN_VERSION_V3
                | RESTORE_PLAN_VERSION_V4
                | RESTORE_PLAN_VERSION_V5
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
        self.transaction_reference()?;
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
            || (self.current_base_kind == ControlMvpRestoreCurrentBaseKind::Empty
                && self.result_logical_sequence <= self.source.logical_sequence())
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
        let suffix = format!("{suffix}-rg-{:020}", self.observed_reclamation_generation);
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
            || (self.version < RESTORE_PLAN_VERSION_V3 && self.checkpoint_interval.is_some())
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
    ) -> Result<RestoreParticipantInspection> {
        let planned_tx_ref = plan.transaction_reference()?;
        let tx = self.store.load_tx(planned_tx_ref).await?;
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
            .get_json(&plan.candidate_manifest_path, MAX_CONTROL_JSON_BYTES)
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
        let base_manifest = self
            .store
            .load_manifest_with_expected_checksum(
                &plan.base_manifest_id,
                Some(manifest.parent_manifest_sha256.as_deref().ok_or_else(|| {
                    invariant_violation("restore candidate parent witness absent")
                })?),
            )
            .await?;
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
            || manifest.tx_refs.last() != Some(planned_tx_ref)
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
            reclamation_generation: manifest.reclamation_generation,
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
            token: self
                .store
                .token(
                    plan.candidate_manifest_id.clone(),
                    plan.result_logical_sequence,
                )
                .with_manifest_witness(sha256_hex(&manifest_bytes)),
            evidence,
        })
    }
}

impl PartialEq for ControlMvpProjectionOutboxRecord {
    fn eq(&self, other: &Self) -> bool {
        self.record_id == other.record_id
            && self.payload == other.payload
            && self.origin_sequence == other.origin_sequence
    }
}

impl ControlMvpProjectionOutboxRecord {
    /// Creates a projection outbox record for staging in a transaction.
    #[must_use]
    pub fn new(record_id: impl Into<String>, payload: Bytes) -> Self {
        Self {
            record_id: record_id.into(),
            payload,
            observed_root: None,
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
            observed_root: None,
            origin_sequence: Some(origin_sequence),
        }
    }
}

/// Concrete control-MVP transaction with MVP-only staging helpers.
pub struct ControlMvpTxn {
    store: ControlMvpStateStore,
    base: TransactionBase,
    reads: TransactionReads,
    nonce: u128,
    #[cfg(any(test, feature = "test-utils"))]
    eager_base: Option<ControlMvpBase>,
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
            self.base.logical_sequence(),
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
    pub async fn stage_projection_outbox(
        &mut self,
        record: ControlMvpProjectionOutboxRecord,
    ) -> Result<()> {
        self.ensure_outbox_id_available(&record.record_id, "projection outbox record")
            .await?;
        self.reads
            .reserve(record.record_id.len() + record.payload.len() + 96, 1)?;
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
    pub async fn stage_projection_intent(
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
            self.base.logical_sequence(),
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
        self.ensure_outbox_id_available(&staged.intent_id, "projection intent")
            .await?;
        self.reads.reserve(
            staged.intent_id.len() + staged.projection_kind.len() + staged.payload.len() + 96,
            1,
        )?;
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
    pub async fn trim_projection_outbox(
        &mut self,
        targets: impl IntoIterator<Item = ControlMvpOutboxTrimTarget>,
    ) -> Result<()> {
        let mut staged = BTreeMap::new();
        let mut bytes = 0_usize;
        for target in targets {
            if self
                .outbox_trim
                .iter()
                .any(|entry| entry.record_id == target.record_id)
                || staged.contains_key(&target.record_id)
            {
                return Err(precondition_failed(
                    "projection outbox record is already staged for trimming",
                ));
            }
            let present = self
                .base_outbox_record(&target.record_id)
                .await?
                .ok_or_else(|| {
                    precondition_failed(
                        "cannot trim projection outbox record: not present in current state",
                    )
                })?;
            if present.origin_sequence != Some(target.origin_sequence) {
                return Err(precondition_failed(
                    "cannot trim a different incarnation of the same record id",
                ));
            }
            bytes = bytes.saturating_add(target.record_id.len() + 96);
            self.reads.check_essential(bytes, staged.len() + 1)?;
            staged.insert(
                target.record_id.clone(),
                ControlMvpOutboxTrimEntry {
                    record_id: target.record_id,
                    origin_sequence: target.origin_sequence,
                },
            );
        }
        // No mutation before all awaited validation completes: errors and
        // cancellation cannot expose a partially staged batch.
        self.reads.reserve(bytes, staged.len())?;
        self.outbox_trim.extend(staged.into_values());
        Ok(())
    }

    /// Commits the transaction and returns the resulting state token.
    ///
    /// # Errors
    ///
    /// Returns an error when artifact writes fail, preconditions are not met, or
    /// pointer CAS publication loses to another writer.
    pub async fn commit(self) -> Result<CommitOutcome> {
        Box::pin(self.commit_inner()).await
    }

    #[allow(clippy::too_many_lines)]
    #[allow(unused_mut, reason = "test-only eager reference consumes its snapshot")]
    async fn commit_inner(mut self) -> Result<CommitOutcome> {
        // This is the complete Gate 3 publication boundary. Selective caches
        // never substitute for replay or redundant-anchor equivalence checks.
        #[cfg(any(test, feature = "test-utils"))]
        let base = match self.eager_base.take() {
            Some(base) => base,
            None => {
                cost::phase(
                    "commit_replay",
                    self.base.materialize_for_commit(&self.store),
                )
                .await?
            }
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let base = cost::phase(
            "commit_replay",
            self.base.materialize_for_commit(&self.store),
        )
        .await?;
        self.reads.validate(&base.state)?;
        for precondition in &self.preconditions {
            base.state.validate_precondition(precondition)?;
        }
        validate_publication_epoch(self.store.writer_epoch, base.writer_epoch)?;
        if base.tx_refs.is_empty() && !base.base_states.is_empty() {
            cost::phase(
                "commit_replay",
                self.store
                    .verify_materialized_state(&base.base_states, &base.state),
            )
            .await?;
        }

        let rendering_phase = cost::PhaseGuard::enter("candidate_rendering");
        let next_sequence = next_logical_sequence(
            base.state.logical_sequence,
            "committing a control MVP transaction",
        )?;
        let mut committed_token = self.store.token(self.manifest_id.clone(), next_sequence);
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
            history: HistoryLink::default(),
            reclamation_generation: base.reclamation_generation,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            tx_id: self.tx_id.clone(),
            base_manifest_id: base.manifest_id.clone(),
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
        tx.history = HistoryLink::new(&tx, &base.state.history_root)?;
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
        self.store
            .validate_rendered_transaction(&tx, &l0_bytes, &l0_index_bytes)?;
        let tx_bytes = encode_envelope_limited(
            "control-mvp-tx",
            &tx,
            MAX_TRANSACTION_JSON_BYTES,
            "control MVP transaction",
        )?;
        let tx_checksum = sha256_hex(&tx_bytes);
        let candidate_tx_ref = ControlMvpTxRef {
            size_bytes: tx_bytes.len() as u64,
            history: tx.history.clone(),
            tx_id: self.tx_id.clone(),
            sequence: next_sequence,
            checksum_sha256: tx_checksum.clone(),
        };
        let mut candidate_state = base.state.clone();
        candidate_state.apply_tx(&tx)?;

        let mut tx_refs = base.tx_refs.clone();
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
            base.layout_generation,
            tx_refs.len(),
        )?;

        let mut manifest = ControlMvpManifest {
            history_anchor: base.history_anchor.clone(),
            history_root: candidate_state.history_root.clone(),
            physical_root: String::new(),
            equivalence: None,
            parent_manifest_sha256: base.manifest_checksum_sha256.clone(),
            reclamation_generation: base.reclamation_generation,
            format_version: CONTROL_MVP_FORMAT_VERSION,
            implementation: IMPLEMENTATION.to_string(),
            scope: ControlMvpScopeDoc::from(&self.store.scope),
            manifest_id: self.manifest_id.clone(),
            logical_sequence: next_sequence,
            base_manifest_id: base.manifest_id,
            writer_epoch: self.store.writer_epoch,
            layout_generation: base.layout_generation,
            base_states: base.base_states,
            anchor_states,
            tx_refs,
            state_checksum_sha256: candidate_state.checksum()?,
            maintenance_intent,
        };
        manifest.physical_root = manifest.physical_digest()?;
        manifest.validate(&self.store.scope, &manifest.manifest_id)?;
        let manifest_bytes = encode_envelope_limited(
            "control-mvp-manifest",
            &manifest,
            MAX_CONTROL_JSON_BYTES,
            "control MVP manifest",
        )?;
        let manifest_checksum = sha256_hex(&manifest_bytes);
        committed_token.expected_manifest_sha256 = Some(manifest_checksum.clone());
        let pointer = ControlMvpPointer {
            reclamation_generation: base.reclamation_generation,
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
        let precondition = base.pointer_version.map_or(
            AuthorityWritePrecondition::DoesNotExist,
            AuthorityWritePrecondition::MatchesVersion,
        );

        // Candidate replay, required-anchor rendering, manifest encoding, and
        // head encoding all complete before the first immutable artifact is
        // published. A capacity failure therefore leaves no orphan candidate.
        drop(rendering_phase);
        cost::phase("candidate_publication", async {
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
            Ok::<(), CatalogError>(())
        })
        .await?;
        let pointer_write = cost::phase(
            "head_cas",
            self.store.storage.put(
                &self.store.paths.current_pointer(),
                pointer_bytes.clone(),
                precondition,
            ),
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
                    .get_json(&self.store.paths.current_pointer(), MAX_HEAD_JSON_BYTES)
                    .await
                    .is_ok_and(|current| current == pointer_bytes);
                if exact_pointer_match {
                    Ok(CommitOutcome::new(committed_token, projection_intents))
                } else {
                    let visible_lineage_contains_candidate =
                        match self.store.load_current_base_state().await {
                            Ok(visible) => {
                                match self.store.tx_in_lineage(&visible, &candidate_tx_ref).await {
                                    Ok(found) => found,
                                    Err(
                                        error @ (CatalogError::InvariantViolation { .. }
                                        | CatalogError::Validation { .. }),
                                    ) => {
                                        return Err(error);
                                    }
                                    Err(_) => false,
                                }
                            }
                            Err(
                                error @ (CatalogError::InvariantViolation { .. }
                                | CatalogError::Validation { .. }),
                            ) => return Err(error),
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
            let manifest = self
                .load_manifest_with_expected_checksum(
                    token.authority_manifest_id(),
                    Some(token.manifest_witness()?),
                )
                .await?;
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
                Some(
                    self.token(pointer.manifest_id, pointer.logical_sequence)
                        .with_manifest_witness(pointer.manifest_checksum_sha256),
                ),
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
        let manifest = self
            .load_manifest_with_expected_checksum(
                token.authority_manifest_id(),
                Some(token.manifest_witness()?),
            )
            .await?;
        if manifest.logical_sequence != token.logical_sequence() {
            return Err(invariant_violation(
                "StateToken logical sequence does not match manifest",
            ));
        }
        self.validate_manifest_read_metadata(&manifest)?;
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
        let checkpoint = self.load_checkpoint(&token).await?;
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
        checkpoint.validate_source(&manifest)?;
        let mut state = self.load_state_snapshots(&checkpoint.states).await?;
        checkpoint.validate_state(&mut state)?;
        Ok(Box::new(ControlMvpRetainedReader {
            scope: self.scope.clone(),
            token: self
                .token(checkpoint.manifest_id, checkpoint.logical_sequence)
                .with_manifest_witness(checkpoint.manifest_checksum_sha256),
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
        self.load_manifest_for_pointer(&pointer).await?;
        Ok(self
            .token(pointer.manifest_id, pointer.logical_sequence)
            .with_manifest_witness(pointer.manifest_checksum_sha256))
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
        let publication = async {
            let (checkpoint, rendered) = self.prepare_checkpoint(&opts).await?;
            // Incomplete segments do not publish protection. A failed staging
            // write can leave only orphan objects, so it need not hold the epoch.
            self.write_rendered_state_snapshots(&rendered).await?;
            let bytes = encode_envelope_limited(
                "control-mvp-checkpoint",
                &checkpoint,
                MAX_CONTROL_JSON_BYTES,
                "checkpoint",
            )?;
            let witness = sha256_hex(&bytes);
            epoch
                .run_external_mutation(self.write_checkpoint(&checkpoint, bytes))
                .await?;
            Ok(self
                .checkpoint_token(checkpoint.checkpoint_id)
                .with_checkpoint_witness(witness))
        }
        .await;
        // A transport error may return before a remote immutable PUT completes.
        // Only successful publication (including exact readback reconciliation)
        // permits settlement. Otherwise the durable epoch remains in flight.
        let settlement = epoch.settle().await;
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
        self.validate_state_token_protection(token, Utc::now())
            .await?;
        let manifest_path = self.paths.manifest_object(token.authority_manifest_id());
        let bytes = self
            .get_json(&manifest_path, MAX_CONTROL_JSON_BYTES)
            .await?;
        validate_raw_checksum(
            &bytes,
            Some(token.manifest_witness()?),
            "persisted state token witness",
        )?;
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
        let checkpoint_bytes = self
            .get_json(&checkpoint_path, MAX_CONTROL_JSON_BYTES)
            .await?;
        validate_raw_checksum(
            &checkpoint_bytes,
            Some(token.checkpoint_witness()?),
            "persisted checkpoint token witness",
        )?;
        let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
            &checkpoint_bytes,
            "control-mvp-checkpoint",
            MAX_CONTROL_JSON_BYTES,
            "control MVP checkpoint",
        )?;
        checkpoint.validate(&self.scope, token.checkpoint_id())?;
        self.validate_checkpoint_protection(&checkpoint, Utc::now())
            .await?;

        let manifest_path = self.paths.manifest_object(&checkpoint.manifest_id);
        let manifest_bytes = self
            .get_json(&manifest_path, MAX_CONTROL_JSON_BYTES)
            .await?;
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
        checkpoint.validate_source(&manifest)?;
        let mut state = self.load_state_snapshots(&checkpoint.states).await?;
        checkpoint.validate_state(&mut state)?;

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

    #[allow(clippy::too_many_lines)] // Keep the persisted-reference validation boundary together.
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
        let manifest_bytes = self
            .get_json(&manifest_path, MAX_CONTROL_JSON_BYTES)
            .await?;
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
                let token = self
                    .token(
                        reference.manifest_id().to_string(),
                        reference.logical_sequence(),
                    )
                    .with_manifest_witness(sha256_hex(&manifest_bytes));
                self.read_at(token).await
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
                let checkpoint_bytes = self
                    .get_json(checkpoint_path, MAX_CONTROL_JSON_BYTES)
                    .await?;
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
                checkpoint.validate_source(&manifest)?;
                self.validate_checkpoint_protection(&checkpoint, now)
                    .await?;
                if checkpoint.manifest_checksum_sha256 != sha256_hex(&manifest_bytes) {
                    return Err(invariant_violation(
                        "persisted checkpoint does not match authority manifest",
                    ));
                }
                self.read_checkpoint(
                    self.checkpoint_token(checkpoint_id.to_string())
                        .with_checkpoint_witness(sha256_hex(&checkpoint_bytes)),
                )
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
        let planned_tx_ref = plan.transaction_reference()?;
        let in_lineage = self
            .store
            .tx_in_lineage(&stable.candidate_parent, planned_tx_ref)
            .await?;
        if in_lineage {
            return self.inspect_visible_restore(plan).await;
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

#[derive(Debug, Clone)]
struct ControlMvpBase {
    history_anchor: HistoryAnchor,
    manifest_checksum_sha256: Option<String>,
    reclamation_generation: u64,
    pointer_version: Option<String>,
    manifest_id: Option<String>,
    writer_epoch: u64,
    layout_generation: u64,
    state: ReplayState,
    base_states: Vec<ControlMvpStateRef>,
    tx_refs: Vec<ControlMvpTxRef>,
}

struct AncestorTransition {
    sequence: u64,
    layout: u64,
    checksum: String,
    predecessor_digest: String,
    parent_history_root: String,
    equivalence: Option<RewriteEquivalence>,
}

struct StableRestoreBase {
    current: ControlMvpBase,
    candidate_parent: ControlMvpBase,
    current_base_kind: ControlMvpRestoreCurrentBaseKind,
    writer_epoch: u64,
    pointer_bytes: Bytes,
}

struct RenderedControlMvpRestore {
    transaction_ref: ControlMvpTxRef,
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

struct BlockScanBudget {
    blocks: usize,
    segments: usize,
    bytes: usize,
}

type BinaryKeyBounds = Option<(Vec<u8>, Vec<u8>)>;

struct BlockScanCursor {
    range: Option<KeyRange>,
    expected_bounds: BTreeMap<String, BinaryKeyBounds>,
    references: std::collections::VecDeque<ControlMvpSegmentRef>,
    current: Option<(ControlMvpSegmentRef, ControlMvpSegmentIndex, usize, bool)>,
    rows: std::collections::VecDeque<ControlMvpSegmentRow>,
}

impl BlockScanCursor {
    fn new(references: std::collections::VecDeque<ControlMvpSegmentRef>) -> Self {
        Self {
            range: None,
            expected_bounds: BTreeMap::new(),
            references,
            current: None,
            rows: std::collections::VecDeque::new(),
        }
    }
    fn may_have_more(&self) -> bool {
        !self.rows.is_empty()
            || !self.references.is_empty()
            || self
                .current
                .as_ref()
                .is_some_and(|(_, index, next, _)| *next < index.blocks.len())
    }
    async fn fill(
        &mut self,
        store: &ControlMvpStateStore,
        prefix: &[u8],
        start: Option<&[u8]>,
        budget: &mut BlockScanBudget,
    ) -> Result<bool> {
        while self.rows.is_empty() {
            if self.current.is_none() {
                let Some(reference) = self.references.pop_front() else {
                    return Ok(true);
                };
                let (_, index) = store.load_segment_index(&reference).await?;
                if let Some(bounds) = self.expected_bounds.get(&reference.segment_id)
                    && &index_key_bounds(&index)? != bounds
                {
                    return Err(invariant_violation(
                        "scan directory bounds differ from manifest",
                    ));
                }
                self.current = Some((reference, index, 0, false));
            }
            let (reference, index, next, charged) = self
                .current
                .as_mut()
                .ok_or_else(|| invariant_violation("scan cursor lost directory"))?;
            let Some(block) = index.blocks.get(*next) else {
                self.current = None;
                continue;
            };
            let bounds = block_key_bounds(block)?;
            if self.range.as_ref().is_some_and(|range| {
                bounds.as_ref().is_none_or(|(min, max)| {
                    max.as_slice() < range.start() || min.as_slice() >= range.end()
                })
            }) || block.record_kind != Some(SEGMENT_RECORD_KV)
                || !key_bounds_overlap_prefix(bounds.as_ref(), prefix)
                || bounds
                    .as_ref()
                    .is_some_and(|(_, max)| start.is_some_and(|start| max.as_slice() <= start))
            {
                *next += 1;
                continue;
            }
            if budget.blocks == 0
                || (!*charged && budget.segments == 0)
                || block.length > budget.bytes as u64
            {
                return Ok(false);
            }
            budget.blocks -= 1;
            budget.bytes -= usize::try_from(block.length)
                .map_err(|_| invariant_violation("block length exceeds address space"))?;
            if !*charged {
                budget.segments -= 1;
                *charged = true;
            }
            self.rows = store
                .load_block(reference, block)
                .await?
                .into_iter()
                .filter(|row| {
                    self.range
                        .as_ref()
                        .is_none_or(|range| key_in_range(&row.key, range))
                        && row.key.starts_with(prefix)
                        && start.is_none_or(|start| row.key.as_slice() > start)
                })
                .collect();
            *next += 1;
        }
        Ok(true)
    }
}

fn block_key_bounds(block: &ControlMvpBlock) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    match (&block.min_key_hex, &block.max_key_hex) {
        (Some(min), Some(max)) => Ok(Some((
            hex::decode(min).map_err(|_| invariant_violation("invalid block minimum"))?,
            hex::decode(max).map_err(|_| invariant_violation("invalid block maximum"))?,
        ))),
        (None, None) => Ok(None),
        _ => Err(invariant_violation("incomplete block bounds")),
    }
}

fn stored_row_value(row: ControlMvpSegmentRow) -> StoredValue {
    StoredValue {
        bytes: Bytes::from(row.value.unwrap_or_default()),
        generation: row.generation,
        tombstone: row.tombstone,
    }
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ReplayState {
    history_root: String,
    logical_sequence: u64,
    kv: BTreeMap<Vec<u8>, StoredValue>,
    outbox: Vec<ControlMvpProjectionOutboxRecord>,
}

impl ReplayState {
    fn append_snapshot(&mut self, shard: ControlMvpStateObject) -> Result<()> {
        #[cfg(feature = "test-utils")]
        cost::record(8, shard.entries.len() + shard.outbox.len());
        if shard.logical_sequence != self.logical_sequence
            || shard
                .kv_start_ordinal
                .is_some_and(|start| start != self.kv.len() as u64)
            || shard
                .outbox_start_ordinal
                .is_some_and(|start| start != self.outbox.len() as u64)
        {
            return Err(invariant_violation(
                "discontinuous L1 shard sequence or ordinals",
            ));
        }
        for entry in shard.entries {
            let value = StoredValue {
                tombstone: entry.value.is_none(),
                bytes: Bytes::from(entry.value.unwrap_or_default()),
                generation: entry.generation,
            };
            if self.kv.insert(entry.key, value).is_some() {
                return Err(invariant_violation("duplicate L1 key"));
            }
        }
        let mut ids = self
            .outbox
            .iter()
            .map(|record| record.record_id.clone())
            .collect::<BTreeSet<_>>();
        for entry in shard.outbox {
            if !ids.insert(entry.record_id.clone()) {
                return Err(invariant_violation("duplicate L1 outbox incarnation"));
            }
            self.outbox.push(entry.to_record());
        }
        Ok(())
    }
    fn empty(scope: &StateScope) -> Self {
        Self {
            history_root: integrity::genesis(&ControlMvpScopeDoc::from(scope)).root,
            ..Self::default()
        }
    }
    fn apply_tx(&mut self, tx: &ControlMvpTxObject) -> Result<()> {
        #[cfg(feature = "test-utils")]
        cost::record(8, tx.writes.len() + tx.outbox.len() + tx.outbox_trim.len());
        tx.history.validate(&tx.scope, tx.sequence)?;
        if tx.history.preceding_root != self.history_root
            || integrity::mutation_digest(tx) != tx.history.mutation_sha256
        {
            return Err(invariant_violation(
                "replayed mutation differs from logical history",
            ));
        }
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
        self.history_root.clone_from(&tx.history.resulting_root);
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
            hash_tag(&mut hasher, u8::from(value.tombstone));
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
                PointWitness::Absent => hash_tag(&mut hasher, 0),
                PointWitness::Present(generation) => {
                    hash_tag(&mut hasher, 1);
                    hash_u64(&mut hasher, generation);
                }
                PointWitness::Tombstone(generation) => {
                    hash_tag(&mut hasher, 2);
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
        #[cfg(feature = "test-utils")]
        cost::record(9, 1);
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
        #[cfg(feature = "test-utils")]
        cost::record(12, bytes.len());
        Ok(sha256_hex(&bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    format_version: u32,
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
    reclamation_generation: u64,
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
        if !integrity::valid_immutable_id(&self.manifest_id)
            || !valid_raw_digest(&self.manifest_checksum_sha256)
        {
            return Err(invariant_violation("invalid pointer manifest reference"));
        }
        Ok(())
    }
}

/// Reference to an immutable state-snapshot object, bound by raw-byte checksum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ControlMvpStateRef {
    segment_size_bytes: u64,
    index_size_bytes: u64,
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
    segment_size_bytes: u64,
    index_size_bytes: u64,
    segment_id: String,
    level: ControlMvpSegmentLevel,
    logical_sequence: u64,
    checksum_sha256: String,
    index_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlMvpSegmentIndex {
    blocks: Vec<ControlMvpBlock>,
    bloom_mode: BloomMode,
    bloom_hash_version: u32,
    bloom_probes: u32,
    distinct_kv_keys: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlMvpBlock {
    offset: u64,
    length: u64,
    record_kind: Option<u8>,
    min_key_hex: Option<String>,
    max_key_hex: Option<String>,
    row_count: u64,
    min_ordinal: Option<u64>,
    max_ordinal: Option<u64>,
    checksum_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum BloomMode {
    Empty,
    Enabled,
    Disabled,
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
    kv_start_ordinal: Option<u64>,
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
            kv_start_ordinal: (!state.kv.is_empty()).then_some(0),
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
    history_anchor: HistoryAnchor,
    history_root: String,
    physical_root: String,
    equivalence: Option<RewriteEquivalence>,
    parent_manifest_sha256: Option<String>,
    reclamation_generation: u64,
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
        if !integrity::valid_immutable_id(&self.manifest_id)
            || self
                .base_manifest_id
                .as_deref()
                .is_some_and(|id| !integrity::valid_immutable_id(id))
            || !valid_raw_digest(&self.state_checksum_sha256)
        {
            return Err(invariant_violation(
                "invalid manifest identity or state digest",
            ));
        }
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
        if self.base_manifest_id.is_some() != self.parent_manifest_sha256.is_some()
            || self
                .parent_manifest_sha256
                .as_ref()
                .is_some_and(|digest| !valid_raw_digest(digest))
            || self.base_manifest_id.as_deref() == Some(self.manifest_id.as_str())
        {
            return Err(invariant_violation("invalid manifest parent witness"));
        }
        if self.base_manifest_id.is_none()
            && (self.logical_sequence != 1
                || !self.base_states.is_empty()
                || self.tx_refs.len() != 1
                || self.tx_refs.first().is_none_or(|tx| tx.sequence != 1))
        {
            return Err(invariant_violation(
                "non-genesis manifest has no authenticated parent or logical sequence overflow",
            ));
        }
        let base_sequence = self.validate_base_states()?;
        self.validate_replay_suffix(base_sequence)?;
        self.validate_anchor_states()?;
        self.validate_maintenance_intent(scope)?;
        self.validate_integrity()?;
        Ok(())
    }

    fn validate_base_states(&self) -> Result<Option<u64>> {
        let sequence = integrity::validate_state_refs(&self.base_states)?;
        integrity::validate_state_refs(&self.anchor_states)?;
        let mut ids = BTreeSet::new();
        if self
            .owning_states()
            .any(|reference| !ids.insert(&reference.state_id))
        {
            return Err(invariant_violation("duplicate owning state identity"));
        }
        Ok(sequence)
    }

    fn owning_states(&self) -> impl Iterator<Item = &ControlMvpStateRef> {
        self.base_states.iter().chain(&self.anchor_states)
    }

    fn validate_replay_suffix(&self, base_sequence: Option<u64>) -> Result<()> {
        let mut ids = BTreeSet::new();
        if self.tx_refs.iter().any(|reference| {
            !integrity::valid_immutable_id(&reference.tx_id) || !ids.insert(&reference.tx_id)
        }) {
            return Err(invariant_violation(
                "duplicate or noncanonical transaction identity",
            ));
        }
        if self.tx_refs.windows(2).any(|pair| match pair {
            [first, second] => first.sequence.checked_add(1) != Some(second.sequence),
            _ => true,
        }) || self
            .tx_refs
            .iter()
            .any(|tx| !valid_raw_digest(&tx.checksum_sha256))
        {
            return Err(invariant_violation("invalid manifest transaction suffix"));
        }
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
    size_bytes: u64,
    history: HistoryLink,
    tx_id: String,
    sequence: u64,
    checksum_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpTxObject {
    history: HistoryLink,
    reclamation_generation: u64,
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

/// Exact event incarnation removed by an identified L0 transaction row.
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
        self.history.validate(&self.scope, self.sequence)?;
        if self.history != tx_ref.history {
            return Err(invariant_violation(
                "transaction history differs from owning reference",
            ));
        }
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
            || !valid_raw_digest(&self.l0_segment.checksum_sha256)
            || !valid_raw_digest(&self.l0_segment.index_checksum_sha256)
            || self.l0_segment.segment_size_bytes == 0
            || self.l0_segment.segment_size_bytes > MAX_SEGMENT_BYTES as u64
            || self.l0_segment.index_size_bytes == 0
            || self.l0_segment.index_size_bytes > MAX_SEGMENT_INDEX_BYTES as u64
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
    if !trim_ids.is_disjoint(&outbox_ids) {
        return Err(invariant_violation(
            "outbox ID cannot be trimmed and added in one transaction",
        ));
    }
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
        segment_size_bytes: 0,
        index_size_bytes: 0,
        segment_id: segment_id.to_string(),
        level: ControlMvpSegmentLevel::L0,
        logical_sequence,
        checksum_sha256: String::new(),
        index_checksum_sha256: String::new(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            observed_root: None,
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
            observed_root: None,
            record_id: self.record_id.clone(),
            payload: Bytes::from(self.payload.clone()),
            origin_sequence: self.origin_sequence,
        }
    }
}

const SEGMENT_RECORD_KV: u8 = 0;
const SEGMENT_RECORD_OUTBOX: u8 = 1;
const SEGMENT_RECORD_OUTBOX_TRIM: u8 = 2;

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
    let mut kv_start_ordinal: Option<u64> = None;
    let mut outbox = Vec::new();
    for row in rows {
        if row.logical_sequence != reference.logical_sequence {
            return Err(invariant_violation(
                "control MVP L1 segment row sequence does not match its reference",
            ));
        }
        match row.record_kind {
            SEGMENT_RECORD_KV => {
                let start = *kv_start_ordinal.get_or_insert(row.logical_ordinal);
                if row.generation == 0
                    || row.generation > reference.logical_sequence
                    || row.origin_sequence.is_some()
                    || start.checked_add(entries.len() as u64) != Some(row.logical_ordinal)
                {
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
        kv_start_ordinal,
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
        block_target: limits.block_target,
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

// Writer offsets are derived solely from checked bounds in this loop.
#[allow(clippy::indexing_slicing, clippy::too_many_lines)]
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
    let mut output = Vec::new();
    let mut blocks = Vec::new();
    let mut start = 0;
    loop {
        let mut end = start;
        let mut estimate = 2048_usize;
        while end < rows.len()
            && (end == start
                || (rows[end].record_kind == rows[start].record_kind
                    && estimate < limits.block_target.saturating_sub(2048)))
        {
            estimate = estimate
                .saturating_add(rows[end].key.len())
                .saturating_add(rows[end].value.as_ref().map_or(0, Vec::len))
                .saturating_add(64);
            end += 1;
        }
        let mut block_bytes = encode_arrow_block(&rows[start..end])?;
        while end - start > 1 && block_bytes.len() > MAX_BLOCK_BYTES {
            end = start + (end - start) / 2;
            block_bytes = encode_arrow_block(&rows[start..end])?;
        }
        if blocks.len() == MAX_SEGMENT_BLOCKS {
            return Err(segment_capacity_error(
                level,
                "control MVP segment block count exceeds capacity",
            ));
        }
        blocks.push(block_metadata(
            output.len() as u64,
            &block_bytes,
            &rows[start..end],
        ));
        if output
            .len()
            .checked_add(block_bytes.len())
            .is_none_or(|total| total > limits.bytes)
        {
            return Err(segment_capacity_error(
                level,
                "control MVP segment exceeds the supported byte limit",
            ));
        }
        output.extend_from_slice(&block_bytes);
        start = end;
        if start == rows.len() {
            break;
        }
    }
    if output.len() > limits.bytes {
        return Err(segment_capacity_error(
            level,
            "control MVP segment exceeds the supported byte limit",
        ));
    }
    let segment_bytes = Bytes::from(output);
    let segment_checksum_sha256 = sha256_hex(&segment_bytes);
    let mut index = build_segment_index(
        segment_id,
        level,
        logical_sequence,
        scope,
        rows,
        &segment_bytes,
        segment_checksum_sha256.clone(),
        blocks,
    )?;
    let mut index_bytes = encode_json(&index, "control MVP segment index")?;
    if (index_bytes.len() > limits.index_bytes || index.bloom_mode == BloomMode::Disabled)
        && level == ControlMvpSegmentLevel::L0
    {
        index.bloom_mode = BloomMode::Disabled;
        index.bloom_probes = 0;
        index.bloom_bits_hex.clear();
        index_bytes = encode_json(&index, "control MVP segment index")?;
    } else if index.bloom_mode == BloomMode::Disabled {
        return Err(segment_capacity_error(
            level,
            "control MVP L1 Bloom filter exceeds capacity",
        ));
    }
    if index_bytes.len() > limits.index_bytes {
        return Err(segment_capacity_error(
            level,
            "control MVP segment index exceeds the supported byte limit",
        ));
    }
    let reference = ControlMvpSegmentRef {
        segment_size_bytes: segment_bytes.len() as u64,
        index_size_bytes: index_bytes.len() as u64,
        segment_id: segment_id.to_string(),
        level,
        logical_sequence,
        checksum_sha256: segment_checksum_sha256,
        index_checksum_sha256: sha256_hex(&index_bytes),
    };
    Ok((segment_bytes, index_bytes, reference))
}

fn encode_arrow_block(rows: &[ControlMvpSegmentRow]) -> Result<Vec<u8>> {
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
    Ok(output)
}

fn block_metadata(offset: u64, bytes: &[u8], rows: &[ControlMvpSegmentRow]) -> ControlMvpBlock {
    ControlMvpBlock {
        offset,
        length: bytes.len() as u64,
        record_kind: rows.first().map(|row| row.record_kind),
        min_key_hex: rows.first().map(|row| hex::encode(&row.key)),
        max_key_hex: rows.last().map(|row| hex::encode(&row.key)),
        row_count: rows.len() as u64,
        min_ordinal: rows.iter().map(|row| row.logical_ordinal).min(),
        max_ordinal: rows.iter().map(|row| row.logical_ordinal).max(),
        checksum_sha256: sha256_hex(bytes),
    }
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

#[allow(clippy::too_many_arguments)]
fn build_segment_index(
    segment_id: &str,
    level: ControlMvpSegmentLevel,
    logical_sequence: u64,
    scope: &StateScope,
    rows: &[ControlMvpSegmentRow],
    segment_bytes: &[u8],
    segment_checksum_sha256: String,
    blocks: Vec<ControlMvpBlock>,
) -> Result<ControlMvpSegmentIndex> {
    let keys = rows
        .iter()
        .filter(|row| row.record_kind == SEGMENT_RECORD_KV)
        .map(|row| row.key.as_slice())
        .collect::<Vec<_>>();
    let min_key = keys.first().copied();
    let max_key = keys.last().copied();
    let (bloom_mode, bloom_bits_hex) = sized_bloom(&keys);
    Ok(ControlMvpSegmentIndex {
        record_batch_offsets: blocks.iter().map(|block| block.offset).collect(),
        blocks,
        bloom_mode,
        bloom_hash_version: 1,
        bloom_probes: if bloom_mode == BloomMode::Enabled {
            7
        } else {
            0
        },
        distinct_kv_keys: keys.len() as u64,
        format_version: SEGMENT_FORMAT_VERSION,
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
        bloom_bits_hex,
        segment_checksum_sha256,
    })
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
        let start = usize::try_from(offset)
            .map_err(|_| invariant_violation("Arrow message offset overflow"))?;
        let span = usize::try_from(metadata_length)
            .map_err(|_| invariant_violation("Arrow metadata span overflow"))?;
        let message = preflight_ipc_message(bytes, start, span)?;
        let batch = message
            .header_as_record_batch()
            .ok_or_else(|| invariant_violation("Arrow block message is not a record batch"))?;
        if message.bodyLength() != block.bodyLength()
            || batch.compression().is_some()
            || batch.length() < 0
            || u64::try_from(batch.length()).unwrap_or(u64::MAX) > MAX_SEGMENT_ROWS as u64
        {
            return Err(invariant_violation(
                "Arrow batch length or compression is unsupported",
            ));
        }
        let nodes = batch
            .nodes()
            .ok_or_else(|| invariant_violation("Arrow batch nodes absent"))?;
        if nodes.len() != 8
            || nodes.iter().any(|node| {
                node.length() != batch.length()
                    || node.null_count() < 0
                    || node.null_count() > node.length()
            })
        {
            return Err(invariant_violation("Arrow batch nodes are invalid"));
        }
        let buffers = batch
            .buffers()
            .ok_or_else(|| invariant_violation("Arrow batch buffers absent"))?;
        if buffers.len() > 32
            || buffers.iter().any(|buffer| {
                buffer.offset() < 0
                    || buffer.length() < 0
                    || buffer
                        .offset()
                        .checked_add(buffer.length())
                        .is_none_or(|end| end > message.bodyLength())
            })
        {
            return Err(invariant_violation("Arrow batch buffers are out of bounds"));
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

fn preflight_ipc_message(
    bytes: &[u8],
    offset: usize,
    span: usize,
) -> Result<arrow::ipc::Message<'_>> {
    let end = offset
        .checked_add(span)
        .ok_or_else(|| invariant_violation("IPC metadata span overflow"))?;
    let region = bytes
        .get(offset..end)
        .ok_or_else(|| invariant_violation("IPC metadata span out of bounds"))?;
    let first: [u8; 4] = region
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| invariant_violation("IPC message prefix absent"))?;
    let (prefix, length) = if first == [255; 4] {
        let length: [u8; 4] = region
            .get(4..8)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| invariant_violation("IPC message length absent"))?;
        (8_usize, u32::from_le_bytes(length))
    } else {
        (4_usize, u32::from_le_bytes(first))
    };
    let length =
        usize::try_from(length).map_err(|_| invariant_violation("IPC metadata length overflow"))?;
    let payload = region
        .get(
            prefix
                ..prefix
                    .checked_add(length)
                    .ok_or_else(|| invariant_violation("IPC metadata length overflow"))?,
        )
        .ok_or_else(|| invariant_violation("IPC metadata length exceeds span"))?;
    arrow::ipc::root_as_message_with_opts(&segment_verifier_options(), payload)
        .map_err(|error| segment_serialization_error("preflight IPC message", error))
}

fn segment_verifier_options() -> VerifierOptions {
    VerifierOptions {
        max_depth: MAX_SEGMENT_FOOTER_DEPTH,
        max_tables: MAX_SEGMENT_FOOTER_TABLES,
        max_apparent_size: MAX_SEGMENT_FOOTER_APPARENT_BYTES,
        ignore_missing_null_terminator: false,
    }
}

// SHA-256 always contains 32 bytes; probe positions are reduced to the byte-bounded filter.
#[allow(clippy::indexing_slicing)]
fn bloom_positions(key: &[u8], bits: usize) -> impl Iterator<Item = usize> {
    #[cfg(feature = "test-utils")]
    {
        cost::record(15, 1);
        cost::record(16, key.len());
    }
    let digest = Sha256::digest(key);
    let mut first = [0; 8];
    first.copy_from_slice(&digest[..8]);
    let mut second = [0; 8];
    second.copy_from_slice(&digest[8..16]);
    let first = u64::from_be_bytes(first);
    let second = u64::from_be_bytes(second) | 1;
    (0_u64..7).map(move |probe| {
        usize::try_from(first.wrapping_add(probe.wrapping_mul(second)) % bits as u64)
            .unwrap_or_default()
    })
}

fn sized_bloom(keys: &[&[u8]]) -> (BloomMode, String) {
    if keys.is_empty() {
        return (BloomMode::Empty, String::new());
    }
    let size = keys.len().saturating_mul(10).div_ceil(8);
    if size > MAX_BLOOM_BYTES {
        return (BloomMode::Disabled, String::new());
    }
    let mut bytes = vec![0_u8; size];
    for key in keys {
        for bit in bloom_positions(key, size * 8) {
            if let Some(byte) = bytes.get_mut(bit / 8) {
                *byte |= 1 << (bit % 8);
            }
        }
    }
    (BloomMode::Enabled, hex::encode(bytes))
}

#[allow(clippy::too_many_lines)]
fn validate_segment_directory(index: &ControlMvpSegmentIndex) -> Result<()> {
    if index.blocks.is_empty() || index.blocks.len() > MAX_SEGMENT_BLOCKS {
        return Err(invariant_violation("invalid block count"));
    }
    let mut end = 0_u64;
    let mut count = 0_u64;
    let mut kv_count = 0_u64;
    let mut prior: Option<(u8, Vec<u8>)> = None;
    for block in &index.blocks {
        if block.offset != end
            || block.length == 0
            || block.length > MAX_SEGMENT_BYTES as u64
            || (block.row_count > 1 && block.length > MAX_BLOCK_BYTES as u64)
            || !valid_raw_digest(&block.checksum_sha256)
        {
            return Err(invariant_violation("invalid block span or digest"));
        }
        end = end
            .checked_add(block.length)
            .ok_or_else(|| invariant_violation("block span overflow"))?;
        count = count
            .checked_add(block.row_count)
            .ok_or_else(|| invariant_violation("block count overflow"))?;
        if block.row_count == 0 {
            if index.blocks.len() != 1
                || block.record_kind.is_some()
                || block.min_key_hex.is_some()
                || block.max_key_hex.is_some()
                || block.min_ordinal.is_some()
                || block.max_ordinal.is_some()
            {
                return Err(invariant_violation("invalid empty block"));
            }
        } else {
            let kind = block
                .record_kind
                .ok_or_else(|| invariant_violation("block kind absent"))?;
            if kind > SEGMENT_RECORD_OUTBOX_TRIM
                || (index.level == ControlMvpSegmentLevel::L1 && kind == SEGMENT_RECORD_OUTBOX_TRIM)
            {
                return Err(invariant_violation("invalid block record kind"));
            }
            let minimum = hex::decode(
                block
                    .min_key_hex
                    .as_deref()
                    .ok_or_else(|| invariant_violation("block minimum absent"))?,
            )
            .map_err(|_| invariant_violation("invalid block minimum"))?;
            let maximum = hex::decode(
                block
                    .max_key_hex
                    .as_deref()
                    .ok_or_else(|| invariant_violation("block maximum absent"))?,
            )
            .map_err(|_| invariant_violation("invalid block maximum"))?;
            if minimum > maximum
                || prior
                    .as_ref()
                    .is_some_and(|(k, key)| *k > kind || (*k == kind && key >= &minimum))
                || block
                    .min_ordinal
                    .zip(block.max_ordinal)
                    .is_none_or(|(min, max)| min > max)
            {
                return Err(invariant_violation(
                    "invalid block ordering or ordinal bounds",
                ));
            }
            if kind == SEGMENT_RECORD_KV {
                kv_count = kv_count
                    .checked_add(block.row_count)
                    .ok_or_else(|| invariant_violation("KV count overflow"))?;
            }
            prior = Some((kind, maximum));
        }
    }
    if end != index.segment_size_bytes
        || count != index.row_count
        || count > MAX_SEGMENT_ROWS as u64
        || kv_count != index.distinct_kv_keys
        || index.record_batch_offsets
            != index
                .blocks
                .iter()
                .map(|block| block.offset)
                .collect::<Vec<_>>()
    {
        return Err(invariant_violation("block directory totals mismatch"));
    }
    let kv_blocks = index
        .blocks
        .iter()
        .filter(|block| block.record_kind == Some(SEGMENT_RECORD_KV))
        .collect::<Vec<_>>();
    if index.min_key_hex.as_ref()
        != kv_blocks
            .first()
            .and_then(|block| block.min_key_hex.as_ref())
        || index.max_key_hex.as_ref()
            != kv_blocks
                .last()
                .and_then(|block| block.max_key_hex.as_ref())
    {
        return Err(invariant_violation(
            "directory key bounds differ from KV blocks",
        ));
    }
    let bloom = hex::decode(&index.bloom_bits_hex)
        .map_err(|_| invariant_violation("invalid Bloom bits"))?;
    if index.bloom_hash_version != 1 || bloom.len() > MAX_BLOOM_BYTES {
        return Err(invariant_violation("unsupported Bloom encoding"));
    }
    let valid = match index.bloom_mode {
        BloomMode::Empty => kv_count == 0 && bloom.is_empty() && index.bloom_probes == 0,
        BloomMode::Disabled => bloom.is_empty() && index.bloom_probes == 0,
        BloomMode::Enabled => {
            kv_count > 0 && bloom.len() as u64 * 8 >= kv_count * 10 && index.bloom_probes == 7
        }
    };
    if !valid {
        return Err(invariant_violation("invalid Bloom parameters"));
    }
    validate_segment_index_key_metadata(index)
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
    validate_version_header(index_bytes, SEGMENT_FORMAT_VERSION, "segment directory")?;
    let index: ControlMvpSegmentIndex = decode_json(index_bytes, "control MVP segment index")?;
    validate_segment_index_identity(&index, reference, scope)?;
    validate_segment_directory(&index)?;
    if bytes.len() as u64 != reference.segment_size_bytes
        || index_bytes.len() as u64 != reference.index_size_bytes
    {
        return Err(invariant_violation(
            "segment or index length differs from owning reference",
        ));
    }
    let mut rows = Vec::new();
    for block in &index.blocks {
        let end = block
            .offset
            .checked_add(block.length)
            .ok_or_else(|| invariant_violation("block span overflow"))?;
        let data = bytes
            .get(
                usize::try_from(block.offset)
                    .map_err(|_| invariant_violation("block offset overflow"))?
                    ..usize::try_from(end)
                        .map_err(|_| invariant_violation("block end overflow"))?,
            )
            .ok_or_else(|| invariant_violation("block span outside segment"))?;
        rows.extend(decode_block_rows(data, block)?);
    }
    let mut expected_index = build_segment_index(
        &reference.segment_id,
        reference.level,
        reference.logical_sequence,
        scope,
        &rows,
        bytes,
        sha256_hex(bytes),
        index.blocks.clone(),
    )?;
    if index.bloom_mode == BloomMode::Disabled {
        expected_index.bloom_mode = BloomMode::Disabled;
        expected_index.bloom_bits_hex.clear();
        expected_index.bloom_probes = 0;
    }
    if index != expected_index {
        return Err(invariant_violation(
            "control MVP segment directory does not match contents",
        ));
    }
    Ok(rows)
}

fn decode_block_rows(bytes: &[u8], block: &ControlMvpBlock) -> Result<Vec<ControlMvpSegmentRow>> {
    if bytes.len() as u64 != block.length {
        return Err(invariant_violation("block length mismatch"));
    }
    validate_raw_checksum(bytes, Some(&block.checksum_sha256), "block digest")?;
    if preflight_arrow_segment(bytes)?.record_batch_offsets.len() != 1 {
        return Err(invariant_violation("block must contain one batch"));
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
    if block_metadata(block.offset, bytes, &rows) != *block {
        return Err(invariant_violation("decoded block metadata mismatch"));
    }
    if rows.windows(2).any(|pair| match pair {
        [first, second] => first.record_kind != second.record_kind || first.key >= second.key,
        _ => true,
    }) {
        return Err(invariant_violation(
            "decoded block rows are not strictly ordered",
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
    let identity_matches = index.format_version == SEGMENT_FORMAT_VERSION
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
    if index.segment_size_bytes != reference.segment_size_bytes {
        return Err(invariant_violation(
            "directory segment length differs from reference",
        ));
    }
    validate_segment_directory(index)?;
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
        segment_size_bytes: reference.segment_size_bytes,
        index_size_bytes: reference.index_size_bytes,
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
    if index.bloom_mode == BloomMode::Disabled {
        return Ok(true);
    }
    if index.bloom_mode == BloomMode::Empty {
        return Ok(false);
    }
    for bit in bloom_positions(key, bloom.len() * 8) {
        if bloom
            .get(bit / 8)
            .is_none_or(|byte| byte & (1 << (bit % 8)) == 0)
        {
            return Ok(false);
        }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlMvpCheckpoint {
    validation: CheckpointValidation,
    reclamation_generation: u64,
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
        if !integrity::valid_immutable_id(&self.checkpoint_id)
            || !integrity::valid_immutable_id(&self.manifest_id)
            || !valid_raw_digest(&self.manifest_checksum_sha256)
        {
            return Err(invariant_violation(
                "invalid checkpoint root identity or digest",
            ));
        }
        if self.validation.encoding_version != 1
            || self.validation.source_manifest_sha256 != self.manifest_checksum_sha256
            || !valid_raw_digest(&self.validation.source_history_root)
            || !valid_raw_digest(&self.validation.source_physical_root)
            || !valid_raw_digest(&self.validation.state_checksum_sha256)
            || integrity::checkpoint_physical_digest(&self.scope, &self.states)?
                != self.validation.checkpoint_physical_root
        {
            return Err(invariant_violation(
                "invalid checkpoint validation evidence",
            ));
        }
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
            let probe_end = (bytes.len() as u64)
                .checked_add(1)
                .ok_or_else(|| invariant_violation("immutable readback length overflow"))?;
            let existing = storage.get_range(path, 0..probe_end).await?;
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
            let probe_end = (bytes.len() as u64)
                .checked_add(1)
                .ok_or_else(|| invariant_violation("immutable readback length overflow"))?;
            let existing = storage.get_range(path, 0..probe_end).await?;
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
        format_version: CONTROL_MVP_FORMAT_VERSION,
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
    validate_version_header(bytes, CONTROL_MVP_FORMAT_VERSION, context)?;
    let envelope: ChecksumEnvelope<T> = decode_json(bytes, context)?;
    if envelope.artifact_type != artifact_type {
        return Err(invariant_violation(format!(
            "{context} artifact type mismatch"
        )));
    }
    let payload_bytes = encode_json_vec(&envelope.payload, context)?;
    #[cfg(feature = "test-utils")]
    {
        cost::record(13, 1);
        cost::record(14, payload_bytes.len());
    }
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

fn validate_version_header(bytes: &[u8], expected: u32, context: &str) -> Result<()> {
    #[derive(Deserialize)]
    struct Header {
        #[serde(alias = "formatVersion")]
        format_version: Option<u32>,
    }
    if bytes.len() > MAX_TRANSACTION_JSON_BYTES {
        return Err(invariant_violation("authority header byte budget exceeded"));
    }
    let header: Header = decode_json(bytes, context)?;
    if header.format_version != Some(expected) {
        return Err(CatalogError::UnsupportedAuthorityFormat {
            message: format!(
                "{context}: unsupported format {:?}, expected {expected}",
                header.format_version
            ),
        });
    }
    Ok(())
}

fn valid_raw_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_raw_checksum(bytes: &[u8], expected: Option<&str>, context: &str) -> Result<()> {
    if let Some(expected) = expected {
        #[cfg(feature = "test-utils")]
        {
            cost::record(13, 1);
            cost::record(14, bytes.len());
        }
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

#[cfg(feature = "test-utils")]
thread_local! {
    static TEST_SHA256_WORK: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
    static TEST_INTEGRITY_WORK: std::cell::Cell<[u64; 6]> = const { std::cell::Cell::new([0; 6]) };
}

#[cfg(feature = "test-utils")]
#[allow(clippy::indexing_slicing)] // Three internal kinds, each owning its count/byte pair.
fn record_integrity_work(kind: usize, bytes: usize) {
    cost::record(2 + kind * 2, 1);
    cost::record(3 + kind * 2, bytes);
    TEST_INTEGRITY_WORK.with(|work| {
        let mut counts = work.get();
        counts[kind * 2] += 1;
        counts[kind * 2 + 1] += bytes as u64;
        work.set(counts);
    });
}

fn sha256_hex(bytes: &[u8]) -> String {
    #[cfg(feature = "test-utils")]
    {
        cost::record(0, 1);
        cost::record(1, bytes.len());
    }
    #[cfg(feature = "test-utils")]
    TEST_SHA256_WORK.with(|work| {
        let (calls, total) = work.get();
        work.set((calls + 1, total + bytes.len() as u64));
    });
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
    #[cfg(feature = "test-utils")]
    cost::record(10, 1);
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

fn hash_tag(hasher: &mut Sha256, tag: u8) {
    #[cfg(feature = "test-utils")]
    cost::record(11, 1);
    hasher.update([tag]);
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    #[cfg(feature = "test-utils")]
    cost::record(11, bytes.len());
    hash_u64(hasher, bytes.len() as u64);
    hasher.update(bytes);
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    #[cfg(feature = "test-utils")]
    cost::record(11, 8);
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
        gate_path: Mutex<String>,
        delete_gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
        pause_after_put: AtomicBool,
        lose_head_response: AtomicBool,
        defer_checkpoint: AtomicBool,
        lose_checkpoint_response: AtomicBool,
        deferred_checkpoint: Mutex<Option<(String, Bytes, WritePrecondition)>>,
        fail_fence_readback: AtomicBool,
        arrow_gets: AtomicUsize,
    }

    impl PauseCheckpointPutBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryBackend::new(),
                gate: Mutex::new(None),
                gate_path: Mutex::new("/checkpoints/".to_string()),
                delete_gate: Mutex::new(None),
                pause_after_put: AtomicBool::new(false),
                lose_head_response: AtomicBool::new(false),
                defer_checkpoint: AtomicBool::new(false),
                lose_checkpoint_response: AtomicBool::new(false),
                deferred_checkpoint: Mutex::new(None),
                fail_fence_readback: AtomicBool::new(false),
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
            if path.ends_with("/head/current.json")
                && self.fail_fence_readback.load(Ordering::SeqCst)
                && !self.lose_head_response.load(Ordering::SeqCst)
            {
                return Err(arco_core::Error::storage(
                    "injected unavailable fence readback",
                ));
            }
            if std::path::Path::new(path)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("arrow"))
            {
                self.arrow_gets.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            if path.ends_with("/head/current.json")
                && self.fail_fence_readback.load(Ordering::SeqCst)
                && !self.lose_head_response.load(Ordering::SeqCst)
            {
                return Err(arco_core::Error::storage(
                    "injected unavailable fence readback",
                ));
            }
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if path.contains("/checkpoints/") && self.defer_checkpoint.swap(false, Ordering::SeqCst)
            {
                *self.deferred_checkpoint.lock().unwrap() =
                    Some((path.to_string(), data, precondition));
                return Err(arco_core::Error::storage(
                    "checkpoint PUT is still in flight",
                ));
            }
            let result = if self.pause_after_put.load(Ordering::SeqCst) {
                Some(
                    self.inner
                        .put(path, data.clone(), precondition.clone())
                        .await,
                )
            } else {
                None
            };
            if path.contains(&*self.gate_path.lock().unwrap()) {
                let gate = self.gate.lock().expect("checkpoint gate").take();
                if let Some((reached, release)) = gate {
                    let _ = reached.send(());
                    let _ = release.await;
                }
            }
            let result = match result {
                Some(result) => result,
                None => self.inner.put(path, data, precondition).await,
            };
            if path.contains("/checkpoints/")
                && self.lose_checkpoint_response.swap(false, Ordering::SeqCst)
            {
                return Err(arco_core::Error::storage(
                    "injected lost checkpoint response",
                ));
            }
            if path.ends_with("/head/current.json")
                && self.lose_head_response.swap(false, Ordering::SeqCst)
            {
                return Err(arco_core::Error::storage("injected lost fence response"));
            }
            result
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            let gate = self.delete_gate.lock().unwrap().take();
            if let Some((reached, release)) = gate {
                let _ = reached.send(());
                let _ = release.await;
            }
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

    #[tokio::test]
    async fn eager_reads_use_bounded_ranges_for_entire_segments() {
        let backend = PauseCheckpointPutBackend::new();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        backend.reset_arrow_gets();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        assert_eq!(
            backend.arrow_gets(),
            0,
            "full reconstruction must use bounded declared-length range probes"
        );
    }

    #[tokio::test]
    async fn keyless_sharded_anchors_remain_readable_after_successor_commit() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .unwrap()
                .with_checkpoint_interval(NonZeroU64::new(1).unwrap())
                .with_segment_limits(SegmentLimits {
                    rows: 2,
                    ..PRODUCTION_SEGMENT_LIMITS
                });
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        for id in ["a", "b"] {
            tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
                id,
                Bytes::from_static(b"payload"),
            ))
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let records = store
            .current_projection_outbox()
            .await
            .expect("successor remains readable");
        assert_eq!(
            records
                .iter()
                .map(ControlMvpProjectionOutboxRecord::record_id)
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[tokio::test]
    async fn committed_history_roots_advance_even_for_empty_mutations() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let mut roots = Vec::new();
        for _ in 0..2 {
            let token = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap()
                .into_state_token();
            let bytes = storage
                .get_raw(&store.paths.manifest_object(token.authority_manifest_id()))
                .await
                .unwrap();
            let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let root = doc["payload"]["history_root"]
                .as_str()
                .expect("durable logical-history root");
            assert_eq!(root.len(), 64);
            assert_eq!(doc["payload"]["physical_root"].as_str().unwrap().len(), 64);
            roots.push(root.to_string());
        }
        assert_ne!(roots[0], roots[1]);
    }

    #[tokio::test]
    async fn current_pointer_fences_cannot_precede_authenticated_manifest() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let mut pointer = store.load_pointer().await.unwrap();
        let mut manifest = store.load_manifest_for_pointer(&pointer).await.unwrap();
        manifest.reclamation_generation = 2;
        manifest.writer_epoch = 2;
        let bytes = encode_envelope("control-mvp-manifest", &manifest).unwrap();
        pointer.manifest_checksum_sha256 = sha256_hex(&bytes);
        storage
            .put_raw(
                &store.paths.manifest_object(&manifest.manifest_id),
                bytes,
                WritePrecondition::None,
            )
            .await
            .unwrap();
        for (writer, reclamation, valid) in [(3, 3, true), (1, 2, false), (2, 1, false)] {
            pointer.writer_epoch = writer;
            pointer.reclamation_generation = reclamation;
            storage
                .put_raw(
                    &store.paths.current_pointer(),
                    encode_json(&pointer, "pointer").unwrap(),
                    WritePrecondition::None,
                )
                .await
                .unwrap();
            assert_eq!(store.get(b"key").await.is_ok(), valid);
            assert_eq!(
                store
                    .clone()
                    .with_writer_epoch(writer)
                    .unwrap()
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .is_ok(),
                valid
            );
        }
    }

    #[tokio::test]
    async fn current_pointer_sequence_must_match_authenticated_manifest() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let mut pointer = store.load_pointer().await.unwrap();
        pointer.logical_sequence += 1;
        storage
            .put_raw(
                &store.paths.current_pointer(),
                encode_json(&pointer, "pointer").unwrap(),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        assert!(matches!(
            store.get(b"key").await,
            Err(CatalogError::InvariantViolation { .. })
        ));
        assert!(store.scan(ScanRequest::new(b"")).await.is_err());
        assert!(
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .is_err()
        );
        assert!(
            store
                .checkpoint(CheckpointOptions::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn retained_tokens_reject_coherently_replaced_roots() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        let token = tx.commit().await.unwrap().into_state_token();
        let mut manifest = store
            .load_manifest(token.authority_manifest_id())
            .await
            .unwrap();
        manifest.writer_epoch += 1;
        storage
            .put_raw(
                &store.paths.manifest_object(token.authority_manifest_id()),
                encode_envelope("control-mvp-manifest", &manifest).unwrap(),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        assert!(
            matches!(
                store.read_at(token).await,
                Err(CatalogError::InvariantViolation { .. })
            ),
            "a self-checksummed replacement must not authenticate a historical root"
        );
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

    #[test]
    fn full_l1_decode_rejects_invalid_kv_metadata() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        for corruption in 0..3 {
            let mut first = one_kv_row();
            first.key = b"a".to_vec();
            let mut second = one_kv_row();
            second.key = b"b".to_vec();
            second.logical_ordinal = 1;
            match corruption {
                0 => first.origin_sequence = Some(1),
                1 => second.logical_ordinal = 0,
                _ => second.logical_ordinal = 2,
            }
            let (bytes, index, reference) = encode_segment(
                "metadata",
                ControlMvpSegmentLevel::L1,
                1,
                &scope,
                &[first, second],
                PRODUCTION_SEGMENT_LIMITS,
            )
            .unwrap();
            let rows = decode_segment_rows(&bytes, &index, &reference, &scope).unwrap();
            let state_ref = ControlMvpStateRef {
                state_id: reference.segment_id,
                logical_sequence: 1,
                segment_size_bytes: reference.segment_size_bytes,
                index_size_bytes: reference.index_size_bytes,
                checksum_sha256: reference.checksum_sha256,
                index_checksum_sha256: reference.index_checksum_sha256,
                min_key_hex: Some(hex::encode(b"a")),
                max_key_hex: Some(hex::encode(b"b")),
            };
            assert!(
                state_object_from_segment_rows(&state_ref, rows, &scope).is_err(),
                "corruption {corruption}"
            );
        }
    }

    #[test]
    fn block_container_uses_independent_bounded_files() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let rows = (0_u64..1024)
            .map(|ordinal| {
                let mut row = one_kv_row();
                row.key = format!("{ordinal:032}").into_bytes();
                row.value = Some(vec![42; 1024]);
                row.logical_ordinal = ordinal;
                row
            })
            .collect::<Vec<_>>();
        let (bytes, index_bytes, reference) = encode_segment(
            "blocks",
            ControlMvpSegmentLevel::L1,
            1,
            &scope,
            &rows,
            PRODUCTION_SEGMENT_LIMITS,
        )
        .unwrap();
        let directory: serde_json::Value = serde_json::from_slice(&index_bytes).unwrap();
        let blocks = directory["blocks"]
            .as_array()
            .expect("versioned block directory");
        assert!(blocks.len() >= 16);
        for block in blocks {
            let offset = usize::try_from(block["offset"].as_u64().unwrap()).unwrap();
            let length = usize::try_from(block["length"].as_u64().unwrap()).unwrap();
            assert!(length <= 256 * 1024);
            preflight_arrow_segment(&bytes[offset..offset + length]).unwrap();
        }
        assert_eq!(
            decode_segment_rows(&bytes, &index_bytes, &reference, &scope).unwrap(),
            rows
        );
    }

    #[test]
    fn bloom_has_no_false_negatives_and_bounded_false_positives() {
        let keys = (0..4096)
            .map(|i| format!("present-{i:024}").into_bytes())
            .collect::<Vec<_>>();
        let refs = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let (mode, bits) = sized_bloom(&refs);
        assert_eq!(mode, BloomMode::Enabled);
        let bits = hex::decode(bits).unwrap();
        let may_match = |key: &[u8]| {
            bloom_positions(key, bits.len() * 8).all(|bit| bits[bit / 8] & (1 << (bit % 8)) != 0)
        };
        assert!(keys.iter().all(|key| may_match(key)));
        let false_positives = (0..100_000)
            .filter(|i| may_match(format!("absent--{i:024}").as_bytes()))
            .count();
        assert!(false_positives < 2000, "{false_positives} false positives");
        assert_eq!(sized_bloom(&[]), (BloomMode::Empty, String::new()));
        let too_many = vec![b"key".as_slice(); MAX_BLOOM_BYTES * 8 / 10 + 1];
        assert_eq!(sized_bloom(&too_many), (BloomMode::Disabled, String::new()));
    }

    #[test]
    fn blocks_reject_substitution_and_directory_span_corruption() {
        let (bytes, index_bytes, reference, scope) = encoded_test_segment();
        let index: ControlMvpSegmentIndex = decode_json(&index_bytes, "index").unwrap();
        for mutation in 0..6 {
            let mut corrupt = index.clone();
            match mutation {
                0 => corrupt.blocks[0].offset = 1,
                1 => corrupt.blocks[0].length = u64::MAX,
                2 => corrupt.blocks[0].row_count += 1,
                3 => corrupt.bloom_bits_hex.clear(),
                4 => corrupt.blocks[0].max_key_hex = Some(hex::encode(b"aaa")),
                _ => corrupt.blocks.push(corrupt.blocks[0].clone()),
            }
            assert!(validate_segment_directory(&corrupt).is_err());
        }
        let mut substituted = bytes.to_vec();
        substituted[20] ^= 1;
        assert!(decode_block_rows(&substituted, &index.blocks[0]).is_err());
        assert!(decode_block_rows(&bytes[..bytes.len() - 1], &index.blocks[0]).is_err());
        let mut appended = bytes.to_vec();
        appended.push(0);
        assert!(decode_block_rows(&appended, &index.blocks[0]).is_err());
        let mut replaced = reference;
        replaced.index_size_bytes += 1;
        assert!(decode_segment_rows(&bytes, &index_bytes, &replaced, &scope).is_err());
    }

    #[tokio::test]
    async fn token_equality_does_not_replace_digest_authentication() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .unwrap();
        let token = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap()
            .into_state_token();
        let replacement = token.clone().with_manifest_witness("f".repeat(64));
        assert_eq!(token, replacement);
        assert!(matches!(
            store.read_at(replacement).await,
            Err(CatalogError::InvariantViolation { .. })
        ));
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let replacement = checkpoint.clone().with_checkpoint_witness("f".repeat(64));
        assert_eq!(checkpoint, replacement);
        assert!(matches!(
            store.read_checkpoint(replacement).await,
            Err(CatalogError::InvariantViolation { .. })
        ));
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
            &"x".repeat(256),
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
            .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
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
    async fn released_external_pin_cannot_revive_still_present_checkpoint_bytes() {
        use crate::workspace_snapshot::{
            decode_retention_pin_revision, retention_pin_revision_path,
        };
        let (storage, store, reference) = externally_pinned_checkpoint().await;
        let pin_id = "pin_00000000000000000000000001";
        let pin = decode_retention_pin_revision(
            &storage
                .get_raw(&retention_pin_revision_path(pin_id, 1).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        publish_test_pin(
            &storage,
            &pin.release(2, Utc::now() + ChronoDuration::days(32))
                .unwrap(),
        )
        .await;
        let now = Utc::now() + ChronoDuration::days(33);
        assert!(
            storage
                .head_raw(reference.checkpoint_path().unwrap())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .resolve_persisted_reference_at(&reference, now)
                .await
                .is_err()
        );
        let token = store
            .token(
                reference.manifest_id().to_string(),
                reference.logical_sequence(),
            )
            .with_manifest_witness(
                reference
                    .manifest_sha256()
                    .strip_prefix("sha256:")
                    .unwrap()
                    .to_string(),
            );
        assert!(
            store
                .validate_state_token_protection(&token, now)
                .await
                .is_err(),
            "released external evidence cannot renew an expired state token"
        );
        let worker =
            ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone()).unwrap();
        let plan = worker.plan_gc_at(now, Vec::new()).await.unwrap();
        assert!(
            plan.candidates()
                .iter()
                .any(|candidate| Some(candidate.path()) == reference.checkpoint_path())
        );
        worker.collect_gc_at(now, Vec::new()).await.unwrap();
        assert!(
            storage
                .head_raw(reference.checkpoint_path().unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn retained_export_independently_protects_authority_across_selector_pages() {
        use crate::workspace_snapshot::{
            ExportManifest, RelocationPolicy, RequiredObject, RequiredObjectKind,
            RetentionPinRevision, RetentionTarget, decode_retention_pin_revision,
            decode_workspace_snapshot, encode_export_manifest, export_record_path,
            retention_pin_revision_path, snapshot_record_path,
        };
        let (storage, store, reference) = externally_pinned_checkpoint().await;
        let snapshot_id = "snap_00000000000000000000000001";
        let source_pin = "pin_00000000000000000000000001";
        let export_id = "exp_00000000000000000000000001";
        let export_pin = "pin_00000000000000000000000002";
        let snapshot_path = snapshot_record_path(snapshot_id).unwrap();
        let bytes = storage.get_raw(&snapshot_path).await.unwrap();
        let snapshot = decode_workspace_snapshot(&bytes).unwrap();
        let export = ExportManifest::new(
            export_id,
            export_pin,
            snapshot_id,
            source_pin,
            snapshot.scope().clone(),
            snapshot.created_at(),
            snapshot.retained_until(),
            snapshot.domains().to_vec(),
            Vec::new(),
            snapshot.event_archives().to_vec(),
            vec![
                RequiredObject::new(
                    snapshot_path,
                    u64::try_from(bytes.len()).unwrap(),
                    RequiredObjectKind::SnapshotRecord,
                    prefixed_sha256(&bytes),
                )
                .unwrap(),
            ],
            Vec::new(),
            RelocationPolicy::relative_to_caller_export_root(),
        )
        .unwrap();
        storage
            .put_raw(
                &export_record_path(export_id).unwrap(),
                Bytes::from(encode_export_manifest(&export).unwrap()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        publish_test_pin(
            &storage,
            &RetentionPinRevision::new(
                export_pin,
                1,
                RetentionTarget::export(export_id).unwrap(),
                snapshot.created_at(),
                snapshot.retained_until(),
                None,
            )
            .unwrap(),
        )
        .await;
        let pin = decode_retention_pin_revision(
            &storage
                .get_raw(&retention_pin_revision_path(source_pin, 1).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        publish_test_pin(
            &storage,
            &pin.release(2, Utc::now() + ChronoDuration::days(1))
                .unwrap(),
        )
        .await;
        // Fill a selector-inventory page with unselected revision objects.
        for ordinal in 0..260 {
            storage
                .put_raw(
                    &format!(
                        "retention/pins/pin_00000000000000000000000000/revisions/{ordinal:020}.json"
                    ),
                    Bytes::new(),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        let now = Utc::now() + ChronoDuration::days(31);
        let worker = ControlMvpMaintenanceWorker::new(storage, store.scope.clone()).unwrap();
        worker.collect_gc_at(now, Vec::new()).await.unwrap();
        assert_eq!(
            store
                .resolve_persisted_reference_at(&reference, now)
                .await
                .unwrap()
                .get(b"historical")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"retained"))
        );
    }

    async fn publish_test_pin(
        storage: &ScopedStorage,
        pin: &crate::workspace_snapshot::RetentionPinRevision,
    ) {
        use crate::workspace_snapshot::{
            RetentionPinLatest, encode_retention_pin_latest, encode_retention_pin_revision,
            retention_pin_latest_path, retention_pin_revision_path,
        };
        let bytes = Bytes::from(encode_retention_pin_revision(pin).unwrap());
        let path = retention_pin_revision_path(pin.pin_id(), pin.revision()).unwrap();
        let latest =
            RetentionPinLatest::new(pin.pin_id(), pin.revision(), &path, prefixed_sha256(&bytes))
                .unwrap();
        storage
            .put_raw(&path, bytes, WritePrecondition::DoesNotExist)
            .await
            .unwrap();
        storage
            .put_raw(
                &retention_pin_latest_path(pin.pin_id()).unwrap(),
                Bytes::from(encode_retention_pin_latest(&latest).unwrap()),
                WritePrecondition::None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn active_external_pin_protects_checkpoint_closure_after_intrinsic_expiry() {
        let (storage, store, reference) = externally_pinned_checkpoint().await;
        let now = Utc::now() + ChronoDuration::days(31);
        let token = store
            .token(
                reference.manifest_id().to_string(),
                reference.logical_sequence(),
            )
            .with_manifest_witness(
                reference
                    .manifest_sha256()
                    .strip_prefix("sha256:")
                    .unwrap()
                    .to_string(),
            );
        store
            .validate_state_token_protection(&token, now)
            .await
            .expect("active external pin also protects the exact historical state token");
        let worker =
            ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone()).unwrap();
        let checkpoint_path = reference.checkpoint_path().unwrap();
        let plan = worker.plan_gc_at(now, Vec::new()).await.unwrap();
        assert!(
            !plan
                .candidates()
                .iter()
                .any(|candidate| candidate.path() == checkpoint_path),
            "active workspace pin must protect checkpoint beyond its intrinsic lifetime"
        );
        worker.collect_gc_at(now, Vec::new()).await.unwrap();
        let reader = store
            .resolve_persisted_reference_at(&reference, now)
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"historical").await.unwrap(),
            Some(Bytes::from_static(b"retained"))
        );
    }

    #[tokio::test]
    async fn malformed_external_pin_aborts_control_gc_before_any_delete() {
        let (storage, store, _reference) = externally_pinned_checkpoint().await;
        let selector =
            crate::workspace_snapshot::retention_pin_latest_path("pin_00000000000000000000000001")
                .unwrap();
        storage
            .put_raw(
                &selector,
                Bytes::from_static(b"corrupt"),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        let before = storage
            .list_meta(&format!("{}/", store.paths.base_prefix()))
            .await
            .unwrap();
        let worker =
            ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone()).unwrap();
        assert!(
            worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(31), Vec::new())
                .await
                .is_err(),
            "invalid retained-root evidence must deny reclamation"
        );
        let after = storage
            .list_meta(&format!("{}/", store.paths.base_prefix()))
            .await
            .unwrap();
        assert_eq!(before.len(), after.len());
    }

    async fn externally_pinned_checkpoint() -> (
        ScopedStorage,
        ControlMvpStateStore,
        PersistedAuthorityReference,
    ) {
        use crate::workspace_snapshot::{
            DomainAuthorityReference, DomainEventArchive, RetentionPinLatest, RetentionPinRevision,
            RetentionTarget, WorkspaceScope, WorkspaceSnapshot, encode_retention_pin_latest,
            encode_retention_pin_revision, encode_workspace_snapshot, retention_pin_latest_path,
            retention_pin_revision_path, snapshot_record_path,
        };
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(1).unwrap());
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        txn.put(b"historical", Bytes::from_static(b"retained"))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let created = Utc::now();
        let deadline = created + ChronoDuration::days(60);
        let reference = store
            .persist_checkpoint_reference(&checkpoint, deadline)
            .await
            .unwrap();
        let scope = WorkspaceScope::new("tenant", "workspace").unwrap();
        let snapshot_id = "snap_00000000000000000000000001";
        let pin_id = "pin_00000000000000000000000001";
        let snapshot = WorkspaceSnapshot::new(
            snapshot_id,
            pin_id,
            scope.clone(),
            created,
            deadline,
            None,
            vec![DomainAuthorityReference::new("catalog", scope, reference.clone()).unwrap()],
            Vec::new(),
            vec![DomainEventArchive::empty("catalog").unwrap()],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let pin = RetentionPinRevision::new(
            pin_id,
            1,
            RetentionTarget::snapshot(snapshot_id).unwrap(),
            created,
            deadline,
            None,
        )
        .unwrap();
        let revision = Bytes::from(encode_retention_pin_revision(&pin).unwrap());
        let revision_path = retention_pin_revision_path(pin_id, 1).unwrap();
        let selector =
            RetentionPinLatest::new(pin_id, 1, &revision_path, prefixed_sha256(&revision)).unwrap();
        // This fixture installs already-published retained-root evidence. Service
        // tests cover its coordinated publication; these tests exercise readers/GC.
        for (path, bytes) in [
            (
                snapshot_record_path(snapshot_id).unwrap(),
                Bytes::from(encode_workspace_snapshot(&snapshot).unwrap()),
            ),
            (revision_path, revision),
            (
                retention_pin_latest_path(pin_id).unwrap(),
                Bytes::from(encode_retention_pin_latest(&selector).unwrap()),
            ),
        ] {
            storage
                .put_raw(&path, bytes, WritePrecondition::DoesNotExist)
                .await
                .unwrap();
        }
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        txn.delete(b"historical").await.unwrap();
        txn.commit().await.unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        (storage, store, reference)
    }

    #[tokio::test]
    async fn late_maintenance_recovery_reauthenticates_after_waiting_for_gc() {
        let backend = PauseCheckpointPutBackend::new();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        for _ in 0..16 {
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
        }
        let worker = DurableMaintenanceWorker::new(
            storage.clone(),
            scope.clone(),
            DurableAuthorityBinding::new([31; 32]),
        )
        .unwrap();
        let now = Utc::now();
        let plan = worker.prepare_at(now).await.unwrap().unwrap();
        worker.start_at(&plan, now).await.unwrap();
        let pin_paths = storage
            .list_meta("retention/pins/")
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.path.to_string())
            .collect::<Vec<_>>();
        assert!(!pin_paths.is_empty());
        *backend.gate_path.lock().unwrap() = RETENTION_GC_LOCK_PATH.into();
        let (reached, release) = backend.arm();
        let expired = now + ChronoDuration::days(9);
        let id = plan.job_id().clone();
        let recovery =
            tokio::spawn(async move { worker.recover_activation_at(&id, expired).await });
        // The backend pauses the lock PUT after the descriptor and every plan page
        // have been read, but before recovery owns retention coordination.
        reached.await.unwrap();
        let collector = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
        let mut cursor = None;
        loop {
            let page = collector
                .collect_gc_page_at(expired, Vec::new(), cursor.as_deref())
                .await
                .unwrap();
            cursor = page.continuation().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        for path in &pin_paths {
            assert!(storage.head_raw(path).await.unwrap().is_none());
        }
        let descriptor = format!(
            "{}/maintenance/{}/descriptor.json",
            store.paths.base_prefix(),
            plan.job_id().as_str()
        );
        assert!(storage.head_raw(&descriptor).await.unwrap().is_none());
        release.send(()).unwrap();
        assert!(recovery.await.unwrap().is_err());
        for path in &pin_paths {
            assert!(
                storage.head_raw(path).await.unwrap().is_none(),
                "recovery published a pin after GC removed its authenticated plan"
            );
        }
    }

    #[tokio::test]
    async fn maintenance_crossing_reclamation_fence_regenerates_outputs() {
        for after in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
            let scope = StateScope::new("tenant", "workspace", "catalog");
            let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
            for ordinal in 0..L0_MAINTENANCE_INTENT_THRESHOLD {
                let mut txn = store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap();
                txn.put(b"key", Bytes::from(ordinal.to_string()))
                    .await
                    .unwrap();
                txn.commit().await.unwrap();
            }
            let before = store.load_pointer().await.unwrap();
            *backend.gate_path.lock().unwrap() = "/manifests/".to_string();
            backend.pause_after_put.store(after, Ordering::SeqCst);
            let (reached, release) = backend.arm();
            let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope.clone()).unwrap();
            let maintenance = tokio::spawn(async move {
                worker
                    .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
                    .await
            });
            reached.await.unwrap();
            let prior_outputs = storage
                .list_meta(&format!("{}/segments/l1/", store.paths.base_prefix()))
                .await
                .unwrap()
                .into_iter()
                .map(|object| object.path.to_string())
                .collect::<BTreeSet<_>>();
            assert!(!prior_outputs.is_empty());
            // Force an eligible deletion while the maintenance pin is still live.
            // At 31 days the fixed job evidence is correctly collectible and cannot
            // authorize an automatic retry of the missing job.
            storage
                .put_raw(
                    &store.paths.state_object("reclamation-fence-orphan"),
                    Bytes::from_static(b"orphan"),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
            let collector = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
            collector
                .collect_gc_at(
                    Utc::now() + ChronoDuration::days(7) + ChronoDuration::hours(1),
                    Vec::new(),
                )
                .await
                .unwrap();
            release.send(()).unwrap();
            maintenance
                .await
                .unwrap()
                .unwrap()
                .expect("regenerated maintenance publishes");
            let selected = store.load_pointer().await.unwrap();
            assert_eq!(selected.reclamation_generation, 1);
            assert_eq!(selected.logical_sequence, before.logical_sequence);
            assert_eq!(selected.writer_epoch, before.writer_epoch);
            let manifest = store.load_manifest_for_pointer(&selected).await.unwrap();
            assert_eq!(manifest.reclamation_generation, 1);
            assert!(manifest.base_states.iter().all(|reference| {
                !prior_outputs.contains(&store.paths.state_object(&reference.state_id))
            }));
            assert_eq!(store.get(b"key").await.unwrap(), Some(Bytes::from("15")));
        }
    }

    #[tokio::test]
    async fn restore_crossing_reclamation_fence_is_superseded() {
        for after in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
            let scope = StateScope::new("tenant", "workspace", "catalog");
            let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
            let checkpoint = store
                .checkpoint(
                    CheckpointOptions::default().with_min_retention_seconds(60 * 24 * 60 * 60),
                )
                .await
                .unwrap();
            let source = store
                .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::days(60))
                .await
                .unwrap();
            let mut txn = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            txn.put(b"live", Bytes::from_static(b"current"))
                .await
                .unwrap();
            let current = txn.commit().await.unwrap().state_token().clone();
            let participant = ControlMvpRestoreParticipant::new(store.clone());
            let identity =
                RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
                    .unwrap();
            let plan = participant
                .plan_restore(&source, &identity, Utc::now())
                .await
                .unwrap();
            storage
                .put_raw(
                    &store.paths.tx_object("orphan"),
                    Bytes::new(),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
            *backend.gate_path.lock().unwrap() = "/transactions/".to_string();
            backend.pause_after_put.store(after, Ordering::SeqCst);
            let (reached, release) = backend.arm();
            let restore =
                tokio::spawn(async move { participant.apply_restore(&plan, Utc::now()).await });
            reached.await.unwrap();
            let collector = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
            collector
                .collect_gc_at(Utc::now() + ChronoDuration::days(31), Vec::new())
                .await
                .unwrap();
            release.send(()).unwrap();
            assert!(matches!(
                restore.await.unwrap().unwrap(),
                RestoreParticipantInspection::Superseded
            ));
            assert_eq!(store.current_state_token().await.unwrap(), current);
            assert_eq!(
                store.get(b"live").await.unwrap(),
                Some(Bytes::from_static(b"current"))
            );
            let fresh = ControlMvpRestoreParticipant::new(store.clone())
                .plan_restore(&source, &identity, Utc::now())
                .await
                .unwrap();
            assert!(matches!(
                ControlMvpRestoreParticipant::new(store)
                    .apply_restore(&fresh, Utc::now())
                    .await
                    .unwrap(),
                RestoreParticipantInspection::Visible { .. }
            ));
        }
    }

    #[tokio::test]
    async fn frozen_operation_retry_after_head_only_change_gets_fresh_artifact_identity() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let options = TxnOptions::default().with_operation_id("frozen-command");
        let stale = store.begin_control_txn(options.clone()).await.unwrap();
        let old_id = stale.tx_id.clone();
        let claimed = store.claim_writer_authority().await.unwrap();
        assert!(stale.commit().await.is_err());
        let fresh = claimed.begin_control_txn(options).await.unwrap();
        assert_ne!(
            old_id, fresh.tx_id,
            "identities bind the exact HEAD, not only its sequence and generation"
        );
        fresh.commit().await.unwrap();
    }

    #[tokio::test]
    async fn reclamation_fence_racing_authority_invalidates_the_deletion_plan() {
        for after in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
            let scope = StateScope::new("tenant", "workspace", "catalog");
            let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
            let orphan = store.paths.tx_object("orphan");
            storage
                .put_raw(&orphan, Bytes::new(), WritePrecondition::DoesNotExist)
                .await
                .unwrap();
            *backend.gate_path.lock().unwrap() = "/head/current.json".to_string();
            backend.pause_after_put.store(after, Ordering::SeqCst);
            let (reached, release) = backend.arm();
            let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
            let collector = tokio::spawn(async move {
                worker
                    .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                    .await
            });
            reached.await.unwrap();
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
            release.send(()).unwrap();
            assert!(matches!(
                collector.await.unwrap(),
                Err(CatalogError::CasFailed { .. })
            ));
            assert!(storage.head_raw(&orphan).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn delayed_delete_with_concurrent_commit_aborts_remaining_page_and_restarts() {
        let backend = PauseCheckpointPutBackend::new();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        for id in ["orphan-a", "orphan-b"] {
            storage
                .put_raw(
                    &store.paths.tx_object(id),
                    Bytes::new(),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        let (reached_tx, reached) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *backend.delete_gate.lock().unwrap() = Some((reached_tx, release_rx));
        let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope.clone()).unwrap();
        let collector = tokio::spawn(async move {
            worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                .await
        });
        reached.await.unwrap();
        let mut fresh = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        fresh
            .put(b"live", Bytes::from_static(b"after fence"))
            .await
            .unwrap();
        let acknowledged = fresh.commit().await.unwrap().state_token().clone();
        release.send(()).unwrap();
        assert!(matches!(
            collector.await.unwrap(),
            Err(CatalogError::CasFailed { .. })
        ));
        assert!(
            storage
                .head_raw(&store.paths.tx_object("orphan-a"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .head_raw(&store.paths.tx_object("orphan-b"))
                .await
                .unwrap()
                .is_some()
        );
        let restarted = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
        assert_eq!(
            restarted
                .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                .await
                .unwrap()
                .objects_deleted(),
            1
        );
        assert_eq!(store.current_state_token().await.unwrap(), acknowledged);
        assert_eq!(
            store.get(b"live").await.unwrap(),
            Some(Bytes::from_static(b"after fence"))
        );
    }

    #[tokio::test]
    async fn persisted_reference_preparation_rejects_an_unpublished_manifest() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope).unwrap();
        let token = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap()
            .state_token()
            .clone();
        let mut manifest = store
            .load_manifest(token.authority_manifest_id())
            .await
            .unwrap();
        manifest.manifest_id = "unpublished-manifest".to_string();
        let bytes = encode_envelope_limited(
            "control-mvp-manifest",
            &manifest,
            MAX_CONTROL_JSON_BYTES,
            "unpublished manifest",
        )
        .unwrap();
        storage
            .put_raw(
                &store.paths.manifest_object(&manifest.manifest_id),
                bytes,
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let unacknowledged = store.token(manifest.manifest_id, manifest.logical_sequence);
        assert!(
            store
                .persist_state_reference(&unacknowledged, Utc::now() + ChronoDuration::days(1))
                .await
                .is_err(),
            "staging an artifact is not publication of a retained authority"
        );
    }

    #[tokio::test]
    async fn expired_checkpoint_cannot_be_revived_by_preparing_a_longer_reference() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let reference = store
            .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::days(60))
            .await
            .unwrap();
        assert!(
            store
                .resolve_persisted_reference_at(&reference, Utc::now() + ChronoDuration::days(31))
                .await
                .is_err(),
            "a prepared reference is not a durable extension of checkpoint protection"
        );
    }

    #[tokio::test]
    async fn reclamation_fence_rejects_commit_paused_before_and_after_artifact_put() {
        for after in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
            let scope = StateScope::new("tenant", "workspace", "catalog");
            let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
            let mut stale = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            stale
                .put(b"key", Bytes::from_static(b"stale"))
                .await
                .unwrap();
            let old_id = stale.tx_id.clone();
            storage
                .put_raw(
                    &store.paths.tx_object("orphan"),
                    Bytes::new(),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
            *backend.gate_path.lock().unwrap() = "/transactions/".to_string();
            backend.pause_after_put.store(after, Ordering::SeqCst);
            let (reached, release) = backend.arm();
            let task = tokio::spawn(stale.commit());
            reached.await.unwrap();
            let worker = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
            worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                .await
                .unwrap();
            release.send(()).unwrap();
            assert!(matches!(
                task.await.unwrap(),
                Err(CatalogError::CasFailed { .. })
            ));
            let fresh = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            assert!(old_id.ends_with("-rg-00000000000000000000"));
            assert!(fresh.tx_id.ends_with("-rg-00000000000000000001"));
            fresh.commit().await.unwrap();
            assert_eq!(store.get(b"key").await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn reclamation_fence_lost_response_requires_exact_readback_before_delete() {
        for fail_readback in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
            let scope = StateScope::new("tenant", "workspace", "catalog");
            let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
            let orphan = store.paths.tx_object("orphan");
            storage
                .put_raw(&orphan, Bytes::new(), WritePrecondition::DoesNotExist)
                .await
                .unwrap();
            backend.lose_head_response.store(true, Ordering::SeqCst);
            backend
                .fail_fence_readback
                .store(fail_readback, Ordering::SeqCst);
            let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
            let result = worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                .await;
            assert_eq!(result.is_err(), fail_readback);
            assert_eq!(
                storage.head_raw(&orphan).await.unwrap().is_some(),
                fail_readback
            );
        }
    }

    #[tokio::test]
    async fn reclamation_generation_exhaustion_never_authorizes_delete() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let mut pointer = store.load_pointer().await.unwrap();
        pointer.reclamation_generation = u64::MAX;
        let bytes = encode_json(&pointer, "exhausted head").unwrap();
        storage
            .put_raw(
                &store.paths.current_pointer(),
                bytes.clone(),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        let orphan = store.paths.tx_object("orphan");
        storage
            .put_raw(&orphan, Bytes::new(), WritePrecondition::DoesNotExist)
            .await
            .unwrap();
        let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
        assert!(
            worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                .await
                .is_err()
        );
        assert!(storage.head_raw(&orphan).await.unwrap().is_some());
        assert_eq!(
            storage
                .get_raw(&store.paths.current_pointer())
                .await
                .unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn reclamation_fence_invalidates_prepared_commit_without_changing_authority() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
        let mut initial = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        initial
            .put(b"live", Bytes::from_static(b"before"))
            .await
            .unwrap();
        let token = initial.commit().await.unwrap().state_token().clone();
        let before = store.load_pointer().await.unwrap();
        let mut stale = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        stale
            .put(b"live", Bytes::from_static(b"stale"))
            .await
            .unwrap();
        storage
            .put_raw(
                &store.paths.tx_object("orphan"),
                Bytes::new(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
        assert_eq!(
            worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(8), Vec::new())
                .await
                .unwrap()
                .objects_deleted(),
            1
        );
        assert!(
            matches!(stale.commit().await, Err(CatalogError::CasFailed { .. })),
            "a transaction prepared before reclamation must rerun from the fenced HEAD"
        );
        assert_eq!(store.current_state_token().await.unwrap(), token);
        let after = store.load_pointer().await.unwrap();
        assert_eq!(before.writer_epoch, after.writer_epoch);
        assert_eq!(
            store.get(b"live").await.unwrap(),
            Some(Bytes::from_static(b"before"))
        );
        let mut fresh = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        fresh
            .put(b"live", Bytes::from_static(b"after"))
            .await
            .unwrap();
        fresh.commit().await.unwrap();
        assert_eq!(
            store.get(b"live").await.unwrap(),
            Some(Bytes::from_static(b"after"))
        );
    }

    #[tokio::test]
    async fn stale_gc_epoch_recovery_cannot_reenable_pre_fence_candidates() {
        use crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH;
        let backend = PauseCheckpointPutBackend::new();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        let mut stale = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        stale
            .put(b"catalog/live", Bytes::from_static(b"stale"))
            .await
            .unwrap();
        let orphan = store.paths.tx_object("orphan-before-recovery");
        storage
            .put_raw(&orphan, Bytes::new(), WritePrecondition::DoesNotExist)
            .await
            .unwrap();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *backend.delete_gate.lock().unwrap() = Some((reached_tx, release_rx));
        let first = ControlMvpMaintenanceWorker::new(storage.clone(), scope.clone()).unwrap();
        let now = Utc::now() + ChronoDuration::days(8);
        let first_task = tokio::spawn(async move { first.collect_gc_at(now, Vec::new()).await });
        reached_rx.await.unwrap();
        assert_eq!(
            store.load_pointer().await.unwrap().reclamation_generation,
            1
        );

        // Advance the fixture's durable epoch clock and expire its lease without
        // completing the already-issued DELETE. A later collector may adopt it.
        let mut epoch: serde_json::Value = serde_json::from_slice(
            &storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap(),
        )
        .unwrap();
        epoch["started_at"] = serde_json::to_value(Utc::now() - ChronoDuration::hours(1)).unwrap();
        storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                Bytes::from(serde_json::to_vec(&epoch).unwrap()),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        storage.delete(RETENTION_GC_LOCK_PATH).await.unwrap();
        let restarted = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
        assert_eq!(
            restarted
                .collect_gc_at(now, Vec::new())
                .await
                .unwrap()
                .objects_deleted(),
            1
        );
        assert_eq!(
            store.load_pointer().await.unwrap().reclamation_generation,
            2
        );
        assert!(matches!(
            stale.commit().await,
            Err(CatalogError::CasFailed { .. })
        ));
        let mut fresh = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        fresh
            .put(b"catalog/live", Bytes::from_static(b"fresh"))
            .await
            .unwrap();
        let selected = fresh.commit().await.unwrap().state_token().clone();
        release_tx.send(()).unwrap();
        assert!(
            first_task.await.unwrap().is_err(),
            "the old collector cannot settle the adopted epoch"
        );
        assert_eq!(store.current_state_token().await.unwrap(), selected);
        assert_eq!(
            store.get(b"catalog/live").await.unwrap(),
            Some(Bytes::from_static(b"fresh"))
        );
        assert!(storage.head_raw(&orphan).await.unwrap().is_none());
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
    async fn checkpoint_lost_response_reconciles_exact_immutable_record() {
        let backend = PauseCheckpointPutBackend::new();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope).unwrap();
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        backend
            .lose_checkpoint_response
            .store(true, Ordering::SeqCst);
        let token = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        store.read_checkpoint(token).await.unwrap();
        let epoch: serde_json::Value = serde_json::from_slice(
            &storage
                .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            epoch["state"], "IDLE",
            "exact record reconciles the lost response"
        );
        store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_checkpoint_publication_retains_durable_exclusion() {
        for after in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            backend.pause_after_put.store(after, Ordering::SeqCst);
            let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
            let scope = StateScope::new("tenant", "workspace", "catalog");
            let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
            let (reached, _release) = backend.arm();
            let task =
                tokio::spawn(async move { store.checkpoint(CheckpointOptions::default()).await });
            reached.await.unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let epoch: serde_json::Value = serde_json::from_slice(
                &storage
                    .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(epoch["state"], "IN_FLIGHT");
            // Simulate lease expiry, retaining the durable publication epoch.
            storage.delete(RETENTION_GC_LOCK_PATH).await.unwrap();
            let worker = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
            assert!(
                worker
                    .collect_gc_at(Utc::now() + ChronoDuration::days(31), Vec::new())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_transport_error_keeps_epoch_in_flight_until_remote_completion() {
        let backend = PauseCheckpointPutBackend::new();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        txn.put(b"catalog/live", Bytes::from_static(b"retained"))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        backend.defer_checkpoint.store(true, Ordering::SeqCst);
        assert!(
            store
                .checkpoint(CheckpointOptions::default())
                .await
                .is_err()
        );
        let epoch: serde_json::Value = serde_json::from_slice(
            &storage
                .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            epoch["state"], "IN_FLIGHT",
            "a returned transport error does not prove the remote PUT is terminal"
        );
        let worker = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
        assert!(
            worker
                .collect_gc_at(Utc::now() + ChronoDuration::days(31), Vec::new())
                .await
                .is_err(),
            "GC cannot cross an uncertain publication epoch"
        );
        let (path, bytes, condition) = backend.deferred_checkpoint.lock().unwrap().take().unwrap();
        backend.inner.put(&path, bytes, condition).await.unwrap();
        let checkpoint_id = path.rsplit('/').next().unwrap().trim_end_matches(".json");
        let reader = store
            .read_checkpoint(
                store
                    .checkpoint_token(checkpoint_id.to_string())
                    .with_checkpoint_witness(sha256_hex(
                        &store
                            .storage
                            .get(&store.paths.checkpoint_object(checkpoint_id))
                            .await
                            .unwrap(),
                    )),
            )
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"catalog/live").await.unwrap(),
            Some(Bytes::from_static(b"retained"))
        );
    }

    #[tokio::test]
    async fn checkpoint_publication_excludes_gc_until_the_retained_root_is_visible() {
        for after in [false, true] {
            let backend = PauseCheckpointPutBackend::new();
            backend.pause_after_put.store(after, Ordering::SeqCst);
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
            history: HistoryLink::default(),
            reclamation_generation: 0,
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
            .expect_err("trim rows must identify the removed event incarnation");

        assert!(matches!(error, CatalogError::InvariantViolation { .. }));
    }

    #[test]
    fn replay_rejects_sequence_zero_after_terminal_logical_sequence_without_panicking() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let tx = ControlMvpTxObject {
            history: HistoryLink::default(),
            reclamation_generation: 0,
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
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn authenticated_resolution_distinguishes_budgets_missing_links_and_substitution() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.stage_projection_intent("projection", "test", Bytes::from_static(b"payload"))
            .await
            .unwrap();
        let first = tx.commit().await.unwrap();
        for _ in 0..2 {
            store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap()
                .commit()
                .await
                .unwrap();
        }
        let token = store.current_state_token().await.unwrap();
        let records = store.current_projection_outbox().await.unwrap();
        let intent = &first.projection_intents()[0];
        assert_eq!(
            store
                .resolve_projection_source(&records[0], intent)
                .await
                .unwrap(),
            *first.state_token()
        );
        for (count, bytes) in [(1, usize::MAX), (4096, 1)] {
            let error = store
                .resolve_ancestor_bounded(
                    token.authority_manifest_id(),
                    token.manifest_witness().unwrap(),
                    |_, _| None::<()>,
                    count,
                    bytes,
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                CatalogError::AmbiguousAuthorityOutcome { .. }
            ));
        }
        let mut forged_record = records[0].clone();
        forged_record.origin_sequence = Some(2);
        assert!(matches!(
            store
                .resolve_projection_source(&forged_record, intent)
                .await,
            Err(CatalogError::InvariantViolation { .. })
        ));
        let manifest = store
            .load_manifest(token.authority_manifest_id())
            .await
            .unwrap();
        let parent_path = store
            .paths
            .manifest_object(manifest.base_manifest_id.as_deref().unwrap());
        let parent_bytes = storage.get_raw(&parent_path).await.unwrap();
        storage.delete(&parent_path).await.unwrap();
        assert!(
            store.read_at(token.clone()).await.is_ok(),
            "witnessed opening does not require its parent"
        );
        assert!(matches!(
            store.resolve_projection_source(&records[0], intent).await,
            Err(CatalogError::AmbiguousAuthorityOutcome { .. })
        ));
        let mut parent: ControlMvpManifest =
            decode_envelope(&parent_bytes, "control-mvp-manifest", "parent").unwrap();
        parent.writer_epoch += 1;
        storage
            .put_raw(
                &parent_path,
                encode_envelope("control-mvp-manifest", &parent).unwrap(),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        assert!(matches!(
            store.resolve_projection_source(&records[0], intent).await,
            Err(CatalogError::InvariantViolation { .. })
        ));
        let mut orphan = manifest.clone();
        orphan.base_manifest_id = None;
        orphan.parent_manifest_sha256 = None;
        assert!(orphan.validate(&store.scope, &orphan.manifest_id).is_err());
        storage
            .put_raw(&parent_path, parent_bytes, WritePrecondition::None)
            .await
            .unwrap();
        let mut splice = manifest;
        splice.tx_refs[0].checksum_sha256 = "f".repeat(64);
        let bytes = encode_envelope("control-mvp-manifest", &splice).unwrap();
        let digest = sha256_hex(&bytes);
        storage
            .put_raw(
                &store.paths.manifest_object(&splice.manifest_id),
                bytes,
                WritePrecondition::None,
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .resolve_ancestor(&splice.manifest_id, &digest, |_, _| None::<()>)
                .await,
            Err(CatalogError::InvariantViolation { .. })
        ));
    }

    #[test]
    fn oversized_rows_are_isolated() {
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let mut oversized = one_kv_row();
        oversized.key = Vec::new();
        oversized.value = Some(vec![1; 300 * 1024]);
        let mut ordinary = one_kv_row();
        ordinary.logical_ordinal = 1;
        let rows = vec![oversized, ordinary];
        let (bytes, index_bytes, reference) = encode_segment(
            "large",
            ControlMvpSegmentLevel::L0,
            1,
            &scope,
            &rows,
            PRODUCTION_SEGMENT_LIMITS,
        )
        .unwrap();
        let index: ControlMvpSegmentIndex = decode_json(&index_bytes, "index").unwrap();
        assert_eq!(index.blocks.len(), 2);
        assert_eq!(index.blocks[0].row_count, 1);
        assert!(index.blocks[0].length > MAX_BLOCK_BYTES as u64);
        assert_eq!(
            decode_segment_rows(&bytes, &index_bytes, &reference, &scope).unwrap(),
            rows
        );
    }

    #[tokio::test]
    async fn checkpoint_reuse_and_persistence_validate_entire_closure() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(1).unwrap());
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        let token = tx.commit().await.unwrap();
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let manifest = store
            .load_manifest(token.authority_manifest_id())
            .await
            .unwrap();
        let path = store
            .paths
            .state_object(&manifest.anchor_states[0].state_id);
        storage.delete(&path).await.unwrap();
        assert!(
            store
                .checkpoint(CheckpointOptions::default())
                .await
                .is_err(),
            "checkpoint publication must verify reused data"
        );
        assert!(
            store
                .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
                .await
                .is_err(),
            "checkpoint persistence must verify every owning reference"
        );
    }

    #[test]
    fn compressed_ipc_blocks_are_unsupported() {
        let rows = vec![one_kv_row()];
        let plain = encode_arrow_block(&rows).unwrap();
        let batch = FileReaderBuilder::new()
            .build(Cursor::new(plain))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        for compression in [
            arrow::ipc::CompressionType::LZ4_FRAME,
            arrow::ipc::CompressionType::ZSTD,
        ] {
            let options = arrow::ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(compression))
                .unwrap();
            let mut output = Vec::new();
            let mut writer =
                FileWriter::try_new_with_options(&mut output, batch.schema().as_ref(), options)
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            let block = block_metadata(0, &output, &rows);
            assert!(
                decode_block_rows(&output, &block).is_err(),
                "compressed files must fail preflight"
            );
        }
    }
}

#[cfg(test)]
mod fixture_driver {
    use crate as arco_catalog;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/benches/support/durable_maintenance.rs"
    ));
}
