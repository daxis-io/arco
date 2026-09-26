//! Authority-8 bounded directory roots.

use super::{
    CatalogError, ControlMvpPointer, ControlMvpSegmentLevel, ControlMvpSegmentRow,
    ControlMvpStateStore, ControlMvpTxn, ControlMvpWriteEntry, Result, StagedWrite, StateScope,
    StateToken, StoredValue, TransactionBase, decode_json, directory, encode_json,
    encode_json_limited, encode_segment, invariant_violation, logical_v2, next_logical_sequence,
    physical, precondition_failed, put_immutable_matching, sha256_hex, valid_raw_digest,
    validate_raw_checksum,
};
use arco_core::{AuthorityWritePrecondition, WriteResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod synthetic;
#[cfg(feature = "test-utils")]
pub use synthetic::SyntheticKvEntry;

const AUTHORITY_FORMAT: u32 = 8;
const LEAF_ENCODING: &str = "physical_descriptor_v1";
const MAX_SELECTED_BLOCKS: usize = 16;
const MAX_REWRITTEN_BLOCKS: usize = 32;
type OutboxMutations = BTreeMap<Vec<u8>, Option<ControlMvpSegmentRow>>;
type OutboxTargets<'a> = BTreeMap<
    [u8; 32],
    (
        directory::Leaf,
        Vec<(&'a Vec<u8>, &'a Option<ControlMvpSegmentRow>)>,
    ),
>;

/// Rendered replacement inventory, checked again by independent proof verification.
#[derive(Default)]
struct BoundedCommitWork {
    rewritten: BTreeSet<[u8; 32]>,
}

impl BoundedCommitWork {
    fn render(&mut self, digest: [u8; 32]) -> Result<()> {
        if self.rewritten.insert(digest) {
            super::cost::bounded_work(super::cost::BoundedWork {
                rewritten_blocks: 1,
                ..Default::default()
            });
        }
        self.check_rewritten()
    }

    fn verify_rewrite(&mut self, digest: [u8; 32]) -> Result<()> {
        self.rewritten.insert(digest);
        self.check_rewritten()
    }

    fn check_rewritten(&self) -> Result<()> {
        if self.rewritten.len() > MAX_REWRITTEN_BLOCKS {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "bounded mutation exceeds 32 rewritten physical blocks".into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootBinding {
    role: physical::Role,
    leaf_encoding: String,
    directory_root_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest8 {
    format_version: u32,
    implementation: String,
    scope: StateScope,
    manifest_id: String,
    logical_sequence: u64,
    logical_history: String,
    kind: ManifestKind8,
    parent_manifest_id: Option<String>,
    parent_manifest_sha256: Option<String>,
    writer_epoch: u64,
    reclamation_generation: u64,
    kv_root: RootBinding,
    active_id_root: RootBinding,
    delivery_order_root: RootBinding,
    transaction: Option<ArtifactRef>,
    transition: Option<ArtifactRef>,
    genesis_witness: Option<ArtifactRef>,
    projection_source: Option<ArtifactRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestKind8 {
    Transaction,
    SyntheticGenesis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRef {
    path: String,
    sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Transaction8 {
    format_version: u32,
    scope: StateScope,
    transaction_id: String,
    logical_sequence: u64,
    operation: logical_v2::Operation,
    logical_commit_id: String,
    logical_history: String,
    writes: Vec<BoundedWrite8>,
    additions: Vec<logical_v2::ProjectionIntentV2>,
    trims: Vec<logical_v2::Trim>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundedWrite8 {
    key: Vec<u8>,
    generation: u64,
    delete: bool,
    value_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Transition8 {
    format_version: u32,
    scope: StateScope,
    transaction_id: String,
    roles: Vec<RoleTransition8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleTransition8 {
    role: physical::Role,
    old_root_hex: String,
    new_root_hex: String,
    edits: Vec<EditProof8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EditProof8 {
    old: Option<LeafProof8>,
    new: Vec<LeafProof8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeafProof8 {
    first_hex: String,
    last_hex: String,
    rows: u64,
    bytes: u32,
    digest_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedCandidateV1 {
    record_type: String,
    encoding_version: u32,
    scope: StateScope,
    candidate_id: String,
    kind: ManifestKind8,
    original_head: Option<PreparedHeadV1>,
    candidate_tx: Option<ArtifactRef>,
    candidate_genesis: Option<ArtifactRef>,
    candidate_manifest: ArtifactRef,
    candidate_head: PreparedHeadV1,
    transitions: Option<Vec<RoleTransition8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedHeadV1 {
    raw_bytes: Vec<u8>,
    raw_sha256: String,
    version: Option<String>,
    manifest: ArtifactRef,
    writer_epoch: u64,
    reclamation_generation: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActiveRecord8 {
    record_id: String,
    origin_sequence: u64,
    ordinal: u64,
    source_descriptor_sha256: String,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionSource8 {
    encoding_version: u32,
    scope: StateScope,
    logical_sequence: u64,
    logical_commit_id: String,
    kv_root_hex: String,
}

impl Manifest8 {
    fn root(&self, role: physical::Role) -> Result<&RootBinding> {
        let root = match role {
            physical::Role::Kv => &self.kv_root,
            physical::Role::ActiveId => &self.active_id_root,
            physical::Role::DeliveryOrder => &self.delivery_order_root,
        };
        if root.role != role || root.leaf_encoding != LEAF_ENCODING {
            return Err(invariant_violation(
                "bounded manifest root role or encoding differs",
            ));
        }
        Ok(root)
    }

    fn validate(&self, scope: &StateScope, expected_id: &str) -> Result<()> {
        if self.format_version != AUTHORITY_FORMAT
            || self.implementation != super::IMPLEMENTATION
            || &self.scope != scope
            || self.manifest_id != expected_id
            || !super::integrity::valid_immutable_id(&self.manifest_id)
            || self.logical_sequence == 0
            || !valid_raw_digest(&self.logical_history)
            || self.writer_epoch == u64::MAX
            || self
                .projection_source
                .as_ref()
                .is_some_and(|source| source.path.is_empty() || !valid_raw_digest(&source.sha256))
        {
            return Err(invariant_violation(
                "invalid bounded manifest identity or format",
            ));
        }
        match self.kind {
            ManifestKind8::Transaction
                if self.transaction.as_ref().is_some_and(valid_artifact_ref)
                    && self.transition.as_ref().is_some_and(valid_artifact_ref)
                    && self.genesis_witness.is_none() => {}
            ManifestKind8::SyntheticGenesis
                if self
                    .genesis_witness
                    .as_ref()
                    .is_some_and(valid_artifact_ref)
                    && self.transaction.is_none()
                    && self.transition.is_none()
                    && self.parent_manifest_id.is_none()
                    && self.parent_manifest_sha256.is_none() => {}
            _ => {
                return Err(invariant_violation(
                    "bounded manifest kind and artifacts are inconsistent",
                ));
            }
        }
        if self.parent_manifest_id.is_some() != self.parent_manifest_sha256.is_some()
            || self
                .parent_manifest_id
                .as_deref()
                .is_some_and(|id| !super::integrity::valid_immutable_id(id))
            || self
                .parent_manifest_sha256
                .as_deref()
                .is_some_and(|digest| !valid_raw_digest(digest))
            || self.parent_manifest_id.as_deref() == Some(self.manifest_id.as_str())
            || (self.kind == ManifestKind8::Transaction
                && (self.logical_sequence == 1) != self.parent_manifest_id.is_none())
        {
            return Err(invariant_violation(
                "invalid bounded manifest parent witness",
            ));
        }
        for role in [
            physical::Role::Kv,
            physical::Role::ActiveId,
            physical::Role::DeliveryOrder,
        ] {
            let root = self.root(role)?;
            hex::decode(&root.directory_root_hex)
                .map_err(|_| invariant_violation("invalid bounded directory root hex"))?;
        }
        Ok(())
    }
}

fn valid_artifact_ref(reference: &ArtifactRef) -> bool {
    !reference.path.is_empty() && valid_raw_digest(&reference.sha256)
}

const PREPARED_CANDIDATE_RECORD_TYPE: &str = "arco_control_bounded_prepared_candidate";
const PREPARED_CANDIDATE_ENCODING_VERSION: u32 = 1;
const RECOVERY_MAX_MANIFESTS: usize = 32;
const RECOVERY_MAX_BYTES: usize = 64 * 1024 * 1024;

fn prepared_head(
    store: &ControlMvpStateStore,
    pointer: &ControlMvpPointer,
    bytes: &[u8],
    version: Option<String>,
) -> PreparedHeadV1 {
    PreparedHeadV1 {
        raw_bytes: bytes.to_vec(),
        raw_sha256: sha256_hex(bytes),
        version,
        manifest: ArtifactRef {
            path: store.paths.manifest_object(&pointer.manifest_id),
            sha256: pointer.manifest_checksum_sha256.clone(),
        },
        writer_epoch: pointer.writer_epoch,
        reclamation_generation: pointer.reclamation_generation,
    }
}

fn validate_prepared_head(
    store: &ControlMvpStateStore,
    head: &PreparedHeadV1,
    require_version: bool,
) -> Result<ControlMvpPointer> {
    if sha256_hex(&head.raw_bytes) != head.raw_sha256
        || !valid_raw_digest(&head.raw_sha256)
        || !valid_artifact_ref(&head.manifest)
    {
        return Err(invariant_violation("invalid bounded prepared HEAD witness"));
    }
    if require_version
        != head
            .version
            .as_deref()
            .is_some_and(|version| !version.is_empty())
    {
        return Err(invariant_violation(
            "invalid bounded prepared HEAD version witness",
        ));
    }
    let pointer: ControlMvpPointer = decode_json(&head.raw_bytes, "bounded prepared HEAD")?;
    pointer.validate_versioned(&store.scope, AUTHORITY_FORMAT)?;
    if head.manifest.path != store.paths.manifest_object(&pointer.manifest_id)
        || pointer.manifest_checksum_sha256 != head.manifest.sha256
        || pointer.writer_epoch != head.writer_epoch
        || pointer.reclamation_generation != head.reclamation_generation
    {
        return Err(invariant_violation(
            "bounded prepared HEAD fields differ from raw bytes",
        ));
    }
    Ok(pointer)
}

/// An authenticated format-8 root and its exact pinned HEAD observation.
#[derive(Clone, Debug)]
pub(super) struct Base {
    manifest: Option<Manifest8>,
    manifest_digest: Option<String>,
    pointer_version: Option<String>,
    pointer_bytes: Option<Vec<u8>>,
    writer_epoch: u64,
    reclamation_generation: u64,
    logical_sequence: u64,
    selection: Option<super::lazy::BoundedSelection>,
    kv_root: directory::Root,
    active_id_root: directory::Root,
    delivery_order_root: directory::Root,
}

#[derive(Debug)]
pub(super) struct DeliveryPage8 {
    pub(super) records: Vec<super::ControlMvpProjectionOutboxRecord>,
    pub(super) next_after: Option<Vec<u8>>,
}

/// Opaque continuation for an authority-8 projection delivery page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionContinuationV2 {
    token: StateToken,
    after: Vec<u8>,
}

/// An authority-pinned page of V2 projection envelopes.
#[derive(Debug)]
pub struct ProjectionPageV2 {
    token: StateToken,
    records: Vec<logical_v2::ProjectionIntentV2>,
    continuation: Option<ProjectionContinuationV2>,
}

impl ProjectionPageV2 {
    /// Returns the retained authority token that authenticated this page.
    #[must_use]
    pub const fn token(&self) -> &StateToken {
        &self.token
    }

    /// Returns exact V2 envelopes authenticated through both outbox indexes.
    #[must_use]
    pub fn records(&self) -> &[logical_v2::ProjectionIntentV2] {
        &self.records
    }

    /// Returns the opaque continuation for the next page, if more records remain.
    #[must_use]
    pub const fn continuation(&self) -> Option<&ProjectionContinuationV2> {
        self.continuation.as_ref()
    }
}

/// The authenticated outcome of inspecting a previously prepared authority-8 candidate.
///
/// A direct publication CAS failure is reported as [`CatalogError::CasFailed`]; a restarted
/// inspection has no trusted local failure receipt and therefore returns only evidence-derived
/// outcomes below.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateRecoveryV2 {
    /// The exact candidate HEAD was observed and authenticated.
    Committed(StateToken),
    /// An authenticated conflicting successor chain superseded the candidate.
    Superseded(StateToken),
    /// Available evidence cannot establish a terminal result.
    Unresolved,
}

fn decode_roots(
    directory: &directory::Directory,
    manifest: &Manifest8,
) -> Result<(directory::Root, directory::Root, directory::Root)> {
    let decode_root = |role| -> Result<directory::Root> {
        let root = manifest.root(role)?;
        let bytes = hex::decode(&root.directory_root_hex)
            .map_err(|_| invariant_violation("invalid bounded directory root hex"))?;
        directory.decode_root(&bytes)
    };
    Ok((
        decode_root(physical::Role::Kv)?,
        decode_root(physical::Role::ActiveId)?,
        decode_root(physical::Role::DeliveryOrder)?,
    ))
}

fn base_from_manifest(
    directory: &directory::Directory,
    manifest: Manifest8,
    manifest_digest: String,
    pointer_version: Option<String>,
    pointer_bytes: Option<Vec<u8>>,
    pinned_writer_epoch: u64,
    pinned_reclamation_generation: u64,
) -> Result<Base> {
    let logical_sequence = manifest.logical_sequence;
    let (kv_root, active_id_root, delivery_order_root) = decode_roots(directory, &manifest)?;
    Ok(Base {
        manifest: Some(manifest),
        manifest_digest: Some(manifest_digest),
        pointer_version,
        pointer_bytes,
        writer_epoch: pinned_writer_epoch,
        reclamation_generation: pinned_reclamation_generation,
        logical_sequence,
        selection: None,
        kv_root,
        active_id_root,
        delivery_order_root,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "the manifest and its authenticated artifacts are one validation boundary"
)]
async fn validate_manifest_artifacts(
    store: &ControlMvpStateStore,
    manifest: &Manifest8,
    roots: (&directory::Root, &directory::Root, &directory::Root),
) -> Result<()> {
    if manifest.kind == ManifestKind8::SyntheticGenesis {
        return synthetic::validate_manifest_witness(store, manifest, roots).await;
    }
    let transaction_ref = manifest
        .transaction
        .as_ref()
        .ok_or_else(|| invariant_violation("transaction manifest has no transaction reference"))?;
    let transition_ref = manifest
        .transition
        .as_ref()
        .ok_or_else(|| invariant_violation("transaction manifest has no transition reference"))?;
    let transaction_bytes = store
        .get_json(&transaction_ref.path, super::MAX_TRANSACTION_JSON_BYTES)
        .await?;
    validate_raw_checksum(
        &transaction_bytes,
        Some(&transaction_ref.sha256),
        "bounded manifest transaction checksum",
    )?;
    let transaction: Transaction8 =
        decode_json(&transaction_bytes, "bounded manifest transaction")?;
    if transaction.format_version != AUTHORITY_FORMAT
        || transaction.scope != store.scope
        || transaction.logical_sequence != manifest.logical_sequence
        || transaction.logical_history != manifest.logical_history
        || transaction_ref.path != store.paths.tx_object(&transaction.transaction_id)
    {
        return Err(invariant_violation(
            "bounded manifest transaction differs from manifest",
        ));
    }
    let transition_bytes = store
        .get_json(&transition_ref.path, super::MAX_CONTROL_JSON_BYTES)
        .await?;
    validate_raw_checksum(
        &transition_bytes,
        Some(&transition_ref.sha256),
        "bounded manifest transition checksum",
    )?;
    let transition: Transition8 = decode_json(&transition_bytes, "bounded manifest transition")?;
    if transition.format_version != AUTHORITY_FORMAT
        || transition.scope != store.scope
        || transition.transaction_id != transaction.transaction_id
        || transition.roles.len() != 3
        || transition_ref.path
            != format!(
                "{}/transitions/{}.json",
                store.paths.base_prefix(),
                manifest.manifest_id
            )
    {
        return Err(invariant_violation(
            "bounded manifest transition differs from transaction",
        ));
    }
    for (role, root) in [
        (physical::Role::Kv, roots.0),
        (physical::Role::ActiveId, roots.1),
        (physical::Role::DeliveryOrder, roots.2),
    ] {
        let mut matches = transition.roles.iter().filter(|proof| proof.role == role);
        let proof = matches
            .next()
            .ok_or_else(|| invariant_violation("bounded manifest transition omits a role"))?;
        if matches.next().is_some() || proof.new_root_hex != hex::encode(root.encode()) {
            return Err(invariant_violation(
                "bounded manifest root differs from transition",
            ));
        }
    }
    match (
        &manifest.projection_source,
        transaction.additions.is_empty(),
    ) {
        (None, true) => {}
        (Some(source), false) => {
            let resolved = load_projection_source(store, &source.sha256).await?;
            if source.path
                != format!(
                    "{}/projection-sources/{}.json",
                    store.paths.base_prefix(),
                    source.sha256
                )
                || resolved.logical_sequence != manifest.logical_sequence
                || resolved.logical_commit_id != transaction.logical_commit_id
                || resolved.kv_root_hex != hex::encode(roots.0.encode())
            {
                return Err(invariant_violation(
                    "bounded manifest projection source differs from candidate",
                ));
            }
        }
        _ => {
            return Err(invariant_violation(
                "bounded manifest projection source presence differs from transaction",
            ));
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "prepared-candidate cross-bindings must be audited together"
)]
async fn validate_prepared_candidate(
    store: &ControlMvpStateStore,
    prepared: &PreparedCandidateV1,
    expected_id: &str,
) -> Result<(Manifest8, ControlMvpPointer)> {
    if prepared.record_type != PREPARED_CANDIDATE_RECORD_TYPE
        || prepared.encoding_version != PREPARED_CANDIDATE_ENCODING_VERSION
        || prepared.scope != store.scope
        || prepared.candidate_id != expected_id
        || !super::integrity::valid_immutable_id(&prepared.candidate_id)
        || !valid_artifact_ref(&prepared.candidate_manifest)
        || prepared.candidate_manifest.path != store.paths.manifest_object(expected_id)
    {
        return Err(invariant_violation(
            "invalid bounded prepared candidate identity",
        ));
    }
    let manifest_bytes = store
        .get_json(
            &prepared.candidate_manifest.path,
            super::MAX_CONTROL_JSON_BYTES,
        )
        .await?;
    validate_raw_checksum(
        &manifest_bytes,
        Some(&prepared.candidate_manifest.sha256),
        "bounded prepared candidate manifest checksum",
    )?;
    let manifest: Manifest8 = decode_json(&manifest_bytes, "bounded prepared candidate manifest")?;
    manifest.validate(&store.scope, expected_id)?;
    let pointer = validate_prepared_head(store, &prepared.candidate_head, false)?;
    if pointer.manifest_id != expected_id
        || pointer.manifest_checksum_sha256 != prepared.candidate_manifest.sha256
        || pointer.logical_sequence != manifest.logical_sequence
        || pointer.writer_epoch != manifest.writer_epoch
        || pointer.reclamation_generation != manifest.reclamation_generation
        || prepared.kind != manifest.kind
    {
        return Err(invariant_violation(
            "bounded prepared candidate HEAD differs from manifest",
        ));
    }
    match prepared.kind {
        ManifestKind8::Transaction => {
            let transaction = prepared.candidate_tx.as_ref().ok_or_else(|| {
                invariant_violation("prepared transaction candidate lacks transaction")
            })?;
            if prepared.candidate_genesis.is_some()
                || manifest.transaction.as_ref() != Some(transaction)
                || prepared
                    .transitions
                    .as_ref()
                    .is_none_or(|roles| roles.len() != 3)
            {
                return Err(invariant_violation(
                    "prepared transaction candidate artifacts differ",
                ));
            }
            let transition = manifest.transition.as_ref().ok_or_else(|| {
                invariant_violation("prepared transaction candidate lacks transition")
            })?;
            let bytes = store
                .get_json(&transition.path, super::MAX_CONTROL_JSON_BYTES)
                .await?;
            validate_raw_checksum(
                &bytes,
                Some(&transition.sha256),
                "prepared transition checksum",
            )?;
            let actual: Transition8 = decode_json(&bytes, "prepared transition")?;
            if prepared.transitions.as_ref() != Some(&actual.roles) {
                return Err(invariant_violation(
                    "prepared transitions differ from transition artifact",
                ));
            }
        }
        ManifestKind8::SyntheticGenesis => {
            let matching_genesis =
                prepared.candidate_genesis.as_ref() == manifest.genesis_witness.as_ref();
            if prepared.candidate_tx.is_some()
                || prepared.transitions.is_some()
                || !matching_genesis
            {
                return Err(invariant_violation(
                    "prepared synthetic candidate artifacts differ",
                ));
            }
        }
    }
    match &prepared.original_head {
        Some(original) => {
            let original_pointer = validate_prepared_head(store, original, true)?;
            if manifest.parent_manifest_id.as_deref() != Some(original_pointer.manifest_id.as_str())
                || manifest.parent_manifest_sha256.as_deref()
                    != Some(original_pointer.manifest_checksum_sha256.as_str())
                || manifest.logical_sequence
                    != original_pointer
                        .logical_sequence
                        .checked_add(1)
                        .ok_or_else(|| {
                            invariant_violation("prepared original sequence overflows")
                        })?
            {
                return Err(invariant_violation(
                    "prepared original HEAD differs from candidate parent",
                ));
            }
        }
        None if manifest.parent_manifest_id.is_some()
            || manifest.parent_manifest_sha256.is_some() =>
        {
            return Err(invariant_violation(
                "prepared absent HEAD differs from candidate parent",
            ));
        }
        None => {}
    }
    let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
    let roots = decode_roots(&directory, &manifest)?;
    if let Some(transitions) = &prepared.transitions {
        let old_roots = if let Some(original) = &prepared.original_head {
            let pointer = validate_prepared_head(store, original, true)?;
            let bytes = store
                .get_json(&original.manifest.path, super::MAX_CONTROL_JSON_BYTES)
                .await?;
            validate_raw_checksum(
                &bytes,
                Some(&pointer.manifest_checksum_sha256),
                "prepared original manifest checksum",
            )?;
            let old: Manifest8 = decode_json(&bytes, "prepared original manifest")?;
            old.validate(&store.scope, &pointer.manifest_id)?;
            decode_roots(&directory, &old)?
        } else {
            let empty = directory.empty_root_reference()?;
            (empty.clone(), empty.clone(), empty)
        };
        for (role, old_root, new_root) in [
            (physical::Role::Kv, &old_roots.0, &roots.0),
            (physical::Role::ActiveId, &old_roots.1, &roots.1),
            (physical::Role::DeliveryOrder, &old_roots.2, &roots.2),
        ] {
            let matches = transitions
                .iter()
                .filter(|transition| transition.role == role)
                .count();
            let Some(transition) = transitions
                .iter()
                .find(|transition| transition.role == role)
            else {
                return Err(invariant_violation("prepared transition omits a role"));
            };
            if matches != 1
                || transition.old_root_hex != hex::encode(old_root.encode())
                || transition.new_root_hex != hex::encode(new_root.encode())
            {
                return Err(invariant_violation(
                    "prepared transition roots differ from manifests",
                ));
            }
        }
    }
    validate_manifest_artifacts(store, &manifest, (&roots.0, &roots.1, &roots.2)).await?;
    Ok((manifest, pointer))
}

async fn prepared_candidate(
    store: &ControlMvpStateStore,
    base: &Base,
    manifest: &Manifest8,
    manifest_ref: ArtifactRef,
    candidate_head: PreparedHeadV1,
) -> Result<PreparedCandidateV1> {
    let original_head = match (base.pointer_bytes(), base.pointer_version()) {
        (None, None) => None,
        (Some(bytes), Some(version)) => {
            let pointer: ControlMvpPointer = decode_json(bytes, "bounded original HEAD")?;
            Some(prepared_head(
                store,
                &pointer,
                bytes,
                Some(version.to_owned()),
            ))
        }
        _ => {
            return Err(invariant_violation(
                "bounded base HEAD witness is incomplete",
            ));
        }
    };
    let transitions = match manifest.kind {
        ManifestKind8::Transaction => {
            let transition = manifest
                .transition
                .as_ref()
                .ok_or_else(|| invariant_violation("transaction candidate has no transition"))?;
            let bytes = store
                .get_json(&transition.path, super::MAX_CONTROL_JSON_BYTES)
                .await?;
            validate_raw_checksum(
                &bytes,
                Some(&transition.sha256),
                "candidate transition checksum",
            )?;
            Some(decode_json::<Transition8>(&bytes, "candidate transition")?.roles)
        }
        ManifestKind8::SyntheticGenesis => None,
    };
    Ok(PreparedCandidateV1 {
        record_type: PREPARED_CANDIDATE_RECORD_TYPE.to_owned(),
        encoding_version: PREPARED_CANDIDATE_ENCODING_VERSION,
        scope: store.scope.clone(),
        candidate_id: manifest.manifest_id.clone(),
        kind: manifest.kind,
        original_head,
        candidate_tx: manifest.transaction.clone(),
        candidate_genesis: manifest.genesis_witness.clone(),
        candidate_manifest: manifest_ref,
        candidate_head,
        transitions,
    })
}

async fn publish_manifest_candidate(
    store: &ControlMvpStateStore,
    base: &Base,
    manifest: Manifest8,
) -> Result<StateToken> {
    manifest.validate(&store.scope, &manifest.manifest_id)?;
    let manifest_path = store.paths.manifest_object(&manifest.manifest_id);
    let manifest_bytes =
        encode_json_limited(&manifest, super::MAX_CONTROL_JSON_BYTES, "bounded manifest")?;
    let manifest_ref = ArtifactRef {
        path: manifest_path.clone(),
        sha256: sha256_hex(&manifest_bytes),
    };
    put_immutable_matching(
        &store.storage,
        &manifest_path,
        manifest_bytes,
        "bounded manifest already exists with different bytes",
    )
    .await?;
    let pointer = ControlMvpPointer {
        reclamation_generation: base.reclamation_generation(),
        format_version: AUTHORITY_FORMAT,
        implementation: super::IMPLEMENTATION.to_string(),
        scope: store.scope.clone(),
        manifest_id: manifest.manifest_id.clone(),
        logical_sequence: manifest.logical_sequence,
        manifest_checksum_sha256: manifest_ref.sha256.clone(),
        writer_epoch: base.writer_epoch(),
        claim_id: None,
    };
    let pointer_bytes = encode_json_limited(
        &pointer,
        super::MAX_HEAD_JSON_BYTES,
        "bounded candidate HEAD",
    )?;
    let prepared = prepared_candidate(
        store,
        base,
        &manifest,
        manifest_ref.clone(),
        prepared_head(store, &pointer, &pointer_bytes, None),
    )
    .await?;
    validate_prepared_candidate(store, &prepared, &manifest.manifest_id).await?;
    let prepared_path = format!(
        "{}/prepared/{}.json",
        store.paths.base_prefix(),
        manifest.manifest_id
    );
    put_immutable_matching(
        &store.storage,
        &prepared_path,
        encode_json_limited(
            &prepared,
            super::MAX_CONTROL_JSON_BYTES,
            "bounded prepared candidate",
        )?,
        "bounded prepared candidate already exists with different bytes",
    )
    .await?;
    let precondition = base
        .pointer_version()
        .map_or(AuthorityWritePrecondition::DoesNotExist, |version| {
            AuthorityWritePrecondition::MatchesVersion(version.to_string())
        });
    let publication = store
        .storage
        .put(&store.paths.current_pointer(), pointer_bytes, precondition)
        .await;
    match publication {
        Err(error) => match store.reconcile_candidate_v2(&manifest.manifest_id).await {
            Ok(CandidateRecoveryV2::Committed(token)) => Ok(token),
            outcome => Err(CatalogError::AmbiguousAuthorityOutcome {
                message: format!(
                    "bounded authority HEAD outcome for candidate {} is ambiguous ({outcome:?}); write error: {error}",
                    manifest.manifest_id
                ),
            }),
        },
        Ok(WriteResult::Success { .. }) => Ok(store
            .token(manifest.manifest_id, manifest.logical_sequence)
            .with_manifest_witness(manifest_ref.sha256)),
        Ok(WriteResult::PreconditionFailed { .. }) => Err(CatalogError::CasFailed {
            message: "bounded authority HEAD changed before candidate publication".into(),
        }),
    }
}

async fn recovery_manifest(
    store: &ControlMvpStateStore,
    manifest_id: &str,
    digest: &str,
) -> Result<Manifest8> {
    if !valid_raw_digest(digest) {
        return Err(invariant_violation(
            "invalid bounded recovery manifest digest",
        ));
    }
    let bytes = store
        .get_json(
            &store.paths.manifest_object(manifest_id),
            super::MAX_CONTROL_JSON_BYTES,
        )
        .await?;
    validate_raw_checksum(&bytes, Some(digest), "bounded recovery manifest checksum")?;
    let manifest: Manifest8 = decode_json(&bytes, "bounded recovery manifest")?;
    manifest.validate(&store.scope, manifest_id)?;
    for reference in [
        manifest.transaction.as_ref(),
        manifest.transition.as_ref(),
        manifest.genesis_witness.as_ref(),
        manifest.projection_source.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        let limit = if manifest.transaction.as_ref() == Some(reference) {
            super::MAX_TRANSACTION_JSON_BYTES
        } else {
            super::MAX_CONTROL_JSON_BYTES
        };
        let bytes = store.get_json(&reference.path, limit).await?;
        validate_raw_checksum(
            &bytes,
            Some(&reference.sha256),
            "bounded recovery artifact checksum",
        )?;
    }
    Ok(manifest)
}

fn leaf_proof(leaf: &directory::Leaf) -> LeafProof8 {
    LeafProof8 {
        first_hex: hex::encode(&leaf.first),
        last_hex: hex::encode(&leaf.last),
        rows: leaf.rows,
        bytes: leaf.bytes,
        digest_hex: hex::encode(leaf.digest),
    }
}

fn role_transition(
    role: physical::Role,
    old: &directory::Root,
    new: &directory::Root,
    edits: Vec<EditProof8>,
) -> RoleTransition8 {
    RoleTransition8 {
        role,
        old_root_hex: hex::encode(old.encode()),
        new_root_hex: hex::encode(new.encode()),
        edits,
    }
}

fn leaf_from_proof(proof: &LeafProof8) -> Result<directory::Leaf> {
    let digest = hex::decode(&proof.digest_hex)
        .map_err(|_| invariant_violation("transition leaf digest is not hexadecimal"))?;
    let digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| invariant_violation("transition leaf digest has wrong length"))?;
    Ok(directory::Leaf {
        first: hex::decode(&proof.first_hex)
            .map_err(|_| invariant_violation("transition leaf first key is not hexadecimal"))?,
        last: hex::decode(&proof.last_hex)
            .map_err(|_| invariant_violation("transition leaf last key is not hexadecimal"))?,
        rows: proof.rows,
        bytes: proof.bytes,
        digest,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "all three authenticated role roots and the frozen transaction form one proof boundary"
)]
#[allow(
    clippy::too_many_lines,
    reason = "the transition proof must be verified as one auditable flow"
)]
async fn verify_candidate_transition(
    store: &ControlMvpStateStore,
    base: &Base,
    candidate_kv: &directory::Root,
    candidate_active: &directory::Root,
    candidate_delivery: &directory::Root,
    transaction: &Transaction8,
    transition: &Transition8,
    work: &mut BoundedCommitWork,
) -> Result<Vec<ControlMvpWriteEntry>> {
    let candidate_sequence = next_logical_sequence(base.logical_sequence(), "bounded verifier")?;
    if transition.format_version != AUTHORITY_FORMAT
        || transition.scope != store.scope
        || transition.transaction_id != transaction.transaction_id
        || transition.roles.len() != 3
        || transaction.format_version != AUTHORITY_FORMAT
        || transaction.scope != store.scope
        || transaction.logical_sequence != candidate_sequence
        || !valid_raw_digest(&transaction.logical_commit_id)
        || !valid_raw_digest(&transaction.logical_history)
    {
        return Err(invariant_violation(
            "candidate transition identity is invalid",
        ));
    }
    let prior_history = if base.logical_sequence() == 0 {
        logical_v2::genesis(&store.scope)
    } else {
        base.logical_history().to_owned()
    };
    if logical_v2::commit_id(
        &store.scope,
        &prior_history,
        transaction.logical_sequence,
        &transaction.operation,
    )? != transaction.logical_commit_id
    {
        return Err(invariant_violation(
            "candidate logical commit ID differs from frozen operation",
        ));
    }
    let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
    let mut budget = directory::ReadBudget::default();
    let expected = [
        (physical::Role::Kv, &base.kv_root, candidate_kv),
        (
            physical::Role::ActiveId,
            &base.active_id_root,
            candidate_active,
        ),
        (
            physical::Role::DeliveryOrder,
            &base.delivery_order_root,
            candidate_delivery,
        ),
    ];
    let mut kv_old = BTreeMap::new();
    let mut kv_new = BTreeMap::new();
    let mut active_old = BTreeMap::new();
    let mut active_new = BTreeMap::new();
    let mut delivery_old = BTreeMap::new();
    let mut delivery_new = BTreeMap::new();
    for (role, old_root, new_root) in expected {
        let mut proofs = transition.roles.iter().filter(|proof| proof.role == role);
        let proof = proofs
            .next()
            .ok_or_else(|| invariant_violation("candidate transition role is absent"))?;
        if proofs.next().is_some() {
            return Err(invariant_violation("candidate transition repeats a role"));
        }
        if proof.old_root_hex != hex::encode(old_root.encode())
            || proof.new_root_hex != hex::encode(new_root.encode())
        {
            return Err(invariant_violation(
                "candidate transition root differs from proof",
            ));
        }
        let edits = proof
            .edits
            .iter()
            .map(|edit| {
                Ok(directory::update::Edit {
                    old: edit.old.as_ref().map(leaf_from_proof).transpose()?,
                    new: edit
                        .new
                        .iter()
                        .map(leaf_from_proof)
                        .collect::<Result<Vec<_>>>()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        directory
            .verify_update(old_root, new_root, &edits, &mut budget)
            .await?;
        for edit in edits {
            if let Some(old) = edit.old {
                base.selection().select(&old)?;
                let rows = store.resolve_physical_block(role, &old).await?;
                super::cost::bounded_work(super::cost::BoundedWork {
                    transition_proof_rows: rows.len() as u64,
                    ..Default::default()
                });
                for row in rows {
                    let target = match role {
                        physical::Role::Kv => &mut kv_old,
                        physical::Role::ActiveId => &mut active_old,
                        physical::Role::DeliveryOrder => &mut delivery_old,
                    };
                    if target.insert(row.key.clone(), row).is_some() {
                        return Err(invariant_violation(
                            "candidate transition repeats an old physical row",
                        ));
                    }
                }
            }
            for new in edit.new {
                work.verify_rewrite(new.digest)?;
                let rows = store.resolve_physical_block(role, &new).await?;
                super::cost::bounded_work(super::cost::BoundedWork {
                    transition_proof_rows: rows.len() as u64,
                    ..Default::default()
                });
                for row in rows {
                    if row.logical_sequence != transaction.logical_sequence {
                        return Err(invariant_violation(
                            "candidate transition has a new row at the wrong sequence",
                        ));
                    }
                    let target = match role {
                        physical::Role::Kv => &mut kv_new,
                        physical::Role::ActiveId => &mut active_new,
                        physical::Role::DeliveryOrder => &mut delivery_new,
                    };
                    if target.insert(row.key.clone(), row).is_some() {
                        return Err(invariant_violation(
                            "candidate transition repeats a new physical row",
                        ));
                    }
                }
            }
        }
    }
    let writes = verify_kv_mutation(transaction, &kv_old, &kv_new)?;
    verify_outbox_mutation(
        store,
        candidate_kv,
        transaction,
        &active_old,
        &active_new,
        &delivery_old,
        &delivery_new,
    )
    .await?;
    if logical_v2::history(
        &store.scope,
        &prior_history,
        transaction.logical_sequence,
        &transaction.logical_commit_id,
        &writes,
        &transaction.additions,
        &transaction.trims,
    )? != transaction.logical_history
    {
        return Err(invariant_violation(
            "candidate logical history differs from authenticated rows",
        ));
    }
    Ok(writes)
}

fn verify_kv_mutation(
    transaction: &Transaction8,
    old: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
    new: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
) -> Result<Vec<ControlMvpWriteEntry>> {
    let mut writes = BTreeMap::new();
    for write in &transaction.writes {
        if write.key.is_empty()
            || write.generation == 0
            || write.generation > transaction.logical_sequence
            || write.delete != write.value_sha256.is_none()
            || write
                .value_sha256
                .as_deref()
                .is_some_and(|digest| !valid_raw_digest(digest))
            || writes.insert(write.key.clone(), write).is_some()
        {
            return Err(invariant_violation(
                "bounded transaction has invalid KV mutation",
            ));
        }
    }
    for (key, candidate) in new {
        let Some(write) = writes.get(key) else {
            let Some(previous) = old.get(key) else {
                return Err(invariant_violation(
                    "candidate transition added an undeclared KV row",
                ));
            };
            if candidate.record_kind != previous.record_kind
                || candidate.generation != previous.generation
                || candidate.tombstone != previous.tombstone
                || candidate.value != previous.value
                || candidate.origin_sequence != previous.origin_sequence
            {
                return Err(invariant_violation(
                    "candidate transition changed an undeclared KV row",
                ));
            }
            continue;
        };
        if candidate.record_kind != super::SEGMENT_RECORD_KV
            || candidate.generation != write.generation
            || candidate.tombstone != write.delete
            || (write.delete && candidate.value.is_some())
            || (!write.delete
                && candidate
                    .value
                    .as_deref()
                    .is_none_or(|value| Some(sha256_hex(value)) != write.value_sha256))
        {
            return Err(invariant_violation(
                "candidate KV row differs from declared mutation",
            ));
        }
    }
    for key in writes.keys() {
        if !new.contains_key(key) {
            return Err(invariant_violation(
                "declared KV mutation is absent from candidate rows",
            ));
        }
    }
    for key in old.keys() {
        if !new.contains_key(key) {
            return Err(invariant_violation("candidate transition removed a KV row"));
        }
    }
    writes
        .into_iter()
        .map(|(key, write)| {
            let candidate = new.get(&key).ok_or_else(|| {
                invariant_violation("declared KV mutation is absent from candidate rows")
            })?;
            Ok(ControlMvpWriteEntry {
                key,
                generation: candidate.generation,
                value: (!write.delete).then(|| candidate.value.clone()).flatten(),
            })
        })
        .collect()
}

#[allow(
    clippy::too_many_lines,
    reason = "both outbox indexes are cross-validated in one proof"
)]
async fn verify_outbox_mutation(
    store: &ControlMvpStateStore,
    candidate_kv: &directory::Root,
    transaction: &Transaction8,
    active_old: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
    active_new: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
    delivery_old: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
    delivery_new: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
) -> Result<()> {
    let additions = transaction
        .additions
        .iter()
        .map(|intent| (intent.intent_id(), intent))
        .collect::<BTreeMap<_, _>>();
    if additions.len() != transaction.additions.len() {
        return Err(invariant_violation(
            "bounded transaction repeats a V2 projection ID",
        ));
    }
    let trims = transaction
        .trims
        .iter()
        .map(|trim| (trim.record_id.as_str(), trim))
        .collect::<BTreeMap<_, _>>();
    if trims.len() != transaction.trims.len() {
        return Err(invariant_violation(
            "bounded transaction repeats a V2 projection trim",
        ));
    }
    for intent in &transaction.additions {
        if intent.source_scope() != &store.scope
            || intent.source_logical_sequence() != transaction.logical_sequence
            || intent.logical_commit_id() != transaction.logical_commit_id
        {
            return Err(invariant_violation(
                "bounded transaction has invalid V2 projection identity",
            ));
        }
        let key = intent.intent_id().as_bytes();
        let active = active_new
            .get(key)
            .ok_or_else(|| invariant_violation("bounded V2 active addition is absent"))?;
        let bytes = active
            .value
            .as_deref()
            .ok_or_else(|| invariant_violation("bounded V2 active addition has no payload"))?;
        let outer: ActiveRecord8 = decode_json(bytes, "bounded V2 active addition")?;
        if outer.record_id != intent.intent_id()
            || outer.origin_sequence != intent.source_logical_sequence()
            || outer.ordinal != intent.ordinal()
            || outer.payload.as_slice()
                != encode_json(intent, "bounded V2 projection intent")?.as_ref()
        {
            return Err(invariant_violation(
                "bounded V2 active addition differs from its envelope",
            ));
        }
        let source = load_projection_source(store, &outer.source_descriptor_sha256).await?;
        if source.logical_sequence != transaction.logical_sequence
            || source.logical_commit_id != transaction.logical_commit_id
            || source.kv_root_hex != hex::encode(candidate_kv.encode())
        {
            return Err(invariant_violation(
                "bounded V2 projection source differs from candidate",
            ));
        }
        let delivery_key = delivery_key(
            intent.source_logical_sequence(),
            intent.ordinal(),
            intent.intent_id(),
        )?;
        let delivery = delivery_new
            .get(&delivery_key)
            .ok_or_else(|| invariant_violation("bounded V2 delivery addition is absent"))?;
        if delivery.value.as_deref() != Some(outer.source_descriptor_sha256.as_bytes())
            || delivery.origin_sequence != Some(intent.source_logical_sequence())
            || delivery.logical_ordinal != intent.ordinal()
        {
            return Err(invariant_violation(
                "bounded V2 delivery addition differs from active source",
            ));
        }
    }
    for trim in &transaction.trims {
        let active = active_old
            .get(trim.record_id.as_bytes())
            .ok_or_else(|| precondition_failed("bounded V2 trim active row is absent"))?;
        let bytes = active
            .value
            .as_deref()
            .ok_or_else(|| invariant_violation("bounded V2 trim active row has no payload"))?;
        let outer: ActiveRecord8 = decode_json(bytes, "bounded V2 trim active row")?;
        if outer.record_id != trim.record_id
            || outer.origin_sequence != trim.origin_sequence
            || outer.ordinal != trim.ordinal
            || active_new.contains_key(trim.record_id.as_bytes())
        {
            return Err(invariant_violation(
                "bounded V2 trim differs from active incarnation",
            ));
        }
        let delivery_key = delivery_key(trim.origin_sequence, trim.ordinal, &trim.record_id)?;
        if !delivery_old.contains_key(&delivery_key) || delivery_new.contains_key(&delivery_key) {
            return Err(invariant_violation(
                "bounded V2 trim differs from delivery incarnation",
            ));
        }
    }
    let mut addition_keys = BTreeSet::new();
    for intent in &transaction.additions {
        addition_keys.insert(intent.intent_id().as_bytes().to_vec());
        addition_keys.insert(delivery_key(
            intent.source_logical_sequence(),
            intent.ordinal(),
            intent.intent_id(),
        )?);
    }
    let mut trim_keys = BTreeSet::new();
    for trim in &transaction.trims {
        trim_keys.insert(trim.record_id.as_bytes().to_vec());
        trim_keys.insert(delivery_key(
            trim.origin_sequence,
            trim.ordinal,
            &trim.record_id,
        )?);
    }
    verify_unchanged_outbox_rows(active_old, active_new, &addition_keys, &trim_keys)?;
    verify_unchanged_outbox_rows(delivery_old, delivery_new, &addition_keys, &trim_keys)?;
    Ok(())
}

fn verify_unchanged_outbox_rows(
    old: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
    new: &BTreeMap<Vec<u8>, ControlMvpSegmentRow>,
    additions: &BTreeSet<Vec<u8>>,
    trims: &BTreeSet<Vec<u8>>,
) -> Result<()> {
    for (key, row) in new {
        if additions.contains(key) {
            continue;
        }
        let prior = old.get(key).ok_or_else(|| {
            invariant_violation("bounded transition added an undeclared outbox row")
        })?;
        if row.record_kind != prior.record_kind
            || row.value != prior.value
            || row.generation != prior.generation
            || row.tombstone != prior.tombstone
            || row.logical_ordinal != prior.logical_ordinal
            || row.origin_sequence != prior.origin_sequence
        {
            return Err(invariant_violation(
                "bounded transition changed an undeclared outbox row",
            ));
        }
    }
    for key in old.keys() {
        if new.contains_key(key) {
            continue;
        }
        if !trims.contains(key) {
            return Err(invariant_violation(
                "bounded transition removed an undeclared outbox row",
            ));
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "physical block publication binds segment, index, and descriptor bytes"
)]
async fn persist_role_rows(
    store: &ControlMvpStateStore,
    role: physical::Role,
    manifest_id: &str,
    part: usize,
    sequence: u64,
    rows: &[ControlMvpSegmentRow],
) -> Result<Vec<directory::Leaf>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let segment_id = super::state_segment_id_for_manifest(
        manifest_id,
        1 + (role as usize * (MAX_SELECTED_BLOCKS + 1)) + part,
    );
    let (segment_bytes, index_bytes, reference) = encode_segment(
        &segment_id,
        ControlMvpSegmentLevel::L1,
        sequence,
        &store.scope,
        rows,
        store.segment_limits,
    )?;
    let segment_path = store.paths.state_object(&segment_id);
    let index_path = store.paths.segment_index(&segment_id);
    put_immutable_matching(
        &store.storage,
        &segment_path,
        segment_bytes,
        "bounded physical segment already exists with different bytes",
    )
    .await?;
    put_immutable_matching(
        &store.storage,
        &index_path,
        index_bytes.clone(),
        "bounded physical index already exists with different bytes",
    )
    .await?;
    let segment_version = store
        .storage
        .head(&segment_path)
        .await?
        .ok_or_else(|| invariant_violation("bounded physical segment disappeared after write"))?
        .version;
    let index_version = store
        .storage
        .head(&index_path)
        .await?
        .ok_or_else(|| invariant_violation("bounded physical index disappeared after write"))?
        .version;
    if segment_version.is_empty() || index_version.is_empty() {
        return Err(invariant_violation(
            "bounded physical object has empty version",
        ));
    }
    let index: super::ControlMvpSegmentIndex = decode_json(&index_bytes, "bounded physical index")?;
    let mut leaves = Vec::with_capacity(index.blocks.len());
    for block in index.blocks {
        let descriptor = physical::Descriptor {
            encoding_version: 1,
            scope: store.scope.clone(),
            role,
            segment: reference.clone(),
            segment_version: segment_version.clone(),
            index_version: index_version.clone(),
            block: block.clone(),
        };
        let descriptor_bytes = encode_json(&descriptor, "bounded physical descriptor")?;
        let descriptor_digest = sha256_hex(&descriptor_bytes);
        let digest: [u8; 32] = hex::decode(&descriptor_digest)
            .map_err(|_| invariant_violation("bounded descriptor digest is invalid"))?
            .try_into()
            .map_err(|_| invariant_violation("bounded descriptor digest has wrong length"))?;
        let descriptor_path = format!(
            "{}/physical/descriptors/{}.json",
            store.paths.base_prefix(),
            hex::encode(digest)
        );
        put_immutable_matching(
            &store.storage,
            &descriptor_path,
            descriptor_bytes,
            "bounded physical descriptor already exists with different bytes",
        )
        .await?;
        let first = hex::decode(
            block
                .min_key_hex
                .as_deref()
                .ok_or_else(|| invariant_violation("bounded descriptor block minimum absent"))?,
        )
        .map_err(|_| invariant_violation("bounded descriptor block minimum invalid"))?;
        let last = hex::decode(
            block
                .max_key_hex
                .as_deref()
                .ok_or_else(|| invariant_violation("bounded descriptor block maximum absent"))?,
        )
        .map_err(|_| invariant_violation("bounded descriptor block maximum invalid"))?;
        leaves.push(directory::Leaf {
            first,
            last,
            rows: block.row_count,
            bytes: u32::try_from(block.length)
                .map_err(|_| invariant_violation("bounded block length overflow"))?,
            digest,
        });
    }
    Ok(leaves)
}

#[allow(
    clippy::too_many_lines,
    reason = "one KV rewrite atomically derives rows, blocks, and directory edits"
)]
async fn rewrite_kv(
    store: &ControlMvpStateStore,
    directory: &directory::Directory,
    base: &Base,
    manifest_id: &str,
    sequence: u64,
    writes: &[ControlMvpWriteEntry],
    work: &mut BoundedCommitWork,
) -> Result<(directory::Root, Vec<directory::update::Edit>)> {
    if writes.is_empty() {
        return Ok((base.kv_root.clone(), Vec::new()));
    }
    let mut budget = directory::ReadBudget::default();
    let mut targets: BTreeMap<[u8; 32], (directory::Leaf, Vec<&ControlMvpWriteEntry>)> =
        BTreeMap::new();
    let mut genesis = Vec::new();
    for write in writes {
        match directory
            .floor_leaf(&base.kv_root, &write.key, &mut budget)
            .await?
        {
            Some(leaf) => targets
                .entry(leaf.digest)
                .or_insert_with(|| (leaf, Vec::new()))
                .1
                .push(write),
            None => genesis.push(write),
        }
    }
    let selection = base.selection();
    for (leaf, _) in targets.values() {
        selection.select(leaf)?;
    }
    let mut edits = Vec::new();
    for (part, (_, (leaf, group))) in targets.into_iter().enumerate() {
        let mut rows = store
            .resolve_physical_block(physical::Role::Kv, &leaf)
            .await?
            .into_iter()
            .map(|row| (row.key.clone(), row))
            .collect::<BTreeMap<_, _>>();
        for write in group {
            rows.insert(
                write.key.clone(),
                ControlMvpSegmentRow {
                    record_kind: super::SEGMENT_RECORD_KV,
                    key: write.key.clone(),
                    value: write.value.clone(),
                    generation: write.generation,
                    tombstone: write.value.is_none(),
                    logical_sequence: sequence,
                    logical_ordinal: 0,
                    origin_sequence: None,
                },
            );
        }
        let rows = rows
            .into_values()
            .enumerate()
            .map(|(ordinal, mut row)| {
                row.logical_sequence = sequence;
                row.logical_ordinal = ordinal as u64;
                row
            })
            .collect::<Vec<_>>();
        let replacements = persist_role_rows(
            store,
            physical::Role::Kv,
            manifest_id,
            part,
            sequence,
            &rows,
        )
        .await?;
        for replacement in &replacements {
            work.render(replacement.digest)?;
        }
        edits.push(directory::update::Edit {
            old: Some(leaf),
            new: replacements,
        });
    }
    if !genesis.is_empty() {
        let rows = genesis
            .into_iter()
            .enumerate()
            .map(|(ordinal, write)| ControlMvpSegmentRow {
                record_kind: super::SEGMENT_RECORD_KV,
                key: write.key.clone(),
                value: write.value.clone(),
                generation: write.generation,
                tombstone: write.value.is_none(),
                logical_sequence: sequence,
                logical_ordinal: ordinal as u64,
                origin_sequence: None,
            })
            .collect::<Vec<_>>();
        let replacements = persist_role_rows(
            store,
            physical::Role::Kv,
            manifest_id,
            edits.len(),
            sequence,
            &rows,
        )
        .await?;
        for replacement in &replacements {
            work.render(replacement.digest)?;
        }
        edits.push(directory::update::Edit {
            old: None,
            new: replacements,
        });
    }
    let root = directory.update(&base.kv_root, &edits, &mut budget).await?;
    Ok((root, edits))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the role rewrite needs its root, authenticated selection, and publication identity together"
)]
async fn rewrite_outbox_role(
    store: &ControlMvpStateStore,
    directory: &directory::Directory,
    root: &directory::Root,
    role: physical::Role,
    manifest_id: &str,
    sequence: u64,
    mutations: &OutboxMutations,
    selection: &super::lazy::BoundedSelection,
    work: &mut BoundedCommitWork,
) -> Result<(directory::Root, Vec<directory::update::Edit>)> {
    if mutations.is_empty() {
        return Ok((root.clone(), Vec::new()));
    }
    let mut budget = directory::ReadBudget::default();
    let mut targets: OutboxTargets<'_> = BTreeMap::new();
    let mut genesis = Vec::new();
    for mutation in mutations {
        match directory.floor_leaf(root, mutation.0, &mut budget).await? {
            Some(leaf) => targets
                .entry(leaf.digest)
                .or_insert_with(|| (leaf, Vec::new()))
                .1
                .push(mutation),
            None => genesis.push(mutation),
        }
    }
    for (leaf, _) in targets.values() {
        selection.select(leaf)?;
    }
    let mut edits = Vec::new();
    for (part, (_, (leaf, group))) in targets.into_iter().enumerate() {
        let mut rows = store
            .resolve_physical_block(role, &leaf)
            .await?
            .into_iter()
            .map(|row| (row.key.clone(), row))
            .collect::<BTreeMap<_, _>>();
        for (key, row) in group {
            match row {
                Some(row) => {
                    rows.insert(key.clone(), row.clone());
                }
                None => {
                    if rows.remove(key).is_none() {
                        return Err(precondition_failed(
                            "cannot trim an absent V2 projection row",
                        ));
                    }
                }
            }
        }
        let rows = rows
            .into_values()
            .map(|mut row| {
                row.logical_sequence = sequence;
                row
            })
            .collect::<Vec<_>>();
        let replacements =
            persist_role_rows(store, role, manifest_id, part, sequence, &rows).await?;
        for replacement in &replacements {
            work.render(replacement.digest)?;
        }
        edits.push(directory::update::Edit {
            old: Some(leaf),
            new: replacements,
        });
    }
    if !genesis.is_empty() {
        let mut rows = Vec::with_capacity(genesis.len());
        for (key, row) in genesis {
            let row = row
                .clone()
                .ok_or_else(|| precondition_failed("cannot trim an absent V2 projection row"))?;
            if row.key != *key {
                return Err(invariant_violation(
                    "outbox mutation key differs from its row",
                ));
            }
            rows.push(row);
        }
        let replacements =
            persist_role_rows(store, role, manifest_id, edits.len(), sequence, &rows).await?;
        for replacement in &replacements {
            work.render(replacement.digest)?;
        }
        edits.push(directory::update::Edit {
            old: None,
            new: replacements,
        });
    }
    let root = directory.update(root, &edits, &mut budget).await?;
    Ok((root, edits))
}

fn staged_v2_intents(
    txn: &ControlMvpTxn,
    sequence: u64,
    logical_commit_id: &str,
) -> Result<Vec<logical_v2::ProjectionIntentV2>> {
    txn.projection_intents
        .iter()
        .enumerate()
        .map(|(ordinal, staged)| {
            logical_v2::ProjectionIntentV2::new(
                &staged.intent_id,
                &staged.projection_kind,
                txn.store.scope.clone(),
                sequence,
                logical_commit_id,
                ordinal as u64,
                &staged.payload,
            )
        })
        .collect()
}

async fn write_projection_source(
    store: &ControlMvpStateStore,
    sequence: u64,
    logical_commit_id: &str,
    kv_root: &directory::Root,
) -> Result<ArtifactRef> {
    let source = ProjectionSource8 {
        encoding_version: 1,
        scope: store.scope.clone(),
        logical_sequence: sequence,
        logical_commit_id: logical_commit_id.to_owned(),
        kv_root_hex: hex::encode(kv_root.encode()),
    };
    let bytes = encode_json_limited(
        &source,
        super::MAX_CONTROL_JSON_BYTES,
        "bounded projection source",
    )?;
    let sha256 = sha256_hex(&bytes);
    let path = format!(
        "{}/projection-sources/{sha256}.json",
        store.paths.base_prefix()
    );
    put_immutable_matching(
        &store.storage,
        &path,
        bytes,
        "bounded projection source already exists with different bytes",
    )
    .await?;
    Ok(ArtifactRef { path, sha256 })
}

fn outbox_mutations(
    additions: &[logical_v2::ProjectionIntentV2],
    trims: &[logical_v2::Trim],
    sequence: u64,
    source: Option<&ArtifactRef>,
) -> Result<(OutboxMutations, OutboxMutations)> {
    let mut active = BTreeMap::new();
    let mut delivery = BTreeMap::new();
    if additions.is_empty() != source.is_none() {
        return Err(invariant_violation(
            "bounded projection source presence differs from additions",
        ));
    }
    for intent in additions {
        let source =
            source.ok_or_else(|| invariant_violation("missing bounded projection source"))?;
        let payload = encode_json(intent, "bounded V2 projection intent")?;
        let outer = ActiveRecord8 {
            record_id: intent.intent_id().to_owned(),
            origin_sequence: intent.source_logical_sequence(),
            ordinal: intent.ordinal(),
            source_descriptor_sha256: source.sha256.clone(),
            payload: payload.to_vec(),
        };
        let active_row = ControlMvpSegmentRow {
            record_kind: super::SEGMENT_RECORD_OUTBOX,
            key: intent.intent_id().as_bytes().to_vec(),
            value: Some(encode_json(&outer, "bounded active V2 projection")?.to_vec()),
            generation: 0,
            tombstone: false,
            logical_sequence: sequence,
            logical_ordinal: intent.ordinal(),
            origin_sequence: Some(intent.source_logical_sequence()),
        };
        let delivery_key = delivery_key(
            intent.source_logical_sequence(),
            intent.ordinal(),
            intent.intent_id(),
        )?;
        let delivery_row = ControlMvpSegmentRow {
            record_kind: super::SEGMENT_RECORD_OUTBOX,
            key: delivery_key.clone(),
            value: Some(source.sha256.as_bytes().to_vec()),
            generation: 0,
            tombstone: false,
            logical_sequence: sequence,
            logical_ordinal: intent.ordinal(),
            origin_sequence: Some(intent.source_logical_sequence()),
        };
        if active
            .insert(active_row.key.clone(), Some(active_row))
            .is_some()
            || delivery.insert(delivery_key, Some(delivery_row)).is_some()
        {
            return Err(invariant_violation(
                "duplicate bounded V2 projection addition",
            ));
        }
    }
    for trim in trims {
        let delivery_key = delivery_key(trim.origin_sequence, trim.ordinal, &trim.record_id)?;
        if active
            .insert(trim.record_id.as_bytes().to_vec(), None)
            .is_some()
            || delivery.insert(delivery_key, None).is_some()
        {
            return Err(invariant_violation(
                "bounded V2 trim conflicts with an addition or trim",
            ));
        }
    }
    Ok((active, delivery))
}

impl Base {
    pub(super) fn with_transaction_selection(mut self) -> Self {
        self.selection = Some(super::lazy::BoundedSelection::default());
        self
    }

    fn selection(&self) -> super::lazy::BoundedSelection {
        self.selection.clone().unwrap_or_default()
    }

    fn is_absent(&self, directory: &directory::Directory) -> Result<bool> {
        if self.manifest.is_some()
            || self.manifest_digest.is_some()
            || self.pointer_version.is_some()
            || self.pointer_bytes.is_some()
            || self.logical_sequence != 0
            || self.writer_epoch != 0
            || self.reclamation_generation != 0
        {
            return Ok(false);
        }
        let empty = directory.empty_root_reference()?;
        Ok(self.kv_root == empty
            && self.active_id_root == empty
            && self.delivery_order_root == empty)
    }

    #[must_use]
    pub(super) const fn logical_sequence(&self) -> u64 {
        self.logical_sequence
    }

    #[must_use]
    pub(super) const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    #[must_use]
    pub(super) const fn reclamation_generation(&self) -> u64 {
        self.reclamation_generation
    }

    #[must_use]
    pub(super) fn pointer_version(&self) -> Option<&str> {
        self.pointer_version.as_deref()
    }

    pub(super) fn pointer_bytes(&self) -> Option<&[u8]> {
        self.pointer_bytes.as_deref()
    }

    pub(super) fn logical_history(&self) -> &str {
        self.manifest
            .as_ref()
            .map_or_else(|| "", |manifest| manifest.logical_history.as_str())
    }

    pub(super) fn token(&self, store: &ControlMvpStateStore) -> Option<StateToken> {
        self.manifest
            .as_ref()
            .zip(self.manifest_digest.as_ref())
            .map(|(manifest, digest)| {
                store
                    .token(manifest.manifest_id.clone(), manifest.logical_sequence)
                    .with_manifest_witness(digest.clone())
            })
    }

    pub(super) async fn get(
        &self,
        store: &ControlMvpStateStore,
        key: &[u8],
    ) -> Result<Option<StoredValue>> {
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        if self.is_absent(&directory)? {
            return Ok(None);
        }
        let mut budget = directory::ReadBudget::default();
        let selection = self.selection();
        let mut leaves = Vec::new();
        if let Some(leaf) = directory
            .floor_leaf(&self.kv_root, key, &mut budget)
            .await?
        {
            selection.select(&leaf)?;
            leaves.push(leaf);
        }
        let successor = directory
            .scan(&self.kv_root, key, None, 1, None, &mut budget)
            .await?;
        for leaf in successor.leaves {
            selection.select(&leaf)?;
            if !leaves.contains(&leaf) {
                leaves.push(leaf);
            }
        }
        let mut value = None;
        for leaf in leaves {
            for row in store
                .resolve_physical_block(physical::Role::Kv, &leaf)
                .await?
            {
                if row.key == key && value.replace(super::stored_row_value(row)).is_some() {
                    return Err(invariant_violation(
                        "bounded point has duplicate physical rows",
                    ));
                }
            }
        }
        Ok(value)
    }

    pub(super) async fn range(
        &self,
        store: &ControlMvpStateStore,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, StoredValue)>> {
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        if self.is_absent(&directory)? {
            return Ok(Vec::new());
        }
        let mut budget = directory::ReadBudget::default();
        let selection = self.selection();
        let page = directory
            .scan(
                &self.kv_root,
                lower,
                upper,
                MAX_SELECTED_BLOCKS,
                None,
                &mut budget,
            )
            .await?;
        if page.next.is_some() {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "bounded range read exceeds 16 physical blocks".into(),
            });
        }
        let mut leaves = page.leaves;
        for leaf in &leaves {
            selection.select(leaf)?;
        }
        if let Some(leaf) = directory
            .floor_leaf(&self.kv_root, lower, &mut budget)
            .await?
            && !leaves.contains(&leaf)
        {
            selection.select(&leaf)?;
            leaves.push(leaf);
        }
        if let Some(upper) = upper {
            let right = directory
                .scan(&self.kv_root, upper, None, 1, None, &mut budget)
                .await?;
            for leaf in right.leaves {
                selection.select(&leaf)?;
                if !leaves.contains(&leaf) {
                    leaves.push(leaf);
                }
            }
        }
        if leaves.len() > MAX_SELECTED_BLOCKS {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "bounded range read including boundary witnesses exceeds 16 blocks".into(),
            });
        }
        let mut values = Vec::new();
        for leaf in leaves {
            for row in store
                .resolve_physical_block(physical::Role::Kv, &leaf)
                .await?
            {
                if row.key.as_slice() >= lower && upper.is_none_or(|end| row.key.as_slice() < end) {
                    values.push((row.key.clone(), super::stored_row_value(row)));
                }
            }
        }
        if values
            .windows(2)
            .any(|pair| matches!(pair, [before, after] if before.0 >= after.0))
        {
            return Err(invariant_violation(
                "bounded range rows are not strictly ordered",
            ));
        }
        Ok(values)
    }

    pub(super) async fn active(
        &self,
        store: &ControlMvpStateStore,
        id: &str,
    ) -> Result<Option<super::ControlMvpProjectionOutboxRecord>> {
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        if self.is_absent(&directory)? {
            return Ok(None);
        }
        let mut budget = directory::ReadBudget::default();
        let selection = self.selection();
        let mut leaves = Vec::new();
        if let Some(leaf) = directory
            .floor_leaf(&self.active_id_root, id.as_bytes(), &mut budget)
            .await?
        {
            selection.select(&leaf)?;
            leaves.push(leaf);
        }
        let successor = directory
            .scan(
                &self.active_id_root,
                id.as_bytes(),
                None,
                1,
                None,
                &mut budget,
            )
            .await?;
        for leaf in successor.leaves {
            selection.select(&leaf)?;
            if !leaves.contains(&leaf) {
                leaves.push(leaf);
            }
        }
        let mut record = None;
        for leaf in leaves {
            for row in store
                .resolve_physical_block(physical::Role::ActiveId, &leaf)
                .await?
            {
                if row.key != id.as_bytes() {
                    continue;
                }
                let bytes = row
                    .value
                    .ok_or_else(|| invariant_violation("active-ID row has no payload"))?;
                let active: ActiveRecord8 = decode_json(&bytes, "active-ID V2 record")?;
                let origin = row
                    .origin_sequence
                    .ok_or_else(|| invariant_violation("active-ID row has no origin sequence"))?;
                if active.record_id != id
                    || active.origin_sequence != origin
                    || active.ordinal != row.logical_ordinal
                {
                    return Err(invariant_violation(
                        "active-ID row identity differs from payload",
                    ));
                }
                if record
                    .replace(
                        super::ControlMvpProjectionOutboxRecord::with_origin_sequence(
                            id,
                            Bytes::from(active.payload),
                            origin,
                        ),
                    )
                    .is_some()
                {
                    return Err(invariant_violation(
                        "active-ID point has duplicate physical rows",
                    ));
                }
            }
        }
        Ok(record)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one delivery page verifies both ordered outbox indexes"
    )]
    async fn delivery_page(
        &self,
        store: &ControlMvpStateStore,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<DeliveryPage8> {
        if limit == 0 {
            return Err(invariant_violation(
                "bounded delivery page limit must be positive",
            ));
        }
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        if self.is_absent(&directory)? {
            return Ok(DeliveryPage8 {
                records: Vec::new(),
                next_after: None,
            });
        }
        let mut budget = directory::ReadBudget::default();
        let selection = super::lazy::BoundedSelection::default();
        let mut active_blocks: BTreeMap<[u8; 32], Vec<ControlMvpSegmentRow>> = BTreeMap::new();
        let mut records = Vec::new();
        let mut cursor = None;
        loop {
            let page = directory
                .scan(
                    &self.delivery_order_root,
                    after.unwrap_or_default(),
                    None,
                    1,
                    cursor.as_ref(),
                    &mut budget,
                )
                .await?;
            let has_next_leaf = page.next.is_some();
            let Some(leaf) = page.leaves.into_iter().next() else {
                return Ok(DeliveryPage8 {
                    records,
                    next_after: None,
                });
            };
            cursor = page.next;
            selection.select(&leaf)?;
            let delivery_rows = store
                .resolve_physical_block(physical::Role::DeliveryOrder, &leaf)
                .await?;
            for (index, row) in delivery_rows.iter().enumerate() {
                if after.is_some_and(|after| row.key.as_slice() <= after) {
                    continue;
                }
                let (origin, ordinal, record_id) = parse_delivery_key(&row.key)?;
                let digest = std::str::from_utf8(row.value.as_deref().ok_or_else(|| {
                    invariant_violation("bounded delivery row has no source digest")
                })?)
                .map_err(|_| invariant_violation("bounded delivery source digest is not UTF-8"))?;
                let active_leaf = directory
                    .lookup(&self.active_id_root, record_id.as_bytes(), &mut budget)
                    .await?
                    .ok_or_else(|| {
                        invariant_violation("bounded delivery row has no active counterpart")
                    })?;
                selection.select(&active_leaf)?;
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    active_blocks.entry(active_leaf.digest)
                {
                    entry.insert(
                        store
                            .resolve_physical_block(physical::Role::ActiveId, &active_leaf)
                            .await?,
                    );
                }
                let active_row = active_blocks
                    .get(&active_leaf.digest)
                    .ok_or_else(|| invariant_violation("bounded active block cache is absent"))?
                    .iter()
                    .find(|active| active.key == record_id.as_bytes())
                    .ok_or_else(|| invariant_violation("bounded active counterpart is absent"))?;
                let active: ActiveRecord8 = decode_json(
                    active_row.value.as_deref().ok_or_else(|| {
                        invariant_violation("bounded active counterpart has no payload")
                    })?,
                    "bounded active delivery counterpart",
                )?;
                if active.source_descriptor_sha256 != digest
                    || active.origin_sequence != origin
                    || active.ordinal != ordinal
                    || active_row.origin_sequence != Some(origin)
                    || active_row.logical_ordinal != ordinal
                {
                    return Err(invariant_violation(
                        "bounded active and delivery rows disagree",
                    ));
                }
                let intent: logical_v2::ProjectionIntentV2 =
                    decode_json(&active.payload, "bounded delivered V2 intent")?;
                let source = load_projection_source(store, digest).await?;
                if intent.intent_id() != record_id
                    || intent.source_scope() != &store.scope
                    || intent.source_logical_sequence() != origin
                    || intent.ordinal() != ordinal
                    || source.logical_sequence != origin
                    || source.logical_commit_id != intent.logical_commit_id()
                {
                    return Err(invariant_violation(
                        "bounded delivery source differs from V2 intent",
                    ));
                }
                records.push(
                    super::ControlMvpProjectionOutboxRecord::with_origin_sequence(
                        record_id,
                        Bytes::from(active.payload),
                        origin,
                    ),
                );
                if records.len() == limit {
                    let more_in_leaf =
                        delivery_rows
                            .get(index.saturating_add(1)..)
                            .is_some_and(|tail| {
                                tail.iter().any(|next| {
                                    after.is_none_or(|after| next.key.as_slice() > after)
                                })
                            });
                    return Ok(DeliveryPage8 {
                        records,
                        next_after: (more_in_leaf || has_next_leaf).then(|| row.key.clone()),
                    });
                }
            }
            if !has_next_leaf {
                return Ok(DeliveryPage8 {
                    records,
                    next_after: None,
                });
            }
        }
    }

    async fn validate_live_intent(
        &self,
        store: &ControlMvpStateStore,
        intent: &logical_v2::ProjectionIntentV2,
    ) -> Result<()> {
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        if self.is_absent(&directory)? {
            return Err(precondition_failed(
                "cannot trim an inactive V2 projection intent",
            ));
        }
        let mut budget = directory::ReadBudget::default();
        let active_leaf = directory
            .lookup(
                &self.active_id_root,
                intent.intent_id().as_bytes(),
                &mut budget,
            )
            .await?
            .ok_or_else(|| precondition_failed("cannot trim an inactive V2 projection intent"))?;
        let active_row = store
            .resolve_physical_block(physical::Role::ActiveId, &active_leaf)
            .await?
            .into_iter()
            .find(|row| row.key == intent.intent_id().as_bytes())
            .ok_or_else(|| precondition_failed("active V2 projection row is absent"))?;
        let active_bytes = active_row
            .value
            .as_deref()
            .ok_or_else(|| invariant_violation("active V2 projection row has no payload"))?;
        let active: ActiveRecord8 = decode_json(active_bytes, "active V2 projection record")?;
        let intent_bytes = encode_json(intent, "V2 projection intent")?;
        if active.payload.as_slice() != intent_bytes.as_ref()
            || active.record_id != intent.intent_id()
            || active.origin_sequence != intent.source_logical_sequence()
            || active.ordinal != intent.ordinal()
            || active_row.origin_sequence != Some(intent.source_logical_sequence())
            || active_row.logical_ordinal != intent.ordinal()
        {
            return Err(precondition_failed(
                "active V2 projection incarnation differs from trim target",
            ));
        }
        let source = load_projection_source(store, &active.source_descriptor_sha256).await?;
        if source.logical_sequence != intent.source_logical_sequence()
            || source.logical_commit_id != intent.logical_commit_id()
        {
            return Err(invariant_violation(
                "projection source differs from active V2 intent",
            ));
        }
        let delivery_key = delivery_key(
            intent.source_logical_sequence(),
            intent.ordinal(),
            intent.intent_id(),
        )?;
        let delivery_leaf = directory
            .lookup(&self.delivery_order_root, &delivery_key, &mut budget)
            .await?
            .ok_or_else(|| precondition_failed("delivery V2 projection row is absent"))?;
        let delivery = store
            .resolve_physical_block(physical::Role::DeliveryOrder, &delivery_leaf)
            .await?
            .into_iter()
            .find(|row| row.key == delivery_key)
            .ok_or_else(|| precondition_failed("delivery V2 projection row is absent"))?;
        if delivery.value.as_deref() != Some(active.source_descriptor_sha256.as_bytes())
            || delivery.origin_sequence != Some(intent.source_logical_sequence())
            || delivery.logical_ordinal != intent.ordinal()
        {
            return Err(invariant_violation(
                "delivery V2 projection row differs from active source",
            ));
        }
        Ok(())
    }
}

fn delivery_key(sequence: u64, ordinal: u64, record_id: &str) -> Result<Vec<u8>> {
    if record_id.is_empty() || record_id.contains('/') {
        return Err(invariant_violation("invalid V2 delivery record ID"));
    }
    Ok(format!("{sequence:020}/{ordinal:020}/{record_id}").into_bytes())
}

fn parse_delivery_key(key: &[u8]) -> Result<(u64, u64, &str)> {
    let key = std::str::from_utf8(key)
        .map_err(|_| invariant_violation("bounded delivery key is not UTF-8"))?;
    let mut parts = key.splitn(3, '/');
    let sequence = parts
        .next()
        .filter(|part| part.len() == 20 && part.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|part| part.parse::<u64>().ok())
        .ok_or_else(|| invariant_violation("bounded delivery sequence is invalid"))?;
    let ordinal = parts
        .next()
        .filter(|part| part.len() == 20 && part.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|part| part.parse::<u64>().ok())
        .ok_or_else(|| invariant_violation("bounded delivery ordinal is invalid"))?;
    let record_id = parts
        .next()
        .filter(|part| !part.is_empty() && !part.contains('/'))
        .ok_or_else(|| invariant_violation("bounded delivery record ID is invalid"))?;
    Ok((sequence, ordinal, record_id))
}

async fn load_projection_source(
    store: &ControlMvpStateStore,
    digest: &str,
) -> Result<ProjectionSource8> {
    if !valid_raw_digest(digest) {
        return Err(invariant_violation("projection source digest is invalid"));
    }
    let path = format!(
        "{}/projection-sources/{digest}.json",
        store.paths.base_prefix()
    );
    let bytes = store.get_json(&path, super::MAX_CONTROL_JSON_BYTES).await?;
    validate_raw_checksum(&bytes, Some(digest), "bounded projection source checksum")?;
    let source: ProjectionSource8 = decode_json(&bytes, "bounded projection source")?;
    if source.encoding_version != 1
        || source.scope != store.scope
        || source.logical_sequence == 0
        || !valid_raw_digest(&source.logical_commit_id)
        || hex::decode(&source.kv_root_hex).is_err()
    {
        return Err(invariant_violation("invalid bounded projection source"));
    }
    Ok(source)
}

impl ControlMvpTxn {
    /// Stages exact-incarnation V2 projection removals after authenticating both indexes.
    ///
    /// # Errors
    /// Returns an error for non-V2 bases, malformed intents, duplicate trims, or inactive records.
    pub async fn trim_projection_intents_v2(
        &mut self,
        intents: &[logical_v2::ProjectionIntentV2],
    ) -> Result<()> {
        if self.store.authority_format != AUTHORITY_FORMAT {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "V2 projection trims require explicit authority format 8".into(),
            });
        }
        let TransactionBase::Bounded(base) = &self.base else {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "V2 projection trims require a bounded transaction base".into(),
            });
        };
        let mut staged = Vec::with_capacity(intents.len());
        let mut seen = BTreeSet::new();
        let mut reserved = 0_usize;
        for intent in intents {
            if intent.source_scope() != &self.store.scope
                || !seen.insert((
                    intent.intent_id(),
                    intent.source_logical_sequence(),
                    intent.ordinal(),
                ))
                || self
                    .projection_intents
                    .iter()
                    .any(|staged| staged.intent_id == intent.intent_id())
                || self
                    .bounded_trims
                    .iter()
                    .any(|trim| trim.record_id == intent.intent_id())
            {
                return Err(precondition_failed(
                    "V2 projection trim is duplicate or conflicts with a staged intent",
                ));
            }
            base.validate_live_intent(&self.store, intent).await?;
            reserved = reserved
                .checked_add(intent.intent_id().len() + 96)
                .ok_or_else(|| CatalogError::MaintenanceBackpressure {
                    message: "V2 projection trim accounting overflow".into(),
                })?;
            staged.push(logical_v2::Trim {
                record_id: intent.intent_id().to_owned(),
                origin_sequence: intent.source_logical_sequence(),
                ordinal: intent.ordinal(),
            });
        }
        self.reads.reserve(reserved, staged.len())?;
        self.bounded_trims.extend(staged);
        Ok(())
    }
}

/// Validates one V2 outbox row after the descriptor, index, and Arrow bytes are authenticated.
pub(super) fn validate_outbox_row(
    role: physical::Role,
    row: &ControlMvpSegmentRow,
    scope: &StateScope,
) -> Result<()> {
    if row.record_kind != super::SEGMENT_RECORD_OUTBOX
        || row.tombstone
        || row.generation != 0
        || row
            .origin_sequence
            .is_none_or(|origin| origin == 0 || origin > row.logical_sequence)
    {
        return Err(invariant_violation("invalid bounded outbox row metadata"));
    }
    match role {
        physical::Role::Kv => Err(invariant_violation("KV row sent to outbox decoder")),
        physical::Role::ActiveId => {
            let record_id = std::str::from_utf8(&row.key)
                .map_err(|_| invariant_violation("active-ID key is not UTF-8"))?;
            let bytes = row
                .value
                .as_deref()
                .ok_or_else(|| invariant_violation("active-ID row has no V2 payload"))?;
            let active: ActiveRecord8 = decode_json(bytes, "active-ID V2 record")?;
            if encode_json(&active, "active-ID V2 record")?.as_ref() != bytes {
                return Err(invariant_violation("active-ID row is not canonical JSON"));
            }
            if !valid_raw_digest(&active.source_descriptor_sha256) {
                return Err(invariant_violation(
                    "active-ID source descriptor digest is invalid",
                ));
            }
            let intent: logical_v2::ProjectionIntentV2 =
                decode_json(&active.payload, "active-ID V2 projection payload")?;
            if encode_json(&intent, "active-ID V2 projection payload")?.as_ref()
                != active.payload.as_slice()
            {
                return Err(invariant_violation(
                    "active-ID projection payload is not canonical JSON",
                ));
            }
            if intent.intent_id() != record_id
                || intent.source_scope() != scope
                || intent.source_logical_sequence() != row.origin_sequence.unwrap_or_default()
                || intent.ordinal() != row.logical_ordinal
                || active.record_id != record_id
                || active.origin_sequence != row.origin_sequence.unwrap_or_default()
                || active.ordinal != row.logical_ordinal
            {
                return Err(invariant_violation(
                    "active-ID row differs from its V2 projection payload",
                ));
            }
            Ok(())
        }
        physical::Role::DeliveryOrder => {
            let value = row
                .value
                .as_deref()
                .ok_or_else(|| invariant_violation("delivery-order row has no source digest"))?;
            let digest = std::str::from_utf8(value)
                .map_err(|_| invariant_violation("delivery-order source digest is not UTF-8"))?;
            if !valid_raw_digest(digest) {
                return Err(invariant_violation("invalid delivery-order source digest"));
            }
            let key = std::str::from_utf8(&row.key)
                .map_err(|_| invariant_violation("delivery-order key is not UTF-8"))?;
            let mut parts = key.splitn(3, '/');
            let Some(sequence) = parts.next() else {
                return Err(invariant_violation("delivery-order key is incomplete"));
            };
            let Some(ordinal) = parts.next() else {
                return Err(invariant_violation("delivery-order key is incomplete"));
            };
            let Some(record_id) = parts.next() else {
                return Err(invariant_violation("delivery-order key is incomplete"));
            };
            if record_id.is_empty()
                || sequence.len() != 20
                || ordinal.len() != 20
                || !sequence.bytes().all(|byte| byte.is_ascii_digit())
                || !ordinal.bytes().all(|byte| byte.is_ascii_digit())
                || sequence.parse::<u64>().ok() != row.origin_sequence
                || ordinal.parse::<u64>().ok() != Some(row.logical_ordinal)
            {
                return Err(invariant_violation("invalid delivery-order key"));
            }
            Ok(())
        }
    }
}

impl ControlMvpStateStore {
    /// Returns one authenticated, retained authority-8 delivery page.
    ///
    /// # Errors
    /// Returns an error for an invalid authority, continuation, page bound, or authenticated outbox proof.
    pub async fn projection_outbox_page_v2(
        &self,
        token: Option<StateToken>,
        continuation: Option<ProjectionContinuationV2>,
        max_records: usize,
    ) -> Result<ProjectionPageV2> {
        if self.authority_format != AUTHORITY_FORMAT {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "V2 delivery pages require explicit authority format 8".into(),
            });
        }
        if continuation
            .as_ref()
            .zip(token.as_ref())
            .is_some_and(|(cursor, supplied)| cursor.token != *supplied)
        {
            return Err(invariant_violation(
                "V2 delivery continuation token differs from supplied token",
            ));
        }
        let (retained, base) = if let Some(cursor) = continuation.as_ref() {
            let token = cursor.token.clone();
            let base = self.read_bounded_token(&token).await?;
            (token, base)
        } else if let Some(token) = token {
            let base = self.read_bounded_token(&token).await?;
            (token, base)
        } else {
            let base = self.pin_bounded_base().await?;
            let token = base.token(self).ok_or_else(|| {
                invariant_violation("authority-8 delivery page has no published token")
            })?;
            (token, base)
        };
        let page = base
            .delivery_page(
                self,
                continuation.as_ref().map(|cursor| cursor.after.as_slice()),
                max_records,
            )
            .await?;
        let records = page
            .records
            .iter()
            .map(|record| decode_json(record.payload(), "authority-8 delivered V2 intent"))
            .collect::<Result<Vec<_>>>()?;
        let continuation = page.next_after.map(|after| ProjectionContinuationV2 {
            token: retained.clone(),
            after,
        });
        Ok(ProjectionPageV2 {
            token: retained,
            records,
            continuation,
        })
    }

    /// Reconciles a persisted authority-8 candidate without retrying its HEAD CAS.
    /// Direct CAS precondition failures remain [`CatalogError::CasFailed`]; this restart-safe API
    /// returns only outcomes established by retained authenticated evidence.
    ///
    /// # Errors
    /// Returns an error only when the recovery request itself is invalid; incomplete evidence is unresolved.
    pub async fn reconcile_candidate_v2(&self, candidate_id: &str) -> Result<CandidateRecoveryV2> {
        self.reconcile_candidate_v2_with_budget(candidate_id, RECOVERY_MAX_BYTES)
            .await
    }

    pub(super) async fn reconcile_candidate_v2_with_budget(
        &self,
        candidate_id: &str,
        max_bytes: usize,
    ) -> Result<CandidateRecoveryV2> {
        let mut limited = self.clone();
        limited.bounded_recovery_bytes =
            Some(std::sync::Arc::new(std::sync::Mutex::new(max_bytes)));
        limited.reconcile_candidate_v2_inner(candidate_id).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "recovery must evaluate one bounded authenticated ancestry walk"
    )]
    async fn reconcile_candidate_v2_inner(
        &self,
        candidate_id: &str,
    ) -> Result<CandidateRecoveryV2> {
        if self.authority_format != AUTHORITY_FORMAT {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "candidate recovery requires explicit authority format 8".into(),
            });
        }
        if !super::integrity::valid_immutable_id(candidate_id) {
            return Err(invariant_violation("invalid bounded candidate identifier"));
        }
        let path = format!("{}/prepared/{candidate_id}.json", self.paths.base_prefix());
        let prepared = self
            .get_json(&path, super::MAX_CONTROL_JSON_BYTES)
            .await
            .ok()
            .and_then(|bytes| decode_json(&bytes, "bounded prepared candidate").ok());
        let Some(prepared) = prepared else {
            return Ok(CandidateRecoveryV2::Unresolved);
        };
        let Ok((candidate, _)) = validate_prepared_candidate(self, &prepared, candidate_id).await
        else {
            return Ok(CandidateRecoveryV2::Unresolved);
        };
        let candidate_token = self
            .token(candidate_id.to_owned(), candidate.logical_sequence)
            .with_manifest_witness(prepared.candidate_manifest.sha256.clone());
        let Ok(base) = self.pin_bounded_base().await else {
            return Ok(CandidateRecoveryV2::Unresolved);
        };
        let Some(current_bytes) = base.pointer_bytes() else {
            return Ok(CandidateRecoveryV2::Unresolved);
        };
        if current_bytes == prepared.candidate_head.raw_bytes {
            return Ok(CandidateRecoveryV2::Committed(candidate_token));
        }
        if prepared
            .original_head
            .as_ref()
            .is_some_and(|original| current_bytes == original.raw_bytes)
        {
            return Ok(CandidateRecoveryV2::Unresolved);
        }
        let Some(mut manifest) = base.manifest.clone() else {
            return Ok(CandidateRecoveryV2::Unresolved);
        };
        let Some(mut digest) = base.manifest_digest.clone() else {
            return Ok(CandidateRecoveryV2::Unresolved);
        };
        let current_token = base
            .token(self)
            .ok_or_else(|| invariant_violation("published bounded base has no token"))?;
        let mut conflicting_successor = false;
        for _ in 0..RECOVERY_MAX_MANIFESTS {
            if manifest.manifest_id == candidate.manifest_id
                && digest == prepared.candidate_manifest.sha256
            {
                return Ok(CandidateRecoveryV2::Committed(candidate_token));
            }
            if manifest.logical_sequence == candidate.logical_sequence {
                conflicting_successor = true;
            }
            match (
                &prepared.original_head,
                &manifest.parent_manifest_id,
                &manifest.parent_manifest_sha256,
            ) {
                (Some(original), _, _)
                    if manifest.manifest_id
                        == original
                            .manifest
                            .path
                            .rsplit('/')
                            .next()
                            .and_then(|name| name.strip_suffix(".json"))
                            .unwrap_or_default()
                        && digest == original.manifest.sha256 =>
                {
                    return Ok(if conflicting_successor {
                        CandidateRecoveryV2::Superseded(current_token)
                    } else {
                        CandidateRecoveryV2::Unresolved
                    });
                }
                (None, None, None) => {
                    return Ok(if conflicting_successor {
                        CandidateRecoveryV2::Superseded(current_token)
                    } else {
                        CandidateRecoveryV2::Unresolved
                    });
                }
                (_, Some(parent_id), Some(parent_digest)) => {
                    let Ok(parent) = recovery_manifest(self, parent_id, parent_digest).await else {
                        return Ok(CandidateRecoveryV2::Unresolved);
                    };
                    if manifest.logical_sequence
                        != parent.logical_sequence.checked_add(1).unwrap_or_default()
                    {
                        return Ok(CandidateRecoveryV2::Unresolved);
                    }
                    let parent_digest = parent_digest.clone();
                    manifest = parent;
                    digest = parent_digest;
                }
                _ => return Ok(CandidateRecoveryV2::Unresolved),
            }
        }
        Ok(CandidateRecoveryV2::Unresolved)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "candidate construction binds all immutable artifacts before one CAS"
    )]
    pub(super) async fn commit_bounded(
        &self,
        txn: ControlMvpTxn,
    ) -> Result<(StateToken, Vec<logical_v2::ProjectionIntentV2>, String)> {
        if self.authority_format != AUTHORITY_FORMAT {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "bounded commit requires explicit authority format 8".into(),
            });
        }
        txn.validate_bounded_preconditions().await?;
        if !txn.outbox.is_empty() || !txn.outbox_trim.is_empty() {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "authority-8 rejects V1 projection records".into(),
            });
        }
        let TransactionBase::Bounded(base) = &txn.base else {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "bounded commit received a format-7 transaction base".into(),
            });
        };
        base.selection().check()?;
        if base.logical_sequence() == 0 {
            // The pure absent-HEAD root becomes durable only while preparing a commit.
            directory::Directory::new(self.retention.clone(), &self.scope)?
                .empty_root()
                .await?;
        }
        let sequence = next_logical_sequence(base.logical_sequence(), "bounded commit")?;
        let operation = txn.logical_operation.as_ref().ok_or_else(|| {
            invariant_violation("bounded commit has no frozen V2 logical operation")
        })?;
        let prior_history = if base.logical_sequence() == 0 {
            logical_v2::genesis(&self.scope)
        } else {
            base.logical_history().to_owned()
        };
        let logical_commit_id =
            logical_v2::commit_id(&self.scope, &prior_history, sequence, operation)?;
        let additions = staged_v2_intents(&txn, sequence, &logical_commit_id)?;
        let trims = txn.bounded_trims.clone();
        let writes = txn
            .writes
            .iter()
            .map(|(key, write)| match write {
                StagedWrite::Put(value) => ControlMvpWriteEntry {
                    key: key.clone(),
                    generation: sequence,
                    value: Some(value.to_vec()),
                },
                StagedWrite::Delete => ControlMvpWriteEntry {
                    key: key.clone(),
                    generation: sequence,
                    value: None,
                },
            })
            .collect::<Vec<_>>();
        let logical_history = logical_v2::history(
            &self.scope,
            &prior_history,
            sequence,
            &logical_commit_id,
            &writes,
            &additions,
            &trims,
        )?;
        let transaction = Transaction8 {
            format_version: AUTHORITY_FORMAT,
            scope: self.scope.clone(),
            transaction_id: txn.tx_id.clone(),
            logical_sequence: sequence,
            operation: operation.clone(),
            logical_commit_id: logical_commit_id.clone(),
            logical_history: logical_history.clone(),
            writes: writes
                .iter()
                .map(|write| BoundedWrite8 {
                    key: write.key.clone(),
                    generation: write.generation,
                    delete: write.value.is_none(),
                    value_sha256: write.value.as_ref().map(|value| sha256_hex(value)),
                })
                .collect(),
            additions: additions.clone(),
            trims: trims.clone(),
        };
        let directory = directory::Directory::new(self.retention.clone(), &self.scope)?;
        let mut work = BoundedCommitWork::default();
        let (kv_root, kv_edits) = rewrite_kv(
            self,
            &directory,
            base,
            &txn.manifest_id,
            sequence,
            &writes,
            &mut work,
        )
        .await?;
        let projection_source = if additions.is_empty() {
            None
        } else {
            Some(write_projection_source(self, sequence, &logical_commit_id, &kv_root).await?)
        };
        let (active_mutations, delivery_mutations) =
            outbox_mutations(&additions, &trims, sequence, projection_source.as_ref())?;
        let selection = base.selection();
        let (active_id_root, active_edits) = rewrite_outbox_role(
            self,
            &directory,
            &base.active_id_root,
            physical::Role::ActiveId,
            &txn.manifest_id,
            sequence,
            &active_mutations,
            &selection,
            &mut work,
        )
        .await?;
        let (delivery_order_root, delivery_edits) = rewrite_outbox_role(
            self,
            &directory,
            &base.delivery_order_root,
            physical::Role::DeliveryOrder,
            &txn.manifest_id,
            sequence,
            &delivery_mutations,
            &selection,
            &mut work,
        )
        .await?;
        let transition = Transition8 {
            format_version: AUTHORITY_FORMAT,
            scope: self.scope.clone(),
            transaction_id: txn.tx_id.clone(),
            roles: vec![
                role_transition(
                    physical::Role::Kv,
                    &base.kv_root,
                    &kv_root,
                    kv_edits
                        .iter()
                        .map(|edit| EditProof8 {
                            old: edit.old.as_ref().map(leaf_proof),
                            new: edit.new.iter().map(leaf_proof).collect(),
                        })
                        .collect(),
                ),
                role_transition(
                    physical::Role::ActiveId,
                    &base.active_id_root,
                    &active_id_root,
                    active_edits
                        .iter()
                        .map(|edit| EditProof8 {
                            old: edit.old.as_ref().map(leaf_proof),
                            new: edit.new.iter().map(leaf_proof).collect(),
                        })
                        .collect(),
                ),
                role_transition(
                    physical::Role::DeliveryOrder,
                    &base.delivery_order_root,
                    &delivery_order_root,
                    delivery_edits
                        .iter()
                        .map(|edit| EditProof8 {
                            old: edit.old.as_ref().map(leaf_proof),
                            new: edit.new.iter().map(leaf_proof).collect(),
                        })
                        .collect(),
                ),
            ],
        };
        verify_candidate_transition(
            self,
            base,
            &kv_root,
            &active_id_root,
            &delivery_order_root,
            &transaction,
            &transition,
            &mut work,
        )
        .await?;
        let transition_path = format!(
            "{}/transitions/{}.json",
            self.paths.base_prefix(),
            txn.manifest_id
        );
        let transition_bytes = encode_json_limited(
            &transition,
            super::MAX_CONTROL_JSON_BYTES,
            "bounded transition",
        )?;
        let transition_ref = ArtifactRef {
            path: transition_path.clone(),
            sha256: sha256_hex(&transition_bytes),
        };
        put_immutable_matching(
            &self.storage,
            &transition_path,
            transition_bytes,
            "bounded transition already exists with different bytes",
        )
        .await?;
        let transaction_path = self.paths.tx_object(&txn.tx_id);
        let transaction_bytes = encode_json_limited(
            &transaction,
            super::MAX_TRANSACTION_JSON_BYTES,
            "bounded transaction",
        )?;
        let transaction_ref = ArtifactRef {
            path: transaction_path.clone(),
            sha256: sha256_hex(&transaction_bytes),
        };
        put_immutable_matching(
            &self.storage,
            &transaction_path,
            transaction_bytes,
            "bounded transaction already exists with different bytes",
        )
        .await?;
        let root = |role, root: &directory::Root| RootBinding {
            role,
            leaf_encoding: LEAF_ENCODING.to_string(),
            directory_root_hex: hex::encode(root.encode()),
        };
        let manifest = Manifest8 {
            format_version: AUTHORITY_FORMAT,
            implementation: super::IMPLEMENTATION.to_string(),
            scope: self.scope.clone(),
            manifest_id: txn.manifest_id.clone(),
            logical_sequence: sequence,
            logical_history,
            kind: ManifestKind8::Transaction,
            parent_manifest_id: base
                .manifest
                .as_ref()
                .map(|manifest| manifest.manifest_id.clone()),
            parent_manifest_sha256: base.manifest_digest.clone(),
            writer_epoch: base.writer_epoch(),
            reclamation_generation: base.reclamation_generation(),
            kv_root: root(physical::Role::Kv, &kv_root),
            active_id_root: root(physical::Role::ActiveId, &active_id_root),
            delivery_order_root: root(physical::Role::DeliveryOrder, &delivery_order_root),
            transaction: Some(transaction_ref),
            transition: Some(transition_ref),
            genesis_witness: None,
            projection_source,
        };
        let token = publish_manifest_candidate(self, base, manifest).await?;
        Ok((token, additions, logical_commit_id))
    }

    pub(super) async fn read_bounded_token(&self, token: &StateToken) -> Result<Base> {
        if token.scope() != &self.scope {
            return Err(invariant_violation(
                "bounded retained token scope does not match control MVP store",
            ));
        }
        let manifest_bytes = self
            .get_json(
                &self.paths.manifest_object(token.authority_manifest_id()),
                super::MAX_CONTROL_JSON_BYTES,
            )
            .await?;
        let digest = token.manifest_witness()?.to_string();
        validate_raw_checksum(
            &manifest_bytes,
            Some(&digest),
            "bounded retained manifest checksum",
        )?;
        let manifest: Manifest8 = decode_json(&manifest_bytes, "bounded retained manifest")?;
        manifest.validate(&self.scope, token.authority_manifest_id())?;
        if manifest.logical_sequence != token.logical_sequence() {
            return Err(invariant_violation(
                "bounded retained token sequence differs from manifest",
            ));
        }
        let directory = directory::Directory::new(self.retention.clone(), &self.scope)?;
        let roots = decode_roots(&directory, &manifest)?;
        validate_manifest_artifacts(self, &manifest, (&roots.0, &roots.1, &roots.2)).await?;
        let writer_epoch = manifest.writer_epoch;
        let reclamation_generation = manifest.reclamation_generation;
        base_from_manifest(
            &directory,
            manifest,
            digest,
            None,
            None,
            writer_epoch,
            reclamation_generation,
        )
    }

    /// Exports the verified logical tuple committed by an authority-8 token.
    ///
    /// # Errors
    /// Returns an error when the retained token, immutable artifacts, or transition proof is invalid.
    #[cfg(feature = "test-utils")]
    #[allow(
        clippy::too_many_lines,
        reason = "test export reconstructs and verifies the complete published tuple"
    )]
    pub async fn export_published_logical_v2(
        &self,
        token: &StateToken,
    ) -> Result<serde_json::Value> {
        let candidate = self.read_bounded_token(token).await?;
        let manifest = candidate
            .manifest
            .as_ref()
            .ok_or_else(|| invariant_violation("published logical export has no manifest"))?;
        if manifest.kind != ManifestKind8::Transaction {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "published logical export requires an authority-8 transaction manifest"
                    .into(),
            });
        }
        let (parent_id, parent_digest) = match (
            manifest.parent_manifest_id.as_deref(),
            manifest.parent_manifest_sha256.as_deref(),
        ) {
            (None, None) => (None, None),
            (Some(id), Some(digest)) => (Some(id), Some(digest)),
            _ => {
                return Err(invariant_violation(
                    "published logical export has an incomplete parent",
                ));
            }
        };
        let parent = if let (Some(parent_id), Some(parent_digest)) = (parent_id, parent_digest) {
            let sequence = manifest.logical_sequence.checked_sub(1).ok_or_else(|| {
                invariant_violation("published logical export parent sequence underflows")
            })?;
            self.read_bounded_token(
                &self
                    .token(parent_id.to_string(), sequence)
                    .with_manifest_witness(parent_digest.to_string()),
            )
            .await?
        } else {
            let directory = directory::Directory::new(self.retention.clone(), &self.scope)?;
            let empty = directory.empty_root_reference()?;
            Base {
                manifest: None,
                manifest_digest: None,
                pointer_version: None,
                pointer_bytes: None,
                writer_epoch: 0,
                reclamation_generation: 0,
                logical_sequence: 0,
                selection: None,
                kv_root: empty.clone(),
                active_id_root: empty.clone(),
                delivery_order_root: empty,
            }
        };
        let transaction_ref = manifest
            .transaction
            .as_ref()
            .ok_or_else(|| invariant_violation("published logical export lacks a transaction"))?;
        let transaction_bytes = self
            .get_json(&transaction_ref.path, super::MAX_TRANSACTION_JSON_BYTES)
            .await?;
        validate_raw_checksum(
            &transaction_bytes,
            Some(&transaction_ref.sha256),
            "published logical transaction checksum",
        )?;
        let transaction: Transaction8 =
            decode_json(&transaction_bytes, "published logical transaction")?;
        let transition_ref = manifest
            .transition
            .as_ref()
            .ok_or_else(|| invariant_violation("published logical export lacks a transition"))?;
        let transition_bytes = self
            .get_json(&transition_ref.path, super::MAX_CONTROL_JSON_BYTES)
            .await?;
        validate_raw_checksum(
            &transition_bytes,
            Some(&transition_ref.sha256),
            "published logical transition checksum",
        )?;
        let transition: Transition8 =
            decode_json(&transition_bytes, "published logical transition")?;
        let mut work = BoundedCommitWork::default();
        let writes = verify_candidate_transition(
            self,
            &parent,
            &candidate.kv_root,
            &candidate.active_id_root,
            &candidate.delivery_order_root,
            &transaction,
            &transition,
            &mut work,
        )
        .await?;
        let prior_history = if parent.logical_sequence() == 0 {
            logical_v2::genesis(&self.scope)
        } else {
            parent.logical_history().to_string()
        };
        Ok(serde_json::json!({
            "scope": {
                "tenantId": transaction.scope.tenant_id(),
                "workspaceId": transaction.scope.workspace_id(),
                "domain": transaction.scope.domain(),
            },
            "priorHistory": prior_history,
            "nextSeq": transaction.logical_sequence,
            "operation": {
                "operationId": transaction.operation.operation_id,
                "family": transaction.operation.family,
                "requestDigest": transaction.operation.request_digest,
            },
            "writes": writes.into_iter().map(|write| serde_json::json!({
                "rawKeyHex": hex::encode(write.key),
                "generation": write.generation,
                "rawValueHex": write.value.map(hex::encode),
            })).collect::<Vec<_>>(),
            "additionsV2": transaction.additions,
            "trims": transaction.trims.into_iter().map(|trim| serde_json::json!({
                "recordId": trim.record_id,
                "originSequence": trim.origin_sequence,
                "ordinal": trim.ordinal,
            })).collect::<Vec<_>>(),
            "logicalCommitId": transaction.logical_commit_id,
            "logicalHistory": transaction.logical_history,
            "actualRootDepths": {
                "kv": candidate.kv_root.depth(),
                "activeId": candidate.active_id_root.depth(),
                "deliveryOrder": candidate.delivery_order_root.depth(),
            },
        }))
    }

    /// Exports authenticated current-root counts for separately measured fixture inventory.
    ///
    /// # Errors
    /// Returns an error if the current authority-8 root cannot be authenticated.
    #[cfg(feature = "test-utils")]
    pub async fn bounded_root_inventory_v2(&self) -> Result<serde_json::Value> {
        let base = self.pin_bounded_base().await?;
        let token = base
            .token(self)
            .ok_or_else(|| invariant_violation("inventory requires a published root"))?;
        Ok(serde_json::json!({
            "manifestId": token.authority_manifest_id(),
            "manifestSha256": base.manifest_digest,
            "logicalSequence": base.logical_sequence(),
            "rootRows": {
                "kv": base.kv_root.row_count(),
                "activeId": base.active_id_root.row_count(),
                "deliveryOrder": base.delivery_order_root.row_count(),
            },
            "rootDepths": {
                "kv": base.kv_root.depth(),
                "activeId": base.active_id_root.depth(),
                "deliveryOrder": base.delivery_order_root.depth(),
            },
        }))
    }

    /// Exports the verified logical tuple at the currently authenticated HEAD.
    ///
    /// # Errors
    /// Returns an error when no authority-8 HEAD exists or its published tuple cannot be verified.
    #[cfg(feature = "test-utils")]
    pub async fn export_published_logical_v2_current(&self) -> Result<serde_json::Value> {
        let base = self.pin_bounded_base().await?;
        let token = base
            .token(self)
            .ok_or_else(|| invariant_violation("published logical export has no current token"))?;
        self.export_published_logical_v2(&token).await
    }

    /// Pins the format-8 HEAD or the explicit empty format-8 genesis base.
    pub(super) async fn pin_bounded_base(&self) -> Result<Base> {
        if self.authority_format != AUTHORITY_FORMAT {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "bounded authority requires explicit authority format 8".into(),
            });
        }
        let directory = directory::Directory::new(self.retention.clone(), &self.scope)?;
        let Some((pointer, version, pointer_bytes)) = self.load_pinned_pointer().await? else {
            let empty = directory.empty_root_reference()?;
            return Ok(Base {
                manifest: None,
                manifest_digest: None,
                pointer_version: None,
                pointer_bytes: None,
                writer_epoch: 0,
                reclamation_generation: 0,
                logical_sequence: 0,
                selection: None,
                kv_root: empty.clone(),
                active_id_root: empty.clone(),
                delivery_order_root: empty,
            });
        };
        let manifest_bytes = self
            .get_json(
                &self.paths.manifest_object(&pointer.manifest_id),
                super::MAX_CONTROL_JSON_BYTES,
            )
            .await?;
        validate_raw_checksum(
            &manifest_bytes,
            Some(&pointer.manifest_checksum_sha256),
            "bounded manifest reference checksum",
        )?;
        let manifest: Manifest8 = decode_json(&manifest_bytes, "bounded manifest")?;
        manifest.validate(&self.scope, &pointer.manifest_id)?;
        if manifest.logical_sequence != pointer.logical_sequence
            || pointer.writer_epoch < manifest.writer_epoch
            || pointer.reclamation_generation < manifest.reclamation_generation
        {
            return Err(invariant_violation(
                "bounded HEAD fences or sequence differ from manifest",
            ));
        }
        let roots = decode_roots(&directory, &manifest)?;
        validate_manifest_artifacts(self, &manifest, (&roots.0, &roots.1, &roots.2)).await?;
        base_from_manifest(
            &directory,
            manifest,
            pointer.manifest_checksum_sha256,
            Some(version),
            Some(pointer_bytes.to_vec()),
            pointer.writer_epoch,
            pointer.reclamation_generation,
        )
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arco_core::{MemoryBackend, ScopedStorage};
    use bytes::Bytes;

    use super::*;
    use crate::state_store::{ArcoStateTxn, StateScope, TxnOptions};

    #[tokio::test]
    async fn bounded_empty_base_reads_missing_key() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("authority-8 test store");

        let base = store
            .pin_bounded_base()
            .await
            .expect("bounded genesis base");
        assert_eq!(base.logical_sequence(), 0);
        assert_eq!(base.get(&store, b"missing").await.expect("read"), None);
    }

    #[tokio::test]
    async fn bounded_commit_path_copies_only_the_changed_kv_leaf_and_publishes_head() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("authority-8 test store");
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin bounded transaction");
        txn.set_logical_operation("op-1", "catalog", &"11".repeat(32))
            .expect("freeze V2 operation");
        txn.put(b"changed", Bytes::from_static(b"value"))
            .await
            .expect("stage write");

        let (token, _, _) = store
            .commit_bounded(txn)
            .await
            .expect("commit bounded write");
        assert_eq!(token.logical_sequence(), 1);
        let base = store
            .pin_bounded_base()
            .await
            .expect("published bounded base");
        assert_eq!(
            base.get(&store, b"changed")
                .await
                .expect("read changed key")
                .expect("stored value")
                .bytes,
            Bytes::from_static(b"value")
        );
    }

    #[tokio::test]
    async fn bounded_delivery_page_returns_the_authenticated_v2_envelope() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("authority-8 test store");
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("begin bounded transaction");
        txn.set_logical_operation("deliver-1", "catalog", &"44".repeat(32))
            .expect("freeze V2 operation");
        txn.stage_projection_intent_v2(
            "delivery-1",
            "catalog-audit",
            Bytes::from_static(b"opaque payload"),
        )
        .await
        .expect("stage V2 intent");
        store.commit_bounded(txn).await.expect("commit V2 intent");

        let base = store
            .pin_bounded_base()
            .await
            .expect("published bounded base");
        let page = base
            .delivery_page(&store, None, 1)
            .await
            .expect("bounded delivery page");
        assert_eq!(page.records.len(), 1);
        let intent: logical_v2::ProjectionIntentV2 =
            decode_json(page.records[0].payload(), "returned V2 intent").expect("V2 envelope");
        assert_eq!(intent.intent_id(), "delivery-1");
        assert!(page.next_after.is_none());
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the forged proof fixture binds all three authenticated roots"
    )]
    async fn bounded_transition_rejects_authenticated_rows_not_declared_by_transaction() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("authority-8 test store");
        let base = store
            .pin_bounded_base()
            .await
            .expect("bounded genesis base");
        let sequence = 1;
        let rows = vec![ControlMvpSegmentRow {
            record_kind: super::super::SEGMENT_RECORD_KV,
            key: b"persisted-but-not-declared".to_vec(),
            value: Some(b"value".to_vec()),
            generation: sequence,
            tombstone: false,
            logical_sequence: sequence,
            logical_ordinal: 0,
            origin_sequence: None,
        }];
        let leaves = persist_role_rows(
            &store,
            physical::Role::Kv,
            "forged-transition",
            0,
            sequence,
            &rows,
        )
        .await
        .expect("persist authenticated forged block");
        let directory =
            directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
        directory
            .empty_root()
            .await
            .expect("persist explicit empty test root");
        let mut budget = directory::ReadBudget::default();
        let edit = directory::update::Edit {
            old: None,
            new: leaves.clone(),
        };
        let candidate_kv = directory
            .update(&base.kv_root, std::slice::from_ref(&edit), &mut budget)
            .await
            .expect("candidate root");
        let mut transaction = Transaction8 {
            format_version: AUTHORITY_FORMAT,
            scope: store.scope.clone(),
            transaction_id: "forged-transition".to_string(),
            logical_sequence: sequence,
            operation: logical_v2::Operation {
                operation_id: "operation".to_string(),
                family: "catalog".to_string(),
                request_digest: "11".repeat(32),
            },
            logical_commit_id: "22".repeat(32),
            logical_history: "33".repeat(32),
            writes: vec![BoundedWrite8 {
                key: b"declared-but-not-persisted".to_vec(),
                generation: sequence,
                delete: false,
                value_sha256: Some(sha256_hex(b"value")),
            }],
            additions: Vec::new(),
            trims: Vec::new(),
        };
        transaction.logical_commit_id = logical_v2::commit_id(
            &store.scope,
            &logical_v2::genesis(&store.scope),
            sequence,
            &transaction.operation,
        )
        .expect("logical ID for forged transition");
        let transition = Transition8 {
            format_version: AUTHORITY_FORMAT,
            scope: store.scope.clone(),
            transaction_id: transaction.transaction_id.clone(),
            roles: vec![
                role_transition(
                    physical::Role::Kv,
                    &base.kv_root,
                    &candidate_kv,
                    vec![EditProof8 {
                        old: None,
                        new: leaves.iter().map(leaf_proof).collect(),
                    }],
                ),
                role_transition(
                    physical::Role::ActiveId,
                    &base.active_id_root,
                    &base.active_id_root,
                    Vec::new(),
                ),
                role_transition(
                    physical::Role::DeliveryOrder,
                    &base.delivery_order_root,
                    &base.delivery_order_root,
                    Vec::new(),
                ),
            ],
        };

        #[cfg(feature = "test-utils")]
        super::super::cost::take_bounded_work();
        let mut work = BoundedCommitWork::default();
        assert!(
            verify_candidate_transition(
                &store,
                &base,
                &candidate_kv,
                &base.active_id_root,
                &base.delivery_order_root,
                &transaction,
                &transition,
                &mut work,
            )
            .await
            .is_err()
        );
        #[cfg(feature = "test-utils")]
        assert!(
            super::super::cost::take_bounded_work()
                .values()
                .any(|work| work.transition_proof_rows > 0),
            "rejected transition proof rows remain observable",
        );

        transaction.writes[0].key = b"persisted-but-not-declared".to_vec();
        assert!(
            verify_candidate_transition(
                &store,
                &base,
                &candidate_kv,
                &base.active_id_root,
                &base.delivery_order_root,
                &transaction,
                &transition,
                &mut work,
            )
            .await
            .is_err()
        );
    }
}
