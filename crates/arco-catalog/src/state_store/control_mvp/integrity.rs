//! Canonical authority-format-7 integrity commitments. No storage I/O lives here.
use arco_core::AuthorityRoot;

use crate::StateScope;

#[cfg(feature = "test-utils")]
use super::record_integrity_work;
use super::{
    BTreeSet, CONTROL_MVP_FORMAT_VERSION, ControlMvpCheckpoint, ControlMvpManifest,
    ControlMvpStateRef, ControlMvpTxObject, Deserialize, IMPLEMENTATION, MAX_SEGMENT_BYTES,
    MAX_SEGMENT_INDEX_BYTES, MAX_TRANSACTION_JSON_BYTES, ReplayState, Result,
    SEGMENT_FORMAT_VERSION, Serialize, invariant_violation, segment_serialization_error,
    sha256_hex, state_reference_key_bounds, valid_raw_digest,
};
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct HistoryAnchor {
    pub sequence: u64,
    pub root: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct HistoryLink {
    pub preceding_root: String,
    pub mutation_sha256: String,
    pub resulting_root: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RewriteEquivalence {
    pub encoding_version: u32,
    pub source_manifest_id: String,
    pub source_manifest_sha256: String,
    pub source_history_root: String,
    pub source_physical_root: String,
    pub logical_sequence: u64,
    pub state_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CheckpointValidation {
    pub encoding_version: u32,
    pub source_manifest_sha256: String,
    pub source_history_root: String,
    pub source_physical_root: String,
    pub state_checksum_sha256: String,
    pub checkpoint_physical_root: String,
}

struct Canonical(Vec<u8>);

impl Canonical {
    fn new(tag: &[u8], scope: &StateScope) -> Result<Self> {
        let mut this = Self(Vec::new());
        this.bytes(tag);
        this.u32(1);
        this.bytes(IMPLEMENTATION.as_bytes());
        this.u32(CONTROL_MVP_FORMAT_VERSION);
        this.bytes(scope.tenant_id.as_bytes());
        match scope.root() {
            AuthorityRoot::Workspace { workspace_id } => {
                // Workspace digests are unchanged from legacy v1 `StateScope`.
                // Avoid adding `this.bytes(b"root=workspace")`.
                this.bytes(workspace_id.as_bytes())
            }
            AuthorityRoot::Metastore { metastore_id } => {
                this.bytes(b"root=metastore");
                this.bytes(metastore_id.as_bytes())
            }
            AuthorityRoot::TenantIdentity => {
                this.bytes(b"root=identity");
            }
            _ => {
                return Err(invariant_violation(
                    "unsupported authority root for canonical digest",
                ));
            }
        }
        this.bytes(scope.domain.as_bytes());
        Ok(this)
    }
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.extend_from_slice(value);
    }
    fn optional_bytes(&mut self, value: Option<&[u8]>) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.bytes(value);
            }
        }
    }
    fn digest(&mut self, value: &str) -> Result<()> {
        if !valid_raw_digest(value) {
            return Err(invariant_violation("invalid canonical digest"));
        }
        self.0.extend_from_slice(
            &hex::decode(value)
                .map_err(|error| segment_serialization_error("canonical digest", error))?,
        );
        Ok(())
    }
    fn finish(self) -> String {
        #[cfg(feature = "test-utils")]
        record_integrity_work(0, self.0.len());
        sha256_hex(&self.0)
    }
}

pub(super) fn genesis(scope: &StateScope) -> Result<HistoryAnchor> {
    Ok(HistoryAnchor {
        sequence: 0,
        root: Canonical::new(b"arco/control-v1/history-genesis", scope)?.finish(),
    })
}

pub(super) fn history_step(
    scope: &StateScope,
    preceding: &str,
    sequence: u64,
    mutation: &str,
) -> Result<String> {
    let mut out = Canonical::new(b"arco/control-v1/history-step", scope)?;
    out.digest(preceding)?;
    out.u64(sequence);
    out.digest(mutation)?;
    Ok(out.finish())
}

pub(super) fn mutation_digest(tx: &ControlMvpTxObject) -> Result<String> {
    let mut out = Canonical::new(b"arco/control-v1/mutation", &tx.scope)?;
    out.optional_bytes(tx.request_id.as_deref().map(str::as_bytes));
    let mut writes = tx.writes.iter().collect::<Vec<_>>();
    writes.sort_by(|a, b| a.key.cmp(&b.key));
    out.u64(writes.len() as u64);
    for write in writes {
        out.bytes(&write.key);
        out.u64(write.generation);
        out.optional_bytes(write.value.as_deref());
    }
    out.u64(tx.outbox.len() as u64);
    for entry in &tx.outbox {
        out.bytes(entry.record_id.as_bytes());
        out.bytes(&entry.payload);
        out.u64(tx.sequence);
    }
    let mut trims = tx.outbox_trim.iter().collect::<Vec<_>>();
    trims.sort_by(|a, b| (&a.record_id, a.origin_sequence).cmp(&(&b.record_id, b.origin_sequence)));
    out.u64(trims.len() as u64);
    for trim in trims {
        out.bytes(trim.record_id.as_bytes());
        out.u64(trim.origin_sequence);
    }
    Ok(out.finish())
}

impl HistoryLink {
    pub(super) fn new(tx: &ControlMvpTxObject, preceding: &str) -> Result<Self> {
        let mutation_sha256 = mutation_digest(tx)?;
        Ok(Self {
            preceding_root: preceding.to_string(),
            resulting_root: history_step(&tx.scope, preceding, tx.sequence, &mutation_sha256)?,
            mutation_sha256,
        })
    }
    pub(super) fn validate(&self, scope: &StateScope, sequence: u64) -> Result<()> {
        if history_step(scope, &self.preceding_root, sequence, &self.mutation_sha256)?
            != self.resulting_root
        {
            return Err(invariant_violation("invalid logical-history link"));
        }
        Ok(())
    }
}

fn encode_states(out: &mut Canonical, role: u8, states: &[ControlMvpStateRef]) -> Result<()> {
    validate_state_refs(states)?;
    out.u8(role);
    out.u64(states.len() as u64);
    for reference in states {
        out.bytes(reference.state_id.as_bytes());
        out.u64(reference.logical_sequence);
        out.u64(reference.segment_size_bytes);
        out.u64(reference.index_size_bytes);
        out.u32(SEGMENT_FORMAT_VERSION);
        out.u32(SEGMENT_FORMAT_VERSION);
        let bounds = state_reference_key_bounds(reference)?;
        out.optional_bytes(bounds.as_ref().map(|(min, _)| min.as_slice()));
        out.optional_bytes(bounds.as_ref().map(|(_, max)| max.as_slice()));
        out.digest(&reference.checksum_sha256)?;
        out.digest(&reference.index_checksum_sha256)?;
    }
    Ok(())
}

pub(super) fn validate_state_refs(states: &[ControlMvpStateRef]) -> Result<Option<u64>> {
    let sequence = states.first().map(|state| state.logical_sequence);
    let mut ids = BTreeSet::new();
    let mut previous: Option<Vec<u8>> = None;
    let mut keyless = false;
    for reference in states {
        if !valid_immutable_id(&reference.state_id)
            || Some(reference.logical_sequence) != sequence
            || reference.segment_size_bytes == 0
            || reference.segment_size_bytes > MAX_SEGMENT_BYTES as u64
            || reference.index_size_bytes == 0
            || reference.index_size_bytes > MAX_SEGMENT_INDEX_BYTES as u64
            || !valid_raw_digest(&reference.checksum_sha256)
            || !valid_raw_digest(&reference.index_checksum_sha256)
            || !ids.insert(&reference.state_id)
        {
            return Err(invariant_violation("invalid owning state reference"));
        }
        match state_reference_key_bounds(reference)? {
            Some((minimum, maximum)) => {
                if keyless || previous.as_ref().is_some_and(|prior| prior >= &minimum) {
                    return Err(invariant_violation("unordered owning state bounds"));
                }
                previous = Some(maximum);
            }
            None => keyless = true,
        }
    }
    Ok(sequence)
}

pub(super) fn valid_immutable_id(value: &str) -> bool {
    !value.trim().is_empty()
        && !matches!(value, "." | "..")
        && !value.contains(['/', '\\', '%'])
        && !value.chars().any(char::is_control)
}

impl ControlMvpManifest {
    pub(super) fn physical_digest(&self) -> Result<String> {
        let mut out = Canonical::new(b"arco/control-v1/manifest-layout", &self.scope)?;
        encode_states(&mut out, 1, &self.base_states)?;
        encode_states(&mut out, 2, &self.anchor_states)?;
        out.u8(3);
        out.u64(self.tx_refs.len() as u64);
        for reference in &self.tx_refs {
            out.bytes(reference.tx_id.as_bytes());
            out.u64(reference.sequence);
            out.u64(reference.size_bytes);
            out.u32(CONTROL_MVP_FORMAT_VERSION);
            out.digest(&reference.checksum_sha256)?;
        }
        Ok(out.finish())
    }

    pub(super) fn successor_history_anchor(&self) -> HistoryAnchor {
        if self.anchor_states.is_empty() {
            self.history_anchor.clone()
        } else {
            HistoryAnchor {
                sequence: self.logical_sequence,
                root: self.history_root.clone(),
            }
        }
    }

    pub(super) fn validate_integrity(&self) -> Result<()> {
        let base_sequence = self
            .base_states
            .first()
            .map_or(0, |state| state.logical_sequence);
        if self.history_anchor.sequence != base_sequence
            || !valid_raw_digest(&self.history_anchor.root)
            || (base_sequence == 0 && self.history_anchor != genesis(&self.scope)?)
        {
            return Err(invariant_violation(
                "history anchor does not match replay base",
            ));
        }
        let mut preceding = self.history_anchor.root.as_str();
        for reference in &self.tx_refs {
            if reference.size_bytes == 0
                || reference.size_bytes > MAX_TRANSACTION_JSON_BYTES as u64
                || reference.history.preceding_root != preceding
            {
                return Err(invariant_violation(
                    "discontinuous history suffix or invalid transaction length",
                ));
            }
            reference
                .history
                .validate(&self.scope, reference.sequence)?;
            preceding = &reference.history.resulting_root;
        }
        if preceding != self.history_root || self.physical_digest()? != self.physical_root {
            return Err(invariant_violation("manifest integrity root mismatch"));
        }
        if let Some(evidence) = &self.equivalence {
            if evidence.encoding_version != 1
                || !self.tx_refs.is_empty()
                || self.base_manifest_id.as_deref() != Some(&evidence.source_manifest_id)
                || self.parent_manifest_sha256.as_deref() != Some(&evidence.source_manifest_sha256)
                || evidence.source_history_root != self.history_root
                || evidence.logical_sequence != self.logical_sequence
                || evidence.state_checksum_sha256 != self.state_checksum_sha256
                || !valid_raw_digest(&evidence.source_physical_root)
                || self.layout_generation == 0
            {
                return Err(invariant_violation("invalid rewrite equivalence evidence"));
            }
        } else if self.tx_refs.is_empty() {
            return Err(invariant_violation(
                "materialized maintenance lacks equivalence evidence",
            ));
        }
        Ok(())
    }
}

pub(super) fn checkpoint_physical_digest(
    scope: &StateScope,
    states: &[ControlMvpStateRef],
) -> Result<String> {
    let mut out = Canonical::new(b"arco/control-v1/checkpoint-layout", scope)?;
    encode_states(&mut out, 4, states)?;
    Ok(out.finish())
}

impl ControlMvpCheckpoint {
    pub(super) fn validate_source(&self, manifest: &ControlMvpManifest) -> Result<()> {
        let evidence = &self.validation;
        let source_digest_matches =
            evidence.source_manifest_sha256 == self.manifest_checksum_sha256;
        if self.manifest_id != manifest.manifest_id
            || self.logical_sequence != manifest.logical_sequence
            || !source_digest_matches
            || evidence.source_history_root != manifest.history_root
            || evidence.source_physical_root != manifest.physical_root
            || evidence.state_checksum_sha256 != manifest.state_checksum_sha256
        {
            return Err(invariant_violation(
                "checkpoint source validation evidence mismatch",
            ));
        }
        Ok(())
    }
    pub(super) fn validate_state(&self, state: &mut ReplayState) -> Result<()> {
        if state.logical_sequence != self.logical_sequence
            || state.checksum()? != self.validation.state_checksum_sha256
        {
            return Err(invariant_violation(
                "checkpoint state differs from validation evidence",
            ));
        }
        state
            .history_root
            .clone_from(&self.validation.source_history_root);
        Ok(())
    }
}
