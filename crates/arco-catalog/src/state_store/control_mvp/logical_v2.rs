//! Authority-8 logical identity and projection-envelope commitments.

use super::{ControlMvpWriteEntry, Result, StateScope, invariant_violation, valid_raw_digest};
use arco_core::AuthorityRoot;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const ENCODING_VERSION: u32 = 2;

struct Canonical {
    hasher: Sha256,
    #[cfg(feature = "test-utils")]
    input_bytes: usize,
}

impl Canonical {
    fn new(tag: &[u8], scope: &StateScope) -> Self {
        let mut out = Self {
            hasher: Sha256::new(),
            #[cfg(feature = "test-utils")]
            input_bytes: 0,
        };
        out.bytes(tag);
        out.u32(ENCODING_VERSION);
        out.scope(scope);
        out
    }

    fn update(&mut self, bytes: impl AsRef<[u8]>) {
        let bytes = bytes.as_ref();
        self.hasher.update(bytes);
        #[cfg(feature = "test-utils")]
        {
            self.input_bytes = self.input_bytes.saturating_add(bytes.len());
        }
    }

    fn u8(&mut self, value: u8) {
        self.update([value]);
    }

    fn u32(&mut self, value: u32) {
        self.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.update(value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.update(value);
    }

    fn scope(&mut self, scope: &StateScope) {
        self.bytes(scope.tenant_id().as_bytes());
        match scope.root() {
            AuthorityRoot::Workspace { workspace_id } => self.bytes(workspace_id.as_bytes()),
            AuthorityRoot::Metastore { metastore_id } => {
                self.bytes(b"root=metastore");
                self.bytes(metastore_id.as_bytes());
            }
            AuthorityRoot::TenantIdentity => self.bytes(b"root=identity"),
            _ => self.bytes(b"root=unsupported"),
        }
        self.bytes(scope.domain().as_bytes());
    }

    fn digest(&mut self, value: &str) -> Result<()> {
        validate_digest(value, "logical digest")?;
        let mut bytes = [0_u8; 32];
        hex::decode_to_slice(value, &mut bytes)
            .map_err(|_| invariant_violation("logical digest hex decoding failed"))?;
        self.update(bytes);
        Ok(())
    }

    fn finish(self) -> String {
        #[cfg(feature = "test-utils")]
        super::record_sha256_work(self.input_bytes);
        hex::encode(self.hasher.finalize())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
// Field spelling is part of the frozen authority-8 transaction encoding.
#[allow(clippy::struct_field_names)]
pub(super) struct Operation {
    pub(super) operation_id: String,
    pub(super) family: String,
    pub(super) request_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Trim {
    pub(super) record_id: String,
    pub(super) origin_sequence: u64,
    pub(super) ordinal: u64,
}

/// Version-two committed projection-intent envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionIntentV2 {
    contract_version: u32,
    intent_id: String,
    projection_kind: String,
    source_scope: StateScope,
    source_logical_sequence: u64,
    logical_commit_id: String,
    ordinal: u64,
    payload: Vec<u8>,
}

impl ProjectionIntentV2 {
    /// Wire-contract version written by this implementation.
    pub const CONTRACT_VERSION: u32 = 2;

    /// Creates and validates a V2 projection intent.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed logical fields or an empty payload.
    pub(crate) fn new(
        intent_id: impl Into<String>,
        projection_kind: impl Into<String>,
        source_scope: StateScope,
        source_logical_sequence: u64,
        logical_commit_id: impl Into<String>,
        ordinal: u64,
        payload: impl AsRef<[u8]>,
    ) -> Result<Self> {
        let intent = Self {
            contract_version: Self::CONTRACT_VERSION,
            intent_id: intent_id.into(),
            projection_kind: projection_kind.into(),
            source_scope,
            source_logical_sequence,
            logical_commit_id: logical_commit_id.into(),
            ordinal,
            payload: payload.as_ref().to_vec(),
        };
        intent.validate()?;
        Ok(intent)
    }

    #[must_use]
    /// Returns the wire-contract version.
    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    #[must_use]
    /// Returns the immutable intent identifier.
    pub fn intent_id(&self) -> &str {
        &self.intent_id
    }

    #[must_use]
    /// Returns the projection family this intent targets.
    pub fn projection_kind(&self) -> &str {
        &self.projection_kind
    }

    #[must_use]
    /// Returns the authority scope that produced this intent.
    pub const fn source_scope(&self) -> &StateScope {
        &self.source_scope
    }

    #[must_use]
    /// Returns the committed logical sequence that produced this intent.
    pub const fn source_logical_sequence(&self) -> u64 {
        self.source_logical_sequence
    }

    #[must_use]
    /// Returns the physical-layout-independent logical commit identity.
    pub fn logical_commit_id(&self) -> &str {
        &self.logical_commit_id
    }

    #[must_use]
    /// Returns this intent's delivery order in its source commit.
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    #[must_use]
    /// Returns the opaque projection payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn validate(&self) -> Result<()> {
        if self.contract_version != Self::CONTRACT_VERSION {
            return Err(invariant_violation(
                "projection intent V2 contract version is unsupported",
            ));
        }
        validate_id(&self.intent_id, "projection intent ID")?;
        validate_id(&self.projection_kind, "projection kind")?;
        self.source_scope.validate()?;
        validate_sequence(self.source_logical_sequence, "projection source sequence")?;
        validate_digest(&self.logical_commit_id, "projection logical commit ID")?;
        if self.payload.is_empty() {
            return Err(invariant_violation("projection payload must not be empty"));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ProjectionIntentV2 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            contract_version: u32,
            intent_id: String,
            projection_kind: String,
            source_scope: StateScope,
            source_logical_sequence: u64,
            logical_commit_id: String,
            ordinal: u64,
            payload: Vec<u8>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let intent = Self {
            contract_version: wire.contract_version,
            intent_id: wire.intent_id,
            projection_kind: wire.projection_kind,
            source_scope: wire.source_scope,
            source_logical_sequence: wire.source_logical_sequence,
            logical_commit_id: wire.logical_commit_id,
            ordinal: wire.ordinal,
            payload: wire.payload,
        };
        intent.validate().map_err(serde::de::Error::custom)?;
        Ok(intent)
    }
}

pub(super) fn genesis(scope: &StateScope) -> String {
    Canonical::new(b"arco/control-v2/history-genesis", scope).finish()
}

pub(super) fn commit_id(
    scope: &StateScope,
    prior_history: &str,
    sequence: u64,
    operation: &Operation,
) -> Result<String> {
    scope.validate()?;
    validate_digest(prior_history, "prior logical history")?;
    validate_sequence(sequence, "logical commit sequence")?;
    validate_id(&operation.operation_id, "operation ID")?;
    validate_id(&operation.family, "operation family")?;
    validate_digest(&operation.request_digest, "operation request digest")?;

    let mut out = Canonical::new(b"arco/control-v2/logical-commit-id", scope);
    out.digest(prior_history)?;
    out.u64(sequence);
    out.bytes(operation.operation_id.as_bytes());
    out.bytes(operation.family.as_bytes());
    out.digest(&operation.request_digest)?;
    Ok(out.finish())
}

pub(super) fn history(
    scope: &StateScope,
    prior: &str,
    sequence: u64,
    logical_commit_id: &str,
    writes: &[ControlMvpWriteEntry],
    additions: &[ProjectionIntentV2],
    trims: &[Trim],
) -> Result<String> {
    scope.validate()?;
    validate_digest(prior, "prior logical history")?;
    validate_sequence(sequence, "logical history sequence")?;
    validate_digest(logical_commit_id, "logical commit ID")?;

    let mut out = Canonical::new(b"arco/control-v2/history-step", scope);
    out.digest(prior)?;
    out.u64(sequence);
    out.digest(logical_commit_id)?;
    encode_writes(&mut out, writes, sequence)?;
    encode_additions(&mut out, additions, scope, sequence, logical_commit_id)?;
    encode_trims(&mut out, trims, sequence)?;
    Ok(out.finish())
}

pub(super) struct SyntheticGenesisHistory {
    out: Canonical,
    scope: StateScope,
    logical_sequence: u64,
    declared_kv_count: u64,
    declared_intent_count: u64,
    accepted_kv_count: u64,
    accepted_intent_count: u64,
    intents_started: bool,
    previous_kv_key: Option<Vec<u8>>,
    previous_intent_id: Option<String>,
    previous_delivery: Option<(u64, u64, String)>,
}

impl SyntheticGenesisHistory {
    pub(super) fn new(
        scope: &StateScope,
        logical_sequence: u64,
        declared_kv_count: u64,
        declared_intent_count: u64,
    ) -> Result<Self> {
        scope.validate()?;
        validate_sequence(logical_sequence, "synthetic genesis sequence")?;
        let mut out = Canonical::new(b"arco/control-v2/synthetic-genesis", scope);
        out.u64(logical_sequence);
        out.u64(declared_kv_count);
        Ok(Self {
            out,
            scope: scope.clone(),
            logical_sequence,
            declared_kv_count,
            declared_intent_count,
            accepted_kv_count: 0,
            accepted_intent_count: 0,
            intents_started: false,
            previous_kv_key: None,
            previous_intent_id: None,
            previous_delivery: None,
        })
    }

    pub(super) fn push_kv(
        &mut self,
        key: &[u8],
        generation: u64,
        value: Option<&[u8]>,
    ) -> Result<()> {
        if self.intents_started || self.accepted_kv_count == self.declared_kv_count {
            return Err(invariant_violation(
                "synthetic genesis KV count exceeds declaration",
            ));
        }
        if self
            .previous_kv_key
            .as_deref()
            .is_some_and(|previous| previous >= key)
        {
            return Err(invariant_violation(
                "synthetic genesis KV keys are not strictly ordered",
            ));
        }
        if generation == 0 || generation > self.logical_sequence {
            return Err(invariant_violation(
                "invalid synthetic genesis KV generation",
            ));
        }
        encode_write_tuple(&mut self.out, key, generation, value);
        self.previous_kv_key = Some(key.to_vec());
        self.accepted_kv_count += 1;
        Ok(())
    }

    pub(super) fn push_intent(&mut self, intent: &ProjectionIntentV2) -> Result<()> {
        if self.accepted_kv_count != self.declared_kv_count {
            return Err(invariant_violation(
                "synthetic genesis intent precedes declared KV rows",
            ));
        }
        if self.accepted_intent_count == self.declared_intent_count {
            return Err(invariant_violation(
                "synthetic genesis intent count exceeds declaration",
            ));
        }
        if !self.intents_started {
            self.out.u64(self.declared_intent_count);
            self.intents_started = true;
        }
        intent.validate()?;
        if intent.source_scope != self.scope
            || intent.source_logical_sequence > self.logical_sequence
        {
            return Err(invariant_violation(
                "invalid synthetic genesis V2 intent origin",
            ));
        }
        if self
            .previous_intent_id
            .as_deref()
            .is_some_and(|previous| previous.as_bytes() >= intent.intent_id.as_bytes())
        {
            return Err(invariant_violation(
                "synthetic genesis V2 intent IDs are not strictly ordered",
            ));
        }
        let delivery = (
            intent.source_logical_sequence,
            intent.ordinal,
            intent.intent_id.clone(),
        );
        if self
            .previous_delivery
            .as_ref()
            .is_some_and(|previous| previous >= &delivery)
        {
            return Err(invariant_violation(
                "synthetic genesis V2 delivery tuples are not strictly ordered",
            ));
        }
        encode_intent_tuple(&mut self.out, intent)?;
        self.previous_intent_id = Some(intent.intent_id.clone());
        self.previous_delivery = Some(delivery);
        self.accepted_intent_count += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<String> {
        if self.accepted_kv_count != self.declared_kv_count
            || self.accepted_intent_count != self.declared_intent_count
        {
            return Err(invariant_violation(
                "synthetic genesis accepted counts differ from declaration",
            ));
        }
        if !self.intents_started {
            self.out.u64(self.declared_intent_count);
        }
        Ok(self.out.finish())
    }
}

fn validate_id(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty()
        || matches!(value, "." | "..")
        || value.contains(['/', '\\', '%'])
        || value.chars().any(char::is_control)
    {
        return Err(invariant_violation(format!("invalid {field}")));
    }
    Ok(())
}

fn validate_sequence(sequence: u64, field: &str) -> Result<()> {
    if sequence == 0 {
        return Err(invariant_violation(format!("invalid {field}")));
    }
    Ok(())
}

fn validate_digest(value: &str, field: &str) -> Result<()> {
    if !valid_raw_digest(value) {
        return Err(invariant_violation(format!("invalid {field}")));
    }
    Ok(())
}

fn encode_writes(
    out: &mut Canonical,
    writes: &[ControlMvpWriteEntry],
    sequence: u64,
) -> Result<()> {
    let mut sorted = writes.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.key.cmp(&right.key));
    if sorted
        .windows(2)
        .any(|pair| matches!(pair, [first, second] if first.key == second.key))
    {
        return Err(invariant_violation("duplicate logical V2 write key"));
    }
    out.u64(sorted.len() as u64);
    for write in sorted {
        if write.generation == 0 || write.generation > sequence {
            return Err(invariant_violation("invalid logical V2 write generation"));
        }
        encode_write_tuple(out, &write.key, write.generation, write.value.as_deref());
    }
    Ok(())
}

fn encode_additions(
    out: &mut Canonical,
    additions: &[ProjectionIntentV2],
    scope: &StateScope,
    sequence: u64,
    logical_commit_id: &str,
) -> Result<()> {
    out.u64(additions.len() as u64);
    for (index, addition) in additions.iter().enumerate() {
        addition.validate()?;
        if addition.source_scope != *scope
            || addition.source_logical_sequence != sequence
            || addition.logical_commit_id != logical_commit_id
            || addition.ordinal != index as u64
        {
            return Err(invariant_violation(
                "invalid logical V2 projection addition",
            ));
        }
        encode_intent_tuple(out, addition)?;
    }
    Ok(())
}

fn encode_write_tuple(out: &mut Canonical, key: &[u8], generation: u64, value: Option<&[u8]>) {
    out.bytes(key);
    out.u64(generation);
    match value {
        None => out.u8(0),
        Some(value) => {
            out.u8(1);
            out.bytes(value);
        }
    }
}

fn encode_intent_tuple(out: &mut Canonical, intent: &ProjectionIntentV2) -> Result<()> {
    intent.validate()?;
    out.u32(intent.contract_version);
    out.bytes(intent.intent_id.as_bytes());
    out.bytes(intent.projection_kind.as_bytes());
    out.scope(&intent.source_scope);
    out.u64(intent.source_logical_sequence);
    out.digest(&intent.logical_commit_id)?;
    out.u64(intent.ordinal);
    out.bytes(&intent.payload);
    Ok(())
}

fn encode_trims(out: &mut Canonical, trims: &[Trim], sequence: u64) -> Result<()> {
    let mut sorted = trims.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        (left.record_id.as_bytes(), left.origin_sequence)
            .cmp(&(right.record_id.as_bytes(), right.origin_sequence))
    });
    if sorted.windows(2).any(|pair| {
        matches!(pair, [first, second] if first.record_id == second.record_id && first.origin_sequence == second.origin_sequence)
    }) {
        return Err(invariant_violation("duplicate logical V2 trim"));
    }
    out.u64(sorted.len() as u64);
    for trim in sorted {
        validate_id(&trim.record_id, "trim record ID")?;
        if trim.origin_sequence == 0 || trim.origin_sequence > sequence {
            return Err(invariant_violation(
                "invalid logical V2 trim origin sequence",
            ));
        }
        out.bytes(trim.record_id.as_bytes());
        out.u64(trim.origin_sequence);
        out.u64(trim.ordinal);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> StateScope {
        StateScope::new("tenant", "workspace", "catalog")
    }

    fn operation() -> Operation {
        Operation {
            operation_id: "op-7".to_string(),
            family: "catalog".to_string(),
            request_digest: "11".repeat(32),
        }
    }

    fn write(key: &[u8], generation: u64, value: Option<&[u8]>) -> ControlMvpWriteEntry {
        ControlMvpWriteEntry {
            key: key.to_vec(),
            generation,
            value: value.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn v2_logical_identity_has_a_fixed_golden_vector() {
        let scope = scope();
        let genesis = genesis(&scope);
        assert_eq!(
            genesis,
            "2bcac15d2eeca61813513ae512100e8bd3c6f4a0c63199a3bc98d9439c7c6b49"
        );
        let commit = commit_id(&scope, &genesis, 7, &operation()).unwrap();
        assert_eq!(
            commit,
            "86a903a03c1c45daf8578cda5491abba8bce53bd049a1e76b811951b85018ef0"
        );

        let addition = ProjectionIntentV2::new(
            "intent-0",
            "audit",
            scope.clone(),
            7,
            commit.clone(),
            0,
            [0, 255, 0, 1],
        )
        .unwrap();
        assert_eq!(
            history(
                &scope,
                &genesis,
                7,
                &commit,
                &[
                    write(b"", 7, Some(&[0, 255, 1])),
                    write(&[0xff, 0], 7, None),
                ],
                &[addition],
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 9,
                }],
            )
            .unwrap(),
            "bf650bafb2df765edcb69ee313bcf0c8071d39e2eb03e185a255385d03c0fb83"
        );
    }

    #[test]
    // One mutation matrix keeps every field comparison tied to the same reference.
    #[allow(clippy::too_many_lines)]
    fn v2_history_binds_kv_bytes_generations_deletes_order_trims_and_scope() {
        let scope = scope();
        let prior = genesis(&scope);
        let commit = commit_id(&scope, &prior, 7, &operation()).unwrap();
        let addition = ProjectionIntentV2::new(
            "intent-0",
            "audit",
            scope.clone(),
            7,
            commit.clone(),
            0,
            [0, 255],
        )
        .unwrap();
        let base = history(
            &scope,
            &prior,
            7,
            &commit,
            &[write(b"a", 7, Some(&[0, 255])), write(b"b", 7, None)],
            std::slice::from_ref(&addition),
            &[Trim {
                record_id: "intent-old".to_string(),
                origin_sequence: 3,
                ordinal: 1,
            }],
        )
        .unwrap();
        assert_eq!(
            base,
            history(
                &scope,
                &prior,
                7,
                &commit,
                &[write(b"b", 7, None), write(b"a", 7, Some(&[0, 255]))],
                std::slice::from_ref(&addition),
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 1,
                }],
            )
            .unwrap()
        );
        for changed in [
            history(
                &scope,
                &prior,
                7,
                &commit,
                &[write(b"a", 6, Some(&[0, 255])), write(b"b", 7, None)],
                std::slice::from_ref(&addition),
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 1,
                }],
            )
            .unwrap(),
            history(
                &scope,
                &prior,
                7,
                &commit,
                &[write(b"a", 7, Some(&[0, 254])), write(b"b", 7, None)],
                std::slice::from_ref(&addition),
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 1,
                }],
            )
            .unwrap(),
            history(
                &scope,
                &prior,
                7,
                &commit,
                &[write(b"a", 7, Some(&[0, 255])), write(b"b", 7, Some(&[]))],
                std::slice::from_ref(&addition),
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 1,
                }],
            )
            .unwrap(),
            history(
                &scope,
                &prior,
                7,
                &commit,
                &[write(b"a", 7, Some(&[0, 255])), write(b"b", 7, None)],
                std::slice::from_ref(&addition),
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 2,
                }],
            )
            .unwrap(),
        ] {
            assert_ne!(base, changed);
        }
        let other_scope = StateScope::new("other", "workspace", "catalog");
        let other_prior = genesis(&other_scope);
        let other_commit = commit_id(&other_scope, &other_prior, 7, &operation()).unwrap();
        let other_addition = ProjectionIntentV2::new(
            "intent-0",
            "audit",
            other_scope.clone(),
            7,
            other_commit.clone(),
            0,
            [0, 255],
        )
        .unwrap();
        assert_ne!(
            base,
            history(
                &other_scope,
                &other_prior,
                7,
                &other_commit,
                &[write(b"a", 7, Some(&[0, 255])), write(b"b", 7, None)],
                &[other_addition],
                &[Trim {
                    record_id: "intent-old".to_string(),
                    origin_sequence: 3,
                    ordinal: 1,
                }],
            )
            .unwrap()
        );
    }

    #[test]
    fn synthetic_genesis_stream_has_independent_golden_and_rejects_unordered_input() {
        let scope = scope();
        let first = ProjectionIntentV2::new(
            "intent-1",
            "audit",
            scope.clone(),
            2,
            "22".repeat(32),
            3,
            [1, 255],
        )
        .unwrap();
        let second = ProjectionIntentV2::new(
            "intent-2",
            "audit",
            scope.clone(),
            7,
            "33".repeat(32),
            9,
            [0],
        )
        .unwrap();
        let mut stream = SyntheticGenesisHistory::new(&scope, 7, 2, 2).unwrap();
        stream.push_kv(b"", 1, Some(&[0, 255])).unwrap();
        stream.push_kv(&[255, 0], 7, None).unwrap();
        stream.push_intent(&first).unwrap();
        stream.push_intent(&second).unwrap();
        assert_eq!(
            stream.finish().unwrap(),
            "9ab60dcce695783924d177a83dbc883fe3a0d21af5aec52a1b7ac774e713e595"
        );

        let mut unordered = SyntheticGenesisHistory::new(&scope, 7, 2, 0).unwrap();
        unordered.push_kv(b"b", 1, None).unwrap();
        assert!(unordered.push_kv(b"a", 1, None).is_err());

        let incomplete = SyntheticGenesisHistory::new(&scope, 7, 1, 0).unwrap();
        assert!(incomplete.finish().is_err());

        let mut unordered_delivery = SyntheticGenesisHistory::new(&scope, 7, 0, 2).unwrap();
        unordered_delivery.push_intent(&second).unwrap();
        assert!(unordered_delivery.push_intent(&first).is_err());
    }

    #[test]
    fn v2_envelope_rejects_untrusted_wire_input() {
        let valid = ProjectionIntentV2::new(
            "intent-0",
            "audit",
            scope(),
            7,
            "22".repeat(32),
            0,
            [0, 255],
        )
        .unwrap();
        let bytes = serde_json::to_vec(&valid).unwrap();
        assert_eq!(
            serde_json::from_slice::<ProjectionIntentV2>(&bytes).unwrap(),
            valid
        );
        for field in [
            "contractVersion",
            "intentId",
            "sourceLogicalSequence",
            "logicalCommitId",
            "payload",
        ] {
            let mut wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            match field {
                "contractVersion" => wire[field] = serde_json::json!(1),
                "intentId" => wire[field] = serde_json::json!(""),
                "sourceLogicalSequence" => wire[field] = serde_json::json!(0),
                "logicalCommitId" => wire[field] = serde_json::json!("bad"),
                "payload" => wire[field] = serde_json::json!([]),
                _ => unreachable!(),
            }
            assert!(
                serde_json::from_value::<ProjectionIntentV2>(wire).is_err(),
                "{field}"
            );
        }
        let mut unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown["physicalManifestId"] = serde_json::json!("forbidden");
        assert!(serde_json::from_value::<ProjectionIntentV2>(unknown).is_err());
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn v2_history_hashes_large_opaque_payload_without_a_full_preimage_copy() {
        let scope = scope();
        let prior = genesis(&scope);
        let commit = commit_id(&scope, &prior, 1, &operation()).unwrap();
        let payload = vec![0xa5; 256 * 1024];
        let addition = ProjectionIntentV2::new(
            "intent-large",
            "audit",
            scope.clone(),
            1,
            commit.clone(),
            0,
            &payload,
        )
        .unwrap();
        let allocations = allocation_counter::measure(|| {
            history(&scope, &prior, 1, &commit, &[], &[addition], &[]).unwrap();
        });
        assert!(
            usize::try_from(allocations.bytes_max).unwrap() < payload.len() / 8,
            "large V2 payload was copied into a hash preimage: {allocations:?}"
        );
    }
}
