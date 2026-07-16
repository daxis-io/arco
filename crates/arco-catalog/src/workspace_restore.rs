//! Durable, exact-path roll-forward workspace restore workflow.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arco_core::ScopedStorage;
use arco_core::lock::{DistributedLock, LockGuard};
use arco_core::storage::{WritePrecondition, WriteResult};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use ulid::Ulid;

use crate::error::{CatalogError, Result};
use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
use crate::state_store::{
    PersistedAuthorityKind, PersistedRestoreParticipantPlan, RestoreAttemptIdentity,
    RestoreParticipantInspection, RestoredAuthorityEvidence,
};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL, WorkspaceScope,
};
use crate::workspace_snapshot_service::{
    PreflightCut, RestoreSource, RestoreSourceKind, WorkspaceDomainRegistry,
    WorkspaceSnapshotService,
};

const VERSION: u32 = 1;
const REQUEST_RECORD_TYPE: &str = "workspace_restore_request";

fn validation(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

fn validate_restore_id(value: &str) -> Result<()> {
    let Some(ulid) = value.strip_prefix("rst_") else {
        return Err(validation("restore_id must start with rst_"));
    };
    if ulid.len() != 26 {
        return Err(validation(
            "restore_id must contain exactly one 26-character ULID",
        ));
    }
    let parsed =
        Ulid::from_string(ulid).map_err(|_| validation("restore_id must contain a valid ULID"))?;
    if parsed.to_string() != ulid {
        return Err(validation(
            "restore_id must use the canonical uppercase ULID spelling",
        ));
    }
    Ok(())
}

/// Returns the immutable restore-request path.
///
/// # Errors
///
/// Returns a validation error for a malformed restore ID.
pub fn restore_request_path(restore_id: &str) -> Result<String> {
    validate_restore_id(restore_id)?;
    Ok(format!("transactions/restores/{restore_id}/request.json"))
}

/// Returns one immutable restore-attempt plan path.
///
/// # Errors
///
/// Returns a validation error for a malformed restore ID or zero attempt.
pub fn restore_attempt_plan_path(restore_id: &str, attempt: u64) -> Result<String> {
    validate_restore_id(restore_id)?;
    if attempt == 0 {
        return Err(validation("restore attempt must be positive"));
    }
    Ok(format!(
        "transactions/restores/{restore_id}/attempts/{attempt:020}.plan.json"
    ))
}

/// Returns the mutable restore-journal path.
///
/// # Errors
///
/// Returns a validation error for a malformed restore ID.
pub fn restore_journal_path(restore_id: &str) -> Result<String> {
    validate_restore_id(restore_id)?;
    Ok(format!("transactions/restores/{restore_id}/journal.json"))
}

/// Returns the immutable opt-in restore read-manifest path.
///
/// # Errors
///
/// Returns a validation error for a malformed restore ID.
pub fn restore_read_manifest_path(restore_id: &str) -> Result<String> {
    validate_restore_id(restore_id)?;
    Ok(format!(
        "transactions/restores/{restore_id}/read.manifest.json"
    ))
}

/// Explicit behavior for configured domains omitted from a workspace source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmittedDomainPolicy {
    /// Leave configured domains absent from the source untouched and record their names.
    Omit,
    /// Reject any configured/source domain-set mismatch.
    Reject,
}

/// Restore scope selected by the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RestoreOperationTarget {
    /// Restore exactly one named domain.
    Domain {
        /// Canonical state-store domain name.
        domain: String,
    },
    /// Restore a workspace cut with an explicit omission policy.
    Workspace {
        /// Required behavior for configured domains absent from the source.
        omitted_domain_policy: OmittedDomainPolicy,
    },
}

impl RestoreOperationTarget {
    /// Creates a domain-only restore target.
    #[must_use]
    pub fn domain(domain: impl Into<String>) -> Self {
        Self::Domain {
            domain: domain.into(),
        }
    }

    /// Creates a workspace restore target with an explicit omission policy.
    #[must_use]
    pub const fn workspace(omitted_domain_policy: OmittedDomainPolicy) -> Self {
        Self::Workspace {
            omitted_domain_policy,
        }
    }

    fn validate(&self) -> Result<()> {
        if let Self::Domain { domain } = self
            && !is_path_safe_component(domain)
        {
            return Err(validation(
                "restore target domain must be one nonblank path-safe component",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RestoreRecordKind {
    Snapshot,
    Export,
}

/// Immutable retry identity for a roll-forward restore request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRestoreRequestRecord {
    record_type: String,
    version: u32,
    restore_id: String,
    source_kind: RestoreRecordKind,
    source_id: String,
    source_pin_id: String,
    scope: WorkspaceScope,
    requested_at: DateTime<Utc>,
    target: RestoreOperationTarget,
}

impl WorkspaceRestoreRequestRecord {
    /// Creates and validates immutable restore request semantics.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identity, scope, source, or target.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        restore_id: impl Into<String>,
        source: RestoreSource,
        scope: WorkspaceScope,
        requested_at: DateTime<Utc>,
        target: RestoreOperationTarget,
    ) -> Result<Self> {
        let record = Self {
            record_type: REQUEST_RECORD_TYPE.to_string(),
            version: VERSION,
            restore_id: restore_id.into(),
            source_kind: match source.kind() {
                RestoreSourceKind::Snapshot => RestoreRecordKind::Snapshot,
                RestoreSourceKind::Export => RestoreRecordKind::Export,
            },
            source_id: source.id().to_string(),
            source_pin_id: source.pin_id().to_string(),
            scope,
            requested_at,
            target,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<()> {
        if self.record_type != REQUEST_RECORD_TYPE || self.version != VERSION {
            return Err(validation("unsupported workspace restore request envelope"));
        }
        validate_restore_id(&self.restore_id)?;
        self.scope.validate()?;
        self.target.validate()?;
        match self.source_kind {
            RestoreRecordKind::Snapshot => {
                RestoreSource::snapshot(&self.source_id, &self.source_pin_id)?;
            }
            RestoreRecordKind::Export => {
                RestoreSource::export(&self.source_id, &self.source_pin_id)?;
            }
        }
        Ok(())
    }

    /// Returns the canonical restore identifier.
    #[must_use]
    pub fn restore_id(&self) -> &str {
        &self.restore_id
    }
}

/// Encodes a canonical immutable restore request.
///
/// # Errors
///
/// Returns an error if validation or canonical serialization fails.
pub fn encode_workspace_restore_request(record: &WorkspaceRestoreRequestRecord) -> Result<Vec<u8>> {
    record.validate()?;
    serde_jcs::to_vec(record).map_err(|error| CatalogError::Serialization {
        message: format!("failed to serialize workspace restore request: {error}"),
    })
}

/// Decodes and validates an immutable restore request.
///
/// # Errors
///
/// Returns an error for malformed JSON, an unsupported envelope, or invalid fields.
pub fn decode_workspace_restore_request(bytes: &[u8]) -> Result<WorkspaceRestoreRequestRecord> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| CatalogError::Serialization {
            message: format!("failed to deserialize workspace restore request: {error}"),
        })?;
    if value.get("record_type").and_then(Value::as_str) != Some(REQUEST_RECORD_TYPE)
        || value.get("version").and_then(Value::as_u64) != Some(u64::from(VERSION))
    {
        return Err(validation("unsupported workspace restore request envelope"));
    }
    let record: WorkspaceRestoreRequestRecord =
        serde_json::from_value(value).map_err(|error| CatalogError::Serialization {
            message: format!("failed to deserialize workspace restore request: {error}"),
        })?;
    record.validate()?;
    Ok(record)
}

impl WorkspaceRestoreRequestRecord {
    fn source(&self) -> Result<RestoreSource> {
        match self.source_kind {
            RestoreRecordKind::Snapshot => {
                RestoreSource::snapshot(&self.source_id, &self.source_pin_id)
            }
            RestoreRecordKind::Export => {
                RestoreSource::export(&self.source_id, &self.source_pin_id)
            }
        }
    }

    fn scope(&self) -> &WorkspaceScope {
        &self.scope
    }
}

/// Caller request for a workspace-wide roll-forward restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreWorkspaceToSnapshot {
    record: WorkspaceRestoreRequestRecord,
}

impl RestoreWorkspaceToSnapshot {
    /// Creates a validated workspace restore request.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identity, source, scope, or policy.
    pub fn new(
        restore_id: impl Into<String>,
        source: RestoreSource,
        scope: WorkspaceScope,
        requested_at: DateTime<Utc>,
        omitted_domain_policy: OmittedDomainPolicy,
    ) -> Result<Self> {
        Ok(Self {
            record: WorkspaceRestoreRequestRecord::new(
                restore_id,
                source,
                scope,
                requested_at,
                RestoreOperationTarget::workspace(omitted_domain_policy),
            )?,
        })
    }
}

/// Caller request for one domain roll-forward restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreDomainToSnapshot {
    record: WorkspaceRestoreRequestRecord,
}

impl RestoreDomainToSnapshot {
    /// Creates a validated single-domain restore request.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identity, source, scope, or domain.
    pub fn new(
        restore_id: impl Into<String>,
        source: RestoreSource,
        scope: WorkspaceScope,
        requested_at: DateTime<Utc>,
        domain: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            record: WorkspaceRestoreRequestRecord::new(
                restore_id,
                source,
                scope,
                requested_at,
                RestoreOperationTarget::domain(domain),
            )?,
        })
    }
}

/// Durable workspace restore lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkspaceRestoreStatus {
    /// Durable metadata exists and no participant is being applied.
    Prepared,
    /// The active aggregate may be applied by recovery helpers.
    Applying,
    /// At least one participant needs deterministic repair.
    RepairRequired,
    /// Every participant is visible and final-manifest bytes are frozen.
    Finalizing,
    /// The immutable opt-in read manifest is durable.
    Visible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RestoreFailureCategory {
    CasLost,
    ParticipantFailed,
    StorageUncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RestoreParticipantPlanRecord {
    domain: String,
    participant_attempt: u64,
    plan_sha256: String,
    plan_wire: Value,
    plan: PersistedRestoreParticipantPlan,
}

#[derive(Serialize)]
struct RestoreParticipantPlanRecordRef<'a> {
    domain: &'a str,
    participant_attempt: u64,
    plan_sha256: &'a str,
    plan: &'a Value,
}

#[derive(Deserialize)]
struct RestoreParticipantPlanRecordWire {
    domain: String,
    participant_attempt: u64,
    plan_sha256: String,
    plan: Value,
}

impl Serialize for RestoreParticipantPlanRecord {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        RestoreParticipantPlanRecordRef {
            domain: &self.domain,
            participant_attempt: self.participant_attempt,
            plan_sha256: &self.plan_sha256,
            plan: &self.plan_wire,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RestoreParticipantPlanRecord {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RestoreParticipantPlanRecordWire::deserialize(deserializer)?;
        let plan = serde_json::from_value(wire.plan.clone()).map_err(serde::de::Error::custom)?;
        Ok(Self {
            domain: wire.domain,
            participant_attempt: wire.participant_attempt,
            plan_sha256: wire.plan_sha256,
            plan_wire: wire.plan,
            plan,
        })
    }
}

impl RestoreParticipantPlanRecord {
    fn new(
        domain: impl Into<String>,
        participant_attempt: u64,
        plan: PersistedRestoreParticipantPlan,
    ) -> Result<Self> {
        let plan_wire =
            serde_json::to_value(&plan).map_err(|error| CatalogError::Serialization {
                message: format!("failed to serialize restore participant plan: {error}"),
            })?;
        let bytes = canonical_bytes(&plan_wire, "restore participant plan")?;
        let record = Self {
            domain: domain.into(),
            participant_attempt,
            plan_sha256: prefixed_sha256(&bytes),
            plan_wire,
            plan,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<()> {
        validate_domain(&self.domain)?;
        if self.participant_attempt == 0 {
            return Err(validation("restore participant attempt must be positive"));
        }
        let bytes = canonical_bytes(&self.plan_wire, "restore participant plan")?;
        if prefixed_sha256(&bytes) != self.plan_sha256 {
            return Err(validation("restore participant plan checksum mismatch"));
        }
        let decoded: PersistedRestoreParticipantPlan =
            serde_json::from_value(self.plan_wire.clone()).map_err(|error| {
                validation(format!("restore participant plan is unsupported: {error}"))
            })?;
        if decoded != self.plan {
            return Err(validation(
                "typed restore participant plan does not match retained wire plan",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkspaceRestoreAttemptPlan {
    record_type: String,
    version: u32,
    restore_id: String,
    aggregate_attempt: u64,
    scope: WorkspaceScope,
    request_sha256: String,
    source_record_sha256: String,
    active_retention_deadline: DateTime<Utc>,
    participants: Vec<RestoreParticipantPlanRecord>,
    omitted_domains: Vec<String>,
}

impl WorkspaceRestoreAttemptPlan {
    fn validate(&self) -> Result<()> {
        if self.record_type != "workspace_restore_attempt"
            || self.version != VERSION
            || self.aggregate_attempt == 0
        {
            return Err(validation("unsupported workspace restore attempt"));
        }
        validate_restore_id(&self.restore_id)?;
        self.scope.validate()?;
        validate_prefixed_sha256(&self.request_sha256)?;
        validate_prefixed_sha256(&self.source_record_sha256)?;
        if self.participants.is_empty() {
            return Err(validation(
                "restore attempt requires at least one participant",
            ));
        }
        validate_ordered_participant_plans(
            &self.participants,
            &self.restore_id,
            self.aggregate_attempt,
        )?;
        validate_ordered_domains(&self.omitted_domains)?;
        if self.participants.iter().any(|participant| {
            self.omitted_domains
                .binary_search(&participant.domain)
                .is_ok()
        }) {
            return Err(validation(
                "restore attempt participant and omitted sets must be disjoint",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RestoreJournalParticipant {
    domain: String,
    participant_attempt: u64,
    plan_sha256: String,
    evidence: Option<RestoredAuthorityEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkspaceRestoreJournal {
    record_type: String,
    version: u32,
    restore_id: String,
    revision: u64,
    status: WorkspaceRestoreStatus,
    scope: WorkspaceScope,
    request_sha256: String,
    request_path: String,
    aggregate_attempt: u64,
    attempt_path: String,
    attempt_sha256: String,
    required_domains: Vec<String>,
    participants: Vec<RestoreJournalParticipant>,
    omitted_domains: Vec<String>,
    failure_category: Option<RestoreFailureCategory>,
    read_manifest_path: String,
    finalized_at: Option<DateTime<Utc>>,
    read_manifest_sha256: Option<String>,
}

impl WorkspaceRestoreJournal {
    fn validate(&self) -> Result<()> {
        if self.record_type != "workspace_restore_journal"
            || self.version != VERSION
            || self.revision == 0
            || self.aggregate_attempt == 0
        {
            return Err(validation("unsupported workspace restore journal"));
        }
        validate_restore_id(&self.restore_id)?;
        self.scope.validate()?;
        validate_prefixed_sha256(&self.request_sha256)?;
        validate_prefixed_sha256(&self.attempt_sha256)?;
        if self.request_path != restore_request_path(&self.restore_id)? {
            return Err(validation("restore journal request path mismatch"));
        }
        if self.attempt_path != restore_attempt_plan_path(&self.restore_id, self.aggregate_attempt)?
        {
            return Err(validation("restore journal attempt path mismatch"));
        }
        let domains = self
            .participants
            .iter()
            .map(|participant| participant.domain.clone())
            .collect::<Vec<_>>();
        if domains.is_empty() {
            return Err(validation(
                "restore journal requires at least one participant",
            ));
        }
        validate_ordered_domains(&domains)?;
        validate_ordered_domains(&self.required_domains)?;
        if domains != self.required_domains {
            return Err(validation(
                "restore journal required domains do not match participants",
            ));
        }
        if self.read_manifest_path != restore_read_manifest_path(&self.restore_id)? {
            return Err(validation("restore journal read manifest path mismatch"));
        }
        for participant in &self.participants {
            if participant.participant_attempt == 0
                || participant.participant_attempt > self.aggregate_attempt
            {
                return Err(validation("restore journal participant attempt mismatch"));
            }
            validate_prefixed_sha256(&participant.plan_sha256)?;
            if let Some(evidence) = &participant.evidence
                && (evidence.validate().is_err()
                    || evidence.participant_attempt() != participant.participant_attempt
                    || evidence.scope().tenant_id() != self.scope.tenant_id()
                    || evidence.scope().workspace_id() != self.scope.workspace_id()
                    || evidence.scope().domain() != participant.domain)
            {
                return Err(validation("restore journal participant evidence mismatch"));
            }
        }
        validate_ordered_domains(&self.omitted_domains)?;
        if self
            .required_domains
            .iter()
            .any(|domain| self.omitted_domains.binary_search(domain).is_ok())
        {
            return Err(validation(
                "restore required and omitted domain sets must be disjoint",
            ));
        }
        if (self.status == WorkspaceRestoreStatus::RepairRequired)
            != self.failure_category.is_some()
        {
            return Err(validation(
                "restore journal failure category does not match lifecycle",
            ));
        }
        if let Some(digest) = &self.read_manifest_sha256 {
            validate_prefixed_sha256(digest)?;
        }
        match self.status {
            WorkspaceRestoreStatus::Finalizing | WorkspaceRestoreStatus::Visible => {
                if self
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_none())
                    || self.finalized_at.is_none()
                    || self.read_manifest_sha256.is_none()
                {
                    return Err(validation("final restore journal is incomplete"));
                }
            }
            _ => {
                if self.finalized_at.is_some() || self.read_manifest_sha256.is_some() {
                    return Err(validation(
                        "nonfinal restore journal has finalization fields",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// One stable participant entry in the opt-in read manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRestoreReadParticipant {
    domain: String,
    evidence: RestoredAuthorityEvidence,
}

impl WorkspaceRestoreReadParticipant {
    /// Returns the canonical participant domain.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the stable visible-authority evidence for this participant.
    #[must_use]
    pub const fn evidence(&self) -> &RestoredAuthorityEvidence {
        &self.evidence
    }
}

/// Immutable opt-in read cut produced only after every participant is visible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRestoreReadManifest {
    record_type: String,
    version: u32,
    restore_id: String,
    source_kind: RestoreRecordKind,
    source_id: String,
    source_pin_id: String,
    scope: WorkspaceScope,
    request_sha256: String,
    finalized_at: DateTime<Utc>,
    publication_mode: String,
    participants: Vec<WorkspaceRestoreReadParticipant>,
    omitted_domains: Vec<String>,
}

impl WorkspaceRestoreReadManifest {
    fn validate(&self) -> Result<()> {
        if self.record_type != "workspace_restore_read_manifest"
            || self.version != VERSION
            || self.publication_mode != "sequential_repairable"
        {
            return Err(validation("unsupported workspace restore read manifest"));
        }
        validate_restore_id(&self.restore_id)?;
        self.scope.validate()?;
        match self.source_kind {
            RestoreRecordKind::Snapshot => {
                RestoreSource::snapshot(&self.source_id, &self.source_pin_id)?;
            }
            RestoreRecordKind::Export => {
                RestoreSource::export(&self.source_id, &self.source_pin_id)?;
            }
        }
        validate_prefixed_sha256(&self.request_sha256)?;
        let domains = self
            .participants
            .iter()
            .map(|participant| participant.domain.clone())
            .collect::<Vec<_>>();
        if domains.is_empty() {
            return Err(validation(
                "restore read manifest requires at least one participant",
            ));
        }
        validate_ordered_domains(&domains)?;
        for participant in &self.participants {
            participant.evidence.validate()?;
            if participant.evidence.scope().tenant_id() != self.scope.tenant_id()
                || participant.evidence.scope().workspace_id() != self.scope.workspace_id()
                || participant.evidence.scope().domain() != participant.domain
            {
                return Err(validation(
                    "restore read-manifest participant evidence scope mismatch",
                ));
            }
        }
        validate_ordered_domains(&self.omitted_domains)?;
        if domains
            .iter()
            .any(|domain| self.omitted_domains.binary_search(domain).is_ok())
        {
            return Err(validation(
                "restore read-manifest participant and omitted sets must be disjoint",
            ));
        }
        Ok(())
    }

    /// Returns the caller-supplied restore identifier.
    #[must_use]
    pub fn restore_id(&self) -> &str {
        &self.restore_id
    }

    /// Reconstructs the direct-addressed source identity and retention pin.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialized source identity is malformed.
    pub fn source(&self) -> Result<RestoreSource> {
        match self.source_kind {
            RestoreRecordKind::Snapshot => {
                RestoreSource::snapshot(&self.source_id, &self.source_pin_id)
            }
            RestoreRecordKind::Export => {
                RestoreSource::export(&self.source_id, &self.source_pin_id)
            }
        }
    }

    /// Returns the workspace scope repeated by the final manifest.
    #[must_use]
    pub const fn scope(&self) -> &WorkspaceScope {
        &self.scope
    }

    /// Returns the immutable request byte digest bound to this manifest.
    #[must_use]
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    /// Returns the timestamp frozen before immutable manifest publication.
    #[must_use]
    pub const fn finalized_at(&self) -> DateTime<Utc> {
        self.finalized_at
    }

    /// Returns the explicit publication/repair model.
    #[must_use]
    pub fn publication_mode(&self) -> &str {
        &self.publication_mode
    }

    /// Returns canonical visible participants.
    #[must_use]
    pub fn participants(&self) -> &[WorkspaceRestoreReadParticipant] {
        &self.participants
    }

    /// Returns canonical domains explicitly omitted from this read cut.
    #[must_use]
    pub fn omitted_domains(&self) -> &[String] {
        &self.omitted_domains
    }
}

/// Safe result of a restore or recovery call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRestoreOutcome {
    status: WorkspaceRestoreStatus,
    completed_domains: Vec<String>,
    pending_domains: Vec<String>,
    omitted_domains: Vec<String>,
    read_manifest: Option<WorkspaceRestoreReadManifest>,
}

impl WorkspaceRestoreOutcome {
    /// Returns the durable lifecycle status.
    #[must_use]
    pub const fn status(&self) -> WorkspaceRestoreStatus {
        self.status
    }

    /// Returns canonical completed domain names.
    #[must_use]
    pub fn completed_domains(&self) -> &[String] {
        &self.completed_domains
    }

    /// Returns canonical pending domain names.
    #[must_use]
    pub fn pending_domains(&self) -> &[String] {
        &self.pending_domains
    }

    /// Returns canonical explicitly omitted domain names.
    #[must_use]
    pub fn omitted_domains(&self) -> &[String] {
        &self.omitted_domains
    }

    /// Returns the final opt-in read manifest when visible.
    #[must_use]
    pub const fn read_manifest(&self) -> Option<&WorkspaceRestoreReadManifest> {
        self.read_manifest.as_ref()
    }
}

/// Direct-addressed durable roll-forward restore module for one workspace.
pub struct WorkspaceRestoreService {
    storage: ScopedStorage,
    snapshots: WorkspaceSnapshotService,
}

impl WorkspaceRestoreService {
    /// Creates a restore module with the same workspace scope as its registry.
    ///
    /// # Errors
    ///
    /// Returns an error when storage and registry scopes disagree.
    pub fn new(storage: ScopedStorage, registry: WorkspaceDomainRegistry) -> Result<Self> {
        let snapshots = WorkspaceSnapshotService::new(storage.clone(), registry)?;
        Ok(Self { storage, snapshots })
    }

    /// Restores every source domain using an explicit omission policy.
    ///
    /// # Errors
    ///
    /// Returns an error before mutation for failed source/participant preflight.
    pub async fn restore_workspace_to_snapshot(
        &self,
        request: &RestoreWorkspaceToSnapshot,
    ) -> Result<WorkspaceRestoreOutcome> {
        self.restore_with_terminal_winner_adoption(&request.record)
            .await
    }

    /// Restores exactly one source domain.
    ///
    /// # Errors
    ///
    /// Returns an error before mutation for failed source/participant preflight.
    pub async fn restore_domain_to_snapshot(
        &self,
        request: &RestoreDomainToSnapshot,
    ) -> Result<WorkspaceRestoreOutcome> {
        self.restore_with_terminal_winner_adoption(&request.record)
            .await
    }

    /// Resumes a nonterminal restore by exact ID without listing.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, missing, corrupt, or unrecoverable records.
    pub async fn recover_restore(&self, restore_id: &str) -> Result<WorkspaceRestoreOutcome> {
        let request_bytes = self
            .storage
            .get_raw(&restore_request_path(restore_id)?)
            .await?;
        let request = decode_workspace_restore_request(&request_bytes)?;
        if request.restore_id() != restore_id {
            return Err(validation(
                "restore request identity does not match its exact path",
            ));
        }
        Box::pin(self.restore(&request)).await
    }

    /// Reads current restore state by exact ID without mutation.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, missing, or corrupt records.
    pub async fn get_restore(&self, restore_id: &str) -> Result<WorkspaceRestoreOutcome> {
        let journal = self.load_journal(restore_id).await?.0;
        self.outcome(&journal).await
    }

    async fn restore_with_terminal_winner_adoption(
        &self,
        request: &WorkspaceRestoreRequestRecord,
    ) -> Result<WorkspaceRestoreOutcome> {
        match Box::pin(self.restore(request)).await {
            Ok(outcome) => Ok(outcome),
            Err(original_error) => {
                let Some((journal, _version)) =
                    self.load_optional_journal(request.restore_id()).await?
                else {
                    return Err(original_error);
                };
                if journal.status != WorkspaceRestoreStatus::Visible {
                    return Err(original_error);
                }
                // Re-enter the normal terminal path once. It revalidates immutable
                // request identity, selected attempt and receipts, and settles an
                // exact matching retention epoch if the winner crashed after visibility.
                Box::pin(self.restore(request)).await
            }
        }
    }

    async fn acquire_apply_coordination(
        &self,
        restore_id: &str,
        participant_attempt: u64,
        domain: &str,
        plan_sha256: &str,
    ) -> Result<(LockGuard<ScopedStorage>, RetentionMutationEpoch)> {
        let operation_id =
            restore_apply_operation_id(restore_id, participant_attempt, domain, plan_sha256);
        let mut guard =
            DistributedLock::new(Arc::new(self.storage.clone()), RETENTION_GC_LOCK_PATH)
                .acquire_with_operation(
                    RETENTION_GC_LOCK_TTL,
                    RETENTION_GC_LOCK_MAX_RETRIES,
                    Some("workspace-restore-apply".to_string()),
                )
                .await
                .map_err(CatalogError::from)?;
        match RetentionMutationEpoch::claim(
            self.storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceRestoreApply,
            operation_id,
        )
        .await
        {
            Ok(epoch) => Ok((guard, epoch)),
            Err(error) => {
                let _ = guard.release().await;
                Err(error)
            }
        }
    }

    async fn finish_apply_coordination<T>(
        guard: LockGuard<ScopedStorage>,
        epoch: RetentionMutationEpoch,
        operation: Result<T>,
    ) -> Result<T> {
        let settlement = epoch.settle().await;
        let release = guard.release().await.map_err(CatalogError::from);
        match (operation, settlement, release) {
            (Ok(value), Ok(()), Ok(())) => Ok(value),
            (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        }
    }

    async fn settle_terminal_apply_coordination(
        &self,
        terminal_operation_ids: &BTreeSet<String>,
    ) -> Result<()> {
        if terminal_operation_ids.is_empty() {
            return Ok(());
        }
        if !self
            .terminal_apply_coordination_is_in_flight(terminal_operation_ids)
            .await?
        {
            return Ok(());
        }
        let mut guard =
            DistributedLock::new(Arc::new(self.storage.clone()), RETENTION_GC_LOCK_PATH)
                .acquire_with_operation(
                    RETENTION_GC_LOCK_TTL,
                    RETENTION_GC_LOCK_MAX_RETRIES,
                    Some("workspace-restore-recovery".to_string()),
                )
                .await
                .map_err(CatalogError::from)?;
        let settlement = RetentionMutationEpoch::settle_terminal_matching(
            self.storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceRestoreApply,
            terminal_operation_ids,
        )
        .await;
        let release = guard.release().await.map_err(CatalogError::from);
        match (settlement, release) {
            (Ok(_), Ok(())) => Ok(()),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn terminal_apply_coordination_is_in_flight(
        &self,
        terminal_operation_ids: &BTreeSet<String>,
    ) -> Result<bool> {
        RetentionMutationEpoch::terminal_match_is_in_flight(
            &self.storage,
            RetentionMutationKind::WorkspaceRestoreApply,
            terminal_operation_ids,
        )
        .await
    }

    async fn durable_receipt_operation_ids(
        &self,
        attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
    ) -> Result<BTreeSet<String>> {
        let mut operation_ids = BTreeSet::new();
        for recorded in journal
            .participants
            .iter()
            .filter(|participant| participant.evidence.is_some())
        {
            let participant = if let Some(participant) = attempt
                .participants
                .iter()
                .find(|participant| participant.domain == recorded.domain)
            {
                participant.clone()
            } else {
                self.load_origin_participant_plan(attempt, journal, recorded)
                    .await?
            };
            if participant.participant_attempt != recorded.participant_attempt
                || participant.plan_sha256 != recorded.plan_sha256
            {
                return Err(validation(
                    "durable receipt does not match selected participant plan",
                ));
            }
            operation_ids.insert(restore_apply_operation_id(
                &journal.restore_id,
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256,
            ));
        }
        Ok(operation_ids)
    }

    async fn settle_after_direct_visible_adoption(
        &self,
        journal: &WorkspaceRestoreJournal,
        participant: &RestoreParticipantPlanRecord,
        expected_evidence: &RestoredAuthorityEvidence,
    ) -> Result<()> {
        let recorded = journal
            .participants
            .iter()
            .find(|recorded| recorded.domain == participant.domain)
            .ok_or_else(|| validation("visible receipt winner omits participant"))?;
        if recorded.participant_attempt != participant.participant_attempt
            || recorded.plan_sha256 != participant.plan_sha256
            || recorded.evidence.as_ref() != Some(expected_evidence)
        {
            return Err(validation(
                "visible receipt winner does not contain the exact adopted evidence",
            ));
        }
        let selected_attempt = self.load_selected_attempt(journal).await?;
        self.validate_recorded_receipts(&selected_attempt, journal)
            .await?;
        let terminal_operation_ids = self
            .durable_receipt_operation_ids(&selected_attempt, journal)
            .await?;
        self.settle_terminal_apply_coordination(&terminal_operation_ids)
            .await
    }

    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn restore(
        &self,
        request: &WorkspaceRestoreRequestRecord,
    ) -> Result<WorkspaceRestoreOutcome> {
        request.validate()?;
        let request_bytes = encode_workspace_restore_request(request)?;
        let request_sha256 = prefixed_sha256(&request_bytes);
        let journal_path = restore_journal_path(request.restore_id())?;
        let mut applying_before_preflight = None;

        if let Some((journal, version)) = self.load_optional_journal(request.restore_id()).await? {
            let durable_request_bytes = self.storage.get_raw(&journal.request_path).await?;
            if prefixed_sha256(&durable_request_bytes) != journal.request_sha256 {
                return Err(validation(
                    "durable restore request does not match journal checksum",
                ));
            }
            let durable_request = decode_workspace_restore_request(&durable_request_bytes)?;
            if &durable_request != request {
                return Err(precondition_failed(
                    "restore ID already names different immutable request semantics",
                ));
            }
            if journal.status == WorkspaceRestoreStatus::Visible {
                let attempt = self.load_selected_attempt(&journal).await?;
                let terminal_operation_ids = self
                    .durable_receipt_operation_ids(&attempt, &journal)
                    .await?;
                if self
                    .terminal_apply_coordination_is_in_flight(&terminal_operation_ids)
                    .await?
                {
                    self.validate_completed_receipts(&attempt, &journal).await?;
                    self.settle_terminal_apply_coordination(&terminal_operation_ids)
                        .await?;
                }
                return self.outcome(&journal).await;
            }
            if journal.status == WorkspaceRestoreStatus::Applying
                && journal
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_some())
                && journal
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_none())
            {
                // A strict visible subset is already a repair condition. Persist that
                // fact before consulting the retained source or configured adapters:
                // either may disappear after the first participant became visible.
                // The selected immutable attempt is still loaded here so malformed or
                // cross-bound durable state cannot be blessed as repairable.
                let attempt = self.load_selected_attempt(&journal).await?;
                self.validate_recorded_receipts(&attempt, &journal).await?;
                let mut repair = journal;
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut repair)?;
                let (winner, _) = self.cas_journal(&repair, &version).await?;
                return self.outcome(&winner).await;
            }
            if matches!(
                journal.status,
                WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
            ) && journal
                .participants
                .iter()
                .any(|participant| participant.evidence.is_none())
                && let Some(outcome) = self
                    .reconcile_unrecorded_applying(request, journal.clone(), version.clone())
                    .await?
            {
                return Ok(outcome);
            }
            if journal
                .participants
                .iter()
                .all(|participant| participant.evidence.is_some())
            {
                let attempt = self.load_selected_attempt(&journal).await?;
                self.validate_completed_receipts(&attempt, &journal).await?;
                let terminal_operation_ids = self
                    .durable_receipt_operation_ids(&attempt, &journal)
                    .await?;
                self.settle_terminal_apply_coordination(&terminal_operation_ids)
                    .await?;
                return self
                    .resume_attempt(request, attempt, journal, version)
                    .await;
            }
            applying_before_preflight = Some((journal, version));
            // Full source/participant preflight happens below before mutation.
        }

        let source = request.source()?;
        let now = Utc::now();
        let preflight = async {
            let immutable = self
                .snapshots
                .immutable_restore_cut(&source, request.scope())
                .await?;
            let (required_domains, omitted_domains) =
                self.resolve_domains(request, &immutable.domains)?;
            let cut = self
                .snapshots
                .validated_restore_cut_for_domains(&source, request.scope(), &required_domains, now)
                .await?;
            Ok::<_, CatalogError>((cut, required_domains, omitted_domains))
        }
        .await;
        let (cut, required_domains, omitted_domains) = match preflight {
            Ok(preflight) => preflight,
            Err(error) => {
                if let Some((journal, version)) = applying_before_preflight {
                    return self
                        .persist_repair_required(journal, &version, safe_failure_category(&error))
                        .await;
                }
                return Err(error);
            }
        };

        let existing = self.load_optional_journal(request.restore_id()).await?;
        let (attempt, mut journal, journal_version) = if let Some((existing_journal, version)) =
            existing
        {
            let attempt_bytes = self.storage.get_raw(&existing_journal.attempt_path).await?;
            if prefixed_sha256(&attempt_bytes) != existing_journal.attempt_sha256 {
                return Err(validation("restore attempt checksum mismatch"));
            }
            let attempt: WorkspaceRestoreAttemptPlan =
                decode_record(&attempt_bytes, "workspace restore attempt")?;
            attempt.validate()?;
            let inspections = self
                .preflight_existing_attempt(
                    &attempt,
                    &existing_journal,
                    &cut.domains,
                    &cut.source_record_sha256,
                    cut.usable_retention_deadline,
                    &required_domains,
                    &omitted_domains,
                )
                .await?;
            // Adapter inspection is an externally implemented read and may take long
            // enough for the retained source to expire or be released. No journal
            // revision may follow it until the exact retained cut is fenced again.
            self.fence_restore_source(request, &cut, &required_domains, &attempt)
                .await?;
            let replacement_requested = inspections
                .values()
                .any(|inspection| matches!(inspection, RestoreParticipantInspection::Superseded));
            if inspections
                .values()
                .any(|inspection| matches!(inspection, RestoreParticipantInspection::Superseded))
                && existing_journal.status != WorkspaceRestoreStatus::RepairRequired
            {
                let mut repair = existing_journal;
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::CasLost);
                bump_journal_revision(&mut repair)?;
                let (winner, _) = self.cas_journal(&repair, &version).await?;
                return self.outcome(&winner).await;
            }
            let prior_aggregate_attempt = attempt.aggregate_attempt;
            let (attempt, journal, version) = self
                .replace_superseded_participants(
                    request,
                    attempt,
                    existing_journal,
                    version,
                    &cut,
                    inspections,
                    now,
                )
                .await?;
            if replacement_requested
                && (journal.status == WorkspaceRestoreStatus::RepairRequired
                    || attempt.aggregate_attempt == prior_aggregate_attempt)
            {
                return self.outcome(&journal).await;
            }
            (attempt, journal, Some(version))
        } else {
            let request_path = restore_request_path(request.restore_id())?;
            let attempt_path = restore_attempt_plan_path(request.restore_id(), 1)?;
            let orphan_request = self.get_optional_raw(&request_path).await?;
            let orphan_attempt = self.get_optional_raw(&attempt_path).await?;
            let adopting_orphan_attempt = orphan_attempt.is_some();
            if orphan_request.is_none() && orphan_attempt.is_some() {
                return Err(validation(
                    "restore attempt exists without its immutable request",
                ));
            }
            let (request_publication_bytes, request_sha256) = if let Some(bytes) = orphan_request {
                let durable_request = decode_workspace_restore_request(&bytes)?;
                if &durable_request != request {
                    return Err(precondition_failed(
                        "restore ID already names different immutable request semantics",
                    ));
                }
                let digest = prefixed_sha256(&bytes);
                (bytes, digest)
            } else {
                (Bytes::from(request_bytes.clone()), request_sha256.clone())
            };
            let (attempt, attempt_bytes) = if let Some(bytes) = orphan_attempt {
                let attempt: WorkspaceRestoreAttemptPlan =
                    decode_record(&bytes, "orphan workspace restore attempt")?;
                attempt.validate()?;
                let domains = attempt
                    .participants
                    .iter()
                    .map(|participant| participant.domain.clone())
                    .collect::<BTreeSet<_>>();
                if attempt.restore_id != request.restore_id()
                    || attempt.aggregate_attempt != 1
                    || attempt.scope != request.scope
                    || attempt.request_sha256 != request_sha256
                    || attempt.source_record_sha256 != cut.source_record_sha256
                    || cut.usable_retention_deadline < attempt.active_retention_deadline
                    || attempt.omitted_domains != omitted_domains
                    || domains != required_domains
                {
                    return Err(validation(
                        "orphan restore attempt does not match current immutable request and source",
                    ));
                }
                (attempt, bytes)
            } else {
                let attempt = self
                    .plan_initial_attempt(
                        request,
                        &request_sha256,
                        &cut.source_record_sha256,
                        cut.usable_retention_deadline,
                        &cut.domains,
                        &required_domains,
                        &omitted_domains,
                        now,
                    )
                    .await?;
                let bytes = Bytes::from(canonical_bytes(&attempt, "workspace restore attempt")?);
                (attempt, bytes)
            };
            let attempt_sha256 = prefixed_sha256(&attempt_bytes);
            let journal = WorkspaceRestoreJournal {
                record_type: "workspace_restore_journal".to_string(),
                version: VERSION,
                restore_id: request.restore_id().to_string(),
                revision: 1,
                status: WorkspaceRestoreStatus::Prepared,
                scope: request.scope.clone(),
                request_sha256: request_sha256.clone(),
                request_path: restore_request_path(request.restore_id())?,
                aggregate_attempt: 1,
                attempt_path: attempt_path.clone(),
                attempt_sha256,
                required_domains: required_domains.iter().cloned().collect(),
                participants: attempt
                    .participants
                    .iter()
                    .map(|participant| RestoreJournalParticipant {
                        domain: participant.domain.clone(),
                        participant_attempt: participant.participant_attempt,
                        plan_sha256: participant.plan_sha256.clone(),
                        evidence: None,
                    })
                    .collect(),
                omitted_domains: omitted_domains.clone(),
                failure_category: None,
                read_manifest_path: restore_read_manifest_path(request.restore_id())?,
                finalized_at: None,
                read_manifest_sha256: None,
            };
            journal.validate()?;
            let inspections = self
                .preflight_existing_attempt(
                    &attempt,
                    &journal,
                    &cut.domains,
                    &cut.source_record_sha256,
                    cut.usable_retention_deadline,
                    &required_domains,
                    &omitted_domains,
                )
                .await?;
            if !adopting_orphan_attempt
                && (inspections.len() != journal.participants.len()
                    || inspections.values().any(|inspection| {
                        !matches!(inspection, RestoreParticipantInspection::Ready)
                    }))
            {
                return Err(precondition_failed(
                    "unpublished restore attempt is no longer ready",
                ));
            }
            // Participant planning is implementation-owned and may take long
            // enough for the retained cut or its active pin to change. Fence the
            // exact source again after every planner/inspection and immediately
            // before the first immutable restore write.
            self.fence_restore_source(request, &cut, &required_domains, &attempt)
                .await?;
            if self
                .load_optional_journal(request.restore_id())
                .await?
                .is_some()
            {
                return Err(CatalogError::CasFailed {
                    message: "restore journal appeared during unpublished attempt preflight"
                        .to_string(),
                });
            }
            // Every participant has been planned successfully before the first write.
            put_immutable_exact(&self.storage, &request_path, request_publication_bytes).await?;
            put_immutable_exact(&self.storage, &attempt_path, attempt_bytes).await?;
            let journal_bytes = canonical_bytes(&journal, "workspace restore journal")?;
            let journal_write = self
                .storage
                .put_raw(
                    &journal_path,
                    Bytes::from(journal_bytes),
                    WritePrecondition::DoesNotExist,
                )
                .await;
            let (selected, version) = match journal_write {
                Ok(WriteResult::Success { .. } | WriteResult::PreconditionFailed { .. }) => {
                    self.load_journal(request.restore_id()).await?
                }
                Err(write_error) => match self.load_journal(request.restore_id()).await {
                    Ok(selected) => selected,
                    Err(CatalogError::NotFound { .. }) => return Err(write_error.into()),
                    Err(read_error) => return Err(read_error),
                },
            };
            if !journal_winner_is_compatible(&journal, &selected) {
                return Err(precondition_failed("conflicting restore journal winner"));
            }
            if selected.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&selected).await;
            }
            let mut selected_attempt = self.load_selected_attempt(&selected).await?;
            let (selected, version) = if adopting_orphan_attempt
                && selected.status == WorkspaceRestoreStatus::Prepared
                && selected.aggregate_attempt == 1
                && selected.attempt_sha256 == journal.attempt_sha256
            {
                self.reconcile_adopted_orphan_attempt(
                    request,
                    &cut,
                    &selected_attempt,
                    selected,
                    version,
                )
                .await?
            } else {
                (selected, version)
            };
            if selected.status == WorkspaceRestoreStatus::RepairRequired {
                return self.outcome(&selected).await;
            }
            if selected_attempt.aggregate_attempt != selected.aggregate_attempt {
                selected_attempt = self.load_selected_attempt(&selected).await?;
            }
            (selected_attempt, selected, Some(version))
        };

        let version =
            journal_version.ok_or_else(|| validation("restore journal version missing"))?;
        if journal.status == WorkspaceRestoreStatus::Prepared
            || journal.status == WorkspaceRestoreStatus::RepairRequired
        {
            let selected_domains = journal.required_domains.iter().cloned().collect();
            self.fence_restore_source(request, &cut, &selected_domains, &attempt)
                .await?;
            journal.status = WorkspaceRestoreStatus::Applying;
            journal.failure_category = None;
            bump_journal_revision(&mut journal)?;
            let (winner, winner_version) = self.cas_journal(&journal, &version).await?;
            if winner.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&winner).await;
            }
            if winner.status != WorkspaceRestoreStatus::Applying
                || winner.aggregate_attempt != attempt.aggregate_attempt
                || winner.attempt_sha256 != journal.attempt_sha256
            {
                return self.outcome(&winner).await;
            }
            journal = winner;
            return self
                .resume_attempt(request, attempt, journal, winner_version)
                .await;
        }
        self.resume_attempt(request, attempt, journal, version)
            .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn replace_superseded_participants(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: WorkspaceRestoreAttemptPlan,
        journal: WorkspaceRestoreJournal,
        version: String,
        source_cut: &PreflightCut,
        inspections: BTreeMap<String, RestoreParticipantInspection>,
        now: DateTime<Utc>,
    ) -> Result<(WorkspaceRestoreAttemptPlan, WorkspaceRestoreJournal, String)> {
        if !inspections
            .values()
            .any(|inspection| matches!(inspection, RestoreParticipantInspection::Superseded))
        {
            return Ok((attempt, journal, version));
        }

        let aggregate_attempt = attempt
            .aggregate_attempt
            .checked_add(1)
            .ok_or_else(|| validation("restore aggregate attempt overflow"))?;
        let replacement_path = restore_attempt_plan_path(request.restore_id(), aggregate_attempt)?;
        let authorities = source_cut
            .domains
            .iter()
            .map(|authority| (authority.domain(), authority))
            .collect::<BTreeMap<_, _>>();
        let orphan = self.get_optional_raw(&replacement_path).await?;
        let adopting_orphan = orphan.is_some();
        let (replacement, replacement_bytes) = if let Some(bytes) = orphan {
            let replacement: WorkspaceRestoreAttemptPlan =
                decode_record(&bytes, "orphan replacement restore attempt")?;
            Self::validate_orphan_replacement(
                &replacement,
                &attempt,
                &journal,
                &authorities,
                source_cut.usable_retention_deadline,
                &inspections,
            )?;
            (replacement, bytes)
        } else {
            let active = attempt
                .participants
                .iter()
                .map(|participant| (participant.domain.as_str(), participant))
                .collect::<BTreeMap<_, _>>();
            let mut participants = Vec::new();
            for recorded in journal
                .participants
                .iter()
                .filter(|participant| participant.evidence.is_none())
            {
                let prior = active
                    .get(recorded.domain.as_str())
                    .ok_or_else(|| validation("active attempt omits unfinished participant"))?;
                let inspection = inspections.get(&recorded.domain).ok_or_else(|| {
                    validation("restore preflight omitted unfinished participant")
                })?;
                let participant = if matches!(inspection, RestoreParticipantInspection::Superseded)
                {
                    let authority = authorities.get(recorded.domain.as_str()).ok_or_else(|| {
                        validation("superseded participant is absent from source")
                    })?;
                    let adapter = self
                        .snapshots
                        .registry()
                        .get(&recorded.domain)
                        .and_then(|binding| binding.restore_participant())
                        .ok_or_else(|| validation("restore participant is not configured"))?;
                    let identity = RestoreAttemptIdentity::new(
                        request.restore_id(),
                        aggregate_attempt,
                        &recorded.domain,
                    )?;
                    RestoreParticipantPlanRecord::new(
                        &recorded.domain,
                        aggregate_attempt,
                        adapter
                            .plan_restore(authority.authority(), &identity, now)
                            .await?,
                    )?
                } else {
                    (*prior).clone()
                };
                participants.push(participant);
            }

            let replacement = WorkspaceRestoreAttemptPlan {
                record_type: "workspace_restore_attempt".to_string(),
                version: VERSION,
                restore_id: attempt.restore_id.clone(),
                aggregate_attempt,
                scope: attempt.scope.clone(),
                request_sha256: attempt.request_sha256.clone(),
                source_record_sha256: attempt.source_record_sha256.clone(),
                active_retention_deadline: source_cut.usable_retention_deadline,
                participants,
                omitted_domains: attempt.omitted_domains.clone(),
            };
            replacement.validate()?;
            let bytes = Bytes::from(canonical_bytes(&replacement, "workspace restore attempt")?);
            (replacement, bytes)
        };
        let replacement_sha256 = prefixed_sha256(&replacement_bytes);

        let required_domains = journal.required_domains.iter().cloned().collect();
        if adopting_orphan {
            self.fence_restore_source(request, source_cut, &required_domains, &replacement)
                .await?;
            self.fence_journal_unchanged(&journal, &version).await?;
        } else {
            let candidate = replacement_journal_candidate(
                &journal,
                &replacement,
                &replacement_path,
                &replacement_sha256,
            )?;
            let replacement_inspections = self
                .preflight_existing_attempt(
                    &replacement,
                    &candidate,
                    &source_cut.domains,
                    &source_cut.source_record_sha256,
                    source_cut.usable_retention_deadline,
                    &required_domains,
                    &candidate.omitted_domains,
                )
                .await?;
            if replacement_inspections.values().any(|inspection| {
                matches!(inspection, RestoreParticipantInspection::Visible { .. })
            }) {
                // A carried plan became visible while another participant was being
                // replanned. Record that receipt against the still-selected attempt;
                // never publish a replacement that races an unrecorded visible result.
                let _ = self
                    .reconcile_unrecorded_applying(request, journal.clone(), version.clone())
                    .await?;
                let (reconciled, reconciled_version) =
                    self.load_journal(request.restore_id()).await?;
                let selected = self.load_selected_attempt(&reconciled).await?;
                return Ok((selected, reconciled, reconciled_version));
            }
            if replacement_inspections.len() != replacement.participants.len()
                || replacement_inspections
                    .values()
                    .any(|inspection| !matches!(inspection, RestoreParticipantInspection::Ready))
            {
                return Err(precondition_failed(
                    "replacement restore attempt is no longer ready",
                ));
            }
            self.fence_restore_source(request, source_cut, &required_domains, &replacement)
                .await?;
            self.fence_journal_unchanged(&journal, &version).await?;
        }

        // No new durable record is written until every completed receipt, carried
        // participant, source reference, and newly planned participant validates.
        put_immutable_exact(&self.storage, &replacement_path, replacement_bytes).await?;
        let (winner, winner_version) = self
            .select_frozen_replacement(
                &attempt,
                journal,
                version,
                &replacement,
                &replacement_path,
                &replacement_sha256,
            )
            .await?;
        if winner.aggregate_attempt != aggregate_attempt
            || winner.attempt_path != replacement_path
            || winner.attempt_sha256 != replacement_sha256
        {
            let selected_attempt = self.load_selected_attempt(&winner).await?;
            return Ok((selected_attempt, winner, winner_version));
        }

        let required_domains = winner.required_domains.iter().cloned().collect();
        let replacement_inspections = match async {
            self.fence_restore_source(request, source_cut, &required_domains, &replacement)
                .await?;
            self.preflight_existing_attempt(
                &replacement,
                &winner,
                &source_cut.domains,
                &replacement.source_record_sha256,
                source_cut.usable_retention_deadline,
                &required_domains,
                &winner.omitted_domains,
            )
            .await
        }
        .await
        {
            Ok(inspections) => inspections,
            Err(error) => {
                let mut repair = winner.clone();
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(safe_failure_category(&error));
                bump_journal_revision(&mut repair)?;
                let (repair, repair_version) = self.cas_journal(&repair, &winner_version).await?;
                return Ok((replacement, repair, repair_version));
            }
        };
        if replacement_inspections
            .values()
            .any(|inspection| matches!(inspection, RestoreParticipantInspection::Superseded))
        {
            let mut repair = winner.clone();
            repair.status = WorkspaceRestoreStatus::RepairRequired;
            repair.failure_category = Some(RestoreFailureCategory::CasLost);
            bump_journal_revision(&mut repair)?;
            let (repair, repair_version) = self.cas_journal(&repair, &winner_version).await?;
            return Ok((replacement, repair, repair_version));
        }
        Ok((replacement, winner, winner_version))
    }

    async fn fence_restore_source(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        expected: &PreflightCut,
        selected_domains: &BTreeSet<String>,
        attempt: &WorkspaceRestoreAttemptPlan,
    ) -> Result<()> {
        let observed = self
            .snapshots
            .validated_restore_cut_for_domains(
                &request.source()?,
                request.scope(),
                selected_domains,
                Utc::now(),
            )
            .await?;
        let source_is_unchanged = observed.source_record_sha256 == expected.source_record_sha256
            && observed.scope == expected.scope
            && observed.initial_pin == expected.initial_pin
            && observed.usable_retention_deadline == expected.usable_retention_deadline
            && observed.domains == expected.domains;
        let attempt_source_matches = observed.source_record_sha256 == attempt.source_record_sha256;
        let observed_deadline = observed.usable_retention_deadline;
        let planned_deadline = attempt.active_retention_deadline;
        let attempt_is_covered = attempt_source_matches && observed_deadline >= planned_deadline;
        if !source_is_unchanged || !attempt_is_covered {
            return Err(precondition_failed(
                "restore source changed during participant preflight",
            ));
        }
        Ok(())
    }

    async fn fence_journal_unchanged(
        &self,
        expected: &WorkspaceRestoreJournal,
        expected_version: &str,
    ) -> Result<()> {
        let (observed, observed_version) = self.load_journal(&expected.restore_id).await?;
        if observed_version != expected_version || &observed != expected {
            return Err(CatalogError::CasFailed {
                message: "restore journal changed during replacement preflight".to_string(),
            });
        }
        Ok(())
    }

    async fn reconcile_adopted_orphan_attempt(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        source_cut: &PreflightCut,
        attempt: &WorkspaceRestoreAttemptPlan,
        mut journal: WorkspaceRestoreJournal,
        version: String,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        let required_domains = journal.required_domains.iter().cloned().collect();
        let inspections = self
            .preflight_existing_attempt(
                attempt,
                &journal,
                &source_cut.domains,
                &source_cut.source_record_sha256,
                source_cut.usable_retention_deadline,
                &required_domains,
                &journal.omitted_domains,
            )
            .await?;
        if inspections.len() != journal.participants.len() {
            return Err(validation(
                "adopted orphan preflight omitted a restore participant",
            ));
        }

        let mut visible = false;
        let mut superseded = false;
        for recorded in &mut journal.participants {
            match inspections.get(&recorded.domain).ok_or_else(|| {
                validation("adopted orphan preflight omitted a restore participant")
            })? {
                RestoreParticipantInspection::Ready => {}
                RestoreParticipantInspection::Visible { evidence, .. } => {
                    recorded.evidence = Some(evidence.clone());
                    visible = true;
                }
                RestoreParticipantInspection::Superseded => superseded = true,
            }
        }
        if !visible && !superseded {
            return Ok((journal, version));
        }

        // Inspection is implementation-owned. Re-fence the exact retained cut
        // after every participant has been inspected and before journal adoption
        // can authorize any participant apply.
        self.fence_restore_source(request, source_cut, &required_domains, attempt)
            .await?;
        if superseded {
            journal.status = WorkspaceRestoreStatus::RepairRequired;
            journal.failure_category = Some(RestoreFailureCategory::CasLost);
        } else if journal
            .participants
            .iter()
            .all(|participant| participant.evidence.is_some())
        {
            journal.status = WorkspaceRestoreStatus::Applying;
        } else {
            journal.status = WorkspaceRestoreStatus::RepairRequired;
            journal.failure_category = Some(RestoreFailureCategory::StorageUncertain);
        }
        bump_journal_revision(&mut journal)?;
        self.cas_journal(&journal, &version).await
    }

    fn validate_orphan_replacement(
        replacement: &WorkspaceRestoreAttemptPlan,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
        authorities: &BTreeMap<&str, &crate::workspace_snapshot::DomainAuthorityReference>,
        active_retention_deadline: DateTime<Utc>,
        inspections: &BTreeMap<String, RestoreParticipantInspection>,
    ) -> Result<()> {
        replacement.validate()?;
        if replacement.restore_id != active_attempt.restore_id
            || replacement.aggregate_attempt
                != active_attempt.aggregate_attempt.checked_add(1).unwrap_or(0)
            || replacement.scope != active_attempt.scope
            || replacement.scope != journal.scope
            || replacement.request_sha256 != active_attempt.request_sha256
            || replacement.request_sha256 != journal.request_sha256
            || replacement.source_record_sha256 != active_attempt.source_record_sha256
            || replacement.omitted_domains != active_attempt.omitted_domains
            || replacement.omitted_domains != journal.omitted_domains
            || replacement.active_retention_deadline > active_retention_deadline
        {
            return Err(validation(
                "orphan replacement attempt does not match active restore authority",
            ));
        }
        for recorded in &journal.participants {
            let participant = replacement
                .participants
                .iter()
                .find(|participant| participant.domain == recorded.domain);
            if recorded.evidence.is_none() && participant.is_none() {
                return Err(validation(
                    "orphan replacement attempt omits unfinished participant",
                ));
            }
            if recorded.evidence.is_some()
                && participant.is_some_and(|participant| {
                    participant.participant_attempt != recorded.participant_attempt
                        || participant.plan_sha256 != recorded.plan_sha256
                })
            {
                return Err(validation(
                    "orphan replacement changes a completed participant plan",
                ));
            }
        }
        for participant in &replacement.participants {
            if !journal
                .participants
                .iter()
                .any(|recorded| recorded.domain == participant.domain)
            {
                return Err(validation(
                    "orphan replacement contains an unknown participant",
                ));
            }
            let authority = authorities
                .get(participant.domain.as_str())
                .ok_or_else(|| {
                    validation("orphan replacement participant is absent from source")
                })?;
            let PersistedRestoreParticipantPlan::ControlMvp(plan) = &participant.plan;
            if plan.source() != authority.authority() {
                return Err(validation(
                    "orphan replacement participant source does not match source cut",
                ));
            }
            let prior = active_attempt
                .participants
                .iter()
                .find(|prior| prior.domain == participant.domain)
                .ok_or_else(|| {
                    validation("orphan replacement participant has no active provenance")
                })?;
            if participant.participant_attempt == prior.participant_attempt {
                if participant.plan_sha256 != prior.plan_sha256
                    || participant.plan_wire != prior.plan_wire
                    || participant.plan != prior.plan
                {
                    return Err(validation(
                        "orphan replacement changed a carried participant plan",
                    ));
                }
            } else if participant.participant_attempt == replacement.aggregate_attempt {
                if !matches!(
                    inspections.get(&participant.domain),
                    Some(RestoreParticipantInspection::Superseded)
                ) {
                    return Err(validation(
                        "orphan replacement replans a participant not proven superseded",
                    ));
                }
            } else {
                return Err(validation(
                    "orphan replacement participant attempt has no valid provenance",
                ));
            }
        }
        Ok(())
    }

    async fn select_frozen_replacement(
        &self,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        mut journal: WorkspaceRestoreJournal,
        mut version: String,
        replacement: &WorkspaceRestoreAttemptPlan,
        replacement_path: &str,
        replacement_sha256: &str,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        for _ in 0..4 {
            if journal.status == WorkspaceRestoreStatus::Visible {
                return Ok((journal, version));
            }
            if journal.status == WorkspaceRestoreStatus::Applying {
                self.validate_recorded_receipts(active_attempt, &journal)
                    .await?;
                let mut repair = journal.clone();
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut repair)?;
                let (winner, winner_version) = self.cas_journal(&repair, &version).await?;
                journal = winner;
                version = winner_version;
                continue;
            }
            if journal.status != WorkspaceRestoreStatus::RepairRequired {
                return Err(validation(
                    "replacement attempt requires a durable repair journal",
                ));
            }
            self.validate_recorded_receipts(active_attempt, &journal)
                .await?;
            let selected = replacement_journal_candidate(
                &journal,
                replacement,
                replacement_path,
                replacement_sha256,
            )?;
            match self.cas_journal(&selected, &version).await {
                Ok(winner) => return Ok(winner),
                Err(error @ CatalogError::CasFailed { .. }) => {
                    let (observed, observed_version) =
                        self.load_journal(&journal.restore_id).await?;
                    if !same_attempt_monotonic_receipt_progress(&journal, &observed) {
                        return Err(error);
                    }
                    journal = observed;
                    version = observed_version;
                }
                Err(error) => return Err(error),
            }
        }
        Err(CatalogError::CasFailed {
            message: "restore replacement selection remained unstable".to_string(),
        })
    }

    async fn load_selected_attempt(
        &self,
        journal: &WorkspaceRestoreJournal,
    ) -> Result<WorkspaceRestoreAttemptPlan> {
        let bytes = self.storage.get_raw(&journal.attempt_path).await?;
        if prefixed_sha256(&bytes) != journal.attempt_sha256 {
            return Err(validation("selected restore attempt checksum mismatch"));
        }
        let attempt: WorkspaceRestoreAttemptPlan =
            decode_record(&bytes, "selected workspace restore attempt")?;
        attempt.validate()?;
        if attempt.restore_id != journal.restore_id
            || attempt.aggregate_attempt != journal.aggregate_attempt
            || attempt.scope != journal.scope
            || attempt.request_sha256 != journal.request_sha256
            || attempt.omitted_domains != journal.omitted_domains
        {
            return Err(validation(
                "selected restore attempt does not match journal",
            ));
        }
        for participant in &attempt.participants {
            let recorded = journal
                .participants
                .iter()
                .find(|recorded| recorded.domain == participant.domain)
                .ok_or_else(|| validation("selected attempt has unknown participant"))?;
            if participant.participant_attempt != recorded.participant_attempt
                || participant.plan_sha256 != recorded.plan_sha256
            {
                return Err(validation(
                    "selected attempt participant does not match journal",
                ));
            }
        }
        for recorded in &journal.participants {
            if attempt
                .participants
                .iter()
                .any(|participant| participant.domain == recorded.domain)
            {
                continue;
            }
            if recorded.evidence.is_none() {
                return Err(validation(
                    "selected attempt omits unfinished journal participant",
                ));
            }
            self.load_origin_participant_plan(&attempt, journal, recorded)
                .await?;
        }
        Ok(attempt)
    }

    async fn cas_repair_journal(
        &self,
        mut journal: WorkspaceRestoreJournal,
        version: &str,
        category: RestoreFailureCategory,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(category);
        bump_journal_revision(&mut journal)?;
        self.cas_journal(&journal, version).await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn apply_ready_participant_coordinated(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: &WorkspaceRestoreAttemptPlan,
        participant: &RestoreParticipantPlanRecord,
        adapter: &Arc<dyn crate::state_store::StateRestoreParticipant>,
        mut journal: WorkspaceRestoreJournal,
        mut version: String,
        journal_index: usize,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        let (guard, mut epoch) = self
            .acquire_apply_coordination(
                request.restore_id(),
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256,
            )
            .await?;
        let operation: Result<(WorkspaceRestoreJournal, String)> = async {
            let preflight_now = Utc::now();
            let revalidated = async {
                let source = request.source()?;
                let selected_domains = journal
                    .required_domains
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let cut = self
                    .snapshots
                    .validated_restore_cut_for_domains(
                        &source,
                        request.scope(),
                        &selected_domains,
                        preflight_now,
                    )
                    .await?;
                let pin_check_now = Utc::now();
                self.snapshots
                    .require_active_restore_source_pin(&source, request.scope(), pin_check_now)
                    .await?;
                let mutation_now = Utc::now();
                let authority = cut
                    .domains
                    .iter()
                    .find(|authority| authority.domain() == participant.domain);
                let PersistedRestoreParticipantPlan::ControlMvp(plan) = &participant.plan;
                let source_matches = cut.source_record_sha256 == attempt.source_record_sha256;
                let retention_covers_attempt =
                    cut.usable_retention_deadline >= attempt.active_retention_deadline;
                let retention_is_active = cut.usable_retention_deadline > mutation_now
                    && authority
                        .is_some_and(|entry| entry.authority().retention_deadline() > mutation_now);
                let authority_matches =
                    authority.is_some_and(|entry| entry.authority() == plan.source());
                if !source_matches
                    || !retention_covers_attempt
                    || !retention_is_active
                    || !authority_matches
                {
                    return Err(validation(
                        "fresh restore source cut does not match active participant plan",
                    ));
                }
                Ok(mutation_now)
            }
            .await;
            let mutation_now = match revalidated {
                Ok(now) => now,
                Err(error) => {
                    return self
                        .cas_repair_journal(journal, &version, safe_failure_category(&error))
                        .await;
                }
            };

            // The durable retention epoch now owns the linearization window.
            // Re-fence aggregate selection immediately before participant CAS.
            let (refenced, refenced_version) = self.load_journal(request.restore_id()).await?;
            let refenced_participant = refenced
                .participants
                .get(journal_index)
                .ok_or_else(|| validation("refenced restore journal omits participant"))?;
            if refenced.status != WorkspaceRestoreStatus::Applying
                || refenced.aggregate_attempt != attempt.aggregate_attempt
                || refenced.attempt_sha256 != journal.attempt_sha256
                || refenced_participant.participant_attempt != participant.participant_attempt
                || refenced_participant.plan_sha256 != participant.plan_sha256
                || refenced_participant.evidence.is_some()
            {
                return Ok((refenced, refenced_version));
            }
            journal = refenced;
            version = refenced_version;

            // Reserve the receipt revision before the adapter can make authority
            // visible. Once apply returns Visible, every remaining failure must
            // retain the coordination epoch until the receipt is proven durable.
            let receipt_revision = journal
                .revision
                .checked_add(1)
                .ok_or_else(|| validation("restore journal revision overflow"))?;

            let applied = match adapter.apply_restore(&participant.plan, mutation_now).await {
                Ok(inspection) => inspection,
                Err(error) => match adapter.inspect_restore(&participant.plan).await {
                    Ok(
                        inspection @ (RestoreParticipantInspection::Visible { .. }
                        | RestoreParticipantInspection::Superseded),
                    ) => inspection,
                    Ok(RestoreParticipantInspection::Ready) => {
                        // The adapter returned an error and cannot prove whether its
                        // authority mutation happened. A Ready read is not terminal
                        // evidence, so retain the coordinated epoch until recovery
                        // observes exact Visible or Superseded state.
                        epoch.mark_uncertain();
                        return self
                            .cas_repair_journal(journal, &version, safe_failure_category(&error))
                            .await;
                    }
                    Err(_) => {
                        epoch.mark_uncertain();
                        return self
                            .cas_repair_journal(
                                journal,
                                &version,
                                RestoreFailureCategory::StorageUncertain,
                            )
                            .await;
                    }
                },
            };
            let evidence = match applied {
                RestoreParticipantInspection::Visible { evidence, .. } => evidence,
                RestoreParticipantInspection::Superseded => {
                    let repair = self
                        .cas_repair_journal(journal, &version, RestoreFailureCategory::CasLost)
                        .await;
                    if repair.is_err() {
                        epoch.mark_uncertain();
                    }
                    return repair;
                }
                RestoreParticipantInspection::Ready => {
                    return self
                        .cas_repair_journal(
                            journal,
                            &version,
                            RestoreFailureCategory::ParticipantFailed,
                        )
                        .await;
                }
            };
            let Some(recorded) = journal.participants.get_mut(journal_index) else {
                epoch.mark_uncertain();
                return Err(validation("restore journal omits applied participant"));
            };
            recorded.evidence = Some(evidence);
            journal.revision = receipt_revision;
            let receipt = self.cas_journal(&journal, &version).await;
            if receipt.is_err() {
                epoch.mark_uncertain();
            }
            receipt
        }
        .await;
        Self::finish_apply_coordination(guard, epoch, operation).await
    }

    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn resume_attempt(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: WorkspaceRestoreAttemptPlan,
        mut journal: WorkspaceRestoreJournal,
        mut version: String,
    ) -> Result<WorkspaceRestoreOutcome> {
        if journal.status == WorkspaceRestoreStatus::Visible {
            return self.outcome(&journal).await;
        }
        for participant in &attempt.participants {
            let journal_index = journal
                .participants
                .iter()
                .position(|entry| entry.domain == participant.domain)
                .ok_or_else(|| validation("restore journal omits attempt participant"))?;
            if journal
                .participants
                .get(journal_index)
                .is_some_and(|recorded| recorded.evidence.is_some())
            {
                continue;
            }
            if journal.status == WorkspaceRestoreStatus::Applying
                && journal
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_some())
                && journal
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_none())
            {
                self.validate_recorded_receipts(&attempt, &journal).await?;
                let mut repair = journal.clone();
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut repair)?;
                let (winner, winner_version) = self.cas_journal(&repair, &version).await?;
                if winner.status == WorkspaceRestoreStatus::Visible {
                    return self.outcome(&winner).await;
                }
                if winner.status != WorkspaceRestoreStatus::RepairRequired
                    || winner.aggregate_attempt != attempt.aggregate_attempt
                    || winner.attempt_sha256 != journal.attempt_sha256
                {
                    return self.outcome(&winner).await;
                }
                let mut applying = winner;
                applying.status = WorkspaceRestoreStatus::Applying;
                applying.failure_category = None;
                bump_journal_revision(&mut applying)?;
                let (winner, _winner_version) =
                    self.cas_journal(&applying, &winner_version).await?;
                if winner.status == WorkspaceRestoreStatus::Visible {
                    return self.outcome(&winner).await;
                }
                if winner.status != WorkspaceRestoreStatus::Applying
                    || winner.aggregate_attempt != attempt.aggregate_attempt
                    || winner.attempt_sha256 != journal.attempt_sha256
                {
                    return self.outcome(&winner).await;
                }
                journal = winner;
            }
            // Stable pre-apply fence against aggregate replacement or completion.
            let (fenced, fenced_version) = self.load_journal(request.restore_id()).await?;
            let fenced_participant = fenced
                .participants
                .get(journal_index)
                .ok_or_else(|| validation("fenced restore journal omits participant"))?;
            if fenced.status != WorkspaceRestoreStatus::Applying
                || fenced.aggregate_attempt != attempt.aggregate_attempt
                || fenced.attempt_sha256 != journal.attempt_sha256
                || fenced_participant.participant_attempt != participant.participant_attempt
                || fenced_participant.plan_sha256 != participant.plan_sha256
            {
                return self.outcome(&fenced).await;
            }
            journal = fenced;
            version = fenced_version;
            let binding = self
                .snapshots
                .registry()
                .get(&participant.domain)
                .ok_or_else(|| validation("restore participant binding disappeared"))?;
            let adapter = binding
                .restore_participant()
                .ok_or_else(|| validation("restore participant is not configured"))?;
            let inspection = match adapter.inspect_restore(&participant.plan).await {
                Ok(inspection) => inspection,
                Err(error) => {
                    journal.status = WorkspaceRestoreStatus::RepairRequired;
                    journal.failure_category = Some(safe_failure_category(&error));
                    bump_journal_revision(&mut journal)?;
                    let (winner, _) = self.cas_journal(&journal, &version).await?;
                    return self.outcome(&winner).await;
                }
            };
            let visible = match inspection {
                RestoreParticipantInspection::Visible { evidence, .. } => evidence,
                RestoreParticipantInspection::Ready => {
                    let selected_attempt_sha256 = journal.attempt_sha256.clone();
                    let (winner, winner_version) = self
                        .apply_ready_participant_coordinated(
                            request,
                            &attempt,
                            participant,
                            adapter,
                            journal,
                            version,
                            journal_index,
                        )
                        .await?;
                    let winner_has_receipt = winner
                        .participants
                        .get(journal_index)
                        .is_some_and(|recorded| recorded.evidence.is_some());
                    if winner.status != WorkspaceRestoreStatus::Applying
                        || winner.aggregate_attempt != attempt.aggregate_attempt
                        || winner.attempt_sha256 != selected_attempt_sha256
                        || !winner_has_receipt
                    {
                        return self.outcome(&winner).await;
                    }
                    journal = winner;
                    version = winner_version;
                    continue;
                }
                RestoreParticipantInspection::Superseded => {
                    journal.status = WorkspaceRestoreStatus::RepairRequired;
                    journal.failure_category = Some(RestoreFailureCategory::CasLost);
                    bump_journal_revision(&mut journal)?;
                    let (winner, _) = self.cas_journal(&journal, &version).await?;
                    return self.outcome(&winner).await;
                }
            };
            journal
                .participants
                .get_mut(journal_index)
                .ok_or_else(|| validation("restore journal omits visible participant"))?
                .evidence = Some(visible.clone());
            bump_journal_revision(&mut journal)?;
            let (winner, winner_version) = self.cas_journal(&journal, &version).await?;
            self.settle_after_direct_visible_adoption(&winner, participant, &visible)
                .await?;
            if winner.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&winner).await;
            }
            if winner.aggregate_attempt != attempt.aggregate_attempt
                || winner.attempt_sha256 != journal.attempt_sha256
            {
                return self.outcome(&winner).await;
            }
            journal = winner;
            version = winner_version;
        }

        if journal
            .participants
            .iter()
            .any(|participant| participant.evidence.is_none())
        {
            journal.status = WorkspaceRestoreStatus::RepairRequired;
            journal.failure_category = Some(RestoreFailureCategory::StorageUncertain);
            bump_journal_revision(&mut journal)?;
            let (winner, _) = self.cas_journal(&journal, &version).await?;
            return self.outcome(&winner).await;
        }

        // Finalization is an authority publication boundary. Re-inspect every
        // exact persisted plan after the last receipt CAS so earlier artifacts
        // cannot disappear or change while later participants are applying.
        self.validate_completed_receipts(&attempt, &journal).await?;

        let finalized_at = journal.finalized_at.unwrap_or_else(Utc::now);
        let manifest = WorkspaceRestoreReadManifest {
            record_type: "workspace_restore_read_manifest".to_string(),
            version: VERSION,
            restore_id: journal.restore_id.clone(),
            source_kind: request.source_kind,
            source_id: request.source_id.clone(),
            source_pin_id: request.source_pin_id.clone(),
            scope: request.scope.clone(),
            request_sha256: journal.request_sha256.clone(),
            finalized_at,
            publication_mode: "sequential_repairable".to_string(),
            participants: journal
                .participants
                .iter()
                .map(|participant| {
                    Ok(WorkspaceRestoreReadParticipant {
                        domain: participant.domain.clone(),
                        evidence: participant
                            .evidence
                            .clone()
                            .ok_or_else(|| validation("restore receipt disappeared"))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            omitted_domains: journal.omitted_domains.clone(),
        };
        manifest.validate()?;
        let manifest_bytes = canonical_bytes(&manifest, "workspace restore read manifest")?;
        let manifest_sha256 = prefixed_sha256(&manifest_bytes);
        if journal.status == WorkspaceRestoreStatus::Finalizing
            && journal.read_manifest_sha256.as_deref() != Some(manifest_sha256.as_str())
        {
            return Err(validation(
                "frozen restore read manifest digest does not match reconstructed bytes",
            ));
        }
        if journal.status != WorkspaceRestoreStatus::Finalizing {
            journal.status = WorkspaceRestoreStatus::Finalizing;
            journal.failure_category = None;
            journal.finalized_at = Some(finalized_at);
            journal.read_manifest_sha256 = Some(manifest_sha256.clone());
            bump_journal_revision(&mut journal)?;
            let (winner, winner_version) = self.cas_journal(&journal, &version).await?;
            if winner.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&winner).await;
            }
            if winner.status != WorkspaceRestoreStatus::Finalizing {
                return self.outcome(&winner).await;
            }
            if winner.finalized_at != journal.finalized_at
                || winner.read_manifest_sha256 != journal.read_manifest_sha256
            {
                return Box::pin(self.resume_attempt(request, attempt, winner, winner_version))
                    .await;
            }
            journal = winner;
            version = winner_version;
        }
        put_immutable_exact(
            &self.storage,
            &restore_read_manifest_path(request.restore_id())?,
            Bytes::from(manifest_bytes),
        )
        .await?;
        journal.status = WorkspaceRestoreStatus::Visible;
        journal.failure_category = None;
        bump_journal_revision(&mut journal)?;
        let (journal, _) = self.cas_journal(&journal, &version).await?;
        self.outcome(&journal).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn plan_initial_attempt(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        request_sha256: &str,
        source_record_sha256: &str,
        active_retention_deadline: DateTime<Utc>,
        source_domains: &[crate::workspace_snapshot::DomainAuthorityReference],
        required_domains: &BTreeSet<String>,
        omitted_domains: &[String],
        now: DateTime<Utc>,
    ) -> Result<WorkspaceRestoreAttemptPlan> {
        let authorities: BTreeMap<&str, _> = source_domains
            .iter()
            .map(|authority| (authority.domain(), authority))
            .collect();
        let mut participants = Vec::new();
        for domain in required_domains {
            let authority = authorities
                .get(domain.as_str())
                .ok_or_else(|| validation("required restore domain is absent from source"))?;
            if authority.authority().reference_kind() != PersistedAuthorityKind::Checkpoint {
                return Err(validation(
                    "restore requires checkpoint authority references",
                ));
            }
            let binding = self
                .snapshots
                .registry()
                .get(domain)
                .ok_or_else(|| validation("source restore domain is not configured"))?;
            let adapter = binding
                .restore_participant()
                .ok_or_else(|| validation("restore participant is not configured"))?;
            let identity = RestoreAttemptIdentity::new(request.restore_id(), 1, domain)?;
            let plan = adapter
                .plan_restore(authority.authority(), &identity, now)
                .await?;
            participants.push(RestoreParticipantPlanRecord::new(domain, 1, plan)?);
        }
        let attempt = WorkspaceRestoreAttemptPlan {
            record_type: "workspace_restore_attempt".to_string(),
            version: VERSION,
            restore_id: request.restore_id().to_string(),
            aggregate_attempt: 1,
            scope: request.scope.clone(),
            request_sha256: request_sha256.to_string(),
            source_record_sha256: source_record_sha256.to_string(),
            active_retention_deadline,
            participants,
            omitted_domains: omitted_domains.to_vec(),
        };
        attempt.validate()?;
        Ok(attempt)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn preflight_existing_attempt(
        &self,
        attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
        source_domains: &[crate::workspace_snapshot::DomainAuthorityReference],
        cut_source_record_sha256: &str,
        cut_retention_deadline: DateTime<Utc>,
        required_domains: &BTreeSet<String>,
        omitted_domains: &[String],
    ) -> Result<BTreeMap<String, RestoreParticipantInspection>> {
        if attempt.restore_id != journal.restore_id
            || attempt.aggregate_attempt != journal.aggregate_attempt
            || attempt.scope != journal.scope
            || attempt.request_sha256 != journal.request_sha256
            || attempt.source_record_sha256 != cut_source_record_sha256
            || cut_retention_deadline < attempt.active_retention_deadline
            || attempt.omitted_domains != journal.omitted_domains
            || journal.omitted_domains != omitted_domains
        {
            return Err(validation("restore attempt and journal do not match"));
        }
        let journal_domains = journal
            .participants
            .iter()
            .map(|participant| participant.domain.clone())
            .collect::<BTreeSet<_>>();
        if &journal_domains != required_domains {
            return Err(validation(
                "restore journal domains do not match the current source cut",
            ));
        }
        for participant in &attempt.participants {
            let recorded = journal
                .participants
                .iter()
                .find(|entry| entry.domain == participant.domain)
                .ok_or_else(|| validation("restore attempt has an unknown participant"))?;
            if recorded.participant_attempt != participant.participant_attempt
                || recorded.plan_sha256 != participant.plan_sha256
            {
                return Err(validation(
                    "restore attempt participant does not match journal selection",
                ));
            }
        }
        let source: BTreeMap<&str, _> = source_domains
            .iter()
            .map(|authority| (authority.domain(), authority))
            .collect();
        let mut inspections = BTreeMap::new();
        for recorded in &journal.participants {
            let authority = source
                .get(recorded.domain.as_str())
                .ok_or_else(|| validation("persisted restore participant is absent from source"))?;
            if authority.authority().reference_kind() != PersistedAuthorityKind::Checkpoint {
                return Err(validation(
                    "restore requires checkpoint authority references",
                ));
            }
            let binding = self
                .snapshots
                .registry()
                .get(&recorded.domain)
                .ok_or_else(|| validation("persisted restore participant is not configured"))?;
            let adapter = binding
                .restore_participant()
                .ok_or_else(|| validation("restore participant is not configured"))?;
            let participant = if let Some(participant) = attempt
                .participants
                .iter()
                .find(|participant| participant.domain == recorded.domain)
            {
                participant.clone()
            } else if recorded.evidence.is_some() {
                self.load_origin_participant_plan(attempt, journal, recorded)
                    .await?
            } else {
                return Err(validation(
                    "active restore attempt omits an unfinished participant",
                ));
            };
            if participant.participant_attempt != recorded.participant_attempt
                || participant.plan_sha256 != recorded.plan_sha256
            {
                return Err(validation(
                    "restore participant origin does not match journal receipt",
                ));
            }
            let PersistedRestoreParticipantPlan::ControlMvp(control_plan) = &participant.plan;
            if control_plan.source() != authority.authority() {
                return Err(validation(
                    "persisted restore participant source does not match validated source cut",
                ));
            }
            let inspection = adapter.inspect_restore(&participant.plan).await?;
            match inspection {
                RestoreParticipantInspection::Visible { token, evidence } => {
                    if let Some(recorded_evidence) = recorded.evidence.as_ref()
                        && recorded_evidence != &evidence
                    {
                        return Err(validation("completed restore receipt revalidation failed"));
                    }
                    if recorded.evidence.is_none() {
                        inspections.insert(
                            participant.domain,
                            RestoreParticipantInspection::Visible { token, evidence },
                        );
                    }
                }
                RestoreParticipantInspection::Ready => {
                    if recorded.evidence.is_some() {
                        return Err(validation("completed restore receipt is no longer visible"));
                    }
                    inspections.insert(participant.domain, RestoreParticipantInspection::Ready);
                }
                RestoreParticipantInspection::Superseded => {
                    if recorded.evidence.is_some() {
                        return Err(validation("completed restore receipt was superseded"));
                    }
                    inspections
                        .insert(participant.domain, RestoreParticipantInspection::Superseded);
                }
            }
        }
        Ok(inspections)
    }

    async fn load_origin_participant_plan(
        &self,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
        recorded: &RestoreJournalParticipant,
    ) -> Result<RestoreParticipantPlanRecord> {
        let bytes = self
            .storage
            .get_raw(&restore_attempt_plan_path(
                &journal.restore_id,
                recorded.participant_attempt,
            )?)
            .await?;
        let attempt: WorkspaceRestoreAttemptPlan =
            decode_record(&bytes, "workspace restore origin attempt")?;
        attempt.validate()?;
        let attempt_identity_matches = attempt.restore_id == journal.restore_id
            && attempt.request_sha256 == journal.request_sha256
            && attempt.scope == active_attempt.scope
            && attempt.scope == journal.scope;
        let origin_attempt_matches = attempt.aggregate_attempt == recorded.participant_attempt;
        let origin_deadline = attempt.active_retention_deadline;
        let active_deadline = active_attempt.active_retention_deadline;
        let source_matches = attempt.source_record_sha256 == active_attempt.source_record_sha256
            && attempt.omitted_domains == active_attempt.omitted_domains
            && attempt.omitted_domains == journal.omitted_domains
            && active_deadline >= origin_deadline;
        if !attempt_identity_matches || !origin_attempt_matches || !source_matches {
            return Err(validation("restore participant origin attempt mismatch"));
        }
        let participant = attempt
            .participants
            .into_iter()
            .find(|participant| participant.domain == recorded.domain)
            .ok_or_else(|| validation("restore origin attempt omits participant"))?;
        if participant.participant_attempt != recorded.participant_attempt
            || participant.plan_sha256 != recorded.plan_sha256
        {
            return Err(validation("restore origin participant digest mismatch"));
        }
        Ok(participant)
    }

    async fn validate_completed_receipts(
        &self,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
    ) -> Result<()> {
        if journal
            .participants
            .iter()
            .any(|recorded| recorded.evidence.is_none())
        {
            return Err(validation("restore receipt validation requires completion"));
        }
        self.validate_recorded_receipts(active_attempt, journal)
            .await
    }

    async fn validate_recorded_receipts(
        &self,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
    ) -> Result<()> {
        for recorded in &journal.participants {
            let Some(expected) = recorded.evidence.as_ref() else {
                continue;
            };
            expected.validate()?;
            let participant = if let Some(participant) = active_attempt
                .participants
                .iter()
                .find(|participant| participant.domain == recorded.domain)
            {
                participant.clone()
            } else {
                self.load_origin_participant_plan(active_attempt, journal, recorded)
                    .await?
            };
            let adapter = self
                .snapshots
                .registry()
                .get(&recorded.domain)
                .and_then(|binding| binding.restore_participant())
                .ok_or_else(|| validation("restore receipt adapter is not configured"))?;
            match adapter.inspect_restore(&participant.plan).await? {
                RestoreParticipantInspection::Visible { evidence, .. } if &evidence == expected => {
                }
                RestoreParticipantInspection::Visible { .. } => {
                    return Err(validation("restore receipt evidence changed"));
                }
                RestoreParticipantInspection::Ready | RestoreParticipantInspection::Superseded => {
                    return Err(validation("persisted restore receipt is not visible"));
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn reconcile_unrecorded_applying(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        mut journal: WorkspaceRestoreJournal,
        version: String,
    ) -> Result<Option<WorkspaceRestoreOutcome>> {
        let attempt = self.load_selected_attempt(&journal).await?;
        let durable_journal = journal.clone();
        // Never revise a repair journal or adopt newly discovered work until every
        // receipt it already contains is still the exact visible participant result.
        // This check uses immutable participant plans and direct reads only; it does
        // not require the retained source to remain active.
        self.validate_recorded_receipts(&attempt, &journal).await?;
        let mut visible_terminal_ids = self
            .durable_receipt_operation_ids(&attempt, &journal)
            .await?;
        let mut ready_operation_ids = BTreeSet::new();
        let Ok(immutable_cut) = self
            .snapshots
            .immutable_restore_cut(&request.source()?, request.scope())
            .await
        else {
            return self
                .persist_repair_required(
                    journal,
                    &version,
                    RestoreFailureCategory::StorageUncertain,
                )
                .await
                .map(Some);
        };
        if attempt.source_record_sha256 != immutable_cut.source_record_sha256
            || attempt.scope != immutable_cut.scope
        {
            return Err(validation(
                "active restore attempt does not match immutable source record",
            ));
        }
        let authorities = immutable_cut
            .domains
            .iter()
            .map(|authority| (authority.domain(), authority))
            .collect::<BTreeMap<_, _>>();
        let mut discovered = false;
        let mut superseded = false;
        let mut superseded_terminal_ids = BTreeSet::new();
        for recorded in &mut journal.participants {
            if recorded.evidence.is_some() {
                continue;
            }
            let participant = attempt
                .participants
                .iter()
                .find(|participant| participant.domain == recorded.domain)
                .ok_or_else(|| validation("active restore attempt omits participant"))?;
            let authority = authorities
                .get(recorded.domain.as_str())
                .ok_or_else(|| validation("active restore participant is absent from source"))?;
            let PersistedRestoreParticipantPlan::ControlMvp(control_plan) = &participant.plan;
            if control_plan.source() != authority.authority() {
                return Err(validation(
                    "active restore participant source does not match immutable source record",
                ));
            }
            let Some(adapter) = self
                .snapshots
                .registry()
                .get(&recorded.domain)
                .and_then(|binding| binding.restore_participant())
            else {
                return self
                    .persist_repair_required(
                        journal,
                        &version,
                        RestoreFailureCategory::StorageUncertain,
                    )
                    .await
                    .map(Some);
            };
            match adapter.inspect_restore(&participant.plan).await {
                Ok(RestoreParticipantInspection::Visible { evidence, .. }) => {
                    recorded.evidence = Some(evidence);
                    discovered = true;
                    visible_terminal_ids.insert(restore_apply_operation_id(
                        request.restore_id(),
                        participant.participant_attempt,
                        &participant.domain,
                        &participant.plan_sha256,
                    ));
                }
                Ok(RestoreParticipantInspection::Ready) => {
                    ready_operation_ids.insert(restore_apply_operation_id(
                        request.restore_id(),
                        participant.participant_attempt,
                        &participant.domain,
                        &participant.plan_sha256,
                    ));
                }
                Ok(RestoreParticipantInspection::Superseded) => {
                    superseded = true;
                    superseded_terminal_ids.insert(restore_apply_operation_id(
                        request.restore_id(),
                        participant.participant_attempt,
                        &participant.domain,
                        &participant.plan_sha256,
                    ));
                }
                Err(_) => {
                    return self
                        .persist_repair_required(
                            journal,
                            &version,
                            RestoreFailureCategory::StorageUncertain,
                        )
                        .await
                        .map(Some);
                }
            }
        }
        if self
            .terminal_apply_coordination_is_in_flight(&ready_operation_ids)
            .await?
        {
            // An implementation-owned apply returned an ambiguous error and the
            // exact plan is still merely Ready. Preserve both durable repair state
            // and the in-flight retention exclusion until a later exact Visible or
            // Superseded observation proves the operation terminal.
            if journal.status == WorkspaceRestoreStatus::Applying {
                journal.status = WorkspaceRestoreStatus::RepairRequired;
                journal.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut journal)?;
                let (winner, _) = self.cas_journal(&journal, &version).await?;
                return self.outcome(&winner).await.map(Some);
            }
            return self.outcome(&durable_journal).await.map(Some);
        }
        if !discovered && !superseded {
            self.settle_terminal_apply_coordination(&visible_terminal_ids)
                .await?;
            return Ok(None);
        }
        if journal.status == WorkspaceRestoreStatus::RepairRequired
            && !discovered
            && (!superseded || journal.failure_category == Some(RestoreFailureCategory::CasLost))
        {
            if superseded && journal.failure_category == Some(RestoreFailureCategory::CasLost) {
                visible_terminal_ids.extend(superseded_terminal_ids);
                self.settle_terminal_apply_coordination(&visible_terminal_ids)
                    .await?;
            }
            return Ok(None);
        }
        let all_visible = journal
            .participants
            .iter()
            .all(|participant| participant.evidence.is_some());
        let was_repair_required = journal.status == WorkspaceRestoreStatus::RepairRequired;
        if was_repair_required {
            journal.status = WorkspaceRestoreStatus::Applying;
            journal.failure_category = None;
        } else if !all_visible {
            journal.status = WorkspaceRestoreStatus::RepairRequired;
            journal.failure_category = Some(if superseded {
                RestoreFailureCategory::CasLost
            } else {
                RestoreFailureCategory::StorageUncertain
            });
        }
        bump_journal_revision(&mut journal)?;
        let (winner, winner_version) = self.cas_journal(&journal, &version).await?;
        if was_repair_required && winner.status == WorkspaceRestoreStatus::Applying && !all_visible
        {
            let category = if superseded {
                RestoreFailureCategory::CasLost
            } else {
                RestoreFailureCategory::StorageUncertain
            };
            let outcome = self
                .persist_repair_required(winner, &winner_version, category)
                .await?;
            let mut terminal_operation_ids = visible_terminal_ids;
            if category == RestoreFailureCategory::CasLost {
                terminal_operation_ids.extend(superseded_terminal_ids);
            }
            self.settle_terminal_apply_coordination(&terminal_operation_ids)
                .await?;
            return Ok(Some(outcome));
        }
        if discovered {
            self.validate_recorded_receipts(&attempt, &winner).await?;
        }
        let mut terminal_operation_ids = visible_terminal_ids;
        if superseded {
            if winner.status != WorkspaceRestoreStatus::RepairRequired
                || winner.failure_category != Some(RestoreFailureCategory::CasLost)
            {
                return Err(validation(
                    "superseded restore apply lacks durable CAS_LOST evidence",
                ));
            }
            terminal_operation_ids.extend(superseded_terminal_ids);
        }
        self.settle_terminal_apply_coordination(&terminal_operation_ids)
            .await?;
        if winner.status == WorkspaceRestoreStatus::Visible {
            return self.outcome(&winner).await.map(Some);
        }
        if winner
            .participants
            .iter()
            .all(|participant| participant.evidence.is_some())
        {
            let selected_attempt = self.load_selected_attempt(&winner).await?;
            self.validate_completed_receipts(&selected_attempt, &winner)
                .await?;
            return self
                .resume_attempt(request, selected_attempt, winner, winner_version)
                .await
                .map(Some);
        }
        self.outcome(&winner).await.map(Some)
    }

    async fn persist_repair_required(
        &self,
        mut journal: WorkspaceRestoreJournal,
        version: &str,
        category: RestoreFailureCategory,
    ) -> Result<WorkspaceRestoreOutcome> {
        if journal.status == WorkspaceRestoreStatus::RepairRequired {
            return self.outcome(&journal).await;
        }
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(category);
        bump_journal_revision(&mut journal)?;
        let (winner, _) = self.cas_journal(&journal, version).await?;
        self.outcome(&winner).await
    }

    fn resolve_domains(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        source_domains: &[crate::workspace_snapshot::DomainAuthorityReference],
    ) -> Result<(BTreeSet<String>, Vec<String>)> {
        let source = source_domains
            .iter()
            .map(|authority| authority.domain().to_string())
            .collect::<BTreeSet<_>>();
        let configured = self
            .snapshots
            .registry()
            .domains()
            .map(|(domain, _binding)| domain.to_string())
            .collect::<BTreeSet<_>>();
        match &request.target {
            RestoreOperationTarget::Domain { domain } => {
                if !source.contains(domain) {
                    return Err(validation("domain restore target is absent from source"));
                }
                if !configured.contains(domain) {
                    return Err(validation("domain restore target is not configured"));
                }
                Ok((BTreeSet::from([domain.clone()]), Vec::new()))
            }
            RestoreOperationTarget::Workspace {
                omitted_domain_policy,
            } => {
                if !source.is_subset(&configured) {
                    return Err(validation("source restore domain is not configured"));
                }
                let omitted = configured.difference(&source).cloned().collect::<Vec<_>>();
                if *omitted_domain_policy == OmittedDomainPolicy::Reject && !omitted.is_empty() {
                    return Err(validation(
                        "Reject policy forbids omitted configured domains",
                    ));
                }
                Ok((source, omitted))
            }
        }
    }

    async fn load_journal(&self, restore_id: &str) -> Result<(WorkspaceRestoreJournal, String)> {
        let path = restore_journal_path(restore_id)?;
        for _ in 0..4 {
            let before =
                self.storage
                    .head_raw(&path)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound {
                        entity: "workspace restore journal".to_string(),
                        name: restore_id.to_string(),
                    })?;
            let bytes = self.storage.get_raw(&path).await?;
            let after = self
                .storage
                .head_raw(&path)
                .await?
                .ok_or_else(|| validation("restore journal disappeared during read"))?;
            if before.version != after.version {
                continue;
            }
            let journal: WorkspaceRestoreJournal =
                decode_record(&bytes, "workspace restore journal")?;
            if journal.restore_id != restore_id {
                return Err(validation(
                    "restore journal identity does not match its exact path",
                ));
            }
            journal.validate()?;
            return Ok((journal, before.version));
        }
        Err(CatalogError::CasFailed {
            message: "restore journal was unstable during version-bound read".to_string(),
        })
    }

    async fn load_optional_journal(
        &self,
        restore_id: &str,
    ) -> Result<Option<(WorkspaceRestoreJournal, String)>> {
        match self.load_journal(restore_id).await {
            Ok(journal) => Ok(Some(journal)),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn get_optional_raw(&self, path: &str) -> Result<Option<Bytes>> {
        match self.storage.get_raw(path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(arco_core::Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn cas_journal(
        &self,
        intended: &WorkspaceRestoreJournal,
        expected_version: &str,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        intended.validate()?;
        let (observed, observed_version) = self.load_journal(&intended.restore_id).await?;
        if observed_version != expected_version {
            if journal_winner_is_compatible(intended, &observed) {
                return Ok((observed, observed_version));
            }
            return Err(CatalogError::CasFailed {
                message: "restore journal CAS base changed incompatibly".to_string(),
            });
        }
        validate_journal_transition(&observed, intended)?;
        let path = restore_journal_path(&intended.restore_id)?;
        let bytes = canonical_bytes(intended, "workspace restore journal")?;
        let write = self
            .storage
            .put_raw(
                &path,
                Bytes::from(bytes),
                WritePrecondition::MatchesVersion(expected_version.to_string()),
            )
            .await;
        match write {
            Err(_write_error) => {
                let (winner, version) = self.load_journal(&intended.restore_id).await?;
                if journal_winner_is_compatible(intended, &winner) {
                    Ok((winner, version))
                } else {
                    Err(CatalogError::CasFailed {
                        message: "restore journal write outcome is uncertain and the selected state is incompatible"
                            .to_string(),
                    })
                }
            }
            Ok(WriteResult::PreconditionFailed { .. }) => {
                let (winner, version) = self.load_journal(&intended.restore_id).await?;
                if journal_winner_is_compatible(intended, &winner) {
                    Ok((winner, version))
                } else {
                    Err(CatalogError::CasFailed {
                        message: "restore journal CAS lost to incompatible state".to_string(),
                    })
                }
            }
            Ok(WriteResult::Success { .. }) => {
                let (winner, version) = self.load_journal(&intended.restore_id).await?;
                if journal_winner_is_compatible(intended, &winner) {
                    Ok((winner, version))
                } else {
                    Err(CatalogError::CasFailed {
                        message: "restore journal post-write state is incompatible".to_string(),
                    })
                }
            }
        }
    }

    async fn outcome(&self, journal: &WorkspaceRestoreJournal) -> Result<WorkspaceRestoreOutcome> {
        let request_bytes = self.storage.get_raw(&journal.request_path).await?;
        if prefixed_sha256(&request_bytes) != journal.request_sha256 {
            return Err(validation("restore journal request checksum mismatch"));
        }
        let request = decode_workspace_restore_request(&request_bytes)?;
        if request.restore_id != journal.restore_id || request.scope != journal.scope {
            return Err(validation(
                "restore journal does not match immutable request identity",
            ));
        }
        self.load_selected_attempt(journal).await?;
        let completed_domains = journal
            .participants
            .iter()
            .filter(|participant| participant.evidence.is_some())
            .map(|participant| participant.domain.clone())
            .collect();
        let pending_domains = journal
            .participants
            .iter()
            .filter(|participant| participant.evidence.is_none())
            .map(|participant| participant.domain.clone())
            .collect();
        let read_manifest = if journal.status == WorkspaceRestoreStatus::Visible {
            let bytes = self
                .storage
                .get_raw(&restore_read_manifest_path(&journal.restore_id)?)
                .await?;
            if Some(prefixed_sha256(&bytes).as_str()) != journal.read_manifest_sha256.as_deref() {
                return Err(validation("restore read manifest checksum mismatch"));
            }
            let manifest: WorkspaceRestoreReadManifest =
                decode_record(&bytes, "workspace restore read manifest")?;
            manifest.validate()?;
            if manifest.restore_id != journal.restore_id
                || manifest.scope != journal.scope
                || manifest.request_sha256 != journal.request_sha256
                || manifest.source_kind != request.source_kind
                || manifest.source_id != request.source_id
                || manifest.source_pin_id != request.source_pin_id
                || Some(manifest.finalized_at) != journal.finalized_at
                || manifest.omitted_domains != journal.omitted_domains
                || manifest.participants.len() != journal.participants.len()
                || manifest.participants.iter().zip(&journal.participants).any(
                    |(manifest, recorded)| {
                        manifest.domain != recorded.domain
                            || recorded.evidence.as_ref() != Some(&manifest.evidence)
                    },
                )
            {
                return Err(validation(
                    "restore read manifest does not match terminal journal receipts",
                ));
            }
            Some(manifest)
        } else {
            None
        };
        Ok(WorkspaceRestoreOutcome {
            status: journal.status,
            completed_domains,
            pending_domains,
            omitted_domains: journal.omitted_domains.clone(),
            read_manifest,
        })
    }
}

fn validate_domain(domain: &str) -> Result<()> {
    if !is_path_safe_component(domain) {
        return Err(validation(
            "restore domain must be one nonblank path-safe component",
        ));
    }
    Ok(())
}

fn is_path_safe_component(value: &str) -> bool {
    !value.trim().is_empty()
        && !matches!(value, "." | "..")
        && !value.contains(['/', '\\'])
        && !value.chars().any(char::is_control)
}

fn validate_journal_transition(
    previous: &WorkspaceRestoreJournal,
    next: &WorkspaceRestoreJournal,
) -> Result<()> {
    previous.validate()?;
    next.validate()?;
    if previous.status == WorkspaceRestoreStatus::Visible {
        return Err(validation(
            "VISIBLE restore journal is immutable terminal state",
        ));
    }
    if next.revision != previous.revision.checked_add(1).unwrap_or(0)
        || next.restore_id != previous.restore_id
        || next.scope != previous.scope
        || next.request_sha256 != previous.request_sha256
        || next.request_path != previous.request_path
        || next.required_domains != previous.required_domains
        || next.omitted_domains != previous.omitted_domains
        || next.read_manifest_path != previous.read_manifest_path
        || next.participants.len() != previous.participants.len()
    {
        return Err(validation(
            "illegal restore journal revision or immutable-field change",
        ));
    }
    let legal_status = matches!(
        (previous.status, next.status),
        (
            WorkspaceRestoreStatus::Prepared | WorkspaceRestoreStatus::RepairRequired,
            WorkspaceRestoreStatus::Applying
        ) | (
            WorkspaceRestoreStatus::Prepared,
            WorkspaceRestoreStatus::RepairRequired
        ) | (
            WorkspaceRestoreStatus::Applying,
            WorkspaceRestoreStatus::Applying
                | WorkspaceRestoreStatus::RepairRequired
                | WorkspaceRestoreStatus::Finalizing
        ) | (
            WorkspaceRestoreStatus::Finalizing,
            WorkspaceRestoreStatus::Visible
        )
    );
    if !legal_status {
        return Err(validation("illegal restore journal lifecycle transition"));
    }
    if next.aggregate_attempt == previous.aggregate_attempt {
        if next.attempt_path != previous.attempt_path
            || next.attempt_sha256 != previous.attempt_sha256
        {
            return Err(validation("same aggregate changed selected attempt"));
        }
    } else if next.aggregate_attempt == previous.aggregate_attempt.checked_add(1).unwrap_or(0) {
        if previous.status != WorkspaceRestoreStatus::RepairRequired
            || next.status != WorkspaceRestoreStatus::Applying
        {
            return Err(validation(
                "replacement aggregate requires repair transition",
            ));
        }
    } else {
        return Err(validation(
            "restore aggregate attempt must stay or advance by one",
        ));
    }
    for (before, after) in previous.participants.iter().zip(&next.participants) {
        if before.domain != after.domain
            || before.evidence.as_ref().is_some_and(|evidence| {
                after.evidence.as_ref() != Some(evidence)
                    || before.participant_attempt != after.participant_attempt
                    || before.plan_sha256 != after.plan_sha256
            })
            || (next.aggregate_attempt == previous.aggregate_attempt
                && (before.participant_attempt != after.participant_attempt
                    || before.plan_sha256 != after.plan_sha256))
        {
            return Err(validation(
                "restore journal participant evidence is not monotonic",
            ));
        }
    }
    if previous.status == WorkspaceRestoreStatus::Finalizing
        && (next.finalized_at != previous.finalized_at
            || next.read_manifest_sha256 != previous.read_manifest_sha256)
    {
        return Err(validation("restore finalization evidence changed"));
    }
    Ok(())
}

fn same_attempt_monotonic_receipt_progress(
    previous: &WorkspaceRestoreJournal,
    observed: &WorkspaceRestoreJournal,
) -> bool {
    if previous.validate().is_err()
        || observed.validate().is_err()
        || observed.revision < previous.revision
        || observed.restore_id != previous.restore_id
        || observed.scope != previous.scope
        || observed.request_sha256 != previous.request_sha256
        || observed.request_path != previous.request_path
        || observed.aggregate_attempt != previous.aggregate_attempt
        || observed.attempt_path != previous.attempt_path
        || observed.attempt_sha256 != previous.attempt_sha256
        || observed.required_domains != previous.required_domains
        || observed.omitted_domains != previous.omitted_domains
        || observed.read_manifest_path != previous.read_manifest_path
        || observed.participants.len() != previous.participants.len()
        || !matches!(
            observed.status,
            WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
        )
    {
        return false;
    }
    previous
        .participants
        .iter()
        .zip(&observed.participants)
        .all(|(before, after)| {
            before.domain == after.domain
                && before.participant_attempt == after.participant_attempt
                && before.plan_sha256 == after.plan_sha256
                && before
                    .evidence
                    .as_ref()
                    .is_none_or(|evidence| after.evidence.as_ref() == Some(evidence))
        })
}

fn same_revision_monotonic_receipt_winner(
    intended: &WorkspaceRestoreJournal,
    winner: &WorkspaceRestoreJournal,
) -> bool {
    if intended.validate().is_err()
        || winner.validate().is_err()
        || intended.revision != winner.revision
        || intended.aggregate_attempt != winner.aggregate_attempt
        || intended.attempt_path != winner.attempt_path
        || intended.attempt_sha256 != winner.attempt_sha256
        || intended.participants.len() != winner.participants.len()
        || !matches!(
            intended.status,
            WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
        )
        || !matches!(
            winner.status,
            WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
        )
    {
        return false;
    }
    let mut added_receipt = false;
    for (expected, selected) in intended.participants.iter().zip(&winner.participants) {
        if expected.domain != selected.domain
            || expected.participant_attempt != selected.participant_attempt
            || expected.plan_sha256 != selected.plan_sha256
            || expected
                .evidence
                .as_ref()
                .is_some_and(|evidence| selected.evidence.as_ref() != Some(evidence))
        {
            return false;
        }
        added_receipt |= expected.evidence.is_none() && selected.evidence.is_some();
    }
    added_receipt
}

fn journal_winner_is_compatible(
    intended: &WorkspaceRestoreJournal,
    winner: &WorkspaceRestoreJournal,
) -> bool {
    if winner.restore_id != intended.restore_id
        || winner.scope != intended.scope
        || winner.request_sha256 != intended.request_sha256
        || winner.request_path != intended.request_path
        || winner.required_domains != intended.required_domains
        || winner.omitted_domains != intended.omitted_domains
        || winner.read_manifest_path != intended.read_manifest_path
        || winner.revision < intended.revision
        || winner.aggregate_attempt < intended.aggregate_attempt
        || winner.participants.len() != intended.participants.len()
        || (intended.status == WorkspaceRestoreStatus::Visible
            && winner.status != WorkspaceRestoreStatus::Visible)
    {
        return false;
    }
    if winner.revision == intended.revision {
        return winner == intended
            || same_revision_monotonic_receipt_winner(intended, winner)
            || (intended.status == WorkspaceRestoreStatus::Finalizing
                && winner.status == WorkspaceRestoreStatus::Finalizing
                && winner.aggregate_attempt == intended.aggregate_attempt
                && winner.attempt_path == intended.attempt_path
                && winner.attempt_sha256 == intended.attempt_sha256
                && winner.participants == intended.participants
                && winner.failure_category.is_none()
                && winner.finalized_at.is_some()
                && winner.read_manifest_sha256.is_some());
    }
    let status_can_advance = match intended.status {
        WorkspaceRestoreStatus::Prepared => true,
        WorkspaceRestoreStatus::Applying => winner.status != WorkspaceRestoreStatus::Prepared,
        WorkspaceRestoreStatus::RepairRequired => {
            !matches!(winner.status, WorkspaceRestoreStatus::Prepared)
        }
        WorkspaceRestoreStatus::Finalizing => matches!(
            winner.status,
            WorkspaceRestoreStatus::Finalizing | WorkspaceRestoreStatus::Visible
        ),
        WorkspaceRestoreStatus::Visible => winner.status == WorkspaceRestoreStatus::Visible,
    };
    if !status_can_advance {
        return false;
    }
    if winner.aggregate_attempt == intended.aggregate_attempt
        && (winner.attempt_path != intended.attempt_path
            || winner.attempt_sha256 != intended.attempt_sha256)
    {
        return false;
    }
    for (expected, selected) in intended.participants.iter().zip(&winner.participants) {
        if expected.domain != selected.domain
            || selected.participant_attempt < expected.participant_attempt
            || (winner.aggregate_attempt == intended.aggregate_attempt
                && (selected.participant_attempt != expected.participant_attempt
                    || selected.plan_sha256 != expected.plan_sha256))
            || expected
                .evidence
                .as_ref()
                .is_some_and(|evidence| selected.evidence.as_ref() != Some(evidence))
        {
            return false;
        }
    }
    let adopting_finalization_winner = intended.status == WorkspaceRestoreStatus::Finalizing
        && matches!(
            winner.status,
            WorkspaceRestoreStatus::Finalizing | WorkspaceRestoreStatus::Visible
        )
        && winner.aggregate_attempt == intended.aggregate_attempt
        && winner.attempt_path == intended.attempt_path
        && winner.attempt_sha256 == intended.attempt_sha256
        && winner.participants == intended.participants
        && winner.finalized_at.is_some()
        && winner.read_manifest_sha256.is_some();
    if !adopting_finalization_winner
        && (intended
            .finalized_at
            .is_some_and(|value| winner.finalized_at != Some(value))
            || intended
                .read_manifest_sha256
                .as_ref()
                .is_some_and(|value| winner.read_manifest_sha256.as_ref() != Some(value)))
    {
        return false;
    }
    true
}

fn replacement_journal_candidate(
    journal: &WorkspaceRestoreJournal,
    replacement: &WorkspaceRestoreAttemptPlan,
    replacement_path: &str,
    replacement_sha256: &str,
) -> Result<WorkspaceRestoreJournal> {
    if journal.status != WorkspaceRestoreStatus::RepairRequired {
        return Err(validation(
            "replacement attempt requires a durable repair journal",
        ));
    }
    let mut selected = journal.clone();
    selected.status = WorkspaceRestoreStatus::Applying;
    selected.failure_category = None;
    selected.aggregate_attempt = replacement.aggregate_attempt;
    selected.attempt_path = replacement_path.to_string();
    selected.attempt_sha256 = replacement_sha256.to_string();
    bump_journal_revision(&mut selected)?;
    for recorded in selected
        .participants
        .iter_mut()
        .filter(|participant| participant.evidence.is_none())
    {
        let participant = replacement
            .participants
            .iter()
            .find(|participant| participant.domain == recorded.domain)
            .ok_or_else(|| validation("replacement attempt omits participant"))?;
        recorded.participant_attempt = participant.participant_attempt;
        recorded.plan_sha256 = participant.plan_sha256.clone();
    }
    selected.validate()?;
    Ok(selected)
}

fn bump_journal_revision(journal: &mut WorkspaceRestoreJournal) -> Result<()> {
    journal.revision = journal
        .revision
        .checked_add(1)
        .ok_or_else(|| validation("restore journal revision overflow"))?;
    Ok(())
}

const fn safe_failure_category(error: &CatalogError) -> RestoreFailureCategory {
    match error {
        CatalogError::Storage { .. } | CatalogError::CasFailed { .. } => {
            RestoreFailureCategory::StorageUncertain
        }
        _ => RestoreFailureCategory::ParticipantFailed,
    }
}

fn validate_ordered_domains(domains: &[String]) -> Result<()> {
    for domain in domains {
        validate_domain(domain)?;
    }
    if domains
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left >= right))
    {
        return Err(validation(
            "restore domains must be unique and strictly sorted",
        ));
    }
    Ok(())
}

fn validate_ordered_participant_plans(
    participants: &[RestoreParticipantPlanRecord],
    restore_id: &str,
    aggregate_attempt: u64,
) -> Result<()> {
    for participant in participants {
        participant.validate()?;
        if participant.participant_attempt > aggregate_attempt {
            return Err(validation("participant attempt exceeds aggregate attempt"));
        }
        let PersistedRestoreParticipantPlan::ControlMvp(plan) = &participant.plan;
        if plan.identity().restore_id() != restore_id
            || plan.identity().domain() != participant.domain
            || plan.identity().attempt() != participant.participant_attempt
        {
            return Err(validation(
                "restore participant plan identity does not match aggregate record",
            ));
        }
    }
    let domains = participants
        .iter()
        .map(|participant| participant.domain.clone())
        .collect::<Vec<_>>();
    validate_ordered_domains(&domains)
}

fn validate_prefixed_sha256(value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(validation("restore digest must use sha256: prefix"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation("restore digest must contain lowercase SHA-256"));
    }
    Ok(())
}

fn prefixed_sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn restore_apply_operation_id(
    restore_id: &str,
    participant_attempt: u64,
    domain: &str,
    plan_sha256: &str,
) -> String {
    let mut identity = Sha256::new();
    hash_identity_component(&mut identity, restore_id.as_bytes());
    hash_identity_component(&mut identity, &participant_attempt.to_be_bytes());
    hash_identity_component(&mut identity, domain.as_bytes());
    hash_identity_component(&mut identity, plan_sha256.as_bytes());
    format!("restore-apply-{}", &hex::encode(identity.finalize())[..32])
}

fn hash_identity_component(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn canonical_bytes<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    serde_jcs::to_vec(value).map_err(|error| CatalogError::Serialization {
        message: format!("failed to serialize {context}: {error}"),
    })
}

fn decode_record<T: for<'de> Deserialize<'de>>(bytes: &[u8], context: &str) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| CatalogError::Serialization {
        message: format!("failed to deserialize {context}: {error}"),
    })
}

async fn put_immutable_exact(storage: &ScopedStorage, path: &str, bytes: Bytes) -> Result<()> {
    let write = storage
        .put_raw(path, bytes.clone(), WritePrecondition::DoesNotExist)
        .await;
    match write {
        Ok(WriteResult::Success { .. }) => Ok(()),
        Ok(WriteResult::PreconditionFailed { .. }) => {
            if storage.get_raw(path).await? == bytes {
                Ok(())
            } else {
                Err(precondition_failed(
                    "immutable restore object already exists with conflicting bytes",
                ))
            }
        }
        Err(write_error) => match storage.get_raw(path).await {
            Ok(winner) if winner == bytes => Ok(()),
            Ok(_) => Err(precondition_failed(
                "uncertain immutable restore write selected conflicting bytes",
            )),
            Err(arco_core::Error::NotFound(_)) => Err(write_error.into()),
            Err(read_error) => Err(read_error.into()),
        },
    }
}

fn precondition_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: message.into(),
    }
}
