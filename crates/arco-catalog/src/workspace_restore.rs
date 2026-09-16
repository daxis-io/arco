//! Durable, exact-path roll-forward workspace restore workflow.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arco_core::lock::{DistributedLock, LockGuard};
use arco_core::storage::{WritePrecondition, WriteResult};
use arco_core::{RootStorage, ScopedStorage};
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use ulid::Ulid;

use crate::error::{CatalogError, Result};
use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
use crate::state_store::{
    PersistedAuthorityKind, PersistedRestoreParticipantPlan, RestoreAttemptIdentity,
    RestoreBoundedInspectionContext, RestoreParticipantInspection, RestorePlanningContext,
    RestoredAuthorityEvidence,
};
use crate::workspace_io_budget::{RECORD_BYTES, WorkspaceIoBudget};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL, WorkspaceScope,
};
use crate::workspace_snapshot_service::{
    PreflightCut, RestoreSource, RestoreSourceKind, WorkspaceCaptureIo, WorkspaceDomainRegistry,
    WorkspaceSnapshotService,
};

const VERSION: u32 = 1;
const REQUEST_RECORD_TYPE: &str = "workspace_restore_request";

/// Invocation-local routing for public restore control records.
///
/// Bounded callers borrow one workspace invoice. It never exposes storage to
/// a participant and therefore cannot be retained or reconstructed from a
/// persisted restore record.
enum RestoreInvocationIo<'a> {
    Legacy,
    Bounded(&'a mut WorkspaceIoBudget),
}

impl RestoreInvocationIo<'_> {
    const fn is_bounded(&self) -> bool {
        matches!(self, Self::Bounded(_))
    }

    fn bounded(&mut self) -> Option<&mut WorkspaceIoBudget> {
        match self {
            Self::Legacy => None,
            Self::Bounded(budget) => Some(*budget),
        }
    }
}

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
                    || evidence.scope().workspace_id() != Some(self.scope.workspace_id())
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
                || participant.evidence.scope().workspace_id() != Some(self.scope.workspace_id())
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

/// Service-owned capability for one restore advance invocation.
///
/// This value cannot be serialized or constructed from persisted restore records.
pub struct RestoreAdvanceContext<'a> {
    mode: RestoreAdvanceMode<'a>,
}

enum RestoreAdvanceMode<'a> {
    Legacy(DateTime<Utc>),
    Bounded(RestoreAdvanceFence<'a>),
}

struct RestoreAdvanceFence<'a> {
    service: &'a WorkspaceRestoreService,
    request: &'a WorkspaceRestoreRequestRecord,
    attempt: &'a WorkspaceRestoreAttemptPlan,
    participant: &'a RestoreParticipantPlanRecord,
    journal: &'a WorkspaceRestoreJournal,
    journal_version: &'a str,
    adapter: &'a dyn crate::state_store::StateRestoreParticipant,
    epoch: &'a RetentionMutationEpoch,
    guard: &'a mut LockGuard<ScopedStorage>,
    budget: &'a mut WorkspaceIoBudget,
}

/// Borrows one selected workspace record and its invocation budget together.
/// It conveys selection provenance only; store/root validation is separate.
#[allow(
    dead_code,
    reason = "native bounded advance integration remains disabled"
)]
pub(crate) struct RestoreUnitSelection<'a> {
    observed_now: DateTime<Utc>,
    plan: &'a PersistedRestoreParticipantPlan,
    plan_sha256: &'a str,
    budget: &'a mut WorkspaceIoBudget,
}

#[allow(
    dead_code,
    reason = "native bounded advance integration remains disabled"
)]
impl RestoreUnitSelection<'_> {
    pub(crate) const fn observed_now(&self) -> DateTime<Utc> {
        self.observed_now
    }
    pub(crate) fn parts(
        &mut self,
    ) -> (
        &PersistedRestoreParticipantPlan,
        &str,
        &mut WorkspaceIoBudget,
    ) {
        (self.plan, self.plan_sha256, self.budget)
    }
}

impl RestoreAdvanceContext<'_> {
    /// Returns the service's decision time for this invocation.
    #[must_use]
    pub fn observed_now(&self) -> DateTime<Utc> {
        match &self.mode {
            RestoreAdvanceMode::Legacy(now) => *now,
            RestoreAdvanceMode::Bounded(fence) => fence.service.now(),
        }
    }

    /// Returns whether this invocation requires explicitly bounded participant I/O.
    #[must_use]
    pub const fn is_bounded(&self) -> bool {
        matches!(self.mode, RestoreAdvanceMode::Bounded(_))
    }

    #[allow(
        dead_code,
        reason = "native bounded advance integration remains disabled"
    )]
    pub(crate) async fn unit_selection(
        &mut self,
        supplied: &PersistedRestoreParticipantPlan,
    ) -> Result<RestoreUnitSelection<'_>> {
        self.refence().await?;
        let RestoreAdvanceMode::Bounded(fence) = &mut self.mode else {
            return Err(validation("legacy restore has no bounded selection"));
        };
        if supplied != &fence.participant.plan {
            return Err(validation(
                "supplied restore plan differs from the selected plan",
            ));
        }
        Ok(RestoreUnitSelection {
            observed_now: fence.service.now(),
            plan: &fence.participant.plan,
            plan_sha256: &fence.participant.plan_sha256,
            budget: fence.budget,
        })
    }

    pub(crate) async fn refence(&mut self) -> Result<()> {
        let RestoreAdvanceMode::Bounded(fence) = &mut self.mode else {
            return Err(validation(
                "legacy restore has no bounded publication capability",
            ));
        };
        fence.validate_selection()?;
        fence.validate_deadlines()?;
        let mut io = RestoreInvocationIo::Bounded(fence.budget);
        let attempt = fence
            .service
            .load_selected_attempt(fence.request, fence.journal, &mut io)
            .await?;
        if attempt != *fence.attempt {
            return Err(validation(
                "selected restore attempt changed during advance",
            ));
        }
        fence
            .service
            .require_active_source_pin(
                &fence.request.source()?,
                fence.request.scope(),
                fence.service.now(),
                &mut io,
            )
            .await?;
        fence
            .epoch
            .refence_bounded(fence.guard, fence.budget)
            .await?;
        // Selection is the final awaited control read: pin/attempt checks must
        // not leave a later window for an undetected journal replacement.
        let (journal, version) = fence
            .service
            .load_journal(
                fence.request.restore_id(),
                &mut RestoreInvocationIo::Bounded(fence.budget),
            )
            .await?;
        if version != fence.journal_version || journal != *fence.journal {
            return Err(precondition_failed(
                "selected restore journal changed during advance",
            ));
        }
        fence.validate_deadlines()
    }
}

impl RestoreAdvanceFence<'_> {
    fn validate_deadlines(&self) -> Result<()> {
        let deadline = self
            .request
            .requested_at
            .checked_add_signed(ChronoDuration::hours(24))
            .ok_or_else(|| validation("restore execution deadline overflow"))?;
        let now = self.service.now();
        if self.request.requested_at > now
            || now >= deadline
            || now >= self.attempt.active_retention_deadline
            || now >= self.participant.plan.source().retention_deadline()
        {
            return Err(validation(
                "restore advance is outside its original execution or source window",
            ));
        }
        Ok(())
    }

    fn validate_selection(&self) -> Result<()> {
        self.journal.validate()?;
        self.attempt.validate()?;
        self.participant.validate()?;
        validate_attempt_request_binding(self.attempt, self.request)?;
        let selected = self
            .journal
            .participants
            .iter()
            .find(|entry| entry.domain == self.participant.domain);
        let registered = self
            .service
            .snapshots
            .registry()
            .get(&self.participant.domain)
            .and_then(|binding| binding.restore_participant());
        if !matches!(
            self.journal.status,
            WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
        ) || self.journal_version.is_empty()
            || self.journal.restore_id != self.request.restore_id
            || self.journal.scope != self.request.scope
            || self.journal.request_sha256 != self.attempt.request_sha256
            || self.journal.aggregate_attempt != self.attempt.aggregate_attempt
            || !self.attempt.participants.contains(self.participant)
            || !selected.is_some_and(|entry| {
                entry.evidence.is_none()
                    && entry.participant_attempt == self.participant.participant_attempt
                    && entry.plan_sha256 == self.participant.plan_sha256
            })
            || !registered.is_some_and(|adapter| {
                adapter.restore_binding_identity() == self.adapter.restore_binding_identity()
            })
            || self.adapter.scope() != self.participant.plan.source().scope()
            || self.adapter.implementation() != self.participant.plan.source().implementation()
        {
            return Err(validation(
                "restore advance does not own the selected participant plan",
            ));
        }
        Ok(())
    }
}

// Keep the legacy epoch behavior while lending a cancellation-safe bounded
// epoch to the same apply/receipt workflow.
enum RestoreApplyEpoch<'a> {
    Legacy(&'a mut RetentionMutationEpoch),
    Bounded(crate::retention_coordination::BoundedMutation<'a>),
}

impl RestoreApplyEpoch<'_> {
    fn epoch(&self) -> &RetentionMutationEpoch {
        match self {
            Self::Legacy(epoch) => epoch,
            Self::Bounded(mutation) => mutation.epoch(),
        }
    }

    fn mark_uncertain(&mut self) {
        match self {
            Self::Legacy(epoch) => epoch.mark_uncertain(),
            Self::Bounded(mutation) => mutation.mark_uncertain(),
        }
    }

    fn finish<T>(self, result: Result<T>) -> Result<T> {
        match self {
            Self::Legacy(_) => result,
            Self::Bounded(mutation) => mutation.finish(result),
        }
    }
}

/// Direct-addressed durable roll-forward restore module for one workspace.
pub struct WorkspaceRestoreService {
    storage: ScopedStorage,
    snapshots: WorkspaceSnapshotService,
    #[cfg(feature = "test-utils")]
    clock: Option<Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>>,
}

impl WorkspaceRestoreService {
    /// Creates a restore module with the same workspace scope as its registry.
    ///
    /// # Errors
    ///
    /// Returns an error when storage and registry scopes disagree.
    pub fn new(storage: ScopedStorage, registry: WorkspaceDomainRegistry) -> Result<Self> {
        let snapshots = WorkspaceSnapshotService::new(storage.clone(), registry)?;
        Ok(Self {
            storage,
            snapshots,
            #[cfg(feature = "test-utils")]
            clock: None,
        })
    }

    #[cfg(feature = "test-utils")]
    #[must_use]
    /// Overrides the restore decision clock for deterministic test schedules.
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>) -> Self {
        self.clock = Some(clock);
        self
    }

    #[cfg_attr(not(feature = "test-utils"), allow(clippy::unused_self))]
    fn now(&self) -> DateTime<Utc> {
        #[cfg(feature = "test-utils")]
        if let Some(clock) = &self.clock {
            return clock();
        }
        Utc::now()
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
        if self.snapshots.bounded_workspace_io() && request.record.requested_at > self.now() {
            return Err(validation("restore request is future-dated"));
        }
        let mut budget = WorkspaceIoBudget::new();
        let mut io = if self.snapshots.bounded_workspace_io() {
            RestoreInvocationIo::Bounded(&mut budget)
        } else {
            RestoreInvocationIo::Legacy
        };
        self.restore_with_terminal_winner_adoption(&request.record, &mut io)
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
        if self.snapshots.bounded_workspace_io() && request.record.requested_at > self.now() {
            return Err(validation("restore request is future-dated"));
        }
        let mut budget = WorkspaceIoBudget::new();
        let mut io = if self.snapshots.bounded_workspace_io() {
            RestoreInvocationIo::Bounded(&mut budget)
        } else {
            RestoreInvocationIo::Legacy
        };
        self.restore_with_terminal_winner_adoption(&request.record, &mut io)
            .await
    }

    /// Resumes a nonterminal restore by exact ID without listing.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, missing, corrupt, or unrecoverable records.
    pub async fn recover_restore(&self, restore_id: &str) -> Result<WorkspaceRestoreOutcome> {
        let mut budget = WorkspaceIoBudget::new();
        let mut io = if self.snapshots.bounded_workspace_io() {
            RestoreInvocationIo::Bounded(&mut budget)
        } else {
            RestoreInvocationIo::Legacy
        };
        let request_bytes = self
            .read_record(&restore_request_path(restore_id)?, &mut io)
            .await?
            .0;
        let request = decode_workspace_restore_request(&request_bytes)?;
        if request.restore_id() != restore_id {
            return Err(validation(
                "restore request identity does not match its exact path",
            ));
        }
        Box::pin(self.restore(&request, &mut io)).await
    }

    /// Reads current restore state by exact ID without mutation.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, missing, or corrupt records.
    pub async fn get_restore(&self, restore_id: &str) -> Result<WorkspaceRestoreOutcome> {
        let mut budget = WorkspaceIoBudget::new();
        let mut io = if self.snapshots.bounded_workspace_io() {
            RestoreInvocationIo::Bounded(&mut budget)
        } else {
            RestoreInvocationIo::Legacy
        };
        let journal = self.load_journal(restore_id, &mut io).await?.0;
        self.outcome(&journal, &mut io).await
    }

    async fn restore_with_terminal_winner_adoption(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WorkspaceRestoreOutcome> {
        match Box::pin(self.restore(request, io)).await {
            Ok(outcome) => Ok(outcome),
            Err(original_error) => {
                let Some((journal, _version)) =
                    self.load_optional_journal(request.restore_id(), io).await?
                else {
                    return Err(original_error);
                };
                if journal.status != WorkspaceRestoreStatus::Visible {
                    return Err(original_error);
                }
                // Re-enter the normal terminal path once. It revalidates immutable
                // request identity, selected attempt and receipts, and settles an
                // exact matching retention epoch if the winner crashed after visibility.
                Box::pin(self.restore(request, io)).await
            }
        }
    }

    async fn acquire_apply_coordination(
        &self,
        restore_id: &str,
        participant_attempt: u64,
        domain: &str,
        plan_sha256: &str,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(LockGuard<ScopedStorage>, RetentionMutationEpoch)> {
        let operation_id =
            restore_apply_operation_id(restore_id, participant_attempt, domain, plan_sha256);
        let mut guard = self
            .acquire_restore_lock("workspace-restore-apply", io)
            .await?;
        let claim = if let Some(budget) = io.bounded() {
            RetentionMutationEpoch::claim_bounded(
                self.storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                &operation_id,
                budget,
            )
            .await
        } else {
            RetentionMutationEpoch::claim_workspace(
                self.storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                operation_id,
            )
            .await
        };
        match claim {
            Ok(epoch) => Ok((guard, epoch)),
            Err(error) => {
                let _ = Self::release_restore_lock(guard, io).await;
                Err(error)
            }
        }
    }

    async fn finish_apply_coordination<T>(
        mut guard: LockGuard<ScopedStorage>,
        epoch: RetentionMutationEpoch,
        operation: Result<T>,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<T> {
        let settlement = if let Some(budget) = io.bounded() {
            epoch.settle_bounded(&mut guard, budget).await
        } else {
            epoch.settle().await
        };
        let release = Self::release_restore_lock(guard, io).await;
        match (operation, settlement, release) {
            (Ok(value), Ok(()), Ok(())) => Ok(value),
            (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        }
    }

    async fn settle_terminal_apply_coordination(
        &self,
        terminal_operation_ids: &BTreeSet<String>,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        if terminal_operation_ids.is_empty() {
            return Ok(());
        }
        if !self
            .terminal_apply_coordination_is_in_flight(terminal_operation_ids, io)
            .await?
        {
            return Ok(());
        }
        let mut guard = self
            .acquire_restore_lock("workspace-restore-recovery", io)
            .await?;
        let settlement = if let Some(budget) = io.bounded() {
            RetentionMutationEpoch::settle_terminal_matching_bounded(
                &self.storage,
                &mut guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                terminal_operation_ids,
                budget,
            )
            .await
        } else {
            RetentionMutationEpoch::settle_terminal_matching_workspace(
                self.storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                terminal_operation_ids,
            )
            .await
        };
        let release = Self::release_restore_lock(guard, io).await;
        match (settlement, release) {
            (Ok(_), Ok(())) => Ok(()),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn terminal_apply_coordination_is_in_flight(
        &self,
        terminal_operation_ids: &BTreeSet<String>,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<bool> {
        if let Some(budget) = io.bounded() {
            return RetentionMutationEpoch::terminal_match_is_in_flight_bounded(
                &self.storage,
                RetentionMutationKind::WorkspaceRestoreApply,
                terminal_operation_ids,
                budget,
            )
            .await;
        }
        RetentionMutationEpoch::terminal_match_is_in_flight(
            &RootStorage::from(self.storage.clone()),
            RetentionMutationKind::WorkspaceRestoreApply,
            terminal_operation_ids,
        )
        .await
    }

    async fn durable_receipt_operation_ids(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,

        io: &mut RestoreInvocationIo<'_>,
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
                self.load_origin_participant_plan(request, attempt, journal, recorded, io)
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
        request: &WorkspaceRestoreRequestRecord,
        journal: &WorkspaceRestoreJournal,
        participant: &RestoreParticipantPlanRecord,
        expected_evidence: &RestoredAuthorityEvidence,

        io: &mut RestoreInvocationIo<'_>,
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
        let selected_attempt = self.load_selected_attempt(request, journal, io).await?;
        self.validate_recorded_receipts(request, &selected_attempt, journal, io)
            .await?;
        let terminal_operation_ids = self
            .durable_receipt_operation_ids(request, &selected_attempt, journal, io)
            .await?;
        self.settle_terminal_apply_coordination(&terminal_operation_ids, io)
            .await
    }

    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn restore(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WorkspaceRestoreOutcome> {
        request.validate()?;
        let request_bytes = encode_workspace_restore_request(request)?;
        let request_sha256 = prefixed_sha256(&request_bytes);
        let journal_path = restore_journal_path(request.restore_id())?;
        let mut applying_before_preflight = None;

        if let Some((journal, version)) =
            self.load_optional_journal(request.restore_id(), io).await?
        {
            let durable_request_bytes = self.read_record(&journal.request_path, io).await?.0;
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
                let attempt = self.load_selected_attempt(request, &journal, io).await?;
                let terminal_operation_ids = self
                    .durable_receipt_operation_ids(request, &attempt, &journal, io)
                    .await?;
                if self
                    .terminal_apply_coordination_is_in_flight(&terminal_operation_ids, io)
                    .await?
                {
                    self.validate_completed_receipts(request, &attempt, &journal, io)
                        .await?;
                    self.settle_terminal_apply_coordination(&terminal_operation_ids, io)
                        .await?;
                }
                return self.outcome(&journal, io).await;
            }
            if io.is_bounded()
                && matches!(
                    journal.status,
                    WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
                )
            {
                let attempt = self.load_selected_attempt(request, &journal, io).await?;
                let has_pending_plan7 = attempt.participants.iter().any(|p| {
                    matches!(p.plan, PersistedRestoreParticipantPlan::ControlMvpV7(_))
                        && journal
                            .participants
                            .iter()
                            .any(|entry| entry.domain == p.domain && entry.evidence.is_none())
                });
                if has_pending_plan7
                    && let Some(outcome) = self
                        .resume_selected_plan7(
                            request,
                            attempt,
                            journal.clone(),
                            version.clone(),
                            io,
                        )
                        .await?
                {
                    return Ok(outcome);
                }
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
                let attempt = self.load_selected_attempt(request, &journal, io).await?;
                self.validate_recorded_receipts(request, &attempt, &journal, io)
                    .await?;
                let mut repair = journal;
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut repair)?;
                let (winner, _) = self.cas_journal(&repair, &version, io).await?;
                return self.outcome(&winner, io).await;
            }
            if matches!(
                journal.status,
                WorkspaceRestoreStatus::Applying | WorkspaceRestoreStatus::RepairRequired
            ) && journal
                .participants
                .iter()
                .any(|participant| participant.evidence.is_none())
                && let Some(outcome) = self
                    .reconcile_unrecorded_applying(request, journal.clone(), version.clone(), io)
                    .await?
            {
                return Ok(outcome);
            }
            if journal
                .participants
                .iter()
                .all(|participant| participant.evidence.is_some())
            {
                let attempt = self.load_selected_attempt(request, &journal, io).await?;
                self.validate_completed_receipts(request, &attempt, &journal, io)
                    .await?;
                let terminal_operation_ids = self
                    .durable_receipt_operation_ids(request, &attempt, &journal, io)
                    .await?;
                self.settle_terminal_apply_coordination(&terminal_operation_ids, io)
                    .await?;
                return self
                    .resume_attempt(request, attempt, journal, version, io)
                    .await;
            }
            applying_before_preflight = Some((journal, version));
            // Full source/participant preflight happens below before mutation.
        }

        let source = request.source()?;
        let now = if io.is_bounded() {
            self.now()
        } else {
            Utc::now()
        };
        let preflight = async {
            let immutable = if let Some(budget) = io.bounded() {
                self.snapshots
                    .immutable_restore_cut_bounded(&source, request.scope(), budget)
                    .await?
            } else {
                self.snapshots
                    .immutable_restore_cut(&source, request.scope())
                    .await?
            };
            let (required_domains, omitted_domains) =
                self.resolve_domains(request, &immutable.domains)?;
            let cut = if let Some(budget) = io.bounded() {
                self.snapshots
                    .validated_restore_cut_for_domains_bounded(
                        &source,
                        request.scope(),
                        &required_domains,
                        now,
                        budget,
                    )
                    .await?
            } else {
                self.validated_restore_cut(&source, request.scope(), &required_domains, now, io)
                    .await?
            };
            Ok::<_, CatalogError>((cut, required_domains, omitted_domains))
        }
        .await;
        let (cut, required_domains, omitted_domains) = match preflight {
            Ok(preflight) => preflight,
            Err(error) => {
                if let Some((journal, version)) = applying_before_preflight {
                    return self
                        .persist_repair_required(
                            journal,
                            &version,
                            safe_failure_category(&error),
                            io,
                        )
                        .await;
                }
                return Err(error);
            }
        };

        let existing = self.load_optional_journal(request.restore_id(), io).await?;
        let (attempt, mut journal, journal_version) = if let Some((existing_journal, version)) =
            existing
        {
            let attempt_bytes = self
                .read_record(&existing_journal.attempt_path, io)
                .await?
                .0;
            if prefixed_sha256(&attempt_bytes) != existing_journal.attempt_sha256 {
                return Err(validation("restore attempt checksum mismatch"));
            }
            let attempt: WorkspaceRestoreAttemptPlan =
                decode_attempt_record(&attempt_bytes, "workspace restore attempt")?;
            attempt.validate()?;
            let inspections = self
                .preflight_existing_attempt(
                    request,
                    &attempt,
                    &existing_journal,
                    &cut.domains,
                    &cut.source_record_sha256,
                    cut.usable_retention_deadline,
                    &required_domains,
                    &omitted_domains,
                    io,
                )
                .await?;
            // Adapter inspection is an externally implemented read and may take long
            // enough for the retained source to expire or be released. No journal
            // revision may follow it until the exact retained cut is fenced again.
            self.fence_restore_source(request, &cut, &required_domains, &attempt, io)
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
                let (winner, _) = self.cas_journal(&repair, &version, io).await?;
                return self.outcome(&winner, io).await;
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
                    io,
                )
                .await?;
            if replacement_requested
                && (journal.status == WorkspaceRestoreStatus::RepairRequired
                    || attempt.aggregate_attempt == prior_aggregate_attempt)
            {
                return self.outcome(&journal, io).await;
            }
            (attempt, journal, Some(version))
        } else {
            let request_path = restore_request_path(request.restore_id())?;
            let attempt_path = restore_attempt_plan_path(request.restore_id(), 1)?;
            let orphan_request = self.get_optional_raw(&request_path, io).await?;
            let orphan_attempt = self.get_optional_raw(&attempt_path, io).await?;
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
            if io.is_bounded() && request_publication_bytes.as_ref() != request_bytes.as_slice() {
                return Err(validation(
                    "bounded restore request bytes are not canonical",
                ));
            }
            let (attempt, attempt_bytes) = if let Some(bytes) = orphan_attempt {
                let attempt: WorkspaceRestoreAttemptPlan =
                    decode_attempt_record(&bytes, "orphan workspace restore attempt")?;
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
                        io,
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
                    request,
                    &attempt,
                    &journal,
                    &cut.domains,
                    &cut.source_record_sha256,
                    cut.usable_retention_deadline,
                    &required_domains,
                    &omitted_domains,
                    io,
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
            self.fence_restore_source(request, &cut, &required_domains, &attempt, io)
                .await?;
            if self
                .load_optional_journal(request.restore_id(), io)
                .await?
                .is_some()
            {
                return Err(CatalogError::CasFailed {
                    message: "restore journal appeared during unpublished attempt preflight"
                        .to_string(),
                });
            }
            // Every participant has been planned successfully before the first write.
            self.put_immutable_exact(&request_path, request_publication_bytes, io)
                .await?;
            self.put_immutable_exact(&attempt_path, attempt_bytes, io)
                .await?;
            let journal_bytes = canonical_bytes(&journal, "workspace restore journal")?;
            let journal_write = self
                .write_record(
                    &journal_path,
                    Bytes::from(journal_bytes),
                    WritePrecondition::DoesNotExist,
                    io,
                )
                .await;
            let (selected, version) = match journal_write {
                Ok(WriteResult::Success { .. } | WriteResult::PreconditionFailed { .. }) => {
                    self.load_journal(request.restore_id(), io).await?
                }
                Err(write_error) => match self.load_journal(request.restore_id(), io).await {
                    Ok(selected) => selected,
                    Err(CatalogError::NotFound { .. }) => return Err(write_error),
                    Err(read_error) => return Err(read_error),
                },
            };
            if !journal_winner_is_compatible(&journal, &selected) {
                return Err(precondition_failed("conflicting restore journal winner"));
            }
            if selected.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&selected, io).await;
            }
            let mut selected_attempt = self.load_selected_attempt(request, &selected, io).await?;
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
                    io,
                )
                .await?
            } else {
                (selected, version)
            };
            if selected.status == WorkspaceRestoreStatus::RepairRequired {
                return self.outcome(&selected, io).await;
            }
            if selected_attempt.aggregate_attempt != selected.aggregate_attempt {
                selected_attempt = self.load_selected_attempt(request, &selected, io).await?;
            }
            (selected_attempt, selected, Some(version))
        };

        let version =
            journal_version.ok_or_else(|| validation("restore journal version missing"))?;
        if journal.status == WorkspaceRestoreStatus::Prepared
            || journal.status == WorkspaceRestoreStatus::RepairRequired
        {
            let selected_domains = journal.required_domains.iter().cloned().collect();
            self.fence_restore_source(request, &cut, &selected_domains, &attempt, io)
                .await?;
            journal.status = WorkspaceRestoreStatus::Applying;
            journal.failure_category = None;
            bump_journal_revision(&mut journal)?;
            let (winner, winner_version) = self.cas_journal(&journal, &version, io).await?;
            if winner.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&winner, io).await;
            }
            if winner.status != WorkspaceRestoreStatus::Applying
                || winner.aggregate_attempt != attempt.aggregate_attempt
                || winner.attempt_sha256 != journal.attempt_sha256
            {
                return self.outcome(&winner, io).await;
            }
            journal = winner;
            return self
                .resume_attempt(request, attempt, journal, winner_version, io)
                .await;
        }
        self.resume_attempt(request, attempt, journal, version, io)
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

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreAttemptPlan, WorkspaceRestoreJournal, String)> {
        if !inspections
            .values()
            .any(|inspection| matches!(inspection, RestoreParticipantInspection::Superseded))
        {
            return Ok((attempt, journal, version));
        }

        if attempt.participants.iter().any(|participant| {
            matches!(
                participant.plan,
                PersistedRestoreParticipantPlan::ControlMvpV7(_)
            ) && matches!(
                inspections.get(&participant.domain),
                Some(RestoreParticipantInspection::Superseded)
            )
        }) {
            if journal.status == WorkspaceRestoreStatus::RepairRequired
                && journal.failure_category == Some(RestoreFailureCategory::CasLost)
            {
                return Ok((attempt, journal, version));
            }
            let (journal, version) = self
                .cas_repair_journal(journal, &version, RestoreFailureCategory::CasLost, io)
                .await?;
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
        let orphan = self.get_optional_raw(&replacement_path, io).await?;
        let adopting_orphan = orphan.is_some();
        let (replacement, replacement_bytes) = if let Some(bytes) = orphan {
            let replacement: WorkspaceRestoreAttemptPlan =
                decode_attempt_record(&bytes, "orphan replacement restore attempt")?;
            Self::validate_orphan_replacement(
                &replacement,
                &attempt,
                &journal,
                &authorities,
                source_cut.usable_retention_deadline,
                &inspections,
            )?;
            validate_attempt_request_binding(&replacement, request)?;
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
                        self.plan_participant(
                            adapter.as_ref(),
                            authority.authority(),
                            &identity,
                            request,
                            now,
                            io,
                        )
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
            self.fence_restore_source(request, source_cut, &required_domains, &replacement, io)
                .await?;
            self.fence_journal_unchanged(&journal, &version, io).await?;
        } else {
            let candidate = replacement_journal_candidate(
                &journal,
                &replacement,
                &replacement_path,
                &replacement_sha256,
            )?;
            let replacement_inspections = self
                .preflight_existing_attempt(
                    request,
                    &replacement,
                    &candidate,
                    &source_cut.domains,
                    &source_cut.source_record_sha256,
                    source_cut.usable_retention_deadline,
                    &required_domains,
                    &candidate.omitted_domains,
                    io,
                )
                .await?;
            if replacement_inspections.values().any(|inspection| {
                matches!(inspection, RestoreParticipantInspection::Visible { .. })
            }) {
                // A carried plan became visible while another participant was being
                // replanned. Record that receipt against the still-selected attempt;
                // never publish a replacement that races an unrecorded visible result.
                let _ = self
                    .reconcile_unrecorded_applying(request, journal.clone(), version.clone(), io)
                    .await?;
                let (reconciled, reconciled_version) =
                    self.load_journal(request.restore_id(), io).await?;
                let selected = self.load_selected_attempt(request, &reconciled, io).await?;
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
            self.fence_restore_source(request, source_cut, &required_domains, &replacement, io)
                .await?;
            self.fence_journal_unchanged(&journal, &version, io).await?;
        }

        // No new durable record is written until every completed receipt, carried
        // participant, source reference, and newly planned participant validates.
        self.put_immutable_exact(&replacement_path, replacement_bytes, io)
            .await?;
        let (winner, winner_version) = self
            .select_frozen_replacement(
                request,
                &attempt,
                journal,
                version,
                &replacement,
                &replacement_path,
                &replacement_sha256,
                io,
            )
            .await?;
        if winner.aggregate_attempt != aggregate_attempt
            || winner.attempt_path != replacement_path
            || winner.attempt_sha256 != replacement_sha256
        {
            let selected_attempt = self.load_selected_attempt(request, &winner, io).await?;
            return Ok((selected_attempt, winner, winner_version));
        }

        let required_domains = winner.required_domains.iter().cloned().collect();
        let replacement_inspections = match async {
            self.fence_restore_source(request, source_cut, &required_domains, &replacement, io)
                .await?;
            self.preflight_existing_attempt(
                request,
                &replacement,
                &winner,
                &source_cut.domains,
                &replacement.source_record_sha256,
                source_cut.usable_retention_deadline,
                &required_domains,
                &winner.omitted_domains,
                io,
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
                let (repair, repair_version) =
                    self.cas_journal(&repair, &winner_version, io).await?;
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
            let (repair, repair_version) = self.cas_journal(&repair, &winner_version, io).await?;
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

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        let observed = self
            .validated_restore_cut(
                &request.source()?,
                request.scope(),
                selected_domains,
                Utc::now(),
                io,
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

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        let (observed, observed_version) = self.load_journal(&expected.restore_id, io).await?;
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

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        let required_domains = journal.required_domains.iter().cloned().collect();
        let inspections = self
            .preflight_existing_attempt(
                request,
                attempt,
                &journal,
                &source_cut.domains,
                &source_cut.source_record_sha256,
                source_cut.usable_retention_deadline,
                &required_domains,
                &journal.omitted_domains,
                io,
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
        self.fence_restore_source(request, source_cut, &required_domains, attempt, io)
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
        self.cas_journal(&journal, &version, io).await
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
            let plan = &participant.plan;
            if !plan.is_legacy_version() {
                plan.validate_source_authority_format()?;
            }
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

    #[allow(
        clippy::too_many_arguments,
        reason = "carry the shared I/O budget alongside the exact immutable replacement witnesses"
    )]
    async fn select_frozen_replacement(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        mut journal: WorkspaceRestoreJournal,
        mut version: String,
        replacement: &WorkspaceRestoreAttemptPlan,
        replacement_path: &str,
        replacement_sha256: &str,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        for _ in 0..4 {
            if journal.status == WorkspaceRestoreStatus::Visible {
                return Ok((journal, version));
            }
            if journal.status == WorkspaceRestoreStatus::Applying {
                self.validate_recorded_receipts(request, active_attempt, &journal, io)
                    .await?;
                let mut repair = journal.clone();
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut repair)?;
                let (winner, winner_version) = self.cas_journal(&repair, &version, io).await?;
                journal = winner;
                version = winner_version;
                continue;
            }
            if journal.status != WorkspaceRestoreStatus::RepairRequired {
                return Err(validation(
                    "replacement attempt requires a durable repair journal",
                ));
            }
            self.validate_recorded_receipts(request, active_attempt, &journal, io)
                .await?;
            let selected = replacement_journal_candidate(
                &journal,
                replacement,
                replacement_path,
                replacement_sha256,
            )?;
            match self.cas_journal(&selected, &version, io).await {
                Ok(winner) => return Ok(winner),
                Err(error @ CatalogError::CasFailed { .. }) => {
                    let (observed, observed_version) =
                        self.load_journal(&journal.restore_id, io).await?;
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
        request: &WorkspaceRestoreRequestRecord,
        journal: &WorkspaceRestoreJournal,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WorkspaceRestoreAttemptPlan> {
        let (bytes, _) = self.read_record(&journal.attempt_path, io).await?;
        if prefixed_sha256(&bytes) != journal.attempt_sha256 {
            return Err(validation("selected restore attempt checksum mismatch"));
        }
        let attempt: WorkspaceRestoreAttemptPlan =
            decode_attempt_record(&bytes, "selected workspace restore attempt")?;
        attempt.validate()?;
        validate_attempt_request_binding(&attempt, request)?;
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
            self.load_origin_participant_plan(request, &attempt, journal, recorded, io)
                .await?;
        }
        Ok(attempt)
    }

    async fn cas_repair_journal(
        &self,
        mut journal: WorkspaceRestoreJournal,
        version: &str,
        category: RestoreFailureCategory,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(category);
        bump_journal_revision(&mut journal)?;
        self.cas_journal(&journal, version, io).await
    }

    async fn validate_apply_source(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: &WorkspaceRestoreAttemptPlan,
        participant: &RestoreParticipantPlanRecord,
        journal: &WorkspaceRestoreJournal,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<DateTime<Utc>> {
        let preflight_now = if io.is_bounded() {
            self.now()
        } else {
            Utc::now()
        };
        let source = request.source()?;
        let selected_domains = journal
            .required_domains
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let cut = self
            .validated_restore_cut(
                &source,
                request.scope(),
                &selected_domains,
                preflight_now,
                io,
            )
            .await?;
        let pin_check_now = if io.is_bounded() {
            self.now()
        } else {
            Utc::now()
        };
        self.require_active_source_pin(&source, request.scope(), pin_check_now, io)
            .await?;
        let mutation_now = if io.is_bounded() {
            self.now()
        } else {
            Utc::now()
        };
        let authority = cut
            .domains
            .iter()
            .find(|authority| authority.domain() == participant.domain);
        let plan = &participant.plan;
        if !plan.is_legacy_version() {
            plan.validate_source_authority_format()?;
        }
        let source_matches = cut.source_record_sha256 == attempt.source_record_sha256;
        let retention_covers_attempt =
            cut.usable_retention_deadline >= attempt.active_retention_deadline;
        let retention_is_active = cut.usable_retention_deadline > mutation_now
            && authority.is_some_and(|entry| entry.authority().retention_deadline() > mutation_now);
        let authority_matches = authority.is_some_and(|entry| entry.authority() == plan.source());
        if !source_matches
            || !retention_covers_attempt
            || !retention_is_active
            || !authority_matches
        {
            return Err(validation(
                "fresh restore source cut does not match active participant plan",
            ));
        }
        if io.is_bounded() {
            let deadline = request
                .requested_at
                .checked_add_signed(ChronoDuration::hours(24))
                .ok_or_else(|| validation("restore execution deadline overflow"))?;
            if request.requested_at > mutation_now || mutation_now >= deadline {
                return Err(validation(
                    "restore advance is outside its original execution window",
                ));
            }
        }
        Ok(mutation_now)
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

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        if io.is_bounded() && !adapter.supports_bounded_restore_advance() {
            return Err(CatalogError::UnsupportedOperation {
                message: "restore participant does not opt in to bounded advance".into(),
            });
        }
        let (mut guard, mut epoch) = self
            .acquire_apply_coordination(
                request.restore_id(),
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256,
                io,
            )
            .await?;
        let operation: Result<(WorkspaceRestoreJournal, String)> = async {
            let revalidated = self
                .validate_apply_source(request, attempt, participant, &journal, io)
                .await;
            let mutation_now = match revalidated {
                Ok(now) => now,
                Err(error) => {
                    return self
                        .cas_repair_journal(journal, &version, safe_failure_category(&error), io)
                        .await;
                }
            };

            // The durable retention epoch now owns the linearization window.
            // Re-fence aggregate selection immediately before participant CAS.
            let (refenced, refenced_version) = self.load_journal(request.restore_id(), io).await?;
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

            // Complete service-owned admission before arming uncertain work.
            // A definite rejection here has not invoked implementation-owned I/O.
            if let Some(budget) = io.bounded() {
                let mut context = RestoreAdvanceContext {
                    mode: RestoreAdvanceMode::Bounded(RestoreAdvanceFence {
                        service: self,
                        request,
                        attempt,
                        participant,
                        journal: &journal,
                        journal_version: &version,
                        adapter: adapter.as_ref(),
                        epoch: &epoch,
                        guard: &mut guard,
                        budget,
                    }),
                };
                context.refence().await?;
            }
            let mut mutation = if io.is_bounded() {
                RestoreApplyEpoch::Bounded(epoch.begin_bounded_mutation())
            } else {
                RestoreApplyEpoch::Legacy(&mut epoch)
            };
            let advanced_operation = async {
                let advanced = {
                    let mut context = RestoreAdvanceContext {
                        mode: io.bounded().map_or(
                            RestoreAdvanceMode::Legacy(mutation_now),
                            |budget| {
                                RestoreAdvanceMode::Bounded(RestoreAdvanceFence {
                                    service: self,
                                    request,
                                    attempt,
                                    participant,
                                    journal: &journal,
                                    journal_version: &version,
                                    adapter: adapter.as_ref(),
                                    epoch: mutation.epoch(),
                                    guard: &mut guard,
                                    budget,
                                })
                            },
                        ),
                    };
                    adapter
                        .advance_restore(&participant.plan, &mut context)
                        .await
                };
                let applied = match advanced {
                    Ok(crate::state_store::RestoreParticipantAdvance::Terminal(inspection)) => {
                        inspection
                    }
                    Ok(crate::state_store::RestoreParticipantAdvance::InProgress {
                        completed_units,
                    }) => {
                        if completed_units == 0 || !io.is_bounded() {
                            mutation.mark_uncertain();
                            return Err(validation(
                                "restore progress requires a completed bounded unit",
                            ));
                        }
                        return Ok((journal, version));
                    }
                    Err(error) => {
                        // Transfer arrived error ownership to the outer invocation
                        // before inspection, recovery, or any cleanup await.
                        if let Some(budget) = io.bounded() {
                            if budget.charge_error(&error).is_err() {
                                mutation.mark_uncertain();
                                return Err(error);
                            }
                        }
                        match self
                            .inspect_participant(adapter.as_ref(), participant, io)
                            .await
                        {
                            Ok(
                                inspection @ (RestoreParticipantInspection::Visible { .. }
                                | RestoreParticipantInspection::Superseded),
                            ) => inspection,
                            Ok(RestoreParticipantInspection::Ready) => {
                                // The adapter returned an error and cannot prove whether its
                                // authority mutation happened. A Ready read is not terminal
                                // evidence, so retain the coordinated epoch until recovery
                                // observes exact Visible or Superseded state.
                                mutation.mark_uncertain();
                                return self
                                    .cas_repair_journal(
                                        journal,
                                        &version,
                                        safe_failure_category(&error),
                                        io,
                                    )
                                    .await;
                            }
                            Err(_) => {
                                mutation.mark_uncertain();
                                return self
                                    .cas_repair_journal(
                                        journal,
                                        &version,
                                        RestoreFailureCategory::StorageUncertain,
                                        io,
                                    )
                                    .await;
                            }
                        }
                    }
                };
                let evidence = match applied {
                    RestoreParticipantInspection::Visible { evidence, .. } => evidence,
                    RestoreParticipantInspection::Superseded => {
                        let repair = self
                            .cas_repair_journal(
                                journal,
                                &version,
                                RestoreFailureCategory::CasLost,
                                io,
                            )
                            .await;
                        if repair.is_err() {
                            mutation.mark_uncertain();
                        }
                        return repair;
                    }
                    RestoreParticipantInspection::Ready => {
                        return self
                            .cas_repair_journal(
                                journal,
                                &version,
                                RestoreFailureCategory::ParticipantFailed,
                                io,
                            )
                            .await;
                    }
                };
                let Some(recorded) = journal.participants.get_mut(journal_index) else {
                    mutation.mark_uncertain();
                    return Err(validation("restore journal omits applied participant"));
                };
                recorded.evidence = Some(evidence);
                journal.revision = receipt_revision;
                let receipt = self.cas_journal(&journal, &version, io).await;
                if receipt.is_err() {
                    mutation.mark_uncertain();
                }
                receipt
            }
            .await;
            mutation.finish(advanced_operation)
        }
        .await;
        Self::finish_apply_coordination(guard, epoch, operation, io).await
    }

    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn resume_attempt(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: WorkspaceRestoreAttemptPlan,
        mut journal: WorkspaceRestoreJournal,
        mut version: String,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WorkspaceRestoreOutcome> {
        validate_attempt_request_binding(&attempt, request)?;
        if journal.status == WorkspaceRestoreStatus::Visible {
            return self.outcome(&journal, io).await;
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
            if !io.is_bounded()
                && journal.status == WorkspaceRestoreStatus::Applying
                && journal
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_some())
                && journal
                    .participants
                    .iter()
                    .any(|participant| participant.evidence.is_none())
            {
                self.validate_recorded_receipts(request, &attempt, &journal, io)
                    .await?;
                let mut repair = journal.clone();
                repair.status = WorkspaceRestoreStatus::RepairRequired;
                repair.failure_category = Some(RestoreFailureCategory::StorageUncertain);
                bump_journal_revision(&mut repair)?;
                let (winner, winner_version) = self.cas_journal(&repair, &version, io).await?;
                if winner.status == WorkspaceRestoreStatus::Visible {
                    return self.outcome(&winner, io).await;
                }
                if winner.status != WorkspaceRestoreStatus::RepairRequired
                    || winner.aggregate_attempt != attempt.aggregate_attempt
                    || winner.attempt_sha256 != journal.attempt_sha256
                {
                    return self.outcome(&winner, io).await;
                }
                let mut applying = winner;
                applying.status = WorkspaceRestoreStatus::Applying;
                applying.failure_category = None;
                bump_journal_revision(&mut applying)?;
                let (winner, _winner_version) =
                    self.cas_journal(&applying, &winner_version, io).await?;
                if winner.status == WorkspaceRestoreStatus::Visible {
                    return self.outcome(&winner, io).await;
                }
                if winner.status != WorkspaceRestoreStatus::Applying
                    || winner.aggregate_attempt != attempt.aggregate_attempt
                    || winner.attempt_sha256 != journal.attempt_sha256
                {
                    return self.outcome(&winner, io).await;
                }
                journal = winner;
            }
            // Stable pre-apply fence against aggregate replacement or completion.
            let (fenced, fenced_version) = self.load_journal(request.restore_id(), io).await?;
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
                return self.outcome(&fenced, io).await;
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
            let inspection = match self
                .inspect_participant(adapter.as_ref(), participant, io)
                .await
            {
                Ok(inspection) => inspection,
                Err(error) => {
                    journal.status = WorkspaceRestoreStatus::RepairRequired;
                    journal.failure_category = Some(safe_failure_category(&error));
                    bump_journal_revision(&mut journal)?;
                    let (winner, _) = self.cas_journal(&journal, &version, io).await?;
                    return self.outcome(&winner, io).await;
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
                            io,
                        )
                        .await?;
                    let winner_has_receipt = winner
                        .participants
                        .get(journal_index)
                        .is_some_and(|recorded| recorded.evidence.is_some());
                    if io.is_bounded()
                        || winner.status != WorkspaceRestoreStatus::Applying
                        || winner.aggregate_attempt != attempt.aggregate_attempt
                        || winner.attempt_sha256 != selected_attempt_sha256
                        || !winner_has_receipt
                    {
                        return self.outcome(&winner, io).await;
                    }
                    journal = winner;
                    version = winner_version;
                    continue;
                }
                RestoreParticipantInspection::Superseded => {
                    journal.status = WorkspaceRestoreStatus::RepairRequired;
                    journal.failure_category = Some(RestoreFailureCategory::CasLost);
                    bump_journal_revision(&mut journal)?;
                    let (winner, _) = self.cas_journal(&journal, &version, io).await?;
                    return self.outcome(&winner, io).await;
                }
            };
            journal
                .participants
                .get_mut(journal_index)
                .ok_or_else(|| validation("restore journal omits visible participant"))?
                .evidence = Some(visible.clone());
            bump_journal_revision(&mut journal)?;
            let (winner, winner_version) = self.cas_journal(&journal, &version, io).await?;
            self.settle_after_direct_visible_adoption(request, &winner, participant, &visible, io)
                .await?;
            if winner.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&winner, io).await;
            }
            if winner.aggregate_attempt != attempt.aggregate_attempt
                || winner.attempt_sha256 != journal.attempt_sha256
            {
                return self.outcome(&winner, io).await;
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
            let (winner, _) = self.cas_journal(&journal, &version, io).await?;
            return self.outcome(&winner, io).await;
        }

        // Finalization is an authority publication boundary. Re-inspect every
        // exact persisted plan after the last receipt CAS so earlier artifacts
        // cannot disappear or change while later participants are applying.
        self.validate_completed_receipts(request, &attempt, &journal, io)
            .await?;

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
            let (winner, winner_version) = self.cas_journal(&journal, &version, io).await?;
            if winner.status == WorkspaceRestoreStatus::Visible {
                return self.outcome(&winner, io).await;
            }
            if winner.status != WorkspaceRestoreStatus::Finalizing {
                return self.outcome(&winner, io).await;
            }
            if winner.finalized_at != journal.finalized_at
                || winner.read_manifest_sha256 != journal.read_manifest_sha256
            {
                return Box::pin(self.resume_attempt(request, attempt, winner, winner_version, io))
                    .await;
            }
            journal = winner;
            version = winner_version;
        }
        self.put_immutable_exact(
            &restore_read_manifest_path(request.restore_id())?,
            Bytes::from(manifest_bytes),
            io,
        )
        .await?;
        journal.status = WorkspaceRestoreStatus::Visible;
        journal.failure_category = None;
        bump_journal_revision(&mut journal)?;
        let (journal, _) = self.cas_journal(&journal, &version, io).await?;
        self.outcome(&journal, io).await
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

        io: &mut RestoreInvocationIo<'_>,
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
            if !io.is_bounded()
                && authority.authority().reference_kind() != PersistedAuthorityKind::Checkpoint
            {
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
            let plan = self
                .plan_participant(
                    adapter.as_ref(),
                    authority.authority(),
                    &identity,
                    request,
                    now,
                    io,
                )
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
        request: &WorkspaceRestoreRequestRecord,
        attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
        source_domains: &[crate::workspace_snapshot::DomainAuthorityReference],
        cut_source_record_sha256: &str,
        cut_retention_deadline: DateTime<Utc>,
        required_domains: &BTreeSet<String>,
        omitted_domains: &[String],

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<BTreeMap<String, RestoreParticipantInspection>> {
        validate_attempt_request_binding(attempt, request)?;
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
            if !io.is_bounded()
                && authority.authority().reference_kind() != PersistedAuthorityKind::Checkpoint
            {
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
                self.load_origin_participant_plan(request, attempt, journal, recorded, io)
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
            let control_plan = &participant.plan;
            if !control_plan.is_legacy_version() {
                control_plan.validate_source_authority_format()?;
            }
            if control_plan.source() != authority.authority() {
                return Err(validation(
                    "persisted restore participant source does not match validated source cut",
                ));
            }
            let inspection = self
                .inspect_participant(adapter.as_ref(), &participant, io)
                .await?;
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
        request: &WorkspaceRestoreRequestRecord,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,
        recorded: &RestoreJournalParticipant,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<RestoreParticipantPlanRecord> {
        let (bytes, _) = self
            .read_record(
                &restore_attempt_plan_path(&journal.restore_id, recorded.participant_attempt)?,
                io,
            )
            .await?;
        let attempt: WorkspaceRestoreAttemptPlan =
            decode_attempt_record(&bytes, "workspace restore origin attempt")?;
        attempt.validate()?;
        validate_attempt_request_binding(&attempt, request)?;
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
        request: &WorkspaceRestoreRequestRecord,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        if journal
            .participants
            .iter()
            .any(|recorded| recorded.evidence.is_none())
        {
            return Err(validation("restore receipt validation requires completion"));
        }
        self.validate_recorded_receipts(request, active_attempt, journal, io)
            .await
    }

    async fn validate_recorded_receipts(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        active_attempt: &WorkspaceRestoreAttemptPlan,
        journal: &WorkspaceRestoreJournal,

        io: &mut RestoreInvocationIo<'_>,
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
                self.load_origin_participant_plan(request, active_attempt, journal, recorded, io)
                    .await?
            };
            let adapter = self
                .snapshots
                .registry()
                .get(&recorded.domain)
                .and_then(|binding| binding.restore_participant())
                .ok_or_else(|| validation("restore receipt adapter is not configured"))?;
            match self
                .inspect_participant(adapter.as_ref(), &participant, io)
                .await?
            {
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

    /// Reconcile a selected Plan7 before consulting active-source eligibility.
    /// Generic V6 replacement must never rebase this selected physical job.
    #[allow(clippy::too_many_lines)]
    async fn resume_selected_plan7(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        attempt: WorkspaceRestoreAttemptPlan,
        mut journal: WorkspaceRestoreJournal,
        mut version: String,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<Option<WorkspaceRestoreOutcome>> {
        self.validate_recorded_receipts(request, &attempt, &journal, io)
            .await?;
        let original = journal.clone();
        let mut terminal_ids = self
            .durable_receipt_operation_ids(request, &attempt, &journal, io)
            .await?;
        let mut ready_ids = BTreeSet::new();
        let mut superseded = false;
        let mut pending_legacy = false;
        for recorded in &mut journal.participants {
            if recorded.evidence.is_some() {
                continue;
            }
            let participant = attempt
                .participants
                .iter()
                .find(|p| p.domain == recorded.domain)
                .ok_or_else(|| validation("selected Plan7 attempt omits participant"))?;
            if !matches!(
                participant.plan,
                PersistedRestoreParticipantPlan::ControlMvpV7(_)
            ) {
                pending_legacy = true;
                continue;
            }
            let adapter = self
                .snapshots
                .registry()
                .get(&participant.domain)
                .and_then(|binding| binding.restore_participant())
                .ok_or_else(|| validation("selected Plan7 participant is not configured"))?;
            let operation_id = restore_apply_operation_id(
                request.restore_id(),
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256,
            );
            match self
                .inspect_participant(adapter.as_ref(), participant, io)
                .await
            {
                Ok(RestoreParticipantInspection::Visible { evidence, .. }) => {
                    recorded.evidence = Some(evidence);
                    terminal_ids.insert(operation_id);
                }
                Ok(RestoreParticipantInspection::Superseded) => {
                    superseded = true;
                    terminal_ids.insert(operation_id);
                }
                Ok(RestoreParticipantInspection::Ready) => {
                    ready_ids.insert(operation_id);
                }
                Err(error) => {
                    return self
                        .persist_repair_required(
                            journal,
                            &version,
                            safe_failure_category(&error),
                            io,
                        )
                        .await
                        .map(Some);
                }
            }
        }
        // A pending V6 participant retains the existing replacement workflow.
        // Ready Plan7 plans are carried unchanged; a later superseded observation
        // is rejected again at the shared replacement boundary.
        if pending_legacy && !superseded && journal == original {
            return Ok(None);
        }
        if superseded || pending_legacy {
            journal.status = WorkspaceRestoreStatus::RepairRequired;
            journal.failure_category = Some(if superseded {
                RestoreFailureCategory::CasLost
            } else {
                RestoreFailureCategory::StorageUncertain
            });
        } else if !ready_ids.is_empty() {
            // Generic Ready cannot clear a cancelled/ambiguous unit. The future
            // unit recovery path must provide its separate authenticated proof.
            if self
                .terminal_apply_coordination_is_in_flight(&ready_ids, io)
                .await?
            {
                return self
                    .persist_repair_required(
                        journal,
                        &version,
                        RestoreFailureCategory::StorageUncertain,
                        io,
                    )
                    .await
                    .map(Some);
            }
            let pending = attempt
                .participants
                .iter()
                .find(|p| {
                    journal
                        .participants
                        .iter()
                        .any(|entry| entry.domain == p.domain && entry.evidence.is_none())
                })
                .ok_or_else(|| validation("selected Plan7 has no pending participant"))?;
            let adapter = self
                .snapshots
                .registry()
                .get(&pending.domain)
                .and_then(|binding| binding.restore_participant())
                .ok_or_else(|| validation("selected Plan7 participant is not configured"))?;
            if !adapter.supports_bounded_restore_advance() {
                return Err(CatalogError::UnsupportedOperation {
                    message: "restore participant does not opt in to bounded advance".into(),
                });
            }
            if let Err(error) = self
                .validate_apply_source(request, &attempt, pending, &journal, io)
                .await
            {
                return self
                    .persist_repair_required(journal, &version, safe_failure_category(&error), io)
                    .await
                    .map(Some);
            }
            journal.status = WorkspaceRestoreStatus::Applying;
            journal.failure_category = None;
        }
        if journal != original {
            bump_journal_revision(&mut journal)?;
            let (winner, winner_version) = self.cas_journal(&journal, &version, io).await?;
            if winner != journal {
                return self.outcome(&winner, io).await.map(Some);
            }
            version = winner_version;
        }
        self.validate_recorded_receipts(request, &attempt, &journal, io)
            .await?;
        self.settle_terminal_apply_coordination(&terminal_ids, io)
            .await?;
        if superseded || pending_legacy {
            return self.outcome(&journal, io).await.map(Some);
        }
        self.resume_attempt(request, attempt, journal, version, io)
            .await
            .map(Some)
    }

    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn reconcile_unrecorded_applying(
        &self,
        request: &WorkspaceRestoreRequestRecord,
        mut journal: WorkspaceRestoreJournal,
        version: String,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<Option<WorkspaceRestoreOutcome>> {
        let attempt = self.load_selected_attempt(request, &journal, io).await?;
        let durable_journal = journal.clone();
        // Never revise a repair journal or adopt newly discovered work until every
        // receipt it already contains is still the exact visible participant result.
        // This check uses immutable participant plans and direct reads only; it does
        // not require the retained source to remain active.
        self.validate_recorded_receipts(request, &attempt, &journal, io)
            .await?;
        let mut visible_terminal_ids = self
            .durable_receipt_operation_ids(request, &attempt, &journal, io)
            .await?;
        let mut ready_operation_ids = BTreeSet::new();
        let immutable_cut = if let Some(budget) = io.bounded() {
            self.snapshots
                .immutable_restore_cut_bounded(&request.source()?, request.scope(), budget)
                .await
        } else {
            self.snapshots
                .immutable_restore_cut(&request.source()?, request.scope())
                .await
        };
        let Ok(immutable_cut) = immutable_cut else {
            return self
                .persist_repair_required(
                    journal,
                    &version,
                    RestoreFailureCategory::StorageUncertain,
                    io,
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
            let control_plan = &participant.plan;
            if !control_plan.is_legacy_version() {
                control_plan.validate_source_authority_format()?;
            }
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
                        io,
                    )
                    .await
                    .map(Some);
            };
            match self
                .inspect_participant(adapter.as_ref(), participant, io)
                .await
            {
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
                            io,
                        )
                        .await
                        .map(Some);
                }
            }
        }
        if self
            .terminal_apply_coordination_is_in_flight(&ready_operation_ids, io)
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
                let (winner, _) = self.cas_journal(&journal, &version, io).await?;
                return self.outcome(&winner, io).await.map(Some);
            }
            return self.outcome(&durable_journal, io).await.map(Some);
        }
        if !discovered && !superseded {
            self.settle_terminal_apply_coordination(&visible_terminal_ids, io)
                .await?;
            return Ok(None);
        }
        if journal.status == WorkspaceRestoreStatus::RepairRequired
            && !discovered
            && (!superseded || journal.failure_category == Some(RestoreFailureCategory::CasLost))
        {
            if superseded && journal.failure_category == Some(RestoreFailureCategory::CasLost) {
                visible_terminal_ids.extend(superseded_terminal_ids);
                self.settle_terminal_apply_coordination(&visible_terminal_ids, io)
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
        let (winner, winner_version) = self.cas_journal(&journal, &version, io).await?;
        if was_repair_required && winner.status == WorkspaceRestoreStatus::Applying && !all_visible
        {
            let category = if superseded {
                RestoreFailureCategory::CasLost
            } else {
                RestoreFailureCategory::StorageUncertain
            };
            let outcome = self
                .persist_repair_required(winner, &winner_version, category, io)
                .await?;
            let mut terminal_operation_ids = visible_terminal_ids;
            if category == RestoreFailureCategory::CasLost {
                terminal_operation_ids.extend(superseded_terminal_ids);
            }
            self.settle_terminal_apply_coordination(&terminal_operation_ids, io)
                .await?;
            return Ok(Some(outcome));
        }
        if discovered {
            self.validate_recorded_receipts(request, &attempt, &winner, io)
                .await?;
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
        self.settle_terminal_apply_coordination(&terminal_operation_ids, io)
            .await?;
        if winner.status == WorkspaceRestoreStatus::Visible {
            return self.outcome(&winner, io).await.map(Some);
        }
        if winner
            .participants
            .iter()
            .all(|participant| participant.evidence.is_some())
        {
            let selected_attempt = self.load_selected_attempt(request, &winner, io).await?;
            self.validate_completed_receipts(request, &selected_attempt, &winner, io)
                .await?;
            return self
                .resume_attempt(request, selected_attempt, winner, winner_version, io)
                .await
                .map(Some);
        }
        self.outcome(&winner, io).await.map(Some)
    }

    async fn persist_repair_required(
        &self,
        mut journal: WorkspaceRestoreJournal,
        version: &str,
        category: RestoreFailureCategory,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WorkspaceRestoreOutcome> {
        if journal.status == WorkspaceRestoreStatus::RepairRequired {
            return self.outcome(&journal, io).await;
        }
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(category);
        bump_journal_revision(&mut journal)?;
        let (winner, _) = self.cas_journal(&journal, version, io).await?;
        self.outcome(&winner, io).await
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

    async fn load_journal(
        &self,
        restore_id: &str,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        if let Some(budget) = io.bounded() {
            let (bytes, version) = budget
                .read_exact_stable_record(
                    &self.storage,
                    &restore_journal_path(restore_id)?,
                    RECORD_BYTES,
                )
                .await
                .map_err(|error| match error {
                    CatalogError::NotFound { .. } => CatalogError::NotFound {
                        entity: "workspace restore journal".into(),
                        name: restore_id.into(),
                    },
                    error => error,
                })?;
            let journal: WorkspaceRestoreJournal =
                decode_record(&bytes, "workspace restore journal")?;
            if journal.restore_id != restore_id {
                return Err(validation(
                    "restore journal identity does not match its exact path",
                ));
            }
            journal.validate()?;
            return Ok((journal, version));
        }

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

    async fn read_bounded_record(
        &self,
        path: &str,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<(Bytes, String)> {
        budget
            .read_exact_stable_record(&self.storage, path, RECORD_BYTES)
            .await
    }

    async fn read_record(
        &self,
        path: &str,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(Bytes, String)> {
        match io.bounded() {
            Some(budget) => self.read_bounded_record(path, budget).await,
            None => Ok((self.storage.get_raw(path).await?, String::new())),
        }
    }

    async fn load_optional_journal(
        &self,
        restore_id: &str,

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<Option<(WorkspaceRestoreJournal, String)>> {
        match self.load_journal(restore_id, io).await {
            Ok(journal) => Ok(Some(journal)),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn get_optional_raw(
        &self,
        path: &str,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<Option<Bytes>> {
        if io.is_bounded() {
            return match self.read_record(path, io).await {
                Ok((bytes, _)) => Ok(Some(bytes)),
                Err(CatalogError::NotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            };
        }
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

        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<(WorkspaceRestoreJournal, String)> {
        intended.validate()?;
        let (observed, observed_version) = self.load_journal(&intended.restore_id, io).await?;
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
            .write_record(
                &path,
                Bytes::from(bytes),
                WritePrecondition::MatchesVersion(expected_version.to_string()),
                io,
            )
            .await;
        match write {
            Err(_write_error) => {
                let (winner, version) = self.load_journal(&intended.restore_id, io).await?;
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
                let (winner, version) = self.load_journal(&intended.restore_id, io).await?;
                if journal_winner_is_compatible(intended, &winner) {
                    Ok((winner, version))
                } else {
                    Err(CatalogError::CasFailed {
                        message: "restore journal CAS lost to incompatible state".to_string(),
                    })
                }
            }
            Ok(WriteResult::Success { .. }) => {
                let (winner, version) = self.load_journal(&intended.restore_id, io).await?;
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

    async fn outcome(
        &self,
        journal: &WorkspaceRestoreJournal,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WorkspaceRestoreOutcome> {
        let (request_bytes, _) = self.read_record(&journal.request_path, io).await?;
        if prefixed_sha256(&request_bytes) != journal.request_sha256 {
            return Err(validation("restore journal request checksum mismatch"));
        }
        let request = decode_workspace_restore_request(&request_bytes)?;
        if request.restore_id != journal.restore_id || request.scope != journal.scope {
            return Err(validation(
                "restore journal does not match immutable request identity",
            ));
        }
        self.load_selected_attempt(&request, journal, io).await?;
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
            let path = restore_read_manifest_path(&journal.restore_id)?;
            let digest = journal.read_manifest_sha256.as_deref().ok_or_else(|| {
                validation("visible restore journal omits read manifest checksum")
            })?;
            let bytes = match io.bounded() {
                Some(budget) => {
                    budget
                        .read_immutable(&self.storage, &path, None, digest, RECORD_BYTES)
                        .await?
                }
                None => self.storage.get_raw(&path).await?,
            };
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
    async fn write_record(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: WritePrecondition,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<WriteResult> {
        if let Some(budget) = io.bounded() {
            if bytes.is_empty() || bytes.len() > RECORD_BYTES {
                return Err(validation("restore record exceeds its 4 MiB cap"));
            }
            budget.reserve_bytes(bytes.len())?;
            budget.charge_operations(1)?;
        }
        let result = self.storage.put_raw(path, bytes, precondition).await?;
        if let Some(budget) = io.bounded() {
            budget
                .charge_write(&result)
                .map_err(|_| CatalogError::AmbiguousAuthorityOutcome {
                    message: "restore record PUT response exceeded control admission".into(),
                })?;
        }
        Ok(result)
    }

    async fn put_immutable_exact(
        &self,
        path: &str,
        bytes: Bytes,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        let write = self
            .write_record(path, bytes.clone(), WritePrecondition::DoesNotExist, io)
            .await;
        match write {
            Ok(WriteResult::Success { .. }) => Ok(()),
            Ok(WriteResult::PreconditionFailed { .. }) => {
                if self.read_record(path, io).await?.0 == bytes {
                    Ok(())
                } else {
                    Err(precondition_failed(
                        "immutable restore object already exists with conflicting bytes",
                    ))
                }
            }
            Err(write_error) => match self.read_record(path, io).await {
                Ok((winner, _)) if winner == bytes => Ok(()),
                Ok(_) => Err(precondition_failed(
                    "uncertain immutable restore write selected conflicting bytes",
                )),
                Err(CatalogError::NotFound { .. }) => Err(write_error),
                Err(read_error) => Err(read_error),
            },
        }
    }

    async fn plan_participant(
        &self,
        adapter: &dyn crate::state_store::StateRestoreParticipant,
        source: &crate::state_store::PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        request: &WorkspaceRestoreRequestRecord,
        now: DateTime<Utc>,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<PersistedRestoreParticipantPlan> {
        if let Some(budget) = io.bounded() {
            let deadline = request
                .requested_at
                .checked_add_signed(ChronoDuration::hours(24))
                .ok_or_else(|| validation("restore execution deadline overflow"))?;
            let observed_now = self.now();
            if request.requested_at > observed_now || observed_now >= deadline {
                return Err(validation(
                    "restore request is outside its execution window",
                ));
            }
            let mut context = RestorePlanningContext::new(
                prefixed_sha256(&encode_workspace_restore_request(request)?),
                request.requested_at,
                deadline,
                observed_now,
                WorkspaceCaptureIo::new(&self.storage, budget),
            );
            adapter
                .plan_restore_bounded(source, identity, &mut context)
                .await
        } else {
            adapter.plan_restore(source, identity, now).await
        }
    }

    async fn inspect_participant(
        &self,
        adapter: &dyn crate::state_store::StateRestoreParticipant,
        participant: &RestoreParticipantPlanRecord,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<RestoreParticipantInspection> {
        if let Some(budget) = io.bounded() {
            let plan = &participant.plan;
            let mut context = RestoreBoundedInspectionContext::new(
                plan.identity().restore_id().to_owned(),
                participant.participant_attempt,
                participant.domain.clone(),
                participant.plan_sha256.clone(),
                WorkspaceCaptureIo::new(&self.storage, budget),
            );
            adapter
                .inspect_restore_bounded(&participant.plan, &mut context)
                .await
        } else {
            adapter.inspect_restore(&participant.plan).await
        }
    }

    async fn validated_restore_cut(
        &self,
        source: &RestoreSource,
        scope: &WorkspaceScope,
        domains: &BTreeSet<String>,
        now: DateTime<Utc>,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<PreflightCut> {
        if let Some(budget) = io.bounded() {
            self.snapshots
                .validated_restore_cut_for_domains_bounded(
                    source,
                    scope,
                    domains,
                    self.now(),
                    budget,
                )
                .await
        } else {
            self.snapshots
                .validated_restore_cut_for_domains(source, scope, domains, now)
                .await
        }
    }

    async fn require_active_source_pin(
        &self,
        source: &RestoreSource,
        scope: &WorkspaceScope,
        now: DateTime<Utc>,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        if let Some(budget) = io.bounded() {
            self.snapshots
                .require_active_restore_source_pin_bounded(source, scope, self.now(), budget)
                .await
        } else {
            self.snapshots
                .require_active_restore_source_pin(source, scope, now)
                .await
        }
    }

    async fn acquire_restore_lock(
        &self,
        operation: &str,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<LockGuard<ScopedStorage>> {
        if let Some(budget) = io.bounded() {
            budget
                .acquire_retention_lock(self.storage.clone(), operation)
                .await
        } else {
            Ok(
                DistributedLock::new(Arc::new(self.storage.clone()), RETENTION_GC_LOCK_PATH)
                    .acquire_with_operation(
                        RETENTION_GC_LOCK_TTL,
                        RETENTION_GC_LOCK_MAX_RETRIES,
                        Some(operation.to_owned()),
                    )
                    .await?,
            )
        }
    }

    async fn release_restore_lock(
        guard: LockGuard<ScopedStorage>,
        io: &mut RestoreInvocationIo<'_>,
    ) -> Result<()> {
        if let Some(budget) = io.bounded() {
            budget.release_retention_lock(guard).await
        } else {
            Ok(guard.release().await?)
        }
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
        let plan = &participant.plan;
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

fn decode_attempt_record(bytes: &[u8], context: &str) -> Result<WorkspaceRestoreAttemptPlan> {
    let attempt: WorkspaceRestoreAttemptPlan = decode_record(bytes, context)?;
    if attempt.participants.iter().any(|participant| {
        matches!(
            participant.plan,
            PersistedRestoreParticipantPlan::ControlMvpV7(_)
        )
    }) && canonical_bytes(&attempt, context)? != bytes
    {
        return Err(validation(
            "Plan7 workspace attempt must contain exact canonical bytes",
        ));
    }
    Ok(attempt)
}

fn precondition_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: message.into(),
    }
}

fn validate_attempt_request_binding(
    attempt: &WorkspaceRestoreAttemptPlan,
    request: &WorkspaceRestoreRequestRecord,
) -> Result<()> {
    if !attempt.participants.iter().any(|participant| {
        matches!(
            participant.plan,
            PersistedRestoreParticipantPlan::ControlMvpV7(_)
        )
    }) {
        return Ok(());
    }
    let digest = prefixed_sha256(&encode_workspace_restore_request(request)?);
    if attempt.request_sha256 != digest
        || attempt.restore_id != request.restore_id
        || attempt.scope != request.scope
    {
        return Err(validation(
            "Plan7 attempt differs from original workspace request",
        ));
    }
    for participant in &attempt.participants {
        if let PersistedRestoreParticipantPlan::ControlMvpV7(plan) = &participant.plan {
            plan.validate_workspace_request(&digest, request.requested_at)?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) use plan7_tests::with_unit_selection;

#[cfg(test)]
mod plan7_tests {
    #![allow(clippy::expect_used, clippy::indexing_slicing)]
    use super::*;
    use crate::state_store::{
        ArcoStateTxn, ControlMvpRestoreParticipant, ControlMvpStateStore, DurableAuthorityBinding,
        PersistedAuthorityAdapter, StateRestoreParticipant, StateScope, TxnOptions,
    };

    // The adapters below are fixture-only routing probes. A still builds an
    // actual authority-8 Plan7 through the production bounded planner; B still
    // builds an actual authority-7 V6 plan through the production legacy
    // planner. Only their bounded inspection result is scripted, so this test
    // exercises replacement composition without claiming a native Plan7
    // candidate proof or advance implementation.

    #[derive(Clone, Copy)]
    enum MixedAInspection {
        Visible,
        Ready,
        AdvanceVisible,
        AdvanceInProgress,
        AdvanceNativeUnit,
    }

    struct MixedPlan7Adapter {
        inner: Arc<dyn StateRestoreParticipant>,
        inspection: MixedAInspection,
        visible_token: crate::state_store::StateToken,
        visible_evidence: RestoredAuthorityEvidence,
        plan_calls: AtomicUsize,
        advance_calls: AtomicUsize,
    }

    impl MixedPlan7Adapter {
        fn new(
            inner: Arc<dyn StateRestoreParticipant>,
            inspection: MixedAInspection,
            visible_token: crate::state_store::StateToken,
            visible_evidence: RestoredAuthorityEvidence,
        ) -> Self {
            Self {
                inner,
                inspection,
                visible_token,
                visible_evidence,
                plan_calls: AtomicUsize::new(0),
                advance_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateRestoreParticipant for MixedPlan7Adapter {
        fn implementation(&self) -> &'static str {
            self.inner.implementation()
        }

        fn scope(&self) -> &StateScope {
            self.inner.scope()
        }

        fn restore_binding_identity(&self) -> crate::state_store::StateStoreBindingIdentity {
            self.inner.restore_binding_identity()
        }

        async fn plan_restore(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            now: DateTime<Utc>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.inner.plan_restore(source, identity, now).await
        }

        async fn plan_restore_bounded(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            context: &mut RestorePlanningContext<'_>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            self.inner
                .plan_restore_bounded(source, identity, context)
                .await
        }

        async fn inspect_restore(
            &self,
            plan: &PersistedRestoreParticipantPlan,
        ) -> Result<RestoreParticipantInspection> {
            self.inner.inspect_restore(plan).await
        }

        async fn inspect_restore_bounded(
            &self,
            _plan: &PersistedRestoreParticipantPlan,
            _context: &mut RestoreBoundedInspectionContext<'_>,
        ) -> Result<RestoreParticipantInspection> {
            if matches!(self.inspection, MixedAInspection::AdvanceNativeUnit) {
                return self.inner.inspect_restore_bounded(_plan, _context).await;
            }
            Ok(match self.inspection {
                MixedAInspection::Visible => RestoreParticipantInspection::Visible {
                    token: self.visible_token.clone(),
                    evidence: self.visible_evidence.clone(),
                },
                MixedAInspection::AdvanceVisible
                    if self.advance_calls.load(Ordering::SeqCst) > 0 =>
                {
                    RestoreParticipantInspection::Visible {
                        token: self.visible_token.clone(),
                        evidence: self.visible_evidence.clone(),
                    }
                }
                MixedAInspection::Ready
                | MixedAInspection::AdvanceVisible
                | MixedAInspection::AdvanceInProgress
                | MixedAInspection::AdvanceNativeUnit => RestoreParticipantInspection::Ready,
            })
        }

        fn supports_bounded_restore_advance(&self) -> bool {
            matches!(
                self.inspection,
                MixedAInspection::AdvanceVisible
                    | MixedAInspection::AdvanceInProgress
                    | MixedAInspection::AdvanceNativeUnit
            )
        }

        async fn advance_restore(
            &self,
            plan: &PersistedRestoreParticipantPlan,
            context: &mut RestoreAdvanceContext<'_>,
        ) -> Result<RestoreParticipantAdvance> {
            context.refence().await?;
            self.advance_calls.fetch_add(1, Ordering::SeqCst);
            match self.inspection {
                MixedAInspection::AdvanceNativeUnit => {
                    self.inner.advance_restore(plan, context).await
                }
                MixedAInspection::AdvanceVisible => Ok(RestoreParticipantAdvance::Terminal(
                    RestoreParticipantInspection::Visible {
                        token: self.visible_token.clone(),
                        evidence: self.visible_evidence.clone(),
                    },
                )),
                MixedAInspection::AdvanceInProgress => {
                    Ok(RestoreParticipantAdvance::InProgress { completed_units: 1 })
                }
                _ => Err(CatalogError::UnsupportedOperation {
                    message: "fixture has no advance script".into(),
                }),
            }
        }

        async fn apply_restore(
            &self,
            plan: &PersistedRestoreParticipantPlan,
            now: DateTime<Utc>,
        ) -> Result<RestoreParticipantInspection> {
            self.inner.apply_restore(plan, now).await
        }
    }

    struct MixedV6Adapter {
        inner: Arc<dyn StateRestoreParticipant>,
        plan_calls: AtomicUsize,
        inspect_calls: AtomicUsize,
        superseded_attempt: AtomicUsize,
    }

    impl MixedV6Adapter {
        fn new(inner: Arc<dyn StateRestoreParticipant>) -> Self {
            Self {
                inner,
                plan_calls: AtomicUsize::new(0),
                inspect_calls: AtomicUsize::new(0),
                superseded_attempt: AtomicUsize::new(1),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateRestoreParticipant for MixedV6Adapter {
        fn implementation(&self) -> &'static str {
            self.inner.implementation()
        }

        fn scope(&self) -> &StateScope {
            self.inner.scope()
        }

        fn restore_binding_identity(&self) -> crate::state_store::StateStoreBindingIdentity {
            self.inner.restore_binding_identity()
        }

        async fn plan_restore(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            now: DateTime<Utc>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.inner.plan_restore(source, identity, now).await
        }

        async fn plan_restore_bounded(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            context: &mut RestorePlanningContext<'_>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            // V6 owns the legacy plan bytes. The wrapper exists only because a
            // mixed invocation must never fall through the trait's default
            // bounded rejection before this real V6 planner is reached.
            self.inner
                .plan_restore(source, identity, context.observed_now())
                .await
        }

        async fn inspect_restore(
            &self,
            _plan: &PersistedRestoreParticipantPlan,
        ) -> Result<RestoreParticipantInspection> {
            Ok(RestoreParticipantInspection::Ready)
        }

        async fn inspect_restore_bounded(
            &self,
            _plan: &PersistedRestoreParticipantPlan,
            _context: &mut RestoreBoundedInspectionContext<'_>,
        ) -> Result<RestoreParticipantInspection> {
            // Classification belongs to the selected plan, irrespective of how
            // many recovery reads precede replacement.
            self.inspect_calls.fetch_add(1, Ordering::SeqCst);
            Ok(
                if _plan.identity().attempt()
                    == self.superseded_attempt.load(Ordering::SeqCst) as u64
                {
                    RestoreParticipantInspection::Superseded
                } else {
                    RestoreParticipantInspection::Ready
                },
            )
        }

        async fn apply_restore(
            &self,
            _plan: &PersistedRestoreParticipantPlan,
            _now: DateTime<Utc>,
        ) -> Result<RestoreParticipantInspection> {
            Ok(RestoreParticipantInspection::Ready)
        }
    }

    struct MixedSelectedResumeFixture {
        service: WorkspaceRestoreService,
        request: WorkspaceRestoreRequestRecord,
        initial_attempt: WorkspaceRestoreAttemptPlan,
        visible_evidence: RestoredAuthorityEvidence,
        plan7: Arc<MixedPlan7Adapter>,
        v6: Arc<MixedV6Adapter>,
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one fixture creates real A8 and V7 retained source records before scripting only restore inspection"
    )]
    async fn mixed_selected_resume_fixture(
        a_inspection: MixedAInspection,
    ) -> MixedSelectedResumeFixture {
        mixed_selected_resume_fixture_with_backend(
            a_inspection,
            Arc::new(arco_core::MemoryBackend::new()),
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "shared fixture creates real authority-8 and format-7 source records"
    )]
    async fn mixed_selected_resume_fixture_with_backend(
        a_inspection: MixedAInspection,
        backend: Arc<dyn arco_core::StorageBackend>,
    ) -> MixedSelectedResumeFixture {
        use crate::workspace_snapshot_service::{
            CreateWorkspaceSnapshotRequest, WorkspaceDomainBinding,
        };

        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let catalog_scope = StateScope::new("tenant", "workspace", "catalog");
        let catalog = Arc::new(
            ControlMvpStateStore::new_synthetic_bounded(storage.clone(), catalog_scope.clone())
                .expect("authority-8 catalog")
                .with_durable_authority_binding(DurableAuthorityBinding::new([81; 32])),
        );
        let mut catalog_txn = catalog
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("authority-8 transaction");
        catalog_txn
            .set_logical_operation("mixed-resume-a", "fixture", &"ab".repeat(32))
            .expect("authority-8 operation");
        catalog_txn
            .put(b"a", Bytes::from_static(b"a"))
            .await
            .expect("authority-8 write");
        let catalog_token = catalog_txn
            .commit_v2()
            .await
            .expect("authority-8 commit")
            .token()
            .clone();

        let legacy_scope = StateScope::new("tenant", "workspace", "legacy");
        let legacy = Arc::new(
            ControlMvpStateStore::new(storage.clone(), legacy_scope.clone())
                .expect("authority-7 legacy"),
        );
        let mut legacy_txn = legacy
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("authority-7 transaction");
        legacy_txn
            .put(b"b", Bytes::from_static(b"b"))
            .await
            .expect("authority-7 write");
        legacy_txn.commit().await.expect("authority-7 commit");

        let now = Utc::now();
        let evidence_source = catalog
            .persist_state_reference(&catalog_token, now + ChronoDuration::days(2))
            .await
            .expect("A8 evidence source");
        let visible_evidence = RestoredAuthorityEvidence::new(
            "arco-state-control-mvp",
            catalog_scope.clone(),
            "mixed-plan7-visible",
            evidence_source.manifest_id(),
            evidence_source.manifest_path(),
            evidence_source.manifest_sha256(),
            evidence_source.logical_sequence(),
            1,
        )
        .expect("visible A evidence");
        let plan7 = Arc::new(MixedPlan7Adapter::new(
            Arc::new(ControlMvpRestoreParticipant::new(catalog.as_ref().clone())),
            a_inspection,
            catalog_token,
            visible_evidence.clone(),
        ));
        let v6 = Arc::new(MixedV6Adapter::new(Arc::new(
            ControlMvpRestoreParticipant::new(legacy.as_ref().clone()),
        )));
        let registry = WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            vec![
                WorkspaceDomainBinding::new(
                    catalog_scope.clone(),
                    catalog.clone(),
                    catalog.clone(),
                    Arc::new(EmptyCapture),
                    Arc::new(EmptyCapture),
                )
                .expect("A8 binding")
                .with_restore_participant(plan7.clone())
                .expect("A8 restore participant"),
                WorkspaceDomainBinding::new(
                    legacy_scope.clone(),
                    legacy.clone(),
                    legacy.clone(),
                    Arc::new(EmptyCapture),
                    Arc::new(EmptyCapture),
                )
                .expect("V7 binding")
                .with_restore_participant(v6.clone())
                .expect("V6 restore participant"),
            ],
        )
        .expect("mixed registry");
        let service = WorkspaceRestoreService::new(storage.clone(), registry).expect("service");
        let snapshot_id = format!("snap_{}", Ulid::from(8_101_u128));
        let pin_id = format!("pin_{}", Ulid::from(8_102_u128));
        service
            .snapshots
            .create_snapshot(
                &CreateWorkspaceSnapshotRequest::new(
                    &snapshot_id,
                    &pin_id,
                    now,
                    now + ChronoDuration::days(1),
                    None,
                )
                .expect("snapshot request"),
            )
            .await
            .expect("mixed retained source snapshot");
        let source = RestoreSource::snapshot(snapshot_id, pin_id).expect("snapshot source");
        let request = WorkspaceRestoreRequestRecord::new(
            format!("rst_{}", Ulid::from(8_103_u128)),
            source.clone(),
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            now - ChronoDuration::minutes(1),
            RestoreOperationTarget::Workspace {
                omitted_domain_policy: OmittedDomainPolicy::Reject,
            },
        )
        .expect("restore request");
        let request_raw = encode_workspace_restore_request(&request).expect("canonical request");
        let request_sha256 = prefixed_sha256(&request_raw);
        let required = BTreeSet::from(["catalog".to_string(), "legacy".to_string()]);
        let mut budget = WorkspaceIoBudget::new();
        let cut = service
            .snapshots
            .validated_restore_cut_for_domains_bounded(
                &source,
                request.scope(),
                &required,
                now,
                &mut budget,
            )
            .await
            .expect("mixed bounded source cut");
        let catalog_source = cut
            .domains
            .iter()
            .find(|source| source.domain() == "catalog")
            .expect("catalog source");
        let legacy_source = cut
            .domains
            .iter()
            .find(|source| source.domain() == "legacy")
            .expect("legacy source");
        let plan_a = {
            let identity = RestoreAttemptIdentity::new(request.restore_id(), 1, "catalog")
                .expect("A identity");
            let mut context = RestorePlanningContext::new(
                request_sha256.clone(),
                request.requested_at,
                request.requested_at + ChronoDuration::hours(24),
                now,
                WorkspaceCaptureIo::new(&storage, &mut budget),
            );
            plan7
                .plan_restore_bounded(catalog_source.authority(), &identity, &mut context)
                .await
                .expect("actual A8 Plan7")
        };
        assert!(matches!(
            plan_a,
            PersistedRestoreParticipantPlan::ControlMvpV7(_)
        ));
        let plan_b = {
            let identity =
                RestoreAttemptIdentity::new(request.restore_id(), 1, "legacy").expect("B identity");
            let mut context = RestorePlanningContext::new(
                request_sha256.clone(),
                request.requested_at,
                request.requested_at + ChronoDuration::hours(24),
                now,
                WorkspaceCaptureIo::new(&storage, &mut budget),
            );
            v6.plan_restore_bounded(legacy_source.authority(), &identity, &mut context)
                .await
                .expect("actual V7 source V6 plan")
        };
        assert!(matches!(
            plan_b,
            PersistedRestoreParticipantPlan::ControlMvp(_)
        ));
        let initial_attempt = WorkspaceRestoreAttemptPlan {
            record_type: "workspace_restore_attempt".to_string(),
            version: VERSION,
            restore_id: request.restore_id().to_string(),
            aggregate_attempt: 1,
            scope: request.scope.clone(),
            request_sha256: request_sha256.clone(),
            source_record_sha256: cut.source_record_sha256,
            active_retention_deadline: cut.usable_retention_deadline,
            participants: vec![
                RestoreParticipantPlanRecord::new("catalog", 1, plan_a).expect("A participant"),
                RestoreParticipantPlanRecord::new("legacy", 1, plan_b).expect("B participant"),
            ],
            omitted_domains: vec![],
        };
        initial_attempt.validate().expect("mixed selected attempt");
        MixedSelectedResumeFixture {
            service,
            request,
            initial_attempt,
            visible_evidence,
            plan7,
            v6,
        }
    }

    async fn install_mixed_selected_attempt(
        fixture: &MixedSelectedResumeFixture,
        completed_a: bool,
    ) {
        let request_path =
            restore_request_path(fixture.request.restore_id()).expect("request path");
        let attempt_path =
            restore_attempt_plan_path(fixture.request.restore_id(), 1).expect("attempt path");
        let mut journal = selected_plan7_journal(&fixture.initial_attempt);
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(RestoreFailureCategory::CasLost);
        if completed_a {
            journal
                .participants
                .iter_mut()
                .find(|entry| entry.domain == "catalog")
                .expect("A journal entry")
                .evidence = Some(fixture.visible_evidence.clone());
        }
        journal.validate().expect("mixed repair journal");
        for (path, raw) in [
            (
                request_path,
                encode_workspace_restore_request(&fixture.request).expect("request"),
            ),
            (
                attempt_path,
                canonical_bytes(&fixture.initial_attempt, "attempt").expect("attempt"),
            ),
            (
                restore_journal_path(fixture.request.restore_id()).expect("journal path"),
                canonical_bytes(&journal, "journal").expect("journal"),
            ),
        ] {
            fixture
                .service
                .storage
                .put_raw(&path, Bytes::from(raw), WritePrecondition::DoesNotExist)
                .await
                .expect("selected record");
        }
    }

    async fn selected_mixed_records(
        fixture: &MixedSelectedResumeFixture,
    ) -> (WorkspaceRestoreJournal, WorkspaceRestoreAttemptPlan) {
        let journal: WorkspaceRestoreJournal = decode_record(
            &fixture
                .service
                .storage
                .get_raw(&restore_journal_path(fixture.request.restore_id()).expect("journal path"))
                .await
                .expect("journal"),
            "journal",
        )
        .expect("journal decode");
        let attempt: WorkspaceRestoreAttemptPlan = decode_attempt_record(
            &fixture
                .service
                .storage
                .get_raw(&journal.attempt_path)
                .await
                .expect("selected attempt"),
            "selected attempt",
        )
        .expect("attempt decode");
        (journal, attempt)
    }

    #[cfg(feature = "test-utils")]
    struct ExpireAfterProgress {
        inner: arco_core::MemoryBackend,
        ordinal: u64,
        expired: std::sync::atomic::AtomicBool,
        late_selectors: AtomicUsize,
    }

    #[cfg(feature = "test-utils")]
    #[async_trait::async_trait]
    impl arco_core::StorageBackend for ExpireAfterProgress {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }
        async fn get_range(
            &self,
            path: &str,
            range: std::ops::Range<u64>,
        ) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }
        async fn get_range_with_ownership(
            &self,
            path: &str,
            range: std::ops::Range<u64>,
        ) -> arco_core::Result<arco_core::storage::ClassifiedBytes> {
            self.inner.get_range_with_ownership(path, range).await
        }
        async fn put(
            &self,
            path: &str,
            data: Bytes,
            condition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if path.ends_with("/selector.json") && self.expired.load(Ordering::SeqCst) {
                self.late_selectors.fetch_add(1, Ordering::SeqCst);
            }
            let result = self.inner.put(path, data, condition).await;
            if path.ends_with(&format!("/progress/{:020}.json", self.ordinal)) {
                self.expired.store(true, Ordering::SeqCst);
            }
            result
        }
        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }
        async fn list(&self, path: &str) -> arco_core::Result<Vec<arco_core::ObjectMeta>> {
            self.inner.list(path).await
        }
        async fn head(&self, path: &str) -> arco_core::Result<Option<arco_core::ObjectMeta>> {
            self.inner.head(path).await
        }
        async fn signed_url(&self, path: &str, duration: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, duration).await
        }
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn native_restore_driver_refences_after_immutable_staging() {
        for ordinal in [0, 1] {
            let backend = Arc::new(ExpireAfterProgress {
                inner: arco_core::MemoryBackend::new(),
                ordinal,
                expired: std::sync::atomic::AtomicBool::new(false),
                late_selectors: AtomicUsize::new(0),
            });
            let mut fixture = mixed_selected_resume_fixture_with_backend(
                MixedAInspection::AdvanceNativeUnit,
                backend.clone(),
            )
            .await;
            fixture.v6.superseded_attempt.store(0, Ordering::SeqCst);
            install_mixed_selected_attempt(&fixture, false).await;
            let now = Utc::now();
            let deadline = fixture.request.requested_at + ChronoDuration::hours(24);
            let clock_backend = backend.clone();
            fixture.service.clock = Some(Arc::new(move || {
                if clock_backend.expired.load(Ordering::SeqCst) {
                    deadline
                } else {
                    now
                }
            }));
            let _outcome = fixture
                .service
                .recover_restore(fixture.request.restore_id())
                .await;
            assert!(
                backend.expired.load(Ordering::SeqCst),
                "reached immutable progress {ordinal}"
            );
            assert_eq!(
                backend.late_selectors.load(Ordering::SeqCst),
                0,
                "selector after deadline, ordinal {ordinal}"
            );
        }
    }

    #[tokio::test]
    async fn native_restore_driver_publishes_one_unit_without_changing_head() {
        let fixture = mixed_selected_resume_fixture(MixedAInspection::AdvanceNativeUnit).await;
        fixture.v6.superseded_attempt.store(0, Ordering::SeqCst);
        install_mixed_selected_attempt(&fixture, false).await;
        let paths = crate::state_store::control_mvp::ControlMvpPaths::new("catalog");
        let head_before = fixture
            .service
            .storage
            .get_raw(&paths.current_pointer())
            .await
            .expect("HEAD");
        for call in 1..=2 {
            let outcome = fixture
                .service
                .recover_restore(fixture.request.restore_id())
                .await
                .expect("native unit");
            assert_eq!(
                outcome.status(),
                WorkspaceRestoreStatus::Applying,
                "call {call}"
            );
            assert!(outcome.read_manifest().is_none());
            assert_eq!(fixture.plan7.advance_calls.load(Ordering::SeqCst), call);
        }
        let (journal, attempt) = selected_mixed_records(&fixture).await;
        assert!(journal.participants.iter().all(|p| p.evidence.is_none()));
        let participant = attempt
            .participants
            .iter()
            .find(|p| p.domain == "catalog")
            .expect("catalog");
        let plan = serde_json::to_value(&participant.plan).expect("plan");
        let candidate = plan["candidate_id"].as_str().expect("candidate");
        let selector_path = format!(
            "{}/restore/v7/{candidate}/selector.json",
            paths.base_prefix()
        );
        let selector: Value = serde_json::from_slice(
            &fixture
                .service
                .storage
                .get_raw(&selector_path)
                .await
                .expect("selector"),
        )
        .expect("JSON");
        let progress: Value = serde_json::from_slice(
            &fixture
                .service
                .storage
                .get_raw(selector["current_progress_path"].as_str().expect("path"))
                .await
                .expect("progress"),
        )
        .expect("JSON");
        assert_eq!(progress["next_ordinal"], 2);
        assert_eq!(progress["receipt_count"], 2);
        assert_eq!(progress["terminal"], true);
        assert_eq!(
            fixture
                .service
                .storage
                .get_raw(&paths.current_pointer())
                .await
                .expect("same HEAD"),
            head_before
        );
    }

    #[tokio::test]
    async fn bounded_advance_stops_after_one_participant_including_visible_result() {
        for inspection in [
            MixedAInspection::AdvanceInProgress,
            MixedAInspection::AdvanceVisible,
        ] {
            let fixture = mixed_selected_resume_fixture(inspection).await;
            fixture.v6.superseded_attempt.store(0, Ordering::SeqCst);
            install_mixed_selected_attempt(&fixture, false).await;
            let outcome = fixture
                .service
                .recover_restore(fixture.request.restore_id())
                .await
                .expect("one bounded advance must return before unsupported B");
            assert_eq!(outcome.status(), WorkspaceRestoreStatus::Applying);
            assert!(outcome.read_manifest().is_none());
            assert_eq!(fixture.plan7.advance_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.plan7.plan_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.v6.plan_calls.load(Ordering::SeqCst), 1);
            let (journal, selected) = selected_mixed_records(&fixture).await;
            assert_eq!(selected, fixture.initial_attempt);
            let a = journal
                .participants
                .iter()
                .find(|p| p.domain == "catalog")
                .expect("A");
            let b = journal
                .participants
                .iter()
                .find(|p| p.domain == "legacy")
                .expect("B");
            assert_eq!(
                a.evidence.is_some(),
                matches!(inspection, MixedAInspection::AdvanceVisible)
            );
            assert!(b.evidence.is_none());
            let epoch: Value = serde_json::from_slice(
                &fixture
                    .service
                    .storage
                    .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("epoch"),
            )
            .expect("JSON");
            assert_eq!(epoch["state"], "IDLE");
        }
    }

    #[tokio::test]
    async fn mixed_ready_plan7_inflight_blocks_v6_replacement() {
        let fixture = mixed_selected_resume_fixture(MixedAInspection::Ready).await;
        install_mixed_selected_attempt(&fixture, false).await;
        let (mut journal, _) = selected_mixed_records(&fixture).await;
        journal.status = WorkspaceRestoreStatus::Applying;
        journal.failure_category = None;
        let journal_path = restore_journal_path(fixture.request.restore_id()).expect("path");
        let version = fixture
            .service
            .storage
            .head_raw(&journal_path)
            .await
            .expect("HEAD")
            .expect("journal")
            .version;
        fixture
            .service
            .storage
            .put_raw(
                &journal_path,
                Bytes::from(canonical_bytes(&journal, "fixture journal").expect("bytes")),
                WritePrecondition::MatchesVersion(version),
            )
            .await
            .expect("fixture applying journal");
        let participant = &fixture.initial_attempt.participants[0];
        let mut budget = WorkspaceIoBudget::new();
        let mut io = RestoreInvocationIo::Bounded(&mut budget);
        let (guard, mut epoch) = fixture
            .service
            .acquire_apply_coordination(
                fixture.request.restore_id(),
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256,
                &mut io,
            )
            .await
            .expect("claim selected Plan7 operation");
        {
            let _cancelled = epoch.begin_bounded_mutation();
        }
        WorkspaceRestoreService::release_restore_lock(guard, &mut io)
            .await
            .expect("release lease");
        let epoch_path = crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH;
        let epoch_before = fixture
            .service
            .storage
            .get_raw(epoch_path)
            .await
            .expect("epoch");
        let epoch_record: Value = serde_json::from_slice(&epoch_before).expect("epoch JSON");
        assert_eq!(epoch_record["state"], "IN_FLIGHT");
        assert_eq!(
            epoch_record["operation_id"],
            restore_apply_operation_id(
                fixture.request.restore_id(),
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256
            )
        );
        let outcome = fixture
            .service
            .recover_restore(fixture.request.restore_id())
            .await
            .expect("read-only mixed reconciliation");
        assert_eq!(outcome.status(), WorkspaceRestoreStatus::RepairRequired);
        let (selected_journal, selected) = selected_mixed_records(&fixture).await;
        assert_eq!(selected, fixture.initial_attempt);
        assert_eq!(selected_journal.participants, journal.participants);
        assert_eq!(selected_journal.attempt_sha256, journal.attempt_sha256);
        assert_eq!(
            selected_journal.failure_category,
            Some(RestoreFailureCategory::StorageUncertain)
        );
        assert_eq!(fixture.plan7.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.v6.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .service
                .storage
                .get_raw(epoch_path)
                .await
                .expect("epoch"),
            epoch_before
        );
        assert!(
            fixture
                .service
                .storage
                .head_raw(
                    &restore_attempt_plan_path(fixture.request.restore_id(), 2).expect("path")
                )
                .await
                .expect("HEAD")
                .is_none()
        );
    }

    #[tokio::test]
    async fn mixed_completed_plan7_a_replaces_only_superseded_v6_b() {
        let fixture = mixed_selected_resume_fixture(MixedAInspection::Visible).await;
        install_mixed_selected_attempt(&fixture, true).await;
        let original_a = fixture
            .initial_attempt
            .participants
            .iter()
            .find(|participant| participant.domain == "catalog")
            .expect("original A")
            .clone();
        let original_b = fixture
            .initial_attempt
            .participants
            .iter()
            .find(|participant| participant.domain == "legacy")
            .expect("original B")
            .clone();

        let _ = fixture
            .service
            .recover_restore(fixture.request.restore_id())
            .await;
        let (journal, selected) = selected_mixed_records(&fixture).await;
        assert_eq!(
            selected.aggregate_attempt, 2,
            "B must get a replacement attempt"
        );
        assert_eq!(
            selected.participants.len(),
            1,
            "completed A stays only in its origin attempt"
        );
        let replacement_b = selected.participants.first().expect("replacement B");
        assert_eq!(replacement_b.domain, "legacy");
        assert_eq!(replacement_b.participant_attempt, 2);
        assert_ne!(replacement_b.plan_sha256, original_b.plan_sha256);
        let recorded_a = journal
            .participants
            .iter()
            .find(|participant| participant.domain == "catalog")
            .expect("recorded A");
        assert_eq!(
            recorded_a.participant_attempt,
            original_a.participant_attempt
        );
        assert_eq!(recorded_a.plan_sha256, original_a.plan_sha256);
        assert_eq!(
            recorded_a.evidence.as_ref(),
            Some(&fixture.visible_evidence)
        );
        assert_eq!(
            fixture.plan7.plan_calls.load(Ordering::SeqCst),
            1,
            "A must not replan"
        );
        assert_eq!(
            fixture.v6.plan_calls.load(Ordering::SeqCst),
            2,
            "B replans exactly once"
        );
        assert_eq!(
            selected.request_sha256,
            fixture.initial_attempt.request_sha256
        );
        assert_eq!(
            selected.active_retention_deadline,
            fixture.initial_attempt.active_retention_deadline
        );
    }

    #[tokio::test]
    async fn mixed_ready_plan7_a_is_carried_exactly_while_superseded_v6_b_replans() {
        let fixture = mixed_selected_resume_fixture(MixedAInspection::Ready).await;
        install_mixed_selected_attempt(&fixture, false).await;
        let original_a = fixture
            .initial_attempt
            .participants
            .iter()
            .find(|participant| participant.domain == "catalog")
            .expect("original A")
            .clone();
        let original_b = fixture
            .initial_attempt
            .participants
            .iter()
            .find(|participant| participant.domain == "legacy")
            .expect("original B")
            .clone();

        let _ = fixture
            .service
            .recover_restore(fixture.request.restore_id())
            .await;
        let (journal, selected) = selected_mixed_records(&fixture).await;
        assert_eq!(
            selected.aggregate_attempt, 2,
            "B must get a replacement attempt"
        );
        let carried_a = selected
            .participants
            .iter()
            .find(|participant| participant.domain == "catalog")
            .expect("carried A");
        assert_eq!(
            carried_a, &original_a,
            "A Plan7 owner and raw plan digest must be exact"
        );
        let replacement_b = selected
            .participants
            .iter()
            .find(|participant| participant.domain == "legacy")
            .expect("replacement B");
        assert_eq!(replacement_b.participant_attempt, 2);
        assert_ne!(replacement_b.plan_sha256, original_b.plan_sha256);
        let recorded_a = journal
            .participants
            .iter()
            .find(|participant| participant.domain == "catalog")
            .expect("recorded A");
        assert_eq!(
            recorded_a.participant_attempt,
            original_a.participant_attempt
        );
        assert_eq!(recorded_a.plan_sha256, original_a.plan_sha256);
        assert!(
            recorded_a.evidence.is_none(),
            "Ready A remains pending without synthetic evidence"
        );
        assert_eq!(
            fixture.plan7.plan_calls.load(Ordering::SeqCst),
            1,
            "A must not replan"
        );
        assert_eq!(
            fixture.v6.plan_calls.load(Ordering::SeqCst),
            2,
            "B replans exactly once"
        );
        assert_eq!(
            selected.request_sha256,
            fixture.initial_attempt.request_sha256
        );
        assert_eq!(
            selected.active_retention_deadline,
            fixture.initial_attempt.active_retention_deadline
        );
    }

    #[tokio::test]
    async fn plan7_attempt_rejects_noncanonical_raw_bytes_before_value_loses_duplicates() {
        let storage = ScopedStorage::new(
            Arc::new(arco_core::MemoryBackend::new()),
            "tenant",
            "workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store")
        .with_durable_authority_binding(DurableAuthorityBinding::new([36; 32]));
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("txn");
        txn.set_logical_operation("plan7-raw", "test", &"ab".repeat(32))
            .expect("operation");
        txn.put(b"key", Bytes::from_static(b"value"))
            .await
            .expect("put");
        let token = txn.commit_v2().await.expect("commit").token().clone();
        let now = Utc::now();
        let source = store
            .persist_state_reference(&token, now + ChronoDuration::days(2))
            .await
            .expect("source");
        let identity =
            RestoreAttemptIdentity::new(format!("rst_{}", Ulid::from(706_u128)), 1, "catalog")
                .expect("identity");
        let request_digest = prefixed_sha256(b"workspace-request");
        let mut budget = WorkspaceIoBudget::new();
        let mut context = RestorePlanningContext::new(
            request_digest.clone(),
            now,
            now + ChronoDuration::hours(24),
            now,
            WorkspaceCaptureIo::new(&storage, &mut budget),
        );
        let plan = ControlMvpRestoreParticipant::new(store)
            .plan_restore_bounded(&source, &identity, &mut context)
            .await
            .expect("Plan7");
        let attempt = WorkspaceRestoreAttemptPlan {
            record_type: "workspace_restore_attempt".into(),
            version: VERSION,
            restore_id: identity.restore_id().into(),
            aggregate_attempt: 1,
            scope: WorkspaceScope::new("tenant", "workspace").expect("scope"),
            request_sha256: request_digest,
            source_record_sha256: prefixed_sha256(b"source-record"),
            active_retention_deadline: source.retention_deadline(),
            participants: vec![
                RestoreParticipantPlanRecord::new("catalog", 1, plan).expect("participant"),
            ],
            omitted_domains: vec![],
        };
        attempt.validate().expect("valid fixture");
        let raw = canonical_bytes(&attempt, "attempt").expect("raw");
        assert_eq!(
            decode_attempt_record(&raw, "attempt").expect("canonical"),
            attempt
        );
        let text = String::from_utf8(raw).expect("UTF8");
        for changed in [
            format!(" {text}"),
            text.replace(
                "\"owner_generation\":1",
                "\"owner_generation\":1,\"owner_generation\":1",
            ),
            text.replace("control_mvp_v7", "control_mvp_v\\u0037"),
        ] {
            assert_ne!(changed, text);
            assert!(
                decode_attempt_record(changed.as_bytes(), "attempt").is_err(),
                "accepted noncanonical Plan7 attempt"
            );
        }
    }
    struct EmptyCapture;
    #[async_trait::async_trait]
    impl crate::workspace_snapshot_service::ProjectionWatermarkProvider for EmptyCapture {
        async fn capture(
            &self,
            _: &crate::workspace_snapshot::DomainAuthorityReference,
        ) -> Result<crate::workspace_snapshot_service::ProjectionWatermarkCut> {
            crate::workspace_snapshot_service::ProjectionWatermarkCut::new(vec![], vec![], vec![])
        }
        async fn capture_bounded(
            &self,
            authority: &crate::workspace_snapshot::DomainAuthorityReference,
            _: &mut WorkspaceCaptureIo<'_>,
        ) -> Result<crate::workspace_snapshot_service::ProjectionWatermarkCut> {
            self.capture(authority).await
        }
    }
    #[async_trait::async_trait]
    impl crate::workspace_snapshot_service::EventArchiveProvider for EmptyCapture {
        async fn capture(
            &self,
            authority: &crate::workspace_snapshot::DomainAuthorityReference,
        ) -> Result<crate::workspace_snapshot_service::EventArchiveCapture> {
            crate::workspace_snapshot_service::EventArchiveCapture::new(
                crate::workspace_snapshot::DomainEventArchive::empty(authority.domain())?,
                vec![],
            )
        }
        async fn capture_bounded(
            &self,
            authority: &crate::workspace_snapshot::DomainAuthorityReference,
            _: &mut WorkspaceCaptureIo<'_>,
        ) -> Result<crate::workspace_snapshot_service::EventArchiveCapture> {
            self.capture(authority).await
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "construct actual retained snapshot and detached restore records"
    )]
    async fn detached_plan7_fixture(
        mismatch: &str,
    ) -> (WorkspaceRestoreService, WorkspaceRestoreRequestRecord) {
        detached_plan7_fixture_with_script(mismatch, None).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the optional test adapter preserves the single real fixture path"
    )]
    async fn detached_plan7_fixture_with_script(
        mismatch: &str,
        scripted: Option<Arc<ScriptedAdvanceProbe>>,
    ) -> (WorkspaceRestoreService, WorkspaceRestoreRequestRecord) {
        use crate::workspace_snapshot_service::{
            CreateWorkspaceSnapshotRequest, WorkspaceDomainBinding,
        };
        let storage = ScopedStorage::new(
            Arc::new(arco_core::MemoryBackend::new()),
            "tenant",
            "workspace",
        )
        .expect("storage");
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = Arc::new(
            ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
                .expect("store")
                .with_durable_authority_binding(DurableAuthorityBinding::new([41; 32])),
        );
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("txn");
        txn.set_logical_operation("request-binding", "test", &"ab".repeat(32))
            .expect("operation");
        txn.put(b"key", Bytes::from_static(b"value"))
            .await
            .expect("put");
        txn.commit_v2().await.expect("commit");
        let concrete = Arc::new(ControlMvpRestoreParticipant::new(store.as_ref().clone()));
        let adapter: Arc<dyn StateRestoreParticipant> = match scripted {
            Some(scripted) => {
                scripted.install(concrete);
                scripted
            }
            None => concrete,
        };
        let registry = WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            vec![
                WorkspaceDomainBinding::new(
                    scope,
                    store.clone(),
                    store.clone(),
                    Arc::new(EmptyCapture),
                    Arc::new(EmptyCapture),
                )
                .expect("binding")
                .with_restore_participant(adapter.clone())
                .expect("restore binding"),
            ],
        )
        .expect("registry");
        let service = WorkspaceRestoreService::new(storage.clone(), registry).expect("service");
        let now = Utc::now();
        let snapshot_id = format!("snap_{}", Ulid::from(710_u128));
        let pin_id = format!("pin_{}", Ulid::from(711_u128));
        service
            .snapshots
            .create_snapshot(
                &CreateWorkspaceSnapshotRequest::new(
                    &snapshot_id,
                    &pin_id,
                    now,
                    now + ChronoDuration::days(2),
                    None,
                )
                .expect("snapshot request"),
            )
            .await
            .expect("snapshot");
        let source = RestoreSource::snapshot(snapshot_id, pin_id).expect("source");
        let request = WorkspaceRestoreRequestRecord::new(
            format!("rst_{}", Ulid::from(712_u128)),
            source.clone(),
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            now - ChronoDuration::minutes(2),
            RestoreOperationTarget::Workspace {
                omitted_domain_policy: OmittedDomainPolicy::Reject,
            },
        )
        .expect("request");
        let raw_request = encode_workspace_restore_request(&request).expect("request bytes");
        let request_sha = prefixed_sha256(&raw_request);
        let request_bytes = if mismatch == "raw" {
            Bytes::from([b" ".as_slice(), &raw_request].concat())
        } else {
            Bytes::from(raw_request)
        };
        storage
            .put_raw(
                &restore_request_path(request.restore_id()).expect("path"),
                request_bytes,
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("orphan request");
        if mismatch != "raw" {
            let mut budget = WorkspaceIoBudget::new();
            let domains = BTreeSet::from(["catalog".to_string()]);
            let cut = service
                .snapshots
                .validated_restore_cut_for_domains_bounded(
                    &source,
                    request.scope(),
                    &domains,
                    now,
                    &mut budget,
                )
                .await
                .expect("source cut");
            let identity =
                RestoreAttemptIdentity::new(request.restore_id(), 1, "catalog").expect("identity");
            let planned_time = request.requested_at
                + if mismatch == "time" {
                    ChronoDuration::minutes(1)
                } else {
                    ChronoDuration::zero()
                };
            let mut context = RestorePlanningContext::new(
                if mismatch == "digest" {
                    prefixed_sha256(b"another workspace request")
                } else {
                    request_sha.clone()
                },
                planned_time,
                planned_time + ChronoDuration::hours(24),
                now,
                WorkspaceCaptureIo::new(&storage, &mut budget),
            );
            let plan = adapter
                .plan_restore_bounded(cut.domains[0].authority(), &identity, &mut context)
                .await
                .expect("self-consistent Plan7");
            let attempt = WorkspaceRestoreAttemptPlan {
                record_type: "workspace_restore_attempt".into(),
                version: VERSION,
                restore_id: request.restore_id().into(),
                aggregate_attempt: 1,
                scope: request.scope.clone(),
                request_sha256: request_sha,
                source_record_sha256: cut.source_record_sha256,
                active_retention_deadline: cut.usable_retention_deadline,
                participants: vec![
                    RestoreParticipantPlanRecord::new("catalog", 1, plan).expect("participant"),
                ],
                omitted_domains: vec![],
            };
            attempt.validate().expect("attempt");
            storage
                .put_raw(
                    &restore_attempt_plan_path(request.restore_id(), 1).expect("path"),
                    Bytes::from(canonical_bytes(&attempt, "attempt").expect("bytes")),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .expect("orphan attempt");
        }
        (service, request)
    }

    async fn assert_detached_plan7_rejected(mismatch: &str) {
        let (service, request) = detached_plan7_fixture(mismatch).await;
        let before = plan7_inventory(&service).await;
        let result = service.recover_restore(request.restore_id()).await;
        assert!(
            matches!(result, Err(CatalogError::Validation { .. })),
            "{mismatch}: {result:?}"
        );
        assert_eq!(
            before,
            plan7_inventory(&service).await,
            "admission published artifacts for {mismatch}"
        );
        assert!(
            service
                .storage
                .head_raw(&restore_journal_path(request.restore_id()).expect("path"))
                .await
                .expect("head")
                .is_none()
        );
    }

    #[tokio::test]
    async fn plan7_orphan_rejects_another_original_request_digest() {
        assert_detached_plan7_rejected("digest").await;
    }
    #[tokio::test]
    async fn plan7_orphan_rejects_an_extended_original_request_window() {
        assert_detached_plan7_rejected("time").await;
    }
    #[tokio::test]
    async fn plan7_orphan_rejects_noncanonical_original_request_bytes() {
        assert_detached_plan7_rejected("raw").await;
    }
    #[tokio::test]
    async fn plan7_selected_attempt_rejects_another_original_request_on_read() {
        let (service, request) = detached_plan7_fixture("digest").await;
        let path = restore_attempt_plan_path(request.restore_id(), 1).expect("path");
        let raw = service.storage.get_raw(&path).await.expect("attempt");
        let attempt = decode_attempt_record(&raw, "attempt").expect("attempt");
        let journal = WorkspaceRestoreJournal {
            record_type: "workspace_restore_journal".into(),
            version: VERSION,
            restore_id: request.restore_id().into(),
            revision: 1,
            status: WorkspaceRestoreStatus::Prepared,
            scope: request.scope.clone(),
            request_sha256: attempt.request_sha256.clone(),
            request_path: restore_request_path(request.restore_id()).expect("path"),
            aggregate_attempt: 1,
            attempt_path: path,
            attempt_sha256: prefixed_sha256(&raw),
            required_domains: vec!["catalog".into()],
            participants: attempt
                .participants
                .iter()
                .map(|p| RestoreJournalParticipant {
                    domain: p.domain.clone(),
                    participant_attempt: p.participant_attempt,
                    plan_sha256: p.plan_sha256.clone(),
                    evidence: None,
                })
                .collect(),
            omitted_domains: vec![],
            failure_category: None,
            read_manifest_path: restore_read_manifest_path(request.restore_id()).expect("path"),
            finalized_at: None,
            read_manifest_sha256: None,
        };
        journal.validate().expect("journal fixture");
        service
            .storage
            .put_raw(
                &restore_journal_path(request.restore_id()).expect("path"),
                Bytes::from(canonical_bytes(&journal, "journal").expect("bytes")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("selected journal");
        let before = plan7_inventory(&service).await;
        let result = service.get_restore(request.restore_id()).await;
        assert!(
            matches!(result, Err(CatalogError::Validation { .. })),
            "{result:?}"
        );
        assert_eq!(before, plan7_inventory(&service).await);
    }
    #[tokio::test]
    async fn plan7_valid_orphan_prepares_but_cannot_enter_legacy_apply() {
        let (service, request) = detached_plan7_fixture("valid").await;
        let result = service.recover_restore(request.restore_id()).await;
        assert!(matches!(
            result,
            Err(CatalogError::UnsupportedOperation { .. })
        ));
        let outcome = service
            .get_restore(request.restore_id())
            .await
            .expect("prepared Plan7 remains inspectable");
        assert_eq!(outcome.status(), WorkspaceRestoreStatus::Applying);
        assert!(outcome.read_manifest().is_none());
    }
    async fn plan7_inventory(service: &WorkspaceRestoreService) -> BTreeSet<(String, String, u64)> {
        service
            .storage
            .backend()
            .list("")
            .await
            .expect("inventory")
            .into_iter()
            .map(|o| (o.path, o.version, o.size))
            .collect()
    }

    fn selected_plan7_journal(attempt: &WorkspaceRestoreAttemptPlan) -> WorkspaceRestoreJournal {
        let journal = WorkspaceRestoreJournal {
            record_type: "workspace_restore_journal".into(),
            version: VERSION,
            restore_id: attempt.restore_id.clone(),
            revision: 1,
            status: WorkspaceRestoreStatus::Prepared,
            scope: attempt.scope.clone(),
            request_sha256: attempt.request_sha256.clone(),
            request_path: restore_request_path(&attempt.restore_id).expect("path"),
            aggregate_attempt: attempt.aggregate_attempt,
            attempt_path: restore_attempt_plan_path(&attempt.restore_id, attempt.aggregate_attempt)
                .expect("path"),
            attempt_sha256: prefixed_sha256(&canonical_bytes(attempt, "attempt").expect("bytes")),
            required_domains: attempt
                .participants
                .iter()
                .map(|p| p.domain.clone())
                .collect(),
            participants: attempt
                .participants
                .iter()
                .map(|p| RestoreJournalParticipant {
                    domain: p.domain.clone(),
                    participant_attempt: p.participant_attempt,
                    plan_sha256: p.plan_sha256.clone(),
                    evidence: None,
                })
                .collect(),
            omitted_domains: vec![],
            failure_category: None,
            read_manifest_path: restore_read_manifest_path(&attempt.restore_id).expect("path"),
            finalized_at: None,
            read_manifest_sha256: None,
        };
        journal.validate().expect("journal");
        journal
    }

    async fn plan7_for_request(
        store: &ControlMvpStateStore,
        storage: &ScopedStorage,
        source: &PersistedAuthorityReference,
        request: &WorkspaceRestoreRequestRecord,
        attempt: u64,
        digest: String,
    ) -> PersistedRestoreParticipantPlan {
        let identity =
            RestoreAttemptIdentity::new(request.restore_id(), attempt, source.scope().domain())
                .expect("identity");
        let mut budget = WorkspaceIoBudget::new();
        let mut context = RestorePlanningContext::new(
            digest,
            request.requested_at,
            request.requested_at + ChronoDuration::hours(24),
            Utc::now(),
            WorkspaceCaptureIo::new(storage, &mut budget),
        );
        ControlMvpRestoreParticipant::new(store.clone())
            .plan_restore_bounded(source, &identity, &mut context)
            .await
            .expect("Plan7")
    }

    #[tokio::test]
    async fn plan7_selected_superseded_does_not_adopt_unselected_orphan() {
        let (service, request) = detached_plan7_fixture("valid").await;
        let path1 = restore_attempt_plan_path(request.restore_id(), 1).expect("path");
        let raw1 = service.storage.get_raw(&path1).await.expect("attempt1");
        let attempt1 = decode_attempt_record(&raw1, "attempt1").expect("attempt1");
        let mut journal = selected_plan7_journal(&attempt1);
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(RestoreFailureCategory::CasLost);
        journal.validate().expect("CAS-lost journal");
        let journal_path = restore_journal_path(request.restore_id()).expect("path");
        service
            .storage
            .put_raw(
                &journal_path,
                Bytes::from(canonical_bytes(&journal, "journal").expect("bytes")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("journal");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            service.storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store")
        .with_durable_authority_binding(DurableAuthorityBinding::new([41; 32]));
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("txn");
        txn.set_logical_operation("replacement-new-head", "test", &"cd".repeat(32))
            .expect("operation");
        txn.put(b"key", Bytes::from_static(b"changed"))
            .await
            .expect("put");
        txn.commit_v2().await.expect("advance HEAD");
        let plan2 = plan7_for_request(
            &store,
            &service.storage,
            attempt1.participants[0].plan.source(),
            &request,
            2,
            prefixed_sha256(b"foreign request"),
        )
        .await;
        let mut attempt2 = attempt1;
        attempt2.aggregate_attempt = 2;
        attempt2.participants =
            vec![RestoreParticipantPlanRecord::new("catalog", 2, plan2).expect("participant")];
        attempt2.validate().expect("attempt2");
        service
            .storage
            .put_raw(
                &restore_attempt_plan_path(request.restore_id(), 2).expect("path"),
                Bytes::from(canonical_bytes(&attempt2, "attempt2").expect("bytes")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("orphan2");
        let before = plan7_inventory(&service).await;
        let result = service.recover_restore(request.restore_id()).await;
        assert_eq!(
            result.expect("selected supersession").status(),
            WorkspaceRestoreStatus::RepairRequired
        );
        assert_eq!(
            before,
            plan7_inventory(&service).await,
            "forged replacement changed the journal or candidate inventory"
        );
    }

    #[tokio::test]
    async fn plan7_request_route_omitted_origin_rejects_false_request_on_read() {
        let (service, request) = detached_plan7_fixture("digest").await;
        let raw1 = service
            .storage
            .get_raw(&restore_attempt_plan_path(request.restore_id(), 1).expect("path"))
            .await
            .expect("origin");
        let attempt1 = decode_attempt_record(&raw1, "origin").expect("origin");
        let source_a = attempt1.participants[0].plan.source();
        let store_b = ControlMvpStateStore::new_synthetic_bounded(
            service.storage.clone(),
            StateScope::new("tenant", "workspace", "other"),
        )
        .expect("store B")
        .with_durable_authority_binding(DurableAuthorityBinding::new([41; 32]));
        let mut txn = store_b
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("txn");
        txn.set_logical_operation("origin-domain-b", "test", &"cd".repeat(32))
            .expect("operation");
        txn.put(b"key", Bytes::from_static(b"domain B"))
            .await
            .expect("put");
        let token = txn.commit_v2().await.expect("commit B").token().clone();
        let source_b = store_b
            .persist_state_reference(&token, source_a.retention_deadline())
            .await
            .expect("source B");
        let plan_b = plan7_for_request(
            &store_b,
            &service.storage,
            &source_b,
            &request,
            2,
            prefixed_sha256(&encode_workspace_restore_request(&request).expect("request")),
        )
        .await;
        let mut attempt2 = attempt1.clone();
        attempt2.aggregate_attempt = 2;
        attempt2.participants =
            vec![RestoreParticipantPlanRecord::new("other", 2, plan_b).expect("participant B")];
        attempt2.validate().expect("attempt2");
        validate_attempt_request_binding(&attempt2, &request).expect("active B is correctly bound");
        let mut journal = selected_plan7_journal(&attempt2);
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(RestoreFailureCategory::StorageUncertain);
        journal.required_domains.insert(0, "catalog".into());
        // The recorded receipt is only a claim. Origin admission must reject its
        // wrong request before it could be used as authority for completed A.
        let evidence = RestoredAuthorityEvidence::new(
            "arco-state-control-mvp",
            source_a.scope().clone(),
            "claimed-restore",
            source_a.manifest_id(),
            source_a.manifest_path(),
            source_a.manifest_sha256(),
            source_a.logical_sequence(),
            1,
        )
        .expect("evidence shape");
        journal.participants.insert(
            0,
            RestoreJournalParticipant {
                domain: "catalog".into(),
                participant_attempt: 1,
                plan_sha256: attempt1.participants[0].plan_sha256.clone(),
                evidence: Some(evidence),
            },
        );
        journal
            .validate()
            .expect("journal with omitted completed A");
        for (path, raw) in [
            (
                restore_attempt_plan_path(request.restore_id(), 2).expect("path"),
                canonical_bytes(&attempt2, "attempt2").expect("bytes"),
            ),
            (
                restore_journal_path(request.restore_id()).expect("path"),
                canonical_bytes(&journal, "journal").expect("bytes"),
            ),
        ] {
            service
                .storage
                .put_raw(&path, Bytes::from(raw), WritePrecondition::DoesNotExist)
                .await
                .expect("fixture");
        }
        let before = plan7_inventory(&service).await;
        let result = service.get_restore(request.restore_id()).await;
        assert!(
            matches!(result, Err(CatalogError::Validation { .. })),
            "{result:?}"
        );
        assert_eq!(before, plan7_inventory(&service).await);
    }
    use crate::state_store::{
        PersistedAuthorityReference, RestoreAdvanceContext, RestoreAttemptIdentity,
        RestoreParticipantAdvance,
    };
    use std::sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::sync::Notify;

    enum ScriptedAdvance {
        InProgress,
        UnsupportedAfterRefence,
        OversizedError {
            operations_at_return: Arc<AtomicUsize>,
        },
        ZeroUnits,
        PauseAfterRefence {
            reached: Arc<Notify>,
            release: Arc<Notify>,
        },
    }

    struct ScriptedAdvanceProbe {
        inner: OnceLock<Arc<dyn StateRestoreParticipant>>,
        script: ScriptedAdvance,
        advance_calls: AtomicUsize,
        legacy_apply_calls: AtomicUsize,
    }

    impl ScriptedAdvanceProbe {
        fn in_progress() -> Self {
            Self {
                inner: OnceLock::new(),
                script: ScriptedAdvance::InProgress,
                advance_calls: AtomicUsize::new(0),
                legacy_apply_calls: AtomicUsize::new(0),
            }
        }

        fn pause_after_refence(reached: Arc<Notify>, release: Arc<Notify>) -> Self {
            Self {
                inner: OnceLock::new(),
                script: ScriptedAdvance::PauseAfterRefence { reached, release },
                advance_calls: AtomicUsize::new(0),
                legacy_apply_calls: AtomicUsize::new(0),
            }
        }

        fn install(&self, concrete: Arc<dyn StateRestoreParticipant>) {
            assert!(
                self.inner.set(concrete).is_ok(),
                "fixture installs one adapter"
            );
        }

        fn inner(&self) -> &Arc<dyn StateRestoreParticipant> {
            self.inner.get().expect("fixture installed real adapter")
        }
    }

    #[async_trait::async_trait]
    impl StateRestoreParticipant for ScriptedAdvanceProbe {
        fn implementation(&self) -> &'static str {
            self.inner().implementation()
        }

        fn scope(&self) -> &StateScope {
            self.inner().scope()
        }

        fn restore_binding_identity(&self) -> crate::state_store::StateStoreBindingIdentity {
            self.inner().restore_binding_identity()
        }

        async fn plan_restore(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            now: DateTime<Utc>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.inner().plan_restore(source, identity, now).await
        }

        async fn plan_restore_bounded(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            context: &mut RestorePlanningContext<'_>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.inner()
                .plan_restore_bounded(source, identity, context)
                .await
        }

        async fn inspect_restore(
            &self,
            plan: &PersistedRestoreParticipantPlan,
        ) -> Result<RestoreParticipantInspection> {
            self.inner().inspect_restore(plan).await
        }

        async fn inspect_restore_bounded(
            &self,
            plan: &PersistedRestoreParticipantPlan,
            context: &mut RestoreBoundedInspectionContext<'_>,
        ) -> Result<RestoreParticipantInspection> {
            self.inner().inspect_restore_bounded(plan, context).await
        }

        fn supports_bounded_restore_advance(&self) -> bool {
            true
        }

        async fn advance_restore(
            &self,
            _plan: &PersistedRestoreParticipantPlan,
            context: &mut RestoreAdvanceContext<'_>,
        ) -> Result<RestoreParticipantAdvance> {
            assert!(context.is_bounded(), "route must lend bounded capability");
            context.refence().await?;
            self.advance_calls.fetch_add(1, Ordering::SeqCst);
            match &self.script {
                ScriptedAdvance::OversizedError {
                    operations_at_return,
                } => {
                    let RestoreAdvanceMode::Bounded(fence) = &context.mode else {
                        panic!("expected bounded advance");
                    };
                    operations_at_return.store(fence.budget.test_accounting().1, Ordering::SeqCst);
                    let mut message =
                        String::with_capacity(crate::workspace_io_budget::METADATA_BYTES + 1);
                    message.push_str("oversized decoder error");
                    Err(CatalogError::InvariantViolation { message })
                }
                ScriptedAdvance::UnsupportedAfterRefence => {
                    Err(CatalogError::UnsupportedOperation {
                        message: "fixture opted-in adapter has already performed refence I/O"
                            .into(),
                    })
                }
                ScriptedAdvance::ZeroUnits => {
                    Ok(RestoreParticipantAdvance::InProgress { completed_units: 0 })
                }
                ScriptedAdvance::InProgress => {
                    Ok(RestoreParticipantAdvance::InProgress { completed_units: 1 })
                }
                ScriptedAdvance::PauseAfterRefence { reached, release } => {
                    reached.notify_one();
                    release.notified().await;
                    Ok(RestoreParticipantAdvance::InProgress { completed_units: 1 })
                }
            }
        }

        async fn apply_restore(
            &self,
            plan: &PersistedRestoreParticipantPlan,
            now: DateTime<Utc>,
        ) -> Result<RestoreParticipantInspection> {
            self.legacy_apply_calls.fetch_add(1, Ordering::SeqCst);
            self.inner().apply_restore(plan, now).await
        }
    }

    struct DefaultAdvanceProbe {
        inner: Arc<dyn StateRestoreParticipant>,
        apply_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StateRestoreParticipant for DefaultAdvanceProbe {
        fn implementation(&self) -> &'static str {
            self.inner.implementation()
        }

        fn scope(&self) -> &StateScope {
            self.inner.scope()
        }

        fn restore_binding_identity(&self) -> crate::state_store::StateStoreBindingIdentity {
            self.inner.restore_binding_identity()
        }

        async fn plan_restore(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            now: DateTime<Utc>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.inner.plan_restore(source, identity, now).await
        }

        async fn plan_restore_bounded(
            &self,
            source: &PersistedAuthorityReference,
            identity: &RestoreAttemptIdentity,
            context: &mut RestorePlanningContext<'_>,
        ) -> Result<PersistedRestoreParticipantPlan> {
            self.inner
                .plan_restore_bounded(source, identity, context)
                .await
        }

        async fn inspect_restore(
            &self,
            plan: &PersistedRestoreParticipantPlan,
        ) -> Result<RestoreParticipantInspection> {
            self.inner.inspect_restore(plan).await
        }

        async fn inspect_restore_bounded(
            &self,
            plan: &PersistedRestoreParticipantPlan,
            context: &mut RestoreBoundedInspectionContext<'_>,
        ) -> Result<RestoreParticipantInspection> {
            self.inner.inspect_restore_bounded(plan, context).await
        }

        // Deliberately no advance_restore override: these tests exercise the trait default.
        async fn apply_restore(
            &self,
            _plan: &PersistedRestoreParticipantPlan,
            _now: DateTime<Utc>,
        ) -> Result<RestoreParticipantInspection> {
            self.apply_calls.fetch_add(1, Ordering::SeqCst);
            Ok(RestoreParticipantInspection::Ready)
        }
    }

    struct LiveFenceFixture {
        service: WorkspaceRestoreService,
        request: WorkspaceRestoreRequestRecord,
        attempt: WorkspaceRestoreAttemptPlan,
        journal: WorkspaceRestoreJournal,
        journal_version: String,
        budget: WorkspaceIoBudget,
        guard: LockGuard<ScopedStorage>,
        epoch: RetentionMutationEpoch,
    }

    impl LiveFenceFixture {
        fn context(&mut self) -> RestoreAdvanceContext<'_> {
            let participant = &self.attempt.participants[0];
            let adapter = self
                .service
                .snapshots
                .registry()
                .get(&participant.domain)
                .expect("catalog")
                .restore_participant()
                .expect("adapter");
            RestoreAdvanceContext {
                mode: RestoreAdvanceMode::Bounded(RestoreAdvanceFence {
                    service: &self.service,
                    request: &self.request,
                    attempt: &self.attempt,
                    participant,
                    journal: &self.journal,
                    journal_version: &self.journal_version,
                    adapter: adapter.as_ref(),
                    epoch: &self.epoch,
                    guard: &mut self.guard,
                    budget: &mut self.budget,
                }),
            }
        }
    }

    #[tokio::test]
    async fn plan7_unit_selection_rejects_other_supplied_plan() {
        let mut fixture = live_fence_fixture().await;
        let other = live_fence_fixture().await;
        let supplied = &other.attempt.participants[0].plan;
        assert_ne!(supplied, &fixture.attempt.participants[0].plan);
        assert!(
            matches!(
                fixture.context().unit_selection(supplied).await,
                Err(CatalogError::Validation { .. })
            ),
            "a valid different plan cannot borrow the selected record's digest"
        );
    }

    #[tokio::test]
    async fn plan7_unit_selection_rejects_stale_journal() {
        let mut fixture = live_fence_fixture().await;
        let supplied = fixture.attempt.participants[0].plan.clone();
        fixture
            .service
            .storage
            .put_raw(
                &restore_journal_path(fixture.request.restore_id()).expect("path"),
                Bytes::from(canonical_bytes(&fixture.journal, "journal").expect("bytes")),
                WritePrecondition::MatchesVersion(fixture.journal_version.clone()),
            )
            .await
            .expect("same body, new version");
        let other = live_fence_fixture().await;
        let different = &other.attempt.participants[0].plan;
        assert_ne!(&supplied, different);
        for plan in [&supplied, different] {
            assert!(
                matches!(
                    fixture.context().unit_selection(plan).await,
                    Err(CatalogError::PreconditionFailed { .. })
                ),
                "journal refencing must reject before comparing the supplied plan"
            );
        }
    }

    #[tokio::test]
    async fn plan7_unit_selection_borrows_exact_record_and_original_budget() {
        let mut fixture = live_fence_fixture().await;
        let participant = &fixture.attempt.participants[0];
        let supplied = participant.plan.clone();
        let plan_address = std::ptr::from_ref(&participant.plan);
        let digest_address = std::ptr::from_ref(participant.plan_sha256.as_str());
        let budget_address = std::ptr::from_ref(&fixture.budget);
        let before = fixture.budget.test_accounting();
        let after;
        {
            let mut context = fixture.context();
            {
                let mut selected = context.unit_selection(&supplied).await.expect("selected");
                let (plan, digest, budget) = selected.parts();
                assert!(std::ptr::eq(plan, plan_address));
                assert!(std::ptr::eq(digest, digest_address));
                assert!(std::ptr::eq(budget, budget_address));
                assert_eq!(
                    digest,
                    prefixed_sha256(&canonical_bytes(plan, "plan").expect("wire"))
                );
                let PersistedRestoreParticipantPlan::ControlMvpV7(bare_plan) = plan else {
                    panic!("Plan7 fixture");
                };
                assert_ne!(
                    digest,
                    prefixed_sha256(&canonical_bytes(bare_plan, "bare plan").expect("wire"))
                );
                let fenced = budget.test_accounting();
                assert!(fenced.0 > before.0 && fenced.1 > before.1);
                budget.reserve_bytes(123).expect("same invoice");
                after = budget.test_accounting();
                assert_eq!(after.0, fenced.0 + 123);
                tokio::task::yield_now().await;
            }
            context.refence().await.expect("refence after loan drops");
        }
        assert!(fixture.budget.test_accounting().0 > after.0);
        fixture
            .epoch
            .settle_bounded(&mut fixture.guard, &mut fixture.budget)
            .await
            .expect("settle");
    }

    #[tokio::test]
    async fn plan7_unit_selection_rejects_legacy_context() {
        let fixture = live_fence_fixture().await;
        let mut context = RestoreAdvanceContext {
            mode: RestoreAdvanceMode::Legacy(Utc::now()),
        };
        assert!(
            matches!(context.unit_selection(&fixture.attempt.participants[0].plan).await,
            Err(CatalogError::Validation { message }) if message.contains("legacy restore"))
        );
    }

    pub async fn with_unit_selection<R>(
        run: impl FnOnce(&mut RestoreUnitSelection<'_>, &ControlMvpStateStore) -> R,
    ) -> R {
        let mut fixture = live_fence_fixture().await;
        let supplied = fixture.attempt.participants[0].plan.clone();
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.service.storage.clone(),
            supplied.source().scope().clone(),
        )
        .expect("selected store")
        .with_durable_authority_binding(DurableAuthorityBinding::new([41; 32]));
        let mut context = fixture.context();
        let mut selection = context
            .unit_selection(&supplied)
            .await
            .expect("live selection");
        run(&mut selection, &store)
    }

    async fn live_fence_fixture() -> LiveFenceFixture {
        let (service, request) = detached_plan7_fixture("valid").await;
        let attempt_path =
            restore_attempt_plan_path(request.restore_id(), 1).expect("attempt path");
        let attempt = decode_attempt_record(
            &service
                .storage
                .get_raw(&attempt_path)
                .await
                .expect("attempt"),
            "attempt",
        )
        .expect("attempt");
        let mut journal = selected_plan7_journal(&attempt);
        journal.status = WorkspaceRestoreStatus::Applying;
        journal.validate().expect("Applying journal");
        let journal_path = restore_journal_path(request.restore_id()).expect("journal path");
        let journal_bytes =
            Bytes::from(canonical_bytes(&journal, "journal").expect("journal bytes"));
        service
            .storage
            .put_raw(
                &journal_path,
                journal_bytes,
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("journal");
        let journal_version = service
            .storage
            .head_raw(&journal_path)
            .await
            .expect("journal HEAD")
            .expect("journal exists")
            .version;
        let participant = attempt.participants.first().expect("participant");
        let mut budget = WorkspaceIoBudget::new();
        let mut io = RestoreInvocationIo::Bounded(&mut budget);
        let (guard, epoch) = service
            .acquire_apply_coordination(
                request.restore_id(),
                participant.participant_attempt,
                &participant.domain,
                &participant.plan_sha256,
                &mut io,
            )
            .await
            .expect("coordination");
        LiveFenceFixture {
            service,
            request,
            attempt,
            journal,
            journal_version,
            budget,
            guard,
            epoch,
        }
    }

    #[tokio::test]
    async fn plan7_advance_legacy_default_forwards_one_apply_and_preserves_terminal() {
        let (service, request) = detached_plan7_fixture("valid").await;
        let attempt = decode_attempt_record(
            &service
                .storage
                .get_raw(&restore_attempt_plan_path(request.restore_id(), 1).expect("attempt path"))
                .await
                .expect("attempt"),
            "attempt",
        )
        .expect("attempt");
        let registered = service
            .snapshots
            .registry()
            .get("catalog")
            .expect("catalog")
            .restore_participant()
            .expect("adapter")
            .clone();
        let probe = DefaultAdvanceProbe {
            inner: registered,
            apply_calls: AtomicUsize::new(0),
        };
        let now = Utc::now();
        let mut context = RestoreAdvanceContext {
            mode: RestoreAdvanceMode::Legacy(now),
        };
        assert_eq!(
            probe
                .advance_restore(&attempt.participants[0].plan, &mut context,)
                .await
                .expect("legacy default"),
            RestoreParticipantAdvance::Terminal(RestoreParticipantInspection::Ready)
        );
        assert_eq!(probe.apply_calls.load(Ordering::SeqCst), 1);
        assert_eq!(context.observed_now(), now);
        assert!(!context.is_bounded());
    }

    #[tokio::test]
    async fn plan7_advance_bounded_default_rejects_before_legacy_apply() {
        let LiveFenceFixture {
            service,
            request,
            attempt,
            journal,
            journal_version,
            mut budget,
            mut guard,
            epoch,
        } = live_fence_fixture().await;
        let participant = &attempt.participants[0];
        let registered = service
            .snapshots
            .registry()
            .get(&participant.domain)
            .expect("catalog")
            .restore_participant()
            .expect("adapter")
            .clone();
        let probe = DefaultAdvanceProbe {
            inner: registered,
            apply_calls: AtomicUsize::new(0),
        };
        let mut context = RestoreAdvanceContext {
            mode: RestoreAdvanceMode::Bounded(RestoreAdvanceFence {
                service: &service,
                request: &request,
                attempt: &attempt,
                participant,
                journal: &journal,
                journal_version: &journal_version,
                adapter: &probe,
                epoch: &epoch,
                guard: &mut guard,
                budget: &mut budget,
            }),
        };
        assert!(matches!(
            probe.advance_restore(&participant.plan, &mut context).await,
            Err(CatalogError::UnsupportedOperation { .. })
        ));
        assert_eq!(probe.apply_calls.load(Ordering::SeqCst), 0);
        assert!(context.is_bounded());
        epoch
            .settle_bounded(&mut guard, &mut budget)
            .await
            .expect("settle test epoch");
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn plan7_advance_refence_rejects_execution_deadline_equality() {
        let LiveFenceFixture {
            mut service,
            request,
            attempt,
            journal,
            journal_version,
            mut budget,
            mut guard,
            epoch,
        } = live_fence_fixture().await;
        let deadline = request.requested_at + ChronoDuration::hours(24);
        service.clock = Some(Arc::new(move || deadline));
        let participant = &attempt.participants[0];
        let adapter = service
            .snapshots
            .registry()
            .get(&participant.domain)
            .expect("catalog")
            .restore_participant()
            .expect("adapter");
        let mut context = RestoreAdvanceContext {
            mode: RestoreAdvanceMode::Bounded(RestoreAdvanceFence {
                service: &service,
                request: &request,
                attempt: &attempt,
                participant,
                journal: &journal,
                journal_version: &journal_version,
                adapter: adapter.as_ref(),
                epoch: &epoch,
                guard: &mut guard,
                budget: &mut budget,
            }),
        };
        assert!(
            matches!(context.refence().await, Err(CatalogError::Validation { message })
            if message.contains("outside its original execution or source window"))
        );
        epoch
            .settle_bounded(&mut guard, &mut budget)
            .await
            .expect("settle");
    }

    #[tokio::test]
    async fn plan7_advance_refence_rejects_replaced_journal_version() {
        let LiveFenceFixture {
            service,
            request,
            attempt,
            journal,
            journal_version,
            mut budget,
            mut guard,
            epoch,
        } = live_fence_fixture().await;
        let participant = &attempt.participants[0];
        let adapter = service
            .snapshots
            .registry()
            .get(&participant.domain)
            .expect("catalog")
            .restore_participant()
            .expect("adapter");
        let journal_path = restore_journal_path(request.restore_id()).expect("journal path");
        service
            .storage
            .put_raw(
                &journal_path,
                Bytes::from(canonical_bytes(&journal, "journal").expect("journal bytes")),
                WritePrecondition::MatchesVersion(journal_version.clone()),
            )
            .await
            .expect("same journal under a new version");
        let mut context = RestoreAdvanceContext {
            mode: RestoreAdvanceMode::Bounded(RestoreAdvanceFence {
                service: &service,
                request: &request,
                attempt: &attempt,
                participant,
                journal: &journal,
                journal_version: &journal_version,
                adapter: adapter.as_ref(),
                epoch: &epoch,
                guard: &mut guard,
                budget: &mut budget,
            }),
        };
        assert!(
            context.refence().await.is_err(),
            "same bytes under a new journal version cannot retain the live capability"
        );
        epoch
            .settle_bounded(&mut guard, &mut budget)
            .await
            .expect("settle test epoch");
    }
    async fn apply_coordination_inventory(
        service: &WorkspaceRestoreService,
    ) -> [Option<(String, u64)>; 2] {
        let paths = [
            crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH,
            RETENTION_GC_LOCK_PATH,
        ];
        let mut observed = [None, None];
        for (slot, path) in observed.iter_mut().zip(paths) {
            *slot = service
                .storage
                .head_raw(path)
                .await
                .expect("coordination HEAD")
                .map(|meta| (meta.version, meta.size));
        }
        observed
    }

    #[tokio::test]
    async fn plan7_advance_default_denial_preserves_apply_coordination_objects() {
        let (service, request) = detached_plan7_fixture("valid").await;
        let before = apply_coordination_inventory(&service).await;

        let result = service.recover_restore(request.restore_id()).await;
        assert!(matches!(
            result,
            Err(CatalogError::UnsupportedOperation { .. })
        ));
        assert_eq!(
            apply_coordination_inventory(&service).await,
            before,
            "default bounded-advance denial must precede apply lock and epoch claim"
        );
    }

    #[tokio::test]
    async fn plan7_advance_in_progress_keeps_applying_without_evidence_and_settles_epoch() {
        let probe = Arc::new(ScriptedAdvanceProbe::in_progress());
        let (service, request) =
            detached_plan7_fixture_with_script("valid", Some(probe.clone())).await;

        let outcome = service
            .recover_restore(request.restore_id())
            .await
            .expect("bounded InProgress is nonterminal");
        assert_eq!(outcome.status(), WorkspaceRestoreStatus::Applying);
        assert!(outcome.read_manifest().is_none());
        assert_eq!(probe.advance_calls.load(Ordering::SeqCst), 1);
        assert_eq!(probe.legacy_apply_calls.load(Ordering::SeqCst), 0);

        let journal: WorkspaceRestoreJournal = decode_record(
            &service
                .storage
                .get_raw(&restore_journal_path(request.restore_id()).expect("journal path"))
                .await
                .expect("journal"),
            "journal",
        )
        .expect("journal decode");
        assert_eq!(journal.status, WorkspaceRestoreStatus::Applying);
        assert!(
            journal
                .participants
                .iter()
                .all(|entry| entry.evidence.is_none())
        );
        assert!(journal.read_manifest_sha256.is_none());

        let epoch: Value = serde_json::from_slice(
            &service
                .storage
                .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("epoch"),
        )
        .expect("epoch JSON");
        assert_eq!(epoch["state"], "IDLE");
    }

    #[tokio::test]
    async fn plan7_oversized_returned_error_stops_before_inspection_or_cleanup_io() {
        let operations = Arc::new(AtomicUsize::new(0));
        let mut configured = ScriptedAdvanceProbe::in_progress();
        configured.script = ScriptedAdvance::OversizedError {
            operations_at_return: operations.clone(),
        };
        let probe = Arc::new(configured);
        let (service, request) = detached_plan7_fixture_with_script("valid", Some(probe)).await;
        let journal_path = restore_journal_path(request.restore_id()).expect("journal path");
        let attempt = decode_attempt_record(
            &service
                .storage
                .get_raw(&restore_attempt_plan_path(request.restore_id(), 1).expect("attempt path"))
                .await
                .expect("attempt"),
            "attempt",
        )
        .expect("decode attempt");
        let mut journal = selected_plan7_journal(&attempt);
        journal.status = WorkspaceRestoreStatus::Applying;
        service
            .storage
            .put_raw(
                &journal_path,
                canonical_bytes(&journal, "journal")
                    .expect("encode journal")
                    .into(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("selected journal");
        let before = service
            .storage
            .head_raw(&journal_path)
            .await
            .expect("HEAD")
            .expect("journal")
            .version;
        let mut budget = WorkspaceIoBudget::new();
        let result =
            Box::pin(service.restore(&request, &mut RestoreInvocationIo::Bounded(&mut budget)))
                .await;
        assert!(
            matches!(result, Err(CatalogError::InvariantViolation { ref message }) if message == "oversized decoder error"),
            "{result:?}"
        );
        assert!(budget.test_accounting().0 > crate::workspace_io_budget::METADATA_BYTES);
        assert_eq!(
            budget.test_accounting().1,
            operations.load(Ordering::SeqCst)
        );
        assert_eq!(
            service
                .storage
                .head_raw(&journal_path)
                .await
                .expect("HEAD")
                .expect("journal")
                .version,
            before
        );
        let epoch: Value = serde_json::from_slice(
            &service
                .storage
                .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("epoch"),
        )
        .expect("epoch JSON");
        assert_eq!(epoch["state"], "IN_FLIGHT");
    }

    #[tokio::test]
    async fn plan7_opted_in_error_or_invalid_progress_never_clears_uncertainty() {
        for script in [
            ScriptedAdvance::UnsupportedAfterRefence,
            ScriptedAdvance::ZeroUnits,
        ] {
            let mut configured = ScriptedAdvanceProbe::in_progress();
            configured.script = script;
            let probe = Arc::new(configured);
            let (service, request) =
                detached_plan7_fixture_with_script("valid", Some(probe.clone())).await;
            let result = service.recover_restore(request.restore_id()).await;
            assert!(
                result.as_ref().map_or(true, |outcome| outcome.status()
                    == WorkspaceRestoreStatus::RepairRequired),
                "{result:?}"
            );
            assert_eq!(probe.advance_calls.load(Ordering::SeqCst), 1);
            assert_eq!(probe.legacy_apply_calls.load(Ordering::SeqCst), 0);
            let epoch: Value = serde_json::from_slice(
                &service
                    .storage
                    .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("epoch"),
            )
            .expect("epoch JSON");
            assert_eq!(epoch["state"], "IN_FLIGHT");
            let journal: WorkspaceRestoreJournal = decode_record(
                &service
                    .storage
                    .get_raw(&restore_journal_path(request.restore_id()).expect("path"))
                    .await
                    .expect("journal"),
                "journal",
            )
            .expect("decode");
            assert!(journal.participants.iter().all(|p| p.evidence.is_none()));
            assert!(journal.read_manifest_sha256.is_none());
        }
    }

    #[tokio::test]
    async fn plan7_advance_cancellation_after_refence_leaves_epoch_in_flight() {
        let reached = Arc::new(Notify::new());
        let probe = Arc::new(ScriptedAdvanceProbe::pause_after_refence(
            reached.clone(),
            Arc::new(Notify::new()),
        ));
        let (service, request) =
            detached_plan7_fixture_with_script("valid", Some(probe.clone())).await;
        let service = Arc::new(service);
        let restore_id = request.restore_id().to_owned();
        let runner = service.clone();
        let task = tokio::spawn(async move { runner.recover_restore(&restore_id).await });

        tokio::time::timeout(Duration::from_secs(10), reached.notified())
            .await
            .expect("advance passed refence");
        task.abort();
        assert!(task.await.expect_err("cancelled task").is_cancelled());
        assert_eq!(probe.advance_calls.load(Ordering::SeqCst), 1);
        assert_eq!(probe.legacy_apply_calls.load(Ordering::SeqCst), 0);

        let epoch: Value = serde_json::from_slice(
            &service
                .storage
                .get_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("epoch"),
        )
        .expect("epoch JSON");
        assert_eq!(epoch["state"], "IN_FLIGHT");
    }

    #[tokio::test]
    async fn plan7_selected_superseded_repair_never_replans() {
        let (service, request) = detached_plan7_fixture("valid").await;
        let path = restore_attempt_plan_path(request.restore_id(), 1).expect("path");
        let raw = service.storage.get_raw(&path).await.expect("attempt");
        let attempt = decode_attempt_record(&raw, "attempt").expect("attempt");
        let mut journal = selected_plan7_journal(&attempt);
        journal.status = WorkspaceRestoreStatus::RepairRequired;
        journal.failure_category = Some(RestoreFailureCategory::CasLost);
        service
            .storage
            .put_raw(
                &restore_journal_path(request.restore_id()).expect("path"),
                Bytes::from(canonical_bytes(&journal, "journal").expect("bytes")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("journal");
        let head_path = crate::state_store::ControlMvpPaths::new("catalog").current_pointer();
        let head = service
            .storage
            .head_raw(&head_path)
            .await
            .expect("HEAD")
            .expect("exists");
        let head_bytes = service
            .storage
            .get_raw(&head_path)
            .await
            .expect("HEAD bytes");
        assert!(matches!(
            service
                .storage
                .put_raw(
                    &head_path,
                    head_bytes,
                    WritePrecondition::MatchesVersion(head.version),
                )
                .await
                .expect("advance HEAD version"),
            WriteResult::Success { .. }
        ));
        let result = service.recover_restore(request.restore_id()).await;
        let mut budget = WorkspaceIoBudget::new();
        let (selected, _) = service
            .load_journal(
                request.restore_id(),
                &mut RestoreInvocationIo::Bounded(&mut budget),
            )
            .await
            .expect("selected");
        assert_eq!(
            selected.aggregate_attempt, 1,
            "selected Plan7 was replanned: {result:?}"
        );
        assert_eq!(selected.attempt_sha256, journal.attempt_sha256);
        assert_eq!(selected.participants, journal.participants);
        assert_eq!(selected.status, WorkspaceRestoreStatus::RepairRequired);
        assert_eq!(
            selected.failure_category,
            Some(RestoreFailureCategory::CasLost)
        );
        assert!(
            service
                .storage
                .head_raw(&restore_attempt_plan_path(request.restore_id(), 2).expect("path"))
                .await
                .expect("HEAD")
                .is_none()
        );
        assert_eq!(service.storage.get_raw(&path).await.expect("attempt"), raw);
        assert_eq!(
            result.expect("terminal selected repair").status(),
            WorkspaceRestoreStatus::RepairRequired
        );
    }
}
