//! Shared control-plane transaction paths and storage record types.
//!
//! This module sketches the shared `transactions/...` layout for catalog,
//! orchestration, and root visibility-scoped commits without changing the
//! existing writers yet. Catalog and orchestration remain pointer-published;
//! root transactions use a tx-scoped record plus immutable super-manifest.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::storage_keys::LockKey;

/// Stable discriminator for durable transaction handle records.
pub const CONTROL_PLANE_HANDLE_RECORD_TYPE: &str = "control_plane_transaction_handle";

/// Current durable transaction handle record version.
pub const CONTROL_PLANE_HANDLE_RECORD_VERSION: u32 = 1;

fn handle_validation(message: impl Into<String>) -> Error {
    Error::Validation {
        message: message.into(),
    }
}

fn validate_handle_text(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(handle_validation(format!(
            "{field} must be nonblank and contain no control characters"
        )));
    }
    Ok(())
}

fn validate_handle_digest(value: &str, field: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(handle_validation(format!(
            "{field} must use the sha256: prefix"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(handle_validation(format!(
            "{field} must contain 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_handle_relative_path(path: &str, field: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || matches!(path.as_bytes(), [drive, b':', ..] if drive.is_ascii_alphabetic())
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(handle_validation(format!(
            "{field} must be a canonical relative path"
        )));
    }
    Ok(())
}

/// Validates a canonical path-safe durable transaction handle identifier.
///
/// # Errors
///
/// Returns [`Error::InvalidId`] unless `handle_id` is an `hdl_` prefix followed
/// by exactly one canonical uppercase ULID.
pub fn validate_handle_id(handle_id: &str) -> Result<()> {
    let Some(ulid) = handle_id.strip_prefix("hdl_") else {
        return Err(Error::InvalidId {
            message: "handle_id must start with hdl_".to_string(),
        });
    };
    if ulid.len() != 26 {
        return Err(Error::InvalidId {
            message: "handle_id must contain exactly one 26-character ULID".to_string(),
        });
    }
    let parsed = Ulid::from_string(ulid).map_err(|_| Error::InvalidId {
        message: "handle_id must contain exactly one valid ULID".to_string(),
    })?;
    if parsed.to_string() != ulid {
        return Err(Error::InvalidId {
            message: "handle_id must use the canonical uppercase ULID spelling".to_string(),
        });
    }
    Ok(())
}

/// Canonical control-plane transaction domains used in storage paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneTxDomain {
    /// Single-domain catalog transaction objects.
    Catalog,
    /// Single-domain orchestration transaction objects.
    Orchestration,
    /// Cross-domain root transaction objects.
    Root,
}

impl ControlPlaneTxDomain {
    /// Returns the canonical path segment for this domain.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Orchestration => "orchestration",
            Self::Root => "root",
        }
    }
}

impl Display for ControlPlaneTxDomain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Logical transaction kind stored inside transaction records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneTxKind {
    /// Catalog DDL transaction.
    CatalogDdl,
    /// Orchestration batch commit transaction.
    OrchestrationBatch,
    /// Cross-domain root commit transaction.
    RootCommit,
}

/// Transaction lifecycle state for control-plane records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlPlaneTxStatus {
    /// Immutable artifacts were staged but are not yet visible.
    Prepared,
    /// The domain head or root read token is visible to readers.
    Visible,
    /// The transaction was terminated without becoming visible.
    Aborted,
}

/// Tenant/workspace scope carried by every durable transaction handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneHandleScope {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
}

impl ControlPlaneHandleScope {
    /// Creates a validated handle scope.
    ///
    /// # Errors
    ///
    /// Returns an error when either identifier is blank or contains control characters.
    pub fn new(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Result<Self> {
        let scope = Self {
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Revalidates a persisted handle scope.
    ///
    /// # Errors
    ///
    /// Returns an error when either identifier is blank or contains control characters.
    pub fn validate(&self) -> Result<()> {
        validate_handle_text(&self.tenant_id, "scope.tenant_id")?;
        validate_handle_text(&self.workspace_id, "scope.workspace_id")
    }
}

/// High-level durable transaction handle lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlPlaneHandleStatus {
    /// Typed mutations may still be staged.
    Open,
    /// Immutable staged objects are being validated and frozen.
    Preparing,
    /// The handle is frozen and ready for review-token-authorized commit.
    Prepared,
    /// One or more deterministic low-level participants may be executing.
    Committing,
    /// Every low-level participant is durably visible.
    Visible,
    /// The handle ended before any participant could become visible.
    Aborted,
    /// The handle expired before any participant could become visible.
    Expired,
    /// Durable evidence is partial or uncertain and exact-path recovery is required.
    RepairRequired,
}

impl ControlPlaneHandleStatus {
    /// Returns whether the high-level lifecycle permits a transition to `next`.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Open,
                Self::Preparing | Self::Aborted | Self::Expired | Self::RepairRequired,
            ) | (
                Self::Preparing,
                Self::Prepared | Self::Aborted | Self::Expired | Self::RepairRequired
            ) | (
                Self::Prepared,
                Self::Committing | Self::Aborted | Self::Expired | Self::RepairRequired
            ) | (Self::Committing, Self::Visible | Self::RepairRequired)
                | (Self::RepairRequired, Self::Committing | Self::Visible)
        )
    }
}

/// Bounded durable failure classification; raw error text is never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlPlaneHandleFailureCategory {
    /// Persisted input or immutable staged content failed validation.
    Validation,
    /// An immutable object or CAS winner conflicted with the requested state.
    Conflict,
    /// Storage failed before visibility could be proven.
    Storage,
    /// A low-level participant is durably aborted.
    ParticipantAborted,
    /// A low-level participant's visibility is partial or uncertain.
    ParticipantUncertain,
}

/// Canonical immutable staged-mutation reference held by a handle record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneHandleMutationRef {
    /// Positive one-based mutation ordinal.
    pub ordinal: u64,
    /// Existing supported low-level operation kind.
    pub kind: ControlPlaneTxKind,
    /// Canonical immutable staged-object path.
    pub path: String,
    /// SHA-256 digest of the immutable staged bytes.
    pub sha256: String,
}

impl ControlPlaneHandleMutationRef {
    /// Creates a canonical immutable staged-mutation reference.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed handle ID, zero ordinal, or digest.
    pub fn new(
        handle_id: &str,
        ordinal: u64,
        kind: ControlPlaneTxKind,
        sha256: impl Into<String>,
    ) -> Result<Self> {
        let reference = Self {
            ordinal,
            kind,
            path: ControlPlaneTxPaths::handle_mutation(handle_id, ordinal)?,
            sha256: sha256.into(),
        };
        reference.validate(handle_id)?;
        Ok(reference)
    }

    /// Revalidates a persisted staged-mutation reference for `handle_id`.
    ///
    /// # Errors
    ///
    /// Returns an error for a noncanonical path, zero ordinal, or malformed digest.
    pub fn validate(&self, handle_id: &str) -> Result<()> {
        let expected_path = ControlPlaneTxPaths::handle_mutation(handle_id, self.ordinal)?;
        if self.path != expected_path {
            return Err(handle_validation(
                "mutation reference path must equal the canonical handle path",
            ));
        }
        validate_handle_digest(&self.sha256, "mutation reference sha256")
    }
}

/// Deterministic low-level participant identity and safe recovery evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneHandleParticipant {
    /// Mutation ordinal represented by this participant.
    pub ordinal: u64,
    /// Existing supported low-level operation kind.
    pub kind: ControlPlaneTxKind,
    /// Existing low-level transaction domain.
    pub domain: ControlPlaneTxDomain,
    /// Deterministic low-level request identifier.
    pub request_id: String,
    /// Deterministic low-level idempotency key.
    pub idempotency_key: String,
    /// Canonical request hash of the exact typed staged mutation.
    pub request_hash: String,
    /// Low-level transaction identifier once its claim exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<String>,
    /// Existing low-level lifecycle evidence, without defining new statuses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub low_level_status: Option<ControlPlaneTxStatus>,
    /// Canonical immutable receipt path once one is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_path: Option<String>,
}

impl ControlPlaneHandleParticipant {
    /// Revalidates deterministic participant identity and safe evidence fields.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, domain/kind mapping, or receipt evidence is malformed.
    pub fn validate(&self) -> Result<()> {
        if self.ordinal == 0 {
            return Err(handle_validation("participant ordinal must be positive"));
        }
        let expected_domain = match self.kind {
            ControlPlaneTxKind::CatalogDdl => ControlPlaneTxDomain::Catalog,
            ControlPlaneTxKind::OrchestrationBatch => ControlPlaneTxDomain::Orchestration,
            ControlPlaneTxKind::RootCommit => ControlPlaneTxDomain::Root,
        };
        if self.domain != expected_domain {
            return Err(handle_validation(
                "participant domain must match its low-level transaction kind",
            ));
        }
        validate_handle_text(&self.request_id, "participant request_id")?;
        validate_handle_text(&self.idempotency_key, "participant idempotency_key")?;
        validate_handle_digest(&self.request_hash, "participant request_hash")?;
        if let Some(tx_id) = &self.tx_id {
            let parsed = Ulid::from_string(tx_id).map_err(|_| {
                handle_validation("participant tx_id must be one canonical uppercase ULID")
            })?;
            if tx_id.len() != 26 || parsed.to_string() != *tx_id {
                return Err(handle_validation(
                    "participant tx_id must be one canonical uppercase ULID",
                ));
            }
        }
        if self.low_level_status.is_some() && self.tx_id.is_none() {
            return Err(handle_validation(
                "participant low-level status requires an exact-readable tx_id",
            ));
        }
        if self.tx_id.is_some() && self.low_level_status.is_none() {
            return Err(handle_validation(
                "participant tx_id requires exact-readable low-level status",
            ));
        }
        if let Some(receipt_path) = &self.receipt_path {
            validate_handle_relative_path(receipt_path, "participant receipt_path")?;
        }
        if self.receipt_path.is_some()
            && self.low_level_status != Some(ControlPlaneTxStatus::Visible)
        {
            return Err(handle_validation(
                "participant receipt_path requires low-level VISIBLE evidence",
            ));
        }
        Ok(())
    }
}

/// Versioned CAS-updated coordination record for a durable transaction handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneHandleRecord {
    /// Stable record discriminator.
    pub record_type: String,
    /// Record contract version.
    pub version: u32,
    /// Canonical `hdl_`-prefixed ULID.
    pub handle_id: String,
    /// Tenant/workspace scope.
    pub scope: ControlPlaneHandleScope,
    /// Positive CAS revision.
    pub revision: u64,
    /// High-level lifecycle state.
    pub status: ControlPlaneHandleStatus,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the latest durable handle revision.
    pub updated_at: DateTime<Utc>,
    /// Absolute pre-visibility expiry timestamp.
    pub expires_at: DateTime<Utc>,
    /// Timestamp when preparation completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_at: Option<DateTime<Utc>>,
    /// Timestamp when low-level commit could first begin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committing_at: Option<DateTime<Utc>>,
    /// Timestamp when every participant became visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_at: Option<DateTime<Utc>>,
    /// Timestamp for an aborted or expired terminal state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_at: Option<DateTime<Utc>>,
    /// Canonically ordered immutable mutation references.
    pub mutation_refs: Vec<ControlPlaneHandleMutationRef>,
    /// SHA-256 verifier for the once-returned plaintext review token.
    pub review_token_verifier: String,
    /// Canonically ordered deterministic low-level participant evidence.
    pub participants: Vec<ControlPlaneHandleParticipant>,
    /// Bounded failure category, never a raw error string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_category: Option<ControlPlaneHandleFailureCategory>,
}

impl ControlPlaneHandleRecord {
    /// Creates an empty validated `OPEN` handle record at revision one.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed identity, scope, timestamps, or token verifier.
    pub fn new(
        handle_id: impl Into<String>,
        scope: ControlPlaneHandleScope,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        review_token_verifier: impl Into<String>,
    ) -> Result<Self> {
        let record = Self {
            record_type: CONTROL_PLANE_HANDLE_RECORD_TYPE.to_string(),
            version: CONTROL_PLANE_HANDLE_RECORD_VERSION,
            handle_id: handle_id.into(),
            scope,
            revision: 1,
            status: ControlPlaneHandleStatus::Open,
            created_at,
            updated_at: created_at,
            expires_at,
            prepared_at: None,
            committing_at: None,
            visible_at: None,
            terminal_at: None,
            mutation_refs: Vec::new(),
            review_token_verifier: review_token_verifier.into(),
            participants: Vec::new(),
            failure_category: None,
        };
        record.validate()?;
        Ok(record)
    }

    /// Decodes and validates a persisted versioned handle record.
    ///
    /// Version 1 accepts additive unknown fields. Unknown record types,
    /// unsupported versions, unknown enum variants, and invalid values fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or any invalid persisted contract field.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Serialization {
            message: "failed to deserialize transaction handle record: malformed JSON".to_string(),
        })?;
        let canonical = crate::canonical_json::to_canonical_bytes(&value).map_err(|_| {
            Error::Serialization {
                message: "failed to canonicalize transaction handle record".to_string(),
            }
        })?;
        if canonical.as_slice() != bytes {
            return Err(handle_validation(
                "transaction handle record must use canonical JSON bytes",
            ));
        }
        if value.get("record_type").and_then(Value::as_str)
            != Some(CONTROL_PLANE_HANDLE_RECORD_TYPE)
        {
            return Err(handle_validation(format!(
                "record_type must be {CONTROL_PLANE_HANDLE_RECORD_TYPE}"
            )));
        }
        if value.get("version").and_then(Value::as_u64)
            != Some(u64::from(CONTROL_PLANE_HANDLE_RECORD_VERSION))
        {
            return Err(handle_validation("unsupported handle record version"));
        }
        let record: Self = serde_json::from_value(value).map_err(|_| Error::Serialization {
            message: "failed to deserialize transaction handle record: invalid fields".to_string(),
        })?;
        record.validate()?;
        Ok(record)
    }

    /// Encodes a validated handle record as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory contract is invalid or serialization fails.
    pub fn to_json_vec(&self) -> Result<Vec<u8>> {
        self.validate()?;
        crate::canonical_json::to_canonical_bytes(self).map_err(|error| Error::Serialization {
            message: format!("failed to serialize transaction handle record: {error}"),
        })
    }

    /// Returns the number of participants proven visible by existing low-level records.
    #[must_use]
    pub fn visible_participant_count(&self) -> usize {
        self.participants
            .iter()
            .filter(|participant| {
                participant.low_level_status == Some(ControlPlaneTxStatus::Visible)
            })
            .count()
    }

    /// Revalidates a persisted durable transaction handle record.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed versioned fields, noncanonical references,
    /// invalid timestamps, or lifecycle evidence inconsistent with `status`.
    pub fn validate(&self) -> Result<()> {
        if self.record_type != CONTROL_PLANE_HANDLE_RECORD_TYPE {
            return Err(handle_validation(format!(
                "record_type must be {CONTROL_PLANE_HANDLE_RECORD_TYPE}"
            )));
        }
        if self.version != CONTROL_PLANE_HANDLE_RECORD_VERSION {
            return Err(handle_validation("unsupported handle record version"));
        }
        validate_handle_id(&self.handle_id)?;
        self.scope.validate()?;
        if self.revision == 0 {
            return Err(handle_validation("handle revision must be positive"));
        }
        self.validate_timestamp_order()?;
        validate_handle_digest(&self.review_token_verifier, "review_token_verifier")?;

        for (index, reference) in self.mutation_refs.iter().enumerate() {
            let expected = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| handle_validation("too many mutation references"))?;
            if reference.ordinal != expected {
                return Err(handle_validation(
                    "mutation references must use contiguous canonical ordinals",
                ));
            }
            reference.validate(&self.handle_id)?;
        }

        for (index, participant) in self.participants.iter().enumerate() {
            let expected = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| handle_validation("too many participants"))?;
            if participant.ordinal != expected {
                return Err(handle_validation(
                    "participants must use contiguous canonical ordinals",
                ));
            }
            let Some(reference) = self.mutation_refs.get(index) else {
                return Err(handle_validation(
                    "every participant must match one mutation reference",
                ));
            };
            if participant.kind != reference.kind {
                return Err(handle_validation(
                    "participant kind must match its mutation reference",
                ));
            }
            let expected_identity = format!(
                "handle:{}:mutation:{:020}",
                self.handle_id, participant.ordinal
            );
            if participant.request_id != expected_identity
                || participant.idempotency_key != expected_identity
            {
                return Err(handle_validation(
                    "participant request and idempotency identities must match its handle ordinal",
                ));
            }
            participant.validate()?;
        }

        self.validate_lifecycle_evidence()
    }

    fn validate_timestamp_order(&self) -> Result<()> {
        if self.updated_at < self.created_at {
            return Err(handle_validation("updated_at must not precede created_at"));
        }
        if self.expires_at <= self.created_at {
            return Err(handle_validation("expires_at must follow created_at"));
        }
        for (field, timestamp) in [
            ("prepared_at", self.prepared_at),
            ("committing_at", self.committing_at),
            ("visible_at", self.visible_at),
            ("terminal_at", self.terminal_at),
        ] {
            if timestamp.is_some_and(|value| value < self.created_at || value > self.updated_at) {
                return Err(handle_validation(format!(
                    "{field} must fall between created_at and updated_at"
                )));
            }
        }
        if self
            .prepared_at
            .is_some_and(|prepared_at| prepared_at >= self.expires_at)
        {
            return Err(handle_validation("prepared_at must precede expires_at"));
        }
        if let Some(committing_at) = self.committing_at {
            if self
                .prepared_at
                .is_none_or(|prepared_at| committing_at < prepared_at)
            {
                return Err(handle_validation(
                    "committing_at must not precede prepared_at",
                ));
            }
            if committing_at >= self.expires_at {
                return Err(handle_validation("committing_at must precede expires_at"));
            }
        }
        if let Some(visible_at) = self.visible_at
            && self
                .committing_at
                .is_none_or(|committing_at| visible_at < committing_at)
        {
            return Err(handle_validation(
                "visible_at must not precede committing_at",
            ));
        }
        if let Some(terminal_at) = self.terminal_at
            && self
                .prepared_at
                .is_some_and(|prepared_at| terminal_at < prepared_at)
        {
            return Err(handle_validation(
                "terminal_at must not precede prepared_at",
            ));
        }
        if self.status == ControlPlaneHandleStatus::Aborted
            && self
                .terminal_at
                .is_some_and(|terminal_at| terminal_at >= self.expires_at)
        {
            return Err(handle_validation(
                "ABORTED terminal_at must precede expires_at",
            ));
        }
        Ok(())
    }

    fn validate_lifecycle_evidence(&self) -> Result<()> {
        if !self.lifecycle_evidence_is_consistent() {
            return Err(handle_validation(format!(
                "{:?} lifecycle evidence is inconsistent",
                self.status
            )));
        }
        Ok(())
    }

    fn lifecycle_evidence_is_consistent(&self) -> bool {
        let no_progress_times = self.prepared_at.is_none()
            && self.committing_at.is_none()
            && self.visible_at.is_none()
            && self.terminal_at.is_none();
        let participants_complete =
            !self.mutation_refs.is_empty() && self.participants.len() == self.mutation_refs.len();

        match self.status {
            ControlPlaneHandleStatus::Open => {
                no_progress_times && self.participants.is_empty() && self.failure_category.is_none()
            }
            ControlPlaneHandleStatus::Preparing => {
                !self.mutation_refs.is_empty()
                    && no_progress_times
                    && self.participants.is_empty()
                    && self.failure_category.is_none()
            }
            ControlPlaneHandleStatus::Prepared => {
                self.prepared_at.is_some()
                    && self.committing_at.is_none()
                    && self.visible_at.is_none()
                    && self.terminal_at.is_none()
                    && participants_complete
                    && self
                        .participants
                        .iter()
                        .all(|participant| participant.low_level_status.is_none())
                    && self.failure_category.is_none()
            }
            ControlPlaneHandleStatus::Committing => {
                self.prepared_at.is_some()
                    && self.committing_at.is_some()
                    && self.visible_at.is_none()
                    && self.terminal_at.is_none()
                    && participants_complete
                    && self.participants.iter().all(|participant| {
                        participant.low_level_status != Some(ControlPlaneTxStatus::Aborted)
                    })
                    && self.failure_category.is_none()
            }
            ControlPlaneHandleStatus::Visible => {
                self.prepared_at.is_some()
                    && self.committing_at.is_some()
                    && self.visible_at.is_some()
                    && self.terminal_at.is_none()
                    && participants_complete
                    && self.participants.iter().all(|participant| {
                        participant.low_level_status == Some(ControlPlaneTxStatus::Visible)
                    })
                    && self.failure_category.is_none()
            }
            ControlPlaneHandleStatus::Aborted
            | ControlPlaneHandleStatus::Expired
            | ControlPlaneHandleStatus::RepairRequired => {
                self.exceptional_lifecycle_evidence_is_consistent(participants_complete)
            }
        }
    }

    fn exceptional_lifecycle_evidence_is_consistent(&self, participants_complete: bool) -> bool {
        match self.status {
            ControlPlaneHandleStatus::Aborted => {
                self.committing_at.is_none()
                    && self.visible_at.is_none()
                    && self.terminal_at.is_some()
                    && self.participants.iter().all(|participant| {
                        participant.tx_id.is_none()
                            && participant.low_level_status.is_none()
                            && participant.receipt_path.is_none()
                    })
            }
            ControlPlaneHandleStatus::Expired => {
                self.committing_at.is_none()
                    && self.visible_at.is_none()
                    && self
                        .terminal_at
                        .is_some_and(|terminal_at| terminal_at >= self.expires_at)
                    && self.participants.iter().all(|participant| {
                        participant.tx_id.is_none()
                            && participant.low_level_status.is_none()
                            && participant.receipt_path.is_none()
                    })
            }
            ControlPlaneHandleStatus::RepairRequired => {
                self.prepared_at.is_some()
                    && self.committing_at.is_some()
                    && self.visible_at.is_none()
                    && self.terminal_at.is_none()
                    && participants_complete
                    && self.failure_category.is_some()
            }
            ControlPlaneHandleStatus::Open
            | ControlPlaneHandleStatus::Preparing
            | ControlPlaneHandleStatus::Prepared
            | ControlPlaneHandleStatus::Committing
            | ControlPlaneHandleStatus::Visible => false,
        }
    }
}

/// Canonical path builders for shared control-plane transaction artifacts.
pub struct ControlPlaneTxPaths;

impl ControlPlaneTxPaths {
    /// Top-level transaction prefix.
    pub const PREFIX: &str = "transactions";

    fn hash_idempotency_key(idempotency_key: &str) -> String {
        format!("{:x}", Sha256::digest(idempotency_key.as_bytes()))
    }

    /// Returns the idempotency marker path for a transaction domain and key.
    #[must_use]
    pub fn idempotency(domain: ControlPlaneTxDomain, idempotency_key: &str) -> String {
        let key_hash = Self::hash_idempotency_key(idempotency_key);
        let prefix = key_hash.get(0..2).unwrap_or("00");
        format!(
            "{}/idempotency/{}/{prefix}/{key_hash}.json",
            Self::PREFIX,
            domain.as_str()
        )
    }

    /// Returns the transaction record path for a domain and transaction id.
    #[must_use]
    pub fn record(domain: ControlPlaneTxDomain, tx_id: &str) -> String {
        format!("{}/{}/{}.json", Self::PREFIX, domain.as_str(), tx_id)
    }

    /// Returns the canonical root lock path.
    #[must_use]
    pub fn root_lock() -> String {
        LockKey::custom("root").to_string()
    }

    /// Returns the immutable tx-scoped root super-manifest path.
    #[must_use]
    pub fn root_super_manifest(tx_id: &str) -> String {
        format!("{}/root/{tx_id}.manifest.json", Self::PREFIX)
    }

    /// Returns the optional root commit receipt path.
    #[must_use]
    pub fn root_commit_receipt(commit_id: &str) -> String {
        format!("commits/root/{commit_id}.json")
    }

    /// Returns the immutable orchestration commit receipt path.
    #[must_use]
    pub fn orchestration_commit_receipt(commit_id: &str) -> String {
        format!("commits/orchestration/{commit_id}.json")
    }

    /// Returns the mutable CAS record path for a validated durable handle.
    ///
    /// # Errors
    ///
    /// Returns an error when `handle_id` is not a canonical `hdl_` ULID.
    pub fn handle_record(handle_id: &str) -> Result<String> {
        validate_handle_id(handle_id)?;
        Ok(format!("{}/handles/{handle_id}/handle.json", Self::PREFIX))
    }

    /// Returns an immutable staged-mutation path for a durable handle.
    ///
    /// # Errors
    ///
    /// Returns an error when `handle_id` is malformed or `ordinal` is zero.
    pub fn handle_mutation(handle_id: &str, ordinal: u64) -> Result<String> {
        validate_handle_id(handle_id)?;
        if ordinal == 0 {
            return Err(Error::InvalidInput(
                "handle mutation ordinal must be positive".to_string(),
            ));
        }
        Ok(format!(
            "{}/handles/{handle_id}/mutations/{ordinal:020}.json",
            Self::PREFIX
        ))
    }

    /// Returns an immutable per-ordinal identity-authority path for a durable handle.
    ///
    /// # Errors
    ///
    /// Returns an error when `handle_id` is malformed or `ordinal` is zero.
    pub fn handle_identity_authority(handle_id: &str, ordinal: u64) -> Result<String> {
        validate_handle_id(handle_id)?;
        if ordinal == 0 {
            return Err(Error::InvalidInput(
                "handle identity authority ordinal must be positive".to_string(),
            ));
        }
        Ok(format!(
            "{}/handles/{handle_id}/identities/{ordinal:020}.json",
            Self::PREFIX
        ))
    }
}

/// Shared transaction status record stored under `transactions/{domain}/{tx_id}.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneTxRecord<TResult> {
    /// Transaction identifier.
    pub tx_id: String,
    /// Logical operation kind for audit/debugging.
    pub kind: ControlPlaneTxKind,
    /// Current lifecycle status.
    pub status: ControlPlaneTxStatus,
    /// Whether post-commit repair work is still outstanding.
    #[serde(default)]
    pub repair_pending: bool,
    /// Unique request identifier for this attempt.
    #[serde(default)]
    pub request_id: String,
    /// Caller idempotency key for replay detection.
    #[serde(default)]
    pub idempotency_key: String,
    /// Canonical request payload hash.
    pub request_hash: String,
    /// Canonical lock path used to fence the writer.
    pub lock_path: String,
    /// Lock-derived fencing token or publish permit epoch.
    pub fencing_token: u64,
    /// When the transaction entered prepared state.
    pub prepared_at: DateTime<Utc>,
    /// When the transaction became visible, if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_at: Option<DateTime<Utc>>,
    /// Durable append metadata for prepared records that need repair.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_append: Option<ControlPlaneDurableAppend>,
    /// Domain-specific result payload once visible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<TResult>,
}

/// Durable append metadata recorded before visibility can be confirmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneDurableAppend {
    /// Event object paths appended for this transaction.
    pub event_paths: Vec<String>,
    /// Lock path used by the append attempt.
    pub lock_path: String,
    /// Fencing token held by the append attempt.
    pub fencing_token: u64,
}

/// Catalog transaction success receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogTxReceipt {
    /// Transaction identifier.
    pub tx_id: String,
    /// Ledger event identifier.
    pub event_id: String,
    /// Immutable commit receipt identifier.
    pub commit_id: String,
    /// Immutable catalog manifest identifier.
    pub manifest_id: String,
    /// Visible snapshot version.
    pub snapshot_version: u64,
    /// Object-store version/etag observed for the pointer CAS.
    pub pointer_version: String,
    /// Pinned read token for the visible manifest head.
    pub read_token: String,
    /// Visibility timestamp.
    pub visible_at: DateTime<Utc>,
}

/// Orchestration transaction success receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrchestrationTxReceipt {
    /// Transaction identifier.
    pub tx_id: String,
    /// Immutable commit receipt identifier.
    pub commit_id: String,
    /// Immutable orchestration manifest identifier.
    pub manifest_id: String,
    /// Visible orchestration revision ULID.
    pub revision_ulid: String,
    /// Immutable L0 delta identifier.
    pub delta_id: String,
    /// Object-store version/etag observed for the pointer CAS.
    pub pointer_version: String,
    /// Count of events folded into this commit.
    pub events_processed: u32,
    /// Pinned read token for the visible manifest head.
    pub read_token: String,
    /// Visibility timestamp.
    pub visible_at: DateTime<Utc>,
}

/// Per-domain commit reference returned by a root transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomainCommit {
    /// Domain name (`catalog` or `orchestration`).
    pub domain: ControlPlaneTxDomain,
    /// Domain-local transaction identifier.
    pub tx_id: String,
    /// Immutable commit receipt identifier.
    pub commit_id: String,
    /// Immutable manifest identifier published for this domain.
    pub manifest_id: String,
    /// Immutable manifest path published for this domain.
    pub manifest_path: String,
    /// Pinned per-domain read token.
    pub read_token: String,
}

/// Root transaction success receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootTxReceipt {
    /// Root transaction identifier.
    pub tx_id: String,
    /// Immutable root commit identifier.
    pub root_commit_id: String,
    /// Immutable tx-scoped root super-manifest path.
    pub super_manifest_path: String,
    /// Domain commits published as part of the root transaction.
    pub domain_commits: Vec<DomainCommit>,
    /// Pinned root read token, resolved via `transactions/root/{tx_id}.json`.
    pub read_token: String,
    /// Visibility timestamp.
    pub visible_at: DateTime<Utc>,
}

/// Pinned manifest reference for one domain inside a root super-manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootTxManifestDomain {
    /// Immutable domain manifest identifier.
    pub manifest_id: String,
    /// Immutable domain manifest path.
    pub manifest_path: String,
    /// Immutable domain commit identifier.
    pub commit_id: String,
}

/// Immutable tx-scoped root super-manifest used as a pinned multi-domain read token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootTxManifest {
    /// Root transaction identifier.
    pub tx_id: String,
    /// Fencing token used to finalize this root read token.
    pub fencing_token: u64,
    /// When the root read token became visible.
    pub published_at: DateTime<Utc>,
    /// Pinned manifest references for participating domains.
    pub domains: BTreeMap<ControlPlaneTxDomain, RootTxManifestDomain>,
}

/// Shared idempotency claim record for control-plane transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneIdempotencyRecord {
    /// Transaction identifier that owns the claim.
    pub tx_id: String,
    /// Logical operation kind.
    pub kind: ControlPlaneTxKind,
    /// Unique request identifier for this attempt.
    #[serde(default)]
    pub request_id: String,
    /// Caller idempotency key.
    #[serde(default)]
    pub idempotency_key: String,
    /// Canonical request payload hash.
    pub request_hash: String,
    /// When the claim was created.
    pub created_at: DateTime<Utc>,
    /// When the claimed transaction became visible, if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_at: Option<DateTime<Utc>>,
    /// Cached visible transaction record for replay/repair after partial finalize failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_record: Option<Value>,
}

/// Type alias for catalog transaction records.
pub type CatalogTxRecord = ControlPlaneTxRecord<CatalogTxReceipt>;

/// Type alias for orchestration transaction records.
pub type OrchestrationTxRecord = ControlPlaneTxRecord<OrchestrationTxReceipt>;

/// Type alias for root transaction records.
pub type RootTxRecord = ControlPlaneTxRecord<RootTxReceipt>;
