//! Canonical retained workspace snapshot and portable export contracts.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use ulid::Ulid;

use crate::error::{CatalogError, Result};
use crate::state_store::PersistedAuthorityReference;

const VERSION: u32 = 1;
const SNAPSHOT_RECORD_TYPE: &str = "workspace_snapshot";
const EXPORT_RECORD_TYPE: &str = "workspace_export";
const PIN_REVISION_RECORD_TYPE: &str = "retention_pin_revision";
const PIN_LATEST_RECORD_TYPE: &str = "retention_pin_latest";

fn validation(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(validation(format!(
            "{field} must be nonblank and contain no control characters"
        )));
    }
    Ok(())
}

fn validate_id(value: &str, prefix: &str, field: &str) -> Result<()> {
    let Some(ulid) = value.strip_prefix(prefix) else {
        return Err(validation(format!("{field} must start with {prefix}")));
    };
    if ulid.len() != 26 {
        return Err(validation(format!(
            "{field} must contain exactly one valid 26-character ULID"
        )));
    }
    let Ok(parsed) = Ulid::from_string(ulid) else {
        return Err(validation(format!(
            "{field} must contain exactly one valid 26-character ULID"
        )));
    };
    if parsed.to_string() != ulid {
        return Err(validation(format!(
            "{field} must use the canonical uppercase ULID spelling"
        )));
    }
    Ok(())
}

fn validate_relative_path(path: &str, field: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || is_drive_qualified(path)
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(validation(format!(
            "{field} must be a canonical relative path"
        )));
    }
    Ok(())
}

fn is_drive_qualified(path: &str) -> bool {
    matches!(
        path.as_bytes(),
        [drive, b':', ..] if drive.is_ascii_alphabetic()
    )
}

fn validate_sha256(value: &str, field: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(validation(format!("{field} must use the sha256: prefix")));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation(format!(
            "{field} must contain 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_envelope(value: &Value, expected_type: &str) -> Result<()> {
    if value.get("record_type").and_then(Value::as_str) != Some(expected_type) {
        return Err(validation(format!("record_type must be {expected_type}")));
    }
    if value.get("version").and_then(Value::as_u64) != Some(u64::from(VERSION)) {
        return Err(validation("unsupported record version"));
    }
    Ok(())
}

fn decode_value(bytes: &[u8], context: &str) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(|error| CatalogError::Serialization {
        message: format!("failed to deserialize {context}: {error}"),
    })
}

fn decode_record<T: for<'de> Deserialize<'de>>(value: Value, context: &str) -> Result<T> {
    serde_json::from_value(value).map_err(|error| CatalogError::Serialization {
        message: format!("failed to deserialize {context}: {error}"),
    })
}

fn encode_record<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    serde_jcs::to_vec(value).map_err(|error| CatalogError::Serialization {
        message: format!("failed to serialize {context}: {error}"),
    })
}

/// Tenant/workspace identity repeated by every retained domain reference.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkspaceScope {
    tenant_id: String,
    workspace_id: String,
}

impl WorkspaceScope {
    /// Creates a validated workspace scope.
    ///
    /// # Errors
    ///
    /// Returns a validation error for blank or control-character-bearing IDs.
    pub fn new(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Result<Self> {
        let scope = Self {
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Revalidates the persisted scope.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed scope values.
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.tenant_id, "tenant_id")?;
        validate_text(&self.workspace_id, "workspace_id")
    }

    /// Returns the tenant identifier.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the workspace identifier.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
}

/// Checksum-bearing reference to a workspace-relative object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumReference {
    relative_path: String,
    sha256: String,
}

impl ChecksumReference {
    /// Creates a validated checksum reference.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an unsafe path or malformed digest.
    pub fn new(relative_path: impl Into<String>, sha256: impl Into<String>) -> Result<Self> {
        let reference = Self {
            relative_path: relative_path.into(),
            sha256: sha256.into(),
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Revalidates a deserialized checksum reference.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an unsafe path or malformed digest.
    pub fn validate(&self) -> Result<()> {
        validate_relative_path(&self.relative_path, "relative_path")?;
        validate_sha256(&self.sha256, "sha256")
    }

    /// Returns the workspace-relative path.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the prefixed SHA-256 digest.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// A domain-scoped stable authority reference in a retained workspace cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainAuthorityReference {
    domain: String,
    scope: WorkspaceScope,
    authority: PersistedAuthorityReference,
}

impl DomainAuthorityReference {
    /// Creates a validated domain authority reference.
    ///
    /// # Errors
    ///
    /// Returns a validation error when repeated scopes or domains disagree.
    pub fn new(
        domain: impl Into<String>,
        scope: WorkspaceScope,
        authority: PersistedAuthorityReference,
    ) -> Result<Self> {
        let reference = Self {
            domain: domain.into(),
            scope,
            authority,
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Revalidates repeated scope and authority fields.
    ///
    /// # Errors
    ///
    /// Returns a validation error when any field is malformed or inconsistent.
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.domain, "domain")?;
        self.scope.validate()?;
        self.authority.validate()?;
        if self.authority.scope().tenant_id() != self.scope.tenant_id()
            || self.authority.scope().workspace_id() != self.scope.workspace_id()
            || self.authority.scope().domain() != self.domain
        {
            return Err(validation(
                "domain authority scope must repeat the workspace and domain exactly",
            ));
        }
        Ok(())
    }

    /// Returns the canonical domain name.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the repeated workspace scope.
    #[must_use]
    pub const fn scope(&self) -> &WorkspaceScope {
        &self.scope
    }

    /// Returns the stable persisted authority reference.
    #[must_use]
    pub const fn authority(&self) -> &PersistedAuthorityReference {
        &self.authority
    }
}

/// Projection cut included by a retained workspace snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionWatermark {
    projection_name: String,
    source_domain: String,
    included_authority_sequence: u64,
    manifest: ChecksumReference,
}

impl ProjectionWatermark {
    /// Creates a validated projection watermark.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed names or manifest references.
    pub fn new(
        projection_name: impl Into<String>,
        source_domain: impl Into<String>,
        included_authority_sequence: u64,
        manifest: ChecksumReference,
    ) -> Result<Self> {
        let watermark = Self {
            projection_name: projection_name.into(),
            source_domain: source_domain.into(),
            included_authority_sequence,
            manifest,
        };
        watermark.validate()?;
        Ok(watermark)
    }

    /// Revalidates a projection watermark.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed names or manifest references.
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.projection_name, "projection_name")?;
        validate_text(&self.source_domain, "source_domain")?;
        self.manifest.validate()
    }

    /// Returns the projection name.
    #[must_use]
    pub fn projection_name(&self) -> &str {
        &self.projection_name
    }

    /// Returns the source authority domain.
    #[must_use]
    pub fn source_domain(&self) -> &str {
        &self.source_domain
    }

    /// Returns the included authority sequence.
    #[must_use]
    pub const fn included_authority_sequence(&self) -> u64 {
        self.included_authority_sequence
    }

    /// Returns the checksum-bearing projection manifest.
    #[must_use]
    pub const fn manifest(&self) -> &ChecksumReference {
        &self.manifest
    }
}

/// Explicit event-archive boundary for one source domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventArchiveCut {
    /// No events exist in the retained cut.
    Empty,
    /// An inclusive event-sequence interval and its archive manifest.
    Inclusive {
        /// First included event sequence.
        start_sequence: u64,
        /// Last included event sequence.
        end_sequence: u64,
        /// Checksum-bearing archive manifest.
        archive_manifest: ChecksumReference,
    },
}

/// Domain-specific event archive included by a retained cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainEventArchive {
    source_domain: String,
    cut: EventArchiveCut,
}

impl DomainEventArchive {
    /// Creates an explicit empty archive cut.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed source domain.
    pub fn empty(source_domain: impl Into<String>) -> Result<Self> {
        let archive = Self {
            source_domain: source_domain.into(),
            cut: EventArchiveCut::Empty,
        };
        archive.validate()?;
        Ok(archive)
    }

    /// Creates an inclusive event-archive cut.
    ///
    /// # Errors
    ///
    /// Returns a validation error when the interval is reversed or malformed.
    pub fn inclusive(
        source_domain: impl Into<String>,
        start_sequence: u64,
        end_sequence: u64,
        archive_manifest: ChecksumReference,
    ) -> Result<Self> {
        let archive = Self {
            source_domain: source_domain.into(),
            cut: EventArchiveCut::Inclusive {
                start_sequence,
                end_sequence,
                archive_manifest,
            },
        };
        archive.validate()?;
        Ok(archive)
    }

    /// Revalidates the archive boundary and manifest.
    ///
    /// # Errors
    ///
    /// Returns a validation error for reversed intervals or malformed references.
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.source_domain, "source_domain")?;
        if let EventArchiveCut::Inclusive {
            start_sequence,
            end_sequence,
            archive_manifest,
        } = &self.cut
        {
            if start_sequence > end_sequence {
                return Err(validation(
                    "event archive start_sequence must not exceed end_sequence",
                ));
            }
            archive_manifest.validate()?;
        }
        Ok(())
    }

    /// Returns the source authority domain.
    #[must_use]
    pub fn source_domain(&self) -> &str {
        &self.source_domain
    }

    /// Returns the explicit archive cut.
    #[must_use]
    pub const fn cut(&self) -> &EventArchiveCut {
        &self.cut
    }
}

/// Typed role of an object required by a snapshot or export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredObjectKind {
    /// State-store authority manifest.
    AuthorityManifest,
    /// State-store checkpoint object.
    Checkpoint,
    /// Projection manifest or projection artifact.
    ProjectionManifest,
    /// Event archive manifest.
    EventArchiveManifest,
    /// Immutable workspace snapshot record.
    SnapshotRecord,
    /// Immutable export record.
    ExportRecord,
    /// Root transaction token or read manifest.
    RootToken,
    /// Review-token retained cut.
    ReviewTokenCut,
    /// Read-only old-path compatibility object.
    LegacyCompatibility,
    /// Another explicitly required immutable object.
    Other,
}

/// One checksummed object required to use a snapshot or export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredObject {
    relative_path: String,
    byte_size: u64,
    kind: RequiredObjectKind,
    sha256: String,
}

impl RequiredObject {
    /// Creates a validated required-object record.
    ///
    /// # Errors
    ///
    /// Returns a validation error for unsafe paths or malformed digests.
    pub fn new(
        relative_path: impl Into<String>,
        byte_size: u64,
        kind: RequiredObjectKind,
        sha256: impl Into<String>,
    ) -> Result<Self> {
        let object = Self {
            relative_path: relative_path.into(),
            byte_size,
            kind,
            sha256: sha256.into(),
        };
        object.validate()?;
        Ok(object)
    }

    /// Revalidates a required object.
    ///
    /// # Errors
    ///
    /// Returns a validation error for unsafe paths or malformed digests.
    pub fn validate(&self) -> Result<()> {
        validate_relative_path(&self.relative_path, "required object path")?;
        validate_sha256(&self.sha256, "required object sha256")
    }

    /// Returns the canonical relative path.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the exact expected byte size.
    #[must_use]
    pub const fn byte_size(&self) -> u64 {
        self.byte_size
    }

    /// Returns the object's role.
    #[must_use]
    pub const fn kind(&self) -> RequiredObjectKind {
        self.kind
    }

    /// Returns the prefixed SHA-256 digest.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// Explicit read-only reference to an old-path compatibility artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCompatibilityArtifact {
    relative_path: String,
    sha256: String,
    read_only: bool,
}

impl LegacyCompatibilityArtifact {
    /// Creates a validated read-only compatibility reference.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an unsafe path or malformed digest.
    pub fn new(relative_path: impl Into<String>, sha256: impl Into<String>) -> Result<Self> {
        let artifact = Self {
            relative_path: relative_path.into(),
            sha256: sha256.into(),
            read_only: true,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    /// Revalidates the compatibility artifact.
    ///
    /// # Errors
    ///
    /// Returns a validation error unless the artifact is safe and read-only.
    pub fn validate(&self) -> Result<()> {
        validate_relative_path(&self.relative_path, "compatibility path")?;
        validate_sha256(&self.sha256, "compatibility sha256")?;
        if !self.read_only {
            return Err(validation("compatibility artifacts must be read-only"));
        }
        Ok(())
    }

    /// Returns the old workspace-relative path.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the prefixed SHA-256 digest.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Returns true; compatibility artifacts are never writable through this contract.
    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.read_only
    }
}

fn canonicalize_archives(
    archives: &mut Vec<DomainEventArchive>,
    authority_sequences: &BTreeMap<&str, u64>,
) -> Result<()> {
    for archive in &*archives {
        archive.validate()?;
        let Some(authority_sequence) = authority_sequences.get(archive.source_domain()) else {
            return Err(validation("event archive source domain is not retained"));
        };
        if let EventArchiveCut::Inclusive { end_sequence, .. } = archive.cut()
            && end_sequence > authority_sequence
        {
            return Err(validation(
                "event archive end_sequence exceeds retained authority sequence",
            ));
        }
    }
    archives.sort_by(|left, right| left.source_domain.cmp(&right.source_domain));
    if archives
        .windows(2)
        .any(|window| matches!(window, [left, right] if left.source_domain == right.source_domain))
    {
        return Err(validation("event archive domains must be unique"));
    }
    if archives.len() != authority_sequences.len() {
        return Err(validation(
            "every retained authority domain must declare exactly one event archive cut",
        ));
    }
    Ok(())
}

fn canonicalize_cut(
    scope: &WorkspaceScope,
    retained_until: DateTime<Utc>,
    domains: &mut Vec<DomainAuthorityReference>,
    projections: &mut Vec<ProjectionWatermark>,
    archives: &mut Vec<DomainEventArchive>,
    required_objects: &mut Vec<RequiredObject>,
    compatibility: &mut Vec<LegacyCompatibilityArtifact>,
) -> Result<()> {
    scope.validate()?;
    for domain in &*domains {
        domain.validate()?;
        if domain.scope() != scope {
            return Err(validation(
                "every domain authority reference must repeat the snapshot scope",
            ));
        }
        if domain.authority().retention_deadline() < retained_until {
            return Err(validation(
                "authority retention deadline must cover the retained cut",
            ));
        }
    }
    domains.sort_by(|left, right| left.domain.cmp(&right.domain));
    if domains.is_empty()
        || domains
            .windows(2)
            .any(|window| matches!(window, [left, right] if left.domain == right.domain))
    {
        return Err(validation(
            "domain authority names must be nonempty and unique",
        ));
    }
    let authority_sequences: BTreeMap<&str, u64> = domains
        .iter()
        .map(|domain| (domain.domain(), domain.authority().logical_sequence()))
        .collect();

    for projection in &*projections {
        projection.validate()?;
        let Some(sequence) = authority_sequences.get(projection.source_domain()) else {
            return Err(validation("projection source domain is not retained"));
        };
        if projection.included_authority_sequence() > *sequence {
            return Err(validation(
                "projection watermark exceeds retained authority sequence",
            ));
        }
    }
    projections.sort_by(|left, right| {
        (&left.projection_name, &left.source_domain)
            .cmp(&(&right.projection_name, &right.source_domain))
    });
    if projections.windows(2).any(|window| {
        matches!(
            window,
            [left, right]
                if left.projection_name == right.projection_name
                    && left.source_domain == right.source_domain
        )
    }) {
        return Err(validation("projection watermarks must be unique"));
    }

    canonicalize_archives(archives, &authority_sequences)?;

    for object in &*required_objects {
        object.validate()?;
    }
    required_objects.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if required_objects
        .windows(2)
        .any(|window| matches!(window, [left, right] if left.relative_path == right.relative_path))
    {
        return Err(validation("required object paths must be unique"));
    }

    let required: BTreeMap<&str, &str> = required_objects
        .iter()
        .map(|object| (object.relative_path(), object.sha256()))
        .collect();
    for artifact in &*compatibility {
        artifact.validate()?;
        if required.get(artifact.relative_path()).copied() != Some(artifact.sha256()) {
            return Err(validation(
                "compatibility artifact must exactly match a required object",
            ));
        }
    }
    compatibility.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if compatibility
        .windows(2)
        .any(|window| matches!(window, [left, right] if left.relative_path == right.relative_path))
    {
        return Err(validation("compatibility artifact paths must be unique"));
    }
    Ok(())
}

/// Immutable version-1 retained workspace snapshot record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkspaceSnapshot {
    record_type: String,
    version: u32,
    snapshot_id: String,
    scope: WorkspaceScope,
    created_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    parent_snapshot_id: Option<String>,
    domains: Vec<DomainAuthorityReference>,
    projection_watermarks: Vec<ProjectionWatermark>,
    event_archives: Vec<DomainEventArchive>,
    required_objects: Vec<RequiredObject>,
    compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
}

#[derive(Deserialize)]
struct WorkspaceSnapshotWire {
    record_type: String,
    version: u32,
    snapshot_id: String,
    scope: WorkspaceScope,
    created_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    parent_snapshot_id: Option<String>,
    domains: Vec<DomainAuthorityReference>,
    projection_watermarks: Vec<ProjectionWatermark>,
    event_archives: Vec<DomainEventArchive>,
    required_objects: Vec<RequiredObject>,
    compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
}

impl From<WorkspaceSnapshotWire> for WorkspaceSnapshot {
    fn from(wire: WorkspaceSnapshotWire) -> Self {
        Self {
            record_type: wire.record_type,
            version: wire.version,
            snapshot_id: wire.snapshot_id,
            scope: wire.scope,
            created_at: wire.created_at,
            retained_until: wire.retained_until,
            parent_snapshot_id: wire.parent_snapshot_id,
            domains: wire.domains,
            projection_watermarks: wire.projection_watermarks,
            event_archives: wire.event_archives,
            required_objects: wire.required_objects,
            compatibility_artifacts: wire.compatibility_artifacts,
        }
    }
}

impl WorkspaceSnapshot {
    /// Creates a canonical immutable workspace snapshot.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed or ambiguous retained state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        snapshot_id: impl Into<String>,
        scope: WorkspaceScope,
        created_at: DateTime<Utc>,
        retained_until: DateTime<Utc>,
        parent_snapshot_id: Option<String>,
        domains: Vec<DomainAuthorityReference>,
        projection_watermarks: Vec<ProjectionWatermark>,
        event_archives: Vec<DomainEventArchive>,
        required_objects: Vec<RequiredObject>,
        compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
    ) -> Result<Self> {
        let mut snapshot = Self {
            record_type: SNAPSHOT_RECORD_TYPE.to_string(),
            version: VERSION,
            snapshot_id: snapshot_id.into(),
            scope,
            created_at,
            retained_until,
            parent_snapshot_id,
            domains,
            projection_watermarks,
            event_archives,
            required_objects,
            compatibility_artifacts,
        };
        snapshot.canonicalize_and_validate()?;
        Ok(snapshot)
    }

    fn canonicalize_and_validate(&mut self) -> Result<()> {
        if self.record_type != SNAPSHOT_RECORD_TYPE || self.version != VERSION {
            return Err(validation("unsupported workspace snapshot envelope"));
        }
        validate_id(&self.snapshot_id, "snap_", "snapshot_id")?;
        if let Some(parent) = &self.parent_snapshot_id {
            validate_id(parent, "snap_", "parent_snapshot_id")?;
            if parent == &self.snapshot_id {
                return Err(validation("snapshot cannot be its own parent"));
            }
        }
        if self.retained_until <= self.created_at {
            return Err(validation("retained_until must be after created_at"));
        }
        canonicalize_cut(
            &self.scope,
            self.retained_until,
            &mut self.domains,
            &mut self.projection_watermarks,
            &mut self.event_archives,
            &mut self.required_objects,
            &mut self.compatibility_artifacts,
        )
    }

    /// Returns the immutable snapshot identifier.
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Returns the record version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns the repeated workspace scope.
    #[must_use]
    pub const fn scope(&self) -> &WorkspaceScope {
        &self.scope
    }

    /// Returns the creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the initial retention deadline.
    #[must_use]
    pub const fn retained_until(&self) -> DateTime<Utc> {
        self.retained_until
    }

    /// Returns the optional parent snapshot identifier.
    #[must_use]
    pub fn parent_snapshot_id(&self) -> Option<&str> {
        self.parent_snapshot_id.as_deref()
    }

    /// Returns canonical domain authority references.
    #[must_use]
    pub fn domains(&self) -> &[DomainAuthorityReference] {
        &self.domains
    }

    /// Returns canonical projection watermarks.
    #[must_use]
    pub fn projection_watermarks(&self) -> &[ProjectionWatermark] {
        &self.projection_watermarks
    }

    /// Returns canonical event archives.
    #[must_use]
    pub fn event_archives(&self) -> &[DomainEventArchive] {
        &self.event_archives
    }

    /// Returns canonical required objects.
    #[must_use]
    pub fn required_objects(&self) -> &[RequiredObject] {
        &self.required_objects
    }

    /// Returns canonical read-only compatibility artifacts.
    #[must_use]
    pub fn compatibility_artifacts(&self) -> &[LegacyCompatibilityArtifact] {
        &self.compatibility_artifacts
    }
}

/// Export relocation rule that persists no provider or root URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelocationPolicy {
    paths_relative_to_caller_export_root: bool,
}

impl RelocationPolicy {
    /// Creates the only supported relocation policy.
    #[must_use]
    pub const fn relative_to_caller_export_root() -> Self {
        Self {
            paths_relative_to_caller_export_root: true,
        }
    }

    fn validate(self) -> Result<()> {
        if !self.paths_relative_to_caller_export_root {
            return Err(validation(
                "export paths must be relative to the caller-supplied export root",
            ));
        }
        Ok(())
    }
}

/// Immutable version-1 portable export manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExportManifest {
    record_type: String,
    version: u32,
    export_id: String,
    snapshot_id: String,
    scope: WorkspaceScope,
    created_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    domains: Vec<DomainAuthorityReference>,
    projection_watermarks: Vec<ProjectionWatermark>,
    event_archives: Vec<DomainEventArchive>,
    required_objects: Vec<RequiredObject>,
    compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
    relocation: RelocationPolicy,
}

#[derive(Deserialize)]
struct ExportManifestWire {
    record_type: String,
    version: u32,
    export_id: String,
    snapshot_id: String,
    scope: WorkspaceScope,
    created_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    domains: Vec<DomainAuthorityReference>,
    projection_watermarks: Vec<ProjectionWatermark>,
    event_archives: Vec<DomainEventArchive>,
    required_objects: Vec<RequiredObject>,
    compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
    relocation: RelocationPolicy,
}

impl From<ExportManifestWire> for ExportManifest {
    fn from(wire: ExportManifestWire) -> Self {
        Self {
            record_type: wire.record_type,
            version: wire.version,
            export_id: wire.export_id,
            snapshot_id: wire.snapshot_id,
            scope: wire.scope,
            created_at: wire.created_at,
            retained_until: wire.retained_until,
            domains: wire.domains,
            projection_watermarks: wire.projection_watermarks,
            event_archives: wire.event_archives,
            required_objects: wire.required_objects,
            compatibility_artifacts: wire.compatibility_artifacts,
            relocation: wire.relocation,
        }
    }
}

impl ExportManifest {
    /// Creates a canonical immutable export manifest.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed, ambiguous, or unsafe export state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        export_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        scope: WorkspaceScope,
        created_at: DateTime<Utc>,
        retained_until: DateTime<Utc>,
        domains: Vec<DomainAuthorityReference>,
        projection_watermarks: Vec<ProjectionWatermark>,
        event_archives: Vec<DomainEventArchive>,
        required_objects: Vec<RequiredObject>,
        compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
        relocation: RelocationPolicy,
    ) -> Result<Self> {
        let mut export = Self {
            record_type: EXPORT_RECORD_TYPE.to_string(),
            version: VERSION,
            export_id: export_id.into(),
            snapshot_id: snapshot_id.into(),
            scope,
            created_at,
            retained_until,
            domains,
            projection_watermarks,
            event_archives,
            required_objects,
            compatibility_artifacts,
            relocation,
        };
        export.canonicalize_and_validate()?;
        Ok(export)
    }

    fn canonicalize_and_validate(&mut self) -> Result<()> {
        if self.record_type != EXPORT_RECORD_TYPE || self.version != VERSION {
            return Err(validation("unsupported workspace export envelope"));
        }
        validate_id(&self.export_id, "exp_", "export_id")?;
        validate_id(&self.snapshot_id, "snap_", "snapshot_id")?;
        if self.retained_until <= self.created_at {
            return Err(validation("retained_until must be after created_at"));
        }
        self.relocation.validate()?;
        canonicalize_cut(
            &self.scope,
            self.retained_until,
            &mut self.domains,
            &mut self.projection_watermarks,
            &mut self.event_archives,
            &mut self.required_objects,
            &mut self.compatibility_artifacts,
        )
    }

    /// Returns the immutable export identifier.
    #[must_use]
    pub fn export_id(&self) -> &str {
        &self.export_id
    }

    /// Returns the source snapshot identifier.
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Returns canonical domain authority references.
    #[must_use]
    pub fn domains(&self) -> &[DomainAuthorityReference] {
        &self.domains
    }

    /// Returns canonical projection watermarks.
    #[must_use]
    pub fn projection_watermarks(&self) -> &[ProjectionWatermark] {
        &self.projection_watermarks
    }

    /// Returns canonical event archives.
    #[must_use]
    pub fn event_archives(&self) -> &[DomainEventArchive] {
        &self.event_archives
    }

    /// Returns canonical required objects.
    #[must_use]
    pub fn required_objects(&self) -> &[RequiredObject] {
        &self.required_objects
    }

    /// Returns canonical read-only compatibility artifacts.
    #[must_use]
    pub fn compatibility_artifacts(&self) -> &[LegacyCompatibilityArtifact] {
        &self.compatibility_artifacts
    }
}

/// Retained immutable record selected by a retention pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum RetentionTarget {
    /// An immutable workspace snapshot.
    Snapshot(String),
    /// An immutable export manifest.
    Export(String),
}

impl RetentionTarget {
    /// Creates a snapshot target.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed snapshot ID.
    pub fn snapshot(id: impl Into<String>) -> Result<Self> {
        let target = Self::Snapshot(id.into());
        target.validate()?;
        Ok(target)
    }

    /// Creates an export target.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed export ID.
    pub fn export(id: impl Into<String>) -> Result<Self> {
        let target = Self::Export(id.into());
        target.validate()?;
        Ok(target)
    }

    /// Revalidates the target identifier.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed target ID.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Snapshot(id) => validate_id(id, "snap_", "snapshot target"),
            Self::Export(id) => validate_id(id, "exp_", "export target"),
        }
    }

    /// Returns the target ID.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Snapshot(id) | Self::Export(id) => id,
        }
    }
}

/// Evaluated lifecycle state of a structurally valid retention pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionStatus {
    /// The pin is active and still protects its target.
    Active,
    /// The pin elapsed without a release revision.
    Expired,
    /// The pin was explicitly released.
    Released,
}

/// Checksum-bearing link to the immediately preceding immutable pin revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPinPredecessor {
    revision: u64,
    revision_path: String,
    revision_sha256: String,
}

impl RetentionPinPredecessor {
    fn new(pin_id: &str, revision: u64, revision_sha256: String) -> Result<Self> {
        let predecessor = Self {
            revision,
            revision_path: pin_revision_relative_path(pin_id, revision),
            revision_sha256,
        };
        predecessor.validate(pin_id)?;
        Ok(predecessor)
    }

    fn validate(&self, pin_id: &str) -> Result<()> {
        if self.revision == 0 {
            return Err(validation("pin predecessor revision must be positive"));
        }
        validate_relative_path(&self.revision_path, "pin predecessor revision_path")?;
        validate_sha256(&self.revision_sha256, "pin predecessor revision_sha256")?;
        if self.revision_path != pin_revision_relative_path(pin_id, self.revision) {
            return Err(validation("pin predecessor revision path is not canonical"));
        }
        Ok(())
    }

    /// Returns the predecessor revision number.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the canonical predecessor record path.
    #[must_use]
    pub fn revision_path(&self) -> &str {
        &self.revision_path
    }

    /// Returns the checksum of the predecessor's exact stored bytes.
    #[must_use]
    pub fn revision_sha256(&self) -> &str {
        &self.revision_sha256
    }
}

/// One immutable revision of a retained-target pin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPinRevision {
    record_type: String,
    version: u32,
    pin_id: String,
    revision: u64,
    target: RetentionTarget,
    created_at: DateTime<Utc>,
    revised_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    released_at: Option<DateTime<Utc>>,
    predecessor: Option<RetentionPinPredecessor>,
    #[serde(skip)]
    chain_verified: bool,
    #[serde(skip)]
    raw_sha256: Option<String>,
}

impl PartialEq for RetentionPinRevision {
    fn eq(&self, other: &Self) -> bool {
        self.record_type == other.record_type
            && self.version == other.version
            && self.pin_id == other.pin_id
            && self.revision == other.revision
            && self.target == other.target
            && self.created_at == other.created_at
            && self.revised_at == other.revised_at
            && self.retained_until == other.retained_until
            && self.released_at == other.released_at
            && self.predecessor == other.predecessor
    }
}

impl Eq for RetentionPinRevision {}

impl RetentionPinRevision {
    /// Creates an immutable validated pin revision.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identity, time, or lifecycle fields.
    pub fn new(
        pin_id: impl Into<String>,
        revision: u64,
        target: RetentionTarget,
        created_at: DateTime<Utc>,
        retained_until: DateTime<Utc>,
        released_at: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        if revision != 1 {
            return Err(validation(
                "only retention pin revision 1 may be constructed without predecessor proof",
            ));
        }
        if released_at.is_some() {
            return Err(validation(
                "an initial retention pin revision cannot be released",
            ));
        }
        let pin = Self {
            record_type: PIN_REVISION_RECORD_TYPE.to_string(),
            version: VERSION,
            pin_id: pin_id.into(),
            revision,
            target,
            created_at,
            revised_at: created_at,
            retained_until,
            released_at,
            predecessor: None,
            chain_verified: true,
            raw_sha256: None,
        };
        pin.validate()?;
        Ok(pin)
    }

    /// Revalidates the full pin structure before lifecycle evaluation.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed or ambiguous pin state.
    pub fn validate(&self) -> Result<()> {
        if self.record_type != PIN_REVISION_RECORD_TYPE || self.version != VERSION {
            return Err(validation("unsupported retention pin revision envelope"));
        }
        validate_id(&self.pin_id, "pin_", "pin_id")?;
        if self.revision == 0 {
            return Err(validation("pin revision must be positive"));
        }
        self.target.validate()?;
        if self.retained_until <= self.created_at {
            return Err(validation("pin retained_until must be after created_at"));
        }
        if self.revised_at < self.created_at {
            return Err(validation("pin revised_at must not precede created_at"));
        }
        if let Some(released_at) = self.released_at
            && released_at != self.revised_at
        {
            return Err(validation(
                "released_at must equal the immutable revision timestamp",
            ));
        }
        match (&self.predecessor, self.revision) {
            (None, 1) => {
                if self.revised_at != self.created_at || self.released_at.is_some() {
                    return Err(validation(
                        "initial pin revision must be active at its creation timestamp",
                    ));
                }
            }
            (Some(predecessor), revision) if revision > 1 => {
                predecessor.validate(&self.pin_id)?;
                if predecessor.revision().checked_add(1) != Some(revision) {
                    return Err(validation(
                        "pin predecessor revision must be immediately sequential",
                    ));
                }
            }
            _ => {
                return Err(validation(
                    "pin successor revisions require one checksum-bearing predecessor",
                ));
            }
        }
        Ok(())
    }

    /// Evaluates a valid pin at a caller-supplied time.
    ///
    /// Structural validation always happens before expiry evaluation.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed or ambiguous pin state.
    pub fn status_at(&self, now: DateTime<Utc>) -> Result<RetentionStatus> {
        self.validate()?;
        if self.revision > 1 && !self.chain_verified {
            return Err(validation(
                "pin successor lifecycle requires verified predecessor chain",
            ));
        }
        Ok(self.effective_status_at(now))
    }

    pub(crate) fn structural_status_at(&self, now: DateTime<Utc>) -> Result<RetentionStatus> {
        self.validate()?;
        Ok(self.effective_status_at(now))
    }

    fn effective_status_at(&self, now: DateTime<Utc>) -> RetentionStatus {
        if self
            .released_at
            .is_some_and(|released_at| released_at <= now)
        {
            RetentionStatus::Released
        } else if self.retained_until <= now {
            RetentionStatus::Expired
        } else {
            RetentionStatus::Active
        }
    }

    /// Creates the next immutable revision by extending an active pin.
    ///
    /// # Errors
    ///
    /// Returns a validation error unless the pin is active, sequential, and extended.
    pub fn renew(
        &self,
        next_revision: u64,
        retained_until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self> {
        if self.released_at.is_some() {
            return Err(validation("a release revision cannot be renewed"));
        }
        if self.status_at(now)? != RetentionStatus::Active {
            return Err(validation("only an active retention pin may be renewed"));
        }
        if self.revision.checked_add(1) != Some(next_revision) {
            return Err(validation("renewal revision must be sequential"));
        }
        if retained_until <= self.retained_until {
            return Err(validation("renewal must extend retained_until"));
        }
        if now < self.revised_at {
            return Err(validation("renewal cannot precede the prior revision"));
        }
        let successor = Self {
            record_type: PIN_REVISION_RECORD_TYPE.to_string(),
            version: VERSION,
            pin_id: self.pin_id.clone(),
            revision: next_revision,
            target: self.target.clone(),
            created_at: self.created_at,
            revised_at: now,
            retained_until,
            released_at: None,
            predecessor: Some(self.predecessor_link()?),
            chain_verified: true,
            raw_sha256: None,
        };
        successor.validate()?;
        Ok(successor)
    }

    /// Creates the next immutable release revision, or returns an existing release.
    ///
    /// # Errors
    ///
    /// Returns a validation error for nonsequential or pre-creation release state.
    pub fn release(&self, next_revision: u64, released_at: DateTime<Utc>) -> Result<Self> {
        self.validate()?;
        if self.released_at.is_some() {
            self.status_at(released_at)?;
            return Ok(self.clone());
        }
        if self.status_at(released_at)? != RetentionStatus::Active {
            return Err(validation(
                "only a pin active at the release timestamp may be released",
            ));
        }
        if self.revision.checked_add(1) != Some(next_revision) {
            return Err(validation("release revision must be sequential"));
        }
        if released_at < self.revised_at {
            return Err(validation("release cannot precede the prior revision"));
        }
        let successor = Self {
            record_type: PIN_REVISION_RECORD_TYPE.to_string(),
            version: VERSION,
            pin_id: self.pin_id.clone(),
            revision: next_revision,
            target: self.target.clone(),
            created_at: self.created_at,
            revised_at: released_at,
            retained_until: self.retained_until,
            released_at: Some(released_at),
            predecessor: Some(self.predecessor_link()?),
            chain_verified: true,
            raw_sha256: None,
        };
        successor.validate()?;
        Ok(successor)
    }

    fn predecessor_link(&self) -> Result<RetentionPinPredecessor> {
        let digest = if let Some(raw_sha256) = &self.raw_sha256 {
            raw_sha256.clone()
        } else {
            pin_revision_sha256(&encode_record(self, "retention pin revision")?)
        };
        RetentionPinPredecessor::new(&self.pin_id, self.revision, digest)
    }

    pub(crate) fn mark_chain_verified(&mut self, raw_sha256: String) -> Result<()> {
        validate_sha256(&raw_sha256, "pin revision raw sha256")?;
        self.raw_sha256 = Some(raw_sha256);
        self.chain_verified = true;
        Ok(())
    }

    pub(crate) fn record_raw_sha256(&mut self, raw_sha256: String) -> Result<()> {
        validate_sha256(&raw_sha256, "pin revision raw sha256")?;
        self.raw_sha256 = Some(raw_sha256);
        Ok(())
    }

    /// Returns the pin identifier.
    #[must_use]
    pub fn pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Returns the immutable revision number.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the retained target.
    #[must_use]
    pub const fn target(&self) -> &RetentionTarget {
        &self.target
    }

    /// Returns the original pin creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns this immutable revision's transition timestamp.
    #[must_use]
    pub const fn revised_at(&self) -> DateTime<Utc> {
        self.revised_at
    }

    /// Returns the retention deadline.
    #[must_use]
    pub const fn retained_until(&self) -> DateTime<Utc> {
        self.retained_until
    }

    /// Returns the effective release timestamp, when this is a release revision.
    #[must_use]
    pub const fn released_at(&self) -> Option<DateTime<Utc>> {
        self.released_at
    }

    /// Returns the checksum-bearing predecessor link for a successor revision.
    #[must_use]
    pub const fn predecessor(&self) -> Option<&RetentionPinPredecessor> {
        self.predecessor.as_ref()
    }
}

fn pin_revision_relative_path(pin_id: &str, revision: u64) -> String {
    format!("retention/pins/{pin_id}/revisions/{revision}.json")
}

fn pin_revision_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// CAS-selected pointer to the latest immutable pin revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPinLatest {
    record_type: String,
    version: u32,
    pin_id: String,
    revision: u64,
    revision_path: String,
    revision_sha256: String,
}

impl RetentionPinLatest {
    /// Creates a validated latest-revision selector.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identity, path, or digest fields.
    pub fn new(
        pin_id: impl Into<String>,
        revision: u64,
        revision_path: impl Into<String>,
        revision_sha256: impl Into<String>,
    ) -> Result<Self> {
        let selector = Self {
            record_type: PIN_LATEST_RECORD_TYPE.to_string(),
            version: VERSION,
            pin_id: pin_id.into(),
            revision,
            revision_path: revision_path.into(),
            revision_sha256: revision_sha256.into(),
        };
        selector.validate()?;
        Ok(selector)
    }

    /// Revalidates a latest-revision selector.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed selector state.
    pub fn validate(&self) -> Result<()> {
        if self.record_type != PIN_LATEST_RECORD_TYPE || self.version != VERSION {
            return Err(validation("unsupported retention pin latest envelope"));
        }
        validate_id(&self.pin_id, "pin_", "pin_id")?;
        if self.revision == 0 {
            return Err(validation("pin revision must be positive"));
        }
        validate_relative_path(&self.revision_path, "revision_path")?;
        if self.revision_path != pin_revision_relative_path(&self.pin_id, self.revision) {
            return Err(validation(
                "latest pin selector revision path is not canonical for its pin and revision",
            ));
        }
        validate_sha256(&self.revision_sha256, "revision_sha256")
    }

    /// Returns the selected revision number.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the pin identifier.
    #[must_use]
    pub fn pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Returns the selected immutable revision path.
    #[must_use]
    pub fn revision_path(&self) -> &str {
        &self.revision_path
    }

    /// Returns the selected revision's prefixed SHA-256 digest.
    #[must_use]
    pub fn revision_sha256(&self) -> &str {
        &self.revision_sha256
    }
}

/// Canonically encodes a validated workspace snapshot.
///
/// # Errors
///
/// Returns an error if validation or serialization fails.
pub fn encode_workspace_snapshot(snapshot: &WorkspaceSnapshot) -> Result<Vec<u8>> {
    let mut validated = snapshot.clone();
    validated.canonicalize_and_validate()?;
    encode_record(&validated, "workspace snapshot")
}

/// Decodes, validates, and canonicalizes a version-1 workspace snapshot.
///
/// # Errors
///
/// Returns an error for malformed JSON, the wrong type, or an unsupported version.
pub fn decode_workspace_snapshot(bytes: &[u8]) -> Result<WorkspaceSnapshot> {
    let value = decode_value(bytes, "workspace snapshot")?;
    validate_envelope(&value, SNAPSHOT_RECORD_TYPE)?;
    let wire: WorkspaceSnapshotWire = decode_record(value, "workspace snapshot")?;
    let mut snapshot = WorkspaceSnapshot::from(wire);
    snapshot.canonicalize_and_validate()?;
    Ok(snapshot)
}

/// Canonically encodes a validated portable export manifest.
///
/// # Errors
///
/// Returns an error if validation or serialization fails.
pub fn encode_export_manifest(export: &ExportManifest) -> Result<Vec<u8>> {
    let mut validated = export.clone();
    validated.canonicalize_and_validate()?;
    encode_record(&validated, "workspace export")
}

/// Decodes, validates, and canonicalizes a version-1 export manifest.
///
/// # Errors
///
/// Returns an error for malformed JSON, unsafe relocation, or unsupported version.
pub fn decode_export_manifest(bytes: &[u8]) -> Result<ExportManifest> {
    let value = decode_value(bytes, "workspace export")?;
    validate_envelope(&value, EXPORT_RECORD_TYPE)?;
    let relocation = value
        .get("relocation")
        .and_then(Value::as_object)
        .ok_or_else(|| validation("export relocation must be an object"))?;
    let allowed: BTreeSet<&str> = std::iter::once("paths_relative_to_caller_export_root").collect();
    if relocation.keys().any(|key| !allowed.contains(key.as_str())) {
        return Err(validation(
            "export relocation must not persist provider, root, credential, or secret fields",
        ));
    }
    let wire: ExportManifestWire = decode_record(value, "workspace export")?;
    let mut export = ExportManifest::from(wire);
    export.canonicalize_and_validate()?;
    Ok(export)
}

/// Canonically encodes a validated retention pin revision.
///
/// # Errors
///
/// Returns an error if validation or serialization fails.
pub fn encode_retention_pin_revision(pin: &RetentionPinRevision) -> Result<Vec<u8>> {
    pin.validate()?;
    encode_record(pin, "retention pin revision")
}

/// Decodes and validates a version-1 retention pin revision.
///
/// # Errors
///
/// Returns an error for malformed JSON, the wrong type, or an unsupported version.
pub fn decode_retention_pin_revision(bytes: &[u8]) -> Result<RetentionPinRevision> {
    let value = decode_value(bytes, "retention pin revision")?;
    validate_envelope(&value, PIN_REVISION_RECORD_TYPE)?;
    let mut pin: RetentionPinRevision = decode_record(value, "retention pin revision")?;
    pin.validate()?;
    let raw_sha256 = pin_revision_sha256(bytes);
    if pin.revision() == 1 {
        pin.mark_chain_verified(raw_sha256)?;
    } else {
        pin.record_raw_sha256(raw_sha256)?;
    }
    Ok(pin)
}

/// Canonically encodes a validated latest-pin selector.
///
/// # Errors
///
/// Returns an error if validation or serialization fails.
pub fn encode_retention_pin_latest(selector: &RetentionPinLatest) -> Result<Vec<u8>> {
    selector.validate()?;
    encode_record(selector, "retention pin latest selector")
}

/// Decodes and validates a version-1 latest-pin selector.
///
/// # Errors
///
/// Returns an error for malformed JSON, the wrong type, or an unsupported version.
pub fn decode_retention_pin_latest(bytes: &[u8]) -> Result<RetentionPinLatest> {
    let value = decode_value(bytes, "retention pin latest selector")?;
    validate_envelope(&value, PIN_LATEST_RECORD_TYPE)?;
    let selector: RetentionPinLatest = decode_record(value, "retention pin latest selector")?;
    selector.validate()?;
    Ok(selector)
}
