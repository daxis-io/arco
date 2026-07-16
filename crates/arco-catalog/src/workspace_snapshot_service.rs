//! Direct-addressed workspace snapshot, export, and restore-preflight services.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arco_core::ScopedStorage;
use arco_core::lock::{DistributedLock, LockGuard};
use arco_core::storage::{WritePrecondition, WriteResult};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::{Digest as _, Sha256};

use crate::error::{CatalogError, Result};
use crate::gc::reachability::load_selected_retention_pin;
use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
use crate::state_store::{
    ArcoStateStore, CheckpointOptions, PersistedAuthorityAdapter, PersistedAuthorityKind,
    StateScope, StateStoreCapabilities,
};
use crate::workspace_snapshot::{
    DomainAuthorityReference, DomainEventArchive, EventArchiveCut, ExportManifest,
    LegacyCompatibilityArtifact, ProjectionWatermark, RETENTION_GC_LOCK_MAX_RETRIES,
    RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL, RelocationPolicy, RequiredObject,
    RequiredObjectKind, RetentionPinLatest, RetentionPinRevision, RetentionTarget, WorkspaceScope,
    WorkspaceSnapshot, decode_export_manifest, decode_workspace_snapshot, encode_export_manifest,
    encode_retention_pin_latest, encode_retention_pin_revision, encode_workspace_snapshot,
    export_record_path, retention_pin_latest_path, retention_pin_revision_path,
    snapshot_record_path,
};

/// Caller-controlled identity and timestamps for one immutable workspace snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspaceSnapshotRequest {
    snapshot_id: String,
    pin_id: String,
    created_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
    parent_snapshot_id: Option<String>,
}

impl CreateWorkspaceSnapshotRequest {
    /// Creates a validated, retry-stable snapshot request.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed IDs or an invalid retention interval.
    pub fn new(
        snapshot_id: impl Into<String>,
        pin_id: impl Into<String>,
        created_at: DateTime<Utc>,
        retained_until: DateTime<Utc>,
        parent_snapshot_id: Option<String>,
    ) -> Result<Self> {
        let request = Self {
            snapshot_id: snapshot_id.into(),
            pin_id: pin_id.into(),
            created_at,
            retained_until,
            parent_snapshot_id,
        };
        snapshot_record_path(&request.snapshot_id)?;
        retention_pin_latest_path(&request.pin_id)?;
        if let Some(parent) = &request.parent_snapshot_id {
            snapshot_record_path(parent)?;
            if parent == &request.snapshot_id {
                return Err(validation("snapshot cannot be its own parent"));
            }
        }
        if request.retained_until <= request.created_at {
            return Err(validation("retained_until must be after created_at"));
        }
        Ok(request)
    }

    /// Returns the caller-supplied snapshot identifier.
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Returns the caller-supplied initial pin identifier.
    #[must_use]
    pub fn pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Returns the caller-supplied target retention pin identifier.
    #[must_use]
    pub fn target_pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Returns the deterministic creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the requested retention deadline.
    #[must_use]
    pub const fn retained_until(&self) -> DateTime<Utc> {
        self.retained_until
    }

    /// Returns the optional immutable parent snapshot ID.
    #[must_use]
    pub fn parent_snapshot_id(&self) -> Option<&str> {
        self.parent_snapshot_id.as_deref()
    }
}

/// Caller-controlled identity and timestamps for one immutable snapshot export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspaceExportRequest {
    export_id: String,
    pin_id: String,
    snapshot_id: String,
    source_pin_id: String,
    created_at: DateTime<Utc>,
    retained_until: DateTime<Utc>,
}

impl CreateWorkspaceExportRequest {
    /// Creates a validated, retry-stable export request.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed IDs or an invalid retention interval.
    pub fn new(
        export_id: impl Into<String>,
        pin_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        source_pin_id: impl Into<String>,
        created_at: DateTime<Utc>,
        retained_until: DateTime<Utc>,
    ) -> Result<Self> {
        let request = Self {
            export_id: export_id.into(),
            pin_id: pin_id.into(),
            snapshot_id: snapshot_id.into(),
            source_pin_id: source_pin_id.into(),
            created_at,
            retained_until,
        };
        export_record_path(&request.export_id)?;
        retention_pin_latest_path(&request.pin_id)?;
        snapshot_record_path(&request.snapshot_id)?;
        retention_pin_latest_path(&request.source_pin_id)?;
        if request.pin_id == request.source_pin_id {
            return Err(validation(
                "export pin and source snapshot pin must be distinct",
            ));
        }
        if request.retained_until <= request.created_at {
            return Err(validation("retained_until must be after created_at"));
        }
        Ok(request)
    }

    /// Returns the caller-supplied export identifier.
    #[must_use]
    pub fn export_id(&self) -> &str {
        &self.export_id
    }

    /// Returns the caller-supplied initial pin identifier.
    #[must_use]
    pub fn pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Returns the caller-supplied target retention pin identifier.
    #[must_use]
    pub fn target_pin_id(&self) -> &str {
        &self.pin_id
    }

    /// Returns the exact source snapshot identifier.
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Returns the explicit source-snapshot retention pin identifier.
    #[must_use]
    pub fn source_pin_id(&self) -> &str {
        &self.source_pin_id
    }

    /// Returns the deterministic creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the requested retention deadline.
    #[must_use]
    pub const fn retained_until(&self) -> DateTime<Utc> {
        self.retained_until
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestoreSourceKind {
    Snapshot,
    Export,
}

/// A validated, directly addressable snapshot or export restore source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreSource {
    kind: RestoreSourceKind,
    id: String,
    pin_id: String,
}

impl RestoreSource {
    /// Creates a validated snapshot restore source.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed snapshot ID.
    pub fn snapshot(snapshot_id: impl Into<String>, pin_id: impl Into<String>) -> Result<Self> {
        let id = snapshot_id.into();
        let pin_id = pin_id.into();
        snapshot_record_path(&id)?;
        retention_pin_latest_path(&pin_id)?;
        Ok(Self {
            kind: RestoreSourceKind::Snapshot,
            id,
            pin_id,
        })
    }

    /// Creates a validated export restore source.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed export ID.
    pub fn export(export_id: impl Into<String>, pin_id: impl Into<String>) -> Result<Self> {
        let id = export_id.into();
        let pin_id = pin_id.into();
        export_record_path(&id)?;
        retention_pin_latest_path(&pin_id)?;
        Ok(Self {
            kind: RestoreSourceKind::Export,
            id,
            pin_id,
        })
    }

    /// Returns the safe record identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the explicit retention pin that must actively select the source.
    #[must_use]
    pub fn pin_id(&self) -> &str {
        &self.pin_id
    }
}

/// Safe classification for one read-only restore-preflight issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RestorePreflightIssueKind {
    /// An explicitly referenced record or artifact is absent.
    Missing,
    /// Stored bytes disagree with validated size, checksum, or envelope data.
    Corrupt,
    /// The retained record no longer covers the caller-supplied time.
    Expired,
    /// A configured domain or authority implementation cannot use the record.
    Incompatible,
    /// The record or authority belongs to a different scope.
    OutOfScope,
}

/// One redacted, deterministic restore-preflight issue.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RestorePreflightIssue {
    kind: RestorePreflightIssueKind,
    domain: Option<String>,
    identifier: String,
}

impl RestorePreflightIssue {
    fn new(
        kind: RestorePreflightIssueKind,
        domain: Option<&str>,
        identifier: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            domain: domain.map(ToOwned::to_owned),
            identifier: identifier.into(),
        }
    }

    /// Returns the safe issue classification.
    #[must_use]
    pub const fn kind(&self) -> RestorePreflightIssueKind {
        self.kind
    }

    /// Returns the affected configured domain, when applicable.
    #[must_use]
    pub fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    /// Returns a safe record or artifact-category identifier.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
}

/// Complete sorted result of one read-only restore preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePreflightReport {
    source_id: String,
    issues: Vec<RestorePreflightIssue>,
}

impl RestorePreflightReport {
    fn new(source_id: &str, mut issues: Vec<RestorePreflightIssue>) -> Self {
        issues.sort();
        issues.dedup();
        Self {
            source_id: source_id.to_string(),
            issues,
        }
    }

    /// Returns the safe source record identifier.
    #[must_use]
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// Returns all sorted, deduplicated preflight issues.
    #[must_use]
    pub fn issues(&self) -> &[RestorePreflightIssue] {
        &self.issues
    }

    /// Returns whether the retained cut is ready for a later mutating restore.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.issues.is_empty()
    }
}

struct PreflightCut {
    initial_pin: RetentionPinRevision,
    usable_retention_deadline: DateTime<Utc>,
    scope: WorkspaceScope,
    domains: Vec<DomainAuthorityReference>,
    projections: Vec<ProjectionWatermark>,
    archives: Vec<DomainEventArchive>,
    required_objects: Vec<RequiredObject>,
    compatibility: Vec<LegacyCompatibilityArtifact>,
}

struct CapturedSnapshotCut {
    domains: Vec<DomainAuthorityReference>,
    projections: Vec<ProjectionWatermark>,
    archives: Vec<DomainEventArchive>,
    required_objects: BTreeMap<String, RequiredObject>,
    compatibility: BTreeMap<String, LegacyCompatibilityArtifact>,
}

/// A validated projection cut captured for one retained domain authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionWatermarkCut {
    watermarks: Vec<ProjectionWatermark>,
    required_objects: Vec<RequiredObject>,
    compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
}

impl ProjectionWatermarkCut {
    /// Creates and validates a complete projection cut.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed or duplicate values.
    pub fn new(
        mut watermarks: Vec<ProjectionWatermark>,
        mut required_objects: Vec<RequiredObject>,
        mut compatibility_artifacts: Vec<LegacyCompatibilityArtifact>,
    ) -> Result<Self> {
        for watermark in &watermarks {
            watermark.validate()?;
        }
        watermarks.sort_by(|left, right| {
            (left.projection_name(), left.source_domain())
                .cmp(&(right.projection_name(), right.source_domain()))
        });
        if watermarks.windows(2).any(|pair| {
            matches!(pair, [left, right]
                if left.projection_name() == right.projection_name()
                    && left.source_domain() == right.source_domain())
        }) {
            return Err(validation(
                "projection provider returned duplicate watermarks",
            ));
        }
        canonicalize_required_objects(&mut required_objects)?;
        for artifact in &compatibility_artifacts {
            artifact.validate()?;
        }
        compatibility_artifacts
            .sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
        if compatibility_artifacts.windows(2).any(|pair| {
            matches!(pair, [left, right]
                if left.relative_path() == right.relative_path())
        }) {
            return Err(validation(
                "projection provider returned duplicate compatibility artifacts",
            ));
        }
        Ok(Self {
            watermarks,
            required_objects,
            compatibility_artifacts,
        })
    }

    /// Returns the canonical projection watermarks.
    #[must_use]
    pub fn watermarks(&self) -> &[ProjectionWatermark] {
        &self.watermarks
    }

    /// Returns explicitly required projection objects.
    #[must_use]
    pub fn required_objects(&self) -> &[RequiredObject] {
        &self.required_objects
    }

    /// Returns explicitly declared read-only compatibility artifacts.
    #[must_use]
    pub fn compatibility_artifacts(&self) -> &[LegacyCompatibilityArtifact] {
        &self.compatibility_artifacts
    }
}

/// A validated event archive cut and its explicitly required objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventArchiveCapture {
    archive: DomainEventArchive,
    required_objects: Vec<RequiredObject>,
}

impl EventArchiveCapture {
    /// Creates and validates one explicit domain archive capture.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed or duplicate values.
    pub fn new(
        archive: DomainEventArchive,
        mut required_objects: Vec<RequiredObject>,
    ) -> Result<Self> {
        archive.validate()?;
        canonicalize_required_objects(&mut required_objects)?;
        Ok(Self {
            archive,
            required_objects,
        })
    }

    /// Returns the explicit domain archive boundary.
    #[must_use]
    pub const fn archive(&self) -> &DomainEventArchive {
        &self.archive
    }

    /// Returns explicitly required archive objects.
    #[must_use]
    pub fn required_objects(&self) -> &[RequiredObject] {
        &self.required_objects
    }
}

/// Captures the complete projection cut for one retained authority.
#[async_trait]
pub trait ProjectionWatermarkProvider: Send + Sync {
    /// Captures validated projection metadata and explicit object references.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot capture the requested authority cut.
    async fn capture(&self, authority: &DomainAuthorityReference)
    -> Result<ProjectionWatermarkCut>;
}

/// Captures the explicit event archive boundary for one retained authority.
#[async_trait]
pub trait EventArchiveProvider: Send + Sync {
    /// Captures one validated archive boundary and its explicit objects.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot capture the requested authority cut.
    async fn capture(&self, authority: &DomainAuthorityReference) -> Result<EventArchiveCapture>;
}

/// All mandatory capabilities explicitly configured for one workspace domain.
pub struct WorkspaceDomainBinding {
    state_scope: StateScope,
    state_store: Arc<dyn ArcoStateStore>,
    authority_adapter: Arc<dyn PersistedAuthorityAdapter>,
    projection_provider: Arc<dyn ProjectionWatermarkProvider>,
    event_archive_provider: Arc<dyn EventArchiveProvider>,
}

impl WorkspaceDomainBinding {
    /// Creates a binding with every mandatory capability supplied explicitly.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed state scope.
    pub fn new(
        state_scope: StateScope,
        state_store: Arc<dyn ArcoStateStore>,
        authority_adapter: Arc<dyn PersistedAuthorityAdapter>,
        projection_provider: Arc<dyn ProjectionWatermarkProvider>,
        event_archive_provider: Arc<dyn EventArchiveProvider>,
    ) -> Result<Self> {
        state_scope.validate()?;
        Ok(Self {
            state_scope,
            state_store,
            authority_adapter,
            projection_provider,
            event_archive_provider,
        })
    }

    /// Returns the exact authority scope configured for this domain.
    #[must_use]
    pub const fn state_scope(&self) -> &StateScope {
        &self.state_scope
    }

    /// Returns the configured state-store capability matrix.
    #[must_use]
    pub fn capabilities(&self) -> StateStoreCapabilities {
        self.state_store.capabilities()
    }
}

/// Deterministic, nonempty set of explicitly configured workspace domains.
pub struct WorkspaceDomainRegistry {
    scope: WorkspaceScope,
    bindings: BTreeMap<String, WorkspaceDomainBinding>,
}

impl WorkspaceDomainRegistry {
    /// Creates a validated registry ordered canonically by domain name.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an empty registry, duplicate domain, or
    /// tenant/workspace mismatch.
    pub fn new(scope: WorkspaceScope, bindings: Vec<WorkspaceDomainBinding>) -> Result<Self> {
        scope.validate()?;
        if bindings.is_empty() {
            return Err(validation("workspace domain registry must not be empty"));
        }
        let mut canonical = BTreeMap::new();
        for binding in bindings {
            binding.state_scope.validate()?;
            if binding.state_scope.tenant_id() != scope.tenant_id()
                || binding.state_scope.workspace_id() != scope.workspace_id()
            {
                return Err(validation(
                    "workspace domain binding tenant/workspace scope mismatch",
                ));
            }
            let domain = binding.state_scope.domain().to_string();
            if canonical.insert(domain.clone(), binding).is_some() {
                return Err(validation(format!(
                    "duplicate workspace domain binding {domain}"
                )));
            }
        }
        Ok(Self {
            scope,
            bindings: canonical,
        })
    }

    /// Returns the registry's workspace scope.
    #[must_use]
    pub const fn scope(&self) -> &WorkspaceScope {
        &self.scope
    }

    /// Iterates bindings in canonical domain-name order.
    pub fn domains(&self) -> impl Iterator<Item = (&str, &WorkspaceDomainBinding)> {
        self.bindings
            .iter()
            .map(|(domain, binding)| (domain.as_str(), binding))
    }

    /// Returns the exact configured binding for a domain.
    #[must_use]
    pub fn get(&self, domain: &str) -> Option<&WorkspaceDomainBinding> {
        self.bindings.get(domain)
    }
}

/// Direct-addressed snapshot/export service over one workspace-scoped store.
pub struct WorkspaceSnapshotService {
    storage: ScopedStorage,
    registry: WorkspaceDomainRegistry,
}

impl WorkspaceSnapshotService {
    /// Creates a service whose storage and registry address the same workspace.
    ///
    /// # Errors
    ///
    /// Returns a validation error when the configured scopes disagree.
    pub fn new(storage: ScopedStorage, registry: WorkspaceDomainRegistry) -> Result<Self> {
        if storage.tenant_id() != registry.scope().tenant_id()
            || storage.workspace_id() != registry.scope().workspace_id()
        {
            return Err(validation(
                "snapshot service storage scope does not match domain registry",
            ));
        }
        Ok(Self { storage, registry })
    }

    /// Loads one snapshot by its exact validated identifier.
    ///
    /// # Errors
    ///
    /// Returns a typed not-found error or a validation error for malformed data.
    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<WorkspaceSnapshot> {
        let path = snapshot_record_path(snapshot_id)?;
        let bytes = self
            .get_record_bytes(&path, "workspace snapshot", snapshot_id)
            .await?;
        decode_workspace_snapshot(&bytes)
    }

    /// Loads one export by its exact validated identifier.
    ///
    /// # Errors
    ///
    /// Returns a typed not-found error or a validation error for malformed data.
    pub async fn get_export(&self, export_id: &str) -> Result<ExportManifest> {
        let path = export_record_path(export_id)?;
        let bytes = self
            .get_record_bytes(&path, "workspace export", export_id)
            .await?;
        decode_export_manifest(&bytes)
    }

    /// Revalidates an explicit snapshot closure and publishes an immutable export manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for missing, corrupt, expired, incompatible, out-of-scope,
    /// or conflicting data. The method never lists or copies objects.
    pub async fn export_snapshot(
        &self,
        request: &CreateWorkspaceExportRequest,
    ) -> Result<ExportManifest> {
        let operation_now = Utc::now();
        validate_operation_retention("export", request.retained_until(), operation_now)?;
        let record_path = export_record_path(request.export_id())?;
        match self.storage.get_raw(&record_path).await {
            Ok(bytes) => {
                return self
                    .recover_export_retry(request, &bytes, operation_now)
                    .await;
            }
            Err(arco_core::Error::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }

        let export = self
            .derive_export_from_source(request, operation_now)
            .await?;
        let export_bytes = encode_export_manifest(&export)?;
        let export_initial_pin = Self::export_initial_pin(&export)?;
        let (guard, mut epoch) = self
            .acquire_retention_coordination(
                "workspace-export-finalize",
                RetentionMutationKind::WorkspaceExportFinalize,
                request.export_id(),
            )
            .await?;
        let publication = async {
            self.verify_retained_cut(
                export.domains(),
                export.projection_watermarks(),
                export.event_archives(),
                export.required_objects(),
                export.compatibility_artifacts(),
            )
            .await?;
            self.put_immutable(&mut epoch, &record_path, &export_bytes)
                .await?;
            self.ensure_target_pin(
                &mut epoch,
                &export_initial_pin,
                export.usable_retention_deadline(),
                operation_now,
            )
            .await
        }
        .await;
        Self::finish_retention_coordination(guard, epoch, publication).await?;
        Ok(export)
    }

    /// Performs a complete direct-read restore preflight without mutating state.
    ///
    /// # Errors
    ///
    /// Returns an operation error for a malformed source envelope, permission
    /// failure, or backend outage. Classifiable artifact and compatibility
    /// problems are returned as redacted report issues.
    pub async fn preflight_restore(
        &self,
        source: &RestoreSource,
        expected_scope: &WorkspaceScope,
        now: DateTime<Utc>,
    ) -> Result<RestorePreflightReport> {
        expected_scope.validate()?;
        let cut = self.load_preflight_cut(source).await?;
        let mut issues = Vec::new();
        if &cut.scope != expected_scope || self.registry.scope() != expected_scope {
            issues.push(RestorePreflightIssue::new(
                RestorePreflightIssueKind::OutOfScope,
                None,
                source.id(),
            ));
            return Ok(RestorePreflightReport::new(source.id(), issues));
        }
        self.preflight_source_pin(
            source,
            &cut.initial_pin,
            cut.usable_retention_deadline,
            now,
            &mut issues,
        )
        .await?;
        let invalid_paths = self
            .scan_preflight_objects(&cut.required_objects, &mut issues)
            .await?;

        let required: BTreeMap<&str, &RequiredObject> = cut
            .required_objects
            .iter()
            .map(|object| (object.relative_path(), object))
            .collect();
        Self::preflight_reference_closure(
            &cut.domains,
            &cut.projections,
            &cut.archives,
            &cut.compatibility,
            &required,
            &mut issues,
        );

        self.preflight_authorities(
            &cut.domains,
            expected_scope,
            now,
            &invalid_paths,
            &mut issues,
        )
        .await?;
        Ok(RestorePreflightReport::new(source.id(), issues))
    }

    async fn require_active_retention_pin(
        &self,
        expected_initial: &RetentionPinRevision,
        usable_retention_deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let selected =
            load_selected_retention_pin(&self.storage, expected_initial.pin_id()).await?;
        let latest = selected.latest_revision()?;
        if selected.initial_revision()? != expected_initial
            || latest.target() != expected_initial.target()
            || latest.retained_until() > usable_retention_deadline
        {
            return Err(precondition_failed(
                "source retention pin does not match the immutable target binding",
            ));
        }
        if selected.status_at(now)? != crate::workspace_snapshot::RetentionStatus::Active {
            return Err(validation("source retention pin is not active"));
        }
        Ok(())
    }

    async fn preflight_source_pin(
        &self,
        source: &RestoreSource,
        expected_initial: &RetentionPinRevision,
        usable_retention_deadline: DateTime<Utc>,
        now: DateTime<Utc>,
        issues: &mut Vec<RestorePreflightIssue>,
    ) -> Result<()> {
        if source.pin_id() != expected_initial.pin_id() {
            issues.push(RestorePreflightIssue::new(
                RestorePreflightIssueKind::Corrupt,
                None,
                "retention_pin",
            ));
            return Ok(());
        }
        let selected = match load_selected_retention_pin(&self.storage, source.pin_id()).await {
            Ok(selected) => selected,
            Err(CatalogError::NotFound { .. }) => {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Missing,
                    None,
                    "retention_pin",
                ));
                return Ok(());
            }
            Err(
                CatalogError::Validation { .. }
                | CatalogError::Serialization { .. }
                | CatalogError::InvariantViolation { .. },
            ) => {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Corrupt,
                    None,
                    "retention_pin",
                ));
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if selected.initial_revision()? != expected_initial
            || selected.latest_revision()?.target() != expected_initial.target()
            || selected.latest_revision()?.retained_until() > usable_retention_deadline
        {
            issues.push(RestorePreflightIssue::new(
                RestorePreflightIssueKind::Corrupt,
                None,
                "retention_pin",
            ));
            return Ok(());
        }
        if selected.status_at(now)? != crate::workspace_snapshot::RetentionStatus::Active {
            issues.push(RestorePreflightIssue::new(
                RestorePreflightIssueKind::Expired,
                None,
                "retention_pin",
            ));
        }
        Ok(())
    }

    async fn scan_preflight_objects(
        &self,
        objects: &[RequiredObject],
        issues: &mut Vec<RestorePreflightIssue>,
    ) -> Result<BTreeSet<String>> {
        let mut invalid_paths = BTreeSet::new();
        for object in objects {
            match self.storage.get_raw(object.relative_path()).await {
                Ok(bytes) => {
                    let size_matches =
                        u64::try_from(bytes.len()).is_ok_and(|size| size == object.byte_size());
                    if !size_matches || prefixed_sha256(&bytes) != object.sha256() {
                        invalid_paths.insert(object.relative_path().to_string());
                        issues.push(RestorePreflightIssue::new(
                            RestorePreflightIssueKind::Corrupt,
                            None,
                            safe_object_kind(object.kind()),
                        ));
                    }
                }
                Err(arco_core::Error::NotFound(_)) => {
                    invalid_paths.insert(object.relative_path().to_string());
                    issues.push(RestorePreflightIssue::new(
                        RestorePreflightIssueKind::Missing,
                        None,
                        safe_object_kind(object.kind()),
                    ));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(invalid_paths)
    }

    async fn preflight_authorities(
        &self,
        domains: &[DomainAuthorityReference],
        expected_scope: &WorkspaceScope,
        now: DateTime<Utc>,
        invalid_paths: &BTreeSet<String>,
        issues: &mut Vec<RestorePreflightIssue>,
    ) -> Result<()> {
        for domain in domains {
            let Some(binding) = self.registry.get(domain.domain()) else {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Incompatible,
                    Some(domain.domain()),
                    "domain",
                ));
                continue;
            };
            if domain.scope() != expected_scope
                || domain.authority().scope() != binding.state_scope()
            {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::OutOfScope,
                    Some(domain.domain()),
                    "authority",
                ));
                continue;
            }
            if domain.authority().retention_deadline() <= now {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Expired,
                    Some(domain.domain()),
                    "authority",
                ));
                continue;
            }
            if domain.authority().implementation() != binding.capabilities().implementation() {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Incompatible,
                    Some(domain.domain()),
                    "authority_implementation",
                ));
                continue;
            }
            let authority_paths_valid = !invalid_paths.contains(domain.authority().manifest_path())
                && domain
                    .authority()
                    .checkpoint_path()
                    .is_none_or(|path| !invalid_paths.contains(path));
            if authority_paths_valid {
                Self::resolve_preflight_authority(binding, domain, now, issues).await?;
            }
        }
        Ok(())
    }

    async fn resolve_preflight_authority(
        binding: &WorkspaceDomainBinding,
        domain: &DomainAuthorityReference,
        now: DateTime<Utc>,
        issues: &mut Vec<RestorePreflightIssue>,
    ) -> Result<()> {
        let issue = match binding
            .authority_adapter
            .resolve_persisted_reference_at(domain.authority(), now)
            .await
        {
            Ok(_) => None,
            Err(CatalogError::NotFound { .. }) => Some(RestorePreflightIssueKind::Missing),
            Err(CatalogError::InvariantViolation { .. } | CatalogError::Serialization { .. }) => {
                Some(RestorePreflightIssueKind::Corrupt)
            }
            Err(CatalogError::Validation { .. } | CatalogError::UnsupportedOperation { .. }) => {
                Some(RestorePreflightIssueKind::Incompatible)
            }
            Err(error) => return Err(error),
        };
        if let Some(kind) = issue {
            issues.push(RestorePreflightIssue::new(
                kind,
                Some(domain.domain()),
                "authority",
            ));
        }
        Ok(())
    }

    async fn load_preflight_cut(&self, source: &RestoreSource) -> Result<PreflightCut> {
        match source.kind {
            RestoreSourceKind::Snapshot => {
                let snapshot = self.get_snapshot(source.id()).await?;
                Ok(PreflightCut {
                    initial_pin: Self::snapshot_initial_pin(&snapshot)?,
                    usable_retention_deadline: snapshot.usable_retention_deadline(),
                    scope: snapshot.scope().clone(),
                    domains: snapshot.domains().to_vec(),
                    projections: snapshot.projection_watermarks().to_vec(),
                    archives: snapshot.event_archives().to_vec(),
                    required_objects: snapshot.required_objects().to_vec(),
                    compatibility: snapshot.compatibility_artifacts().to_vec(),
                })
            }
            RestoreSourceKind::Export => {
                let export = self.get_export(source.id()).await?;
                if export.relocation() != RelocationPolicy::relative_to_caller_export_root() {
                    return Err(validation("unsupported export relocation policy"));
                }
                Ok(PreflightCut {
                    initial_pin: Self::export_initial_pin(&export)?,
                    usable_retention_deadline: export.usable_retention_deadline(),
                    scope: export.scope().clone(),
                    domains: export.domains().to_vec(),
                    projections: export.projection_watermarks().to_vec(),
                    archives: export.event_archives().to_vec(),
                    required_objects: export.required_objects().to_vec(),
                    compatibility: export.compatibility_artifacts().to_vec(),
                })
            }
        }
    }

    fn preflight_reference_closure(
        domains: &[DomainAuthorityReference],
        projections: &[ProjectionWatermark],
        archives: &[DomainEventArchive],
        compatibility: &[LegacyCompatibilityArtifact],
        required: &BTreeMap<&str, &RequiredObject>,
        issues: &mut Vec<RestorePreflightIssue>,
    ) {
        for domain in domains {
            let authority = domain.authority();
            if required
                .get(authority.manifest_path())
                .is_none_or(|object| {
                    object.kind() != RequiredObjectKind::AuthorityManifest
                        || object.sha256() != authority.manifest_sha256()
                })
            {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Corrupt,
                    Some(domain.domain()),
                    "authority_manifest_reference",
                ));
            }

            let checkpoint_matches = match (
                authority.reference_kind(),
                authority.checkpoint_path(),
                authority.checkpoint_sha256(),
            ) {
                (PersistedAuthorityKind::Checkpoint, Some(path), Some(sha256)) => {
                    required.get(path).is_some_and(|object| {
                        object.kind() == RequiredObjectKind::Checkpoint && object.sha256() == sha256
                    })
                }
                (PersistedAuthorityKind::StateToken, None, None) => true,
                _ => false,
            };
            if !checkpoint_matches {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Corrupt,
                    Some(domain.domain()),
                    "checkpoint_reference",
                ));
            }
        }
        for projection in projections {
            if required
                .get(projection.manifest().relative_path())
                .is_none_or(|object| {
                    object.kind() != RequiredObjectKind::ProjectionManifest
                        || object.sha256() != projection.manifest().sha256()
                })
            {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Corrupt,
                    Some(projection.source_domain()),
                    "projection_reference",
                ));
            }
        }
        for archive in archives {
            if let EventArchiveCut::Inclusive {
                archive_manifest, ..
            } = archive.cut()
                && required
                    .get(archive_manifest.relative_path())
                    .is_none_or(|object| {
                        object.kind() != RequiredObjectKind::EventArchiveManifest
                            || object.sha256() != archive_manifest.sha256()
                    })
            {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Corrupt,
                    Some(archive.source_domain()),
                    "archive_reference",
                ));
            }
        }
        for artifact in compatibility {
            if required.get(artifact.relative_path()).is_none_or(|object| {
                object.kind() != RequiredObjectKind::LegacyCompatibility
                    || object.sha256() != artifact.sha256()
            }) {
                issues.push(RestorePreflightIssue::new(
                    RestorePreflightIssueKind::Corrupt,
                    None,
                    "legacy_compatibility",
                ));
            }
        }
    }

    /// Captures every configured domain and publishes one immutable retained snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error without publishing a retained root when any capability,
    /// checkpoint, provider, object, or immutable-write precondition fails.
    pub async fn create_snapshot(
        &self,
        request: &CreateWorkspaceSnapshotRequest,
    ) -> Result<WorkspaceSnapshot> {
        let operation_now = Utc::now();
        validate_operation_retention("snapshot", request.retained_until(), operation_now)?;
        let record_path = snapshot_record_path(request.snapshot_id())?;
        match self.storage.get_raw(&record_path).await {
            Ok(bytes) => {
                let snapshot = self.validate_snapshot_retry(request, &bytes)?;
                let snapshot_initial_pin = Self::snapshot_initial_pin(&snapshot)?;
                let (guard, mut epoch) = self
                    .acquire_retention_coordination(
                        "workspace-snapshot-retry",
                        RetentionMutationKind::WorkspaceSnapshotRetry,
                        request.snapshot_id(),
                    )
                    .await?;
                let publication = async {
                    self.verify_retained_cut(
                        snapshot.domains(),
                        snapshot.projection_watermarks(),
                        snapshot.event_archives(),
                        snapshot.required_objects(),
                        snapshot.compatibility_artifacts(),
                    )
                    .await?;
                    self.ensure_target_pin(
                        &mut epoch,
                        &snapshot_initial_pin,
                        snapshot.usable_retention_deadline(),
                        operation_now,
                    )
                    .await
                }
                .await;
                Self::finish_retention_coordination(guard, epoch, publication).await?;
                return Ok(snapshot);
            }
            Err(arco_core::Error::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }

        self.validate_capture_capabilities()?;
        let retention_seconds = request
            .retained_until()
            .signed_duration_since(operation_now)
            .num_seconds();
        let retention_seconds = u64::try_from(retention_seconds).map_err(|_| {
            validation("snapshot retention deadline must be in the future during capture")
        })?;

        let (guard, mut epoch) = self
            .acquire_retention_coordination(
                "workspace-snapshot-finalize",
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                request.snapshot_id(),
            )
            .await?;
        let publication = async {
            let captured = self
                .capture_workspace_cut(request, retention_seconds, &mut epoch)
                .await?;
            let snapshot = WorkspaceSnapshot::new(
                request.snapshot_id(),
                request.pin_id(),
                self.registry.scope().clone(),
                request.created_at(),
                request.retained_until(),
                request.parent_snapshot_id().map(ToOwned::to_owned),
                captured.domains,
                captured.projections,
                captured.archives,
                captured.required_objects.into_values().collect(),
                captured.compatibility.into_values().collect(),
            )?;
            let snapshot_bytes = encode_workspace_snapshot(&snapshot)?;
            let snapshot_initial_pin = Self::snapshot_initial_pin(&snapshot)?;
            self.verify_retained_cut(
                snapshot.domains(),
                snapshot.projection_watermarks(),
                snapshot.event_archives(),
                snapshot.required_objects(),
                snapshot.compatibility_artifacts(),
            )
            .await?;
            self.put_immutable(&mut epoch, &record_path, &snapshot_bytes)
                .await?;
            self.ensure_target_pin(
                &mut epoch,
                &snapshot_initial_pin,
                snapshot.usable_retention_deadline(),
                operation_now,
            )
            .await?;
            Ok(snapshot)
        }
        .await;
        Self::finish_retention_coordination(guard, epoch, publication).await
    }

    async fn capture_workspace_cut(
        &self,
        request: &CreateWorkspaceSnapshotRequest,
        retention_seconds: u64,
        epoch: &mut RetentionMutationEpoch,
    ) -> Result<CapturedSnapshotCut> {
        let mut captured = CapturedSnapshotCut {
            domains: Vec::new(),
            projections: Vec::new(),
            archives: Vec::new(),
            required_objects: BTreeMap::new(),
            compatibility: BTreeMap::new(),
        };
        for (domain, binding) in self.registry.domains() {
            let checkpoint = epoch
                .run_external_mutation(
                    binding.state_store.checkpoint(
                        CheckpointOptions::new(Some(binding.state_scope.clone()))
                            .with_min_retention_seconds(retention_seconds),
                    ),
                )
                .await?;
            if checkpoint.scope() != &binding.state_scope {
                return Err(validation(format!(
                    "checkpoint scope mismatch for domain {domain}"
                )));
            }
            let authority = binding
                .authority_adapter
                .persist_checkpoint_reference(&checkpoint, request.retained_until())
                .await?;
            Self::validate_authority_reference(domain, binding, &authority, request)?;
            let domain_authority =
                DomainAuthorityReference::new(domain, self.registry.scope().clone(), authority)?;
            self.verify_authority_objects(&domain_authority, &mut captured.required_objects)
                .await?;

            let projection_cut = binding
                .projection_provider
                .capture(&domain_authority)
                .await?;
            self.verify_projection_cut(
                domain,
                &domain_authority,
                &projection_cut,
                &mut captured.required_objects,
                &mut captured.compatibility,
            )
            .await?;
            captured
                .projections
                .extend_from_slice(projection_cut.watermarks());

            let archive_capture = binding
                .event_archive_provider
                .capture(&domain_authority)
                .await?;
            self.verify_archive_capture(
                domain,
                &domain_authority,
                &archive_capture,
                &mut captured.required_objects,
            )
            .await?;
            captured.archives.push(archive_capture.archive().clone());
            captured.domains.push(domain_authority);
        }
        Ok(captured)
    }

    async fn get_record_bytes(&self, path: &str, entity: &str, id: &str) -> Result<Bytes> {
        match self.storage.get_raw(path).await {
            Ok(bytes) => Ok(bytes),
            Err(arco_core::Error::NotFound(_)) => Err(CatalogError::NotFound {
                entity: entity.to_string(),
                name: id.to_string(),
            }),
            Err(error) => Err(error.into()),
        }
    }

    async fn verify_retained_cut(
        &self,
        domains: &[DomainAuthorityReference],
        projections: &[ProjectionWatermark],
        archives: &[DomainEventArchive],
        objects: &[RequiredObject],
        compatibility: &[LegacyCompatibilityArtifact],
    ) -> Result<BTreeMap<String, RequiredObject>> {
        let mut required = BTreeMap::new();
        for object in objects {
            self.verify_and_insert_object(
                &mut required,
                object.relative_path(),
                Some(object.byte_size()),
                object.kind(),
                object.sha256(),
            )
            .await?;
        }
        for domain in domains {
            let Some(binding) = self.registry.get(domain.domain()) else {
                return Err(validation(format!(
                    "retained cut names unknown domain {}",
                    domain.domain()
                )));
            };
            if domain.scope() != self.registry.scope()
                || domain.authority().scope() != binding.state_scope()
                || domain.authority().implementation() != binding.capabilities().implementation()
            {
                return Err(validation(format!(
                    "retained authority is incompatible for domain {}",
                    domain.domain()
                )));
            }
            let manifest = required
                .get(domain.authority().manifest_path())
                .ok_or_else(|| validation("authority manifest is not a required object"))?;
            if manifest.kind() != RequiredObjectKind::AuthorityManifest
                || manifest.sha256() != domain.authority().manifest_sha256()
            {
                return Err(validation(
                    "authority manifest required-object metadata disagrees",
                ));
            }
            let checkpoint_path = domain
                .authority()
                .checkpoint_path()
                .ok_or_else(|| validation("retained export authority is not a checkpoint"))?;
            let checkpoint_sha256 = domain
                .authority()
                .checkpoint_sha256()
                .ok_or_else(|| validation("retained checkpoint has no checksum"))?;
            let checkpoint = required
                .get(checkpoint_path)
                .ok_or_else(|| validation("checkpoint is not a required object"))?;
            if checkpoint.kind() != RequiredObjectKind::Checkpoint
                || checkpoint.sha256() != checkpoint_sha256
            {
                return Err(validation("checkpoint required-object metadata disagrees"));
            }
        }
        for projection in projections {
            let object = required
                .get(projection.manifest().relative_path())
                .ok_or_else(|| validation("projection manifest is not a required object"))?;
            if object.kind() != RequiredObjectKind::ProjectionManifest
                || object.sha256() != projection.manifest().sha256()
            {
                return Err(validation(
                    "projection manifest required-object metadata disagrees",
                ));
            }
        }
        for archive in archives {
            if let EventArchiveCut::Inclusive {
                archive_manifest, ..
            } = archive.cut()
            {
                let object = required
                    .get(archive_manifest.relative_path())
                    .ok_or_else(|| validation("archive manifest is not a required object"))?;
                if object.kind() != RequiredObjectKind::EventArchiveManifest
                    || object.sha256() != archive_manifest.sha256()
                {
                    return Err(validation(
                        "archive manifest required-object metadata disagrees",
                    ));
                }
            }
        }
        for artifact in compatibility {
            let object = required
                .get(artifact.relative_path())
                .ok_or_else(|| validation("compatibility artifact is not a required object"))?;
            if object.kind() != RequiredObjectKind::LegacyCompatibility
                || object.sha256() != artifact.sha256()
            {
                return Err(validation(
                    "compatibility artifact required-object metadata disagrees",
                ));
            }
        }
        Ok(required)
    }

    fn validate_export_retry(
        &self,
        request: &CreateWorkspaceExportRequest,
        bytes: &[u8],
    ) -> Result<ExportManifest> {
        let export = decode_export_manifest(bytes)?;
        if export.export_id() != request.export_id()
            || export.snapshot_id() != request.snapshot_id()
            || export.scope() != self.registry.scope()
            || export.created_at() != request.created_at()
            || export.retained_until() != request.retained_until()
            || export.relocation() != RelocationPolicy::relative_to_caller_export_root()
        {
            return Err(precondition_failed(
                "export ID already names different immutable request semantics",
            ));
        }
        if export.target_pin_id() != request.target_pin_id()
            || export.source_pin_id() != request.source_pin_id()
        {
            return Err(precondition_failed(
                "export ID already names different immutable pin bindings",
            ));
        }
        Ok(export)
    }

    async fn load_export_source_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<(String, Bytes, WorkspaceSnapshot)> {
        let path = snapshot_record_path(snapshot_id)?;
        let bytes = self
            .get_record_bytes(&path, "workspace snapshot", snapshot_id)
            .await?;
        let snapshot = decode_workspace_snapshot(&bytes)?;
        if snapshot.scope() != self.registry.scope() {
            return Err(validation(
                "source snapshot is outside the configured workspace",
            ));
        }
        Ok((path, bytes, snapshot))
    }

    fn validate_source_snapshot_retention(
        request: &CreateWorkspaceExportRequest,
        snapshot: &WorkspaceSnapshot,
    ) -> Result<()> {
        if snapshot.target_pin_id() != request.source_pin_id() {
            return Err(precondition_failed(
                "source pin does not match the snapshot's immutable pin binding",
            ));
        }
        let source_expired_at_export = snapshot.retained_until() <= request.created_at();
        let export_outlives_source = request.retained_until() > snapshot.retained_until();
        if source_expired_at_export || export_outlives_source {
            return Err(validation(
                "source snapshot retention does not cover the requested export",
            ));
        }
        Ok(())
    }

    async fn derive_export_from_source(
        &self,
        request: &CreateWorkspaceExportRequest,
        operation_now: DateTime<Utc>,
    ) -> Result<ExportManifest> {
        let (snapshot_path, snapshot_bytes, snapshot) = self
            .load_export_source_snapshot(request.snapshot_id())
            .await?;
        Self::validate_source_snapshot_retention(request, &snapshot)?;
        let source_initial_pin = Self::snapshot_initial_pin(&snapshot)?;
        self.require_active_retention_pin(
            &source_initial_pin,
            snapshot.usable_retention_deadline(),
            operation_now,
        )
        .await?;

        let mut required = self
            .verify_retained_cut(
                snapshot.domains(),
                snapshot.projection_watermarks(),
                snapshot.event_archives(),
                snapshot.required_objects(),
                snapshot.compatibility_artifacts(),
            )
            .await?;
        let snapshot_object = RequiredObject::new(
            &snapshot_path,
            u64::try_from(snapshot_bytes.len())
                .map_err(|_| validation("snapshot record size exceeds u64"))?,
            RequiredObjectKind::SnapshotRecord,
            prefixed_sha256(&snapshot_bytes),
        )?;
        match required.insert(snapshot_path.clone(), snapshot_object.clone()) {
            Some(existing) if existing != snapshot_object => {
                return Err(validation(
                    "source snapshot record path conflicts with its object closure",
                ));
            }
            _ => {}
        }

        ExportManifest::new(
            request.export_id(),
            request.pin_id(),
            request.snapshot_id(),
            request.source_pin_id(),
            self.registry.scope().clone(),
            request.created_at(),
            request.retained_until(),
            snapshot.domains().to_vec(),
            snapshot.projection_watermarks().to_vec(),
            snapshot.event_archives().to_vec(),
            required.into_values().collect(),
            snapshot.compatibility_artifacts().to_vec(),
            RelocationPolicy::relative_to_caller_export_root(),
        )
    }

    async fn recover_export_retry(
        &self,
        request: &CreateWorkspaceExportRequest,
        bytes: &[u8],
        operation_now: DateTime<Utc>,
    ) -> Result<ExportManifest> {
        let export = self.validate_export_retry(request, bytes)?;
        let expected = self
            .derive_export_from_source(request, operation_now)
            .await?;
        if export != expected {
            return Err(precondition_failed(
                "export ID already names a cut different from its source snapshot",
            ));
        }
        let export_initial_pin = Self::export_initial_pin(&expected)?;
        let (guard, mut epoch) = self
            .acquire_retention_coordination(
                "workspace-export-retry",
                RetentionMutationKind::WorkspaceExportRetry,
                request.export_id(),
            )
            .await?;
        let publication = async {
            self.verify_retained_cut(
                expected.domains(),
                expected.projection_watermarks(),
                expected.event_archives(),
                expected.required_objects(),
                expected.compatibility_artifacts(),
            )
            .await?;
            self.ensure_target_pin(
                &mut epoch,
                &export_initial_pin,
                expected.usable_retention_deadline(),
                operation_now,
            )
            .await
        }
        .await;
        Self::finish_retention_coordination(guard, epoch, publication).await?;
        Ok(export)
    }

    fn snapshot_initial_pin(snapshot: &WorkspaceSnapshot) -> Result<RetentionPinRevision> {
        RetentionPinRevision::new(
            snapshot.target_pin_id(),
            1,
            RetentionTarget::snapshot(snapshot.snapshot_id())?,
            snapshot.created_at(),
            snapshot.retained_until(),
            None,
        )
    }

    fn export_initial_pin(export: &ExportManifest) -> Result<RetentionPinRevision> {
        RetentionPinRevision::new(
            export.target_pin_id(),
            1,
            RetentionTarget::export(export.export_id())?,
            export.created_at(),
            export.retained_until(),
            None,
        )
    }

    fn validate_capture_capabilities(&self) -> Result<()> {
        for (domain, binding) in self.registry.domains() {
            let capabilities = binding.capabilities();
            if !capabilities.checkpoints() || !capabilities.read_at() {
                return Err(CatalogError::UnsupportedOperation {
                    message: format!(
                        "domain {domain} state store {} lacks checkpoints or retained reads",
                        capabilities.implementation()
                    ),
                });
            }
        }
        Ok(())
    }

    fn validate_authority_reference(
        domain: &str,
        binding: &WorkspaceDomainBinding,
        authority: &crate::state_store::PersistedAuthorityReference,
        request: &CreateWorkspaceSnapshotRequest,
    ) -> Result<()> {
        authority.validate()?;
        if authority.reference_kind() != PersistedAuthorityKind::Checkpoint
            || authority.implementation() != binding.capabilities().implementation()
            || authority.scope() != &binding.state_scope
            || authority.scope().domain() != domain
            || authority.retention_deadline() < request.retained_until()
        {
            return Err(validation(format!(
                "persisted checkpoint reference mismatch for domain {domain}"
            )));
        }
        Ok(())
    }

    async fn verify_authority_objects(
        &self,
        authority: &DomainAuthorityReference,
        required: &mut BTreeMap<String, RequiredObject>,
    ) -> Result<()> {
        let persisted = authority.authority();
        self.verify_and_insert_object(
            required,
            persisted.manifest_path(),
            None,
            RequiredObjectKind::AuthorityManifest,
            persisted.manifest_sha256(),
        )
        .await?;
        let checkpoint_path = persisted
            .checkpoint_path()
            .ok_or_else(|| validation("checkpoint authority reference has no checkpoint path"))?;
        let checkpoint_sha256 = persisted.checkpoint_sha256().ok_or_else(|| {
            validation("checkpoint authority reference has no checkpoint checksum")
        })?;
        self.verify_and_insert_object(
            required,
            checkpoint_path,
            None,
            RequiredObjectKind::Checkpoint,
            checkpoint_sha256,
        )
        .await
    }

    async fn verify_projection_cut(
        &self,
        domain: &str,
        authority: &DomainAuthorityReference,
        cut: &ProjectionWatermarkCut,
        required: &mut BTreeMap<String, RequiredObject>,
        compatibility: &mut BTreeMap<String, LegacyCompatibilityArtifact>,
    ) -> Result<()> {
        for watermark in cut.watermarks() {
            if watermark.source_domain() != domain
                || watermark.included_authority_sequence()
                    > authority.authority().logical_sequence()
            {
                return Err(validation(format!(
                    "projection watermark mismatch for domain {domain}"
                )));
            }
        }
        for object in cut.required_objects() {
            self.verify_and_insert_object(
                required,
                object.relative_path(),
                Some(object.byte_size()),
                object.kind(),
                object.sha256(),
            )
            .await?;
        }
        for watermark in cut.watermarks() {
            let object = required
                .get(watermark.manifest().relative_path())
                .ok_or_else(|| validation("projection manifest is not a required object"))?;
            if object.sha256() != watermark.manifest().sha256()
                || object.kind() != RequiredObjectKind::ProjectionManifest
            {
                return Err(validation(
                    "projection manifest required-object metadata disagrees",
                ));
            }
        }
        for artifact in cut.compatibility_artifacts() {
            let object = required
                .get(artifact.relative_path())
                .ok_or_else(|| validation("compatibility artifact is not a required object"))?;
            if object.sha256() != artifact.sha256()
                || object.kind() != RequiredObjectKind::LegacyCompatibility
            {
                return Err(validation(
                    "compatibility artifact required-object metadata disagrees",
                ));
            }
            match compatibility.insert(artifact.relative_path().to_string(), artifact.clone()) {
                Some(existing) if existing != *artifact => {
                    return Err(validation("duplicate compatibility path disagrees"));
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn verify_archive_capture(
        &self,
        domain: &str,
        authority: &DomainAuthorityReference,
        capture: &EventArchiveCapture,
        required: &mut BTreeMap<String, RequiredObject>,
    ) -> Result<()> {
        if capture.archive().source_domain() != domain {
            return Err(validation(format!(
                "event archive source mismatch for domain {domain}"
            )));
        }
        for object in capture.required_objects() {
            self.verify_and_insert_object(
                required,
                object.relative_path(),
                Some(object.byte_size()),
                object.kind(),
                object.sha256(),
            )
            .await?;
        }
        if let EventArchiveCut::Inclusive {
            end_sequence,
            archive_manifest,
            ..
        } = capture.archive().cut()
        {
            if *end_sequence > authority.authority().logical_sequence() {
                return Err(validation("event archive exceeds retained authority"));
            }
            let object = required
                .get(archive_manifest.relative_path())
                .ok_or_else(|| validation("archive manifest is not a required object"))?;
            if object.sha256() != archive_manifest.sha256()
                || object.kind() != RequiredObjectKind::EventArchiveManifest
            {
                return Err(validation(
                    "archive manifest required-object metadata disagrees",
                ));
            }
        }
        Ok(())
    }

    async fn verify_and_insert_object(
        &self,
        required: &mut BTreeMap<String, RequiredObject>,
        path: &str,
        expected_size: Option<u64>,
        kind: RequiredObjectKind,
        expected_sha256: &str,
    ) -> Result<()> {
        let bytes = self.storage.get_raw(path).await?;
        let actual_size = u64::try_from(bytes.len())
            .map_err(|_| validation("required object size exceeds u64"))?;
        if expected_size.is_some_and(|size| size != actual_size)
            || prefixed_sha256(&bytes) != expected_sha256
        {
            return Err(CatalogError::InvariantViolation {
                message: format!("required object metadata mismatch for {path}"),
            });
        }
        let object = RequiredObject::new(path, actual_size, kind, expected_sha256)?;
        match required.insert(path.to_string(), object.clone()) {
            Some(existing) if existing != object => Err(validation(format!(
                "duplicate required object {path} disagrees"
            ))),
            _ => Ok(()),
        }
    }

    fn validate_snapshot_retry(
        &self,
        request: &CreateWorkspaceSnapshotRequest,
        bytes: &[u8],
    ) -> Result<WorkspaceSnapshot> {
        let snapshot = decode_workspace_snapshot(bytes)?;
        if snapshot.snapshot_id() != request.snapshot_id()
            || snapshot.scope() != self.registry.scope()
            || snapshot.created_at() != request.created_at()
            || snapshot.retained_until() != request.retained_until()
            || snapshot.parent_snapshot_id() != request.parent_snapshot_id()
        {
            return Err(precondition_failed(
                "snapshot ID already names different immutable request semantics",
            ));
        }
        if snapshot.target_pin_id() != request.target_pin_id() {
            return Err(precondition_failed(
                "snapshot ID already names a different immutable pin binding",
            ));
        }
        Ok(snapshot)
    }

    async fn acquire_retention_coordination(
        &self,
        operation: &str,
        operation_kind: RetentionMutationKind,
        operation_id: &str,
    ) -> Result<(LockGuard<ScopedStorage>, RetentionMutationEpoch)> {
        let mut guard =
            DistributedLock::new(Arc::new(self.storage.clone()), RETENTION_GC_LOCK_PATH)
                .acquire_with_operation(
                    RETENTION_GC_LOCK_TTL,
                    RETENTION_GC_LOCK_MAX_RETRIES,
                    Some(operation.to_string()),
                )
                .await
                .map_err(CatalogError::from)?;
        match RetentionMutationEpoch::claim(
            self.storage.clone(),
            &mut guard,
            operation_kind,
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

    async fn finish_retention_coordination<T>(
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

    async fn ensure_target_pin(
        &self,
        epoch: &mut RetentionMutationEpoch,
        expected_initial: &RetentionPinRevision,
        usable_retention_deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let pin_id = expected_initial.pin_id();
        let selector_path = retention_pin_latest_path(pin_id)?;
        match self.storage.get_raw(&selector_path).await {
            Ok(_) => {
                let selected = load_selected_retention_pin(&self.storage, pin_id).await?;
                if selected.initial_revision()? != expected_initial
                    || selected.latest_revision()?.target() != expected_initial.target()
                    || selected.latest_revision()?.retained_until() > usable_retention_deadline
                {
                    return Err(precondition_failed(
                        "bound retention pin does not match immutable request semantics",
                    ));
                }
                if selected.status_at(now)? != crate::workspace_snapshot::RetentionStatus::Active {
                    return Err(precondition_failed("bound retention pin is not active"));
                }
                Ok(())
            }
            Err(arco_core::Error::NotFound(_)) => {
                self.publish_initial_pin(epoch, expected_initial).await
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn publish_initial_pin(
        &self,
        epoch: &mut RetentionMutationEpoch,
        pin: &RetentionPinRevision,
    ) -> Result<()> {
        if pin.revision() != 1 || pin.predecessor().is_some() {
            return Err(validation("initial retention pin is not revision 1"));
        }
        let pin_bytes = encode_retention_pin_revision(pin)?;
        let revision_path = retention_pin_revision_path(pin.pin_id(), 1)?;
        let selector =
            RetentionPinLatest::new(pin.pin_id(), 1, &revision_path, prefixed_sha256(&pin_bytes))?;
        let selector_bytes = encode_retention_pin_latest(&selector)?;

        self.put_immutable(epoch, &revision_path, &pin_bytes)
            .await?;
        self.put_immutable(
            epoch,
            &retention_pin_latest_path(pin.pin_id())?,
            &selector_bytes,
        )
        .await
    }

    async fn put_immutable(
        &self,
        epoch: &mut RetentionMutationEpoch,
        path: &str,
        bytes: &[u8],
    ) -> Result<()> {
        match epoch
            .put_raw(
                path,
                Bytes::copy_from_slice(bytes),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                let winner = self.storage.get_raw(path).await?;
                if winner.as_ref() == bytes {
                    Ok(())
                } else {
                    Err(precondition_failed(format!(
                        "immutable object conflict at {path}"
                    )))
                }
            }
        }
    }
}

fn canonicalize_required_objects(objects: &mut Vec<RequiredObject>) -> Result<()> {
    for object in &*objects {
        object.validate()?;
    }
    objects.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
    if objects.windows(2).any(|pair| {
        matches!(pair, [left, right]
            if left.relative_path() == right.relative_path())
    }) {
        return Err(validation("provider returned duplicate required objects"));
    }
    Ok(())
}

fn validate_operation_retention(
    operation: &str,
    retained_until: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<()> {
    if retained_until <= now {
        return Err(validation(format!(
            "{operation} retention deadline must be in the future when the operation starts"
        )));
    }
    Ok(())
}

fn prefixed_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

const fn safe_object_kind(kind: RequiredObjectKind) -> &'static str {
    match kind {
        RequiredObjectKind::AuthorityManifest => "authority_manifest",
        RequiredObjectKind::Checkpoint => "checkpoint",
        RequiredObjectKind::ProjectionManifest => "projection_manifest",
        RequiredObjectKind::EventArchiveManifest => "event_archive_manifest",
        RequiredObjectKind::SnapshotRecord => "snapshot_record",
        RequiredObjectKind::ExportRecord => "export_record",
        RequiredObjectKind::RootToken => "root_token",
        RequiredObjectKind::ReviewTokenCut => "review_token_cut",
        RequiredObjectKind::LegacyCompatibility => "legacy_compatibility",
        RequiredObjectKind::Other => "other",
    }
}

fn precondition_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: message.into(),
    }
}

fn validation(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}
