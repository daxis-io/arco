//! Deterministic protection graph for retained workspace roots.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use sha2::{Digest as _, Sha256};

use arco_core::ScopedStorage;

use crate::error::{CatalogError, Result};
use crate::workspace_snapshot::{
    EventArchiveCut, ExportManifest, RetentionPinLatest, RetentionPinRevision, RetentionStatus,
    RetentionTarget, WorkspaceSnapshot, decode_retention_pin_latest, decode_retention_pin_revision,
};
pub(super) use crate::workspace_snapshot::{
    export_record_path, retention_pin_latest_path as pin_latest_path,
    retention_pin_revision_path as pin_revision_path, snapshot_record_path,
};

/// Maximum immutable revisions read to validate one selected pin chain.
///
/// The bound is checked from the selector before any revision object is read,
/// preventing corrupt selectors from forcing unbounded request-time traversal.
const MAX_RETENTION_PIN_REVISIONS: u64 = 1_024;

/// Complete background inventory used to build one fail-closed protection set.
#[derive(Debug, Clone, Default)]
pub(super) struct ReachabilityInventory {
    pub(super) current_heads: Vec<String>,
    pub(super) snapshots: Vec<WorkspaceSnapshot>,
    pub(super) exports: Vec<ExportManifest>,
    pub(super) selected_pins: Vec<SelectedRetentionPin>,
}

/// One immutable pin revision decoded from its exact stored bytes.
#[derive(Debug, Clone)]
struct PinRevisionEvidence {
    revision: RetentionPinRevision,
    raw_sha256: String,
}

impl PinRevisionEvidence {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            revision: decode_retention_pin_revision(bytes)?,
            raw_sha256: sha256_digest(bytes),
        })
    }
}

/// Latest selector and complete immutable predecessor-chain evidence.
#[derive(Debug, Clone)]
pub struct SelectedRetentionPin {
    selector: RetentionPinLatest,
    revisions: Vec<PinRevisionEvidence>,
}

impl SelectedRetentionPin {
    pub fn from_revision_bytes(
        selector: RetentionPinLatest,
        revision_bytes: &[Vec<u8>],
    ) -> Result<Self> {
        let revisions = revision_bytes
            .iter()
            .map(|bytes| PinRevisionEvidence::from_bytes(bytes))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            selector,
            revisions,
        })
    }

    pub fn latest_revision(&self) -> Result<&RetentionPinRevision> {
        self.revisions
            .last()
            .map(|evidence| &evidence.revision)
            .ok_or_else(|| validation("selected retention pin has no revision evidence"))
    }

    pub fn initial_revision(&self) -> Result<&RetentionPinRevision> {
        self.revisions
            .first()
            .map(|evidence| &evidence.revision)
            .ok_or_else(|| validation("selected retention pin has no revision evidence"))
    }

    pub fn validate(&mut self) -> Result<()> {
        self.selector.validate()?;
        self.validate_revision_evidence()?;
        self.validate_transitions()?;
        self.validate_selector_target()?;
        for evidence in &mut self.revisions {
            evidence
                .revision
                .mark_chain_verified(evidence.raw_sha256.clone())?;
        }
        Ok(())
    }

    fn validate_revision_evidence(&self) -> Result<()> {
        if self.revisions.is_empty() {
            return Err(validation(
                "selected retention pin has no revision evidence",
            ));
        }
        if u64::try_from(self.revisions.len()).unwrap_or(u64::MAX) > MAX_RETENTION_PIN_REVISIONS {
            return Err(validation(
                "selected retention pin exceeds the revision traversal limit",
            ));
        }

        for (index, evidence) in self.revisions.iter().enumerate() {
            evidence.revision.validate()?;
            validate_digest(&evidence.raw_sha256)?;
            let expected_revision = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| validation("retention pin revision chain is too long"))?;
            if evidence.revision.revision() != expected_revision {
                return Err(validation(
                    "retention pin revision evidence must be complete and sequential",
                ));
            }
        }
        Ok(())
    }

    fn validate_transitions(&self) -> Result<()> {
        for pair in self.revisions.windows(2) {
            let [previous, current] = pair else {
                continue;
            };
            Self::validate_transition(previous, current)?;
        }
        Ok(())
    }

    fn validate_transition(
        previous: &PinRevisionEvidence,
        current: &PinRevisionEvidence,
    ) -> Result<()> {
        if current.revision.pin_id() != previous.revision.pin_id()
            || current.revision.target() != previous.revision.target()
            || current.revision.created_at() != previous.revision.created_at()
        {
            return Err(validation(
                "retention pin successor changes immutable pin identity",
            ));
        }
        let predecessor = current
            .revision
            .predecessor()
            .ok_or_else(|| validation("retention pin successor is missing predecessor evidence"))?;
        if predecessor.revision() != previous.revision.revision()
            || predecessor.revision_path()
                != pin_revision_path(previous.revision.pin_id(), previous.revision.revision())?
            || predecessor.revision_sha256() != previous.raw_sha256
        {
            return Err(validation(
                "retention pin predecessor evidence does not match stored bytes",
            ));
        }
        if current.revision.revised_at() < previous.revision.revised_at()
            || previous
                .revision
                .structural_status_at(current.revision.revised_at())?
                != RetentionStatus::Active
        {
            return Err(validation(
                "retention pin successor must transition from an active predecessor",
            ));
        }
        match current.revision.released_at() {
            Some(_) => {
                if previous.revision.released_at().is_some()
                    || current.revision.retained_until() != previous.revision.retained_until()
                {
                    return Err(validation(
                        "retention pin release cannot alter retention or follow release",
                    ));
                }
            }
            None => {
                if previous.revision.released_at().is_some()
                    || current.revision.retained_until() <= previous.revision.retained_until()
                {
                    return Err(validation(
                        "retention pin renewal must extend an active predecessor",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_selector_target(&self) -> Result<()> {
        let latest = self.latest_revision()?;
        let latest_evidence = self
            .revisions
            .last()
            .ok_or_else(|| validation("selected retention pin has no revision evidence"))?;
        if self.selector.pin_id() != latest.pin_id()
            || self.selector.revision() != latest.revision()
        {
            return Err(validation(
                "latest pin selector and immutable revision disagree",
            ));
        }
        let expected_path = pin_revision_path(latest.pin_id(), latest.revision())?;
        if self.selector.revision_path() != expected_path {
            return Err(validation(
                "latest pin selector revision path is not canonical",
            ));
        }
        if self.selector.revision_sha256() != latest_evidence.raw_sha256 {
            return Err(validation(
                "latest pin selector revision checksum is corrupt",
            ));
        }

        Ok(())
    }

    pub fn status_at(&self, now: DateTime<Utc>) -> Result<RetentionStatus> {
        self.latest_revision()?.status_at(now)
    }

    fn revision_paths(&self) -> Result<Vec<String>> {
        self.revisions
            .iter()
            .map(|evidence| {
                pin_revision_path(evidence.revision.pin_id(), evidence.revision.revision())
            })
            .collect()
    }
}

/// Directly loads and validates one selected retention pin without listing.
pub async fn load_selected_retention_pin(
    storage: &ScopedStorage,
    pin_id: &str,
) -> Result<SelectedRetentionPin> {
    let selector_path = pin_latest_path(pin_id)?;
    let selector_bytes = storage.get_raw(&selector_path).await?;
    let selector = decode_retention_pin_latest(&selector_bytes)?;
    if selector.pin_id() != pin_id || pin_latest_path(selector.pin_id())? != selector_path {
        return Err(validation(
            "retention pin selector path is not canonical for the requested pin",
        ));
    }
    if selector.revision() > MAX_RETENTION_PIN_REVISIONS {
        return Err(validation(
            "selected retention pin exceeds the revision traversal limit",
        ));
    }

    let mut revision_path = selector.revision_path().to_string();
    let mut expected_revision = selector.revision();
    let mut revision_bytes = Vec::new();
    loop {
        let bytes = storage.get_raw(&revision_path).await?;
        let revision = decode_retention_pin_revision(&bytes)?;
        if revision.pin_id() != selector.pin_id()
            || revision.revision() != expected_revision
            || revision_path != pin_revision_path(revision.pin_id(), revision.revision())?
        {
            return Err(validation(
                "retention pin revision chain path is not canonical",
            ));
        }
        revision_bytes.push(bytes.to_vec());
        if expected_revision == 1 {
            break;
        }
        let predecessor = revision
            .predecessor()
            .ok_or_else(|| validation("retention pin successor is missing predecessor evidence"))?;
        revision_path = predecessor.revision_path().to_string();
        expected_revision = expected_revision
            .checked_sub(1)
            .ok_or_else(|| validation("retention pin revision chain underflow"))?;
    }
    revision_bytes.reverse();
    let mut selected = SelectedRetentionPin::from_revision_bytes(selector, &revision_bytes)?;
    selected.validate()?;
    Ok(selected)
}

/// Deterministic exact-object and prefix protection computed before deletion.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ProtectionSet {
    exact_objects: BTreeSet<String>,
    prefixes: BTreeSet<String>,
}

impl ProtectionSet {
    pub(super) fn protects_object(&self, path: &str) -> bool {
        self.exact_objects.contains(path)
            || self.prefixes.iter().any(|prefix| path.starts_with(prefix))
    }

    pub(super) fn protects_prefix(&self, prefix: &str) -> bool {
        let canonical = canonical_prefix(prefix);
        self.prefixes
            .iter()
            .any(|protected| protected.starts_with(&canonical) || canonical.starts_with(protected))
            || self
                .exact_objects
                .iter()
                .any(|protected| protected.starts_with(&canonical))
    }

    fn protect_exact(&mut self, path: impl Into<String>) {
        self.exact_objects.insert(path.into());
    }

    fn protect_prefix(&mut self, prefix: &str) {
        self.prefixes.insert(canonical_prefix(prefix));
    }
}

pub(super) fn build_protection_set(
    now: DateTime<Utc>,
    inventory: ReachabilityInventory,
) -> Result<ProtectionSet> {
    let mut protection = ProtectionSet::default();
    for current_head in inventory.current_heads {
        validate_inventory_path(&current_head)?;
        if current_head.ends_with('/') {
            protection.protect_prefix(&current_head);
        } else {
            protection.protect_exact(current_head);
        }
    }

    let mut snapshots = BTreeMap::new();
    for snapshot in inventory.snapshots {
        let canonical = crate::workspace_snapshot::encode_workspace_snapshot(&snapshot)?;
        let decoded = crate::workspace_snapshot::decode_workspace_snapshot(&canonical)?;
        let id = decoded.snapshot_id().to_string();
        if snapshots.insert(id.clone(), decoded).is_some() {
            return Err(validation(format!("duplicate snapshot root {id}")));
        }
    }
    let mut exports = BTreeMap::new();
    for export in inventory.exports {
        let canonical = crate::workspace_snapshot::encode_export_manifest(&export)?;
        let decoded = crate::workspace_snapshot::decode_export_manifest(&canonical)?;
        let id = decoded.export_id().to_string();
        if exports.insert(id.clone(), decoded).is_some() {
            return Err(validation(format!("duplicate export root {id}")));
        }
    }

    let mut pins = BTreeMap::new();
    for mut selected in inventory.selected_pins {
        selected.validate()?;
        let pin_id = selected.latest_revision()?.pin_id().to_string();
        if pins.insert(pin_id.clone(), selected).is_some() {
            return Err(validation(format!(
                "ambiguous latest lifecycle for retention pin {pin_id}"
            )));
        }
    }

    for selected in pins.values() {
        let revision = selected.latest_revision()?;
        protection.protect_exact(pin_latest_path(revision.pin_id())?);
        for path in selected.revision_paths()? {
            protection.protect_exact(path);
        }
        if selected.status_at(now)? != RetentionStatus::Active {
            continue;
        }
        match revision.target() {
            RetentionTarget::Snapshot(snapshot_id) => {
                let snapshot = snapshots.get(snapshot_id).ok_or_else(|| {
                    validation(format!("active pin target {snapshot_id} is missing"))
                })?;
                validate_snapshot_pin_binding(selected, snapshot)?;
                protect_snapshot(&mut protection, snapshot)?;
            }
            RetentionTarget::Export(export_id) => {
                let export = exports.get(export_id).ok_or_else(|| {
                    validation(format!("active pin target {export_id} is missing"))
                })?;
                validate_export_pin_binding(selected, export)?;
                protect_export(&mut protection, export)?;
            }
        }
    }
    Ok(protection)
}

fn validate_snapshot_pin_binding(
    selected: &SelectedRetentionPin,
    snapshot: &WorkspaceSnapshot,
) -> Result<()> {
    let expected = RetentionPinRevision::new(
        snapshot.target_pin_id(),
        1,
        RetentionTarget::snapshot(snapshot.snapshot_id())?,
        snapshot.created_at(),
        snapshot.retained_until(),
        None,
    )?;
    validate_active_pin_binding(selected, &expected, snapshot.usable_retention_deadline())
}

fn validate_export_pin_binding(
    selected: &SelectedRetentionPin,
    export: &ExportManifest,
) -> Result<()> {
    let expected = RetentionPinRevision::new(
        export.target_pin_id(),
        1,
        RetentionTarget::export(export.export_id())?,
        export.created_at(),
        export.retained_until(),
        None,
    )?;
    validate_active_pin_binding(selected, &expected, export.usable_retention_deadline())
}

fn validate_active_pin_binding(
    selected: &SelectedRetentionPin,
    expected: &RetentionPinRevision,
    usable_retention_deadline: DateTime<Utc>,
) -> Result<()> {
    if selected.initial_revision()? != expected
        || selected.latest_revision()?.target() != expected.target()
        || selected.latest_revision()?.retained_until() > usable_retention_deadline
    {
        return Err(validation(
            "active retention pin does not match its immutable target record",
        ));
    }
    Ok(())
}

fn protect_snapshot(protection: &mut ProtectionSet, snapshot: &WorkspaceSnapshot) -> Result<()> {
    protection.protect_exact(snapshot_record_path(snapshot.snapshot_id())?);
    protect_cut(
        protection,
        snapshot.domains(),
        snapshot.projection_watermarks(),
        snapshot.event_archives(),
        snapshot.required_objects(),
        snapshot.compatibility_artifacts(),
    );
    Ok(())
}

fn protect_export(protection: &mut ProtectionSet, export: &ExportManifest) -> Result<()> {
    protection.protect_exact(export_record_path(export.export_id())?);
    protection.protect_exact(snapshot_record_path(export.snapshot_id())?);
    protect_cut(
        protection,
        export.domains(),
        export.projection_watermarks(),
        export.event_archives(),
        export.required_objects(),
        export.compatibility_artifacts(),
    );
    Ok(())
}

fn protect_cut(
    protection: &mut ProtectionSet,
    domains: &[crate::workspace_snapshot::DomainAuthorityReference],
    projections: &[crate::workspace_snapshot::ProjectionWatermark],
    archives: &[crate::workspace_snapshot::DomainEventArchive],
    required_objects: &[crate::workspace_snapshot::RequiredObject],
    compatibility: &[crate::workspace_snapshot::LegacyCompatibilityArtifact],
) {
    for domain in domains {
        protection.protect_exact(domain.authority().manifest_path().to_string());
        if let Some(path) = domain.authority().checkpoint_path() {
            protection.protect_exact(path.to_string());
        }
    }
    for projection in projections {
        protection.protect_exact(projection.manifest().relative_path().to_string());
    }
    for archive in archives {
        if let EventArchiveCut::Inclusive {
            archive_manifest, ..
        } = archive.cut()
        {
            protection.protect_exact(archive_manifest.relative_path().to_string());
        }
    }
    for object in required_objects {
        protection.protect_exact(object.relative_path().to_string());
    }
    for artifact in compatibility {
        protection.protect_exact(artifact.relative_path().to_string());
    }
}

fn canonical_prefix(prefix: &str) -> String {
    format!("{}/", prefix.trim_end_matches('/'))
}

pub(super) fn validate_inventory_path(path: &str) -> Result<()> {
    if path.ends_with("//") {
        return Err(validation("retention inventory path is not canonical"));
    }
    let trimmed = path.strip_suffix('/').unwrap_or(path);
    if trimmed.is_empty()
        || trimmed.starts_with('/')
        || is_drive_qualified(trimmed)
        || trimmed.contains('\\')
        || trimmed.chars().any(char::is_control)
        || trimmed
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(validation("retention inventory path is not canonical"));
    }
    Ok(())
}

fn is_drive_qualified(path: &str) -> bool {
    matches!(
        path.as_bytes(),
        [drive, b':', ..] if drive.is_ascii_alphabetic()
    )
}

fn validate_digest(value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(validation("retention reference digest must use sha256:"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(validation(
            "retention reference digest must be 64 lowercase hex characters",
        ));
    }
    Ok(())
}

pub(super) fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn validation(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone as _, Utc};

    use super::*;
    use crate::state_store::{PersistedAuthorityKind, PersistedAuthorityReference, StateScope};
    use crate::workspace_snapshot::{
        ChecksumReference, DomainAuthorityReference, DomainEventArchive, ExportManifest,
        LegacyCompatibilityArtifact, ProjectionWatermark, RelocationPolicy, RequiredObject,
        RequiredObjectKind, RetentionPinLatest, RetentionPinRevision, RetentionTarget,
        WorkspaceScope, WorkspaceSnapshot, encode_retention_pin_revision,
    };

    const SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const EXPORT_ID: &str = "exp_01ARZ3NDEKTSV4RRFFQ69G5FAW";
    const EXPORT_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAX";
    const PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const ALIAS_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAZ";
    const ALIAS_EXPORT_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FB0";
    const DIGEST_1: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const DIGEST_2: &str =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    const DIGEST_3: &str =
        "sha256:3333333333333333333333333333333333333333333333333333333333333333";

    fn ts(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }

    fn snapshot() -> WorkspaceSnapshot {
        let scope = WorkspaceScope::new("tenant", "workspace").expect("scope");
        let authority = PersistedAuthorityReference::new(
            "arco-state-control-mvp",
            StateScope::new("tenant", "workspace", "catalog"),
            PersistedAuthorityKind::Checkpoint,
            "manifest-7",
            7,
            "state-store/control-mvp/catalog/manifests/manifest-7.json",
            DIGEST_1,
            Some("state-store/control-mvp/catalog/checkpoints/checkpoint-7.json".to_string()),
            Some(DIGEST_2.to_string()),
            ts(1_900_000_000),
        )
        .expect("authority");
        WorkspaceSnapshot::new(
            SNAPSHOT_ID,
            PIN_ID,
            scope.clone(),
            ts(1_700_000_000),
            ts(1_800_000_000),
            None,
            vec![
                DomainAuthorityReference::new("catalog", scope, authority)
                    .expect("domain authority"),
            ],
            vec![
                ProjectionWatermark::new(
                    "search",
                    "catalog",
                    7,
                    ChecksumReference::new("projections/search/manifest.json", DIGEST_3)
                        .expect("projection ref"),
                )
                .expect("watermark"),
            ],
            vec![
                DomainEventArchive::inclusive(
                    "catalog",
                    1,
                    7,
                    ChecksumReference::new("archives/catalog/manifest.json", DIGEST_3)
                        .expect("archive ref"),
                )
                .expect("archive"),
            ],
            vec![
                RequiredObject::new(
                    "roots/root-token.json",
                    1,
                    RequiredObjectKind::RootToken,
                    DIGEST_1,
                )
                .expect("root token"),
                RequiredObject::new(
                    "review/cut.json",
                    1,
                    RequiredObjectKind::ReviewTokenCut,
                    DIGEST_1,
                )
                .expect("review cut"),
                RequiredObject::new(
                    "compat/catalog/v1/catalogs.parquet",
                    1,
                    RequiredObjectKind::LegacyCompatibility,
                    DIGEST_3,
                )
                .expect("compat object"),
            ],
            vec![
                LegacyCompatibilityArtifact::new("compat/catalog/v1/catalogs.parquet", DIGEST_3)
                    .expect("compat ref"),
            ],
        )
        .expect("snapshot")
    }

    fn export() -> ExportManifest {
        let scope = WorkspaceScope::new("tenant", "workspace").expect("scope");
        let authority = PersistedAuthorityReference::new(
            "arco-state-control-mvp",
            StateScope::new("tenant", "workspace", "catalog"),
            PersistedAuthorityKind::StateToken,
            "manifest-8",
            8,
            "state-store/control-mvp/catalog/manifests/manifest-8.json",
            DIGEST_1,
            None,
            None,
            ts(1_900_000_000),
        )
        .expect("authority");
        ExportManifest::new(
            EXPORT_ID,
            EXPORT_PIN_ID,
            SNAPSHOT_ID,
            PIN_ID,
            scope.clone(),
            ts(1_700_000_000),
            ts(1_800_000_000),
            vec![
                DomainAuthorityReference::new("catalog", scope, authority)
                    .expect("domain authority"),
            ],
            vec![],
            vec![DomainEventArchive::empty("catalog").expect("archive")],
            vec![
                RequiredObject::new(
                    snapshot_record_path(SNAPSHOT_ID).expect("source snapshot path"),
                    1,
                    RequiredObjectKind::SnapshotRecord,
                    DIGEST_1,
                )
                .expect("source snapshot record"),
                RequiredObject::new(
                    "exports/root-token.json",
                    1,
                    RequiredObjectKind::RootToken,
                    DIGEST_2,
                )
                .expect("export root token"),
            ],
            vec![],
            RelocationPolicy::relative_to_caller_export_root(),
        )
        .expect("export")
    }

    fn selected_pin_for(
        pin_id: &str,
        target: RetentionTarget,
        released: bool,
        retained_until: i64,
    ) -> SelectedRetentionPin {
        let initial = RetentionPinRevision::new(
            pin_id,
            1,
            target,
            ts(1_700_000_000),
            ts(retained_until),
            None,
        )
        .expect("pin revision");
        let mut revision_bytes =
            vec![encode_retention_pin_revision(&initial).expect("initial pin revision bytes")];
        let revision = if released {
            let release = initial
                .release(2, ts(1_710_000_000))
                .expect("release revision");
            revision_bytes
                .push(encode_retention_pin_revision(&release).expect("release pin revision bytes"));
            release
        } else {
            initial
        };
        let revision_sha256 = sha256_digest(
            revision_bytes
                .last()
                .expect("selected pin has revision bytes"),
        );
        let selector = RetentionPinLatest::new(
            pin_id,
            revision.revision(),
            pin_revision_path(pin_id, revision.revision()).expect("revision path"),
            revision_sha256,
        )
        .expect("selector");
        SelectedRetentionPin::from_revision_bytes(selector, &revision_bytes)
            .expect("selected retention pin")
    }

    fn selected_pin(released: bool, retained_until: i64) -> SelectedRetentionPin {
        selected_pin_for(
            PIN_ID,
            RetentionTarget::snapshot(SNAPSHOT_ID).expect("target"),
            released,
            retained_until,
        )
    }

    fn renewed_selected_pin(
        initial_retained_until: i64,
        renewed_retained_until: i64,
    ) -> SelectedRetentionPin {
        let initial = RetentionPinRevision::new(
            PIN_ID,
            1,
            RetentionTarget::snapshot(SNAPSHOT_ID).expect("target"),
            ts(1_700_000_000),
            ts(initial_retained_until),
            None,
        )
        .expect("initial pin revision");
        let renewed = initial
            .renew(2, ts(renewed_retained_until), ts(1_710_000_000))
            .expect("renewed pin revision");
        let revision_bytes = vec![
            encode_retention_pin_revision(&initial).expect("initial pin bytes"),
            encode_retention_pin_revision(&renewed).expect("renewed pin bytes"),
        ];
        let selector = RetentionPinLatest::new(
            PIN_ID,
            2,
            pin_revision_path(PIN_ID, 2).expect("revision path"),
            sha256_digest(&revision_bytes[1]),
        )
        .expect("selector");
        SelectedRetentionPin::from_revision_bytes(selector, &revision_bytes)
            .expect("selected renewed pin")
    }

    #[test]
    fn protection_is_deterministic_and_covers_every_active_root_category() {
        let snapshot = snapshot();
        let pin = selected_pin(false, 1_800_000_000);
        let export = export();
        let export_pin = selected_pin_for(
            EXPORT_PIN_ID,
            RetentionTarget::export(EXPORT_ID).expect("export target"),
            false,
            1_800_000_000,
        );
        let inventory = ReachabilityInventory {
            current_heads: vec![
                "snapshots/catalog/v99/".to_string(),
                "snapshots/search/v42/".to_string(),
            ],
            snapshots: vec![snapshot],
            exports: vec![export],
            selected_pins: vec![pin, export_pin],
        };
        let reversed = ReachabilityInventory {
            current_heads: inventory.current_heads.iter().cloned().rev().collect(),
            snapshots: inventory.snapshots.iter().cloned().rev().collect(),
            exports: inventory.exports.iter().cloned().rev().collect(),
            selected_pins: inventory.selected_pins.iter().cloned().rev().collect(),
        };

        let protection = build_protection_set(ts(1_750_000_000), inventory).expect("protection");
        assert_eq!(
            protection,
            build_protection_set(ts(1_750_000_000), reversed).expect("reordered protection")
        );
        for path in [
            "snapshots/catalog/v99/tables.parquet",
            "snapshots/search/v42/postings.parquet",
            "state-store/control-mvp/catalog/manifests/manifest-7.json",
            "state-store/control-mvp/catalog/checkpoints/checkpoint-7.json",
            "state-store/control-mvp/catalog/manifests/manifest-8.json",
            "projections/search/manifest.json",
            "archives/catalog/manifest.json",
            "roots/root-token.json",
            "review/cut.json",
            "compat/catalog/v1/catalogs.parquet",
            "exports/root-token.json",
        ] {
            assert!(protection.protects_object(path), "must protect {path}");
        }
        assert!(protection.protects_prefix("snapshots/catalog/v99/"));
        assert!(protection.protects_prefix("snapshots/search/v42/"));
        assert!(
            protection
                .protects_object(&snapshot_record_path(SNAPSHOT_ID).expect("snapshot record path"))
        );
        assert!(
            protection.protects_object(&export_record_path(EXPORT_ID).expect("export record path"))
        );
        for (pin_id, revision) in [(PIN_ID, 1), (EXPORT_PIN_ID, 1)] {
            assert!(protection.protects_object(&pin_latest_path(pin_id).expect("pin latest path")));
            assert!(
                protection.protects_object(
                    &pin_revision_path(pin_id, revision).expect("pin revision path")
                )
            );
        }
    }

    #[test]
    fn active_pin_identity_must_match_the_immutable_target_record_binding() {
        let snapshot_alias = ReachabilityInventory {
            snapshots: vec![snapshot()],
            selected_pins: vec![selected_pin_for(
                ALIAS_PIN_ID,
                RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
                false,
                1_800_000_000,
            )],
            ..ReachabilityInventory::default()
        };
        assert!(build_protection_set(ts(1_750_000_000), snapshot_alias).is_err());

        let export_alias = ReachabilityInventory {
            exports: vec![export()],
            selected_pins: vec![selected_pin_for(
                ALIAS_EXPORT_PIN_ID,
                RetentionTarget::export(EXPORT_ID).expect("export target"),
                false,
                1_800_000_000,
            )],
            ..ReachabilityInventory::default()
        };
        assert!(build_protection_set(ts(1_750_000_000), export_alias).is_err());
    }

    #[test]
    fn active_pin_initial_semantics_must_match_the_immutable_target_record() {
        let snapshot_substitution = ReachabilityInventory {
            snapshots: vec![snapshot()],
            selected_pins: vec![selected_pin_for(
                PIN_ID,
                RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
                false,
                1_800_000_001,
            )],
            ..ReachabilityInventory::default()
        };
        assert!(build_protection_set(ts(1_750_000_000), snapshot_substitution).is_err());

        let export_substitution = ReachabilityInventory {
            exports: vec![export()],
            selected_pins: vec![selected_pin_for(
                EXPORT_PIN_ID,
                RetentionTarget::export(EXPORT_ID).expect("export target"),
                false,
                1_800_000_001,
            )],
            ..ReachabilityInventory::default()
        };
        assert!(build_protection_set(ts(1_750_000_000), export_substitution).is_err());
    }

    #[test]
    fn active_pin_must_not_outlive_the_immutable_target_cut() {
        let inventory = ReachabilityInventory {
            snapshots: vec![snapshot()],
            selected_pins: vec![renewed_selected_pin(1_800_000_000, 1_900_000_000)],
            ..ReachabilityInventory::default()
        };

        assert!(
            build_protection_set(ts(1_750_000_000), inventory).is_err(),
            "GC must fail closed when a selected pin outlives its immutable target cut"
        );
    }

    #[test]
    fn expired_and_released_pins_stop_protecting_well_formed_targets() {
        for pin in [
            selected_pin(false, 1_740_000_000),
            selected_pin(true, 1_800_000_000),
        ] {
            let protection = build_protection_set(
                ts(1_750_000_000),
                ReachabilityInventory {
                    current_heads: vec![],
                    snapshots: vec![snapshot()],
                    exports: vec![],
                    selected_pins: vec![pin],
                },
            )
            .expect("inactive pin inventory remains valid");
            assert!(!protection.protects_object("roots/root-token.json"));
            assert!(!protection.protects_object(
                &snapshot_record_path(SNAPSHOT_ID).expect("snapshot record path")
            ));
        }
    }

    #[test]
    fn malformed_ambiguous_corrupt_or_missing_active_roots_fail_closed() {
        let missing = build_protection_set(
            ts(1_750_000_000),
            ReachabilityInventory {
                current_heads: vec![],
                snapshots: vec![],
                exports: vec![],
                selected_pins: vec![selected_pin(false, 1_800_000_000)],
            },
        );
        assert!(missing.is_err());

        let mut corrupt_digest = selected_pin(false, 1_800_000_000);
        corrupt_digest.selector = RetentionPinLatest::new(
            PIN_ID,
            1,
            pin_revision_path(PIN_ID, 1).expect("pin revision path"),
            DIGEST_1,
        )
        .expect("corrupt selector is structurally valid");
        assert!(
            build_protection_set(
                ts(1_750_000_000),
                ReachabilityInventory {
                    current_heads: vec![],
                    snapshots: vec![snapshot()],
                    exports: vec![],
                    selected_pins: vec![corrupt_digest],
                },
            )
            .is_err()
        );

        let ambiguous = selected_pin(false, 1_800_000_000);
        assert!(
            build_protection_set(
                ts(1_750_000_000),
                ReachabilityInventory {
                    current_heads: vec![],
                    snapshots: vec![snapshot()],
                    exports: vec![],
                    selected_pins: vec![ambiguous.clone(), ambiguous],
                },
            )
            .is_err()
        );
    }

    #[test]
    fn current_head_paths_reject_repeated_trailing_separators() {
        let inventory = ReachabilityInventory {
            current_heads: vec!["snapshots/catalog/v1//".to_string()],
            ..ReachabilityInventory::default()
        };
        assert!(build_protection_set(ts(1_750_000_000), inventory).is_err());
    }

    #[test]
    fn arbitrary_successor_without_predecessor_chain_fails_closed() {
        let initial = RetentionPinRevision::new(
            PIN_ID,
            1,
            RetentionTarget::snapshot(SNAPSHOT_ID).expect("target"),
            ts(1_700_000_000),
            ts(1_800_000_000),
            None,
        )
        .expect("initial revision");
        let revision = initial
            .renew(2, ts(1_900_000_000), ts(1_710_000_000))
            .expect("valid successor");
        let bytes = encode_retention_pin_revision(&revision).expect("revision bytes");
        let digest = sha256_digest(&bytes);
        let selector = RetentionPinLatest::new(
            PIN_ID,
            2,
            pin_revision_path(PIN_ID, 2).expect("pin revision path"),
            digest,
        )
        .expect("selector");
        let inventory = ReachabilityInventory {
            current_heads: vec![],
            snapshots: vec![snapshot()],
            exports: vec![],
            selected_pins: vec![
                SelectedRetentionPin::from_revision_bytes(selector, &[bytes])
                    .expect("structurally decodable successor"),
            ],
        };
        assert!(build_protection_set(ts(1_750_000_000), inventory).is_err());
    }
}
