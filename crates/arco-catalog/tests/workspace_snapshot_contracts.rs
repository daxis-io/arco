//! Phase 7A retained workspace-cut contract tests.

use chrono::{TimeZone as _, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;

use arco_catalog::parquet_util::{
    WorkspaceSnapshotCatalogRecord, workspace_snapshot_schema, write_workspace_snapshots,
};
use arco_catalog::workspace_snapshot::{
    ChecksumReference, DomainAuthorityReference, DomainEventArchive, ExportManifest,
    LegacyCompatibilityArtifact, ProjectionWatermark, RelocationPolicy, RequiredObject,
    RequiredObjectKind, RetentionPinLatest, RetentionPinRevision, RetentionStatus, RetentionTarget,
    WorkspaceScope, WorkspaceSnapshot, decode_export_manifest, decode_retention_pin_revision,
    decode_workspace_snapshot, encode_export_manifest, encode_retention_pin_revision,
    encode_workspace_snapshot, snapshot_record_path,
};
use arco_catalog::{PersistedAuthorityKind, PersistedAuthorityReference, StateScope};

const SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const PARENT_SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAW";
const EXPORT_ID: &str = "exp_01ARZ3NDEKTSV4RRFFQ69G5FAX";
const PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAY";
const EXPORT_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAZ";
const MANIFEST_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const CHECKPOINT_DIGEST: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const PROJECTION_DIGEST: &str =
    "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const ARCHIVE_DIGEST: &str =
    "sha256:4444444444444444444444444444444444444444444444444444444444444444";

trait AmbiguousIfDeserialize<Marker> {
    fn marker() {}
}

// The inferred marker is ambiguous only when the record implements DeserializeOwned.
impl<T: ?Sized> AmbiguousIfDeserialize<()> for T {}
impl<T: ?Sized + DeserializeOwned> AmbiguousIfDeserialize<u8> for T {}

fn ts(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
}

fn workspace_scope() -> WorkspaceScope {
    WorkspaceScope::new("tenant", "workspace").expect("workspace scope")
}

fn authority(domain: &str, sequence: u64, checkpoint: bool) -> DomainAuthorityReference {
    let state_scope = StateScope::new("tenant", "workspace", domain);
    let manifest_path =
        format!("state-store/control-mvp/{domain}/manifests/manifest-{sequence}.json");
    let reference = if checkpoint {
        PersistedAuthorityReference::new(
            "arco-state-control-mvp",
            state_scope,
            PersistedAuthorityKind::Checkpoint,
            format!("manifest-{sequence}"),
            sequence,
            manifest_path,
            MANIFEST_DIGEST,
            Some(format!(
                "state-store/control-mvp/{domain}/checkpoints/checkpoint-{sequence}.json"
            )),
            Some(CHECKPOINT_DIGEST.to_string()),
            ts(4_000_000_000),
        )
        .expect("checkpoint authority reference")
    } else {
        PersistedAuthorityReference::new(
            "arco-state-control-mvp",
            state_scope,
            PersistedAuthorityKind::StateToken,
            format!("manifest-{sequence}"),
            sequence,
            manifest_path,
            MANIFEST_DIGEST,
            None,
            None,
            ts(4_000_000_000),
        )
        .expect("state authority reference")
    };
    DomainAuthorityReference::new(domain, workspace_scope(), reference)
        .expect("domain authority reference")
}

fn required_objects() -> Vec<RequiredObject> {
    vec![
        RequiredObject::new(
            "archives/catalog/events.json",
            7,
            RequiredObjectKind::EventArchiveManifest,
            ARCHIVE_DIGEST,
        )
        .expect("archive object"),
        RequiredObject::new(
            "compat/catalog/v1/catalogs.parquet",
            11,
            RequiredObjectKind::LegacyCompatibility,
            PROJECTION_DIGEST,
        )
        .expect("compatibility object"),
        RequiredObject::new(
            "projections/catalog/snapshots.parquet",
            13,
            RequiredObjectKind::ProjectionManifest,
            PROJECTION_DIGEST,
        )
        .expect("projection object"),
    ]
}

fn snapshot() -> WorkspaceSnapshot {
    WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        Some(PARENT_SNAPSHOT_ID.to_string()),
        vec![
            authority("lineage", 9, false),
            authority("catalog", 7, true),
        ],
        vec![
            ProjectionWatermark::new(
                "search",
                "catalog",
                7,
                ChecksumReference::new("projections/catalog/snapshots.parquet", PROJECTION_DIGEST)
                    .expect("projection manifest reference"),
            )
            .expect("projection watermark"),
        ],
        vec![
            DomainEventArchive::inclusive(
                "catalog",
                3,
                7,
                ChecksumReference::new("archives/catalog/events.json", ARCHIVE_DIGEST)
                    .expect("archive manifest reference"),
            )
            .expect("inclusive archive"),
            DomainEventArchive::empty("lineage").expect("empty archive"),
        ],
        required_objects(),
        vec![
            LegacyCompatibilityArtifact::new(
                "compat/catalog/v1/catalogs.parquet",
                PROJECTION_DIGEST,
            )
            .expect("compatibility artifact"),
        ],
    )
    .expect("workspace snapshot")
}

fn export_required_objects(snapshot: &WorkspaceSnapshot) -> Vec<RequiredObject> {
    let mut objects = snapshot.required_objects().to_vec();
    objects.push(
        RequiredObject::new(
            snapshot_record_path(snapshot.snapshot_id()).expect("source snapshot path"),
            1,
            RequiredObjectKind::SnapshotRecord,
            MANIFEST_DIGEST,
        )
        .expect("source snapshot record"),
    );
    objects
}

#[test]
fn snapshot_v1_round_trips_canonically_and_accepts_additive_fields() {
    let snapshot = snapshot();
    assert_eq!(snapshot.target_pin_id(), PIN_ID);
    assert_eq!(
        snapshot
            .domains()
            .iter()
            .map(DomainAuthorityReference::domain)
            .collect::<Vec<_>>(),
        vec!["catalog", "lineage"]
    );

    let encoded = encode_workspace_snapshot(&snapshot).expect("encode snapshot");
    assert!(!String::from_utf8_lossy(&encoded).contains("StateToken"));
    assert!(!String::from_utf8_lossy(&encoded).contains("CheckpointToken"));
    let decoded = decode_workspace_snapshot(&encoded).expect("decode snapshot");
    assert_eq!(snapshot, decoded);
    assert_eq!(
        encoded,
        encode_workspace_snapshot(&decoded).expect("re-encode")
    );

    let mut additive: Value = serde_json::from_slice(&encoded).expect("snapshot json");
    additive["future_v1_hint"] = Value::String("ignored".to_string());
    let additive = serde_jcs::to_vec(&additive).expect("canonical additive json");
    assert_eq!(
        snapshot,
        decode_workspace_snapshot(&additive).expect("additive v1")
    );

    let mut missing_binding: Value = serde_json::from_slice(&encoded).expect("snapshot json");
    missing_binding
        .as_object_mut()
        .expect("snapshot object")
        .remove("target_pin_id");
    assert!(
        decode_workspace_snapshot(&serde_json::to_vec(&missing_binding).expect("json")).is_err(),
        "snapshot target pin identity is required"
    );
}

#[test]
fn invariant_records_do_not_expose_unvalidated_deserialize() {
    let _ = <WorkspaceSnapshot as AmbiguousIfDeserialize<_>>::marker;
    let _ = <ExportManifest as AmbiguousIfDeserialize<_>>::marker;
}

#[test]
fn snapshot_decode_rejects_wrong_record_type_and_version() {
    let encoded = encode_workspace_snapshot(&snapshot()).expect("encode snapshot");
    let mut value: Value = serde_json::from_slice(&encoded).expect("snapshot json");

    value["record_type"] = Value::String("workspace_export".to_string());
    assert!(decode_workspace_snapshot(&serde_json::to_vec(&value).expect("json")).is_err());

    value["record_type"] = Value::String("workspace_snapshot".to_string());
    value["version"] = Value::from(2_u64);
    assert!(decode_workspace_snapshot(&serde_json::to_vec(&value).expect("json")).is_err());
}

#[test]
fn identifiers_require_canonical_ulid_spelling() {
    let encoded_snapshot = encode_workspace_snapshot(&snapshot()).expect("snapshot bytes");
    let mut snapshot_value: Value =
        serde_json::from_slice(&encoded_snapshot).expect("snapshot json");
    snapshot_value["snapshot_id"] = Value::String(SNAPSHOT_ID.to_ascii_lowercase());
    assert!(
        decode_workspace_snapshot(&serde_json::to_vec(&snapshot_value).expect("snapshot json"))
            .is_err(),
        "lowercase snapshot ULID alias must be rejected"
    );

    let mut snapshot_value: Value =
        serde_json::from_slice(&encoded_snapshot).expect("snapshot json");
    snapshot_value["target_pin_id"] = Value::String(PIN_ID.to_ascii_lowercase());
    assert!(
        decode_workspace_snapshot(&serde_json::to_vec(&snapshot_value).expect("snapshot json"))
            .is_err(),
        "lowercase snapshot target-pin ULID alias must be rejected"
    );

    let retained = snapshot();
    let export = ExportManifest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_100),
        ts(1_800_000_000),
        retained.domains().to_vec(),
        retained.projection_watermarks().to_vec(),
        retained.event_archives().to_vec(),
        export_required_objects(&retained),
        retained.compatibility_artifacts().to_vec(),
        RelocationPolicy::relative_to_caller_export_root(),
    )
    .expect("export manifest");
    let mut export_value: Value =
        serde_json::from_slice(&encode_export_manifest(&export).expect("export bytes"))
            .expect("export json");
    export_value["export_id"] = Value::String(EXPORT_ID.to_ascii_lowercase());
    assert!(
        decode_export_manifest(&serde_json::to_vec(&export_value).expect("export json")).is_err(),
        "lowercase export ULID alias must be rejected"
    );

    let encoded_export = encode_export_manifest(&export).expect("export bytes");
    let mut export_value: Value = serde_json::from_slice(&encoded_export).expect("export json");
    export_value["target_pin_id"] = Value::String(EXPORT_PIN_ID.to_ascii_lowercase());
    assert!(
        decode_export_manifest(&serde_json::to_vec(&export_value).expect("export json")).is_err(),
        "lowercase export target-pin ULID alias must be rejected"
    );

    let mut export_value: Value = serde_json::from_slice(&encoded_export).expect("export json");
    export_value["source_pin_id"] = Value::String(PIN_ID.to_ascii_lowercase());
    assert!(
        decode_export_manifest(&serde_json::to_vec(&export_value).expect("export json")).is_err(),
        "lowercase export source-pin ULID alias must be rejected"
    );

    assert!(
        RetentionPinRevision::new(
            PIN_ID.to_ascii_lowercase(),
            1,
            RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
            ts(1_700_000_000),
            ts(1_800_000_000),
            None,
        )
        .is_err(),
        "lowercase pin ULID alias must be rejected"
    );
}

#[test]
fn latest_pin_selector_path_matches_pin_and_revision() {
    let canonical = format!("retention/pins/{PIN_ID}/revisions/3.json");
    RetentionPinLatest::new(PIN_ID, 3, canonical, MANIFEST_DIGEST)
        .expect("canonical selector path");

    for mismatch in [
        format!("retention/pins/{PIN_ID}/revisions/2.json"),
        format!(
            "retention/pins/{}/revisions/3.json",
            "pin_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
        ),
    ] {
        assert!(
            RetentionPinLatest::new(PIN_ID, 3, &mismatch, MANIFEST_DIGEST).is_err(),
            "selector accepted mismatched revision path {mismatch}"
        );
    }
}

#[test]
fn snapshot_validation_rejects_bad_ids_scope_domains_archives_and_paths() {
    assert!(WorkspaceScope::new(" ", "workspace").is_err());
    assert!(WorkspaceScope::new("tenant", "\n").is_err());
    assert!(
        DomainAuthorityReference::new(
            "catalog",
            workspace_scope(),
            authority("lineage", 1, false).authority().clone()
        )
        .is_err()
    );
    assert!(
        DomainEventArchive::inclusive(
            "catalog",
            8,
            7,
            ChecksumReference::new("archives/catalog/events.json", ARCHIVE_DIGEST)
                .expect("archive reference")
        )
        .is_err()
    );
    let archive_beyond_cut = WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
        vec![authority("catalog", 7, false)],
        vec![],
        vec![
            DomainEventArchive::inclusive(
                "catalog",
                1,
                8,
                ChecksumReference::new("archives/catalog/events.json", ARCHIVE_DIGEST)
                    .expect("archive reference"),
            )
            .expect("archive shape"),
        ],
        vec![],
        vec![],
    );
    assert!(archive_beyond_cut.is_err());

    let omitted_archive = WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
        vec![
            authority("catalog", 7, false),
            authority("lineage", 9, false),
        ],
        vec![],
        vec![DomainEventArchive::empty("catalog").expect("catalog archive")],
        vec![],
        vec![],
    );
    assert!(
        omitted_archive.is_err(),
        "every retained domain must declare empty or inclusive archive state"
    );

    for invalid in [
        "/absolute/path",
        "C:/outside/object",
        "../traversal",
        "a/../b",
        "a/./b",
        "a//b",
        "a\\b",
        "a/\u{0007}b",
        "",
    ] {
        assert!(
            RequiredObject::new(invalid, 1, RequiredObjectKind::Other, MANIFEST_DIGEST).is_err(),
            "path should be rejected: {invalid:?}"
        );
    }
    for invalid in [
        "1111111111111111111111111111111111111111111111111111111111111111",
        "sha256:ABCDEF1111111111111111111111111111111111111111111111111111111111",
        "sha256:1234",
    ] {
        assert!(RequiredObject::new("objects/a", 1, RequiredObjectKind::Other, invalid).is_err());
    }

    let duplicate_domain = WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
        vec![
            authority("catalog", 1, false),
            authority("catalog", 2, false),
        ],
        vec![],
        vec![],
        vec![],
        vec![],
    );
    assert!(duplicate_domain.is_err());

    let duplicate_path = WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
        vec![authority("catalog", 1, false)],
        vec![],
        vec![DomainEventArchive::empty("catalog").expect("archive")],
        vec![
            RequiredObject::new("same/path", 1, RequiredObjectKind::Other, MANIFEST_DIGEST)
                .expect("object"),
            RequiredObject::new("same/path", 2, RequiredObjectKind::Other, CHECKPOINT_DIGEST)
                .expect("object"),
        ],
        vec![],
    );
    assert!(duplicate_path.is_err());

    for invalid in ["snap_not-a-ulid", "exp_01ARZ3NDEKTSV4RRFFQ69G5FAV"] {
        let mut value: Value =
            serde_json::from_slice(&encode_workspace_snapshot(&snapshot()).expect("encode"))
                .expect("json");
        value["snapshot_id"] = Value::String(invalid.to_string());
        assert!(decode_workspace_snapshot(&serde_json::to_vec(&value).expect("json")).is_err());
    }
}

#[test]
fn workspace_paths_reject_drive_qualified_absolute_forms() {
    assert!(
        RequiredObject::new(
            "C:/outside/object",
            1,
            RequiredObjectKind::Other,
            MANIFEST_DIGEST,
        )
        .is_err()
    );
    assert!(ChecksumReference::new("z:/outside/manifest.json", MANIFEST_DIGEST).is_err());
}

#[test]
fn compatibility_is_read_only_and_must_match_a_required_object() {
    let artifact =
        LegacyCompatibilityArtifact::new("compat/catalog/v1/catalogs.parquet", PROJECTION_DIGEST)
            .expect("artifact");
    assert!(artifact.is_read_only());

    let missing = WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
        vec![authority("catalog", 1, false)],
        vec![],
        vec![DomainEventArchive::empty("catalog").expect("archive")],
        vec![],
        vec![artifact.clone()],
    );
    assert!(missing.is_err());

    let wrong_digest = WorkspaceSnapshot::new(
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
        vec![authority("catalog", 1, false)],
        vec![],
        vec![DomainEventArchive::empty("catalog").expect("archive")],
        vec![
            RequiredObject::new(
                artifact.relative_path(),
                1,
                RequiredObjectKind::LegacyCompatibility,
                MANIFEST_DIGEST,
            )
            .expect("object"),
        ],
        vec![artifact],
    );
    assert!(wrong_digest.is_err());
}

#[test]
fn export_v1_is_canonical_portable_and_contains_no_provider_root() {
    let snapshot = snapshot();
    let export = ExportManifest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        workspace_scope(),
        ts(1_700_000_100),
        ts(1_800_000_000),
        snapshot.domains().to_vec(),
        snapshot.projection_watermarks().to_vec(),
        snapshot.event_archives().to_vec(),
        export_required_objects(&snapshot),
        snapshot.compatibility_artifacts().to_vec(),
        RelocationPolicy::relative_to_caller_export_root(),
    )
    .expect("export manifest");

    assert_eq!(export.target_pin_id(), EXPORT_PIN_ID);
    assert_eq!(export.source_pin_id(), PIN_ID);

    let encoded = encode_export_manifest(&export).expect("encode export");
    let text = String::from_utf8_lossy(&encoded);
    for forbidden in [
        "s3://",
        "gs://",
        "provider_uri",
        "root_uri",
        "credential",
        "secret",
    ] {
        assert!(!text.contains(forbidden), "must omit {forbidden}");
    }
    assert_eq!(
        export,
        decode_export_manifest(&encoded).expect("decode export manifest")
    );

    let mut additive: Value = serde_json::from_slice(&encoded).expect("export json");
    additive["future_v1_hint"] = Value::String("ignored".to_string());
    let additive = serde_jcs::to_vec(&additive).expect("canonical additive export json");
    assert_eq!(
        export,
        decode_export_manifest(&additive).expect("additive export v1")
    );

    let mut missing_binding: Value = serde_json::from_slice(&encoded).expect("export json");
    missing_binding
        .as_object_mut()
        .expect("export object")
        .remove("source_pin_id");
    assert!(
        decode_export_manifest(&serde_json::to_vec(&missing_binding).expect("json")).is_err(),
        "source pin identity is required"
    );

    let mut value: Value = serde_json::from_slice(&encoded).expect("export json");
    value["relocation"]["provider_uri"] = Value::String("s3://secret-bucket".to_string());
    assert!(decode_export_manifest(&serde_json::to_vec(&value).expect("json")).is_err());
}

#[test]
fn export_requires_exactly_one_canonical_source_snapshot_record() {
    let snapshot = snapshot();
    let build = |objects: Vec<RequiredObject>| {
        ExportManifest::new(
            EXPORT_ID,
            EXPORT_PIN_ID,
            SNAPSHOT_ID,
            PIN_ID,
            workspace_scope(),
            ts(1_700_000_100),
            ts(1_800_000_000),
            snapshot.domains().to_vec(),
            snapshot.projection_watermarks().to_vec(),
            snapshot.event_archives().to_vec(),
            objects,
            snapshot.compatibility_artifacts().to_vec(),
            RelocationPolicy::relative_to_caller_export_root(),
        )
    };

    assert!(
        build(snapshot.required_objects().to_vec()).is_err(),
        "an export must explicitly retain its source snapshot record"
    );

    let canonical_path = snapshot_record_path(SNAPSHOT_ID).expect("canonical snapshot path");
    let mut wrong_kind = snapshot.required_objects().to_vec();
    wrong_kind.push(
        RequiredObject::new(
            &canonical_path,
            1,
            RequiredObjectKind::Other,
            MANIFEST_DIGEST,
        )
        .expect("wrong-kind source record"),
    );
    assert!(build(wrong_kind).is_err());

    let mut exact = snapshot.required_objects().to_vec();
    exact.push(
        RequiredObject::new(
            &canonical_path,
            1,
            RequiredObjectKind::SnapshotRecord,
            MANIFEST_DIGEST,
        )
        .expect("source snapshot record"),
    );
    assert!(build(exact.clone()).is_ok());

    exact.push(
        RequiredObject::new(
            snapshot_record_path(PARENT_SNAPSHOT_ID).expect("second snapshot path"),
            1,
            RequiredObjectKind::SnapshotRecord,
            CHECKPOINT_DIGEST,
        )
        .expect("second snapshot record"),
    );
    assert!(build(exact).is_err());
}

#[test]
fn pin_constructor_only_creates_active_revision_one() {
    let created = ts(1_700_000_000);
    let retained_until = ts(1_800_000_000);
    assert!(
        RetentionPinRevision::new(
            PIN_ID,
            2,
            RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
            created,
            retained_until,
            None,
        )
        .is_err(),
        "only revision 1 may be constructed without predecessor proof"
    );
    assert!(
        RetentionPinRevision::new(
            PIN_ID,
            1,
            RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
            created,
            retained_until,
            Some(ts(1_750_000_000)),
        )
        .is_err(),
        "an initial pin cannot be born released"
    );
}

fn active_pin() -> RetentionPinRevision {
    RetentionPinRevision::new(
        PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(1_700_000_000),
        ts(1_800_000_000),
        None,
    )
    .expect("active pin")
}

#[test]
fn expired_pin_cannot_transition_to_released() {
    let active = active_pin();
    assert!(
        active.release(2, ts(1_900_000_000)).is_err(),
        "an expired predecessor cannot transition to released"
    );
}

#[test]
fn future_release_stays_protected_until_its_effective_time() {
    let active = active_pin();
    let released = active
        .release(2, ts(1_780_000_000))
        .expect("schedule release while predecessor is active");
    assert_eq!(
        RetentionStatus::Active,
        released
            .status_at(ts(1_770_000_000))
            .expect("future release still protects")
    );
    assert_eq!(
        RetentionStatus::Released,
        released.status_at(ts(1_780_000_000)).expect("status")
    );
}

#[test]
fn successor_pin_records_carry_predecessor_proof_and_decode_fail_closed() {
    let renewed = active_pin()
        .renew(2, ts(1_900_000_000), ts(1_750_000_000))
        .expect("renew pin");
    let renewed_json: Value = serde_json::from_slice(
        &encode_retention_pin_revision(&renewed).expect("renewed pin bytes"),
    )
    .expect("renewed pin json");
    assert!(renewed_json.get("predecessor").is_some());
    assert!(renewed_json.get("revised_at").is_some());
    let decoded_renewed = decode_retention_pin_revision(
        &serde_json::to_vec(&renewed_json).expect("renewed pin json bytes"),
    )
    .expect("decode successor shape");
    assert_eq!(renewed, decoded_renewed);
    assert!(
        decoded_renewed.status_at(ts(1_770_000_000)).is_err(),
        "a decoded successor must fail closed until its predecessor chain is verified"
    );
}

#[test]
fn retention_pin_lifecycle_is_immutable_fail_closed_and_monotonic() {
    let retained_until = ts(1_800_000_000);
    let active = active_pin();
    assert_eq!(
        RetentionStatus::Active,
        active.status_at(ts(1_750_000_000)).expect("status")
    );
    assert_eq!(
        RetentionStatus::Expired,
        active.status_at(ts(1_900_000_000)).expect("status")
    );

    assert!(active.renew(2, retained_until, ts(1_750_000_000)).is_err());
    assert!(
        active
            .renew(3, ts(1_900_000_000), ts(1_750_000_000))
            .is_err()
    );
    let renewed = active
        .renew(2, ts(1_900_000_000), ts(1_750_000_000))
        .expect("extend active pin");
    assert_eq!(2, renewed.revision());
    assert_eq!(1, active.revision());

    let expired_renewal = active.renew(2, ts(2_000_000_000), ts(1_900_000_000));
    assert!(expired_renewal.is_err());
    let scheduled_release_at = ts(1_780_000_000);
    let released = renewed
        .release(3, scheduled_release_at)
        .expect("schedule release while predecessor is active");
    assert_eq!(
        RetentionStatus::Active,
        released
            .status_at(ts(1_770_000_000))
            .expect("future release still protects")
    );
    assert_eq!(
        RetentionStatus::Released,
        released.status_at(scheduled_release_at).expect("status")
    );
    assert!(
        released
            .renew(4, ts(2_000_000_000), ts(1_770_000_000))
            .is_err()
    );
    assert_eq!(
        released,
        released
            .release(4, ts(1_780_000_000))
            .expect("idempotent release")
    );

    let latest = RetentionPinLatest::new(
        PIN_ID,
        3,
        "retention/pins/pin_01ARZ3NDEKTSV4RRFFQ69G5FAY/revisions/3.json",
        MANIFEST_DIGEST,
    )
    .expect("latest selector");
    assert_eq!(3, latest.revision());

    let mut malformed: Value = serde_json::to_value(&active).expect("pin json");
    malformed["pin_id"] = Value::String("bad".to_string());
    malformed["retained_until"] = Value::String("2000-01-01T00:00:00Z".to_string());
    let malformed: RetentionPinRevision = serde_json::from_value(malformed).expect("shape");
    let error = malformed
        .status_at(ts(1_900_000_000))
        .expect_err("structure must fail before expiry evaluation");
    assert!(error.to_string().contains("pin_id"));
}

#[test]
fn snapshot_catalog_projection_has_exact_safe_schema_and_deterministic_bytes() {
    let row = WorkspaceSnapshotCatalogRecord::new(
        SNAPSHOT_ID,
        1,
        ts(1_700_000_000),
        ts(1_800_000_000),
        RetentionStatus::Active,
        2,
        Some(PARENT_SNAPSHOT_ID.to_string()),
        true,
    )
    .expect("safe projection row");
    let schema = workspace_snapshot_schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            "snapshot_id",
            "record_version",
            "created_at",
            "retained_until",
            "retention_status",
            "domain_count",
            "parent_snapshot_id",
            "has_legacy_compatibility",
        ]
    );
    let first = write_workspace_snapshots(std::slice::from_ref(&row)).expect("write projection");
    let second = write_workspace_snapshots(&[row]).expect("write projection again");
    assert_eq!(first, second);
    for forbidden in [
        "authority",
        "checkpoint",
        "creator",
        "checksum",
        "relative_path",
        "archive",
        "relocation",
    ] {
        assert!(!fields.contains(&forbidden));
    }
    assert!(
        WorkspaceSnapshotCatalogRecord::new(
            SNAPSHOT_ID,
            1,
            ts(1_700_000_000),
            ts(1_800_000_000),
            RetentionStatus::Active,
            usize::MAX,
            None,
            false,
        )
        .is_err()
    );
}
