//! Legacy deletion schedules use the same backend-owned remote tasks as control GC.
use super::*;
use arco_catalog::Tier1Writer;
use arco_catalog::gc::{GarbageCollector, RetentionPolicy};
use arco_catalog::manifest::{
    CatalogDomainManifest, DomainManifestPointer, SnapshotFile, SnapshotInfo, format_manifest_id,
};
use arco_catalog::reconciler::{Reconciler, RepairScope};
use arco_catalog::workspace_snapshot::{
    LegacyCompatibilityArtifact, RequiredObject, RequiredObjectKind,
};
use arco_catalog::workspace_snapshot_service::RestoreSource;
use arco_core::{CatalogDomain, CatalogPaths};

#[derive(Debug)]
struct LegacyProjection(ProjectionWatermarkCut);
#[async_trait]
impl ProjectionWatermarkProvider for LegacyProjection {
    async fn capture(&self, _: &DomainAuthorityReference) -> Result<ProjectionWatermarkCut> {
        Ok(self.0.clone())
    }
}

async fn write_json(storage: &ScopedStorage, path: &str, value: &(impl serde::Serialize + Sync)) {
    storage
        .put_raw(
            path,
            Bytes::from(serde_json::to_vec(value).unwrap()),
            WritePrecondition::None,
        )
        .await
        .unwrap();
}

async fn legacy_fixture(retain_candidate: bool) -> (Fixture, String, Bytes) {
    let mut f = Fixture::new().await;
    Tier1Writer::new(f.storage.clone())
        .initialize()
        .await
        .unwrap();
    let current = Bytes::from_static(b"current legacy snapshot");
    let directory = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2);
    let mut snapshot = SnapshotInfo::new(2, directory.clone());
    snapshot.add_file(SnapshotFile {
        path: "current.parquet".to_owned(),
        checksum_sha256: hex::encode(Sha256::digest(&current)),
        byte_size: current.len() as u64,
        row_count: 0,
        position_range: None,
    });
    let path =
        CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &format_manifest_id(2));
    let manifest = CatalogDomainManifest {
        manifest_id: format_manifest_id(2),
        epoch: 2,
        previous_manifest_path: Some(CatalogPaths::domain_manifest_snapshot(
            CatalogDomain::Catalog,
            &format_manifest_id(1),
        )),
        writer_session_id: Some("legacy-schedule".to_owned()),
        snapshot_version: 2,
        snapshot_path: directory,
        snapshot: Some(snapshot),
        watermark_event_id: None,
        last_commit_id: None,
        fencing_token: Some(2),
        commit_ulid: None,
        parent_hash: None,
        updated_at: f.start,
    };
    write_json(&f.storage, &path, &manifest).await;
    write_json(
        &f.storage,
        &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
        &DomainManifestPointer {
            manifest_id: format_manifest_id(2),
            manifest_path: path,
            epoch: 2,
            parent_pointer_hash: None,
            updated_at: f.start,
        },
    )
    .await;
    f.storage
        .put_raw(
            &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 2, "current.parquet"),
            current,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let candidate = CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "compatibility.bin");
    let bytes = Bytes::from_static(b"historical compatibility bytes");
    // Use the backend clock to make this old-v1 file genuinely clear age guards.
    *f.backend.now.lock().unwrap() = f.start - chrono::Duration::days(8);
    f.storage
        .put_raw(&candidate, bytes.clone(), WritePrecondition::DoesNotExist)
        .await
        .unwrap();
    *f.backend.now.lock().unwrap() = f.start;
    if retain_candidate {
        require_compatibility_object(&mut f, &candidate, &bytes);
    }
    (f, candidate, bytes)
}

fn require_compatibility_object(f: &mut Fixture, candidate: &str, bytes: &Bytes) {
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(bytes)));
    let projection = ProjectionWatermarkCut::new(
        Vec::new(),
        vec![
            RequiredObject::new(
                candidate,
                bytes.len() as u64,
                RequiredObjectKind::LegacyCompatibility,
                &digest,
            )
            .unwrap(),
        ],
        vec![LegacyCompatibilityArtifact::new(candidate, digest).unwrap()],
    )
    .unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").unwrap(),
        vec![
            WorkspaceDomainBinding::new(
                scope,
                f.store.clone(),
                f.store.clone(),
                Arc::new(LegacyProjection(projection)),
                Arc::new(EmptyProviders),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let clock = f.backend.now.clone();
    f.service = Arc::new(
        WorkspaceSnapshotService::new(f.storage.clone(), registry)
            .unwrap()
            .with_clock(Arc::new(move || *clock.lock().unwrap())),
    );
}

#[derive(Clone, Copy, Debug)]
enum LegacyCollector {
    Repair,
    Gc,
}
impl LegacyCollector {
    async fn collect(self, storage: ScopedStorage) -> Result<()> {
        // Keep the current version; only old-v1 is eligible. Its timestamp is
        // eight days old, so both real collectors clear their minimum-age guard.
        let policy = RetentionPolicy::new(1, 24, 24, 30);
        match self {
            Self::Repair => {
                let repair = Reconciler::new(storage).with_retention_policy(&policy);
                let report = repair.check(CatalogDomain::Catalog).await?;
                repair
                    .repair_with_scope(&report, RepairScope::Full)
                    .await
                    .map(|_| ())
            }
            Self::Gc => GarbageCollector::new(storage, policy)
                .collect()
                .await
                .map(|_| ()),
        }
    }
}

async fn assert_readable(f: &Fixture, export: bool, path: &str, bytes: &Bytes) {
    assert_eq!(&f.storage.get_raw(path).await.unwrap(), bytes);
    let source = if export {
        RestoreSource::export(EXP, EPIN).unwrap()
    } else {
        RestoreSource::snapshot(SNAP, PIN).unwrap()
    };
    assert!(
        f.service
            .preflight_restore(
                &source,
                &WorkspaceScope::new("tenant", "workspace").unwrap(),
                f.start
            )
            .await
            .unwrap()
            .is_ready()
    );
}

#[tokio::test]
async fn legacy_delete_uncertainty_blocks_publication_even_after_stale_recovery_attempts() {
    for collector in [LegacyCollector::Repair, LegacyCollector::Gc] {
        for class in [
            Class::Snapshot,
            Class::SnapshotRetry,
            Class::Export,
            Class::ExportRetry,
        ] {
            for fault in [Fault::DelayedError, Fault::PauseBefore, Fault::PauseAfter] {
                // Snapshot finalization/retry would retain the retiring object.
                // Exports use an already valid independent source: an active
                // source containing the candidate would itself exclude deletion.
                let (f, candidate, bytes) = legacy_fixture(!class.export()).await;
                let original = class.setup(&f).await;
                let schedule = f.backend.arm(candidate.clone(), 0, fault);
                let storage = f.storage.clone();
                let task = tokio::spawn(async move { collector.collect(storage).await });
                schedule.issued().await;
                if matches!(fault, Fault::PauseAfter) {
                    guard(schedule.applied.notified()).await;
                }
                if matches!(fault, Fault::DelayedError) {
                    assert!(
                        guard(task).await.unwrap().is_err(),
                        "{collector:?}: unresolved DELETE must prevent settlement"
                    );
                } else {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                }
                f.backend.expire_lease(&f.storage).await;
                f.age_epoch().await;
                let before = f
                    .storage
                    .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .unwrap();
                assert_eq!(f.epoch().await, "IN_FLIGHT");
                // Retry each reclamation entry point after the 600-second
                // adoption threshold. No caller may reinterpret a legacy epoch
                // as the generation-fenced control protocol.
                let restart = collector.collect(f.storage.clone()).await;
                // Repair may return a read-only no-op after the sole candidate
                // has already disappeared. It must still leave exclusion intact.
                if !matches!(
                    (collector, fault),
                    (LegacyCollector::Repair, Fault::PauseAfter)
                ) {
                    assert!(
                        restart.is_err(),
                        "{collector:?} {class:?} {fault:?}: restart must be excluded"
                    );
                }
                assert!(f.worker.collect_gc_at(f.start, Vec::new()).await.is_err());
                assert!(
                    class
                        .publish(&f.service, &f.snapshot(), &f.export())
                        .await
                        .is_err()
                );
                assert_eq!(
                    f.storage
                        .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                        .await
                        .unwrap(),
                    before
                );
                let pin = if class.export() { EPIN } else { PIN };
                assert!(
                    f.storage
                        .head_raw(&retention_pin_latest_path(pin).unwrap())
                        .await
                        .unwrap()
                        .is_none()
                );
                if !matches!(fault, Fault::PauseAfter) {
                    assert_eq!(f.storage.get_raw(&candidate).await.unwrap(), bytes);
                }
                // Every remote mutation completes before operator recovery.
                schedule.finish().await;
                assert!(f.storage.head_raw(&candidate).await.unwrap().is_none());
                recover_stale_retention_epoch(
                    &f.storage,
                    "all legacy DELETE requests completed; affected closure reconciled",
                )
                .await
                .unwrap();
                let result = class.publish(&f.service, &f.snapshot(), &f.export()).await;
                assert_eq!(
                    result.is_ok(),
                    class.export(),
                    "missing legacy bytes cannot be revived after recovery"
                );
                if let Some(original) = original {
                    let path = if class.export() {
                        export_record_path(EXP).unwrap()
                    } else {
                        snapshot_record_path(SNAP).unwrap()
                    };
                    assert_eq!(f.storage.get_raw(&path).await.unwrap(), original);
                }
                if class.export() {
                    f.assert_protected(true).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn legacy_collectors_preserve_acknowledged_snapshot_and_export_closures() {
    for collector in [LegacyCollector::Repair, LegacyCollector::Gc] {
        let (f, candidate, bytes) = legacy_fixture(true).await;
        f.service.create_snapshot(&f.snapshot()).await.unwrap();
        f.service.export_snapshot(&f.export()).await.unwrap();
        collector.collect(f.storage.clone()).await.unwrap();
        assert_readable(&f, false, &candidate, &bytes).await;
        assert_readable(&f, true, &candidate, &bytes).await;
        release_pin(&f, PIN, f.start).await;
        collector.collect(f.storage.clone()).await.unwrap();
        assert_readable(&f, true, &candidate, &bytes).await;
        release_pin(&f, EPIN, f.start).await;
        collector.collect(f.storage.clone()).await.unwrap();
        assert!(f.storage.head_raw(&candidate).await.unwrap().is_none());
    }
}

// Seed records accepted by the older publisher, preserving their immutable pin
// identities. Current publication must refuse to create or retry such roots,
// while GC must still protect their complete existing closures.
async fn seed_old_required_object(f: &Fixture, record: &str, candidate: &str, bytes: &Bytes) {
    let mut value: serde_json::Value =
        serde_json::from_slice(&f.storage.get_raw(record).await.unwrap()).unwrap();
    let object = RequiredObject::new(
        candidate,
        bytes.len() as u64,
        RequiredObjectKind::Other,
        format!("sha256:{}", hex::encode(Sha256::digest(bytes))),
    )
    .unwrap();
    let objects = value
        .get_mut("required_objects")
        .unwrap()
        .as_array_mut()
        .unwrap();
    objects.push(serde_json::to_value(object).unwrap());
    for object in objects.iter_mut() {
        if object["kind"] == "snapshot_record" {
            let source = f
                .storage
                .get_raw(object["relative_path"].as_str().unwrap())
                .await
                .unwrap();
            object["byte_size"] = serde_json::to_value(source.len()).unwrap();
            object["sha256"] =
                serde_json::to_value(format!("sha256:{}", hex::encode(Sha256::digest(&source))))
                    .unwrap();
        }
    }
    objects.sort_by(|a, b| {
        a["relative_path"]
            .as_str()
            .cmp(&b["relative_path"].as_str())
    });
    write_json(&f.storage, record, &value).await;
}

async fn control_candidate(f: &Fixture) -> (String, Bytes) {
    let candidate = f.store.paths().tx_object("provider-orphan");
    let bytes = Bytes::from_static(b"provider required control object");
    *f.backend.now.lock().unwrap() = f.start - chrono::Duration::days(8);
    f.storage
        .put_raw(&candidate, bytes.clone(), WritePrecondition::DoesNotExist)
        .await
        .unwrap();
    *f.backend.now.lock().unwrap() = f.start;
    (candidate, bytes)
}

#[tokio::test]
async fn control_provider_objects_in_existing_roots_survive_gc_until_release() {
    let f = Fixture::new().await;
    let (candidate, bytes) = control_candidate(&f).await;
    f.service.create_snapshot(&f.snapshot()).await.unwrap();
    f.service.export_snapshot(&f.export()).await.unwrap();
    seed_old_required_object(&f, &snapshot_record_path(SNAP).unwrap(), &candidate, &bytes).await;
    seed_old_required_object(&f, &export_record_path(EXP).unwrap(), &candidate, &bytes).await;
    f.worker.collect_gc_at(f.start, Vec::new()).await.unwrap();
    assert_readable(&f, false, &candidate, &bytes).await;
    assert_readable(&f, true, &candidate, &bytes).await;
    release_pin(&f, PIN, f.start).await;
    f.worker.collect_gc_at(f.start, Vec::new()).await.unwrap();
    assert_readable(&f, true, &candidate, &bytes).await;
    release_pin(&f, EPIN, f.start).await;
    f.worker.collect_gc_at(f.start, Vec::new()).await.unwrap();
    assert!(f.storage.head_raw(&candidate).await.unwrap().is_none());
}

#[tokio::test]
async fn control_provider_objects_cannot_be_newly_published_or_retried() {
    for class in [
        Class::Snapshot,
        Class::SnapshotRetry,
        Class::Export,
        Class::ExportRetry,
    ] {
        let mut f = Fixture::new().await;
        let (candidate, bytes) = control_candidate(&f).await;
        class.setup(&f).await;
        if !matches!(class, Class::Snapshot) {
            seed_old_required_object(&f, &snapshot_record_path(SNAP).unwrap(), &candidate, &bytes)
                .await;
        }
        if matches!(class, Class::ExportRetry) {
            seed_old_required_object(&f, &export_record_path(EXP).unwrap(), &candidate, &bytes)
                .await;
        }
        require_compatibility_object(&mut f, &candidate, &bytes);
        let error = class
            .publish(&f.service, &f.snapshot(), &f.export())
            .await
            .expect_err("provider cannot bypass control generation protection");
        assert!(
            error
                .to_string()
                .contains("control required object is outside the declared authority references"),
            "{class:?}: {error}"
        );
        let pin = if class.export() { EPIN } else { PIN };
        assert!(
            f.storage
                .head_raw(&retention_pin_latest_path(pin).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn control_provider_cannot_revive_a_pending_delete_after_epoch_settlement() {
    let mut f = Fixture::new().await;
    let (candidate, bytes) = control_candidate(&f).await;
    let schedule = f.backend.arm(candidate.clone(), 0, Fault::DelayedError);
    f.worker
        .collect_gc_at(f.start, Vec::new())
        .await
        .expect_err("transport response is lost but the fenced epoch may settle");
    schedule.issued().await;
    assert_eq!(f.epoch().await, "IDLE");
    assert_eq!(f.storage.get_raw(&candidate).await.unwrap(), bytes);
    require_compatibility_object(&mut f, &candidate, &bytes);
    let error = f
        .service
        .create_snapshot(&f.snapshot())
        .await
        .expect_err("visible deletion-authorized bytes cannot become a retained root");
    assert!(
        error
            .to_string()
            .contains("control required object is outside the declared authority references"),
        "{error}"
    );
    schedule.finish().await;
    assert!(f.storage.head_raw(&candidate).await.unwrap().is_none());
    assert!(
        f.storage
            .head_raw(&retention_pin_latest_path(PIN).unwrap())
            .await
            .unwrap()
            .is_none()
    );
}
