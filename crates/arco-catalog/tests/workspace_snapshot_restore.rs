//! Roll-forward workspace restore contracts.

// Test-target lint scope (#331): tests and their helpers signal failure by
// panicking. clippy.toml scopes the restriction lints out of #[test] fns;
// this header extends the same policy to this file's shared helpers.
#![allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing)]
// Advisory lint scope for test code (#331): the pedantic/nursery lints below
// conflict with test ergonomics here; production code keeps them active.
#![allow(clippy::needless_pass_by_value, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arco_catalog::workspace_restore::{
    OmittedDomainPolicy, RestoreDomainToSnapshot, RestoreOperationTarget,
    RestoreWorkspaceToSnapshot, WorkspaceRestoreRequestRecord, WorkspaceRestoreService,
    WorkspaceRestoreStatus, decode_workspace_restore_request, encode_workspace_restore_request,
    restore_attempt_plan_path, restore_journal_path, restore_read_manifest_path,
    restore_request_path,
};
use arco_catalog::workspace_snapshot::WorkspaceScope;
use arco_catalog::workspace_snapshot::{
    LegacyCompatibilityArtifact, RequiredObject, RequiredObjectKind, RetentionPinLatest,
    WorkspaceSnapshot, decode_retention_pin_revision, decode_workspace_snapshot,
    encode_retention_pin_latest, encode_retention_pin_revision, encode_workspace_snapshot,
    export_record_path, retention_pin_latest_path, retention_pin_revision_path,
    snapshot_record_path,
};
use arco_catalog::workspace_snapshot_service::{
    CreateWorkspaceExportRequest, CreateWorkspaceSnapshotRequest, EventArchiveCapture,
    EventArchiveProvider, ProjectionWatermarkCut, ProjectionWatermarkProvider, RestoreSource,
    WorkspaceDomainBinding, WorkspaceDomainRegistry, WorkspaceSnapshotService,
};
use arco_catalog::{
    ArcoStateAdmin, ArcoStateTxn, CatalogError, CheckpointOptions, ControlMvpRestoreParticipant,
    ControlMvpStateStore, CurrentStateStore, PersistedAuthorityAdapter,
    PersistedRestoreParticipantPlan, RestoreAttemptIdentity, StateRestoreParticipant, StateScope,
    StateStoreBindingIdentity, TxnOptions,
};
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Duration as ChronoDuration;
use chrono::{TimeZone, Utc};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Barrier, Notify};
use ulid::Ulid;

fn restore_id() -> String {
    format!("rst_{}", Ulid::from(1_u128))
}

fn snapshot_id() -> String {
    format!("snap_{}", Ulid::from(2_u128))
}

fn pin_id() -> String {
    format!("pin_{}", Ulid::from(3_u128))
}

struct UnusedProjectionProvider;

#[async_trait]
impl ProjectionWatermarkProvider for UnusedProjectionProvider {
    async fn capture(
        &self,
        _authority: &arco_catalog::workspace_snapshot::DomainAuthorityReference,
    ) -> arco_catalog::Result<ProjectionWatermarkCut> {
        ProjectionWatermarkCut::new(Vec::new(), Vec::new(), Vec::new())
    }
}

struct UnusedArchiveProvider;

#[async_trait]
impl EventArchiveProvider for UnusedArchiveProvider {
    async fn capture(
        &self,
        _authority: &arco_catalog::workspace_snapshot::DomainAuthorityReference,
    ) -> arco_catalog::Result<EventArchiveCapture> {
        EventArchiveCapture::new(
            arco_catalog::workspace_snapshot::DomainEventArchive::empty(_authority.domain())?,
            Vec::new(),
        )
    }
}

#[test]
fn restore_record_contracts_use_canonical_exact_paths_and_round_trip() {
    let restore_id = restore_id();
    assert_eq!(
        format!("transactions/restores/{restore_id}/request.json"),
        restore_request_path(&restore_id).expect("request path")
    );
    assert_eq!(
        format!("transactions/restores/{restore_id}/attempts/00000000000000000001.plan.json"),
        restore_attempt_plan_path(&restore_id, 1).expect("attempt path")
    );
    assert_eq!(
        format!("transactions/restores/{restore_id}/journal.json"),
        restore_journal_path(&restore_id).expect("journal path")
    );
    assert_eq!(
        format!("transactions/restores/{restore_id}/read.manifest.json"),
        restore_read_manifest_path(&restore_id).expect("read manifest path")
    );
    assert!(restore_request_path("rst_not-a-ulid").is_err());
    assert!(restore_attempt_plan_path(&restore_id, 0).is_err());

    let request = WorkspaceRestoreRequestRecord::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id(), pin_id()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        Utc.with_ymd_and_hms(2026, 7, 15, 12, 0, 0)
            .single()
            .expect("time"),
        RestoreOperationTarget::workspace(OmittedDomainPolicy::Reject),
    )
    .expect("request");
    let bytes = encode_workspace_restore_request(&request).expect("encode");
    let decoded = decode_workspace_restore_request(&bytes).expect("decode");
    assert_eq!(request, decoded);
    let json = String::from_utf8(bytes).expect("utf8");
    assert!(json.contains("workspace_restore_request"));
    assert!(!json.contains("StateToken"));
    assert!(!json.contains("CheckpointToken"));
}

#[test]
fn restore_record_contracts_reject_unsupported_versions_and_implicit_policy() {
    let request = WorkspaceRestoreRequestRecord::new(
        restore_id(),
        RestoreSource::snapshot(snapshot_id(), pin_id()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        Utc.with_ymd_and_hms(2026, 7, 15, 12, 0, 0)
            .single()
            .expect("time"),
        RestoreOperationTarget::workspace(OmittedDomainPolicy::Omit),
    )
    .expect("request");
    let mut value: serde_json::Value =
        serde_json::from_slice(&encode_workspace_restore_request(&request).expect("encode"))
            .expect("json");
    value["version"] = serde_json::Value::from(2_u64);
    assert!(decode_workspace_restore_request(&serde_json::to_vec(&value).expect("json")).is_err());
    value["version"] = serde_json::Value::from(1_u64);
    value["target"]["omitted_domain_policy"] = serde_json::Value::Null;
    assert!(decode_workspace_restore_request(&serde_json::to_vec(&value).expect("json")).is_err());

    let domain_request = WorkspaceRestoreRequestRecord::new(
        restore_id(),
        RestoreSource::snapshot(snapshot_id(), pin_id()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        Utc.with_ymd_and_hms(2026, 7, 15, 12, 0, 0)
            .single()
            .expect("time"),
        RestoreOperationTarget::domain("catalog"),
    )
    .expect("domain request");
    for unsafe_domain in [".", "..", "../other", "a/b", r"a\b"] {
        let mut value: serde_json::Value = serde_json::from_slice(
            &encode_workspace_restore_request(&domain_request).expect("domain request bytes"),
        )
        .expect("domain request json");
        value["target"]["domain"] = serde_json::Value::String(unsafe_domain.to_string());
        assert!(
            decode_workspace_restore_request(&serde_json::to_vec(&value).expect("json")).is_err(),
            "unsafe target domain {unsafe_domain:?} must fail during record decoding"
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum PersistedRestoreRecordKind {
    Attempt,
    Journal,
    ReadManifest,
}

#[derive(Debug, Clone, Copy)]
enum PersistedRestoreCorruption {
    UnsupportedRecordType,
    UnsupportedVersion,
    UnknownStatus,
    DuplicateArray(&'static str),
    ReverseArray(&'static str),
}

struct PersistedRestoreFixture {
    inner: Arc<MemoryBackend>,
    audit: Arc<RestoreAuditBackend>,
    storage: ScopedStorage,
    service: WorkspaceRestoreService,
    restore_id: String,
}

impl PersistedRestoreFixture {
    fn record_path(&self, record: PersistedRestoreRecordKind) -> String {
        match record {
            PersistedRestoreRecordKind::Attempt => {
                restore_attempt_plan_path(&self.restore_id, 1).expect("attempt path")
            }
            PersistedRestoreRecordKind::Journal => {
                restore_journal_path(&self.restore_id).expect("journal path")
            }
            PersistedRestoreRecordKind::ReadManifest => {
                restore_read_manifest_path(&self.restore_id).expect("read manifest path")
            }
        }
    }
}

fn corrupt_persisted_restore_record(
    value: &mut serde_json::Value,
    corruption: PersistedRestoreCorruption,
) {
    match corruption {
        PersistedRestoreCorruption::UnsupportedRecordType => {
            value["record_type"] = serde_json::Value::String("unsupported_restore_record".into());
        }
        PersistedRestoreCorruption::UnsupportedVersion => {
            value["version"] = serde_json::Value::from(2_u64);
        }
        PersistedRestoreCorruption::UnknownStatus => {
            value["status"] = serde_json::Value::String("UNKNOWN_LIFECYCLE".to_string());
        }
        PersistedRestoreCorruption::DuplicateArray(field) => {
            let values = value[field]
                .as_array_mut()
                .unwrap_or_else(|| panic!("{field} must be an array"));
            assert!(values.len() >= 2, "{field} needs two fixture values");
            values.insert(1, values[0].clone());
        }
        PersistedRestoreCorruption::ReverseArray(field) => {
            let values = value[field]
                .as_array_mut()
                .unwrap_or_else(|| panic!("{field} must be an array"));
            assert!(values.len() >= 2, "{field} needs two fixture values");
            values.reverse();
        }
    }
}

async fn persisted_restore_fixture() -> PersistedRestoreFixture {
    let inner = Arc::new(MemoryBackend::new());
    let audit = Arc::new(RestoreAuditBackend::new(inner.clone()));
    let storage = ScopedStorage::new(audit.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b", "c", "d"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores[..2] {
        committed_value(store, b"v1").await;
    }

    let now = Utc::now();
    let snapshot_id = snapshot_id();
    let pin_id = pin_id();
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores[..2], &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores[..2] {
        committed_value(store, b"v2").await;
    }

    let restore_id = restore_id();
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b", "c", "d"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Omit,
    )
    .expect("restore request");
    let outcome = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("restore");
    assert_eq!(WorkspaceRestoreStatus::Visible, outcome.status());
    assert_eq!(
        &["c".to_string(), "d".to_string()],
        outcome.omitted_domains()
    );

    PersistedRestoreFixture {
        inner,
        audit,
        storage,
        service,
        restore_id,
    }
}

#[tokio::test]
async fn restore_record_contracts_reject_corrupt_persisted_records_before_mutation() {
    let mut cases = vec![
        (
            PersistedRestoreRecordKind::Attempt,
            PersistedRestoreCorruption::UnsupportedRecordType,
        ),
        (
            PersistedRestoreRecordKind::Attempt,
            PersistedRestoreCorruption::UnsupportedVersion,
        ),
        (
            PersistedRestoreRecordKind::Journal,
            PersistedRestoreCorruption::UnsupportedRecordType,
        ),
        (
            PersistedRestoreRecordKind::Journal,
            PersistedRestoreCorruption::UnsupportedVersion,
        ),
        (
            PersistedRestoreRecordKind::Journal,
            PersistedRestoreCorruption::UnknownStatus,
        ),
        (
            PersistedRestoreRecordKind::ReadManifest,
            PersistedRestoreCorruption::UnsupportedRecordType,
        ),
        (
            PersistedRestoreRecordKind::ReadManifest,
            PersistedRestoreCorruption::UnsupportedVersion,
        ),
    ];
    for (record, field) in [
        (PersistedRestoreRecordKind::Attempt, "participants"),
        (PersistedRestoreRecordKind::Attempt, "omitted_domains"),
        (PersistedRestoreRecordKind::Journal, "required_domains"),
        (PersistedRestoreRecordKind::Journal, "participants"),
        (PersistedRestoreRecordKind::Journal, "omitted_domains"),
        (PersistedRestoreRecordKind::ReadManifest, "participants"),
        (PersistedRestoreRecordKind::ReadManifest, "omitted_domains"),
    ] {
        cases.push((record, PersistedRestoreCorruption::DuplicateArray(field)));
        cases.push((record, PersistedRestoreCorruption::ReverseArray(field)));
    }

    let fixture = persisted_restore_fixture().await;
    let record_paths = [
        fixture.record_path(PersistedRestoreRecordKind::Attempt),
        fixture.record_path(PersistedRestoreRecordKind::Journal),
        fixture.record_path(PersistedRestoreRecordKind::ReadManifest),
    ];
    let mut pristine = BTreeMap::new();
    for path in &record_paths {
        pristine.insert(
            path.clone(),
            fixture
                .storage
                .get_raw(path)
                .await
                .expect("pristine persisted record"),
        );
    }

    for (record, corruption) in cases {
        for (path, bytes) in &pristine {
            fixture
                .storage
                .put_raw(path, bytes.clone(), WritePrecondition::None)
                .await
                .expect("reset pristine persisted record");
        }
        let path = fixture.record_path(record);
        let mut value: serde_json::Value = serde_json::from_slice(
            &fixture
                .storage
                .get_raw(&path)
                .await
                .expect("persisted record"),
        )
        .expect("persisted record JSON");
        corrupt_persisted_restore_record(&mut value, corruption);
        let corrupt_bytes = Bytes::from(
            serde_jcs::to_vec(&value).expect("corrupt canonical persisted record bytes"),
        );
        fixture
            .storage
            .put_raw(&path, corrupt_bytes.clone(), WritePrecondition::None)
            .await
            .expect("write test corruption");
        if matches!(
            record,
            PersistedRestoreRecordKind::Attempt | PersistedRestoreRecordKind::ReadManifest
        ) {
            let journal_path = fixture.record_path(PersistedRestoreRecordKind::Journal);
            let mut journal: serde_json::Value = serde_json::from_slice(
                &fixture
                    .storage
                    .get_raw(&journal_path)
                    .await
                    .expect("pristine journal"),
            )
            .expect("pristine journal JSON");
            let digest = format!("sha256:{}", hex::encode(Sha256::digest(&corrupt_bytes)));
            match record {
                PersistedRestoreRecordKind::Attempt => {
                    journal["attempt_sha256"] = serde_json::Value::String(digest);
                }
                PersistedRestoreRecordKind::ReadManifest => {
                    journal["read_manifest_sha256"] = serde_json::Value::String(digest);
                }
                PersistedRestoreRecordKind::Journal => unreachable!("journal has no outer digest"),
            }
            fixture
                .storage
                .put_raw(
                    &journal_path,
                    Bytes::from(serde_jcs::to_vec(&journal).expect("rebound journal bytes")),
                    WritePrecondition::None,
                )
                .await
                .expect("bind journal to test corruption");
        }
        let before = all_workspace_bytes(fixture.inner.as_ref()).await;
        fixture.audit.clear();
        fixture.audit.deny_lists();

        assert!(
            fixture
                .service
                .get_restore(&fixture.restore_id)
                .await
                .is_err(),
            "get_restore accepted {record:?} {corruption:?}"
        );
        assert!(
            fixture
                .service
                .recover_restore(&fixture.restore_id)
                .await
                .is_err(),
            "recover_restore accepted {record:?} {corruption:?}"
        );
        let operations = fixture.audit.operations();
        assert!(
            operations.iter().all(|operation| !matches!(
                operation,
                AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
            )),
            "{record:?} {corruption:?} caused a forbidden mutation or list: {operations:?}"
        );
        assert_eq!(
            before,
            all_workspace_bytes(fixture.inner.as_ref()).await,
            "{record:?} {corruption:?} changed persisted bytes after corruption setup"
        );
    }
}

#[test]
fn restore_adapter_configuration_is_explicit_and_scope_checked() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = Arc::new(ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store"));
    let binding = WorkspaceDomainBinding::new(
        scope,
        store.clone(),
        store,
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("binding");
    assert!(!binding.restore_configured());

    let other_scope = StateScope::new("tenant", "workspace", "other");
    let other = ControlMvpStateStore::new(storage, other_scope).expect("other store");
    assert!(
        binding
            .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(other)))
            .is_err()
    );
}

#[test]
fn restore_adapter_configuration_rejects_different_backend_with_identical_scope() {
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let first_storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
        .expect("first storage");
    let second_storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
        .expect("second storage");
    let first =
        Arc::new(ControlMvpStateStore::new(first_storage, scope.clone()).expect("first store"));
    let second = ControlMvpStateStore::new(second_storage, scope.clone()).expect("second store");
    let binding = WorkspaceDomainBinding::new(
        scope,
        first.clone(),
        first,
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("binding");

    assert!(
        binding
            .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(second)))
            .is_err(),
        "same implementation and scope must not authorize a restore participant backed by different storage"
    );
}

#[tokio::test]
async fn journal_revision_overflow_is_detected_before_participant_apply() {
    let memory = Arc::new(MemoryBackend::new());
    let audit = Arc::new(RestoreAuditBackend::new(memory));
    let backend = Arc::new(FailNextRestoreLockBackend::new(audit.clone()));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(260_u128));
    let pin_id = format!("pin_{}", Ulid::from(261_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let restore_id = format!("rst_{}", Ulid::from(262_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "lock acquisition failure must leave an APPLYING journal before participant mutation"
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        arco_catalog::ArcoStateReader::get(store.as_ref(), b"catalog/default")
            .await
            .expect("current value")
    );

    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let mut journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&journal_path)
            .await
            .expect("applying journal"),
    )
    .expect("journal JSON");
    assert_eq!("APPLYING", journal["status"]);
    journal["revision"] = serde_json::Value::from(u64::MAX);
    storage
        .put_raw(
            &journal_path,
            Bytes::from(serde_jcs::to_vec(&journal).expect("max revision journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("seed max journal revision");
    audit.clear();

    assert!(
        service.recover_restore(&restore_id).await.is_err(),
        "revision exhaustion must fail recovery"
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        arco_catalog::ArcoStateReader::get(store.as_ref(), b"catalog/default")
            .await
            .expect("authority after rejected recovery"),
        "revision exhaustion must be detected before participant apply"
    );
    assert!(
        audit.operations().iter().all(|operation| {
            !matches!(operation, AuditOperation::Put { path, .. }
                if path.contains("/state-store/control-mvp/catalog/"))
        }),
        "revision exhaustion must not write participant authority artifacts"
    );
}

#[tokio::test]
async fn initial_preflight_adopts_concurrent_visible_journal_after_source_release() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(263_u128));
    let pin_id = format!("pin_{}", Ulid::from(264_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let signals = Arc::new(PauseInitialRestorePlanSignals::default());
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let paused_binding = WorkspaceDomainBinding::new(
        scope,
        store.clone(),
        store.clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("binding")
    .with_restore_participant(Arc::new(PauseInitialRestorePlanParticipant {
        inner: ControlMvpRestoreParticipant::new(store.as_ref().clone()),
        signals: signals.clone(),
        pause_once: AtomicBool::new(true),
    }))
    .expect("paused restore participant");
    let paused_service = Arc::new(
        WorkspaceRestoreService::new(
            storage.clone(),
            WorkspaceDomainRegistry::new(
                WorkspaceScope::new("tenant", "workspace").expect("scope"),
                vec![paused_binding],
            )
            .expect("paused registry"),
        )
        .expect("paused restore service"),
    );
    let winner_service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store, true))
            .expect("winner restore service");
    let restore_id = format!("rst_{}", Ulid::from(265_u128));
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");

    let paused_request = request.clone();
    let paused_worker = {
        let paused_service = paused_service.clone();
        tokio::spawn(async move {
            paused_service
                .restore_workspace_to_snapshot(&paused_request)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), signals.reached.notified())
        .await
        .expect("initial invocation reached participant planning");
    assert!(
        storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .is_err(),
        "paused initial invocation must not have published a journal"
    );

    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        winner_service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("concurrent identical winner")
            .status()
    );
    release_retention_pin(&storage, &pin_id).await;
    signals.resume.notify_one();

    let adopted = tokio::time::timeout(Duration::from_secs(5), paused_worker)
        .await
        .expect("paused invocation completed")
        .expect("paused task joined")
        .expect("paused invocation adopts identical visible journal");
    assert_eq!(WorkspaceRestoreStatus::Visible, adopted.status());
}

fn domain_registry(store: Arc<ControlMvpStateStore>, restore: bool) -> WorkspaceDomainRegistry {
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let mut binding = WorkspaceDomainBinding::new(
        scope,
        store.clone(),
        store.clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("binding");
    if restore {
        binding = binding
            .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
                store.as_ref().clone(),
            )))
            .expect("restore participant");
    }
    WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("workspace scope"),
        vec![binding],
    )
    .expect("registry")
}

async fn committed_value(store: &ControlMvpStateStore, value: &'static [u8]) {
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin");
    txn.put(b"catalog/default", Bytes::from_static(value))
        .await
        .expect("put");
    txn.commit().await.expect("commit");
}

async fn release_retention_pin(storage: &ScopedStorage, pin_id: &str) {
    let revision_one_path = retention_pin_revision_path(pin_id, 1).expect("revision one path");
    let revision_one = decode_retention_pin_revision(
        &storage
            .get_raw(&revision_one_path)
            .await
            .expect("revision one"),
    )
    .expect("decode revision one");
    let release = revision_one
        .release(2, Utc::now())
        .expect("release active pin");
    let release_bytes = encode_retention_pin_revision(&release).expect("encode release");
    let revision_two_path = retention_pin_revision_path(pin_id, 2).expect("revision two path");
    storage
        .put_raw(
            &revision_two_path,
            Bytes::from(release_bytes.clone()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write release revision");
    let latest = RetentionPinLatest::new(
        pin_id,
        2,
        &revision_two_path,
        format!("sha256:{}", hex::encode(Sha256::digest(&release_bytes))),
    )
    .expect("release selector");
    storage
        .put_raw(
            &retention_pin_latest_path(pin_id).expect("latest path"),
            Bytes::from(encode_retention_pin_latest(&latest).expect("encode selector")),
            WritePrecondition::None,
        )
        .await
        .expect("select release");
}

async fn retention_epoch(storage: &ScopedStorage) -> serde_json::Value {
    serde_json::from_slice(
        &storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("retention mutation epoch"),
    )
    .expect("retention mutation epoch JSON")
}

async fn restore_and_state_bytes(backend: &MemoryBackend) -> BTreeMap<String, Bytes> {
    let mut selected = BTreeMap::new();
    for object in backend.list("").await.expect("test inventory") {
        if object.path.contains("/transactions/restores/") || object.path.contains("/state-store/")
        {
            selected.insert(
                object.path.clone(),
                backend.get(&object.path).await.expect("test object bytes"),
            );
        }
    }
    selected
}

async fn all_workspace_bytes(backend: &MemoryBackend) -> BTreeMap<String, Bytes> {
    let mut selected = BTreeMap::new();
    for object in backend.list("").await.expect("test inventory") {
        selected.insert(
            object.path.clone(),
            backend.get(&object.path).await.expect("test object bytes"),
        );
    }
    selected
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditPrecondition {
    DoesNotExist,
    MatchesVersion,
    None,
}

impl From<&WritePrecondition> for AuditPrecondition {
    fn from(value: &WritePrecondition) -> Self {
        match value {
            WritePrecondition::DoesNotExist => Self::DoesNotExist,
            WritePrecondition::MatchesVersion(_) => Self::MatchesVersion,
            WritePrecondition::None => Self::None,
        }
    }
}

#[derive(Debug, Clone)]
enum AuditOperation {
    Get(String),
    Put {
        path: String,
        bytes: Bytes,
        precondition: AuditPrecondition,
    },
    Delete(String),
    List(String),
    Head(String),
}

struct RestoreAuditBackend {
    inner: Arc<dyn StorageBackend>,
    operations: Mutex<Vec<AuditOperation>>,
    deny_list: AtomicBool,
    observe_journal: Mutex<Option<String>>,
    journal_at_first_restore_txlog: Mutex<Option<Bytes>>,
    churn_journal: Mutex<Option<String>>,
    churn_remaining: AtomicUsize,
}

impl RestoreAuditBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            operations: Mutex::new(Vec::new()),
            deny_list: AtomicBool::new(false),
            observe_journal: Mutex::new(None),
            journal_at_first_restore_txlog: Mutex::new(None),
            churn_journal: Mutex::new(None),
            churn_remaining: AtomicUsize::new(0),
        }
    }

    fn clear(&self) {
        self.operations.lock().expect("audit operations").clear();
        *self
            .journal_at_first_restore_txlog
            .lock()
            .expect("journal observation") = None;
    }

    fn operations(&self) -> Vec<AuditOperation> {
        self.operations.lock().expect("audit operations").clone()
    }

    fn deny_lists(&self) {
        self.deny_list.store(true, Ordering::SeqCst);
    }

    fn observe_journal_before_restore_txlog(&self, restore_id: &str) {
        *self.observe_journal.lock().expect("observed journal") =
            Some(restore_journal_path(restore_id).expect("journal path"));
    }

    fn journal_at_first_restore_txlog(&self) -> Bytes {
        self.journal_at_first_restore_txlog
            .lock()
            .expect("journal observation")
            .clone()
            .expect("journal was readable before first restore txlog write")
    }

    fn churn_journal_heads(&self, restore_id: &str, count: usize) {
        *self.churn_journal.lock().expect("churn journal") =
            Some(restore_journal_path(restore_id).expect("journal path"));
        self.churn_remaining.store(count, Ordering::SeqCst);
    }

    fn record(&self, operation: AuditOperation) {
        self.operations
            .lock()
            .expect("audit operations")
            .push(operation);
    }
}

#[async_trait]
impl StorageBackend for RestoreAuditBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.record(AuditOperation::Get(path.to_string()));
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.record(AuditOperation::Get(path.to_string()));
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let observed = self
            .observe_journal
            .lock()
            .expect("observed journal")
            .clone();
        if path.contains("/state-store/control-mvp/")
            && path.contains("/txlog/tx-restore-")
            && self
                .journal_at_first_restore_txlog
                .lock()
                .expect("journal observation")
                .is_none()
            && let Some(relative) = observed
        {
            let prefix = path
                .split("/state-store/control-mvp/")
                .next()
                .expect("workspace prefix");
            let full_path = format!("{prefix}/{relative}");
            let bytes = self.inner.get(&full_path).await?;
            *self
                .journal_at_first_restore_txlog
                .lock()
                .expect("journal observation") = Some(bytes);
        }
        self.record(AuditOperation::Put {
            path: path.to_string(),
            bytes: data.clone(),
            precondition: AuditPrecondition::from(&precondition),
        });
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.record(AuditOperation::Delete(path.to_string()));
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.record(AuditOperation::List(prefix.to_string()));
        assert!(
            !self.deny_list.load(Ordering::SeqCst),
            "restore request/recovery path attempted forbidden list: {prefix}"
        );
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.record(AuditOperation::Head(path.to_string()));
        let churn = self.churn_journal.lock().expect("churn journal").clone();
        if churn
            .as_ref()
            .is_some_and(|relative| path.ends_with(relative))
            && self
                .churn_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            let bytes = self.inner.get(path).await?;
            self.inner.put(path, bytes, WritePrecondition::None).await?;
        }
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

struct FailNextRestoreLockBackend {
    inner: Arc<dyn StorageBackend>,
    fail_next_lock: AtomicBool,
}

impl FailNextRestoreLockBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_next_lock: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.fail_next_lock.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for FailNextRestoreLockBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if path.ends_with("locks/workspace-retention-gc.lock.json")
            && matches!(precondition, WritePrecondition::DoesNotExist)
            && self.fail_next_lock.swap(false, Ordering::SeqCst)
        {
            return Err(arco_core::Error::storage(
                "injected restore lock acquisition failure",
            ));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[derive(Default)]
struct PauseInitialRestorePlanSignals {
    reached: Notify,
    resume: Notify,
}

struct PauseInitialRestorePlanParticipant {
    inner: ControlMvpRestoreParticipant,
    signals: Arc<PauseInitialRestorePlanSignals>,
    pause_once: AtomicBool,
}

#[async_trait]
impl StateRestoreParticipant for PauseInitialRestorePlanParticipant {
    fn implementation(&self) -> &'static str {
        self.inner.implementation()
    }

    fn scope(&self) -> &StateScope {
        self.inner.scope()
    }

    fn restore_binding_identity(&self) -> StateStoreBindingIdentity {
        self.inner.restore_binding_identity()
    }

    async fn plan_restore(
        &self,
        source: &arco_catalog::PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<PersistedRestoreParticipantPlan> {
        if self.pause_once.swap(false, Ordering::SeqCst) {
            self.signals.reached.notify_one();
            self.signals.resume.notified().await;
        }
        self.inner.plan_restore(source, identity, now).await
    }

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.inspect_restore(plan).await
    }

    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.apply_restore(plan, now).await
    }
}

fn assert_restore_json_is_redacted(value: &serde_json::Value) {
    const FORBIDDEN_FIELDS: &[&str] = &[
        "state_token",
        "checkpoint_token",
        "provider_uri",
        "provider_root",
        "export_root",
        "credential",
        "credentials",
        "password",
        "secret",
        "access_key",
        "secret_key",
        "key",
        "value",
        "writes",
        "payload",
        "error",
        "backend_error",
        "atomic",
        "distributed_transaction",
        "two_phase",
    ];
    match value {
        serde_json::Value::Object(fields) => {
            for (field, nested) in fields {
                assert!(
                    !FORBIDDEN_FIELDS.contains(&field.as_str()),
                    "persisted restore record contains forbidden field {field}"
                );
                assert_restore_json_is_redacted(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                assert_restore_json_is_redacted(nested);
            }
        }
        serde_json::Value::String(string) => {
            for forbidden in [
                "StateToken",
                "CheckpointToken",
                "://",
                "Bearer ",
                "AKIA",
                "password",
                "secret",
                "injected failure",
                "distributed transaction",
            ] {
                assert!(
                    !string.contains(forbidden),
                    "persisted restore record contains forbidden string {forbidden:?}"
                );
            }
        }
        _ => {}
    }
}

struct ReleasePinDuringPlanParticipant {
    inner: ControlMvpRestoreParticipant,
    storage: ScopedStorage,
    pin_id: String,
    release_on_attempt: u64,
    armed: AtomicBool,
}

impl ReleasePinDuringPlanParticipant {
    fn new(
        inner: ControlMvpRestoreParticipant,
        storage: ScopedStorage,
        pin_id: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            storage,
            pin_id: pin_id.into(),
            release_on_attempt: 1,
            armed: AtomicBool::new(true),
        }
    }

    fn on_attempt(
        inner: ControlMvpRestoreParticipant,
        storage: ScopedStorage,
        pin_id: impl Into<String>,
        release_on_attempt: u64,
    ) -> Self {
        Self {
            inner,
            storage,
            pin_id: pin_id.into(),
            release_on_attempt,
            armed: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl StateRestoreParticipant for ReleasePinDuringPlanParticipant {
    fn implementation(&self) -> &'static str {
        self.inner.implementation()
    }

    fn scope(&self) -> &StateScope {
        self.inner.scope()
    }

    fn restore_binding_identity(&self) -> StateStoreBindingIdentity {
        self.inner.restore_binding_identity()
    }

    async fn plan_restore(
        &self,
        source: &arco_catalog::PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<PersistedRestoreParticipantPlan> {
        let plan = self.inner.plan_restore(source, identity, now).await?;
        if identity.attempt() == self.release_on_attempt && self.armed.swap(false, Ordering::SeqCst)
        {
            release_retention_pin(&self.storage, &self.pin_id).await;
        }
        Ok(plan)
    }

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.inspect_restore(plan).await
    }

    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.apply_restore(plan, now).await
    }
}

struct FailPlanRestoreParticipant {
    inner: ControlMvpRestoreParticipant,
}

struct MakeOtherParticipantVisibleDuringPlan {
    inner: ControlMvpRestoreParticipant,
    other: ControlMvpRestoreParticipant,
    other_plan: PersistedRestoreParticipantPlan,
    fire_on_attempt: u64,
    armed: AtomicBool,
}

#[async_trait]
impl StateRestoreParticipant for MakeOtherParticipantVisibleDuringPlan {
    fn implementation(&self) -> &'static str {
        self.inner.implementation()
    }

    fn scope(&self) -> &StateScope {
        self.inner.scope()
    }

    fn restore_binding_identity(&self) -> StateStoreBindingIdentity {
        self.inner.restore_binding_identity()
    }

    async fn plan_restore(
        &self,
        source: &arco_catalog::PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<PersistedRestoreParticipantPlan> {
        let plan = self.inner.plan_restore(source, identity, now).await?;
        if identity.attempt() == self.fire_on_attempt && self.armed.swap(false, Ordering::SeqCst) {
            assert!(matches!(
                self.other.apply_restore(&self.other_plan, now).await?,
                arco_catalog::RestoreParticipantInspection::Visible { .. }
            ));
        }
        Ok(plan)
    }

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.inspect_restore(plan).await
    }

    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.apply_restore(plan, now).await
    }
}

#[async_trait]
impl StateRestoreParticipant for FailPlanRestoreParticipant {
    fn implementation(&self) -> &'static str {
        self.inner.implementation()
    }

    fn scope(&self) -> &StateScope {
        self.inner.scope()
    }

    fn restore_binding_identity(&self) -> StateStoreBindingIdentity {
        self.inner.restore_binding_identity()
    }

    async fn plan_restore(
        &self,
        _source: &arco_catalog::PersistedAuthorityReference,
        _identity: &RestoreAttemptIdentity,
        _now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<PersistedRestoreParticipantPlan> {
        Err(CatalogError::UnsupportedOperation {
            message: "injected deterministic plan denial".to_string(),
        })
    }

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.inspect_restore(plan).await
    }

    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.apply_restore(plan, now).await
    }
}

struct FailNextDomainPointerBackend {
    inner: Arc<dyn StorageBackend>,
    domain: String,
    armed: AtomicBool,
    journal_statuses: Mutex<Vec<String>>,
}

struct SupersedeNextDomainPointerBackend {
    inner: Arc<dyn StorageBackend>,
    domain: String,
    armed: AtomicBool,
    journal_statuses: Mutex<Vec<String>>,
}

struct JournalReceiptWriteThenErrorBackend {
    inner: Arc<dyn StorageBackend>,
    arm_after_domain: String,
    fail_next_journal_cas: AtomicBool,
    list_calls: AtomicUsize,
}

struct FailJournalCreateOnceBackend {
    inner: Arc<dyn StorageBackend>,
    armed: AtomicBool,
    after_commit: AtomicBool,
}

struct FinalManifestCrashBackend {
    inner: Arc<dyn StorageBackend>,
    source_marker: String,
    fail_manifest: AtomicBool,
    deny_source: AtomicBool,
}

struct CrashAfterFirstReceiptBackend {
    inner: Arc<dyn StorageBackend>,
    crash_next_journal_cas: AtomicBool,
}

struct DropFirstReceiptCasBackend {
    inner: Arc<dyn StorageBackend>,
    domain: String,
    armed: AtomicBool,
    drop_next_journal_cas: AtomicBool,
}

struct CorruptFirstParticipantBeforeSecondVisibleBackend {
    inner: Arc<dyn StorageBackend>,
    first_restore_transaction: Mutex<Option<String>>,
}

struct FinalizingRaceBackend {
    inner: Arc<dyn StorageBackend>,
    fail_initial_finalizing: AtomicBool,
    race_enabled: AtomicBool,
    barrier: Barrier,
}

struct CrashAfterFinalManifestBackend {
    inner: Arc<dyn StorageBackend>,
    arm_visible_once: AtomicBool,
    fail_visible_cas: AtomicBool,
}

struct CrashAfterReplacementPlanBackend {
    inner: Arc<dyn StorageBackend>,
    fail_pointer: AtomicBool,
    fail_replacement_journal: AtomicUsize,
}

#[derive(Default)]
struct VisibleReceiptRaceSignals {
    helper_direct_inspect: Notify,
    pointer_written: Notify,
    owner_receipt_failed: Notify,
}

struct VisibleReceiptRaceBackend {
    inner: Arc<dyn StorageBackend>,
    domain: String,
    signals: Arc<VisibleReceiptRaceSignals>,
    armed: AtomicBool,
    fail_owner_receipt: AtomicBool,
    pointer_writes: AtomicUsize,
    audit_writes: AtomicBool,
    write_paths: Mutex<Vec<String>>,
}

impl VisibleReceiptRaceBackend {
    fn new(
        inner: Arc<dyn StorageBackend>,
        domain: &str,
        signals: Arc<VisibleReceiptRaceSignals>,
    ) -> Self {
        Self {
            inner,
            domain: domain.to_string(),
            signals,
            armed: AtomicBool::new(false),
            fail_owner_receipt: AtomicBool::new(false),
            pointer_writes: AtomicUsize::new(0),
            audit_writes: AtomicBool::new(false),
            write_paths: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self) {
        self.fail_owner_receipt.store(true, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn pointer_writes(&self) -> usize {
        self.pointer_writes.load(Ordering::SeqCst)
    }

    fn start_write_audit(&self) {
        self.write_paths.lock().expect("write paths").clear();
        self.audit_writes.store(true, Ordering::SeqCst);
    }

    fn stop_write_audit(&self) -> Vec<String> {
        self.audit_writes.store(false, Ordering::SeqCst);
        self.write_paths.lock().expect("write paths").clone()
    }

    fn record_write(&self, path: &str) {
        if self.audit_writes.load(Ordering::SeqCst) {
            self.write_paths
                .lock()
                .expect("write paths")
                .push(path.to_string());
        }
    }
}

#[async_trait]
impl StorageBackend for VisibleReceiptRaceBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.record_write(path);
        let pointer_suffix = format!(
            "/state-store/control-mvp/{}/current.pointer.json",
            self.domain
        );
        if self.armed.load(Ordering::SeqCst) && path.ends_with(&pointer_suffix) {
            let result = self.inner.put(path, data, precondition).await?;
            self.pointer_writes.fetch_add(1, Ordering::SeqCst);
            self.signals.pointer_written.notify_one();
            return Ok(result);
        }
        let journal_value = path
            .ends_with("/journal.json")
            .then(|| serde_json::from_slice::<serde_json::Value>(&data).ok())
            .flatten();
        let is_receipt = journal_value.as_ref().is_some_and(|value| {
            value["participants"]
                .as_array()
                .is_some_and(|participants| {
                    participants
                        .iter()
                        .any(|participant| !participant["evidence"].is_null())
                })
        });
        if self.armed.load(Ordering::SeqCst)
            && is_receipt
            && self.fail_owner_receipt.swap(false, Ordering::SeqCst)
        {
            self.signals.owner_receipt_failed.notify_one();
            return Err(arco_core::Error::storage(
                "injected owner receipt write uncertainty",
            ));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.record_write(path);
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

struct PauseHelperDirectInspectionParticipant {
    inner: ControlMvpRestoreParticipant,
    signals: Arc<VisibleReceiptRaceSignals>,
    inspections: AtomicUsize,
}

#[async_trait]
impl StateRestoreParticipant for PauseHelperDirectInspectionParticipant {
    fn implementation(&self) -> &'static str {
        self.inner.implementation()
    }

    fn scope(&self) -> &StateScope {
        self.inner.scope()
    }

    fn restore_binding_identity(&self) -> StateStoreBindingIdentity {
        self.inner.restore_binding_identity()
    }

    async fn plan_restore(
        &self,
        source: &arco_catalog::PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<PersistedRestoreParticipantPlan> {
        self.inner.plan_restore(source, identity, now).await
    }

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        let inspection = self.inspections.fetch_add(1, Ordering::SeqCst) + 1;
        if inspection == 2 {
            return Err(CatalogError::Storage {
                message: "injected pre-apply inspection crash".to_string(),
            });
        }
        if inspection == 5 {
            self.signals.helper_direct_inspect.notify_one();
            self.signals.pointer_written.notified().await;
            let visible = self.inner.inspect_restore(plan).await?;
            self.signals.owner_receipt_failed.notified().await;
            return Ok(visible);
        }
        self.inner.inspect_restore(plan).await
    }

    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: chrono::DateTime<Utc>,
    ) -> arco_catalog::Result<arco_catalog::RestoreParticipantInspection> {
        self.inner.apply_restore(plan, now).await
    }
}

impl CrashAfterReplacementPlanBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_pointer: AtomicBool::new(false),
            fail_replacement_journal: AtomicUsize::new(0),
        }
    }

    fn arm_pointer(&self) {
        self.fail_pointer.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for CrashAfterReplacementPlanBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if path.ends_with("/state-store/control-mvp/catalog/current.pointer.json")
            && self.fail_pointer.swap(false, Ordering::SeqCst)
        {
            let prior = self.inner.get(path).await?;
            let _ = self.inner.put(path, prior, WritePrecondition::None).await?;
        }
        if path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::MatchesVersion(_))
            && self.fail_replacement_journal.load(Ordering::SeqCst) > 0
        {
            self.fail_replacement_journal.fetch_sub(1, Ordering::SeqCst);
            return Err(arco_core::Error::storage(
                "injected crash after replacement plan before journal selection",
            ));
        }
        let result = self.inner.put(path, data, precondition).await?;
        if path.ends_with("/attempts/00000000000000000002.plan.json")
            && matches!(result, WriteResult::Success { .. })
        {
            self.fail_replacement_journal.store(4, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl CrashAfterFinalManifestBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            arm_visible_once: AtomicBool::new(true),
            fail_visible_cas: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl StorageBackend for CrashAfterFinalManifestBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let is_visible_cas = path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::MatchesVersion(_))
            && serde_json::from_slice::<serde_json::Value>(&data)
                .is_ok_and(|value| value["status"] == "VISIBLE");
        if is_visible_cas && self.fail_visible_cas.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected crash after final manifest before VISIBLE CAS",
            ));
        }
        let result = self.inner.put(path, data, precondition).await?;
        if path.ends_with("/read.manifest.json")
            && self.arm_visible_once.swap(false, Ordering::SeqCst)
        {
            self.fail_visible_cas.store(true, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl FinalizingRaceBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_initial_finalizing: AtomicBool::new(true),
            race_enabled: AtomicBool::new(false),
            barrier: Barrier::new(2),
        }
    }

    fn enable_race(&self) {
        self.race_enabled.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for FinalizingRaceBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let is_finalizing_cas = path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::MatchesVersion(_))
            && serde_json::from_slice::<serde_json::Value>(&data)
                .is_ok_and(|value| value["status"] == "FINALIZING");
        if is_finalizing_cas && self.fail_initial_finalizing.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected crash before initial FINALIZING CAS",
            ));
        }
        if is_finalizing_cas && self.race_enabled.load(Ordering::SeqCst) {
            self.barrier.wait().await;
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl CorruptFirstParticipantBeforeSecondVisibleBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            first_restore_transaction: Mutex::new(None),
        }
    }
}

#[async_trait]
impl StorageBackend for CorruptFirstParticipantBeforeSecondVisibleBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let result = self.inner.put(path, data, precondition).await?;
        if path.contains("/state-store/control-mvp/a/txlog/tx-restore-") {
            *self
                .first_restore_transaction
                .lock()
                .expect("transaction path") = Some(path.to_string());
        }
        if path.ends_with("/state-store/control-mvp/b/current.pointer.json") {
            let first = self
                .first_restore_transaction
                .lock()
                .expect("transaction path")
                .take();
            if let Some(first) = first {
                let _ = self
                    .inner
                    .put(&first, Bytes::from_static(b"{}"), WritePrecondition::None)
                    .await?;
            }
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl DropFirstReceiptCasBackend {
    fn new(inner: Arc<dyn StorageBackend>, domain: &str) -> Self {
        Self {
            inner,
            domain: domain.to_string(),
            armed: AtomicBool::new(false),
            drop_next_journal_cas: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for DropFirstReceiptCasBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let is_journal_cas = path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::MatchesVersion(_));
        if is_journal_cas && self.drop_next_journal_cas.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected crash before durable participant receipt",
            ));
        }
        let result = self.inner.put(path, data, precondition).await?;
        let pointer_suffix = format!(
            "/state-store/control-mvp/{}/current.pointer.json",
            self.domain
        );
        if path.ends_with(&pointer_suffix) && self.armed.swap(false, Ordering::SeqCst) {
            self.drop_next_journal_cas.store(true, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl CrashAfterFirstReceiptBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            crash_next_journal_cas: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl StorageBackend for CrashAfterFirstReceiptBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let is_journal_cas = path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::MatchesVersion(_));
        if is_journal_cas && self.crash_next_journal_cas.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected crash after first durable restore receipt",
            ));
        }
        let result = self.inner.put(path, data.clone(), precondition).await?;
        if is_journal_cas
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&data)
            && value["status"] == "APPLYING"
            && value["participants"]
                .as_array()
                .is_some_and(|participants| {
                    participants
                        .iter()
                        .filter(|participant| !participant["evidence"].is_null())
                        .count()
                        == 1
                        && participants
                            .iter()
                            .any(|participant| participant["evidence"].is_null())
                })
        {
            self.crash_next_journal_cas.store(true, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl FinalManifestCrashBackend {
    fn new(inner: Arc<dyn StorageBackend>, source_marker: &str) -> Self {
        Self {
            inner,
            source_marker: source_marker.to_string(),
            fail_manifest: AtomicBool::new(false),
            deny_source: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.fail_manifest.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for FinalManifestCrashBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        if self.deny_source.load(Ordering::SeqCst) && path.contains(&self.source_marker) {
            return Err(arco_core::Error::storage(
                "source access denied after finalization crash",
            ));
        }
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if path.ends_with("/read.manifest.json") && self.fail_manifest.swap(false, Ordering::SeqCst)
        {
            self.deny_source.store(true, Ordering::SeqCst);
            return Err(arco_core::Error::storage(
                "injected crash before final read manifest write",
            ));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl FailJournalCreateOnceBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(false),
            after_commit: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn arm_after_commit(&self) {
        self.after_commit.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for FailJournalCreateOnceBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::DoesNotExist)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(arco_core::Error::storage(
                "injected crash before restore journal creation",
            ));
        }
        let is_journal_create = path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::DoesNotExist);
        let result = self.inner.put(path, data, precondition).await?;
        if is_journal_create && self.after_commit.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected transport error after restore journal creation",
            ));
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl JournalReceiptWriteThenErrorBackend {
    fn new(inner: Arc<dyn StorageBackend>, domain: &str) -> Self {
        Self {
            inner,
            arm_after_domain: domain.to_string(),
            fail_next_journal_cas: AtomicBool::new(false),
            list_calls: AtomicUsize::new(0),
        }
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StorageBackend for JournalReceiptWriteThenErrorBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let is_journal_cas = path.ends_with("/journal.json")
            && matches!(precondition, WritePrecondition::MatchesVersion(_));
        let result = self.inner.put(path, data, precondition).await?;
        let pointer_suffix = format!(
            "/state-store/control-mvp/{}/current.pointer.json",
            self.arm_after_domain
        );
        if path.ends_with(&pointer_suffix) {
            self.fail_next_journal_cas.store(true, Ordering::SeqCst);
        } else if is_journal_cas && self.fail_next_journal_cas.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected transport error after journal receipt CAS",
            ));
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl FailNextDomainPointerBackend {
    fn new(inner: Arc<dyn StorageBackend>, domain: &str) -> Self {
        Self {
            inner,
            domain: domain.to_string(),
            armed: AtomicBool::new(false),
            journal_statuses: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn journal_statuses(&self) -> Vec<String> {
        self.journal_statuses.lock().expect("statuses").clone()
    }
}

impl SupersedeNextDomainPointerBackend {
    fn new(inner: Arc<dyn StorageBackend>, domain: &str) -> Self {
        Self {
            inner,
            domain: domain.to_string(),
            armed: AtomicBool::new(false),
            journal_statuses: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn journal_statuses(&self) -> Vec<String> {
        self.journal_statuses.lock().expect("statuses").clone()
    }
}

#[async_trait]
impl StorageBackend for SupersedeNextDomainPointerBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if path.ends_with("/journal.json")
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&data)
            && let Some(status) = value["status"].as_str()
        {
            self.journal_statuses
                .lock()
                .expect("statuses")
                .push(status.to_string());
        }
        let suffix = format!(
            "/state-store/control-mvp/{}/current.pointer.json",
            self.domain
        );
        if path.ends_with(&suffix) && self.armed.swap(false, Ordering::SeqCst) {
            let prior = self.inner.get(path).await?;
            let _ = self.inner.put(path, prior, WritePrecondition::None).await?;
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[async_trait]
impl StorageBackend for FailNextDomainPointerBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if path.ends_with("/journal.json")
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&data)
            && let Some(status) = value["status"].as_str()
        {
            self.journal_statuses
                .lock()
                .expect("statuses")
                .push(status.to_string());
        }
        let suffix = format!(
            "/state-store/control-mvp/{}/current.pointer.json",
            self.domain
        );
        if path.ends_with(&suffix) && self.armed.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected failure before participant pointer write",
            ));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn multi_domain_registry(
    stores: &[Arc<ControlMvpStateStore>],
    domains: &[&str],
    restore: bool,
) -> WorkspaceDomainRegistry {
    let bindings = stores
        .iter()
        .zip(domains)
        .map(|(store, domain)| {
            let mut binding = WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", *domain),
                store.clone(),
                store.clone(),
                Arc::new(UnusedProjectionProvider),
                Arc::new(UnusedArchiveProvider),
            )
            .expect("binding");
            if restore {
                binding = binding
                    .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
                        store.as_ref().clone(),
                    )))
                    .expect("restore participant");
            }
            binding
        })
        .collect();
    WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("workspace scope"),
        bindings,
    )
    .expect("registry")
}

#[tokio::test]
async fn preflight_before_mutation_rechecks_pin_after_later_domain_planning() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(220_u128));
    let pin_id = format!("pin_{}", Ulid::from(221_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    let before = restore_and_state_bytes(backend.as_ref()).await;

    let first = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "a"),
        stores[0].clone(),
        stores[0].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("first binding")
    .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
        stores[0].as_ref().clone(),
    )))
    .expect("first restore participant");
    let second = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "b"),
        stores[1].clone(),
        stores[1].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("second binding")
    .with_restore_participant(Arc::new(ReleasePinDuringPlanParticipant::new(
        ControlMvpRestoreParticipant::new(stores[1].as_ref().clone()),
        storage.clone(),
        &pin_id,
    )))
    .expect("later restore participant");
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![first, second],
    )
    .expect("registry");
    let restore_id = format!("rst_{}", Ulid::from(222_u128));
    let service = WorkspaceRestoreService::new(storage.clone(), registry).expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");

    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "a source pin released by the later planner must abort before publication"
    );
    assert_eq!(
        before,
        restore_and_state_bytes(backend.as_ref()).await,
        "post-plan preflight failure must write neither restore records nor state authority"
    );
}

#[tokio::test]
async fn preflight_before_mutation_rejects_missing_and_failing_later_adapters_without_listing() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(RestoreAuditBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(242_u128));
    let pin_id = format!("pin_{}", Ulid::from(243_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    backend.clear();
    backend.deny_lists();

    let a = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "a"),
        stores[0].clone(),
        stores[0].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("A binding")
    .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
        stores[0].as_ref().clone(),
    )))
    .expect("A participant");
    let b_missing = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "b"),
        stores[1].clone(),
        stores[1].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("B binding without restore");
    let missing_service = WorkspaceRestoreService::new(
        storage.clone(),
        WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            vec![a, b_missing],
        )
        .expect("missing registry"),
    )
    .expect("missing service");
    let missing = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(244_u128)),
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("missing request");
    assert!(
        missing_service
            .restore_workspace_to_snapshot(&missing)
            .await
            .is_err()
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(
            operation,
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
        )
    }));

    backend.clear();
    let a = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "a"),
        stores[0].clone(),
        stores[0].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("A binding")
    .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
        stores[0].as_ref().clone(),
    )))
    .expect("A participant");
    let b = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "b"),
        stores[1].clone(),
        stores[1].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("B binding")
    .with_restore_participant(Arc::new(FailPlanRestoreParticipant {
        inner: ControlMvpRestoreParticipant::new(stores[1].as_ref().clone()),
    }))
    .expect("failing B participant");
    let failing_service = WorkspaceRestoreService::new(
        storage,
        WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            vec![a, b],
        )
        .expect("failing registry"),
    )
    .expect("failing service");
    let failing = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(245_u128)),
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("failing request");
    assert!(
        failing_service
            .restore_workspace_to_snapshot(&failing)
            .await
            .is_err()
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(
            operation,
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
        )
    }));
}

#[tokio::test]
async fn preflight_before_mutation_rejects_a_missing_later_domain_checkpoint_without_listing() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(RestoreAuditBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(223_u128));
    let pin_id = format!("pin_{}", Ulid::from(224_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let snapshot = decode_workspace_snapshot(
        &storage
            .get_raw(&snapshot_record_path(&snapshot_id).expect("snapshot path"))
            .await
            .expect("snapshot bytes"),
    )
    .expect("snapshot record");
    let later_checkpoint = snapshot
        .domains()
        .iter()
        .find(|domain| domain.domain() == "b")
        .and_then(|domain| domain.authority().checkpoint_path())
        .expect("later-domain checkpoint");
    storage
        .delete(later_checkpoint)
        .await
        .expect("delete later-domain checkpoint");

    let restore_id = format!("rst_{}", Ulid::from(225_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.clear();
    backend.deny_lists();

    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "a missing checkpoint for the later domain must fail workspace preflight"
    );
    let operations = backend.operations();
    assert!(
        operations.iter().all(|operation| !matches!(
            operation,
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
        )),
        "later-domain artifact failure must write neither restore metadata nor state: {operations:?}"
    );
    assert!(
        storage
            .get_raw(&restore_request_path(&restore_id).expect("restore request path"))
            .await
            .is_err(),
        "artifact preflight must precede immutable restore-request publication"
    );
}

#[tokio::test]
async fn restore_authority_boundaries_bind_request_and_journal_identity_to_exact_paths() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(RestoreAuditBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(242_u128));
    let pin_id = format!("pin_{}", Ulid::from(243_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let restore_b = format!("rst_{}", Ulid::from(244_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("restore service");
    let request_b = RestoreWorkspaceToSnapshot::new(
        &restore_b,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source B"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request B");
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_workspace_to_snapshot(&request_b)
            .await
            .expect("restore B")
            .status()
    );
    let request_b_bytes = storage
        .get_raw(&restore_request_path(&restore_b).expect("request B path"))
        .await
        .expect("request B bytes");
    let journal_b_bytes = storage
        .get_raw(&restore_journal_path(&restore_b).expect("journal B path"))
        .await
        .expect("journal B bytes");

    let restore_a = format!("rst_{}", Ulid::from(245_u128));
    let request_a_path = restore_request_path(&restore_a).expect("request A path");
    storage
        .put_raw(
            &request_a_path,
            request_b_bytes,
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("place request B at A path");
    backend.clear();
    backend.deny_lists();
    assert!(
        service.recover_restore(&restore_a).await.is_err(),
        "recovery must reject request B bytes at request A's exact path"
    );
    let request_mismatch_ops = backend.operations();
    assert!(request_mismatch_ops.iter().all(|operation| !matches!(
        operation,
        AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
    )));
    assert!(
        request_mismatch_ops
            .iter()
            .all(|operation| match operation {
                AuditOperation::Get(path) | AuditOperation::Head(path) =>
                    !path.contains(&restore_b),
                AuditOperation::Put { .. }
                | AuditOperation::Delete(_)
                | AuditOperation::List(_) => true,
            })
    );

    let request_a = WorkspaceRestoreRequestRecord::new(
        &restore_a,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source A"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        RestoreOperationTarget::workspace(OmittedDomainPolicy::Reject),
    )
    .expect("request A");
    storage
        .put_raw(
            &request_a_path,
            Bytes::from(encode_workspace_restore_request(&request_a).expect("request A bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("replace exact request A");
    storage
        .put_raw(
            &restore_journal_path(&restore_a).expect("journal A path"),
            journal_b_bytes,
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("place journal B at A path");

    for read in ["get", "recover"] {
        backend.clear();
        let result = if read == "get" {
            service.get_restore(&restore_a).await
        } else {
            service.recover_restore(&restore_a).await
        };
        assert!(
            result.is_err(),
            "{read} must reject journal B bytes at journal A's exact path"
        );
        let operations = backend.operations();
        assert!(operations.iter().all(|operation| !matches!(
            operation,
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
        )));
        assert!(operations.iter().all(|operation| match operation {
            AuditOperation::Get(path) | AuditOperation::Head(path) => !path.contains(&restore_b),
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_) => {
                true
            }
        }));
    }
}

#[tokio::test]
async fn journal_precedes_domain_commit_journal_cas_and_final_read_manifest_retry_is_read_only() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(RestoreAuditBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(226_u128));
    let pin_id = format!("pin_{}", Ulid::from(227_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let restore_id = format!("rst_{}", Ulid::from(228_u128));
    let service = WorkspaceRestoreService::new(storage.clone(), domain_registry(store, true))
        .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.clear();
    backend.deny_lists();
    backend.observe_journal_before_restore_txlog(&restore_id);
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("restore")
            .status()
    );

    let operations = backend.operations();
    let request_suffix = restore_request_path(&restore_id).expect("request path");
    let attempt_suffix = restore_attempt_plan_path(&restore_id, 1).expect("attempt path");
    let journal_suffix = restore_journal_path(&restore_id).expect("journal path");
    let request_put = operations
        .iter()
        .position(|operation| matches!(operation, AuditOperation::Put { path, precondition: AuditPrecondition::DoesNotExist, .. } if path.ends_with(&request_suffix)))
        .expect("immutable request put");
    let attempt_put = operations
        .iter()
        .position(|operation| matches!(operation, AuditOperation::Put { path, precondition: AuditPrecondition::DoesNotExist, .. } if path.ends_with(&attempt_suffix)))
        .expect("immutable attempt put");
    let journal_puts = operations
        .iter()
        .enumerate()
        .filter_map(|(index, operation)| match operation {
            AuditOperation::Put {
                path,
                bytes,
                precondition,
            } if path.ends_with(&journal_suffix) => Some((
                index,
                serde_json::from_slice::<serde_json::Value>(bytes).expect("journal JSON"),
                *precondition,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!journal_puts.is_empty());
    assert_eq!(AuditPrecondition::DoesNotExist, journal_puts[0].2);
    assert!(
        journal_puts
            .iter()
            .skip(1)
            .all(|(_, _, precondition)| *precondition == AuditPrecondition::MatchesVersion)
    );
    for (offset, (_, journal, _)) in journal_puts.iter().enumerate() {
        assert_eq!(
            u64::try_from(offset + 1).expect("revision"),
            journal["revision"].as_u64().expect("journal revision")
        );
    }
    let prepared_put = journal_puts
        .iter()
        .find(|(_, journal, _)| journal["status"] == "PREPARED")
        .map(|(index, _, _)| *index)
        .expect("prepared journal");
    let applying_put = journal_puts
        .iter()
        .find(|(_, journal, _)| journal["status"] == "APPLYING")
        .map(|(index, _, _)| *index)
        .expect("applying journal");
    let first_restore_txlog = operations
        .iter()
        .position(|operation| matches!(operation, AuditOperation::Put { path, .. } if path.contains("/txlog/tx-restore-")))
        .expect("restore txlog put");
    assert!(request_put < attempt_put);
    assert!(attempt_put < prepared_put);
    assert!(prepared_put < applying_put);
    assert!(applying_put < first_restore_txlog);
    let observed: serde_json::Value =
        serde_json::from_slice(&backend.journal_at_first_restore_txlog())
            .expect("observed journal JSON");
    assert_eq!("APPLYING", observed["status"]);

    backend.clear();
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("terminal retry")
            .status()
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .recover_restore(&restore_id)
            .await
            .expect("terminal recovery")
            .status()
    );
    let terminal_operations = backend.operations();
    let journal_reads = terminal_operations
        .iter()
        .filter_map(|operation| match operation {
            AuditOperation::Head(path) if path.ends_with(&journal_suffix) => Some("HEAD"),
            AuditOperation::Get(path) if path.ends_with(&journal_suffix) => Some("GET"),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(journal_reads.starts_with(&["HEAD", "GET", "HEAD"]));
    assert!(terminal_operations.iter().all(|operation| {
        !matches!(
            operation,
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
        )
    }));

    backend.clear();
    let conflicting = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now + ChronoDuration::seconds(1),
        OmittedDomainPolicy::Reject,
    )
    .expect("conflicting request");
    assert!(matches!(
        service.restore_workspace_to_snapshot(&conflicting).await,
        Err(CatalogError::PreconditionFailed { .. })
    ));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(
            operation,
            AuditOperation::Put { .. } | AuditOperation::Delete(_) | AuditOperation::List(_)
        )
    }));

    backend.clear();
    backend.churn_journal_heads(&restore_id, 8);
    assert!(matches!(
        service.get_restore(&restore_id).await,
        Err(CatalogError::CasFailed { .. })
    ));
    assert_eq!(
        8,
        backend
            .operations()
            .iter()
            .filter(|operation| matches!(operation, AuditOperation::Head(path) if path.ends_with(&journal_suffix)))
            .count(),
        "unstable journal reads must stop after four HEAD/GET/HEAD attempts"
    );
}

#[test]
fn restore_authority_boundaries_current_state_store_has_no_restore_fallback() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let current = Arc::new(CurrentStateStore::new());
    assert!(!current.capabilities().roll_forward_restore());
    let binding = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "catalog"),
        current.clone(),
        current,
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("current binding");
    assert!(!binding.restore_configured());

    let supported =
        ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
            .expect("supported store");
    assert!(
        binding
            .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(supported)))
            .is_err(),
        "unsupported CurrentStateStore must never acquire a fabricated restore adapter"
    );
}

#[tokio::test]
async fn restore_authority_boundaries_export_is_no_list_redacted_and_read_only() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(RestoreAuditBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let catalog = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("catalog store"),
    );
    let other = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "other"),
        )
        .expect("other store"),
    );
    committed_value(&catalog, b"v1").await;
    committed_value(&other, b"other").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(232_u128));
    let snapshot_pin_id = format!("pin_{}", Ulid::from(233_u128));
    let export_id = format!("exp_{}", Ulid::from(234_u128));
    let export_pin_id = format!("pin_{}", Ulid::from(235_u128));
    let snapshots =
        WorkspaceSnapshotService::new(storage.clone(), domain_registry(catalog.clone(), false))
            .expect("snapshot service");
    snapshots
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &snapshot_pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    let legacy_path = "legacy/catalog-v0.snapshot.json";
    let legacy_bytes = Bytes::from_static(br#"{"legacy":"read-only"}"#);
    storage
        .put_raw(
            legacy_path,
            legacy_bytes.clone(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("legacy compatibility object");
    let snapshot_path = snapshot_record_path(&snapshot_id).expect("snapshot path");
    let snapshot = decode_workspace_snapshot(
        &storage
            .get_raw(&snapshot_path)
            .await
            .expect("snapshot bytes"),
    )
    .expect("snapshot record");
    let legacy_sha = format!("sha256:{}", hex::encode(Sha256::digest(&legacy_bytes)));
    let mut required_objects = snapshot.required_objects().to_vec();
    required_objects.push(
        RequiredObject::new(
            legacy_path,
            u64::try_from(legacy_bytes.len()).expect("legacy size"),
            RequiredObjectKind::LegacyCompatibility,
            &legacy_sha,
        )
        .expect("legacy required object"),
    );
    let mut compatibility = snapshot.compatibility_artifacts().to_vec();
    compatibility.push(
        LegacyCompatibilityArtifact::new(legacy_path, legacy_sha)
            .expect("legacy compatibility reference"),
    );
    let snapshot = WorkspaceSnapshot::new(
        snapshot.snapshot_id(),
        snapshot.target_pin_id(),
        snapshot.scope().clone(),
        snapshot.created_at(),
        snapshot.retained_until(),
        snapshot.parent_snapshot_id().map(ToOwned::to_owned),
        snapshot.domains().to_vec(),
        snapshot.projection_watermarks().to_vec(),
        snapshot.event_archives().to_vec(),
        required_objects,
        compatibility,
    )
    .expect("snapshot with compatibility object");
    storage
        .put_raw(
            &snapshot_path,
            Bytes::from(encode_workspace_snapshot(&snapshot).expect("snapshot bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite snapshot with compatibility object");
    let export = snapshots
        .export_snapshot(
            &CreateWorkspaceExportRequest::new(
                &export_id,
                &export_pin_id,
                &snapshot_id,
                &snapshot_pin_id,
                now,
                now + ChronoDuration::minutes(30),
            )
            .expect("export request"),
        )
        .await
        .expect("export");
    assert_eq!(1, export.compatibility_artifacts().len());
    committed_value(&catalog, b"v2").await;

    let mut immutable_source_paths = vec![
        snapshot_record_path(&snapshot_id).expect("snapshot path"),
        export_record_path(&export_id).expect("export path"),
        retention_pin_revision_path(&snapshot_pin_id, 1).expect("snapshot pin revision"),
        retention_pin_latest_path(&snapshot_pin_id).expect("snapshot pin latest"),
        retention_pin_revision_path(&export_pin_id, 1).expect("export pin revision"),
        retention_pin_latest_path(&export_pin_id).expect("export pin latest"),
    ];
    immutable_source_paths.extend(
        export
            .required_objects()
            .iter()
            .map(|object| object.relative_path().to_string()),
    );
    immutable_source_paths.extend(
        export
            .compatibility_artifacts()
            .iter()
            .map(|artifact| artifact.relative_path().to_string()),
    );
    immutable_source_paths.sort();
    immutable_source_paths.dedup();
    let mut source_before = BTreeMap::new();
    for path in &immutable_source_paths {
        source_before.insert(
            path.clone(),
            storage.get_raw(path).await.expect("source object before"),
        );
    }

    let restore_id = format!("rst_{}", Ulid::from(236_u128));
    let stores = vec![catalog, other.clone()];
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["catalog", "other"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::export(&export_id, &export_pin_id).expect("export source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Omit,
    )
    .expect("restore request");
    backend.clear();
    backend.deny_lists();
    let outcome = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("restore export");
    assert_eq!(WorkspaceRestoreStatus::Visible, outcome.status());
    assert_eq!(&["other".to_string()], outcome.omitted_domains());
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .recover_restore(&restore_id)
            .await
            .expect("direct recovery")
            .status()
    );
    assert_eq!(
        Some(Bytes::from_static(b"other")),
        arco_catalog::ArcoStateReader::get(other.as_ref(), b"catalog/default")
            .await
            .expect("omitted domain")
    );

    let operations = backend.operations();
    for operation in &operations {
        if let AuditOperation::List(prefix) = operation {
            panic!("restore attempted forbidden list {prefix}");
        }
    }
    for operation in &operations {
        let path = match operation {
            AuditOperation::Put { path, .. } | AuditOperation::Delete(path) => Some(path),
            _ => None,
        };
        let Some(path) = path else { continue };
        assert!(
            immutable_source_paths
                .iter()
                .all(|source| !path.ends_with(source)),
            "restore mutated immutable source path {path}"
        );
        assert!(!path.contains("/transactions/root/"));
        assert!(!path.contains("/commits/root/"));
        assert!(!path.contains("snapshots.parquet"));
        assert!(!path.contains("transactions.parquet"));
        assert!(!path.ends_with("/state-store/control-mvp/other/current.pointer.json"));
    }
    for (path, before) in source_before {
        assert_eq!(
            before,
            storage.get_raw(&path).await.expect("source object after"),
            "restore must preserve source bytes at {path}"
        );
    }

    let restore_prefix = format!("/transactions/restores/{restore_id}/");
    let persisted = operations
        .iter()
        .filter_map(|operation| match operation {
            AuditOperation::Put { path, bytes, .. } if path.contains(&restore_prefix) => {
                Some((path, bytes))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        persisted.len() >= 5,
        "all restore record revisions are audited"
    );
    for (path, bytes) in persisted {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).unwrap_or_else(|error| panic!("{path}: {error}"));
        assert_restore_json_is_redacted(&value);
    }
    let manifest = outcome.read_manifest().expect("final manifest");
    assert_eq!("sequential_repairable", manifest.publication_mode());
    assert_eq!(&["other".to_string()], manifest.omitted_domains());
}

#[tokio::test]
async fn workspace_restore_success_final_read_manifest_is_roll_forward_and_idempotent() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = snapshot_id();
    let pin_id = pin_id();
    let snapshot_service =
        WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
            .expect("snapshot service");
    snapshot_service
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let before = store.current_state_token().await.expect("before token");
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("restore service");
    let restore_id = restore_id();
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");

    let first = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("restore");
    assert_eq!(WorkspaceRestoreStatus::Visible, first.status());
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        arco_catalog::ArcoStateReader::get(store.as_ref(), b"catalog/default")
            .await
            .expect("read")
    );
    let after = store.current_state_token().await.expect("after token");
    assert!(after.logical_sequence() > before.logical_sequence());
    let replay = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("replay");
    assert_eq!(first, replay);
    assert_eq!(
        after.logical_sequence(),
        store
            .current_state_token()
            .await
            .expect("replay token")
            .logical_sequence(),
        "visible retry must not publish a duplicate empty-delta transaction"
    );

    let attempt_path = restore_attempt_plan_path(&restore_id, 1).expect("attempt path");
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let original_attempt = storage.get_raw(&attempt_path).await.expect("attempt bytes");
    let original_journal = storage.get_raw(&journal_path).await.expect("journal bytes");
    let attempt_value: serde_json::Value =
        serde_json::from_slice(&original_attempt).expect("attempt json");
    for malformed_participants in [
        vec![
            attempt_value["participants"][0].clone(),
            attempt_value["participants"][0].clone(),
        ],
        {
            let mut z = attempt_value["participants"][0].clone();
            z["domain"] = serde_json::Value::String("z".to_string());
            let mut a = attempt_value["participants"][0].clone();
            a["domain"] = serde_json::Value::String("a".to_string());
            vec![z, a]
        },
    ] {
        let mut malformed = attempt_value.clone();
        malformed["participants"] = serde_json::Value::Array(malformed_participants);
        let malformed_bytes = serde_jcs::to_vec(&malformed).expect("malformed attempt bytes");
        let mut selected: serde_json::Value =
            serde_json::from_slice(&original_journal).expect("journal json");
        selected["attempt_sha256"] = serde_json::Value::String(format!(
            "sha256:{}",
            hex::encode(Sha256::digest(&malformed_bytes))
        ));
        storage
            .put_raw(
                &attempt_path,
                Bytes::from(malformed_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("malformed attempt");
        storage
            .put_raw(
                &journal_path,
                Bytes::from(serde_jcs::to_vec(&selected).expect("selected journal")),
                WritePrecondition::None,
            )
            .await
            .expect("selected malformed attempt");
        assert!(service.get_restore(&restore_id).await.is_err());
    }
    storage
        .put_raw(
            &attempt_path,
            original_attempt.clone(),
            WritePrecondition::None,
        )
        .await
        .expect("restore attempt bytes");
    let mut mismatched_selection: serde_json::Value =
        serde_json::from_slice(&original_journal).expect("journal json");
    mismatched_selection["participants"][0]["plan_sha256"] =
        serde_json::Value::String(format!("sha256:{}", "f".repeat(64)));
    storage
        .put_raw(
            &journal_path,
            Bytes::from(
                serde_jcs::to_vec(&mismatched_selection).expect("mismatched journal bytes"),
            ),
            WritePrecondition::None,
        )
        .await
        .expect("mismatched selected participant");
    assert!(
        service.get_restore(&restore_id).await.is_err(),
        "read paths must cross-check journal participant digests against the selected attempt"
    );
    storage
        .put_raw(
            &journal_path,
            original_journal.clone(),
            WritePrecondition::None,
        )
        .await
        .expect("restore journal bytes");

    let manifest_path = restore_read_manifest_path(&restore_id).expect("manifest path");
    let original_manifest = storage
        .get_raw(&manifest_path)
        .await
        .expect("manifest bytes");
    let mut mismatched_manifest: serde_json::Value =
        serde_json::from_slice(&original_manifest).expect("manifest json");
    mismatched_manifest["finalized_at"] =
        serde_json::Value::String("2030-01-01T00:00:00Z".to_string());
    let mismatched_manifest_bytes =
        serde_jcs::to_vec(&mismatched_manifest).expect("mismatched manifest bytes");
    let mut mismatched_terminal: serde_json::Value =
        serde_json::from_slice(&original_journal).expect("journal json");
    mismatched_terminal["read_manifest_sha256"] = serde_json::Value::String(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&mismatched_manifest_bytes))
    ));
    storage
        .put_raw(
            &manifest_path,
            Bytes::from(mismatched_manifest_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("mismatched manifest");
    storage
        .put_raw(
            &journal_path,
            Bytes::from(serde_jcs::to_vec(&mismatched_terminal).expect("terminal journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("terminal manifest digest");
    assert!(
        service.get_restore(&restore_id).await.is_err(),
        "terminal read manifest timestamp must match the journal's frozen timestamp"
    );
    storage
        .put_raw(&manifest_path, original_manifest, WritePrecondition::None)
        .await
        .expect("restore manifest");
    storage
        .put_raw(
            &journal_path,
            original_journal.clone(),
            WritePrecondition::None,
        )
        .await
        .expect("restore terminal journal");

    let request_path = restore_request_path(&restore_id).expect("request path");
    let mut mixed_request: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&request_path).await.expect("request bytes"))
            .expect("request json");
    mixed_request["source_id"] = serde_json::Value::String(format!("snap_{}", Ulid::from(99_u128)));
    let mixed_request_bytes = serde_jcs::to_vec(&mixed_request).expect("mixed request bytes");
    storage
        .put_raw(
            &request_path,
            Bytes::from(mixed_request_bytes.clone()),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt request");
    let mut mixed_journal: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&journal_path).await.expect("journal bytes"))
            .expect("journal json");
    mixed_journal["request_sha256"] = serde_json::Value::String(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&mixed_request_bytes))
    ));
    storage
        .put_raw(
            &journal_path,
            Bytes::from(serde_jcs::to_vec(&mixed_journal).expect("mixed journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt journal");
    assert!(
        service.get_restore(&restore_id).await.is_err(),
        "a mixed request/journal must not bypass the selected attempt binding"
    );
}

#[tokio::test]
async fn partial_applying_is_durably_repair_required_before_released_source_preflight() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(CrashAfterFirstReceiptBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = snapshot_id();
    let pin_id = pin_id();
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let restore_id = restore_id();
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id.clone()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "the injected crash must leave the first receipt durably APPLYING"
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let applying: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&journal_path)
            .await
            .expect("applying journal"),
    )
    .expect("journal json");
    assert_eq!("APPLYING", applying["status"]);
    assert_eq!(
        1,
        applying["participants"]
            .as_array()
            .expect("participants")
            .iter()
            .filter(|participant| !participant["evidence"].is_null())
            .count()
    );

    let revision_one_path = retention_pin_revision_path(&pin_id, 1).expect("revision one path");
    let revision_one = decode_retention_pin_revision(
        &storage
            .get_raw(&revision_one_path)
            .await
            .expect("revision one"),
    )
    .expect("decode revision one");
    let release = revision_one
        .release(2, Utc::now())
        .expect("release active pin");
    let release_bytes = encode_retention_pin_revision(&release).expect("encode release");
    let revision_two_path = retention_pin_revision_path(&pin_id, 2).expect("revision two path");
    storage
        .put_raw(
            &revision_two_path,
            Bytes::from(release_bytes.clone()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write release revision");
    let latest = RetentionPinLatest::new(
        &pin_id,
        2,
        &revision_two_path,
        format!("sha256:{}", hex::encode(Sha256::digest(&release_bytes))),
    )
    .expect("release selector");
    storage
        .put_raw(
            &retention_pin_latest_path(&pin_id).expect("latest path"),
            Bytes::from(encode_retention_pin_latest(&latest).expect("encode selector")),
            WritePrecondition::None,
        )
        .await
        .expect("select release");

    let repaired = service
        .recover_restore(&restore_id)
        .await
        .expect("persist repair state before released-source preflight");
    assert_eq!(WorkspaceRestoreStatus::RepairRequired, repaired.status());
    let durable: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&journal_path)
            .await
            .expect("repair journal"),
    )
    .expect("repair json");
    assert_eq!("REPAIR_REQUIRED", durable["status"]);
    assert_eq!("STORAGE_UNCERTAIN", durable["failure_category"]);
}

#[tokio::test]
async fn journal_cas_rejects_corrupt_recorded_receipt_without_revision() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(CrashAfterFirstReceiptBackend::new(inner));
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(193_u128));
    let pin_id = format!("pin_{}", Ulid::from(194_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    let restore_id = format!("rst_{}", Ulid::from(195_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "the injected crash must leave one durable receipt"
    );

    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let journal_before = storage
        .get_raw(&journal_path)
        .await
        .expect("journal before");
    let journal: serde_json::Value = serde_json::from_slice(&journal_before).expect("journal json");
    let recorded = journal["participants"]
        .as_array()
        .expect("participants")
        .iter()
        .find(|participant| !participant["evidence"].is_null())
        .expect("recorded participant");
    let domain = recorded["domain"].as_str().expect("domain");
    let transaction_id = recorded["evidence"]["transaction_id"]
        .as_str()
        .expect("transaction id");
    storage
        .put_raw(
            &format!("state-store/control-mvp/{domain}/txlog/{transaction_id}.json"),
            Bytes::from_static(b"{}"),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt recorded transaction");

    assert!(
        service.recover_restore(&restore_id).await.is_err(),
        "false recorded evidence must fail exact participant inspection"
    );
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after"),
        "receipt validation failure must write zero journal revisions"
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two"))
            .await
            .is_err(),
        "receipt validation failure must not create a replacement attempt"
    );
}

#[tokio::test]
async fn visible_pointer_without_receipt_is_reconciled_before_released_source_preflight() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(DropFirstReceiptCasBackend::new(inner, "a"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(120_u128));
    let pin_id = format!("pin_{}", Ulid::from(121_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    let restore_id = format!("rst_{}", Ulid::from(122_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id.clone()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "the injected crash drops A's journal receipt after its pointer is visible"
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let applying: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&journal_path).await.expect("journal bytes"))
            .expect("journal json");
    assert_eq!("APPLYING", applying["status"]);
    assert!(
        applying["participants"]
            .as_array()
            .expect("participants")
            .iter()
            .all(|participant| participant["evidence"].is_null()),
        "the crash must occur before A's receipt CAS"
    );
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        arco_catalog::ArcoStateReader::get(stores[0].as_ref(), b"catalog/default")
            .await
            .expect("A visible")
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        arco_catalog::ArcoStateReader::get(stores[1].as_ref(), b"catalog/default")
            .await
            .expect("B pending")
    );
    release_retention_pin(&storage, &pin_id).await;

    let repaired = service
        .recover_restore(&restore_id)
        .await
        .expect("discover visible A before released-source preflight");
    assert_eq!(WorkspaceRestoreStatus::RepairRequired, repaired.status());
    assert_eq!(&["a".to_string()], repaired.completed_domains());
    assert_eq!(&["b".to_string()], repaired.pending_domains());
    let durable: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&journal_path)
            .await
            .expect("repair journal"),
    )
    .expect("repair json");
    assert_eq!("REPAIR_REQUIRED", durable["status"]);
    assert!(!durable["participants"][0]["evidence"].is_null());
    assert!(durable["participants"][1]["evidence"].is_null());
}

#[tokio::test]
async fn journal_cas_recovery_settles_uncertain_epoch_before_a_later_claim() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(DropFirstReceiptCasBackend::new(inner, "b"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(150_u128));
    let pin_id = format!("pin_{}", Ulid::from(151_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    let restore_id = format!("rst_{}", Ulid::from(152_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id.clone()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "B pointer becomes visible before its receipt CAS is dropped"
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let applying: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&journal_path).await.expect("journal bytes"))
            .expect("journal json");
    assert_eq!("APPLYING", applying["status"]);
    assert!(!applying["participants"][0]["evidence"].is_null());
    assert!(applying["participants"][1]["evidence"].is_null());
    let uncertain_epoch = retention_epoch(&storage).await;
    assert_eq!("IN_FLIGHT", uncertain_epoch["state"]);
    assert_eq!("workspace_restore_apply", uncertain_epoch["operation_kind"]);
    for store in &stores {
        assert_eq!(
            Some(Bytes::from_static(b"v1")),
            arco_catalog::ArcoStateReader::get(store.as_ref(), b"catalog/default")
                .await
                .expect("visible participant")
        );
    }
    let next_snapshot_id = format!("snap_{}", Ulid::from(153_u128));
    let next_pin_id = format!("pin_{}", Ulid::from(154_u128));
    let next_snapshot = CreateWorkspaceSnapshotRequest::new(
        &next_snapshot_id,
        &next_pin_id,
        now,
        now + ChronoDuration::hours(1),
        None,
    )
    .expect("next snapshot request");
    let snapshot_service = WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("next snapshot service");
    assert!(
        snapshot_service
            .create_snapshot(&next_snapshot)
            .await
            .is_err(),
        "a different coordinated mutation must fail closed while restore apply is uncertain"
    );
    assert!(
        storage
            .get_raw(&snapshot_record_path(&next_snapshot_id).expect("snapshot path"))
            .await
            .is_err(),
        "a blocked claim must precede snapshot publication"
    );
    release_retention_pin(&storage, &pin_id).await;

    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("mark strict partial repair")
            .status()
    );
    assert_eq!("IN_FLIGHT", retention_epoch(&storage).await["state"]);
    let visible = service
        .recover_restore(&restore_id)
        .await
        .expect("adopt B receipt and finalize without released source");
    assert_eq!(WorkspaceRestoreStatus::Visible, visible.status());
    assert_eq!(
        &["a".to_string(), "b".to_string()],
        visible.completed_domains()
    );
    let settled_epoch = retention_epoch(&storage).await;
    assert_eq!("IDLE", settled_epoch["state"]);
    assert_eq!("workspace_restore_apply", settled_epoch["operation_kind"]);
    snapshot_service
        .create_snapshot(&next_snapshot)
        .await
        .expect("a later coordinated snapshot claim succeeds after terminal reconciliation");
    for store in &stores {
        committed_value(store, b"v3").await;
    }
    let next_restore = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(155_u128)),
        RestoreSource::snapshot(next_snapshot_id, next_pin_id).expect("next source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("next restore request");
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_workspace_to_snapshot(&next_restore)
            .await
            .expect("a later restore claim succeeds after terminal reconciliation")
            .status()
    );
}

#[tokio::test]
async fn final_read_manifest_revalidates_every_receipt_after_the_last_participant() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(CorruptFirstParticipantBeforeSecondVisibleBackend::new(
        inner,
    ));
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(140_u128));
    let pin_id = format!("pin_{}", Ulid::from(141_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    let restore_id = format!("rst_{}", Ulid::from(142_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");

    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "A's corrupt receipt artifacts must block same-call finalization after B"
    );
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal bytes"),
    )
    .expect("journal json");
    assert_eq!("APPLYING", journal["status"]);
    assert!(
        journal["participants"]
            .as_array()
            .expect("participants")
            .iter()
            .all(|participant| !participant["evidence"].is_null())
    );
    assert!(
        storage
            .get_raw(&restore_read_manifest_path(&restore_id).expect("manifest path"))
            .await
            .is_err(),
        "corrupt receipt evidence must be rejected before final-manifest publication"
    );
}

#[tokio::test]
async fn existing_restore_requires_its_immutable_request_before_any_mutation() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(90_u128));
    let pin_id = format!("pin_{}", Ulid::from(91_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(92_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("pointer failure is durably repairable")
            .status()
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let journal_before = storage.get_raw(&journal_path).await.expect("journal");
    let sequence_before = store
        .current_state_token()
        .await
        .expect("state token")
        .logical_sequence();
    let conflicting = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now + ChronoDuration::seconds(1),
        OmittedDomainPolicy::Reject,
    )
    .expect("conflicting request");
    assert!(matches!(
        service.restore_workspace_to_snapshot(&conflicting).await,
        Err(CatalogError::PreconditionFailed { .. })
    ));
    assert_eq!(
        journal_before,
        storage
            .get_raw(&journal_path)
            .await
            .expect("journal after conflict")
    );
    storage
        .delete(&restore_request_path(&restore_id).expect("request path"))
        .await
        .expect("delete immutable request");

    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "a caller-supplied request cannot replace missing durable authority"
    );
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after"),
        "missing immutable request must be rejected before journal mutation"
    );
    assert_eq!(
        sequence_before,
        store
            .current_state_token()
            .await
            .expect("state token after")
            .logical_sequence(),
        "missing immutable request must be rejected before participant mutation"
    );
}

#[tokio::test]
async fn existing_attempt_rejects_a_cross_source_participant_before_metadata_mutation() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let state_scope = StateScope::new("tenant", "workspace", "catalog");
    let store =
        Arc::new(ControlMvpStateStore::new(storage.clone(), state_scope.clone()).expect("store"));
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(100_u128));
    let pin_id = format!("pin_{}", Ulid::from(101_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(102_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("pointer failure is durably repairable")
            .status()
    );

    let alternate_checkpoint = store
        .checkpoint(CheckpointOptions::new(Some(state_scope)))
        .await
        .expect("alternate checkpoint");
    let alternate_source = store
        .persist_checkpoint_reference(&alternate_checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("alternate source reference");
    let alternate_plan = ControlMvpRestoreParticipant::new(store.as_ref().clone())
        .plan_restore(
            &alternate_source,
            &RestoreAttemptIdentity::new(&restore_id, 1, "catalog").expect("identity"),
            Utc::now(),
        )
        .await
        .expect("alternate plan");
    let alternate_plan_bytes = serde_jcs::to_vec(&alternate_plan).expect("plan bytes");
    let alternate_plan_sha = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&alternate_plan_bytes))
    );
    let attempt_path = restore_attempt_plan_path(&restore_id, 1).expect("attempt path");
    let mut attempt: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&attempt_path).await.expect("attempt bytes"))
            .expect("attempt json");
    attempt["participants"][0]["plan"] = serde_json::to_value(&alternate_plan).expect("plan json");
    attempt["participants"][0]["plan_sha256"] =
        serde_json::Value::String(alternate_plan_sha.clone());
    let attempt_bytes = serde_jcs::to_vec(&attempt).expect("attempt bytes");
    let attempt_sha = format!("sha256:{}", hex::encode(Sha256::digest(&attempt_bytes)));
    storage
        .put_raw(
            &attempt_path,
            Bytes::from(attempt_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("replace attempt");
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let mut journal: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&journal_path).await.expect("journal bytes"))
            .expect("journal json");
    journal["attempt_sha256"] = serde_json::Value::String(attempt_sha);
    journal["participants"][0]["plan_sha256"] = serde_json::Value::String(alternate_plan_sha);
    storage
        .put_raw(
            &journal_path,
            Bytes::from(serde_jcs::to_vec(&journal).expect("journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("select cross-source attempt");
    committed_value(&store, b"foreign").await;
    let journal_before = storage
        .get_raw(&journal_path)
        .await
        .expect("journal before");

    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "a participant plan must exactly match the current validated source authority"
    );
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after"),
        "cross-source plan rejection must precede repair or replacement metadata"
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two path"))
            .await
            .is_err(),
        "cross-source persisted plans must not create a replacement attempt"
    );
}

#[tokio::test]
async fn workspace_restore_recovery_preserves_carried_ready_additive_wire_bytes_and_digest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "b"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b", "c"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = snapshot_id();
    let pin_id = pin_id();
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b", "c"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let restore_id = restore_id();
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b", "c"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    let partial = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("partial restore is durably repairable");
    assert_eq!(WorkspaceRestoreStatus::RepairRequired, partial.status());
    assert!(
        backend
            .journal_statuses()
            .windows(3)
            .any(|window| { window == ["REPAIR_REQUIRED", "APPLYING", "REPAIR_REQUIRED"] })
    );
    let mut attempt_one: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 1).expect("attempt path"))
            .await
            .expect("attempt one"),
    )
    .expect("attempt json");
    assert!(attempt_one["source_record_sha256"].is_string());
    assert!(attempt_one["active_retention_deadline"].is_string());
    let mut journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal"),
    )
    .expect("journal json");
    assert_eq!("tenant", journal["scope"]["tenant_id"]);
    assert_eq!(
        restore_request_path(&restore_id).expect("request path"),
        journal["request_path"]
    );
    assert_eq!(
        3,
        journal["required_domains"]
            .as_array()
            .expect("domains")
            .len()
    );
    assert_eq!("CAS_LOST", journal["failure_category"]);
    assert!(!journal.to_string().contains("injected failure"));
    assert_eq!(
        restore_read_manifest_path(&restore_id).expect("manifest path"),
        journal["read_manifest_path"]
    );
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        arco_catalog::ArcoStateReader::get(stores[0].as_ref(), b"catalog/default")
            .await
            .expect("a visible")
    );
    attempt_one["participants"][2]["plan"]["future_additive_v1_field"] =
        serde_json::json!({"retained": ["exact", "canonical", "wire"]});
    let carried_plan = attempt_one["participants"][2]["plan"].clone();
    let carried_plan_bytes = serde_jcs::to_vec(&carried_plan).expect("carried plan bytes");
    let carried_plan_sha = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&carried_plan_bytes))
    );
    attempt_one["participants"][2]["plan_sha256"] =
        serde_json::Value::String(carried_plan_sha.clone());
    let attempt_one_bytes = serde_jcs::to_vec(&attempt_one).expect("additive attempt bytes");
    let attempt_one_sha = format!("sha256:{}", hex::encode(Sha256::digest(&attempt_one_bytes)));
    journal["attempt_sha256"] = serde_json::Value::String(attempt_one_sha);
    journal["participants"][2]["plan_sha256"] = serde_json::Value::String(carried_plan_sha.clone());
    storage
        .put_raw(
            &restore_attempt_plan_path(&restore_id, 1).expect("attempt path"),
            Bytes::from(attempt_one_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("inject additive participant plan field");
    storage
        .put_raw(
            &restore_journal_path(&restore_id).expect("journal path"),
            Bytes::from(serde_jcs::to_vec(&journal).expect("updated journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("bind journal to additive participant plan");
    let recovered = service
        .recover_restore(&restore_id)
        .await
        .expect("recover restore from durable CAS_LOST");
    assert_eq!(WorkspaceRestoreStatus::Visible, recovered.status());
    assert_eq!(
        &["a".to_string(), "b".to_string(), "c".to_string()],
        recovered.completed_domains()
    );
    for store in &stores {
        assert_eq!(
            Some(Bytes::from_static(b"v1")),
            arco_catalog::ArcoStateReader::get(store.as_ref(), b"catalog/default")
                .await
                .expect("restored value")
        );
    }

    let attempt_two: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt path"))
            .await
            .expect("attempt two"),
    )
    .expect("attempt json");
    let participants = attempt_two["participants"]
        .as_array()
        .expect("participants");
    assert_eq!(2, participants.len(), "completed a is receipt-only");
    assert_eq!("b", participants[0]["domain"]);
    assert_eq!(2, participants[0]["participant_attempt"]);
    assert_eq!("c", participants[1]["domain"]);
    assert_eq!(1, participants[1]["participant_attempt"]);
    assert_eq!(carried_plan_sha, participants[1]["plan_sha256"]);
    assert_eq!(carried_plan, participants[1]["plan"]);
    assert_eq!(
        carried_plan_bytes,
        serde_jcs::to_vec(&participants[1]["plan"]).expect("replacement plan bytes"),
        "a Ready participant must retain its exact canonical plan fragment"
    );
    let final_manifest: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_read_manifest_path(&restore_id).expect("manifest path"))
            .await
            .expect("final manifest"),
    )
    .expect("manifest json");
    assert_eq!("snapshot", final_manifest["source_kind"]);
    assert!(final_manifest["source_id"].is_string());
    assert!(final_manifest["source_pin_id"].is_string());
    assert_eq!("workspace", final_manifest["scope"]["workspace_id"]);
}

#[tokio::test]
async fn workspace_restore_recovery_adopts_a_carried_participant_visible_during_replanning() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "b"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b", "c"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(229_u128));
    let pin_id = format!("pin_{}", Ulid::from(230_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b", "c"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let restore_id = format!("rst_{}", Ulid::from(231_u128));
    let ordinary_service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b", "c"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        ordinary_service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("B pointer failure is durably repairable")
            .status()
    );

    let attempt_one: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 1).expect("attempt one path"))
            .await
            .expect("attempt one"),
    )
    .expect("attempt one JSON");
    let c_attempt_one = attempt_one["participants"]
        .as_array()
        .expect("attempt-one participants")
        .iter()
        .find(|participant| participant["domain"] == "c")
        .expect("C attempt-one participant")
        .clone();
    let c_plan: PersistedRestoreParticipantPlan =
        serde_json::from_value(c_attempt_one["plan"].clone()).expect("C attempt-one plan");
    let c_plan_sha = c_attempt_one["plan_sha256"]
        .as_str()
        .expect("C plan digest")
        .to_string();

    let attempt_two_path = restore_attempt_plan_path(&restore_id, 2).expect("attempt two path");
    assert!(
        storage.get_raw(&attempt_two_path).await.is_err(),
        "durable CAS_LOST must precede replacement"
    );

    let bindings = vec![
        WorkspaceDomainBinding::new(
            StateScope::new("tenant", "workspace", "a"),
            stores[0].clone(),
            stores[0].clone(),
            Arc::new(UnusedProjectionProvider),
            Arc::new(UnusedArchiveProvider),
        )
        .expect("A binding")
        .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
            stores[0].as_ref().clone(),
        )))
        .expect("A restore participant"),
        WorkspaceDomainBinding::new(
            StateScope::new("tenant", "workspace", "b"),
            stores[1].clone(),
            stores[1].clone(),
            Arc::new(UnusedProjectionProvider),
            Arc::new(UnusedArchiveProvider),
        )
        .expect("B binding")
        .with_restore_participant(Arc::new(MakeOtherParticipantVisibleDuringPlan {
            inner: ControlMvpRestoreParticipant::new(stores[1].as_ref().clone()),
            other: ControlMvpRestoreParticipant::new(stores[2].as_ref().clone()),
            other_plan: c_plan,
            fire_on_attempt: 2,
            armed: AtomicBool::new(true),
        }))
        .expect("racing B restore participant"),
        WorkspaceDomainBinding::new(
            StateScope::new("tenant", "workspace", "c"),
            stores[2].clone(),
            stores[2].clone(),
            Arc::new(UnusedProjectionProvider),
            Arc::new(UnusedArchiveProvider),
        )
        .expect("C binding")
        .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
            stores[2].as_ref().clone(),
        )))
        .expect("C restore participant"),
    ];
    let racing_service = WorkspaceRestoreService::new(
        storage.clone(),
        WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            bindings,
        )
        .expect("registry"),
    )
    .expect("racing service");

    let adoption = racing_service
        .recover_restore(&restore_id)
        .await
        .expect("adopt C receipt raced during B replanning");
    assert_eq!(WorkspaceRestoreStatus::RepairRequired, adoption.status());
    assert!(
        storage.get_raw(&attempt_two_path).await.is_err(),
        "the helper must adopt the raced C receipt before publishing attempt two"
    );
    let adopted_journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("adopted journal"),
    )
    .expect("adopted journal JSON");
    assert_eq!(1, adopted_journal["aggregate_attempt"]);
    let adopted_c = adopted_journal["participants"]
        .as_array()
        .expect("journal participants")
        .iter()
        .find(|participant| participant["domain"] == "c")
        .expect("adopted C participant");
    assert_eq!(1, adopted_c["participant_attempt"]);
    assert_eq!(c_plan_sha, adopted_c["plan_sha256"]);
    assert_eq!(1, adopted_c["evidence"]["participant_attempt"]);

    let visible = racing_service
        .recover_restore(&restore_id)
        .await
        .expect("replace only B after C receipt adoption");
    assert_eq!(WorkspaceRestoreStatus::Visible, visible.status());
    let attempt_two: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&attempt_two_path)
            .await
            .expect("attempt two"),
    )
    .expect("attempt two JSON");
    let participants = attempt_two["participants"]
        .as_array()
        .expect("attempt-two participants");
    assert_eq!(
        1,
        participants.len(),
        "C must not be duplicated in attempt two"
    );
    assert_eq!("b", participants[0]["domain"]);
    assert_eq!(2, participants[0]["participant_attempt"]);
    let final_journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("final journal"),
    )
    .expect("final journal JSON");
    let final_c = final_journal["participants"]
        .as_array()
        .expect("final participants")
        .iter()
        .find(|participant| participant["domain"] == "c")
        .expect("final C participant");
    assert_eq!(
        adopted_c, final_c,
        "C's attempt-one receipt must remain exact"
    );
}

#[tokio::test]
async fn journal_cas_unknown_write_readback_reconciles_without_listing() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(JournalReceiptWriteThenErrorBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(20_u128));
    let pin_id = format!("pin_{}", Ulid::from(21_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let service = WorkspaceRestoreService::new(storage, domain_registry(store.clone(), true))
        .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(22_u128)),
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");

    let outcome = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("uncertain journal CAS is reconciled");
    assert_eq!(WorkspaceRestoreStatus::Visible, outcome.status());
    assert_eq!(
        3,
        store
            .current_state_token()
            .await
            .expect("token")
            .logical_sequence()
    );
    assert_eq!(
        0,
        backend.list_calls(),
        "restore request path must never list"
    );
}

#[tokio::test]
async fn journal_cas_concurrent_helpers_adopt_terminal_winner() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(30_u128));
    let pin_id = format!("pin_{}", Ulid::from(31_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let first = WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
        .expect("first service");
    let second = WorkspaceRestoreService::new(storage, domain_registry(store.clone(), true))
        .expect("second service");
    let request = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(32_u128)),
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");

    let (left, right) = tokio::join!(
        first.restore_workspace_to_snapshot(&request),
        second.restore_workspace_to_snapshot(&request)
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        left.expect("left").status()
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        right.expect("right").status()
    );
    assert_eq!(
        3,
        store
            .current_state_token()
            .await
            .expect("token")
            .logical_sequence(),
        "concurrent helpers must not publish duplicate restore transactions"
    );
}

#[tokio::test]
async fn concurrent_finalizers_adopt_one_frozen_manifest_winner() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FinalizingRaceBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(170_u128));
    let pin_id = format!("pin_{}", Ulid::from(171_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(172_u128));
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    let initial =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("initial service");
    assert!(
        initial
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "initial crash leaves APPLYING with all receipts durable"
    );
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal bytes"),
    )
    .expect("journal json");
    assert_eq!("APPLYING", journal["status"]);
    assert!(!journal["participants"][0]["evidence"].is_null());

    backend.enable_race();
    let left = WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
        .expect("left service");
    let right = WorkspaceRestoreService::new(storage.clone(), domain_registry(store, true))
        .expect("right service");
    let (left, right) = tokio::join!(
        left.recover_restore(&restore_id),
        right.recover_restore(&restore_id)
    );
    let left = left.expect("left adopts finalizing winner");
    let right = right.expect("right adopts finalizing winner");
    assert_eq!(WorkspaceRestoreStatus::Visible, left.status());
    assert_eq!(left, right);
    let terminal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("terminal journal"),
    )
    .expect("terminal json");
    let manifest_bytes = storage
        .get_raw(&restore_read_manifest_path(&restore_id).expect("manifest path"))
        .await
        .expect("winner manifest");
    assert_eq!(
        format!("sha256:{}", hex::encode(Sha256::digest(&manifest_bytes))),
        terminal["read_manifest_sha256"],
        "immutable manifest bytes must belong to the selected frozen winner"
    );
}

#[tokio::test]
async fn workspace_restore_recovery_adopts_multi_domain_orphan_before_any_participant_apply() {
    let memory: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let crash = Arc::new(FailJournalCreateOnceBackend::new(memory));
    let backend = Arc::new(RestoreAuditBackend::new(crash.clone()));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(237_u128));
    let pin_id = format!("pin_{}", Ulid::from(238_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }
    let restore_id = format!("rst_{}", Ulid::from(239_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    crash.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err()
    );
    let attempt_path = restore_attempt_plan_path(&restore_id, 1).expect("attempt path");
    let orphan_attempt = storage
        .get_raw(&attempt_path)
        .await
        .expect("orphan attempt");
    let a_pointer_path = "state-store/control-mvp/a/current.pointer.json";
    let a_pointer_before = storage
        .get_raw(a_pointer_path)
        .await
        .expect("A pointer before recovery");
    committed_value(&stores[1], b"foreign").await;

    backend.clear();
    backend.deny_lists();
    let outcome = service
        .recover_restore(&restore_id)
        .await
        .expect("adopt orphan and record supersession");
    assert_eq!(WorkspaceRestoreStatus::RepairRequired, outcome.status());
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal bytes"),
    )
    .expect("journal JSON");
    assert_eq!("CAS_LOST", journal["failure_category"]);
    assert_eq!(
        orphan_attempt,
        storage.get_raw(&attempt_path).await.expect("attempt after")
    );
    assert_eq!(
        a_pointer_before,
        storage
            .get_raw(a_pointer_path)
            .await
            .expect("A pointer after recovery"),
        "Ready A must not apply before superseded B is durably classified"
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        arco_catalog::ArcoStateReader::get(stores[0].as_ref(), b"catalog/default")
            .await
            .expect("A remains unchanged")
    );
    assert_eq!(
        Some(Bytes::from_static(b"foreign")),
        arco_catalog::ArcoStateReader::get(stores[1].as_ref(), b"catalog/default")
            .await
            .expect("B foreign winner remains")
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, AuditOperation::Put { path, .. }
            if path.contains("/txlog/tx-restore-") || path.ends_with("/current.pointer.json"))
    }));
}

#[tokio::test]
async fn workspace_restore_adopts_orphan_attempt_before_replanning() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailJournalCreateOnceBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(40_u128));
    let pin_id = format!("pin_{}", Ulid::from(41_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(42_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "journal-create crash leaves request and attempt orphans"
    );
    let orphan = storage
        .get_raw(&restore_attempt_plan_path(&restore_id, 1).expect("attempt path"))
        .await
        .expect("orphan attempt");
    committed_value(&store, b"foreign").await;

    let first_recovery = service
        .recover_restore(&restore_id)
        .await
        .expect("adopt orphan attempt");
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        first_recovery.status()
    );
    assert_eq!(
        orphan,
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 1).expect("attempt path"))
            .await
            .expect("adopted attempt")
    );
    let visible = service
        .recover_restore(&restore_id)
        .await
        .expect("repair superseded orphan");
    assert_eq!(WorkspaceRestoreStatus::Visible, visible.status());

    let uncertain = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(43_u128)),
        RestoreSource::snapshot(
            format!("snap_{}", Ulid::from(40_u128)),
            format!("pin_{}", Ulid::from(41_u128)),
        )
        .expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("uncertain request");
    backend.arm_after_commit();
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_workspace_to_snapshot(&uncertain)
            .await
            .expect("initial journal uncertainty reconciled")
            .status()
    );
}

#[tokio::test]
async fn orphan_request_with_additive_v1_fields_retries_by_typed_semantics() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailJournalCreateOnceBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(110_u128));
    let pin_id = format!("pin_{}", Ulid::from(111_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(112_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "journal-create crash leaves request and attempt orphans"
    );

    let request_path = restore_request_path(&restore_id).expect("request path");
    let mut request_json: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&request_path).await.expect("request bytes"))
            .expect("request json");
    request_json["future_v1_field"] = serde_json::json!({"opaque": true});
    let additive_request_bytes = serde_jcs::to_vec(&request_json).expect("additive request bytes");
    let additive_request_sha = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&additive_request_bytes))
    );
    storage
        .put_raw(
            &request_path,
            Bytes::from(additive_request_bytes.clone()),
            WritePrecondition::None,
        )
        .await
        .expect("write additive request");
    let attempt_path = restore_attempt_plan_path(&restore_id, 1).expect("attempt path");
    let mut attempt: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&attempt_path).await.expect("attempt bytes"))
            .expect("attempt json");
    attempt["request_sha256"] = serde_json::Value::String(additive_request_sha.clone());
    attempt["future_attempt_v1_field"] = serde_json::json!({"preserve": "raw"});
    let additive_attempt_bytes = serde_jcs::to_vec(&attempt).expect("additive attempt bytes");
    storage
        .put_raw(
            &attempt_path,
            Bytes::from(additive_attempt_bytes.clone()),
            WritePrecondition::None,
        )
        .await
        .expect("bind attempt to additive request");

    let outcome = service
        .restore_workspace_to_snapshot(&request)
        .await
        .expect("typed-semantic-equivalent additive request retry");
    assert_eq!(WorkspaceRestoreStatus::Visible, outcome.status());
    assert_eq!(
        Bytes::from(additive_request_bytes),
        storage
            .get_raw(&request_path)
            .await
            .expect("durable request"),
        "retry must preserve the exact additive request bytes"
    );
    assert_eq!(
        Bytes::from(additive_attempt_bytes),
        storage
            .get_raw(&attempt_path)
            .await
            .expect("durable attempt"),
        "retry must preserve the exact additive attempt bytes"
    );
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal bytes"),
    )
    .expect("journal json");
    assert_eq!(additive_request_sha, journal["request_sha256"]);
}

#[tokio::test]
async fn workspace_restore_persists_zero_receipt_supersession_before_attempt_two() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(50_u128));
    let pin_id = format!("pin_{}", Ulid::from(51_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(52_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("pointer failure is durably repairable")
            .status()
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt path"))
            .await
            .is_err(),
        "attempt two must not predate durable REPAIR_REQUIRED"
    );
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal"),
    )
    .expect("journal json");
    assert_eq!("CAS_LOST", journal["failure_category"]);
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .recover_restore(&restore_id)
            .await
            .expect("attempt two")
            .status()
    );
}

#[tokio::test]
async fn workspace_restore_recovery_err_then_ready_keeps_epoch_in_flight_until_terminal_proof() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailNextDomainPointerBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(232_u128));
    let pin_id = format!("pin_{}", Ulid::from(233_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let restore_id = format!("rst_{}", Ulid::from(234_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert!(matches!(
        service.restore_workspace_to_snapshot(&request).await,
        Err(CatalogError::Storage { .. })
    ));
    assert!(
        backend
            .journal_statuses()
            .iter()
            .any(|status| status == "REPAIR_REQUIRED")
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let repair_journal_bytes = storage
        .get_raw(&journal_path)
        .await
        .expect("repair journal");
    let journal: serde_json::Value =
        serde_json::from_slice(&repair_journal_bytes).expect("repair journal JSON");
    assert_eq!("REPAIR_REQUIRED", journal["status"]);
    let in_flight = retention_epoch(&storage).await;
    assert_eq!("IN_FLIGHT", in_flight["state"]);
    assert_eq!("workspace_restore_apply", in_flight["operation_kind"]);
    let in_flight_bytes = storage
        .get_raw("retention/coordination/mutation-epoch.json")
        .await
        .expect("in-flight epoch bytes");
    let token_before_retry = store
        .current_state_token()
        .await
        .expect("state token before repair retry");

    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("Ready under an exact uncertain epoch remains operator repair")
            .status()
    );
    assert_eq!(
        repair_journal_bytes,
        storage
            .get_raw(&journal_path)
            .await
            .expect("repair journal after retry"),
        "Ready recovery must not revise the repair journal"
    );
    assert_eq!(
        in_flight_bytes,
        storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("in-flight epoch after retry"),
        "Ready recovery must not settle or replace the uncertain epoch"
    );
    assert_eq!(
        token_before_retry,
        store
            .current_state_token()
            .await
            .expect("state token after repair retry")
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two path"))
            .await
            .is_err(),
        "Ready recovery must not publish a replacement attempt"
    );

    let blocked_snapshot_id = format!("snap_{}", Ulid::from(235_u128));
    let blocked_pin_id = format!("pin_{}", Ulid::from(236_u128));
    assert!(
        WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
            .expect("second snapshot service")
            .create_snapshot(
                &CreateWorkspaceSnapshotRequest::new(
                    &blocked_snapshot_id,
                    &blocked_pin_id,
                    now,
                    now + ChronoDuration::hours(1),
                    None,
                )
                .expect("second snapshot request"),
            )
            .await
            .is_err(),
        "a different coordinated publication must not claim over uncertain restore apply"
    );
    assert_eq!(
        in_flight,
        retention_epoch(&storage).await,
        "blocked work must not auto-settle or replace the uncertain epoch"
    );
    assert!(
        storage
            .get_raw(&snapshot_record_path(&blocked_snapshot_id).expect("blocked snapshot path"))
            .await
            .is_err()
    );

    committed_value(&store, b"foreign").await;
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("Superseded inspection settles exact uncertain operation")
            .status()
    );
    assert_eq!("IDLE", retention_epoch(&storage).await["state"]);
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .recover_restore(&restore_id)
            .await
            .expect("replacement after terminal settlement")
            .status()
    );
}

#[tokio::test]
async fn workspace_restore_recovery_ready_epoch_blocks_replacement_for_other_superseded_domain() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailNextDomainPointerBackend::new(inner, "a"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(246_u128));
    let pin_id = format!("pin_{}", Ulid::from(247_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let restore_id = format!("rst_{}", Ulid::from(248_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert!(matches!(
        service.restore_workspace_to_snapshot(&request).await,
        Err(CatalogError::Storage { .. })
    ));
    assert_eq!("IN_FLIGHT", retention_epoch(&storage).await["state"]);

    committed_value(&stores[1], b"foreign-b").await;
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let journal_before = storage
        .get_raw(&journal_path)
        .await
        .expect("journal before");
    let epoch_before = storage
        .get_raw("retention/coordination/mutation-epoch.json")
        .await
        .expect("epoch before");
    let a_before = stores[0]
        .current_state_token()
        .await
        .expect("A token before");
    let b_before = stores[1]
        .current_state_token()
        .await
        .expect("B token before");

    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("A's unresolved epoch blocks B replacement")
            .status()
    );
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after")
    );
    assert_eq!(
        epoch_before,
        storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("epoch after")
    );
    assert_eq!(
        a_before,
        stores[0]
            .current_state_token()
            .await
            .expect("A token after")
    );
    assert_eq!(
        b_before,
        stores[1]
            .current_state_token()
            .await
            .expect("B token after")
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two path"))
            .await
            .is_err(),
        "a different Superseded domain cannot trigger replacement while A is unresolved"
    );
}

#[tokio::test]
async fn workspace_restore_recovery_ready_epoch_defers_other_visible_receipt_adoption() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailNextDomainPointerBackend::new(inner, "a"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(252_u128));
    let pin_id = format!("pin_{}", Ulid::from(253_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let restore_id = format!("rst_{}", Ulid::from(254_u128));
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert!(matches!(
        service.restore_workspace_to_snapshot(&request).await,
        Err(CatalogError::Storage { .. })
    ));
    assert_eq!("IN_FLIGHT", retention_epoch(&storage).await["state"]);

    let attempt_one: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 1).expect("attempt one path"))
            .await
            .expect("attempt one"),
    )
    .expect("attempt one JSON");
    let b_plan: PersistedRestoreParticipantPlan = serde_json::from_value(
        attempt_one["participants"]
            .as_array()
            .expect("participants")
            .iter()
            .find(|participant| participant["domain"] == "b")
            .expect("B participant")["plan"]
            .clone(),
    )
    .expect("B plan");
    assert!(matches!(
        ControlMvpRestoreParticipant::new(stores[1].as_ref().clone())
            .apply_restore(&b_plan, Utc::now())
            .await
            .expect("publish exact B plan"),
        arco_catalog::RestoreParticipantInspection::Visible { .. }
    ));

    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let journal_before = storage
        .get_raw(&journal_path)
        .await
        .expect("journal before");
    let epoch_before = storage
        .get_raw("retention/coordination/mutation-epoch.json")
        .await
        .expect("epoch before");
    let b_before = stores[1]
        .current_state_token()
        .await
        .expect("B token before");
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("A's unresolved Ready epoch defers B receipt adoption")
            .status()
    );
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after")
    );
    assert_eq!(
        epoch_before,
        storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("epoch after")
    );
    assert_eq!(
        b_before,
        stores[1]
            .current_state_token()
            .await
            .expect("B token after")
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two path"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn workspace_restore_recovery_seeded_applying_ready_epoch_cas_reduces_once_to_repair() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailNextDomainPointerBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(249_u128));
    let pin_id = format!("pin_{}", Ulid::from(250_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let restore_id = format!("rst_{}", Ulid::from(251_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert!(matches!(
        service.restore_workspace_to_snapshot(&request).await,
        Err(CatalogError::Storage { .. })
    ));

    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let mut seeded: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&journal_path)
            .await
            .expect("repair journal"),
    )
    .expect("repair journal JSON");
    seeded["revision"] =
        serde_json::Value::from(seeded["revision"].as_u64().expect("journal revision") + 1);
    seeded["status"] = serde_json::Value::String("APPLYING".to_string());
    seeded["failure_category"] = serde_json::Value::Null;
    let seeded_revision = seeded["revision"].as_u64().expect("seeded revision");
    storage
        .put_raw(
            &journal_path,
            Bytes::from(serde_jcs::to_vec(&seeded).expect("seeded journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("seed crash-after-claim journal");
    let epoch_before = storage
        .get_raw("retention/coordination/mutation-epoch.json")
        .await
        .expect("epoch before recovery");
    let token_before = store
        .current_state_token()
        .await
        .expect("token before recovery");

    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("APPLYING plus exact Ready epoch reduces to operator repair")
            .status()
    );
    let repaired_bytes = storage
        .get_raw(&journal_path)
        .await
        .expect("repaired journal");
    let repaired: serde_json::Value =
        serde_json::from_slice(&repaired_bytes).expect("repaired journal JSON");
    assert_eq!("REPAIR_REQUIRED", repaired["status"]);
    assert_eq!("STORAGE_UNCERTAIN", repaired["failure_category"]);
    assert_eq!(seeded_revision + 1, repaired["revision"]);
    assert_eq!(
        epoch_before,
        storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("epoch after first recovery")
    );
    assert_eq!(
        token_before,
        store
            .current_state_token()
            .await
            .expect("token after first recovery")
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two path"))
            .await
            .is_err()
    );

    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .recover_restore(&restore_id)
            .await
            .expect("subsequent Ready recovery preserves operator repair")
            .status()
    );
    assert_eq!(
        repaired_bytes,
        storage
            .get_raw(&journal_path)
            .await
            .expect("journal after second recovery")
    );
    assert_eq!(
        epoch_before,
        storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("epoch after second recovery")
    );
}

#[tokio::test]
async fn workspace_restore_recovery_direct_visible_helper_settles_owner_epoch_and_terminal_is_read_only()
 {
    let memory = Arc::new(MemoryBackend::new());
    let signals = Arc::new(VisibleReceiptRaceSignals::default());
    let backend = Arc::new(VisibleReceiptRaceBackend::new(
        memory.clone(),
        "catalog",
        signals.clone(),
    ));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(237_u128));
    let pin_id = format!("pin_{}", Ulid::from(238_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;

    let binding = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "catalog"),
        store.clone(),
        store.clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("binding")
    .with_restore_participant(Arc::new(PauseHelperDirectInspectionParticipant {
        inner: ControlMvpRestoreParticipant::new(store.as_ref().clone()),
        signals: signals.clone(),
        inspections: AtomicUsize::new(0),
    }))
    .expect("restore participant");
    let service = Arc::new(
        WorkspaceRestoreService::new(
            storage.clone(),
            WorkspaceDomainRegistry::new(
                WorkspaceScope::new("tenant", "workspace").expect("scope"),
                vec![binding],
            )
            .expect("registry"),
        )
        .expect("restore service"),
    );
    let restore_id = format!("rst_{}", Ulid::from(239_u128));
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");

    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("setup inspection crash leaves durable repair state")
            .status()
    );
    backend.arm();
    let helper_service = service.clone();
    let helper_restore_id = restore_id.clone();
    let helper =
        tokio::spawn(async move { helper_service.recover_restore(&helper_restore_id).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        signals.helper_direct_inspect.notified(),
    )
    .await
    .expect("helper reached direct inspection before pointer publication");
    let owner_service = service.clone();
    let owner_restore_id = restore_id.clone();
    let owner = tokio::spawn(async move { owner_service.recover_restore(&owner_restore_id).await });
    let (owner_result, helper_result) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(owner, helper)
    })
    .await
    .expect("owner and helper converge");
    assert!(
        owner_result.expect("owner task").is_err(),
        "owner receipt uncertainty must not be reported as success"
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        helper_result
            .expect("helper task")
            .expect("helper adopts direct Visible receipt")
            .status()
    );
    assert_eq!(1, backend.pointer_writes());
    assert_eq!("IDLE", retention_epoch(&storage).await["state"]);
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        arco_catalog::ArcoStateReader::get(store.as_ref(), b"catalog/default")
            .await
            .expect("visible restored value")
    );

    let later_snapshot_id = format!("snap_{}", Ulid::from(240_u128));
    let later_pin_id = format!("pin_{}", Ulid::from(241_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("later snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &later_snapshot_id,
                &later_pin_id,
                Utc::now(),
                Utc::now() + ChronoDuration::hours(1),
                None,
            )
            .expect("later snapshot request"),
        )
        .await
        .expect("a later coordinated publication succeeds after terminal settlement");
    assert_eq!("IDLE", retention_epoch(&storage).await["state"]);

    let terminal_service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), false))
            .expect("terminal service without restore adapter");
    let idle_before = all_workspace_bytes(memory.as_ref()).await;
    backend.start_write_audit();
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        terminal_service
            .recover_restore(&restore_id)
            .await
            .expect("terminal recovery without adapters")
            .status()
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        terminal_service
            .get_restore(&restore_id)
            .await
            .expect("terminal read without adapters")
            .status()
    );
    assert!(
        backend.stop_write_audit().is_empty(),
        "an idle terminal recover/get must perform no writes"
    );
    assert_eq!(idle_before, all_workspace_bytes(memory.as_ref()).await);

    let mut foreign_epoch = retention_epoch(&storage).await;
    foreign_epoch["state"] = serde_json::Value::String("IN_FLIGHT".to_string());
    foreign_epoch["holder_id"] = serde_json::Value::String("foreign-gc-holder".to_string());
    foreign_epoch["operation_kind"] = serde_json::Value::String("catalog_gc".to_string());
    foreign_epoch["operation_id"] = serde_json::Value::String("foreign-gc-operation".to_string());
    foreign_epoch["completed_at"] = serde_json::Value::Null;
    storage
        .put_raw(
            "retention/coordination/mutation-epoch.json",
            Bytes::from(serde_jcs::to_vec(&foreign_epoch).expect("foreign epoch bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("install unrelated in-flight epoch");
    let foreign_before = all_workspace_bytes(memory.as_ref()).await;
    backend.start_write_audit();
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        terminal_service
            .recover_restore(&restore_id)
            .await
            .expect("terminal recovery ignores unrelated epoch")
            .status()
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        terminal_service
            .get_restore(&restore_id)
            .await
            .expect("terminal read ignores unrelated epoch")
            .status()
    );
    assert!(
        backend.stop_write_audit().is_empty(),
        "terminal reads must not acquire a lock for an unrelated epoch"
    );
    assert_eq!(foreign_before, all_workspace_bytes(memory.as_ref()).await);
}

#[tokio::test]
async fn workspace_restore_recovery_replacement_pin_race_writes_no_attempt_or_journal_revision() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "b"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(226_u128));
    let pin_id = format!("pin_{}", Ulid::from(227_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let restore_id = format!("rst_{}", Ulid::from(228_u128));
    let ordinary_service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], true),
    )
    .expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    backend.arm();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        ordinary_service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("later-domain pointer failure is repairable")
            .status()
    );
    let attempt_two_path = restore_attempt_plan_path(&restore_id, 2).expect("attempt two path");
    assert!(storage.get_raw(&attempt_two_path).await.is_err());
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let journal_before = storage
        .get_raw(&journal_path)
        .await
        .expect("journal before");
    let journal_version_before = storage
        .head_raw(&journal_path)
        .await
        .expect("journal metadata before")
        .expect("journal before")
        .version;
    let a_before = stores[0]
        .current_state_token()
        .await
        .expect("a state token before");
    let b_before = stores[1]
        .current_state_token()
        .await
        .expect("b state token before");

    let first = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "a"),
        stores[0].clone(),
        stores[0].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("first binding")
    .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
        stores[0].as_ref().clone(),
    )))
    .expect("first restore participant");
    let second = WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", "b"),
        stores[1].clone(),
        stores[1].clone(),
        Arc::new(UnusedProjectionProvider),
        Arc::new(UnusedArchiveProvider),
    )
    .expect("second binding")
    .with_restore_participant(Arc::new(ReleasePinDuringPlanParticipant::on_attempt(
        ControlMvpRestoreParticipant::new(stores[1].as_ref().clone()),
        storage.clone(),
        &pin_id,
        2,
    )))
    .expect("racing replacement participant");
    let racing_service = WorkspaceRestoreService::new(
        storage.clone(),
        WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            vec![first, second],
        )
        .expect("registry"),
    )
    .expect("racing service");

    assert!(
        racing_service.recover_restore(&restore_id).await.is_err(),
        "source release after replacement planning must abort before replacement publication"
    );
    assert!(
        storage.get_raw(&attempt_two_path).await.is_err(),
        "a replacement whose post-plan source fence fails must publish no attempt bytes"
    );
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after"),
        "replacement prepublication failure must preserve exact journal bytes"
    );
    assert_eq!(
        journal_version_before,
        storage
            .head_raw(&journal_path)
            .await
            .expect("journal metadata after")
            .expect("journal after")
            .version,
        "replacement prepublication failure must not create a journal revision"
    );
    assert_eq!(
        a_before,
        stores[0]
            .current_state_token()
            .await
            .expect("a state token after")
    );
    assert_eq!(
        b_before,
        stores[1]
            .current_state_token()
            .await
            .expect("b state token after")
    );
}

#[tokio::test]
async fn orphan_replacement_attempt_is_adopted_then_advanced_without_changing_its_bytes() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(CrashAfterReplacementPlanBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(203_u128));
    let pin_id = format!("pin_{}", Ulid::from(204_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(205_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm_pointer();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("initial pointer failure is durable")
            .status()
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt path"))
            .await
            .is_err(),
        "attempt two must not predate durable CAS_LOST"
    );
    assert!(
        service.recover_restore(&restore_id).await.is_err(),
        "attempt two must be orphaned by the injected pre-journal crash"
    );
    let attempt_two_path = restore_attempt_plan_path(&restore_id, 2).expect("attempt two path");
    let frozen_attempt_two = storage
        .get_raw(&attempt_two_path)
        .await
        .expect("orphan attempt two");
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal bytes"),
    )
    .expect("journal json");
    assert_eq!(1, journal["aggregate_attempt"]);

    committed_value(&store, b"foreign-two").await;
    let repaired_orphan = service
        .recover_restore(&restore_id)
        .await
        .expect("adopt stale orphan and durably mark it superseded");
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        repaired_orphan.status()
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 3).expect("attempt three path"))
            .await
            .is_err(),
        "one helper invocation may advance at most one aggregate attempt"
    );
    let visible = service
        .recover_restore(&restore_id)
        .await
        .expect("advance from the selected stale orphan on the next invocation");
    assert_eq!(WorkspaceRestoreStatus::Visible, visible.status());
    assert_eq!(
        frozen_attempt_two,
        storage
            .get_raw(&attempt_two_path)
            .await
            .expect("attempt two after recovery"),
        "orphan adoption must preserve exact immutable bytes"
    );
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 3).expect("attempt three path"))
            .await
            .is_ok(),
        "a selected but superseded orphan must advance through a new attempt"
    );
}

#[tokio::test]
async fn zero_receipt_supersession_is_durable_even_after_source_release() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(SupersedeNextDomainPointerBackend::new(inner, "catalog"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(160_u128));
    let pin_id = format!("pin_{}", Ulid::from(161_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(162_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id.clone()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert_eq!(
        WorkspaceRestoreStatus::RepairRequired,
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .expect("pointer failure is durably repairable")
            .status()
    );
    committed_value(&store, b"foreign").await;
    release_retention_pin(&storage, &pin_id).await;

    let repair = service
        .recover_restore(&restore_id)
        .await
        .expect("persist proven supersession before active-source preflight");
    assert_eq!(WorkspaceRestoreStatus::RepairRequired, repair.status());
    let journal: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&restore_journal_path(&restore_id).expect("journal path"))
            .await
            .expect("journal bytes"),
    )
    .expect("journal json");
    assert_eq!("CAS_LOST", journal["failure_category"]);
    assert!(
        storage
            .get_raw(&restore_attempt_plan_path(&restore_id, 2).expect("attempt two path"))
            .await
            .is_err(),
        "released source cannot create attempt two"
    );
}

#[tokio::test]
async fn omitted_domain_policy_requires_explicit_omission_and_supports_domain_only_target() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let catalog = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("catalog store"),
    );
    let other = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "other"),
        )
        .expect("other store"),
    );
    committed_value(&catalog, b"v1").await;
    committed_value(&other, b"other").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(60_u128));
    let pin_id = format!("pin_{}", Ulid::from(61_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(catalog.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&catalog, b"v2").await;
    let stores = vec![catalog.clone(), other.clone()];
    let service = WorkspaceRestoreService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["catalog", "other"], true),
    )
    .expect("restore service");

    let reject_id = format!("rst_{}", Ulid::from(62_u128));
    let reject = RestoreWorkspaceToSnapshot::new(
        &reject_id,
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("reject request");
    assert!(
        service
            .restore_workspace_to_snapshot(&reject)
            .await
            .is_err()
    );
    assert!(
        storage
            .get_raw(&restore_request_path(&reject_id).expect("request path"))
            .await
            .is_err(),
        "failed omission preflight must write nothing"
    );

    let omit = RestoreWorkspaceToSnapshot::new(
        format!("rst_{}", Ulid::from(63_u128)),
        RestoreSource::snapshot(&snapshot_id, &pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Omit,
    )
    .expect("omit request");
    let omitted = service
        .restore_workspace_to_snapshot(&omit)
        .await
        .expect("omit restore");
    assert_eq!(WorkspaceRestoreStatus::Visible, omitted.status());
    assert_eq!(&["other".to_string()], omitted.omitted_domains());
    let manifest = omitted.read_manifest().expect("omit read manifest");
    assert_eq!(&["other".to_string()], manifest.omitted_domains());
    assert_eq!(1, manifest.participants().len());
    assert_eq!("catalog", manifest.participants()[0].domain());
    assert_eq!(
        Some(Bytes::from_static(b"other")),
        arco_catalog::ArcoStateReader::get(other.as_ref(), b"catalog/default")
            .await
            .expect("omitted domain unchanged")
    );

    let domain = RestoreDomainToSnapshot::new(
        format!("rst_{}", Ulid::from(64_u128)),
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        "catalog",
    )
    .expect("domain request");
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_domain_to_snapshot(&domain)
            .await
            .expect("domain restore")
            .status()
    );
}

#[tokio::test]
async fn omitted_domain_policy_domain_only_configures_only_the_named_source_domain() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let stores = ["a", "b"]
        .into_iter()
        .map(|domain| {
            Arc::new(
                ControlMvpStateStore::new(
                    storage.clone(),
                    StateScope::new("tenant", "workspace", domain),
                )
                .expect("store"),
            )
        })
        .collect::<Vec<_>>();
    for store in &stores {
        committed_value(store, b"v1").await;
    }
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(223_u128));
    let pin_id = format!("pin_{}", Ulid::from(224_u128));
    WorkspaceSnapshotService::new(
        storage.clone(),
        multi_domain_registry(&stores, &["a", "b"], false),
    )
    .expect("snapshot service")
    .create_snapshot(
        &CreateWorkspaceSnapshotRequest::new(
            &snapshot_id,
            &pin_id,
            now,
            now + ChronoDuration::hours(1),
            None,
        )
        .expect("snapshot request"),
    )
    .await
    .expect("snapshot");
    for store in &stores {
        committed_value(store, b"v2").await;
    }

    let service =
        WorkspaceRestoreService::new(storage, multi_domain_registry(&stores[..1], &["a"], true))
            .expect("domain-only service");
    let request = RestoreDomainToSnapshot::new(
        format!("rst_{}", Ulid::from(225_u128)),
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        "a",
    )
    .expect("domain request");
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .restore_domain_to_snapshot(&request)
            .await
            .expect("restore only configured named source domain")
            .status()
    );
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        arco_catalog::ArcoStateReader::get(stores[0].as_ref(), b"catalog/default")
            .await
            .expect("a restored")
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        arco_catalog::ArcoStateReader::get(stores[1].as_ref(), b"catalog/default")
            .await
            .expect("unconfigured b untouched")
    );

    for (offset, policy) in [OmittedDomainPolicy::Reject, OmittedDomainPolicy::Omit]
        .into_iter()
        .enumerate()
    {
        let restore_id = format!("rst_{}", Ulid::from(240_u128 + offset as u128));
        let workspace = RestoreWorkspaceToSnapshot::new(
            &restore_id,
            RestoreSource::snapshot(
                format!("snap_{}", Ulid::from(223_u128)),
                format!("pin_{}", Ulid::from(224_u128)),
            )
            .expect("source"),
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            now,
            policy,
        )
        .expect("workspace request");
        assert!(
            service
                .restore_workspace_to_snapshot(&workspace)
                .await
                .is_err(),
            "workspace restore cannot omit a source domain absent from configuration"
        );
        assert!(
            service.get_restore(&restore_id).await.is_err(),
            "failed policy preflight must publish no journal"
        );
    }
}

#[tokio::test]
async fn workspace_restore_finalizing_recovery_uses_receipts_not_source() {
    let snapshot_id = format!("snap_{}", Ulid::from(70_u128));
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FinalManifestCrashBackend::new(inner, &snapshot_id));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let pin_id = format!("pin_{}", Ulid::from(71_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(72_u128));
    let service =
        WorkspaceRestoreService::new(storage, domain_registry(store, true)).expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err()
    );
    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .recover_restore(&restore_id)
            .await
            .expect("finalize from durable receipts")
            .status()
    );
}

#[tokio::test]
async fn recovery_adopts_manifest_written_before_terminal_journal_cas() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(CrashAfterFinalManifestBackend::new(inner));
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", Ulid::from(180_u128));
    let pin_id = format!("pin_{}", Ulid::from(181_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(182_u128));
    let service = WorkspaceRestoreService::new(storage.clone(), domain_registry(store, true))
        .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id.clone()).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err(),
        "crash occurs after immutable final manifest but before VISIBLE journal"
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let finalizing: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&journal_path).await.expect("journal bytes"))
            .expect("journal json");
    assert_eq!("FINALIZING", finalizing["status"]);
    assert!(
        storage
            .get_raw(&restore_read_manifest_path(&restore_id).expect("manifest path"))
            .await
            .is_ok()
    );
    release_retention_pin(&storage, &pin_id).await;

    assert_eq!(
        WorkspaceRestoreStatus::Visible,
        service
            .recover_restore(&restore_id)
            .await
            .expect("adopt exact manifest after source release")
            .status()
    );
}

#[tokio::test]
async fn final_read_manifest_rejects_a_corrupt_frozen_manifest_digest() {
    let snapshot_id = format!("snap_{}", Ulid::from(80_u128));
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FinalManifestCrashBackend::new(inner, &snapshot_id));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let pin_id = format!("pin_{}", Ulid::from(81_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(82_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), domain_registry(store.clone(), true))
            .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err()
    );

    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let mut journal: serde_json::Value =
        serde_json::from_slice(&storage.get_raw(&journal_path).await.expect("journal bytes"))
            .expect("journal json");
    assert_eq!("FINALIZING", journal["status"]);
    journal["read_manifest_sha256"] =
        serde_json::Value::String(format!("sha256:{}", "f".repeat(64)));
    storage
        .put_raw(
            &journal_path,
            Bytes::from(serde_jcs::to_vec(&journal).expect("corrupt journal bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt frozen digest");

    assert!(
        service.recover_restore(&restore_id).await.is_err(),
        "recovery must reject a frozen digest that differs from reconstructed bytes"
    );
    let durable: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&journal_path)
            .await
            .expect("durable journal"),
    )
    .expect("durable json");
    assert_eq!(
        "FINALIZING", durable["status"],
        "corrupt frozen finalization must never become terminal"
    );
    assert!(
        storage
            .get_raw(&restore_read_manifest_path(&restore_id).expect("manifest path"))
            .await
            .is_err(),
        "a mismatched frozen digest must be rejected before immutable publication"
    );
}

#[tokio::test]
async fn final_read_manifest_conflicting_immutable_bytes_fail_closed() {
    let snapshot_id = format!("snap_{}", Ulid::from(229_u128));
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FinalManifestCrashBackend::new(inner, &snapshot_id));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store"),
    );
    committed_value(&store, b"v1").await;
    let now = Utc::now();
    let pin_id = format!("pin_{}", Ulid::from(230_u128));
    WorkspaceSnapshotService::new(storage.clone(), domain_registry(store.clone(), false))
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::hours(1),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("snapshot");
    committed_value(&store, b"v2").await;
    let restore_id = format!("rst_{}", Ulid::from(231_u128));
    let service = WorkspaceRestoreService::new(storage.clone(), domain_registry(store, true))
        .expect("service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("request");
    backend.arm();
    assert!(
        service
            .restore_workspace_to_snapshot(&request)
            .await
            .is_err()
    );
    let journal_path = restore_journal_path(&restore_id).expect("journal path");
    let journal_before = storage.get_raw(&journal_path).await.expect("journal bytes");
    let journal: serde_json::Value = serde_json::from_slice(&journal_before).expect("journal JSON");
    assert_eq!("FINALIZING", journal["status"]);

    let manifest_path = restore_read_manifest_path(&restore_id).expect("manifest path");
    let conflict = Bytes::from_static(br#"{"conflicting":"immutable winner"}"#);
    storage
        .put_raw(
            &manifest_path,
            conflict.clone(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed conflicting immutable winner");
    assert!(service.recover_restore(&restore_id).await.is_err());
    assert_eq!(
        journal_before,
        storage.get_raw(&journal_path).await.expect("journal after")
    );
    assert_eq!(
        conflict,
        storage
            .get_raw(&manifest_path)
            .await
            .expect("conflicting winner remains")
    );
}
