//! Contract tests for direct-addressed workspace snapshot and export services.

use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone as _, Utc};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;

use arco_catalog::state_store::{PersistedAuthorityKind, PersistedAuthorityReference, StateScope};
use arco_catalog::workspace_snapshot::{
    ChecksumReference, DomainAuthorityReference, DomainEventArchive, ExportManifest,
    LegacyCompatibilityArtifact, ProjectionWatermark, RelocationPolicy, RequiredObject,
    RequiredObjectKind, RetentionPinLatest, RetentionPinRevision, RetentionTarget, WorkspaceScope,
    decode_retention_pin_latest, decode_retention_pin_revision, encode_export_manifest,
    encode_retention_pin_latest, encode_retention_pin_revision, export_record_path,
    retention_pin_latest_path, retention_pin_revision_path, snapshot_record_path,
};
use arco_catalog::workspace_snapshot_service::{
    CreateWorkspaceExportRequest, CreateWorkspaceSnapshotRequest, EventArchiveCapture,
    EventArchiveProvider, ProjectionWatermarkCut, ProjectionWatermarkProvider,
    RestorePreflightIssueKind, RestoreSource, WorkspaceDomainBinding, WorkspaceDomainRegistry,
    WorkspaceSnapshotService,
};
use arco_catalog::{
    ArcoStateAdmin as _, ArcoStateReader, ArcoStateTxn as _, CheckpointToken, ControlMvpStateStore,
    CurrentStateStore, PersistedAuthorityAdapter, Result, StateToken, Tier1Writer, TxnOptions,
};
use arco_core::error::Result as StorageResult;
use arco_core::lock::LockInfo;
use arco_core::{
    MemoryBackend, ObjectMeta, ScopedStorage, StorageBackend, WritePrecondition, WriteResult,
};

use arco_catalog::gc::{GarbageCollector, RetentionPolicy};

const SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const EXPORT_ID: &str = "exp_01ARZ3NDEKTSV4RRFFQ69G5FAW";
const PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAX";
const EXPORT_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAY";
const ALT_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAZ";
const ALT_EXPORT_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FB0";
const ALT_SOURCE_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FB1";
const DIGEST: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn ts(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BackendOperation {
    Get(String),
    Put(String),
    Delete(String),
    List(String),
    Head(String),
}

#[derive(Debug, Default)]
struct RecordingBackend {
    inner: MemoryBackend,
    operations: Mutex<Vec<BackendOperation>>,
    collision: Mutex<Option<CollisionPlan>>,
    deny_list: AtomicBool,
    get_failure_suffix: Mutex<Option<String>>,
    put_failure_suffix: Mutex<Option<String>>,
    put_pause_suffix: Mutex<Option<String>>,
    put_reached: Notify,
    resume_put: Notify,
    delete_failure_suffix: Mutex<Option<String>>,
    delete_pause: Mutex<Option<DeletePausePlan>>,
    delete_reached: Notify,
    resume_delete: Notify,
    stale_protected_deletes: AtomicUsize,
    take_over_lease_before_renewal: AtomicBool,
}

#[derive(Debug)]
struct CollisionPlan {
    path_suffix: String,
    identical: bool,
}

#[derive(Debug)]
struct DeletePausePlan {
    path_suffix: String,
    protected_selector_path: String,
}

impl RecordingBackend {
    fn clear(&self) {
        self.operations.lock().expect("operations").clear();
    }

    fn operations(&self) -> Vec<BackendOperation> {
        self.operations.lock().expect("operations").clone()
    }

    fn record(&self, operation: BackendOperation) {
        self.operations.lock().expect("operations").push(operation);
    }

    fn arm_collision(&self, path_suffix: impl Into<String>, identical: bool) {
        *self.collision.lock().expect("collision") = Some(CollisionPlan {
            path_suffix: path_suffix.into(),
            identical,
        });
    }

    fn deny_lists(&self) {
        self.deny_list.store(true, Ordering::SeqCst);
    }

    fn fail_get(&self, path_suffix: impl Into<String>) {
        *self.get_failure_suffix.lock().expect("get failure") = Some(path_suffix.into());
    }

    fn fail_put(&self, path_suffix: impl Into<String>) {
        *self.put_failure_suffix.lock().expect("put failure") = Some(path_suffix.into());
    }

    fn pause_put(&self, path_suffix: impl Into<String>) {
        *self.put_pause_suffix.lock().expect("put pause") = Some(path_suffix.into());
    }

    async fn wait_for_paused_put(&self) {
        self.put_reached.notified().await;
    }

    fn resume_paused_put(&self) {
        self.resume_put.notify_one();
    }

    fn fail_delete(&self, path_suffix: impl Into<String>) {
        *self.delete_failure_suffix.lock().expect("delete failure") = Some(path_suffix.into());
    }

    async fn expire_lock(&self, scoped_path: &str) {
        let current = self.inner.get(scoped_path).await.expect("current lock");
        let mut current: LockInfo = serde_json::from_slice(&current).expect("lock JSON");
        current.expires_at = Utc::now() - chrono::Duration::seconds(1);
        self.inner
            .put(
                scoped_path,
                Bytes::from(serde_json::to_vec(&current).expect("expired lock JSON")),
                WritePrecondition::None,
            )
            .await
            .expect("expire lock");
    }

    fn pause_delete(
        &self,
        path_suffix: impl Into<String>,
        protected_selector_path: impl Into<String>,
    ) {
        *self.delete_pause.lock().expect("delete pause") = Some(DeletePausePlan {
            path_suffix: path_suffix.into(),
            protected_selector_path: protected_selector_path.into(),
        });
    }

    async fn wait_for_paused_delete(&self) {
        self.delete_reached.notified().await;
    }

    fn resume_paused_delete(&self) {
        self.resume_delete.notify_one();
    }

    fn stale_protected_deletes(&self) -> usize {
        self.stale_protected_deletes.load(Ordering::SeqCst)
    }

    fn take_over_lease_before_renewal(&self) {
        self.take_over_lease_before_renewal
            .store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for RecordingBackend {
    async fn get(&self, path: &str) -> StorageResult<Bytes> {
        self.record(BackendOperation::Get(path.to_string()));
        if self
            .get_failure_suffix
            .lock()
            .expect("get failure")
            .as_ref()
            .is_some_and(|suffix| path.ends_with(suffix))
        {
            return Err(arco_core::Error::storage(
                "provider permission denied for secret backend URI",
            ));
        }
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> StorageResult<Bytes> {
        self.record(BackendOperation::Get(path.to_string()));
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> StorageResult<WriteResult> {
        self.record(BackendOperation::Put(path.to_string()));
        if self
            .put_failure_suffix
            .lock()
            .expect("put failure")
            .as_ref()
            .is_some_and(|suffix| path.ends_with(suffix))
        {
            return Err(arco_core::Error::storage("injected lock write failure"));
        }
        if path.ends_with("locks/workspace-retention-gc.lock.json")
            && matches!(&precondition, WritePrecondition::MatchesVersion(_))
            && self
                .take_over_lease_before_renewal
                .swap(false, Ordering::SeqCst)
        {
            let current = self.inner.get(path).await?;
            let current: LockInfo = serde_json::from_slice(&current).expect("current lock info");
            let takeover = LockInfo {
                holder_id: "deterministic-takeover".to_string(),
                expires_at: Utc::now() + chrono::Duration::minutes(5),
                acquired_at: Utc::now(),
                sequence_number: current.sequence_number.saturating_add(1),
                operation: Some("lease-loss-test".to_string()),
            };
            self.inner
                .put(
                    path,
                    Bytes::from(serde_json::to_vec(&takeover).expect("takeover lock JSON")),
                    WritePrecondition::None,
                )
                .await?;
        }
        let collision = {
            let mut plan = self.collision.lock().expect("collision");
            if plan
                .as_ref()
                .is_some_and(|plan| path.ends_with(&plan.path_suffix))
                && matches!(&precondition, WritePrecondition::DoesNotExist)
            {
                plan.take()
            } else {
                None
            }
        };
        if let Some(collision) = collision {
            let winner = if collision.identical {
                data.clone()
            } else {
                Bytes::from_static(b"conflicting immutable winner")
            };
            let result = self
                .inner
                .put(path, winner, WritePrecondition::DoesNotExist)
                .await?;
            let WriteResult::Success { version } = result else {
                panic!("collision winner must be the first write");
            };
            return Ok(WriteResult::PreconditionFailed {
                current_version: version,
            });
        }
        let pause = {
            let mut pause = self.put_pause_suffix.lock().expect("put pause");
            if pause.as_ref().is_some_and(|suffix| path.ends_with(suffix)) {
                pause.take()
            } else {
                None
            }
        };
        if pause.is_some() {
            self.put_reached.notify_one();
            self.resume_put.notified().await;
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> StorageResult<()> {
        self.record(BackendOperation::Delete(path.to_string()));
        let pause = {
            let mut pause = self.delete_pause.lock().expect("delete pause");
            if pause
                .as_ref()
                .is_some_and(|plan| path.ends_with(&plan.path_suffix))
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            self.delete_reached.notify_one();
            self.resume_delete.notified().await;
            if self
                .inner
                .head(&pause.protected_selector_path)
                .await?
                .is_some()
            {
                self.stale_protected_deletes.fetch_add(1, Ordering::SeqCst);
            }
        }
        if self
            .delete_failure_suffix
            .lock()
            .expect("delete failure")
            .as_ref()
            .is_some_and(|suffix| path.ends_with(suffix))
        {
            return Err(arco_core::Error::InvalidInput(
                "injected uncertain delete failure".to_string(),
            ));
        }
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<ObjectMeta>> {
        self.record(BackendOperation::List(prefix.to_string()));
        if self.deny_list.load(Ordering::SeqCst) {
            return Err(arco_core::Error::InvalidInput("list denied".to_string()));
        }
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> StorageResult<Option<ObjectMeta>> {
        self.record(BackendOperation::Head(path.to_string()));
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> StorageResult<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn export_manifest() -> ExportManifest {
    let scope = WorkspaceScope::new("tenant", "workspace").expect("scope");
    let authority = PersistedAuthorityReference::new(
        "arco-state-control-mvp",
        StateScope::new("tenant", "workspace", "catalog"),
        PersistedAuthorityKind::Checkpoint,
        "manifest-7",
        7,
        "state-store/control-mvp/catalog/manifests/manifest-7.json",
        DIGEST,
        Some("state-store/control-mvp/catalog/checkpoints/checkpoint-7.json".to_string()),
        Some(DIGEST.to_string()),
        ts(2_000_000_000),
    )
    .expect("authority");

    ExportManifest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        scope.clone(),
        ts(1_800_000_000),
        ts(1_900_000_000),
        vec![
            DomainAuthorityReference::new("catalog", scope.clone(), authority)
                .expect("domain authority"),
        ],
        Vec::new(),
        vec![DomainEventArchive::empty("catalog").expect("archive")],
        vec![
            RequiredObject::new(
                snapshot_record_path(SNAPSHOT_ID).expect("snapshot path"),
                1,
                RequiredObjectKind::SnapshotRecord,
                DIGEST,
            )
            .expect("source snapshot record"),
        ],
        Vec::new(),
        RelocationPolicy::relative_to_caller_export_root(),
    )
    .expect("export")
}

#[derive(Debug)]
struct EmptyProjectionProvider;

#[async_trait]
impl ProjectionWatermarkProvider for EmptyProjectionProvider {
    async fn capture(
        &self,
        _authority: &DomainAuthorityReference,
    ) -> Result<ProjectionWatermarkCut> {
        ProjectionWatermarkCut::new(Vec::new(), Vec::new(), Vec::new())
    }
}

#[derive(Debug)]
struct EmptyArchiveProvider;

#[async_trait]
impl EventArchiveProvider for EmptyArchiveProvider {
    async fn capture(&self, authority: &DomainAuthorityReference) -> Result<EventArchiveCapture> {
        EventArchiveCapture::new(DomainEventArchive::empty(authority.domain())?, Vec::new())
    }
}

#[derive(Debug)]
struct FixedProjectionProvider {
    cut: ProjectionWatermarkCut,
}

#[derive(Debug)]
struct FailingProjectionProvider;

#[async_trait]
impl ProjectionWatermarkProvider for FailingProjectionProvider {
    async fn capture(
        &self,
        _authority: &DomainAuthorityReference,
    ) -> Result<ProjectionWatermarkCut> {
        Err(arco_catalog::CatalogError::UnsupportedOperation {
            message: "projection unavailable".to_string(),
        })
    }
}

#[async_trait]
impl ProjectionWatermarkProvider for FixedProjectionProvider {
    async fn capture(
        &self,
        _authority: &DomainAuthorityReference,
    ) -> Result<ProjectionWatermarkCut> {
        Ok(self.cut.clone())
    }
}

#[derive(Debug)]
struct FixedArchiveProvider {
    capture: EventArchiveCapture,
}

#[derive(Clone)]
struct MismatchedImplementationAdapter {
    inner: ControlMvpStateStore,
}

#[derive(Debug)]
struct FailOnResolveAdapter;

#[async_trait]
impl PersistedAuthorityAdapter for FailOnResolveAdapter {
    async fn persist_state_reference(
        &self,
        _token: &StateToken,
        _retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        Err(arco_catalog::CatalogError::Storage {
            message: "unexpected persist".to_string(),
        })
    }

    async fn persist_checkpoint_reference(
        &self,
        _token: &CheckpointToken,
        _retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        Err(arco_catalog::CatalogError::Storage {
            message: "unexpected persist".to_string(),
        })
    }

    async fn resolve_persisted_reference_at(
        &self,
        _reference: &PersistedAuthorityReference,
        _now: DateTime<Utc>,
    ) -> Result<Box<dyn ArcoStateReader>> {
        Err(arco_catalog::CatalogError::Storage {
            message: "mismatched adapter must not be resolved".to_string(),
        })
    }
}

#[async_trait]
impl PersistedAuthorityAdapter for MismatchedImplementationAdapter {
    async fn persist_state_reference(
        &self,
        token: &StateToken,
        retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        self.inner
            .persist_state_reference(token, retention_deadline)
            .await
    }

    async fn persist_checkpoint_reference(
        &self,
        token: &CheckpointToken,
        retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        let reference = self
            .inner
            .persist_checkpoint_reference(token, retention_deadline)
            .await?;
        PersistedAuthorityReference::new(
            "mismatched-implementation",
            reference.scope().clone(),
            reference.reference_kind(),
            reference.manifest_id(),
            reference.logical_sequence(),
            reference.manifest_path(),
            reference.manifest_sha256(),
            reference.checkpoint_path().map(ToOwned::to_owned),
            reference.checkpoint_sha256().map(ToOwned::to_owned),
            reference.retention_deadline(),
        )
    }

    async fn resolve_persisted_reference_at(
        &self,
        reference: &PersistedAuthorityReference,
        now: DateTime<Utc>,
    ) -> Result<Box<dyn ArcoStateReader>> {
        self.inner
            .resolve_persisted_reference_at(reference, now)
            .await
    }
}

#[async_trait]
impl EventArchiveProvider for FixedArchiveProvider {
    async fn capture(&self, _authority: &DomainAuthorityReference) -> Result<EventArchiveCapture> {
        Ok(self.capture.clone())
    }
}

async fn put_object(storage: &ScopedStorage, path: &str, bytes: &[u8]) {
    let result = storage
        .put_raw(
            path,
            Bytes::copy_from_slice(bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("put object");
    assert!(matches!(result, WriteResult::Success { .. }));
}

async fn mutate_json(storage: &ScopedStorage, path: &str, mutate: impl FnOnce(&mut Value)) {
    let bytes = storage.get_raw(path).await.expect("read JSON");
    let mut value: Value = serde_json::from_slice(&bytes).expect("decode JSON");
    mutate(&mut value);
    storage
        .put_raw(
            path,
            Bytes::from(serde_json::to_vec(&value).expect("encode JSON")),
            WritePrecondition::None,
        )
        .await
        .expect("overwrite JSON");
}

async fn select_pin_revision(storage: &ScopedStorage, revision: &RetentionPinRevision) {
    let revision_bytes = encode_retention_pin_revision(revision).expect("encode pin revision");
    let revision_path =
        retention_pin_revision_path(revision.pin_id(), revision.revision()).expect("revision path");
    storage
        .put_raw(
            &revision_path,
            Bytes::copy_from_slice(&revision_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("write selected pin revision");
    let selector = RetentionPinLatest::new(
        revision.pin_id(),
        revision.revision(),
        &revision_path,
        sha256(&revision_bytes),
    )
    .expect("latest selector");
    storage
        .put_raw(
            &retention_pin_latest_path(revision.pin_id()).expect("latest path"),
            Bytes::from(encode_retention_pin_latest(&selector).expect("encode selector")),
            WritePrecondition::None,
        )
        .await
        .expect("select pin revision");
}

async fn read_pin_revision(
    storage: &ScopedStorage,
    pin_id: &str,
    revision: u64,
) -> RetentionPinRevision {
    let bytes = storage
        .get_raw(&retention_pin_revision_path(pin_id, revision).expect("revision path"))
        .await
        .expect("read pin revision");
    decode_retention_pin_revision(&bytes).expect("decode pin revision")
}

async fn initialized_control_store(storage: &ScopedStorage, domain: &str) -> ControlMvpStateStore {
    let scope = StateScope::new("tenant", "workspace", domain);
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("store");
    let mut txn = store
        .begin_control_txn(TxnOptions::new(Some(scope)))
        .await
        .expect("begin transaction");
    txn.put(b"seed", Bytes::from_static(b"value"))
        .await
        .expect("seed write");
    txn.commit().await.expect("seed commit");
    store
}

async fn service_fixture(
    domains: &[&str],
) -> (
    Arc<RecordingBackend>,
    ScopedStorage,
    WorkspaceDomainRegistry,
) {
    let backend = Arc::new(RecordingBackend::default());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let mut bindings = Vec::new();

    for domain in domains {
        let store = Arc::new(initialized_control_store(&storage, domain).await);
        let projection_path = format!("projections/{domain}/manifest.json");
        let projection_bytes = format!("projection-{domain}").into_bytes();
        put_object(&storage, &projection_path, &projection_bytes).await;
        let projection_digest = sha256(&projection_bytes);

        let archive_path = format!("archives/{domain}/manifest.json");
        let archive_bytes = format!("archive-{domain}").into_bytes();
        put_object(&storage, &archive_path, &archive_bytes).await;
        let archive_digest = sha256(&archive_bytes);

        let compatibility_path = format!("legacy/{domain}/old.json");
        let compatibility_bytes = format!("compatibility-{domain}").into_bytes();
        put_object(&storage, &compatibility_path, &compatibility_bytes).await;
        let compatibility_digest = sha256(&compatibility_bytes);

        let projection_cut = ProjectionWatermarkCut::new(
            vec![
                ProjectionWatermark::new(
                    format!("{domain}-projection"),
                    *domain,
                    1,
                    ChecksumReference::new(&projection_path, &projection_digest)
                        .expect("projection reference"),
                )
                .expect("projection watermark"),
            ],
            vec![
                RequiredObject::new(
                    &projection_path,
                    projection_bytes.len() as u64,
                    RequiredObjectKind::ProjectionManifest,
                    &projection_digest,
                )
                .expect("projection object"),
                RequiredObject::new(
                    &compatibility_path,
                    compatibility_bytes.len() as u64,
                    RequiredObjectKind::LegacyCompatibility,
                    &compatibility_digest,
                )
                .expect("compatibility object"),
            ],
            vec![
                LegacyCompatibilityArtifact::new(&compatibility_path, &compatibility_digest)
                    .expect("compatibility artifact"),
            ],
        )
        .expect("projection cut");
        let archive_capture = EventArchiveCapture::new(
            DomainEventArchive::inclusive(
                *domain,
                1,
                1,
                ChecksumReference::new(&archive_path, &archive_digest).expect("archive reference"),
            )
            .expect("archive"),
            vec![
                RequiredObject::new(
                    &archive_path,
                    archive_bytes.len() as u64,
                    RequiredObjectKind::EventArchiveManifest,
                    &archive_digest,
                )
                .expect("archive object"),
            ],
        )
        .expect("archive capture");

        bindings.push(
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", *domain),
                store.clone(),
                store,
                Arc::new(FixedProjectionProvider {
                    cut: projection_cut,
                }),
                Arc::new(FixedArchiveProvider {
                    capture: archive_capture,
                }),
            )
            .expect("binding"),
        );
    }

    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        bindings,
    )
    .expect("registry");
    backend.clear();
    (backend, storage, registry)
}

fn binding(domain: &str) -> Result<WorkspaceDomainBinding> {
    let current = Arc::new(CurrentStateStore::new());
    WorkspaceDomainBinding::new(
        StateScope::new("tenant", "workspace", domain),
        current.clone(),
        current,
        Arc::new(EmptyProjectionProvider),
        Arc::new(EmptyArchiveProvider),
    )
}

#[test]
fn paths_are_canonical_and_export_accessors_are_safe() {
    assert_eq!(
        snapshot_record_path(SNAPSHOT_ID).expect("snapshot path"),
        format!("retention/snapshots/{SNAPSHOT_ID}.json")
    );
    assert_eq!(
        export_record_path(EXPORT_ID).expect("export path"),
        format!("retention/exports/{EXPORT_ID}.json")
    );
    assert_eq!(
        retention_pin_latest_path(PIN_ID).expect("pin latest path"),
        format!("retention/pins/{PIN_ID}/latest.json")
    );
    assert_eq!(
        retention_pin_revision_path(PIN_ID, 1).expect("pin revision path"),
        format!("retention/pins/{PIN_ID}/revisions/1.json")
    );

    assert!(snapshot_record_path(EXPORT_ID).is_err());
    assert!(export_record_path(SNAPSHOT_ID).is_err());
    assert!(retention_pin_latest_path(SNAPSHOT_ID).is_err());
    assert!(retention_pin_revision_path(PIN_ID, 0).is_err());

    let export = export_manifest();
    assert_eq!(export.version(), 1);
    assert_eq!(export.scope().tenant_id(), "tenant");
    assert_eq!(export.scope().workspace_id(), "workspace");
    assert_eq!(export.created_at(), ts(1_800_000_000));
    assert_eq!(export.retained_until(), ts(1_900_000_000));
    assert_eq!(
        export.relocation(),
        RelocationPolicy::relative_to_caller_export_root()
    );
}

#[test]
fn registry_is_explicit_canonical_and_has_no_fallback_providers() {
    assert!(!CurrentStateStore::new().capabilities().checkpoints());

    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![
            binding("search").expect("search"),
            binding("catalog").expect("catalog"),
        ],
    )
    .expect("registry");

    assert_eq!(
        registry
            .domains()
            .map(|(domain, _)| domain)
            .collect::<Vec<_>>(),
        vec!["catalog", "search"]
    );
    assert_eq!(
        registry
            .get("catalog")
            .expect("catalog")
            .state_scope()
            .domain(),
        "catalog"
    );
    assert!(
        !registry
            .get("catalog")
            .expect("catalog")
            .capabilities()
            .checkpoints()
    );
}

#[test]
fn registry_rejects_empty_duplicate_invalid_or_mismatched_bindings() {
    let scope = WorkspaceScope::new("tenant", "workspace").expect("scope");
    assert!(WorkspaceDomainRegistry::new(scope.clone(), Vec::new()).is_err());
    assert!(
        WorkspaceDomainRegistry::new(
            scope.clone(),
            vec![
                binding("catalog").expect("first"),
                binding("catalog").expect("second")
            ],
        )
        .is_err()
    );
    assert!(binding("").is_err());

    let other_workspace = {
        let current = Arc::new(CurrentStateStore::new());
        WorkspaceDomainBinding::new(
            StateScope::new("tenant", "other", "catalog"),
            current.clone(),
            current,
            Arc::new(EmptyProjectionProvider),
            Arc::new(EmptyArchiveProvider),
        )
        .expect("binding")
    };
    assert!(WorkspaceDomainRegistry::new(scope, vec![other_workspace]).is_err());
}

#[tokio::test]
async fn create_snapshot_checkpoints_canonically_and_publishes_only_retention_roots() {
    let (backend, storage, registry) = service_fixture(&["search", "catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request");

    let snapshot = service
        .create_snapshot(&request)
        .await
        .expect("create snapshot");

    assert_eq!(
        snapshot
            .domains()
            .iter()
            .map(DomainAuthorityReference::domain)
            .collect::<Vec<_>>(),
        vec!["catalog", "search"]
    );
    assert_eq!(snapshot.projection_watermarks().len(), 2);
    assert_eq!(snapshot.event_archives().len(), 2);
    assert_eq!(snapshot.compatibility_artifacts().len(), 2);
    assert_eq!(snapshot.required_objects().len(), 10);

    let operations = backend.operations();
    let put_paths = operations
        .iter()
        .filter_map(|operation| match operation {
            BackendOperation::Put(path) => Some(path.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let checkpoint_paths = put_paths
        .iter()
        .copied()
        .filter(|path| path.contains("/checkpoints/"))
        .collect::<Vec<_>>();
    assert_eq!(checkpoint_paths.len(), 2);
    assert!(checkpoint_paths[0].contains("/control-mvp/catalog/"));
    assert!(checkpoint_paths[1].contains("/control-mvp/search/"));

    assert_eq!(
        put_paths
            .iter()
            .copied()
            .filter(|path| path.contains("/retention/"))
            .collect::<Vec<_>>(),
        vec![
            "tenant=tenant/workspace=workspace/retention/coordination/mutation-epoch.json"
                .to_string(),
            format!("tenant=tenant/workspace=workspace/retention/snapshots/{SNAPSHOT_ID}.json"),
            format!("tenant=tenant/workspace=workspace/retention/pins/{PIN_ID}/revisions/1.json"),
            format!("tenant=tenant/workspace=workspace/retention/pins/{PIN_ID}/latest.json"),
            "tenant=tenant/workspace=workspace/retention/coordination/mutation-epoch.json"
                .to_string(),
        ]
    );
    assert!(put_paths.iter().all(|path| {
        path.contains("/checkpoints/")
            || path.contains("/retention/")
            || path.ends_with("/locks/workspace-retention-gc.lock.json")
    }));
    assert!(put_paths.iter().all(|path| {
        !path.contains("snapshots.parquet")
            && !path.contains("manifest_root")
            && !path.contains("current.pointer")
            && !path.contains("/legacy/")
    }));
    assert!(
        operations
            .iter()
            .all(|operation| !matches!(operation, BackendOperation::List(_)))
    );
}

#[tokio::test]
async fn create_capability_denial_happens_before_the_first_checkpoint() {
    let backend = Arc::new(RecordingBackend::default());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let catalog = Arc::new(initialized_control_store(&storage, "catalog").await);
    let current = Arc::new(CurrentStateStore::new());
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", "catalog"),
                catalog.clone(),
                catalog,
                Arc::new(EmptyProjectionProvider),
                Arc::new(EmptyArchiveProvider),
            )
            .expect("catalog binding"),
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", "search"),
                current.clone(),
                current,
                Arc::new(EmptyProjectionProvider),
                Arc::new(EmptyArchiveProvider),
            )
            .expect("search binding"),
        ],
    )
    .expect("registry");
    backend.clear();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request");

    let error = service
        .create_snapshot(&request)
        .await
        .expect_err("capability denial");
    assert!(matches!(
        error,
        arco_catalog::CatalogError::UnsupportedOperation { .. }
    ));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/checkpoints/"))
    }));
}

#[tokio::test]
async fn create_adapter_mismatch_aborts_before_retention_publication() {
    let backend = Arc::new(RecordingBackend::default());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(initialized_control_store(&storage, "catalog").await);
    let adapter = Arc::new(MismatchedImplementationAdapter {
        inner: (*store).clone(),
    });
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", "catalog"),
                store,
                adapter,
                Arc::new(EmptyProjectionProvider),
                Arc::new(EmptyArchiveProvider),
            )
            .expect("binding"),
        ],
    )
    .expect("registry");
    backend.clear();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request");

    assert!(service.create_snapshot(&request).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains("/retention/snapshots/")
                || path.contains("/retention/exports/")
                || path.contains("/retention/pins/"))
    }));
}

#[tokio::test]
async fn create_provider_failure_leaves_retention_records_absent() {
    let backend = Arc::new(RecordingBackend::default());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = Arc::new(initialized_control_store(&storage, "catalog").await);
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", "catalog"),
                store.clone(),
                store,
                Arc::new(FailingProjectionProvider),
                Arc::new(EmptyArchiveProvider),
            )
            .expect("binding"),
        ],
    )
    .expect("registry");
    backend.clear();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request");

    assert!(service.create_snapshot(&request).await.is_err());
    let operations = backend.operations();
    assert!(operations.iter().any(|operation| {
        matches!(operation, BackendOperation::Put(path) if path.contains("/checkpoints/"))
    }));
    assert!(operations.iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains("/retention/snapshots/")
                || path.contains("/retention/exports/")
                || path.contains("/retention/pins/"))
    }));
}

#[tokio::test]
async fn create_same_id_retry_is_checkpoint_free_and_semantic_conflicts_fail() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request");
    let first = service
        .create_snapshot(&request)
        .await
        .expect("first create");

    backend.clear();
    let retried = service
        .create_snapshot(&request)
        .await
        .expect("idempotent retry");
    assert_eq!(first, retried);
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/checkpoints/"))
    }));

    backend.clear();
    let conflict = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_001),
        ts(2_100_000_000),
        None,
    )
    .expect("conflicting request");
    assert!(matches!(
        service.create_snapshot(&conflict).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/checkpoints/"))
    }));
}

#[tokio::test]
async fn snapshot_retry_rejects_a_different_target_pin_without_writing_a_second_root() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("initial snapshot");
    let conflicting_pin = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        ALT_PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request with different target pin");
    backend.clear();

    assert!(matches!(
        service.create_snapshot(&conflicting_pin).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(
        storage
            .get_raw(&retention_pin_latest_path(ALT_PIN_ID).expect("alternate selector path"))
            .await
            .is_err(),
        "retry must not create a second retention root"
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(ALT_PIN_ID))
    }));
}

#[tokio::test]
async fn export_retry_rejects_a_different_target_pin_without_writing_a_second_root() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("initial export");
    let conflicting_pin = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        ALT_EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_100),
        ts(2_050_000_000),
    )
    .expect("request with different target pin");
    backend.clear();

    assert!(matches!(
        service.export_snapshot(&conflicting_pin).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(
        storage
            .get_raw(
                &retention_pin_latest_path(ALT_EXPORT_PIN_ID)
                    .expect("alternate export selector path")
            )
            .await
            .is_err(),
        "retry must not create a second export retention root"
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(ALT_EXPORT_PIN_ID))
    }));
}

#[tokio::test]
async fn export_retry_rejects_a_different_source_pin_identity() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("initial export");
    let conflicting_source = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        ALT_SOURCE_PIN_ID,
        ts(2_000_000_100),
        ts(2_050_000_000),
    )
    .expect("request with different source pin");
    backend.clear();

    assert!(matches!(
        service.export_snapshot(&conflicting_source).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/retention/pins/"))
    }));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Get(path) | BackendOperation::Head(path)
            if path.contains(ALT_SOURCE_PIN_ID))
    }));
}

#[tokio::test]
async fn export_retry_rejects_a_source_divergent_cut_before_republishing_its_target_pin() {
    let (backend, storage, registry) = service_fixture(&["catalog", "search"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    let source = service
        .create_snapshot(&snapshot_request())
        .await
        .expect("two-domain source snapshot");
    let source_path = snapshot_record_path(SNAPSHOT_ID).expect("source snapshot path");
    let source_bytes = storage
        .get_raw(&source_path)
        .await
        .expect("source snapshot bytes");

    let mut divergent_objects = source
        .required_objects()
        .iter()
        .filter(|object| object.relative_path().contains("/catalog/"))
        .cloned()
        .collect::<Vec<_>>();
    divergent_objects.push(
        RequiredObject::new(
            &source_path,
            source_bytes.len() as u64,
            RequiredObjectKind::SnapshotRecord,
            sha256(&source_bytes),
        )
        .expect("source snapshot record object"),
    );
    let request = export_request();
    let divergent = ExportManifest::new(
        request.export_id(),
        request.target_pin_id(),
        request.snapshot_id(),
        request.source_pin_id(),
        source.scope().clone(),
        request.created_at(),
        request.retained_until(),
        source
            .domains()
            .iter()
            .filter(|domain| domain.domain() == "catalog")
            .cloned()
            .collect(),
        source
            .projection_watermarks()
            .iter()
            .filter(|projection| projection.source_domain() == "catalog")
            .cloned()
            .collect(),
        source
            .event_archives()
            .iter()
            .filter(|archive| archive.source_domain() == "catalog")
            .cloned()
            .collect(),
        divergent_objects,
        source
            .compatibility_artifacts()
            .iter()
            .filter(|artifact| artifact.relative_path().contains("/catalog/"))
            .cloned()
            .collect(),
        RelocationPolicy::relative_to_caller_export_root(),
    )
    .expect("internally valid but source-divergent export");
    assert_eq!(divergent.domains().len(), 1);
    put_object(
        &storage,
        &export_record_path(EXPORT_ID).expect("export path"),
        &encode_export_manifest(&divergent).expect("divergent export bytes"),
    )
    .await;

    let partial_target_pin = RetentionPinRevision::new(
        EXPORT_PIN_ID,
        1,
        RetentionTarget::export(EXPORT_ID).expect("export target"),
        request.created_at(),
        request.retained_until(),
        None,
    )
    .expect("partial target pin");
    select_pin_revision(&storage, &partial_target_pin).await;
    storage
        .delete(&retention_pin_latest_path(EXPORT_PIN_ID).expect("target selector path"))
        .await
        .expect("remove partial target selector");
    storage
        .delete(&retention_pin_revision_path(EXPORT_PIN_ID, 1).expect("target revision path"))
        .await
        .expect("remove partial target revision");
    backend.clear();

    assert!(matches!(
        service.export_snapshot(&request).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains(&format!("/retention/pins/{EXPORT_PIN_ID}/")))
    }));
    assert!(
        backend
            .operations()
            .iter()
            .all(|operation| { !matches!(operation, BackendOperation::List(_)) })
    );
    assert!(
        storage
            .get_raw(&retention_pin_latest_path(EXPORT_PIN_ID).expect("target selector path"))
            .await
            .is_err(),
        "source-divergent retry must not reactivate the target pin"
    );
}

#[tokio::test]
async fn export_retry_accepts_additive_v1_fields_when_known_cut_matches_its_source() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let request = export_request();
    let expected = service
        .export_snapshot(&request)
        .await
        .expect("source-derived export");
    let export_path = export_record_path(EXPORT_ID).expect("export path");
    mutate_json(&storage, &export_path, |value| {
        value["future_v1_hint"] = Value::String("preserved compatibility".to_string());
    })
    .await;
    storage
        .delete(&retention_pin_latest_path(EXPORT_PIN_ID).expect("target selector path"))
        .await
        .expect("remove target selector");
    storage
        .delete(&retention_pin_revision_path(EXPORT_PIN_ID, 1).expect("target revision path"))
        .await
        .expect("remove target revision");
    backend.clear();

    assert_eq!(
        service
            .export_snapshot(&request)
            .await
            .expect("typed known-field retry"),
        expected
    );
    assert!(
        storage
            .get_raw(&retention_pin_latest_path(EXPORT_PIN_ID).expect("target selector path"))
            .await
            .is_ok()
    );
    assert!(
        backend
            .operations()
            .iter()
            .all(|operation| { !matches!(operation, BackendOperation::List(_)) })
    );
}

#[tokio::test]
async fn export_creation_rejects_an_alias_pin_not_bound_by_the_source_snapshot() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let alias_source_pin = RetentionPinRevision::new(
        ALT_SOURCE_PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("alias source pin");
    select_pin_revision(&storage, &alias_source_pin).await;
    let alias_source = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        ALT_SOURCE_PIN_ID,
        ts(2_000_000_100),
        ts(2_050_000_000),
    )
    .expect("request with alias source pin");
    backend.clear();

    assert!(matches!(
        service.export_snapshot(&alias_source).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(
        storage
            .get_raw(&export_record_path(EXPORT_ID).expect("export path"))
            .await
            .is_err()
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(EXPORT_PIN_ID))
    }));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Get(path) | BackendOperation::Head(path)
            if path.contains(ALT_SOURCE_PIN_ID))
    }));
}

#[tokio::test]
async fn restore_preflight_rejects_alias_pins_not_bound_by_snapshot_or_export_records() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("source export");

    let snapshot_alias = RetentionPinRevision::new(
        ALT_SOURCE_PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("snapshot alias pin");
    select_pin_revision(&storage, &snapshot_alias).await;
    let export_alias = RetentionPinRevision::new(
        ALT_EXPORT_PIN_ID,
        1,
        RetentionTarget::export(EXPORT_ID).expect("export target"),
        ts(2_000_000_100),
        ts(2_050_000_000),
        None,
    )
    .expect("export alias pin");
    select_pin_revision(&storage, &export_alias).await;
    backend.clear();

    for source in [
        RestoreSource::snapshot(SNAPSHOT_ID, ALT_SOURCE_PIN_ID).expect("snapshot source"),
        RestoreSource::export(EXPORT_ID, ALT_EXPORT_PIN_ID).expect("export source"),
    ] {
        let report = service
            .preflight_restore(
                &source,
                &WorkspaceScope::new("tenant", "workspace").expect("scope"),
                ts(2_000_000_200),
            )
            .await
            .expect("preflight report");
        assert!(!report.is_ready(), "alias pin must fail closed");
        assert!(report.issues().iter().any(|issue| {
            issue.kind() == RestorePreflightIssueKind::Corrupt
                && issue.identifier() == "retention_pin"
        }));
    }
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Get(path) | BackendOperation::Head(path)
            if path.contains(ALT_SOURCE_PIN_ID) || path.contains(ALT_EXPORT_PIN_ID))
    }));
}

#[tokio::test]
async fn export_creation_rejects_bound_source_pin_with_substituted_initial_semantics() {
    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let substituted = RetentionPinRevision::new(
        PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(2_000_000_000),
        ts(2_100_000_001),
        None,
    )
    .expect("substituted initial source pin");
    select_pin_revision(&storage, &substituted).await;

    assert!(service.export_snapshot(&export_request()).await.is_err());
    assert!(
        storage
            .get_raw(&export_record_path(EXPORT_ID).expect("export path"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn export_retry_rejects_bound_source_pin_with_substituted_initial_semantics() {
    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let request = export_request();
    service
        .export_snapshot(&request)
        .await
        .expect("initial export");
    let substituted = RetentionPinRevision::new(
        PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(2_000_000_001),
        ts(2_100_000_000),
        None,
    )
    .expect("substituted initial source pin");
    select_pin_revision(&storage, &substituted).await;

    assert!(service.export_snapshot(&request).await.is_err());
}

#[tokio::test]
async fn restore_preflight_rejects_bound_pins_with_substituted_initial_semantics() {
    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let substituted_snapshot_pin = RetentionPinRevision::new(
        PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(2_000_000_001),
        ts(2_100_000_000),
        None,
    )
    .expect("substituted snapshot pin");
    select_pin_revision(&storage, &substituted_snapshot_pin).await;
    let snapshot_report = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("snapshot source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_000_000_200),
        )
        .await
        .expect("snapshot preflight report");
    assert!(snapshot_report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Corrupt && issue.identifier() == "retention_pin"
    }));

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("source export");
    let substituted_export_pin = RetentionPinRevision::new(
        EXPORT_PIN_ID,
        1,
        RetentionTarget::export(EXPORT_ID).expect("export target"),
        ts(2_000_000_100),
        ts(2_050_000_001),
        None,
    )
    .expect("substituted export pin");
    select_pin_revision(&storage, &substituted_export_pin).await;
    let export_report = service
        .preflight_restore(
            &RestoreSource::export(EXPORT_ID, EXPORT_PIN_ID).expect("export source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_000_000_200),
        )
        .await
        .expect("export preflight report");
    assert!(export_report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Corrupt && issue.identifier() == "retention_pin"
    }));
}

#[tokio::test]
async fn snapshot_exact_retry_accepts_a_valid_active_advanced_target_pin() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    let request = snapshot_request();
    let expected = service
        .create_snapshot(&request)
        .await
        .expect("initial snapshot");
    let released_in_the_future = read_pin_revision(&storage, PIN_ID, 1)
        .await
        .release(2, ts(2_050_000_000))
        .expect("schedule target pin release");
    select_pin_revision(&storage, &released_in_the_future).await;
    backend.clear();

    assert_eq!(
        service
            .create_snapshot(&request)
            .await
            .expect("retry accepts advanced pin"),
        expected
    );
    let selector = decode_retention_pin_latest(
        &storage
            .get_raw(&retention_pin_latest_path(PIN_ID).expect("selector path"))
            .await
            .expect("selected pin"),
    )
    .expect("decode selected pin");
    assert_eq!(selector.revision(), 2);
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(PIN_ID))
    }));
}

#[tokio::test]
async fn export_exact_retry_accepts_a_valid_active_advanced_target_pin() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let request = export_request();
    let expected = service
        .export_snapshot(&request)
        .await
        .expect("initial export");
    let released_in_the_future = read_pin_revision(&storage, EXPORT_PIN_ID, 1)
        .await
        .release(2, ts(2_025_000_000))
        .expect("schedule export target pin release");
    select_pin_revision(&storage, &released_in_the_future).await;
    backend.clear();

    assert_eq!(
        service
            .export_snapshot(&request)
            .await
            .expect("retry accepts advanced export pin"),
        expected
    );
    let selector = decode_retention_pin_latest(
        &storage
            .get_raw(&retention_pin_latest_path(EXPORT_PIN_ID).expect("selector path"))
            .await
            .expect("selected export pin"),
    )
    .expect("decode selected export pin");
    assert_eq!(selector.revision(), 2);
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(EXPORT_PIN_ID))
    }));
}

#[tokio::test]
async fn exact_retries_reject_target_pin_renewal_beyond_the_immutable_cut_deadline() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    let snapshot_create_request = snapshot_request();
    service
        .create_snapshot(&snapshot_create_request)
        .await
        .expect("initial snapshot");
    let overextended_snapshot_pin = read_pin_revision(&storage, PIN_ID, 1)
        .await
        .renew(2, ts(2_200_000_000), ts(2_000_000_001))
        .expect("structurally valid renewal");
    select_pin_revision(&storage, &overextended_snapshot_pin).await;
    backend.clear();

    assert!(
        service
            .create_snapshot(&snapshot_create_request)
            .await
            .is_err(),
        "retry must reject a pin that outlives the immutable snapshot cut"
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(PIN_ID))
    }));

    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let export_request = export_request();
    service
        .export_snapshot(&export_request)
        .await
        .expect("initial export");
    let overextended_export_pin = read_pin_revision(&storage, EXPORT_PIN_ID, 1)
        .await
        .renew(2, ts(2_075_000_000), ts(2_000_000_101))
        .expect("structurally valid export renewal");
    select_pin_revision(&storage, &overextended_export_pin).await;
    backend.clear();

    assert!(
        service.export_snapshot(&export_request).await.is_err(),
        "retry must reject a pin that outlives the immutable export cut"
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(EXPORT_PIN_ID))
    }));
}

#[tokio::test]
async fn export_and_preflight_reject_a_source_pin_renewed_beyond_the_snapshot_cut() {
    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let overextended = read_pin_revision(&storage, PIN_ID, 1)
        .await
        .renew(2, ts(2_200_000_000), ts(2_000_000_001))
        .expect("structurally valid renewal");
    select_pin_revision(&storage, &overextended).await;

    assert!(
        service.export_snapshot(&export_request()).await.is_err(),
        "an export must not rely on retention beyond the immutable source cut"
    );
    let report = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_150_000_000),
        )
        .await
        .expect("overextended pin is a classified preflight issue");
    assert!(report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Corrupt && issue.identifier() == "retention_pin"
    }));
}

#[tokio::test]
async fn create_retry_completes_partial_pin_publication_without_checkpointing_again() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = snapshot_request();
    let expected = service
        .create_snapshot(&request)
        .await
        .expect("initial snapshot");

    mutation_storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("latest pin path"))
        .await
        .expect("remove selector to simulate crash");
    mutation_storage
        .delete(&retention_pin_revision_path(PIN_ID, 1).expect("pin revision path"))
        .await
        .expect("remove revision to simulate crash");
    backend.clear();

    let recovered = service
        .create_snapshot(&request)
        .await
        .expect("retry completes pin publication");
    assert_eq!(recovered, expected);
    let operations = backend.operations();
    assert!(operations.iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/checkpoints/"))
    }));
    assert_eq!(
        operations
            .iter()
            .filter(|operation| {
                matches!(operation, BackendOperation::Put(path)
                    if path.contains(&format!("/retention/pins/{PIN_ID}/")))
            })
            .count(),
        2
    );
}

#[tokio::test]
async fn create_retry_revalidates_a_corrupt_closure_before_reactivating_the_pin() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = snapshot_request();
    service
        .create_snapshot(&request)
        .await
        .expect("initial snapshot");

    mutation_storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("latest pin path"))
        .await
        .expect("remove selector to simulate crash");
    mutation_storage
        .delete(&retention_pin_revision_path(PIN_ID, 1).expect("pin revision path"))
        .await
        .expect("remove revision to simulate crash");
    mutation_storage
        .put_raw(
            "projections/catalog/manifest.json",
            Bytes::from(vec![b'x'; b"projection-catalog".len()]),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt retained closure");
    backend.clear();

    assert!(service.create_snapshot(&request).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains(&format!("/retention/pins/{PIN_ID}/")))
    }));
    assert!(
        mutation_storage
            .get_raw(&retention_pin_latest_path(PIN_ID).expect("latest pin path"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn create_immutable_collision_accepts_identical_winner_and_rejects_conflict() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("request");
    backend.arm_collision(format!("retention/snapshots/{SNAPSHOT_ID}.json"), true);
    service
        .create_snapshot(&request)
        .await
        .expect("identical winner");

    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    backend.arm_collision(format!("retention/snapshots/{SNAPSHOT_ID}.json"), false);
    assert!(matches!(
        service.create_snapshot(&request).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains(&format!("retention/pins/{PIN_ID}")))
    }));
}

#[tokio::test]
async fn export_and_direct_get_use_no_list_when_listing_is_denied() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    let snapshot_request = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("snapshot request");
    service
        .create_snapshot(&snapshot_request)
        .await
        .expect("snapshot");

    backend.clear();
    backend.deny_lists();
    assert_eq!(
        service
            .get_snapshot(SNAPSHOT_ID)
            .await
            .expect("direct snapshot get")
            .snapshot_id(),
        SNAPSHOT_ID
    );
    let export_request = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_100),
        ts(2_050_000_000),
    )
    .expect("export request");
    let export = service
        .export_snapshot(&export_request)
        .await
        .expect("export snapshot");
    assert_eq!(export.export_id(), EXPORT_ID);
    assert_eq!(export.snapshot_id(), SNAPSHOT_ID);
    assert_eq!(export.required_objects().len(), 6);
    assert_eq!(
        service
            .get_export(EXPORT_ID)
            .await
            .expect("direct export get"),
        export
    );

    let operations = backend.operations();
    assert!(
        operations
            .iter()
            .all(|operation| !matches!(operation, BackendOperation::List(_)))
    );
    assert!(operations.iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/legacy/") || path.contains("snapshots.parquet") || path.contains("current.pointer"))
    }));
    assert_eq!(
        operations
            .iter()
            .filter(|operation| matches!(operation, BackendOperation::Put(path) if path.contains("/retention/exports/") || path.contains(&format!("/retention/pins/{EXPORT_PIN_ID}/"))))
            .count(),
        3
    );
}

fn snapshot_request() -> CreateWorkspaceSnapshotRequest {
    CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("snapshot request")
}

fn export_request() -> CreateWorkspaceExportRequest {
    CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_100),
        ts(2_050_000_000),
    )
    .expect("export request")
}

async fn retention_coordination_fixture() -> (
    Arc<RecordingBackend>,
    ScopedStorage,
    Arc<WorkspaceSnapshotService>,
    String,
) {
    let backend = Arc::new(RecordingBackend::default());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    Tier1Writer::new(storage.clone())
        .initialize()
        .await
        .expect("initialize Tier 1");

    let version_dir = "snapshots/catalog/v777/";
    let protected_path = format!("{version_dir}protected.bin");
    let protected_bytes = b"must remain in the retained closure";
    put_object(&storage, version_dir, b"directory marker").await;
    put_object(&storage, &protected_path, protected_bytes).await;

    let store = Arc::new(initialized_control_store(&storage, "catalog").await);
    let projection_cut = ProjectionWatermarkCut::new(
        Vec::new(),
        vec![
            RequiredObject::new(
                &protected_path,
                protected_bytes.len() as u64,
                RequiredObjectKind::Other,
                sha256(protected_bytes),
            )
            .expect("protected required object"),
        ],
        Vec::new(),
    )
    .expect("projection cut");
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", "catalog"),
                store.clone(),
                store,
                Arc::new(FixedProjectionProvider {
                    cut: projection_cut,
                }),
                Arc::new(EmptyArchiveProvider),
            )
            .expect("binding"),
        ],
    )
    .expect("registry");
    let service =
        Arc::new(WorkspaceSnapshotService::new(storage.clone(), registry).expect("service"));
    backend.clear();
    (backend, storage, service, protected_path)
}

#[tokio::test]
async fn retention_coordination_prevents_gc_from_deleting_a_newly_retained_object() {
    let (backend, storage, service, protected_path) = retention_coordination_fixture().await;
    let selector_path = format!(
        "tenant=tenant/workspace=workspace/{}",
        retention_pin_latest_path(PIN_ID).expect("selector path")
    );
    backend.pause_delete(&protected_path, selector_path);

    let collector = GarbageCollector::new(storage.clone(), RetentionPolicy::new(0, 0, 0, 0));
    let gc_task = tokio::spawn(async move { collector.collect().await });
    tokio::time::timeout(Duration::from_secs(2), backend.wait_for_paused_delete())
        .await
        .expect("GC reaches deterministic delete barrier");

    let request = snapshot_request();
    let mut create_task = tokio::spawn(async move { service.create_snapshot(&request).await });
    let early = tokio::time::timeout(Duration::from_millis(50), &mut create_task).await;
    let completed_before_gc_released = early.is_ok();

    backend.resume_paused_delete();
    gc_task
        .await
        .expect("GC task")
        .expect("coordinated GC completes");
    let create_result = match early {
        Ok(result) => result.expect("snapshot task"),
        Err(_) => create_task.await.expect("snapshot task"),
    };

    assert!(
        !completed_before_gc_released,
        "snapshot publication must wait while mutating GC owns coordination"
    );
    assert!(
        create_result.is_err(),
        "final closure validation must reject the object GC deleted first"
    );
    assert_eq!(backend.stale_protected_deletes(), 0);
    assert!(
        storage
            .get_raw(&retention_pin_latest_path(PIN_ID).expect("selector path"))
            .await
            .is_err(),
        "a failed final closure must not publish an active pin"
    );
}

#[tokio::test]
async fn durable_epoch_blocks_gc_while_selector_put_is_stalled_after_lease_proof() {
    let (backend, storage, service, protected_path) = retention_coordination_fixture().await;
    let selector_path = retention_pin_latest_path(PIN_ID).expect("selector path");
    backend.pause_put(&selector_path);

    let request = snapshot_request();
    let create_task = tokio::spawn(async move { service.create_snapshot(&request).await });
    tokio::time::timeout(Duration::from_secs(2), backend.wait_for_paused_put())
        .await
        .expect("snapshot finalization reaches stalled selector put");
    let in_flight: Value = serde_json::from_slice(
        &storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("in-flight epoch"),
    )
    .expect("epoch JSON");
    assert_eq!(in_flight["state"], Value::from("IN_FLIGHT"));

    backend
        .expire_lock("tenant=tenant/workspace=workspace/locks/workspace-retention-gc.lock.json")
        .await;
    let collector = GarbageCollector::new(storage.clone(), RetentionPolicy::new(0, 0, 0, 0));
    let gc_result = collector.collect().await;
    let delete_attempted = backend.operations().iter().any(
        |operation| matches!(operation, BackendOperation::Delete(path) if path.ends_with(&protected_path)),
    );

    backend.resume_paused_put();
    let create_result = create_task.await.expect("snapshot task");

    assert!(
        gc_result.is_err(),
        "a newer GC holder must abort on the durable in-flight publication epoch"
    );
    assert!(!delete_attempted, "GC must perform no protected delete");
    create_result.expect("stale publication may finish inside its durable epoch");
    assert_eq!(
        storage
            .get_raw(&protected_path)
            .await
            .expect("retained object remains"),
        Bytes::from_static(b"must remain in the retained closure")
    );
    storage
        .get_raw(&selector_path)
        .await
        .expect("selector becomes visible after stalled put resumes");
    let settled: Value = serde_json::from_slice(
        &storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("settled epoch"),
    )
    .expect("epoch JSON");
    assert_eq!(settled["state"], Value::from("IDLE"));
}

#[tokio::test]
async fn durable_epoch_blocks_publication_while_gc_delete_is_stalled_after_lease_proof() {
    let (backend, storage, service, protected_path) = retention_coordination_fixture().await;
    let selector_path = retention_pin_latest_path(PIN_ID).expect("selector path");
    backend.pause_delete(
        &protected_path,
        format!("tenant=tenant/workspace=workspace/{selector_path}"),
    );

    let collector = GarbageCollector::new(storage.clone(), RetentionPolicy::new(0, 0, 0, 0));
    let gc_task = tokio::spawn(async move { collector.collect().await });
    tokio::time::timeout(Duration::from_secs(2), backend.wait_for_paused_delete())
        .await
        .expect("GC reaches stalled delete");
    let in_flight: Value = serde_json::from_slice(
        &storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("in-flight epoch"),
    )
    .expect("epoch JSON");
    assert_eq!(in_flight["state"], Value::from("IN_FLIGHT"));

    backend
        .expire_lock("tenant=tenant/workspace=workspace/locks/workspace-retention-gc.lock.json")
        .await;
    backend.clear();
    let request = snapshot_request();
    let publication_result = service.create_snapshot(&request).await;
    let selector_visible_before_resume = storage.get_raw(&selector_path).await.is_ok();
    let blocked_publication_operations = backend.operations();

    backend.resume_paused_delete();
    let gc_result = gc_task.await.expect("GC task");
    let retry_result = service.create_snapshot(&request).await;

    assert!(
        publication_result.is_err(),
        "a newer publication holder must abort on the durable in-flight GC epoch"
    );
    assert!(
        !selector_visible_before_resume,
        "publication must not expose a selector while GC is in flight"
    );
    assert!(blocked_publication_operations.iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains("/checkpoints/")
                || path.contains("/retention/snapshots/")
                || path.contains("/retention/exports/")
                || path.contains("/retention/pins/"))
    }));
    gc_result.expect("stale GC may finish inside its durable epoch");
    assert!(
        storage.get_raw(&protected_path).await.is_err(),
        "stalled delete completes after publication is excluded"
    );
    assert!(
        retry_result.is_err(),
        "later retry must revalidate and reject the now-missing closure"
    );
    assert!(storage.get_raw(&selector_path).await.is_err());
    assert_eq!(backend.stale_protected_deletes(), 0);
    let settled: Value = serde_json::from_slice(
        &storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("settled epoch"),
    )
    .expect("epoch JSON");
    assert_eq!(settled["state"], Value::from("IDLE"));
}

#[tokio::test]
async fn uncertain_gc_delete_leaves_epoch_in_flight_and_blocks_publication() {
    let (backend, storage, service, protected_path) = retention_coordination_fixture().await;
    backend.pause_delete(&protected_path, "selector-that-must-not-exist");
    backend.fail_delete(&protected_path);

    let collector = GarbageCollector::new(storage.clone(), RetentionPolicy::new(0, 0, 0, 0));
    let gc_task = tokio::spawn(async move { collector.collect().await });
    tokio::time::timeout(Duration::from_secs(2), backend.wait_for_paused_delete())
        .await
        .expect("GC reaches stalled delete");
    backend.resume_paused_delete();
    assert!(gc_task.await.expect("GC task").is_err());

    let in_flight: Value = serde_json::from_slice(
        &storage
            .get_raw("retention/coordination/mutation-epoch.json")
            .await
            .expect("uncertain epoch remains"),
    )
    .expect("epoch JSON");
    assert_eq!(in_flight["state"], Value::from("IN_FLIGHT"));
    backend.clear();

    assert!(service.create_snapshot(&snapshot_request()).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains("/checkpoints/")
                || path.contains("/retention/snapshots/")
                || path.contains("/retention/exports/")
                || path.contains("/retention/pins/"))
    }));
}

#[tokio::test]
async fn malformed_durable_epoch_aborts_retry_before_any_retention_publication() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    let request = snapshot_request();
    service
        .create_snapshot(&request)
        .await
        .expect("initial snapshot");
    storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("selector path"))
        .await
        .expect("remove selector");
    storage
        .delete(&retention_pin_revision_path(PIN_ID, 1).expect("revision path"))
        .await
        .expect("remove revision");
    storage
        .put_raw(
            "retention/coordination/mutation-epoch.json",
            Bytes::from_static(br#"{"record_type":"wrong","version":1}"#),
            WritePrecondition::None,
        )
        .await
        .expect("seed malformed durable epoch");
    backend.clear();

    assert!(service.create_snapshot(&request).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains("/retention/pins/") || path.contains("/retention/snapshots/"))
    }));
    assert!(
        backend
            .operations()
            .iter()
            .all(|operation| { !matches!(operation, BackendOperation::List(_)) })
    );
}

#[tokio::test]
async fn retention_coordination_lock_failures_abort_service_and_gc_without_mutation() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    backend.fail_put("locks/workspace-retention-gc.lock.json");
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    assert!(service.create_snapshot(&snapshot_request()).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/retention/snapshots/") || path.contains("/retention/pins/"))
    }));

    let (backend, storage, _service, protected_path) = retention_coordination_fixture().await;
    backend.fail_put("locks/workspace-retention-gc.lock.json");
    let collector = GarbageCollector::new(storage.clone(), RetentionPolicy::new(0, 0, 0, 0));
    assert!(collector.collect().await.is_err());
    assert_eq!(
        storage
            .get_raw(&protected_path)
            .await
            .expect("object retained"),
        Bytes::from_static(b"must remain in the retained closure")
    );
}

#[tokio::test]
async fn retention_coordination_lease_loss_blocks_publication_and_gc_deletion() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    backend.take_over_lease_before_renewal();
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    assert!(service.create_snapshot(&snapshot_request()).await.is_err());
    assert!(
        storage
            .get_raw(&retention_pin_latest_path(PIN_ID).expect("selector path"))
            .await
            .is_err()
    );

    let (backend, storage, _service, protected_path) = retention_coordination_fixture().await;
    backend.take_over_lease_before_renewal();
    let collector = GarbageCollector::new(storage.clone(), RetentionPolicy::new(0, 0, 0, 0));
    assert!(collector.collect().await.is_err());
    assert_eq!(
        storage
            .get_raw(&protected_path)
            .await
            .expect("object retained"),
        Bytes::from_static(b"must remain in the retained closure")
    );
}

#[tokio::test]
async fn export_retry_revalidates_a_missing_closure_before_reactivating_the_pin() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    let request = export_request();
    service
        .export_snapshot(&request)
        .await
        .expect("initial export");

    mutation_storage
        .delete(&retention_pin_latest_path(EXPORT_PIN_ID).expect("latest pin path"))
        .await
        .expect("remove selector to simulate crash");
    mutation_storage
        .delete(&retention_pin_revision_path(EXPORT_PIN_ID, 1).expect("pin revision path"))
        .await
        .expect("remove revision to simulate crash");
    mutation_storage
        .delete("projections/catalog/manifest.json")
        .await
        .expect("remove retained closure object");
    backend.clear();

    assert!(service.export_snapshot(&request).await.is_err());
    let operations = backend.operations();
    assert!(
        operations
            .iter()
            .all(|operation| !matches!(operation, BackendOperation::List(_)))
    );
    assert!(operations.iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path)
            if path.contains(&format!("/retention/pins/{EXPORT_PIN_ID}/")))
    }));
    assert!(
        mutation_storage
            .get_raw(&retention_pin_latest_path(EXPORT_PIN_ID).expect("latest pin path"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn wall_clock_expiry_rejects_snapshot_retry_and_first_or_retry_export() {
    let past_created_at = ts(1_000_000_000);
    let past_retained_until = ts(1_000_000_100);

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("initial snapshot");
    mutation_storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("latest pin path"))
        .await
        .expect("remove selector");
    mutation_storage
        .delete(&retention_pin_revision_path(PIN_ID, 1).expect("pin revision path"))
        .await
        .expect("remove revision");
    mutate_json(
        &mutation_storage,
        &snapshot_record_path(SNAPSHOT_ID).expect("snapshot path"),
        |value| {
            value["created_at"] = serde_json::to_value(past_created_at).expect("created_at JSON");
            value["retained_until"] =
                serde_json::to_value(past_retained_until).expect("retained_until JSON");
        },
    )
    .await;
    let expired_snapshot_retry = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        past_created_at,
        past_retained_until,
        None,
    )
    .expect("expired retry request remains structurally valid");
    assert!(
        service
            .create_snapshot(&expired_snapshot_retry)
            .await
            .is_err()
    );
    assert!(
        mutation_storage
            .get_raw(&retention_pin_latest_path(PIN_ID).expect("latest pin path"))
            .await
            .is_err()
    );

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    let backdated_export = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        past_created_at,
        past_retained_until,
    )
    .expect("backdated export request remains structurally valid");
    assert!(service.export_snapshot(&backdated_export).await.is_err());

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("source snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("initial export");
    mutation_storage
        .delete(&retention_pin_latest_path(EXPORT_PIN_ID).expect("latest pin path"))
        .await
        .expect("remove selector");
    mutation_storage
        .delete(&retention_pin_revision_path(EXPORT_PIN_ID, 1).expect("pin revision path"))
        .await
        .expect("remove revision");
    mutate_json(
        &mutation_storage,
        &export_record_path(EXPORT_ID).expect("export path"),
        |value| {
            value["created_at"] = serde_json::to_value(past_created_at).expect("created_at JSON");
            value["retained_until"] =
                serde_json::to_value(past_retained_until).expect("retained_until JSON");
        },
    )
    .await;
    let expired_export_retry = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        past_created_at,
        past_retained_until,
    )
    .expect("expired retry request remains structurally valid");
    assert!(
        service
            .export_snapshot(&expired_export_retry)
            .await
            .is_err()
    );
    assert!(
        mutation_storage
            .get_raw(&retention_pin_latest_path(EXPORT_PIN_ID).expect("latest pin path"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn export_requires_a_valid_active_source_pin_with_the_requested_target() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("latest path"))
        .await
        .expect("remove source pin");
    backend.clear();
    backend.deny_lists();
    assert!(service.export_snapshot(&export_request()).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::List(_))
            && !matches!(operation, BackendOperation::Put(path) if path.contains("/retention/exports/"))
    }));

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutate_json(
        &storage,
        &retention_pin_latest_path(PIN_ID).expect("latest path"),
        |value| value["revision_sha256"] = Value::from(format!("sha256:{}", "f".repeat(64))),
    )
    .await;
    assert!(service.export_snapshot(&export_request()).await.is_err());

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    let past_snapshot = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(1_600_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("snapshot request");
    service
        .create_snapshot(&past_snapshot)
        .await
        .expect("snapshot");
    let released = read_pin_revision(&storage, PIN_ID, 1)
        .await
        .release(2, ts(1_700_000_000))
        .expect("release source pin");
    select_pin_revision(&storage, &released).await;
    assert!(service.export_snapshot(&export_request()).await.is_err());

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&past_snapshot)
        .await
        .expect("snapshot");
    let expired = RetentionPinRevision::new(
        PIN_ID,
        1,
        RetentionTarget::snapshot(SNAPSHOT_ID).expect("snapshot target"),
        ts(1_600_000_000),
        ts(1_700_000_000),
        None,
    )
    .expect("expired source pin");
    select_pin_revision(&storage, &expired).await;
    assert!(service.export_snapshot(&export_request()).await.is_err());

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    let wrong_target = RetentionPinRevision::new(
        PIN_ID,
        1,
        RetentionTarget::snapshot("snap_01ARZ3NDEKTSV4RRFFQ69G5FAZ").expect("other target"),
        ts(2_000_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("wrong-target pin");
    select_pin_revision(&storage, &wrong_target).await;
    assert!(service.export_snapshot(&export_request()).await.is_err());
}

#[tokio::test]
async fn restore_preflight_classifies_source_pin_lifecycle_and_complete_chain_failures() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("latest path"))
        .await
        .expect("remove source pin");
    backend.clear();
    backend.deny_lists();
    let missing = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_000_000_200),
        )
        .await
        .expect("missing pin is classified");
    assert!(missing.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Missing && issue.identifier() == "retention_pin"
    }));
    assert!(
        backend
            .operations()
            .iter()
            .all(|operation| !matches!(operation, BackendOperation::List(_)))
    );

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    let past_snapshot = CreateWorkspaceSnapshotRequest::new(
        SNAPSHOT_ID,
        PIN_ID,
        ts(1_600_000_000),
        ts(2_100_000_000),
        None,
    )
    .expect("snapshot request");
    service
        .create_snapshot(&past_snapshot)
        .await
        .expect("snapshot");
    let released = read_pin_revision(&storage, PIN_ID, 1)
        .await
        .release(2, ts(1_700_000_000))
        .expect("release source pin");
    select_pin_revision(&storage, &released).await;
    let released_report = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(1_800_000_000),
        )
        .await
        .expect("released pin is classified");
    assert!(released_report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Expired && issue.identifier() == "retention_pin"
    }));

    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&past_snapshot)
        .await
        .expect("snapshot");
    let renewed = read_pin_revision(&storage, PIN_ID, 1)
        .await
        .renew(2, ts(2_200_000_000), ts(1_700_000_000))
        .expect("renew source pin");
    select_pin_revision(&storage, &renewed).await;
    mutate_json(
        &storage,
        &retention_pin_revision_path(PIN_ID, 1).expect("revision path"),
        |value| value["additive_but_digest_changing"] = Value::Bool(true),
    )
    .await;
    let corrupt_chain = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(1_800_000_000),
        )
        .await
        .expect("corrupt chain is classified");
    assert!(corrupt_chain.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Corrupt && issue.identifier() == "retention_pin"
    }));
}

#[tokio::test]
async fn export_missing_size_and_checksum_mismatches_fail_closed() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutation_storage
        .delete("projections/catalog/manifest.json")
        .await
        .expect("delete projection");
    backend.clear();
    assert!(service.export_snapshot(&export_request()).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/retention/exports/"))
    }));

    let (_size_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutation_storage
        .put_raw(
            "projections/catalog/manifest.json",
            Bytes::from_static(b"short"),
            WritePrecondition::None,
        )
        .await
        .expect("overwrite projection");
    assert!(service.export_snapshot(&export_request()).await.is_err());

    let (_checksum_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutation_storage
        .put_raw(
            "projections/catalog/manifest.json",
            Bytes::from(vec![b'x'; b"projection-catalog".len()]),
            WritePrecondition::None,
        )
        .await
        .expect("overwrite projection");
    assert!(service.export_snapshot(&export_request()).await.is_err());
}

#[tokio::test]
async fn export_rejects_expired_incompatible_out_of_scope_and_unknown_authority() {
    let (_expired_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");

    let expired = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_100_000_000),
        ts(2_100_000_100),
    )
    .expect("expired request");
    assert!(service.export_snapshot(&expired).await.is_err());

    mutate_json(
        &mutation_storage,
        &format!("retention/snapshots/{SNAPSHOT_ID}.json"),
        |value| value["version"] = Value::from(2),
    )
    .await;
    assert!(service.export_snapshot(&export_request()).await.is_err());

    let (_out_of_scope_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutate_json(
        &mutation_storage,
        &format!("retention/snapshots/{SNAPSHOT_ID}.json"),
        |value| {
            value["scope"]["tenant_id"] = Value::from("other");
            value["domains"][0]["scope"]["tenant_id"] = Value::from("other");
            value["domains"][0]["authority"]["scope"]["tenant_id"] = Value::from("other");
        },
    )
    .await;
    assert!(service.export_snapshot(&export_request()).await.is_err());

    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutate_json(
        &mutation_storage,
        &format!("retention/snapshots/{SNAPSHOT_ID}.json"),
        |value| {
            value["domains"][0]["authority"]["implementation"] =
                Value::from("unknown-implementation");
        },
    )
    .await;
    backend.clear();
    assert!(service.export_snapshot(&export_request()).await.is_err());
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Put(path) if path.contains("/retention/exports/"))
    }));
}

#[tokio::test]
async fn export_retry_conflicts_and_unsafe_relocation_fail_closed() {
    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("export");

    let conflicting = CreateWorkspaceExportRequest::new(
        EXPORT_ID,
        EXPORT_PIN_ID,
        SNAPSHOT_ID,
        PIN_ID,
        ts(2_000_000_101),
        ts(2_050_000_000),
    )
    .expect("conflicting request");
    assert!(matches!(
        service.export_snapshot(&conflicting).await,
        Err(arco_catalog::CatalogError::PreconditionFailed { .. })
    ));

    mutate_json(
        &mutation_storage,
        &format!("retention/exports/{EXPORT_ID}.json"),
        |value| {
            value["relocation"]["provider_uri"] = Value::from("s3://secret-bucket/root");
        },
    )
    .await;
    assert!(service.get_export(EXPORT_ID).await.is_err());
}

async fn preflight_service_with(
    storage: ScopedStorage,
    state_store: Arc<dyn arco_catalog::ArcoStateStore>,
    adapter: Arc<dyn PersistedAuthorityAdapter>,
) -> WorkspaceSnapshotService {
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![
            WorkspaceDomainBinding::new(
                StateScope::new("tenant", "workspace", "catalog"),
                state_store,
                adapter,
                Arc::new(FailingProjectionProvider),
                Arc::new(EmptyArchiveProvider),
            )
            .expect("binding"),
        ],
    )
    .expect("registry");
    WorkspaceSnapshotService::new(storage, registry).expect("service")
}

#[tokio::test]
async fn preflight_ready_snapshot_and_export_are_read_only_and_use_no_list() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("export");

    let control = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("control store"),
    );
    let preflight = preflight_service_with(storage, control.clone(), control).await;
    backend.clear();
    backend.deny_lists();

    for source in [
        RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("snapshot source"),
        RestoreSource::export(EXPORT_ID, EXPORT_PIN_ID).expect("export source"),
    ] {
        let report = preflight
            .preflight_restore(
                &source,
                &WorkspaceScope::new("tenant", "workspace").expect("scope"),
                ts(2_000_000_200),
            )
            .await
            .expect("preflight");
        assert!(report.is_ready());
        assert!(report.issues().is_empty());
    }

    assert!(backend.operations().iter().all(|operation| {
        !matches!(
            operation,
            BackendOperation::Put(_) | BackendOperation::Delete(_) | BackendOperation::List(_)
        )
    }));
}

#[tokio::test]
async fn preflight_uses_supplied_time_and_reports_expired_authority_without_incompatible() {
    let (_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");

    let report = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_100_000_001),
        )
        .await
        .expect("expired authority is classified");
    assert!(report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Expired
            && issue.domain() == Some("catalog")
            && issue.identifier() == "authority"
    }));
    assert!(!report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Incompatible
            && issue.domain() == Some("catalog")
            && issue.identifier() == "authority"
    }));
}

#[tokio::test]
async fn preflight_rejects_incomplete_snapshot_and_export_authority_closures() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage, registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    service
        .export_snapshot(&export_request())
        .await
        .expect("export");

    mutate_json(
        &mutation_storage,
        &format!("retention/snapshots/{SNAPSHOT_ID}.json"),
        |value| {
            value["required_objects"]
                .as_array_mut()
                .expect("required objects")
                .retain(|object| {
                    !matches!(
                        object["kind"].as_str(),
                        Some("authority_manifest" | "checkpoint")
                    )
                });
        },
    )
    .await;
    mutate_json(
        &mutation_storage,
        &format!("retention/exports/{EXPORT_ID}.json"),
        |value| {
            for object in value["required_objects"]
                .as_array_mut()
                .expect("required objects")
            {
                match object["kind"].as_str() {
                    Some("authority_manifest") => object["kind"] = Value::from("other"),
                    Some("checkpoint") => {
                        object["sha256"] = Value::from(format!("sha256:{}", "f".repeat(64)));
                    }
                    _ => {}
                }
            }
        },
    )
    .await;
    backend.clear();
    backend.deny_lists();

    for source in [
        RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("snapshot source"),
        RestoreSource::export(EXPORT_ID, EXPORT_PIN_ID).expect("export source"),
    ] {
        let report = service
            .preflight_restore(
                &source,
                &WorkspaceScope::new("tenant", "workspace").expect("scope"),
                ts(2_000_000_200),
            )
            .await
            .expect("classified preflight report");
        assert!(!report.is_ready());
        assert!(report.issues().iter().any(|issue| {
            issue.kind() == RestorePreflightIssueKind::Corrupt
                && issue.domain() == Some("catalog")
                && issue.identifier() == "authority_manifest_reference"
        }));
        assert!(report.issues().iter().any(|issue| {
            issue.kind() == RestorePreflightIssueKind::Corrupt
                && issue.domain() == Some("catalog")
                && issue.identifier() == "checkpoint_reference"
        }));
    }
    assert!(backend.operations().iter().all(|operation| {
        !matches!(
            operation,
            BackendOperation::Put(_) | BackendOperation::Delete(_) | BackendOperation::List(_)
        )
    }));
}

#[tokio::test]
async fn preflight_reports_sorted_redacted_issue_categories_and_scope() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    mutation_storage
        .delete("projections/catalog/manifest.json")
        .await
        .expect("delete projection");
    mutation_storage
        .put_raw(
            "archives/catalog/manifest.json",
            Bytes::from(vec![b'x'; b"archive-catalog".len()]),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt archive");

    let current = Arc::new(CurrentStateStore::new());
    let preflight =
        preflight_service_with(storage.clone(), current, Arc::new(FailOnResolveAdapter)).await;
    backend.clear();
    let report = preflight
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_100_000_001),
        )
        .await
        .expect("classified report");
    assert_eq!(
        report
            .issues()
            .iter()
            .map(|issue| issue.kind())
            .collect::<Vec<_>>(),
        vec![
            RestorePreflightIssueKind::Missing,
            RestorePreflightIssueKind::Corrupt,
            RestorePreflightIssueKind::Expired,
            RestorePreflightIssueKind::Expired,
        ]
    );
    let safe_details = report
        .issues()
        .iter()
        .map(|issue| {
            format!(
                "{:?}:{}:{}",
                issue.kind(),
                issue.domain().unwrap_or("none"),
                issue.identifier()
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    assert!(!safe_details.contains('/'));
    assert!(!safe_details.contains("sha256:"));
    assert!(!safe_details.contains("secret"));
    assert!(backend.operations().iter().all(|operation| {
        !matches!(
            operation,
            BackendOperation::Put(_) | BackendOperation::Delete(_) | BackendOperation::List(_)
        )
    }));

    let out_of_scope = preflight
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "other").expect("scope"),
            ts(2_000_000_200),
        )
        .await
        .expect("out-of-scope report");
    assert_eq!(out_of_scope.issues().len(), 1);
    assert_eq!(
        out_of_scope.issues()[0].kind(),
        RestorePreflightIssueKind::OutOfScope
    );
}

#[tokio::test]
async fn out_of_scope_preflight_returns_before_probing_the_bound_retention_pin() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    storage
        .delete(&retention_pin_latest_path(PIN_ID).expect("selector path"))
        .await
        .expect("remove selector so probing it would leak another issue");
    backend.clear();

    let report = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "other").expect("scope"),
            ts(2_000_000_200),
        )
        .await
        .expect("scope mismatch is a redacted report");

    assert_eq!(report.issues().len(), 1);
    assert_eq!(
        report.issues()[0].kind(),
        RestorePreflightIssueKind::OutOfScope
    );
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Get(path) | BackendOperation::Head(path)
            if path.contains("/retention/pins/"))
    }));
}

#[tokio::test]
async fn preflight_rejects_an_oversized_pin_selector_before_reading_any_revision() {
    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    let oversized_revision = 1_000_000_u64;
    mutate_json(
        &storage,
        &retention_pin_latest_path(PIN_ID).expect("selector path"),
        |value| {
            value["revision"] = Value::from(oversized_revision);
            value["revision_path"] = Value::from(
                retention_pin_revision_path(PIN_ID, oversized_revision)
                    .expect("oversized canonical revision path"),
            );
        },
    )
    .await;
    backend.clear();

    let report = service
        .preflight_restore(
            &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
            &WorkspaceScope::new("tenant", "workspace").expect("scope"),
            ts(2_000_000_200),
        )
        .await
        .expect("oversized selector is a classified preflight issue");
    assert!(report.issues().iter().any(|issue| {
        issue.kind() == RestorePreflightIssueKind::Corrupt && issue.identifier() == "retention_pin"
    }));
    let oversized_path = format!("/retention/pins/{PIN_ID}/revisions/{oversized_revision}.json");
    assert!(backend.operations().iter().all(|operation| {
        !matches!(operation, BackendOperation::Get(path) | BackendOperation::Head(path)
            if path.ends_with(&oversized_path))
    }));
}

#[tokio::test]
async fn preflight_malformed_records_and_backend_outages_are_operation_errors() {
    let (_malformed_backend, storage, registry) = service_fixture(&["catalog"]).await;
    let mutation_storage = storage.clone();
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    let control = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("control store"),
    );
    let preflight = preflight_service_with(storage.clone(), control.clone(), control).await;
    mutate_json(
        &mutation_storage,
        &format!("retention/snapshots/{SNAPSHOT_ID}.json"),
        |value| value["version"] = Value::from(99),
    )
    .await;
    assert!(
        preflight
            .preflight_restore(
                &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
                &WorkspaceScope::new("tenant", "workspace").expect("scope"),
                ts(2_000_000_200),
            )
            .await
            .is_err()
    );

    let (backend, storage, registry) = service_fixture(&["catalog"]).await;
    let service = WorkspaceSnapshotService::new(storage.clone(), registry).expect("service");
    service
        .create_snapshot(&snapshot_request())
        .await
        .expect("snapshot");
    let control = Arc::new(
        ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("control store"),
    );
    let preflight = preflight_service_with(storage, control.clone(), control).await;
    backend.fail_get("projections/catalog/manifest.json");
    assert!(matches!(
        preflight
            .preflight_restore(
                &RestoreSource::snapshot(SNAPSHOT_ID, PIN_ID).expect("source"),
                &WorkspaceScope::new("tenant", "workspace").expect("scope"),
                ts(2_000_000_200),
            )
            .await,
        Err(arco_catalog::CatalogError::Storage { .. })
    ));
}
