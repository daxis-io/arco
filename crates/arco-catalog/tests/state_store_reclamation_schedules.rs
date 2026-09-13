//! Deterministic remote publication schedules and an independent logical retention oracle.
#![cfg(feature = "test-utils")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
#![allow(clippy::too_many_lines, clippy::unused_async)]

#[path = "../benches/support/durable_maintenance.rs"]
mod durable_maintenance;

use arco_catalog::retention_coordination::{
    RETENTION_MUTATION_EPOCH_PATH, recover_stale_retention_epoch,
};
use arco_catalog::state_store::StateScope;
use arco_catalog::workspace_snapshot::{
    DomainAuthorityReference, DomainEventArchive, WorkspaceScope, export_record_path,
    retention_pin_latest_path, retention_pin_revision_path, snapshot_record_path,
};
use arco_catalog::workspace_snapshot_service::{
    CreateWorkspaceExportRequest, CreateWorkspaceSnapshotRequest, EventArchiveCapture,
    EventArchiveProvider, ProjectionWatermarkCut, ProjectionWatermarkProvider,
    WorkspaceDomainBinding, WorkspaceDomainRegistry, WorkspaceSnapshotService,
};
use arco_catalog::{
    ArcoStateTxn as _, ControlMvpMaintenanceWorker, ControlMvpStateStore,
    PersistedAuthorityAdapter as _, Result, TxnOptions,
};
use arco_core::{
    ListPage, MemoryBackend, ObjectMeta, ScopedStorage, StorageBackend, WritePrecondition,
    WriteResult,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

use arco_catalog::workspace_snapshot::{
    RetentionPinLatest, RetentionPinRevision, decode_retention_pin_revision,
    encode_retention_pin_latest, encode_retention_pin_revision,
};
use arco_catalog::{
    ArcoStateAdmin as _, ArcoStateReader, CheckpointOptions, CheckpointToken, StateToken,
};
use futures::FutureExt as _;
use sha2::{Digest as _, Sha256};
#[path = "support/integrity_oracle.rs"]
mod integrity_oracle;
use integrity_oracle::LogicalOracle;

const RETENTION_GC_LOCK_PATH: &str = "locks/workspace-retention-gc.lock.json";
const SNAP: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const EXP: &str = "exp_01ARZ3NDEKTSV4RRFFQ69G5FAW";
const PIN: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAX";
const EPIN: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAY";

#[derive(Clone, Copy, Debug)]
enum Fault {
    PauseBefore,
    PauseAfter,
    LostResponse,
    DelayedError,
    Unreadable,
    IdenticalWinner,
    DifferentWinner,
    DifferentLostResponse,
}

#[derive(Debug)]
struct Schedule {
    needle: String,
    skip: std::sync::atomic::AtomicUsize,
    fault: Fault,
    issued: Notify,
    apply: Semaphore,
    applied: Notify,
    respond: Semaphore,
    finished: Notify,
}
impl Schedule {
    fn new(needle: String, skip: usize, fault: Fault) -> Arc<Self> {
        Arc::new(Self {
            needle,
            skip: std::sync::atomic::AtomicUsize::new(skip),
            fault,
            issued: Notify::new(),
            apply: Semaphore::new(0),
            applied: Notify::new(),
            respond: Semaphore::new(0),
            finished: Notify::new(),
        })
    }
    async fn issued(&self) {
        guard(self.issued.notified()).await;
    }
    async fn finish(&self) {
        self.apply.add_permits(1);
        self.respond.add_permits(1);
        guard(self.finished.notified()).await;
    }
}
async fn guard<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("schedule deadlock")
}

/// Unbuffered live evidence survives cancellation or process termination.
#[derive(Debug, Default)]
struct EventTrace {
    events: Vec<String>,
    live: Option<std::fs::File>,
}
impl EventTrace {
    fn push(&mut self, event: String) {
        if let Some(file) = &mut self.live {
            use std::io::Write as _;
            writeln!(file, "{event}").unwrap();
        }
        self.events.push(event);
    }
    fn attach(&mut self, path: &std::path::Path, identity: &str) {
        self.live = Some(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap(),
        );
        self.push(identity.to_owned());
        self.live.as_ref().unwrap().sync_all().unwrap();
    }
}
impl std::ops::Deref for EventTrace {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}
impl std::ops::DerefMut for EventTrace {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.events
    }
}

#[derive(Debug)]
struct Backend {
    inner: Arc<MemoryBackend>,
    armed: Mutex<Option<Arc<Schedule>>>,
    denied: Arc<Mutex<Option<String>>>,
    trace: Arc<Mutex<EventTrace>>,
    now: Arc<Mutex<DateTime<Utc>>>,
    timestamps: Arc<Mutex<BTreeMap<String, DateTime<Utc>>>>,
    authorized_deletes: Arc<Mutex<BTreeSet<String>>>,
}
impl Backend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryBackend::new()),
            armed: Mutex::new(None),
            denied: Arc::new(Mutex::new(None)),
            trace: Arc::new(Mutex::new(EventTrace::default())),
            now: Arc::new(Mutex::new(Utc::now())),
            timestamps: Arc::new(Mutex::new(BTreeMap::new())),
            authorized_deletes: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }
    fn arm(&self, needle: String, skip: usize, fault: Fault) -> Arc<Schedule> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("ARM {needle} skip={skip} fault={fault:?}"));
        let schedule = Schedule::new(needle, skip, fault);
        *self.armed.lock().unwrap() = Some(schedule.clone());
        schedule
    }
    fn take(&self, path: &str) -> Option<Arc<Schedule>> {
        let mut armed = self.armed.lock().unwrap();
        if let Some(s) = armed.as_mut() {
            if path.contains(&s.needle) {
                if s.skip.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                    s.skip.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                } else {
                    return armed.take();
                }
            }
        }
        None
    }
    fn meta(&self, mut meta: ObjectMeta) -> ObjectMeta {
        meta.last_modified = self
            .timestamps
            .lock()
            .unwrap()
            .get(&meta.path)
            .copied()
            .or(meta.last_modified);
        meta
    }
    async fn expire_lease(&self, storage: &ScopedStorage) {
        if let Ok(bytes) = storage.get_raw(RETENTION_GC_LOCK_PATH).await {
            let mut value: arco_core::lock::LockInfo = serde_json::from_slice(&bytes).unwrap();
            value.expires_at = Utc::now() - chrono::Duration::seconds(1);
            storage
                .put_raw(
                    RETENTION_GC_LOCK_PATH,
                    Bytes::from(serde_json::to_vec(&value).unwrap()),
                    WritePrecondition::None,
                )
                .await
                .unwrap();
            let stored: arco_core::lock::LockInfo =
                serde_json::from_slice(&storage.get_raw(RETENTION_GC_LOCK_PATH).await.unwrap())
                    .unwrap();
            assert!(
                stored.is_expired(),
                "fixture must expire the actual serialized lease"
            );
        }
    }
}
#[async_trait]
impl StorageBackend for Backend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.trace.lock().unwrap().push(format!("GET {path}"));
        if self
            .denied
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| path.contains(s))
        {
            return Err(arco_core::Error::storage("reconciliation read denied"));
        }
        self.inner.get(path).await
    }
    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("RANGE {path} {range:?}"));
        if self
            .denied
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| path.contains(s))
        {
            return Err(arco_core::Error::storage("reconciliation read denied"));
        }
        self.inner.get_range(path, range).await
    }
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("ISSUE PUT {path} {precondition:?}"));
        let Some(schedule) = self.take(path) else {
            let result = self.inner.put(path, data, precondition).await;
            if matches!(result, Ok(WriteResult::Success { .. })) {
                self.timestamps
                    .lock()
                    .unwrap()
                    .insert(path.to_owned(), *self.now.lock().unwrap());
            }
            self.trace
                .lock()
                .unwrap()
                .push(format!("APPLY PUT {path} {result:?}"));
            return result;
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let inner = self.inner.clone();
        let denied = self.denied.clone();
        let trace = self.trace.clone();
        let timestamps = self.timestamps.clone();
        let now = self.now.clone();
        let path = path.to_owned();
        // The remote task owns the request. Dropping/aborting its caller cannot cancel it.
        tokio::spawn(async move {
            schedule.issued.notify_one();
            let mut tx = Some(tx);
            if matches!(schedule.fault, Fault::DelayedError) {
                tx.take()
                    .unwrap()
                    .send(Err(arco_core::Error::storage(
                        "transport failed; remote request pending",
                    )))
                    .ok();
            }
            if matches!(schedule.fault, Fault::PauseBefore | Fault::DelayedError) {
                schedule.apply.acquire().await.unwrap().forget();
            }
            if matches!(
                schedule.fault,
                Fault::IdenticalWinner | Fault::DifferentWinner
            ) {
                let winner = if matches!(schedule.fault, Fault::IdenticalWinner) {
                    data.clone()
                } else {
                    Bytes::from_static(b"different immutable bytes")
                };
                inner
                    .put(&path, winner, WritePrecondition::DoesNotExist)
                    .await
                    .unwrap();
            }
            let data = if matches!(schedule.fault, Fault::DifferentLostResponse) {
                Bytes::from_static(b"different immutable bytes")
            } else {
                data
            };
            let result = inner.put(&path, data, precondition).await;
            trace
                .lock()
                .unwrap()
                .push(format!("APPLY PUT {path} {result:?}"));
            if matches!(result, Ok(WriteResult::Success { .. })) {
                timestamps
                    .lock()
                    .unwrap()
                    .insert(path.clone(), *now.lock().unwrap());
            }
            if matches!(schedule.fault, Fault::Unreadable) {
                *denied.lock().unwrap() = Some(path.clone());
            }
            schedule.applied.notify_one();
            if matches!(schedule.fault, Fault::PauseAfter | Fault::PauseBefore) {
                schedule.respond.acquire().await.unwrap().forget();
            }
            if let Some(tx) = tx {
                let result = if matches!(
                    schedule.fault,
                    Fault::LostResponse | Fault::Unreadable | Fault::DifferentLostResponse
                ) {
                    Err(arco_core::Error::storage(
                        "remote application succeeded; response lost",
                    ))
                } else {
                    result
                };
                trace
                    .lock()
                    .unwrap()
                    .push(format!("RESPONSE PUT {path} {result:?}"));
                tx.send(result).ok();
            }
            schedule.finished.notify_one();
        });
        rx.await.expect("remote response sender")
    }
    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        let version = self.inner.head(path).await?.map(|meta| meta.version);
        self.trace
            .lock()
            .unwrap()
            .push(format!("ISSUE DELETE {path} {version:?}"));
        if path.contains("/control/v1/") {
            self.authorized_deletes
                .lock()
                .unwrap()
                .insert(path.to_owned());
        }
        let Some(schedule) = self.take(path) else {
            return self.inner.delete(path).await;
        };
        let inner = self.inner.clone();
        let trace = self.trace.clone();
        let path = path.to_owned();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            schedule.issued.notify_one();
            let mut tx = Some(tx);
            if matches!(schedule.fault, Fault::DelayedError) {
                tx.take()
                    .unwrap()
                    .send(Err(arco_core::Error::storage(
                        "DELETE transport failed; remote operation pending",
                    )))
                    .ok();
            }
            if matches!(schedule.fault, Fault::PauseBefore | Fault::DelayedError) {
                schedule.apply.acquire().await.unwrap().forget();
            }
            let result = inner.delete(&path).await;
            trace
                .lock()
                .unwrap()
                .push(format!("APPLY DELETE {path} {result:?}"));
            schedule.applied.notify_one();
            if matches!(schedule.fault, Fault::PauseBefore | Fault::PauseAfter) {
                schedule.respond.acquire().await.unwrap().forget();
            }
            if let Some(tx) = tx {
                let result = if matches!(schedule.fault, Fault::LostResponse) {
                    Err(arco_core::Error::storage(
                        "remote DELETE applied; response lost",
                    ))
                } else {
                    result
                };
                trace
                    .lock()
                    .unwrap()
                    .push(format!("RESPONSE DELETE {path} {result:?}"));
                tx.send(result).ok();
            }
            schedule.finished.notify_one();
        });
        rx.await.expect("remote DELETE response")
    }
    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        Ok(self
            .inner
            .list(prefix)
            .await?
            .into_iter()
            .map(|m| self.meta(m))
            .collect())
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        let mut page = self.inner.list_page(prefix, after, limit).await?;
        for meta in &mut page.objects {
            *meta = self.meta(meta.clone());
        }
        Ok(page)
    }
    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        Ok(self.inner.head(path).await?.map(|m| self.meta(m)))
    }
    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[derive(Debug)]
struct EmptyProviders;
#[async_trait]
impl ProjectionWatermarkProvider for EmptyProviders {
    async fn capture(&self, _: &DomainAuthorityReference) -> Result<ProjectionWatermarkCut> {
        ProjectionWatermarkCut::new(Vec::new(), Vec::new(), Vec::new())
    }
}
#[async_trait]
impl EventArchiveProvider for EmptyProviders {
    async fn capture(&self, authority: &DomainAuthorityReference) -> Result<EventArchiveCapture> {
        EventArchiveCapture::new(DomainEventArchive::empty(authority.domain())?, Vec::new())
    }
}
struct Fixture {
    backend: Arc<Backend>,
    storage: ScopedStorage,
    store: Arc<ControlMvpStateStore>,
    service: Arc<WorkspaceSnapshotService>,
    worker: ControlMvpMaintenanceWorker,
    start: DateTime<Utc>,
}
impl Fixture {
    async fn new() -> Self {
        let backend = Backend::new();
        let start = *backend.now.lock().unwrap();
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = Arc::new(ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap());
        let registry = WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").unwrap(),
            vec![
                WorkspaceDomainBinding::new(
                    scope.clone(),
                    store.clone(),
                    store.clone(),
                    Arc::new(EmptyProviders),
                    Arc::new(EmptyProviders),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let clock = backend.now.clone();
        let service = Arc::new(
            WorkspaceSnapshotService::new(storage.clone(), registry)
                .unwrap()
                .with_clock(Arc::new(move || *clock.lock().unwrap())),
        );
        let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
        let f = Self {
            backend,
            storage,
            store,
            service,
            worker,
            start,
        };
        f.commit(b"value").await;
        f
    }
    async fn commit(&self, value: &[u8]) {
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        txn.put(b"key", Bytes::copy_from_slice(value))
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }
    fn snapshot(&self) -> CreateWorkspaceSnapshotRequest {
        CreateWorkspaceSnapshotRequest::new(
            SNAP,
            PIN,
            self.start,
            self.start + chrono::Duration::days(90),
            None,
        )
        .unwrap()
    }
    fn export(&self) -> CreateWorkspaceExportRequest {
        CreateWorkspaceExportRequest::new(
            EXP,
            EPIN,
            SNAP,
            PIN,
            self.start,
            self.start + chrono::Duration::days(60),
        )
        .unwrap()
    }
    async fn epoch(&self) -> String {
        let value: serde_json::Value = serde_json::from_slice(
            &self
                .storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap(),
        )
        .unwrap();
        value.get("state").unwrap().as_str().unwrap().to_owned()
    }
    async fn age_epoch(&self) {
        let mut epoch: serde_json::Value = serde_json::from_slice(
            &self
                .storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap(),
        )
        .unwrap();
        *epoch.get_mut("started_at").unwrap() =
            serde_json::to_value(Utc::now() - chrono::Duration::hours(1)).unwrap();
        self.storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                Bytes::from(serde_json::to_vec(&epoch).unwrap()),
                WritePrecondition::None,
            )
            .await
            .unwrap();
    }
    async fn assert_protected(&self, export: bool) {
        self.worker
            .collect_gc_at(self.start + chrono::Duration::days(31), Vec::new())
            .await
            .unwrap();
        let domains = if export {
            self.service
                .get_export(EXP)
                .await
                .unwrap()
                .domains()
                .to_vec()
        } else {
            self.service
                .get_snapshot(SNAP)
                .await
                .unwrap()
                .domains()
                .to_vec()
        };
        for domain in domains {
            let reader = self
                .store
                .resolve_persisted_reference_at(
                    domain.authority(),
                    self.start + chrono::Duration::days(31),
                )
                .await
                .unwrap();
            assert_eq!(
                reader.get(b"key").await.unwrap(),
                Some(Bytes::from_static(b"value"))
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Class {
    Snapshot,
    SnapshotRetry,
    Export,
    ExportRetry,
}
impl Class {
    fn export(self) -> bool {
        matches!(self, Self::Export | Self::ExportRetry)
    }
    fn retry(self) -> bool {
        matches!(self, Self::SnapshotRetry | Self::ExportRetry)
    }
    fn boundaries(self) -> Vec<String> {
        let pin = if self.export() { EPIN } else { PIN };
        let mut paths = vec![
            retention_pin_revision_path(pin, 1).unwrap(),
            retention_pin_latest_path(pin).unwrap(),
        ];
        if !self.retry() {
            paths.insert(
                0,
                if self.export() {
                    export_record_path(EXP).unwrap()
                } else {
                    snapshot_record_path(SNAP).unwrap()
                },
            );
        }
        paths
    }
    async fn setup(self, f: &Fixture) -> Option<Bytes> {
        if self.export() || self.retry() {
            f.service.create_snapshot(&f.snapshot()).await.unwrap();
        }
        if matches!(self, Self::ExportRetry) {
            f.service.export_snapshot(&f.export()).await.unwrap();
        }
        if self.retry() {
            let pin = if self.export() { EPIN } else { PIN };
            f.storage
                .delete(&retention_pin_latest_path(pin).unwrap())
                .await
                .unwrap();
            f.storage
                .delete(&retention_pin_revision_path(pin, 1).unwrap())
                .await
                .unwrap();
            let path = if self.export() {
                export_record_path(EXP).unwrap()
            } else {
                snapshot_record_path(SNAP).unwrap()
            };
            let bytes = f.storage.get_raw(&path).await.unwrap();
            f.commit(b"successor").await;
            Some(bytes)
        } else {
            None
        }
    }
    async fn publish(
        self,
        service: &WorkspaceSnapshotService,
        snapshot: &CreateWorkspaceSnapshotRequest,
        export: &CreateWorkspaceExportRequest,
    ) -> Result<()> {
        if self.export() {
            service.export_snapshot(export).await.map(|_| ())
        } else {
            service.create_snapshot(snapshot).await.map(|_| ())
        }
    }
}

#[tokio::test]
async fn exact_immutable_readback_reconciles_all_publication_classes() {
    for class in [
        Class::Snapshot,
        Class::SnapshotRetry,
        Class::Export,
        Class::ExportRetry,
    ] {
        for boundary in class.boundaries() {
            let f = Fixture::new().await;
            let original = class.setup(&f).await;
            let schedule = f.backend.arm(boundary.clone(), 0, Fault::LostResponse);
            let result = class.publish(&f.service, &f.snapshot(), &f.export()).await;
            assert!(
                result.is_ok(),
                "{class:?} {boundary}: {result:?}\n{:?}",
                f.backend.trace.lock().unwrap()
            );
            schedule.finish().await;
            assert_eq!(f.epoch().await, "IDLE");
            class
                .publish(&f.service, &f.snapshot(), &f.export())
                .await
                .unwrap();
            if let Some(original) = original {
                let path = if class.export() {
                    export_record_path(EXP).unwrap()
                } else {
                    snapshot_record_path(SNAP).unwrap()
                };
                assert_eq!(
                    f.storage.get_raw(&path).await.unwrap(),
                    original,
                    "retry preserves cut and pin identity"
                );
            }
            f.assert_protected(class.export()).await;
        }
    }
}

#[tokio::test]
async fn unresolved_publications_exclude_gc_even_after_cancellation_and_lease_expiry() {
    for class in [
        Class::Snapshot,
        Class::SnapshotRetry,
        Class::Export,
        Class::ExportRetry,
    ] {
        for boundary in class.boundaries() {
            for fault in [
                Fault::PauseBefore,
                Fault::PauseAfter,
                Fault::DelayedError,
                Fault::Unreadable,
            ] {
                let f = Fixture::new().await;
                class.setup(&f).await;
                let schedule = f.backend.arm(boundary.clone(), 0, fault);
                let service = f.service.clone();
                let snapshot = f.snapshot();
                let export = f.export();
                let task =
                    tokio::spawn(async move { class.publish(&service, &snapshot, &export).await });
                schedule.issued().await;
                if matches!(fault, Fault::PauseAfter) {
                    guard(schedule.applied.notified()).await;
                }
                if matches!(fault, Fault::PauseBefore | Fault::PauseAfter) {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    assert!(guard(task).await.unwrap().is_err());
                }
                *f.backend.denied.lock().unwrap() = None;
                f.backend.expire_lease(&f.storage).await;
                f.age_epoch().await;
                assert_eq!(
                    f.epoch().await,
                    "IN_FLIGHT",
                    "{class:?} {boundary} {fault:?}"
                );
                let result = f
                    .worker
                    .collect_gc_at(f.start + chrono::Duration::days(31), Vec::new())
                    .await;
                assert!(
                    result.is_err(),
                    "unresolved publication cannot authorize GC"
                );
                assert!(f.backend.authorized_deletes.lock().unwrap().is_empty());
                // Resolve every outstanding remote operation BEFORE operator recovery.
                schedule.finish().await;
                *f.backend.denied.lock().unwrap() = None;
                recover_stale_retention_epoch(
                    &f.storage,
                    "all simulated publication operations completed and reconciled",
                )
                .await
                .unwrap();
                class
                    .publish(&f.service, &f.snapshot(), &f.export())
                    .await
                    .unwrap();
                f.assert_protected(class.export()).await;
            }
        }
    }
}

#[tokio::test]
async fn expiration_while_waiting_for_coordination_rejects_every_publication_class() {
    for class in [
        Class::Snapshot,
        Class::SnapshotRetry,
        Class::Export,
        Class::ExportRetry,
    ] {
        let f = Fixture::new().await;
        class.setup(&f).await;
        let schedule = f
            .backend
            .arm(RETENTION_GC_LOCK_PATH.to_owned(), 0, Fault::PauseBefore);
        let service = f.service.clone();
        let snapshot = f.snapshot();
        let export = f.export();
        let task = tokio::spawn(async move { class.publish(&service, &snapshot, &export).await });
        schedule.issued().await;
        *f.backend.now.lock().unwrap() = f.start + chrono::Duration::days(91);
        schedule.finish().await;
        assert!(
            guard(task).await.unwrap().is_err(),
            "{class:?}: retention expired while acquiring coordination"
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
async fn checkpoint_artifacts_and_record_obey_publication_exclusion() {
    for boundary in [
        "/segments/l1/state-checkpoint-",
        "/indexes/state-checkpoint-",
        "/checkpoints/checkpoint-",
    ] {
        for fault in [
            Fault::PauseBefore,
            Fault::PauseAfter,
            Fault::LostResponse,
            Fault::DelayedError,
            Fault::Unreadable,
        ] {
            let f = Fixture::new().await;
            let schedule = f.backend.arm(boundary.to_owned(), 0, fault);
            let service = f.service.clone();
            let request = f.snapshot();
            let task = tokio::spawn(async move { service.create_snapshot(&request).await });
            schedule.issued().await;
            if matches!(fault, Fault::PauseAfter) {
                guard(schedule.applied.notified()).await;
            }
            let success =
                matches!(fault, Fault::LostResponse) && boundary.contains("/checkpoints/");
            if matches!(fault, Fault::PauseBefore | Fault::PauseAfter) {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert_eq!(
                    guard(task).await.unwrap().is_ok(),
                    success,
                    "{boundary} {fault:?}"
                );
            }
            *f.backend.denied.lock().unwrap() = None;
            f.backend.expire_lease(&f.storage).await;
            if !success {
                f.age_epoch().await;
                assert_eq!(f.epoch().await, "IN_FLIGHT");
                assert!(
                    f.worker
                        .collect_gc_at(f.start + chrono::Duration::days(31), Vec::new())
                        .await
                        .is_err()
                );
            }
            schedule.finish().await;
            *f.backend.denied.lock().unwrap() = None;
            if !success {
                recover_stale_retention_epoch(
                    &f.storage,
                    "checkpoint remote task completed; partial cut reconciled",
                )
                .await
                .unwrap();
            }
            f.service.create_snapshot(&f.snapshot()).await.unwrap();
            f.assert_protected(false).await;
        }
    }
}

#[tokio::test]
async fn lost_epoch_claim_and_settlement_responses_are_conservative() {
    for class in [
        Class::Snapshot,
        Class::SnapshotRetry,
        Class::Export,
        Class::ExportRetry,
    ] {
        for skip in [0, 1] {
            for fault in [
                Fault::LostResponse,
                Fault::Unreadable,
                Fault::DelayedError,
                Fault::PauseBefore,
                Fault::PauseAfter,
            ] {
                let f = Fixture::new().await;
                class.setup(&f).await;
                let schedule = f
                    .backend
                    .arm(RETENTION_MUTATION_EPOCH_PATH.to_owned(), skip, fault);
                let service = f.service.clone();
                let snapshot = f.snapshot();
                let export = f.export();
                let task =
                    tokio::spawn(async move { class.publish(&service, &snapshot, &export).await });
                schedule.issued().await;
                if matches!(fault, Fault::PauseAfter) {
                    guard(schedule.applied.notified()).await;
                }
                if matches!(fault, Fault::PauseBefore | Fault::PauseAfter) {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    assert!(guard(task).await.unwrap().is_err());
                }
                *f.backend.denied.lock().unwrap() = None;
                f.backend.expire_lease(&f.storage).await;
                // A not-yet-applied claim cannot have issued any publication. A
                // landed settlement means every preceding mutation completed.
                if skip == 0
                    && matches!(
                        fault,
                        Fault::LostResponse | Fault::Unreadable | Fault::PauseAfter
                    )
                    || skip == 1 && matches!(fault, Fault::DelayedError | Fault::PauseBefore)
                {
                    assert_eq!(f.epoch().await, "IN_FLIGHT");
                    assert!(
                        f.worker
                            .collect_gc_at(f.start + chrono::Duration::days(31), Vec::new())
                            .await
                            .is_err()
                    );
                }
                schedule.finish().await;
                *f.backend.denied.lock().unwrap() = None;
                recover_stale_retention_epoch(
                    &f.storage,
                    "epoch remote task terminal; no publication requests pending",
                )
                .await
                .unwrap();
                class
                    .publish(&f.service, &f.snapshot(), &f.export())
                    .await
                    .unwrap();
                f.assert_protected(class.export()).await;
            }
        }
    }
}

#[tokio::test]
async fn delayed_delete_survives_collector_cancellation_restart_and_new_retained_publication() {
    let f = Fixture::new().await;
    let orphan = f.store.paths().tx_object("orphan-before-restart");
    f.storage
        .put_raw(&orphan, Bytes::new(), WritePrecondition::DoesNotExist)
        .await
        .unwrap();
    let schedule = f.backend.arm(orphan.clone(), 0, Fault::PauseBefore);
    let worker = ControlMvpMaintenanceWorker::new(
        f.storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    let now = f.start + chrono::Duration::days(8);
    let task = tokio::spawn(async move { worker.collect_gc_at(now, Vec::new()).await });
    schedule.issued().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    f.backend.expire_lease(&f.storage).await;
    let mut epoch: serde_json::Value = serde_json::from_slice(
        &f.storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .unwrap(),
    )
    .unwrap();
    *epoch.get_mut("started_at").unwrap() =
        serde_json::to_value(Utc::now() - chrono::Duration::hours(1)).unwrap();
    f.storage
        .put_raw(
            RETENTION_MUTATION_EPOCH_PATH,
            Bytes::from(serde_json::to_vec(&epoch).unwrap()),
            WritePrecondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        f.worker
            .collect_gc_at(now, Vec::new())
            .await
            .unwrap()
            .objects_deleted(),
        1
    );
    f.commit(b"value").await;
    f.service.create_snapshot(&f.snapshot()).await.unwrap();
    f.service.export_snapshot(&f.export()).await.unwrap();
    schedule.finish().await;
    assert!(f.storage.head_raw(&orphan).await.unwrap().is_none());
    f.assert_protected(false).await;
    f.assert_protected(true).await;
}

async fn release_pin(f: &Fixture, pin: &str, now: DateTime<Utc>) {
    let revision = decode_retention_pin_revision(
        &f.storage
            .get_raw(&retention_pin_revision_path(pin, 1).unwrap())
            .await
            .unwrap(),
    )
    .unwrap();
    let released = revision.release(2, now).unwrap();
    select_revision(f, &released).await;
}
async fn select_revision(f: &Fixture, revision: &RetentionPinRevision) {
    let bytes = encode_retention_pin_revision(revision).unwrap();
    let path = retention_pin_revision_path(revision.pin_id(), revision.revision()).unwrap();
    f.storage
        .put_raw(
            &path,
            Bytes::copy_from_slice(&bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let selector = RetentionPinLatest::new(
        revision.pin_id(),
        revision.revision(),
        path,
        format!("sha256:{}", hex::encode(Sha256::digest(&bytes))),
    )
    .unwrap();
    f.storage
        .put_raw(
            &retention_pin_latest_path(revision.pin_id()).unwrap(),
            Bytes::from(encode_retention_pin_latest(&selector).unwrap()),
            WritePrecondition::None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn released_source_while_waiting_cannot_publish_or_retry_an_export() {
    for class in [Class::Export, Class::ExportRetry] {
        let f = Fixture::new().await;
        class.setup(&f).await;
        let schedule = f
            .backend
            .arm(RETENTION_GC_LOCK_PATH.to_owned(), 0, Fault::PauseBefore);
        let service = f.service.clone();
        let snapshot = f.snapshot();
        let export = f.export();
        let task = tokio::spawn(async move { class.publish(&service, &snapshot, &export).await });
        schedule.issued().await;
        release_pin(&f, PIN, f.start).await;
        schedule.finish().await;
        assert!(guard(task).await.unwrap().is_err());
        assert!(
            f.storage
                .head_raw(&retention_pin_latest_path(EPIN).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
}

type Contents = BTreeMap<Vec<u8>, Bytes>;
struct TokenOracle {
    logical: LogicalOracle,
    token: StateToken,
    contents: Contents,
    until: DateTime<Utc>,
}
struct CheckpointOracle {
    logical: LogicalOracle,
    manifest_id: String,
    token: CheckpointToken,
    contents: Contents,
    until: DateTime<Utc>,
}
struct RootOracle {
    logical: LogicalOracle,
    snapshot: CreateWorkspaceSnapshotRequest,
    export: Option<CreateWorkspaceExportRequest>,
    contents: Contents,
    released: bool,
    bytes: Bytes,
}
impl RootOracle {
    fn until(&self) -> DateTime<Utc> {
        self.export.as_ref().map_or(
            self.snapshot.retained_until(),
            CreateWorkspaceExportRequest::retained_until,
        )
    }
    fn pin(&self) -> &str {
        self.export
            .as_ref()
            .map_or(self.snapshot.pin_id(), CreateWorkspaceExportRequest::pin_id)
    }
    fn active(&self, now: DateTime<Utc>) -> bool {
        !self.released && now < self.until()
    }
    async fn publish(&self, f: &Fixture) -> Result<()> {
        if let Some(export) = &self.export {
            f.service.export_snapshot(export).await.map(|_| ())
        } else {
            f.service.create_snapshot(&self.snapshot).await.map(|_| ())
        }
    }
    async fn path_bytes(&self, f: &Fixture) -> Bytes {
        let path = self.export.as_ref().map_or_else(
            || snapshot_record_path(self.snapshot.snapshot_id()).unwrap(),
            |e| export_record_path(e.export_id()).unwrap(),
        );
        f.storage.get_raw(&path).await.unwrap()
    }
}
async fn compare(reader: &dyn ArcoStateReader, expected: &Contents) {
    for key in [b"key".as_slice(), b"a", b"b", b"c"] {
        assert_eq!(
            reader.get(key).await.unwrap(),
            expected.get(key).cloned(),
            "logical oracle key {key:?}"
        );
    }
}
fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
// Keep operation generation and oracle updates together for schedule review.
#[allow(clippy::cognitive_complexity)]
async fn run_model(seed: u64, trace: &Mutex<Vec<String>>, durable: bool) {
    let mut f = Fixture::new().await;
    let orphan = f.store.paths().tx_object("model-orphan");
    f.storage
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let mut random = seed;
    let mut now = f.start;
    let mut contents = Contents::from([(b"key".to_vec(), Bytes::from_static(b"value"))]);
    let mut logical = LogicalOracle::new();
    logical.commit(
        vec![(b"key".to_vec(), Some(Bytes::from_static(b"value")))],
        Vec::new(),
        Vec::new(),
    );
    let mut tokens = vec![TokenOracle {
        logical: logical.clone(),
        token: f.store.current_state_token().await.unwrap(),
        contents: contents.clone(),
        until: now + chrono::Duration::days(30),
    }];
    let mut checkpoints: Vec<CheckpointOracle> = Vec::new();
    let mut roots: Vec<RootOracle> = Vec::new();
    let families = if durable { 18 } else { 12 };
    let mut counts = vec![0_u32; families];
    let mut job = None;
    for step in 0..if durable { 128 } else { 64 } {
        // Each seed exercises every operation family once, then a fixed PRNG
        // drives ordering. The oracle never calls the collector's planner.
        let op = if step < families {
            step
        } else {
            usize::try_from(next_random(&mut random) % families as u64).unwrap()
        };
        *counts.get_mut(op).unwrap() += 1;
        trace.lock().unwrap().push(format!(
            "seed={seed} step={step} op={op} now={now} keys={:?}",
            contents.keys().collect::<Vec<_>>()
        ));
        match op {
            0..=2 => {
                // Maintenance runs before the existing L0 backpressure limit.
                durable_maintenance::consolidate_pending(
                    &arco_catalog::DurableMaintenanceWorker::new(
                        f.storage.clone(),
                        StateScope::new("tenant", "workspace", "catalog"),
                        arco_catalog::DurableAuthorityBinding::new([17; 32]),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
                let mut txn = f
                    .store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap();
                let key = [b"a", b"b", b"c"]
                    .get(usize::try_from(next_random(&mut random) % 3).unwrap())
                    .unwrap()
                    .to_vec();
                if op == 2 {
                    txn.delete(&key).await.unwrap();
                    contents.remove(&key);
                } else {
                    let value = Bytes::from(format!("{seed}-{step}"));
                    txn.put(&key, value.clone()).await.unwrap();
                    contents.insert(key.clone(), value);
                }
                let write = (key.clone(), contents.get(&key).cloned());
                let mut additions = Vec::new();
                let mut trims = Vec::new();
                if op == 0 && logical.outbox.is_empty() {
                    let payload = Bytes::from(format!("incarnation-{seed}-{step}"));
                    txn.stage_projection_outbox(
                        arco_catalog::ControlMvpProjectionOutboxRecord::new(
                            "model-event",
                            payload.clone(),
                        ),
                    )
                    .await
                    .unwrap();
                    additions.push(("model-event".to_string(), payload));
                } else if op == 1
                    && let Some((id, _, origin)) = logical.outbox.first()
                {
                    txn.trim_projection_outbox([arco_catalog::ControlMvpOutboxTrimTarget::new(
                        id, *origin,
                    )])
                    .await
                    .unwrap();
                    trims.push((id.clone(), *origin));
                }
                let token = txn.commit().await.unwrap().state_token().clone();
                logical.commit(vec![write], additions, trims);
                tokens.push(TokenOracle {
                    logical: logical.clone(),
                    token,
                    contents: contents.clone(),
                    until: now + chrono::Duration::days(30),
                });
            }
            3 => {
                let token = f
                    .store
                    .checkpoint(CheckpointOptions::default())
                    .await
                    .unwrap();
                checkpoints.push(CheckpointOracle {
                    logical: logical.clone(),
                    manifest_id: f
                        .store
                        .current_state_token()
                        .await
                        .unwrap()
                        .authority_manifest_id()
                        .to_string(),
                    token,
                    contents: contents.clone(),
                    until: now + chrono::Duration::days(30),
                });
            }
            4 => {
                let id = ulid::Ulid::from(u128::from(seed) * 1000 + u128::try_from(step).unwrap());
                let request = CreateWorkspaceSnapshotRequest::new(
                    format!("snap_{id}"),
                    format!("pin_{id}"),
                    now,
                    now + chrono::Duration::days(40),
                    None,
                )
                .unwrap();
                f.service.create_snapshot(&request).await.unwrap();
                let bytes = f
                    .storage
                    .get_raw(&snapshot_record_path(request.snapshot_id()).unwrap())
                    .await
                    .unwrap();
                roots.push(RootOracle {
                    logical: logical.clone(),
                    snapshot: request,
                    export: None,
                    contents: contents.clone(),
                    released: false,
                    bytes,
                });
            }
            5 => {
                if let Some(source) = roots
                    .iter()
                    .rev()
                    .find(|r| r.export.is_none() && r.active(now))
                {
                    let id =
                        ulid::Ulid::from(u128::from(seed) * 1000 + u128::try_from(step).unwrap());
                    let request = CreateWorkspaceExportRequest::new(
                        format!("exp_{id}"),
                        format!("pin_{id}"),
                        source.snapshot.snapshot_id(),
                        source.pin(),
                        now,
                        source.until(),
                    )
                    .unwrap();
                    f.service.export_snapshot(&request).await.unwrap();
                    let bytes = f
                        .storage
                        .get_raw(&export_record_path(request.export_id()).unwrap())
                        .await
                        .unwrap();
                    roots.push(RootOracle {
                        logical: source.logical.clone(),
                        snapshot: source.snapshot.clone(),
                        export: Some(request),
                        contents: source.contents.clone(),
                        released: false,
                        bytes,
                    });
                }
            }
            6 => {
                if !roots.is_empty() {
                    let i = usize::try_from(next_random(&mut random)).unwrap() % roots.len();
                    let root = roots.get(i).unwrap();
                    let source_active = root.export.as_ref().is_none_or(|e| {
                        roots.iter().any(|r| {
                            r.export.is_none()
                                && r.snapshot.snapshot_id() == e.snapshot_id()
                                && r.active(now)
                        })
                    });
                    if root.active(now) && source_active {
                        f.storage
                            .delete(&retention_pin_latest_path(root.pin()).unwrap())
                            .await
                            .unwrap();
                        f.storage
                            .delete(&retention_pin_revision_path(root.pin(), 1).unwrap())
                            .await
                            .unwrap();
                    }
                    assert_eq!(
                        root.publish(&f).await.is_ok(),
                        root.active(now) && source_active
                    );
                    assert_eq!(
                        root.path_bytes(&f).await,
                        root.bytes,
                        "retry must preserve original cut and pin identity"
                    );
                }
            }
            7 => {
                if let Some(root) = roots.iter_mut().find(|r| r.active(now)) {
                    release_pin(&f, root.pin(), now).await;
                    root.released = true;
                }
            }
            8 => {
                now += chrono::Duration::days(11);
                *f.backend.now.lock().unwrap() = now;
            }
            9 => {
                durable_maintenance::consolidate_pending(
                    &arco_catalog::DurableMaintenanceWorker::new(
                        f.storage.clone(),
                        StateScope::new("tenant", "workspace", "catalog"),
                        arco_catalog::DurableAuthorityBinding::new([17; 32]),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            }
            10 => {
                let mut cursor = None;
                loop {
                    let result = f
                        .worker
                        .collect_gc_page_at(now, Vec::new(), cursor.as_deref())
                        .await
                        .unwrap();
                    cursor = result.continuation().map(str::to_owned);
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            11 => {
                f.store = Arc::new(
                    ControlMvpStateStore::new(
                        f.storage.clone(),
                        StateScope::new("tenant", "workspace", "catalog"),
                    )
                    .unwrap(),
                );
            }
            12..=15 | 17 => {
                durable_model_step(&f, &mut job, now, op, trace).await;
            }
            16 => {
                use arco_catalog::{
                    ControlMvpRestoreParticipant, RestoreAttemptIdentity,
                    StateRestoreParticipant as _,
                };
                #[derive(serde::Serialize)]
                struct Notice<'a> {
                    restore_id: &'a str,
                    participant_attempt: u64,
                    domain: &'a str,
                    source_logical_sequence: u64,
                    result_logical_sequence: u64,
                }
                let checkpoint = f
                    .store
                    .checkpoint(CheckpointOptions::default())
                    .await
                    .unwrap();
                let reference = f
                    .store
                    .persist_checkpoint_reference(&checkpoint, now + chrono::Duration::days(1))
                    .await
                    .unwrap();
                let restore_id = format!(
                    "rst_{}",
                    ulid::Ulid::from(u128::from(seed) * 1000 + step as u128)
                );
                let identity = RestoreAttemptIdentity::new(&restore_id, 1, "catalog").unwrap();
                let participant = ControlMvpRestoreParticipant::new(f.store.as_ref().clone());
                let plan = participant
                    .plan_restore(&reference, &identity, now)
                    .await
                    .unwrap();
                participant.apply_restore(&plan, now).await.unwrap();
                let request = format!("restore:{restore_id}:1:catalog");
                let notice = Bytes::from(
                    serde_json::to_vec(&Notice {
                        restore_id: &restore_id,
                        participant_attempt: 1,
                        domain: "catalog",
                        source_logical_sequence: logical.sequence,
                        result_logical_sequence: logical.sequence + 1,
                    })
                    .unwrap(),
                );
                logical.commit_with_request(
                    Vec::new(),
                    vec![(request.clone(), notice)],
                    Vec::new(),
                    Some(&request),
                );
                tokens.push(TokenOracle {
                    logical: logical.clone(),
                    token: f.store.current_state_token().await.unwrap(),
                    contents: contents.clone(),
                    until: now + chrono::Duration::days(30),
                });
            }
            _ => unreachable!(),
        }
        trace
            .lock()
            .unwrap()
            .push(format!("acknowledged contents={contents:?}"));
        compare(f.store.as_ref(), &contents).await;
        logical
            .assert_manifest(
                &f.storage,
                f.store
                    .current_state_token()
                    .await
                    .unwrap()
                    .authority_manifest_id(),
            )
            .await;
        let actual_outbox = f.store.current_projection_outbox().await.unwrap();
        assert_eq!(
            actual_outbox
                .iter()
                .map(|record| (
                    record.record_id().to_string(),
                    record.payload().clone(),
                    record.origin_sequence().unwrap()
                ))
                .collect::<Vec<_>>(),
            logical.outbox
        );
        for token in &tokens {
            if now <= token.until {
                token
                    .logical
                    .assert_manifest(&f.storage, token.token.authority_manifest_id())
                    .await;
                compare(
                    f.store.read_at(token.token.clone()).await.unwrap().as_ref(),
                    &token.contents,
                )
                .await;
            }
        }
        for checkpoint in &checkpoints {
            if now <= checkpoint.until {
                checkpoint
                    .logical
                    .assert_manifest(&f.storage, &checkpoint.manifest_id)
                    .await;
                compare(
                    f.store
                        .read_checkpoint(checkpoint.token.clone())
                        .await
                        .unwrap()
                        .as_ref(),
                    &checkpoint.contents,
                )
                .await;
            }
        }
        for root in roots.iter().filter(|r| r.active(now)) {
            let (domains, required) = if let Some(export) = &root.export {
                let record = f.service.get_export(export.export_id()).await.unwrap();
                (
                    record.domains().to_vec(),
                    record.required_objects().to_vec(),
                )
            } else {
                let record = f
                    .service
                    .get_snapshot(root.snapshot.snapshot_id())
                    .await
                    .unwrap();
                (
                    record.domains().to_vec(),
                    record.required_objects().to_vec(),
                )
            };
            for object in required {
                assert!(
                    !f.backend
                        .authorized_deletes
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|path| path.ends_with(object.relative_path())),
                    "retained reference revived a deletion-authorized object: {}",
                    object.relative_path()
                );
            }
            for domain in domains {
                root.logical
                    .assert_manifest(&f.storage, domain.authority().manifest_id())
                    .await;
                compare(
                    f.store
                        .resolve_persisted_reference_at(domain.authority(), now)
                        .await
                        .unwrap()
                        .as_ref(),
                    &root.contents,
                )
                .await;
            }
        }
    }
    assert!(counts.into_iter().all(|count| count > 0));
    assert!(
        f.storage.head_raw(&orphan).await.unwrap().is_none(),
        "model must perform real reclamation"
    );
}

async fn durable_model_step(
    f: &Fixture,
    job: &mut Option<(arco_catalog::MaintenanceJobId, DateTime<Utc>)>,
    now: DateTime<Utc>,
    op: usize,
    trace: &Mutex<Vec<String>>,
) {
    use arco_catalog::{DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceStatus};
    let worker = DurableMaintenanceWorker::new(
        f.storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
        DurableAuthorityBinding::new([31; 32]),
    )
    .unwrap()
    .with_test_segment_sizing(2, 8 * 1024)
    .unwrap();
    if job.is_none() {
        let before = f.backend.trace.lock().unwrap().len();
        let plan = match worker.test_prepare_forced_at(now).await {
            Ok(Some(plan)) => plan,
            Err(error @ arco_catalog::CatalogError::MaintenanceBackpressure { .. }) => {
                assert!(
                    !f.backend
                        .trace
                        .lock()
                        .unwrap()
                        .iter()
                        .skip(before)
                        .any(|entry| entry.starts_with("ISSUE PUT")),
                    "capacity rejection wrote an artifact"
                );
                trace
                    .lock()
                    .unwrap()
                    .push(format!("admission rejected before PUT: {error:?}"));
                return;
            }
            result => panic!(
                "unexpected durable admission: {}",
                result
                    .err()
                    .map_or_else(|| "no source".into(), |error| error.to_string())
            ),
        };
        let id = plan.job_id().clone();
        trace
            .lock()
            .unwrap()
            .push(format!("prepared job={}", id.as_str()));
        worker.start_at(&plan, now).await.unwrap();
        *job = Some((id, now));
    }
    let (id, created_at) = job.as_ref().unwrap();
    let created_at = *created_at;
    let operation = async {
        let progress = worker.resume_at(id, now).await?;
        trace.lock().unwrap().push(format!("job={} status={:?} completed={}", id.as_str(), progress.status, progress.completed));
        if op == 15 && matches!(progress.status, MaintenanceStatus::Active | MaintenanceStatus::ReadyToPublish) {
            worker.abandon_at(id, now).await?;
            return Ok(true);
        }
        if op == 17 {
            trace.lock().unwrap().push("INJECT LostResponse on selected.json; immutable work remains selected only after exact reconciliation".into());
            f.backend.arm("/selected.json".into(), 0, Fault::LostResponse);
        }
        match progress.status {
            MaintenanceStatus::Active => { worker.advance_at(id, now).await?; Ok(false) }
            MaintenanceStatus::ReadyToPublish | MaintenanceStatus::Publishing => Ok(worker.publish_at(id, now).await?.is_some()),
            MaintenanceStatus::Published | MaintenanceStatus::Abandoned | MaintenanceStatus::Superseded => Ok(true),
            _ => panic!("unexpected durable model state: {:?}", progress.status),
        }
    }.await;
    *f.backend.armed.lock().unwrap() = None;
    trace
        .lock()
        .unwrap()
        .extend(std::mem::take(&mut f.backend.trace.lock().unwrap().events));
    match operation {
        Ok(true) => *job = None,
        Ok(false) => {}
        Err(arco_catalog::CatalogError::PreconditionFailed { .. }) => {
            // Expiry, a restore, competing maintenance or a GC generation consumes
            // reuse. Abandon while executable; expired protection retains its fixed deadline.
            let _ = worker.abandon_at(id, now).await;
            *job = None;
        }
        Err(error @ arco_catalog::CatalogError::NotFound { .. }) => {
            assert!(
                now >= created_at + chrono::Duration::days(8),
                "live maintenance evidence disappeared: {error:?}"
            );
            trace.lock().unwrap().push(format!(
                "expired job correctly returned missing evidence after reclamation: {error:?}"
            ));
            *job = None;
        }
        Err(error) => panic!("durable model error: {error:?}"),
    }
}

#[tokio::test]
async fn durable_maintenance_model_32_seeds_of_128_operations() {
    for seed in 1..=32 {
        let trace = Mutex::new(Vec::new());
        let result = std::panic::AssertUnwindSafe(Box::pin(run_model(seed, &trace, true)))
            .catch_unwind()
            .await;
        assert!(
            result.is_ok(),
            "durable model failed seed={seed}\n{}",
            trace.lock().unwrap().join("\n")
        );
    }
}

#[tokio::test]
async fn independent_reclamation_model_32_seeds_of_64_operations() {
    for seed in 1..=32 {
        let trace = Mutex::new(Vec::new());
        let result = std::panic::AssertUnwindSafe(Box::pin(run_model(seed, &trace, false)))
            .catch_unwind()
            .await;
        assert!(
            result.is_ok(),
            "model failed seed={seed}\n{}",
            trace.lock().unwrap().join("\n")
        );
    }
}

#[tokio::test]
async fn immutable_preconditions_and_transport_readback_require_exact_bytes() {
    for class in [
        Class::Snapshot,
        Class::SnapshotRetry,
        Class::Export,
        Class::ExportRetry,
    ] {
        for boundary in class.boundaries() {
            for fault in [
                Fault::IdenticalWinner,
                Fault::DifferentWinner,
                Fault::DifferentLostResponse,
            ] {
                let f = Fixture::new().await;
                class.setup(&f).await;
                let schedule = f.backend.arm(boundary.clone(), 0, fault);
                let result = class.publish(&f.service, &f.snapshot(), &f.export()).await;
                schedule.finish().await;
                match fault {
                    Fault::IdenticalWinner => {
                        result.unwrap();
                        assert_eq!(f.epoch().await, "IDLE");
                        f.assert_protected(class.export()).await;
                    }
                    Fault::DifferentWinner => {
                        assert!(
                            matches!(
                                result,
                                Err(arco_catalog::CatalogError::PreconditionFailed { .. })
                            ),
                            "{class:?} {boundary}: {result:?}"
                        );
                        assert_eq!(
                            f.epoch().await,
                            "IDLE",
                            "terminal create conflict needs no recovery"
                        );
                    }
                    Fault::DifferentLostResponse => {
                        assert!(result.is_err());
                        assert_eq!(
                            f.epoch().await,
                            "IN_FLIGHT",
                            "different readback cannot resolve transport uncertainty"
                        );
                        assert!(
                            f.worker
                                .collect_gc_at(f.start + chrono::Duration::days(31), Vec::new())
                                .await
                                .is_err()
                        );
                        assert!(f.backend.authorized_deletes.lock().unwrap().is_empty());
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[path = "support/legacy_reclamation.rs"]
mod legacy_reclamation;

#[tokio::test]
async fn independent_oracle_covers_empty_and_nonempty_restore_history() {
    use arco_catalog::{
        ControlMvpOutboxTrimTarget, ControlMvpProjectionOutboxRecord, ControlMvpRestoreParticipant,
        RestoreAttemptIdentity, StateRestoreParticipant as _,
    };
    #[derive(serde::Serialize)]
    struct Notice<'a> {
        restore_id: &'a str,
        participant_attempt: u64,
        domain: &'a str,
        source_logical_sequence: u64,
        result_logical_sequence: u64,
    }
    for empty in [false, true] {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let mut source = LogicalOracle::new();
        let source_writes = vec![
            (b"changed".to_vec(), Some(Bytes::from_static(b"source"))),
            (b"keep".to_vec(), Some(Bytes::from_static(b"same"))),
            (b"source-only".to_vec(), Some(Bytes::from_static(b"source"))),
            (b"gone".to_vec(), None),
        ];
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        for (key, value) in &source_writes {
            match value {
                Some(value) => tx.put(key, value.clone()).await.unwrap(),
                None => tx.delete(key).await.unwrap(),
            }
        }
        tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "event",
            Bytes::from_static(b"source incarnation"),
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        source.commit(
            source_writes.clone(),
            vec![("event".into(), Bytes::from_static(b"source incarnation"))],
            Vec::new(),
        );
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let reference = store
            .persist_checkpoint_reference(&checkpoint, Utc::now() + chrono::Duration::days(1))
            .await
            .unwrap();
        let mut expected = source.clone();
        if empty {
            storage
                .delete(&store.paths().current_pointer())
                .await
                .unwrap();
        } else {
            let changes = vec![
                (
                    b"changed".to_vec(),
                    Some(Bytes::from_static(b"destination")),
                ),
                (
                    b"destination-only".to_vec(),
                    Some(Bytes::from_static(b"extra")),
                ),
                (b"gone".to_vec(), Some(Bytes::from_static(b"resurrected"))),
            ];
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            for (key, value) in &changes {
                tx.put(key, value.clone().unwrap()).await.unwrap();
            }
            tx.trim_projection_outbox([ControlMvpOutboxTrimTarget::new("event", 1)])
                .await
                .unwrap();
            tx.commit().await.unwrap();
            expected.commit(changes, Vec::new(), vec![("event".into(), 1)]);
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
                "event",
                Bytes::from_static(b"destination incarnation"),
            ))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            expected.commit(
                Vec::new(),
                vec![(
                    "event".into(),
                    Bytes::from_static(b"destination incarnation"),
                )],
                Vec::new(),
            );
        }
        let restore_id = "rst_00000000000000000000000001";
        let identity = RestoreAttemptIdentity::new(restore_id, 1, "catalog").unwrap();
        let participant = ControlMvpRestoreParticipant::new(store.clone());
        let plan = participant
            .plan_restore(&reference, &identity, Utc::now())
            .await
            .unwrap();
        participant.apply_restore(&plan, Utc::now()).await.unwrap();
        let writes = if empty {
            // Empty destination writes every visible source key over the retained source lineage.
            source_writes
                .into_iter()
                .filter(|(_, value)| value.is_some())
                .collect()
        } else {
            vec![
                (b"changed".to_vec(), Some(Bytes::from_static(b"source"))),
                (b"destination-only".to_vec(), None),
                (b"gone".to_vec(), None),
            ]
        };
        let notice = Bytes::from(
            serde_json::to_vec(&Notice {
                restore_id,
                participant_attempt: 1,
                domain: "catalog",
                source_logical_sequence: source.sequence,
                result_logical_sequence: expected.sequence + 1,
            })
            .unwrap(),
        );
        let request = format!("restore:{restore_id}:1:catalog");
        expected.commit_with_request(
            writes,
            vec![(request.clone(), notice)],
            Vec::new(),
            Some(&request),
        );
        let token = store.current_state_token().await.unwrap();
        expected
            .assert_manifest(&storage, token.authority_manifest_id())
            .await;
        let actual = store.current_projection_outbox().await.unwrap();
        assert_eq!(actual.len(), expected.outbox.len());
        for (record, (id, payload, origin)) in actual.iter().zip(&expected.outbox) {
            assert_eq!(record.record_id(), id);
            assert_eq!(record.payload(), payload);
            assert_eq!(record.origin_sequence(), Some(*origin));
        }
        assert_eq!(
            store.get(b"changed").await.unwrap(),
            Some(Bytes::from_static(b"source"))
        );
        assert_eq!(store.get(b"gone").await.unwrap(), None);
    }
}

#[tokio::test]
async fn future_clock_activation_epoch_repairs_after_expiry() {
    use arco_catalog::{DurableAuthorityBinding, DurableMaintenanceWorker};
    let f = Fixture::new().await;
    let worker = DurableMaintenanceWorker::new(
        f.storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
        DurableAuthorityBinding::new([33; 32]),
    )
    .unwrap();
    let created = f.start + chrono::Duration::hours(48);
    let plan = worker
        .test_prepare_forced_at(created)
        .await
        .unwrap()
        .unwrap();
    let id = plan.job_id();
    assert!(worker.start_at(&plan, f.start).await.is_err());
    let schedule = f.backend.arm(
        RETENTION_MUTATION_EPOCH_PATH.to_owned(),
        0,
        Fault::PauseAfter,
    );
    let mut invocation = Box::pin(worker.start_at(&plan, created));
    tokio::select! {
        result = &mut invocation => panic!("activation did not pause: {result:?}"),
        () = schedule.issued() => {}
    }
    drop(invocation);
    schedule.finish().await;
    assert_eq!(f.epoch().await, "IN_FLIGHT");
    f.backend.expire_lease(&f.storage).await;
    let epoch: serde_json::Value = serde_json::from_slice(
        &f.storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .unwrap(),
    )
    .unwrap();
    let prefix = format!(
        "{}/maintenance/{}",
        f.store.paths().base_prefix(),
        id.as_str()
    );
    let descriptor_path = format!("{prefix}/descriptor.json");
    let descriptor = f.storage.get_raw(&descriptor_path).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&descriptor).unwrap();
    let pin_id = format!("pin_{}", value.get("nonce").unwrap().as_str().unwrap());
    let pin_path = retention_pin_revision_path(&pin_id, 1).unwrap();
    assert!(f.storage.head_raw(&pin_path).await.unwrap().is_none());
    let expired = created + chrono::Duration::hours(25);
    worker.recover_activation_at(id, expired).await.unwrap();
    assert_eq!(f.epoch().await, "IDLE");
    let repaired: serde_json::Value = serde_json::from_slice(
        &f.storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(epoch.get("started_at"), repaired.get("started_at"));
    let started: DateTime<Utc> =
        serde_json::from_value(epoch.get("started_at").unwrap().clone()).unwrap();
    assert!(started >= created && started < created + chrono::Duration::hours(24));
    let expected_pin = RetentionPinRevision::new(
        &pin_id,
        1,
        arco_catalog::workspace_snapshot::RetentionTarget::Maintenance(format!(
            "catalog/{}",
            id.as_str()
        )),
        created,
        created + chrono::Duration::days(8),
        None,
    )
    .unwrap();
    let expected_pin = Bytes::from(encode_retention_pin_revision(&expected_pin).unwrap());
    assert_eq!(f.storage.get_raw(&pin_path).await.unwrap(), expected_pin);
    worker.recover_activation_at(id, expired).await.unwrap();
    assert_eq!(f.storage.get_raw(&pin_path).await.unwrap(), expected_pin);
    assert_eq!(
        f.storage.get_raw(&descriptor_path).await.unwrap(),
        descriptor
    );
    assert!(worker.resume_at(id, expired).await.is_err());
    assert!(worker.advance_at(id, expired).await.is_err());
}

#[tokio::test]
async fn durable_maintenance_remote_faults_at_every_write_class() {
    use arco_catalog::{DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceStatus};
    for stage in ["start", "advance", "publish"] {
        let boundaries: &[(&str, usize)] = match stage {
            "start" => &[
                ("/descriptor.json", 0),
                ("/plans/", 0),
                (RETENTION_MUTATION_EPOCH_PATH, 0),
                ("retention/pins/", 0),
                ("retention/pins/", 1),
                ("/maintenance-revisions-placeholder/", 0),
                ("/selected.json", 0),
                (RETENTION_MUTATION_EPOCH_PATH, 1),
            ],
            "advance" => &[
                ("/segments/l1/maintenance-", 0),
                ("/indexes/maintenance-", 0),
                ("/maintenance-revisions-placeholder/", 0),
                ("/selected.json", 0),
            ],
            "publish" => &[
                ("/attempts/", 0),
                ("/maintenance-revisions-placeholder/", 0),
                ("/selected.json", 0),
                ("/manifests/maintenance-", 0),
                ("/head/current.json", 0),
            ],
            _ => unreachable!(),
        };
        for (boundary, skip) in boundaries {
            for fault in [
                Fault::LostResponse,
                Fault::DelayedError,
                Fault::PauseBefore,
                Fault::PauseAfter,
                Fault::Unreadable,
            ] {
                let f = Fixture::new().await;
                let scope = StateScope::new("tenant", "workspace", "catalog");
                let binding = DurableAuthorityBinding::new([32; 32]);
                let worker =
                    DurableMaintenanceWorker::new(f.storage.clone(), scope.clone(), binding)
                        .unwrap();
                let plan = worker
                    .test_prepare_forced_at(f.start)
                    .await
                    .unwrap()
                    .unwrap();
                let id = plan.job_id().clone();
                if stage != "start" {
                    worker.start_at(&plan, f.start).await.unwrap();
                }
                if stage == "publish" {
                    assert_eq!(
                        worker.advance_at(&id, f.start).await.unwrap().status,
                        MaintenanceStatus::ReadyToPublish
                    );
                }
                let boundary = if *boundary == "/maintenance-revisions-placeholder/" {
                    format!("/maintenance/{}/revisions/", id.as_str())
                } else {
                    (*boundary).to_owned()
                };
                f.backend.trace.lock().unwrap().clear();
                let schedule = f.backend.arm(boundary.clone(), *skip, fault);
                let operation = async {
                    match stage {
                        "start" => worker.start_at(&plan, f.start).await.map(|_| ()),
                        "advance" => worker.advance_at(&id, f.start).await.map(|_| ()),
                        "publish" => worker.publish_at(&id, f.start).await.map(|_| ()),
                        _ => unreachable!(),
                    }
                };
                let mut invocation = Box::pin(operation);
                if matches!(fault, Fault::PauseBefore | Fault::PauseAfter) {
                    tokio::select! { result = &mut invocation => panic!("{stage} {boundary} {fault:?} did not pause: {result:?}"), () = schedule.issued() => {} }
                    drop(invocation);
                } else {
                    let result = invocation.await;
                    if matches!(fault, Fault::DelayedError | Fault::Unreadable) {
                        assert!(
                            result.is_err(),
                            "{stage} {boundary} {fault:?}: uncertainty was hidden"
                        );
                    }
                }
                assert!(
                    f.backend
                        .trace
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|entry| entry.starts_with("ISSUE PUT ")
                            && entry.contains("/head/current.json"))
                        .count()
                        <= 1
                );
                schedule.finish().await;
                *f.backend.denied.lock().unwrap() = None;
                f.backend.expire_lease(&f.storage).await;
                let restarted =
                    DurableMaintenanceWorker::new(f.storage.clone(), scope, binding).unwrap();
                if stage == "start" {
                    // The known preparation also allows a live caller to complete
                    // a descriptor/page prefix that never acquired root protection.
                    restarted
                        .start_at(&plan, f.start)
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "{stage} {boundary} {fault:?}: {error:?}\n{:?}",
                                f.backend.trace.lock().unwrap()
                            )
                        });
                }
                let progress = restarted.resume_at(&id, f.start).await.unwrap();
                if progress.status == MaintenanceStatus::Active {
                    restarted.advance_at(&id, f.start).await.unwrap();
                }
                assert!(restarted.publish_at(&id, f.start).await.unwrap().is_some());
                assert_eq!(
                    f.store.get(b"key").await.unwrap(),
                    Some(Bytes::from_static(b"value"))
                );
                let mut oracle = LogicalOracle::new();
                oracle.commit(
                    vec![(b"key".to_vec(), Some(Bytes::from_static(b"value")))],
                    Vec::new(),
                    Vec::new(),
                );
                oracle
                    .assert_manifest(
                        &f.storage,
                        f.store
                            .current_state_token()
                            .await
                            .unwrap()
                            .authority_manifest_id(),
                    )
                    .await;
            }
        }
    }
}

#[path = "support/gate7_model.rs"]
mod gate7_model;

#[path = "support/gate7_schedules.rs"]
mod gate7_schedules;
