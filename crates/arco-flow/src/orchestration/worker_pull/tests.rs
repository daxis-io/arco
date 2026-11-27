#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use super::*;
use crate::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TriggerInfo,
};
use arco_core::storage::MemoryBackend;
use std::sync::Arc;

fn storage() -> ScopedStorage {
    ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "pull").unwrap()
}

fn plan(run_id: &str, tasks: usize) -> AcceptedRunPlan {
    let plan_id = format!("plan-{run_id}");
    let tasks: Vec<_> = (0..tasks)
        .map(|index| TaskDef {
            key: format!("task-{index:03}"),
            depends_on: Vec::new(),
            asset_key: None,
            partition_key: None,
            max_attempts: 2,
            heartbeat_timeout_sec: 12,
            requires_visible_output: false,
        })
        .collect();
    AcceptedRunPlan {
        version: 1,
        manifest: None,
        task_payloads: tasks
            .iter()
            .map(|task| (task.key.clone(), serde_json::json!({"work": task.key})))
            .collect(),
        events: vec![
            OrchestrationEvent::new(
                "tenant",
                "pull",
                OrchestrationEventData::RunTriggered {
                    run_id: run_id.into(),
                    plan_id: plan_id.clone(),
                    trigger: TriggerInfo::Manual {
                        user_id: "operator".into(),
                    },
                    root_assets: vec![],
                    run_key: Some(run_id.into()),
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            ),
            OrchestrationEvent::new(
                "tenant",
                "pull",
                OrchestrationEventData::PlanCreated {
                    run_id: run_id.into(),
                    plan_id,
                    tasks,
                },
            ),
        ],
    }
}

fn config() -> WorkerPullConfig {
    WorkerPullConfig {
        permits: 1,
        callback_base_url: "http://127.0.0.1:8090".into(),
        claim_key: [42; 32],
        control_token: "operator-control-credential-123456".into(),
        sessions: BTreeMap::from([
            (
                "left".into(),
                "first-worker-session-credential-123456".into(),
            ),
            (
                "right".into(),
                "second-worker-session-credential-123456".into(),
            ),
        ]),
    }
}

#[tokio::test]
async fn idle_wake_observes_selected_deadline_while_a_writer_holds_the_lock() {
    let storage = storage();
    let clock = ManualClock::new();
    let host = clock.attach(WorkerPull::new(storage.clone(), config()).unwrap());
    host.accept_plan(&plan("held", 1)).await.unwrap();
    let ClaimReply::Granted { grant } = host.claim("left", "held").await.unwrap() else {
        panic!("claim missing");
    };
    let (selected, _) = host.compactor.load_state().await.unwrap();
    let path = orchestration_compaction_lock_path();
    let lock = DistributedLock::new(storage.backend().clone(), path);
    let guard = lock.acquire(DEFAULT_LOCK_TTL, 1).await.unwrap();
    let held = storage.backend().get(path).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(1), host.advance()).await;
    let after = storage.backend().get(path).await.unwrap();
    guard.release().await.unwrap();

    let reply = reply
        .expect("an idle wake must not wait for the writer")
        .unwrap();
    assert_eq!(after, held);
    assert_eq!(reply.observed_revision, selected.revision_ulid);
    assert_eq!(reply.advanced, 0);
    assert!(!reply.immediate_work);
    assert_eq!(reply.next_deadline, Some(grant.start_before));
}

#[tokio::test]
async fn competing_workers_recover_only_the_visible_winner_at_capacity() {
    let host = WorkerPull::new(storage(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let (left, right) = tokio::join!(host.claim("left", "one"), host.claim("right", "two"));
    let (left, right) = (left.unwrap(), right.unwrap());
    let (generation, session, request, other) = match (left, right) {
        (ClaimReply::Granted { grant }, ClaimReply::Busy) => (grant, "left", "one", "right"),
        (ClaimReply::Busy, ClaimReply::Granted { grant }) => (grant, "right", "two", "left"),
        results => panic!("exactly one worker must win: {results:?}"),
    };
    let ClaimReply::Granted { grant: replay } = host.claim(session, request).await.unwrap() else {
        panic!("missing replayed grant");
    };
    assert_eq!(generation.generation, replay.generation);
    assert_eq!(generation.envelope, replay.envelope);
    assert!(host.claim(other, request).await.is_err());
    let (_, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(fold.runs.len(), 1);
    assert_eq!(fold.dispatch_outbox.len(), 1);
}

#[allow(clippy::significant_drop_tightening)]
#[path = "../../../examples/operator_owned_dispatcher/persistent_flow_backend.rs"]
mod persistent_flow_backend;

use std::{
    ops::Range,
    sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
};

use arco_core::LockInfo;
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_worker_contract::{
    ErrorCategory, HeartbeatRequest, TaskCompletedRequest, TaskError, TaskStartedRequest,
    WorkerOutcome,
};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::Notify;
use tower::ServiceExt;

struct ManualClock(Arc<AtomicI64>);

impl ManualClock {
    fn new() -> Self {
        Self(Arc::new(AtomicI64::new(Utc::now().timestamp_millis())))
    }

    fn attach(&self, mut host: WorkerPull) -> WorkerPull {
        let clock = self.0.clone();
        host.clock = Arc::new(move || {
            DateTime::from_timestamp_millis(clock.load(Ordering::SeqCst)).unwrap()
        });
        host
    }

    fn jump(&self, seconds: i64) {
        self.0.fetch_add(seconds * 1_000, Ordering::SeqCst);
    }
}

#[repr(u8)]
#[derive(Clone, Copy)]
enum Fault {
    None,
    Before,
    After,
    Pause,
    Read,
    ReadBack,
    Recheck,
    TimeoutAfter,
    TimeoutReadBack,
    PauseProjection,
    PauseFirstL0Read,
    FirstL0ReadPaused,
    ArmPauseHead,
    PauseHead,
    ChangeReceiptAfter,
    ReadChangedReceipt,
}

struct FaultBackend {
    inner: Arc<dyn StorageBackend>,
    armed: AtomicU8,
    projection_reads: AtomicUsize,
    lock_acquisitions: AtomicUsize,
    hold_release: AtomicBool,
    arrived: Notify,
    resume: Notify,
    later_l0_read: Notify,
}

impl FaultBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            armed: AtomicU8::new(0),
            projection_reads: AtomicUsize::new(0),
            lock_acquisitions: AtomicUsize::new(0),
            hold_release: AtomicBool::new(false),
            arrived: Notify::new(),
            resume: Notify::new(),
            later_l0_read: Notify::new(),
        }
    }

    fn arm(&self, fault: Fault) {
        assert_eq!(self.armed.swap(fault as u8, Ordering::SeqCst), 0);
    }

    fn move_fault(&self, current: Fault, next: Fault) -> bool {
        self.armed
            .compare_exchange(
                current as u8,
                next as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    fn take_projection_reads(&self) -> usize {
        self.projection_reads.swap(0, Ordering::SeqCst)
    }

    fn take_lock_acquisitions(&self) -> usize {
        self.lock_acquisitions.swap(0, Ordering::SeqCst)
    }

    fn pointer(path: &str) -> bool {
        path.ends_with(crate::paths::orchestration_manifest_pointer_path())
    }

    fn scope(self: &Arc<Self>) -> ScopedStorage {
        ScopedStorage::new(self.clone(), "tenant", "pull").unwrap()
    }
}

#[async_trait::async_trait]
impl StorageBackend for FaultBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        let l0 = path.contains("/state/orchestration/l0/");
        if l0 || path.contains("/state/orchestration/base/") {
            self.projection_reads.fetch_add(1, Ordering::SeqCst);
            if self.move_fault(Fault::PauseProjection, Fault::None) {
                self.arrived.notify_one();
                self.resume.notified().await;
            }
        }
        if l0 {
            if self.move_fault(Fault::PauseFirstL0Read, Fault::FirstL0ReadPaused) {
                self.arrived.notify_one();
                self.resume.notified().await;
                assert!(self.move_fault(Fault::FirstL0ReadPaused, Fault::None));
            } else if self.armed.load(Ordering::SeqCst) == Fault::FirstL0ReadPaused as u8 {
                self.later_l0_read.notify_one();
            }
        }
        if Self::pointer(path) {
            if self.move_fault(Fault::Read, Fault::None) {
                return Err(arco_core::Error::storage(
                    "fixture: selected pointer read failed",
                ));
            }
            if self.move_fault(Fault::TimeoutReadBack, Fault::None) {
                std::future::pending::<()>().await;
            }
            self.move_fault(Fault::ArmPauseHead, Fault::PauseHead);
        }
        let bytes = self.inner.get(path).await?;
        if path.contains("/state/orchestration/manifests/")
            && self.move_fault(Fault::ReadChangedReceipt, Fault::None)
        {
            let mut manifest: OrchestrationManifest = serde_json::from_slice(&bytes).unwrap();
            let receipt = manifest
                .worker_pull
                .as_mut()
                .unwrap()
                .receipts
                .get_mut("first")
                .unwrap();
            receipt.completion.as_mut().unwrap().accepted_at -= TimeDelta::seconds(1);
            return Ok(serde_json::to_vec(&manifest).unwrap().into());
        }
        Ok(bytes)
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
        let lock = (path == orchestration_compaction_lock_path())
            .then(|| serde_json::from_slice::<LockInfo>(&data).unwrap());
        if lock.as_ref().is_some_and(LockInfo::is_expired)
            && self.hold_release.swap(false, Ordering::SeqCst)
        {
            assert!(matches!(
                &precondition,
                WritePrecondition::MatchesVersion(_)
            ));
            self.arrived.notify_one();
            self.resume.notified().await;
        }
        let fault = if Self::pointer(path) {
            self.armed.swap(Fault::None as u8, Ordering::SeqCst)
        } else {
            Fault::None as u8
        };
        if fault == Fault::Before as u8 {
            return Err(arco_core::Error::storage(
                "fixture: response lost before pointer CAS",
            ));
        }
        if fault == Fault::Pause as u8 {
            self.arrived.notify_one();
            self.resume.notified().await;
        }
        if fault == Fault::Recheck as u8 {
            return Ok(WriteResult::PreconditionFailed {
                current_version: "fixture-conflict".into(),
            });
        }
        let result = self.inner.put(path, data, precondition).await?;
        if matches!(result, WriteResult::Success { .. })
            && lock.is_some_and(|info| !info.is_expired())
        {
            self.lock_acquisitions.fetch_add(1, Ordering::SeqCst);
        }
        if fault == Fault::ReadBack as u8 {
            assert!(matches!(result, WriteResult::Success { .. }));
            self.armed.store(Fault::Read as u8, Ordering::SeqCst);
        }
        if fault == Fault::TimeoutAfter as u8 {
            assert!(matches!(result, WriteResult::Success { .. }));
            self.hold_release.store(true, Ordering::SeqCst);
            self.armed
                .store(Fault::TimeoutReadBack as u8, Ordering::SeqCst);
        }
        if fault == Fault::ChangeReceiptAfter as u8 {
            assert!(matches!(result, WriteResult::Success { .. }));
            self.armed
                .store(Fault::ReadChangedReceipt as u8, Ordering::SeqCst);
        }
        if fault == Fault::After as u8 {
            assert!(matches!(result, WriteResult::Success { .. }));
            return Err(arco_core::Error::storage(
                "fixture: response lost after pointer CAS",
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
        if Self::pointer(path) && self.move_fault(Fault::PauseHead, Fault::None) {
            self.arrived.notify_one();
            self.resume.notified().await;
        }
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[tokio::test]
async fn due_wake_reads_only_the_predecessor_and_matches_the_cold_successor() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let host = WorkerPull::new(backend.scope(), config()).unwrap();
    host.accept_plan(&plan("ready", 1)).await.unwrap();
    backend.take_projection_reads();
    host.compactor.load_state().await.unwrap();
    let predecessor_reads = backend.take_projection_reads();
    backend.take_lock_acquisitions();

    let reply = host.advance().await.unwrap();
    let advance_reads = backend.take_projection_reads();
    let reopened = WorkerPull::new(backend.scope(), config()).unwrap();
    let cold = reopened.advance().await.unwrap();
    let successor_reads = backend.take_projection_reads();
    let (_, fold) = reopened.compactor.load_state().await.unwrap();

    assert_eq!(reply.advanced, 1);
    assert_eq!(cold.advanced, 0);
    assert_eq!(reply.observed_revision, cold.observed_revision);
    assert_eq!(reply.next_deadline, cold.next_deadline);
    assert_eq!(reply.immediate_work, cold.immediate_work);
    assert_eq!(fold.dispatch_outbox.len(), 1);
    assert_eq!(backend.take_lock_acquisitions(), 1);
    assert!(predecessor_reads > 0 && successor_reads > 0);
    assert_eq!(
        advance_reads, predecessor_reads,
        "due wake repeated a projection read after selection ({successor_reads} cold successor reads)"
    );
}

#[tokio::test]
async fn due_wake_requires_selected_readback_even_with_the_folded_successor() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let host = WorkerPull::new(backend.scope(), config()).unwrap();
    host.accept_plan(&plan("due", 1)).await.unwrap();
    backend.arm(Fault::ReadBack);

    let (status, _) = post(
        &host.router(),
        "/run",
        &config().control_token,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let reopened = WorkerPull::new(backend.scope(), config()).unwrap();
    let (selected, fold) = reopened.compactor.load_state().await.unwrap();
    assert_eq!(fold.dispatch_outbox.len(), 1);
    let retry = manual_advance(&reopened).await;
    assert_eq!(retry.observed_revision, selected.revision_ulid);
    assert_eq!(retry.advanced, 0);
    assert!(!retry.immediate_work);
}

#[tokio::test]
async fn due_wake_reloads_when_the_selected_predecessor_changes() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let host = WorkerPull::new(backend.scope(), config()).unwrap();
    host.accept_plan(&plan("first", 1)).await.unwrap();
    backend.arm(Fault::PauseProjection);
    let wake_host = WorkerPull::new(backend.scope(), config()).unwrap();
    let wake = tokio::spawn(async move { wake_host.advance().await });
    tokio::time::timeout(Duration::from_secs(1), backend.arrived.notified())
        .await
        .expect("wake reached its preloaded projection");

    host.accept_plan(&plan("concurrent", 1)).await.unwrap();
    backend.resume.notify_one();
    let reply = wake.await.unwrap().unwrap();
    let (selected, fold) = host.compactor.load_state().await.unwrap();

    assert_eq!(reply.advanced, 2);
    assert_eq!(reply.observed_revision, selected.revision_ulid);
    let dispatched: BTreeSet<_> = fold
        .dispatch_outbox
        .values()
        .map(|row| row.run_id.as_str())
        .collect();
    assert_eq!(dispatched, BTreeSet::from(["concurrent", "first"]));
}

fn grant(reply: ClaimReply) -> Box<ClaimGrant> {
    match reply {
        ClaimReply::Granted { grant } => grant,
        reply => panic!("expected a claim grant, got {reply:?}"),
    }
}

async fn post(
    router: &Router,
    route: &str,
    token: &str,
    body: &(impl Serialize + Sync),
) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::post(route)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            panic!(
                "expected JSON from {route} ({status}): {}",
                String::from_utf8_lossy(&bytes)
            );
        }),
    )
}

fn started_request(grant: &ClaimGrant, worker: &str) -> TaskStartedRequest {
    TaskStartedRequest {
        attempt: grant.envelope.attempt,
        attempt_id: grant.envelope.attempt_id.clone(),
        worker_id: worker.into(),
        traceparent: None,
        started_at: None,
    }
}

fn heartbeat_request(grant: &ClaimGrant, worker: &str) -> HeartbeatRequest {
    HeartbeatRequest {
        attempt: grant.envelope.attempt,
        attempt_id: grant.envelope.attempt_id.clone(),
        worker_id: worker.into(),
        traceparent: None,
        heartbeat_at: None,
        progress_pct: Some(40),
        message: None,
    }
}

fn completed_request(grant: &ClaimGrant, worker: &str) -> TaskCompletedRequest {
    serde_json::from_value(serde_json::json!({
        "attempt": grant.envelope.attempt, "attemptId": grant.envelope.attempt_id,
        "workerId": worker, "outcome": WorkerOutcome::Succeeded,
    }))
    .unwrap()
}

async fn callback(
    host: &WorkerPull,
    grant: &ClaimGrant,
    kind: &str,
    body: &(impl Serialize + Sync),
) -> (StatusCode, Value) {
    post(
        &host.router(),
        &format!("/api/v1/tasks/{}/{kind}", grant.envelope.task_id),
        &grant.envelope.task_token,
        body,
    )
    .await
}

async fn manual_advance(host: &WorkerPull) -> AdvanceReply {
    let (status, body) = post(
        &host.router(),
        "/run",
        &config().control_token,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_value(body).unwrap()
}

#[tokio::test]
async fn prepared_claim_carries_no_authority_and_retry_skips_its_manifest_slot() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let storage = backend.scope();
    let host = WorkerPull::new(storage.clone(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let visible = manual_advance(&host).await;

    backend.arm(Fault::Before);
    let (status, _) = post(
        &host.router(),
        "/api/v1/tasks/claim",
        &config().sessions["left"],
        &serde_json::json!({"requestId": "first"}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (selected, _) = host.compactor.load_state().await.unwrap();
    assert_eq!(selected.revision_ulid, visible.observed_revision);
    assert!(selected.worker_pull.as_ref().unwrap().receipts.is_empty());

    let paths = backend.list("").await.unwrap();
    let mut prepared = None;
    for entry in paths
        .into_iter()
        .filter(|entry| entry.path.contains("state/orchestration/manifests/"))
    {
        let bytes = backend.get(&entry.path).await.unwrap();
        let candidate: OrchestrationManifest = serde_json::from_slice(&bytes).unwrap();
        if let Some(authority) = candidate.worker_pull.as_ref()
            && authority.receipts.contains_key("first")
        {
            prepared = Some(candidate);
        }
    }
    let prepared = prepared.expect("the immutable snapshot was persisted before CAS");
    let receipt = &prepared.worker_pull.as_ref().unwrap().receipts["first"];
    let callback_id = arco_worker_contract::callback_task_id(
        &receipt.dispatch.run_id,
        &receipt.dispatch.task_key,
    );
    let (status, _) = post(&host.router(), &format!("/api/v1/tasks/{callback_id}/started"),
        &host.capability(receipt), &serde_json::json!({
            "attempt": receipt.dispatch.attempt, "attemptId": receipt.dispatch.attempt_id, "workerId": "left",
        })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    backend.arm(Fault::Recheck);
    let winner = grant(host.claim("left", "second").await.unwrap());
    assert!(winner.generation > receipt.generation);
    assert_eq!(
        winner.envelope.payload,
        serde_json::json!({"work": "task-000"})
    );
    let (after, _) = host.compactor.load_state().await.unwrap();
    assert!(after.manifest_id > prepared.manifest_id);
    assert_eq!(
        after.previous_manifest_path,
        Some(crate::paths::orchestration_manifest_snapshot_path(
            &selected.manifest_id
        ))
    );
    assert!(matches!(
        host.claim("left", "first").await.unwrap(),
        ClaimReply::Busy
    ));
    let replay = grant(host.claim("left", "second").await.unwrap());
    assert_eq!(winner.envelope, replay.envelope);
    let report = crate::orchestration::compactor::OrchestrationReconciler::new(storage)
        .check()
        .await
        .unwrap();
    assert!(
        report
            .orphan_manifest_snapshots
            .iter()
            .any(|path| path.ends_with(&format!("{}.json", prepared.manifest_id)))
    );
    assert!(
        !report
            .orphan_manifest_snapshots
            .iter()
            .any(|path| path.ends_with(&format!("{}.json", after.manifest_id)))
    );
}

#[tokio::test]
async fn expired_before_start_reissues_the_same_attempt_and_fences_all_old_callbacks() {
    let clock = ManualClock::new();
    let host = clock.attach(WorkerPull::new(storage(), config()).unwrap());
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let old = grant(host.claim("left", "first").await.unwrap());
    clock.jump(16);
    assert_eq!(
        callback(&host, &old, "started", &started_request(&old, "left"))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let new = grant(host.claim("right", "next").await.unwrap());
    assert_eq!(new.envelope.dispatch_id, old.envelope.dispatch_id);
    assert_eq!(new.envelope.attempt_id, old.envelope.attempt_id);
    assert_eq!(new.permit, old.permit);
    assert!(new.generation > old.generation);
    assert!(matches!(
        host.claim("left", "first").await.unwrap(),
        ClaimReply::Superseded
    ));
    assert_eq!(
        callback(&host, &new, "started", &started_request(&new, "left"))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        callback(&host, &new, "started", &started_request(&new, "right"))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        callback(&host, &old, "heartbeat", &heartbeat_request(&old, "left"))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        callback(&host, &old, "completed", &completed_request(&old, "left"))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert!(matches!(
        host.claim("left", "third").await.unwrap(),
        ClaimReply::Busy
    ));
    let (_, fold) = host.compactor.load_state().await.unwrap();
    assert!(fold.dispatch_outbox.is_empty());
    assert_eq!(fold.runs["run"].tasks_completed, 0);
}

#[tokio::test]
async fn accepted_callbacks_win_over_expiry_after_response_loss() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let clock = ManualClock::new();
    let host = clock.attach(WorkerPull::new(backend.scope(), config()).unwrap());
    host.accept_plan(&plan("run", 2)).await.unwrap();
    manual_advance(&host).await;
    backend.arm(Fault::After);
    let (status, _) = post(
        &host.router(),
        "/api/v1/tasks/claim",
        &config().sessions["left"],
        &serde_json::json!({"requestId": "first"}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let restarted = clock.attach(WorkerPull::new(backend.scope(), config()).unwrap());
    assert!(matches!(
        restarted.claim("right", "loser").await.unwrap(),
        ClaimReply::Busy
    ));
    let original = grant(restarted.claim("left", "first").await.unwrap());
    let (manifest, _) = restarted.compactor.load_state().await.unwrap();
    assert_eq!(
        manifest.worker_pull.unwrap().receipts["first"].generation,
        original.generation
    );

    backend.arm(Fault::After);
    let started = started_request(&original, "left");
    assert_eq!(
        callback(&restarted, &original, "started", &started).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let start_time = (restarted.clock)();
    clock.jump(2);
    let (status, body) = callback(&restarted, &original, "started", &started).await;
    assert_eq!(status, StatusCode::OK);
    let body: arco_worker_contract::TaskStartedResponse = serde_json::from_value(body).unwrap();
    assert_eq!(body.server_time, start_time);
    let old_deadline = manual_advance(&restarted).await.next_deadline.unwrap();

    clock.jump(7);
    backend.arm(Fault::After);
    assert_eq!(
        callback(
            &restarted,
            &original,
            "heartbeat",
            &heartbeat_request(&original, "left")
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    clock.jump(4);
    assert!((restarted.clock)() > old_deadline);
    let reply = manual_advance(&restarted).await;
    assert_eq!(reply.advanced, 0);
    assert!(reply.next_deadline.unwrap() > old_deadline);
    let renewed_grant = grant(restarted.claim("left", "first").await.unwrap());
    assert_eq!(renewed_grant.start_before, original.start_before);
    assert_eq!(renewed_grant.envelope, original.envelope);
    assert_eq!(Some(renewed_grant.lease_expires_at), reply.next_deadline);
    assert!(matches!(
        restarted.claim("right", "loser").await.unwrap(),
        ClaimReply::Busy
    ));

    let completed_request = completed_request(&original, "left");
    backend.arm(Fault::After);
    assert_eq!(
        callback(&restarted, &original, "completed", &completed_request)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    clock.jump(20);
    let fresh = grant(restarted.claim("right", "loser").await.unwrap());
    assert!(fresh.generation > original.generation);
    assert_eq!(fresh.permit, original.permit);
    assert_ne!(fresh.envelope.task_id, original.envelope.task_id);
    let (status, body) = callback(&restarted, &original, "completed", &completed_request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["finalState"], "SUCCEEDED");
    let completed: arco_worker_contract::TaskCompletedResponse =
        serde_json::from_value(body).unwrap();
    let ClaimReply::Completed {
        generation,
        final_state,
        accepted_at,
    } = restarted.claim("left", "first").await.unwrap()
    else {
        panic!("missing completed claim outcome");
    };
    assert_eq!(generation, original.generation);
    assert_eq!(final_state, completed.final_state);
    assert_eq!(accepted_at, completed.server_time);
    assert!(matches!(
        restarted.claim("left", "another").await.unwrap(),
        ClaimReply::Busy
    ));
    let (_, fold) = restarted.compactor.load_state().await.unwrap();
    assert_eq!(fold.runs["run"].tasks_completed, 1);
    assert_eq!(fold.runs["run"].tasks_succeeded, 1);
}

#[tokio::test]
async fn late_renewal_and_completion_lose_to_visible_reclamation() {
    for callback_kind in ["heartbeat", "completed"] {
        let clock = ManualClock::new();
        let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
        let host = clock.attach(WorkerPull::new(backend.scope(), config()).unwrap());
        host.accept_plan(&plan("run", 2)).await.unwrap();
        let original = grant(host.claim("left", "original").await.unwrap());
        assert_eq!(
            callback(
                &host,
                &original,
                "started",
                &started_request(&original, "left")
            )
            .await
            .0,
            StatusCode::OK
        );
        clock.jump(10);
        backend.arm(Fault::Pause);
        let contender = clock.attach(WorkerPull::new(backend.scope(), config()).unwrap());
        let payload = if callback_kind == "heartbeat" {
            serde_json::to_value(heartbeat_request(&original, "left")).unwrap()
        } else {
            serde_json::to_value(completed_request(&original, "left")).unwrap()
        };
        let (late, winner) =
            tokio::join!(callback(&host, &original, callback_kind, &payload), async {
                backend.arrived.notified().await;
                // A local fixture models lease loss after pre-CAS validation.
                DistributedLock::new(backend.clone(), orchestration_compaction_lock_path())
                    .force_break()
                    .await
                    .unwrap();
                clock.jump(3);
                let result = contender.claim("right", "replacement").await;
                backend.resume.notify_one();
                grant(result.unwrap())
            });
        assert_eq!(late.0, StatusCode::CONFLICT, "{callback_kind}: {}", late.1);
        assert!(winner.generation > original.generation);
        assert_ne!(winner.envelope.task_id, original.envelope.task_id);
        assert!(matches!(
            contender.claim("left", "original").await.unwrap(),
            ClaimReply::Expired
        ));
        let (manifest, fold) = contender.compactor.load_state().await.unwrap();
        assert_eq!(
            manifest.worker_pull.unwrap().permits,
            vec![Some(winner.generation)]
        );
        let old_task = &fold.tasks[&("run".into(), original.envelope.task_key.clone())];
        assert_eq!(old_task.state, TaskState::RetryWait);
        assert!(old_task.last_heartbeat_at.is_none());
        assert_eq!(fold.runs["run"].tasks_completed, 0);
    }
}

#[tokio::test]
async fn running_expiry_reopens_full_disk_authority_then_uses_existing_retry_policy() {
    let path = std::env::temp_dir().join(format!("arco-pull-{}.json", Ulid::new()));
    let clock = ManualClock::new();
    let reopen = || {
        let backend = persistent_flow_backend::PersistentFlowBackend::open(&path).unwrap();
        let storage = ScopedStorage::new(Arc::new(backend), "tenant", "pull").unwrap();
        clock.attach(WorkerPull::new(storage, config()).unwrap())
    };
    let host = reopen();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let original = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(
        callback(
            &host,
            &original,
            "started",
            &started_request(&original, "left")
        )
        .await
        .0,
        StatusCode::OK
    );
    drop(host);
    let host = reopen();
    assert!(matches!(
        host.claim("right", "next").await.unwrap(),
        ClaimReply::Busy
    ));
    let replayed = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(original.envelope, replayed.envelope);
    clock.jump(13);
    assert!(matches!(
        host.claim("right", "next").await.unwrap(),
        ClaimReply::Empty
    ));
    let (manifest, fold) = host.compactor.load_state().await.unwrap();
    let task = &fold.tasks[&("run".into(), original.envelope.task_key.clone())];
    assert_eq!(task.state, TaskState::RetryWait);
    assert_eq!(task.attempt, 1);
    let deadline = task.retry_not_before.unwrap();
    assert_eq!(deadline, (host.clock)() + TimeDelta::seconds(30));
    assert_eq!(manifest.worker_pull.unwrap().permits, vec![None]);
    clock.jump(29);
    assert_eq!(manual_advance(&host).await.next_deadline, Some(deadline));
    assert!(matches!(
        host.claim("right", "next").await.unwrap(),
        ClaimReply::Empty
    ));
    clock.jump(1);
    let retry = grant(host.claim("right", "next").await.unwrap());
    assert_eq!(retry.envelope.attempt, 2);
    assert_ne!(retry.envelope.attempt_id, original.envelope.attempt_id);
    assert!(retry.generation > original.generation);
    assert_eq!(
        callback(&host, &retry, "started", &started_request(&retry, "right"))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        callback(
            &host,
            &retry,
            "completed",
            &completed_request(&retry, "right")
        )
        .await
        .0,
        StatusCode::OK
    );
    drop(host);
    let host = reopen();
    let (manifest, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(manifest.worker_pull.unwrap().permits, vec![None]);
    assert_eq!(
        fold.runs["run"].state,
        crate::orchestration::compactor::RunState::Succeeded
    );
    assert_eq!(fold.runs["run"].tasks_completed, 1);
    assert!(matches!(
        host.claim("right", "next").await.unwrap(),
        ClaimReply::Completed { .. }
    ));
}

const RETRY_LABEL: &str = "arco.retry_policy";
const COMPAT_PROFILE: &str = "rivers-d7cbc206-compat-v1";

fn run_labels(plan: &mut AcceptedRunPlan) -> &mut std::collections::HashMap<String, String> {
    let OrchestrationEventData::RunTriggered { labels, .. } = &mut plan.events[0].data else {
        panic!("fixture starts with a run trigger");
    };
    labels
}

async fn assert_rivers_admission(host: &WorkerPull, accepted: &AcceptedRunPlan) -> String {
    let revision = host.accept_plan(accepted).await.unwrap();
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    let authority = selected.worker_pull.unwrap();
    let identity = &authority.accepted_runs["run"];
    assert!(authority.uses_rivers_retry("run"));
    assert_eq!(fold.runs["run"].labels[RETRY_LABEL], COMPAT_PROFILE);
    let frozen = load_accepted_run_plan(&host.storage, "run", &identity.plan_id, &identity.sha256)
        .await
        .unwrap();
    assert_eq!(frozen.version, 2);
    assert_eq!(
        arco_core::canonical_json::to_canonical_bytes(&frozen).unwrap(),
        arco_core::canonical_json::to_canonical_bytes(accepted).unwrap()
    );
    assert_eq!(
        authority
            .events
            .iter()
            .map(|event| &event.event_id)
            .collect::<Vec<_>>(),
        accepted
            .events
            .iter()
            .map(|event| &event.event_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(host.accept_plan(accepted).await.unwrap(), revision);
    identity.sha256.clone()
}

#[tokio::test]
async fn rivers_retry_replays_a_lost_completion_and_dispatches_only_when_due() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let scope = backend.scope();
    let clock = ManualClock::new();
    let reopen = || clock.attach(WorkerPull::new(scope.clone(), config()).unwrap());
    let host = reopen();
    let mut accepted = plan("run", 1);
    accepted.version = 2;
    run_labels(&mut accepted).insert(RETRY_LABEL.into(), COMPAT_PROFILE.into());
    let hash = assert_rivers_admission(&host, &accepted).await;
    clock.jump(1);
    let original = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(
        callback(
            &host,
            &original,
            "started",
            &started_request(&original, "left")
        )
        .await
        .0,
        StatusCode::OK
    );
    clock.jump(1);
    let now = (host.clock)();
    let mut failed = completed_request(&original, "left");
    failed.outcome = WorkerOutcome::Failed;
    failed.completed_at = Some(now - TimeDelta::days(1));
    failed.error = Some(TaskError {
        category: ErrorCategory::Infrastructure,
        message: "retry this attempt".into(),
        stack_trace: None,
        retryable: Some(true),
    });
    backend.arm(Fault::After);
    assert_eq!(
        callback(&host, &original, "completed", &failed).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(host);

    let host = reopen();
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    let authority = selected.worker_pull.unwrap();
    assert_eq!(authority.permits, [None]);
    assert_eq!(authority.accepted_runs["run"].sha256, hash);
    let completion = authority.receipts["first"].completion.as_ref().unwrap();
    assert_eq!(completion.accepted_at, now);
    let (status, body) = callback(&host, &original, "completed", &failed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["finalState"], "FAILED");
    assert_eq!(
        body["serverTime"],
        serde_json::json!(completion.accepted_at)
    );
    assert_eq!(
        host.compactor.load_state().await.unwrap().0.revision_ulid,
        selected.revision_ulid
    );

    let task = &fold.tasks[&("run".into(), original.envelope.task_key.clone())];
    let deadline = now + TimeDelta::seconds(30);
    assert_eq!(task.state, TaskState::RetryWait);
    assert_eq!(task.retry_not_before, Some(deadline));
    assert!(fold.timers.is_empty());
    assert_rivers_retry_dispatches_when_due(&host, &clock, &original, &hash, deadline).await;
}

async fn assert_rivers_retry_dispatches_when_due(
    host: &WorkerPull,
    clock: &ManualClock,
    original: &ClaimGrant,
    hash: &str,
    deadline: DateTime<Utc>,
) {
    clock.jump(29);
    let idle = manual_advance(host).await;
    assert_eq!(idle.advanced, 0);
    assert_eq!(idle.next_deadline, Some(deadline));
    assert!(matches!(
        host.claim("right", "retry").await.unwrap(),
        ClaimReply::Empty
    ));

    clock.jump(1);
    let due = manual_advance(host).await;
    assert_eq!(due.advanced, 1);
    assert_eq!(due.next_deadline, None);
    let retry = grant(host.claim("right", "retry").await.unwrap());
    assert_eq!(retry.envelope.attempt, 2);
    assert_ne!(retry.envelope.attempt_id, original.envelope.attempt_id);
    assert!(retry.generation > original.generation);
    assert_eq!(
        grant(host.claim("right", "retry").await.unwrap()).generation,
        retry.generation
    );
    assert_eq!(
        callback(
            host,
            original,
            "heartbeat",
            &heartbeat_request(original, "left")
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(
        selected.worker_pull.unwrap().permits,
        [Some(retry.generation)]
    );
    assert_eq!(
        fold.dispatch_outbox
            .values()
            .filter(|row| row.attempt == 2)
            .count(),
        1
    );
    assert!(fold.timers.is_empty());
    assert_rivers_retry_completes(host, &retry, hash).await;
}

async fn assert_rivers_retry_completes(host: &WorkerPull, retry: &ClaimGrant, hash: &str) {
    assert_eq!(
        callback(host, retry, "started", &started_request(retry, "right"))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        callback(host, retry, "completed", &completed_request(retry, "right"))
            .await
            .0,
        StatusCode::OK
    );
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    let authority = selected.worker_pull.unwrap();
    assert_eq!(authority.permits, [None]);
    assert_eq!(authority.accepted_runs["run"].sha256, hash);
    assert_eq!(
        fold.runs["run"].state,
        crate::orchestration::compactor::RunState::Succeeded
    );
    assert!(fold.timers.is_empty());
}

#[tokio::test]
async fn fresh_pull_rejects_unbound_or_unsupported_retry_labels_and_supplied_hashes() {
    let host = WorkerPull::new(storage(), config()).unwrap();
    for (version, label, value) in [
        (1, RETRY_LABEL, COMPAT_PROFILE),
        (2, RETRY_LABEL, "rivers-d7cbc206-consumer-v1"),
        (2, RETRY_LABEL, "unknown"),
        (2, "user-label", "missing-policy"),
        (1, "arco.accepted_plan_sha256", "user-supplied"),
    ] {
        let mut rejected = plan("run", 1);
        rejected.version = version;
        run_labels(&mut rejected).insert(label.into(), value.into());
        assert!(
            host.accept_plan(&rejected).await.is_err(),
            "{version} {label} {value}"
        );
    }
    let idle = manual_advance(&host).await;
    assert_eq!(idle.advanced, 0);
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    assert!(selected.worker_pull.unwrap().accepted_runs.is_empty());
    assert!(fold.runs.is_empty());
}

#[tokio::test]
async fn rivers_accepted_plan_binder_freezes_final_causal_ids_before_admission() {
    let host = WorkerPull::new(storage(), config()).unwrap();
    let mut input = plan("run", 1);
    let timestamp = input.events[0]
        .event_id
        .parse::<Ulid>()
        .unwrap()
        .timestamp_ms();
    input.events[0].event_id = Ulid::from_parts(timestamp, (1 << 80) - 1).to_string();
    input.events[1].event_id = Ulid::nil().to_string();
    let bound = input.clone().with_rivers_retry_policy().unwrap();
    assert_eq!(bound.events[0].event_id, input.events[0].event_id);
    assert_eq!(
        bound.events[1].event_id,
        Ulid::from_parts(timestamp + 1, 0).to_string()
    );
    assert!(bound.clone().with_rivers_retry_policy().is_err());
    let rebound = input.with_rivers_retry_policy().unwrap();
    assert_eq!(
        arco_core::canonical_json::to_canonical_bytes(&bound).unwrap(),
        arco_core::canonical_json::to_canonical_bytes(&rebound).unwrap()
    );
    let hash = assert_rivers_admission(&host, &bound).await;
    assert_eq!(assert_rivers_admission(&host, &rebound).await, hash);
}

#[tokio::test]
async fn accepted_plan_v1_reader_preserves_legacy_run_labels() {
    let storage = storage();
    let mut legacy = plan("legacy", 1);
    run_labels(&mut legacy).insert(RETRY_LABEL.into(), "historical-user-label".into());
    let hash = legacy
        .persist(&storage, "legacy", "plan-legacy")
        .await
        .unwrap();
    let stored = load_accepted_run_plan(&storage, "legacy", "plan-legacy", &hash)
        .await
        .unwrap();
    assert_eq!(stored.version, 1);
    assert_eq!(
        arco_core::canonical_json::to_canonical_bytes(&stored).unwrap(),
        arco_core::canonical_json::to_canonical_bytes(&legacy).unwrap()
    );
}

#[tokio::test]
async fn manual_wake_bounds_backlog_and_uncertain_authority_is_retryable() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let host = WorkerPull::new(backend.scope(), config()).unwrap();
    host.accept_plan(&plan("run", 19)).await.unwrap();
    backend.arm(Fault::Read);
    let reply = post(
        &host.router(),
        "/run",
        &config().control_token,
        &Value::Null,
    )
    .await;
    assert_eq!(reply.0, StatusCode::SERVICE_UNAVAILABLE);
    backend.arm(Fault::Read);
    let reply = post(
        &host.router(),
        "/api/v1/tasks/claim",
        &config().sessions["left"],
        &serde_json::json!({"requestId": "first"}),
    )
    .await;
    assert_eq!(reply.0, StatusCode::SERVICE_UNAVAILABLE);
    let steps = [
        manual_advance(&host).await,
        manual_advance(&host).await,
        manual_advance(&host).await,
    ];
    assert_eq!(steps.each_ref().map(|step| step.advanced), [8, 8, 3]);
    assert_eq!(
        steps.each_ref().map(|step| step.immediate_work),
        [true, true, false]
    );
    assert_ne!(steps[0].observed_revision, steps[1].observed_revision);
    let (manifest, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(manifest.revision_ulid, steps[2].observed_revision);
    assert_eq!(fold.dispatch_outbox.len(), 19);
    let idle = manual_advance(&host).await;
    assert_eq!(idle.observed_revision, manifest.revision_ulid);
    assert_eq!(idle.advanced, 0);
    assert!(!idle.immediate_work);
}

#[tokio::test]
async fn selected_cancellation_prevents_start_and_releases_only_its_permit() {
    let host = WorkerPull::new(storage(), config()).unwrap();
    host.accept_plan(&plan("one", 1)).await.unwrap();
    host.accept_plan(&plan("two", 1)).await.unwrap();
    let original = grant(host.claim("left", "first").await.unwrap());
    host.cancel_run(CancelRunRequest {
        run_id: original.envelope.run_id.clone(),
        reason: None,
        requested_by: "operator".into(),
    })
    .await
    .unwrap();
    assert_eq!(
        callback(
            &host,
            &original,
            "started",
            &started_request(&original, "left")
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let replacement = grant(host.claim("right", "next").await.unwrap());
    assert_eq!(replacement.permit, original.permit);
    assert_ne!(replacement.envelope.run_id, original.envelope.run_id);
    assert!(matches!(
        host.claim("left", "first").await.unwrap(),
        ClaimReply::Cancelled
    ));
    let plain =
        MicroCompactor::new(host.storage.clone()).with_delta_tombstones(DeltaTombstoneMode::Emit);
    let error = plain.compact_events(vec![]).await.unwrap_err();
    assert!(error.to_string().contains("worker pull owns compaction"));
}

#[tokio::test]
async fn completion_replays_until_retention_deadline_and_prunes_without_releasing_reuse() {
    let clock = ManualClock::new();
    let storage = storage();
    let reopen = || clock.attach(WorkerPull::new(storage.clone(), config()).unwrap());
    let host = reopen();
    host.accept_plan(&plan("first-run", 1)).await.unwrap();
    let original = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(
        callback(
            &host,
            &original,
            "started",
            &started_request(&original, "left")
        )
        .await
        .0,
        StatusCode::OK
    );
    let completion = completed_request(&original, "left");
    let (status, response) = callback(&host, &original, "completed", &completion).await;
    assert_eq!(status, StatusCode::OK);
    let accepted_at: DateTime<Utc> =
        serde_json::from_value(response["serverTime"].clone()).unwrap();
    drop(host);

    clock.jump(DEDUP_HORIZON.num_seconds() - 1);
    let host = reopen();
    assert!(matches!(
        host.claim("left", "first").await.unwrap(),
        ClaimReply::Completed { generation, final_state, accepted_at: replayed }
            if generation == original.generation && final_state == "SUCCEEDED" && replayed == accepted_at
    ));
    assert!(matches!(
        host.claim("right", "first").await,
        Err(PullError::Conflict(_))
    ));
    host.accept_plan(&plan("second-run", 1)).await.unwrap();
    let replacement = grant(host.claim("right", "second").await.unwrap());
    assert_eq!(replacement.permit, original.permit);
    assert!(replacement.generation > original.generation);

    clock.jump(1);
    let host = reopen();
    let _ = manual_advance(&host).await;
    assert_eq!(
        callback(&host, &original, "completed", &completion).await.0,
        StatusCode::UNAUTHORIZED
    );
    let visible = grant(host.claim("right", "second").await.unwrap());
    assert_eq!(visible.generation, replacement.generation);
    let (manifest, _) = host.compactor.load_state().await.unwrap();
    let state = manifest.worker_pull.unwrap();
    assert!(!state.receipts.contains_key("first"));
    assert_eq!(state.permits, vec![Some(replacement.generation)]);
}

#[tokio::test]
async fn old_selected_watermark_does_not_gate_ordered_work() {
    let clock = ManualClock::new();
    let host = clock.attach(WorkerPull::new(storage(), config()).unwrap());
    let accepted = plan("run", 1);
    let revision = host.accept_plan(&accepted).await.unwrap();
    let selected_revision = host.accept_plan(&accepted).await.unwrap();
    assert_eq!(selected_revision, revision);
    let mut conflict = plan("different", 1);
    let OrchestrationEventData::RunTriggered { run_key, .. } = &mut conflict.events[0].data else {
        panic!("expected run event");
    };
    *run_key = Some("run".into());
    conflict.events[0].idempotency_key = conflict.events[0].data.idempotency_key();
    assert!(matches!(
        host.accept_plan(&conflict).await,
        Err(PullError::Conflict(_))
    ));

    // A legacy raw append is excluded from the selected pull acceptance log.
    super::super::LedgerWriter::new(host.storage.clone())
        .append(OrchestrationEvent::new(
            "tenant",
            "pull",
            OrchestrationEventData::RunCancelRequested {
                run_id: "run".into(),
                reason: None,
                requested_by: "legacy-side-path".into(),
            },
        ))
        .await
        .unwrap();
    clock.jump(3_600);
    let first = grant(host.claim("left", "first").await.unwrap());
    let (_, fold) = host.compactor.load_state().await.unwrap();
    assert!(!fold.runs["run"].cancel_requested);
    assert_eq!(
        callback(&host, &first, "started", &started_request(&first, "left"))
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn metadata_only_projection_reuses_unchanged_replacement_projection() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let clock = ManualClock::new();
    let host = clock.attach(WorkerPull::new(backend.scope(), config()).unwrap());
    host.accept_plan(&plan("run", 2)).await.unwrap();
    let original = grant(host.claim("left", "original").await.unwrap());
    assert_eq!(
        callback(
            &host,
            &original,
            "started",
            &started_request(&original, "left")
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        callback(
            &host,
            &original,
            "heartbeat",
            &heartbeat_request(&original, "left")
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        callback(
            &host,
            &original,
            "completed",
            &completed_request(&original, "left")
        )
        .await
        .0,
        StatusCode::OK
    );
    backend.take_projection_reads();
    let (before, _) = host.compactor.load_state().await.unwrap();
    let one_fold = backend.take_projection_reads();
    assert!(one_fold > 0 && !before.should_compact_l0());
    backend.take_lock_acquisitions();

    let replacement = grant(host.claim("right", "replacement").await.unwrap());
    let claim_cost = (
        backend.take_projection_reads(),
        backend.take_lock_acquisitions(),
    );
    eprintln!(
        "replacement claim: {} projection reads, {} acquisitions",
        claim_cost.0, claim_cost.1
    );
    assert_eq!(
        claim_cost,
        (one_fold, 1),
        "no-op recovery and claim need one fenced projection load"
    );
    let (after, fold) = host.compactor.load_state().await.unwrap();
    assert_ne!(before.manifest_id, after.manifest_id);
    assert_eq!(before.l0_count, after.l0_count);
    assert_eq!(
        after.worker_pull.unwrap().permits,
        vec![Some(replacement.generation)]
    );
    assert_eq!(fold.runs["run"].tasks_completed, 1);
    assert_ne!(replacement.envelope.task_id, original.envelope.task_id);
}

#[tokio::test]
async fn started_rollover_reads_deltas_ahead_in_order() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let scope = backend.scope();
    let reopen = || {
        let mut config = config();
        config.permits = 2;
        WorkerPull::new(scope.clone(), config).unwrap()
    };
    let host = reopen();
    host.accept_plan(&plan("run", 2)).await.unwrap();
    let first = grant(host.claim("left", "first").await.unwrap());
    let request = started_request(&first, "left");
    assert_eq!(
        callback(&host, &first, "started", &request).await.0,
        StatusCode::OK
    );
    let second = grant(host.claim("right", "second").await.unwrap());
    let (before, fold) = host.compactor.load_state().await.unwrap();
    assert!(before.l0_deltas.len() >= 3);
    assert_eq!(fold.dispatch_outbox.len(), 1);
    force_rollover(&scope, before).await;

    backend.arm(Fault::PauseFirstL0Read);
    let callback_host = reopen();
    let starting = tokio::spawn(async move {
        let request = started_request(&second, "right");
        callback(&callback_host, &second, "started", &request).await
    });
    tokio::time::timeout(Duration::from_secs(1), backend.arrived.notified())
        .await
        .expect("first L0 read is paused");
    let read_ahead = tokio::time::timeout(Duration::from_secs(1), backend.later_l0_read.notified())
        .await
        .is_ok();
    backend.resume.notify_one();
    let (status, reply) = starting.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{reply}");
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    assert!(selected.base_snapshot.snapshot_id.is_some());
    assert_eq!(selected.l0_count, 0);
    assert_eq!(fold.tasks.len(), 2);
    assert!(
        fold.tasks
            .values()
            .all(|task| task.state == TaskState::Running)
    );
    assert!(
        fold.dispatch_outbox.is_empty(),
        "earlier dispatch upserts must not resurrect"
    );
    assert!(
        read_ahead,
        "a later delta must read while the first delta is paused"
    );
}

#[tokio::test]
async fn metadata_only_projection_rollover_reads_the_new_projection() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let storage = backend.scope();
    let host = WorkerPull::new(storage.clone(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    host.advance().await.unwrap();
    backend.take_projection_reads();
    let (before, _) = host.compactor.load_state().await.unwrap();
    let one_fold = backend.take_projection_reads();
    assert!(one_fold > 0 && before.base_snapshot.snapshot_id.is_none());
    force_rollover(&storage, before).await;

    let granted = grant(host.claim("left", "first").await.unwrap());
    let claim_reads = backend.take_projection_reads();
    let (after, fold) = host.compactor.load_state().await.unwrap();
    let new_fold = backend.take_projection_reads();
    assert!(new_fold > 0 && after.base_snapshot.snapshot_id.is_some());
    assert_eq!(after.l0_count, 0);
    assert_eq!(
        after.worker_pull.unwrap().permits,
        vec![Some(granted.generation)]
    );
    assert_eq!(fold.dispatch_outbox.len(), 1);
    eprintln!(
        "eventless rollover: {claim_reads} projection reads ({one_fold} old, {new_fold} new)"
    );
    assert_eq!(
        claim_reads,
        one_fold + new_fold,
        "the selected rollover must hydrate its new base"
    );
}

async fn force_rollover(storage: &ScopedStorage, mut before: OrchestrationManifest) {
    before.l0_limits.max_count = before.l0_count;
    let pointer: crate::orchestration::compactor::OrchestrationManifestPointer =
        serde_json::from_slice(
            &storage
                .get_raw(crate::orchestration_manifest_pointer_path())
                .await
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        storage
            .put_raw(
                &pointer.manifest_path,
                Bytes::from(serde_json::to_vec(&before).unwrap()),
                WritePrecondition::None
            )
            .await
            .unwrap(),
        WriteResult::Success { .. }
    ));
    assert!(before.should_compact_l0());
}

#[tokio::test]
async fn callback_reads_only_predecessor_while_a_cold_load_reads_rolled_output() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let storage = backend.scope();
    let host = WorkerPull::new(storage.clone(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let first = grant(host.claim("left", "first").await.unwrap());
    backend.take_projection_reads();
    let (before, _) = host.compactor.load_state().await.unwrap();
    let one_fold = backend.take_projection_reads();
    assert!(one_fold > 0 && before.base_snapshot.snapshot_id.is_none());
    force_rollover(&storage, before).await;

    let (status, body) = callback(&host, &first, "started", &started_request(&first, "left")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let callback_reads = backend.take_projection_reads();
    let restarted = WorkerPull::new(storage, config()).unwrap();
    let (after, fold) = restarted.compactor.load_state().await.unwrap();
    let new_fold = backend.take_projection_reads();
    assert!(new_fold > 0 && after.base_snapshot.snapshot_id.is_some());
    assert_eq!(after.l0_count, 0);
    assert_eq!(new_fold, after.base_snapshot.tables.all().len());
    let state = after.worker_pull.unwrap();
    assert_eq!(state.permits, vec![Some(first.generation)]);
    assert!(state.receipts["first"].started_at.is_some());
    assert_eq!(
        fold.tasks
            .get(&("run".into(), "task-000".into()))
            .unwrap()
            .state,
        TaskState::Running
    );
    eprintln!(
        "started rollover: {callback_reads} callback projection reads ({one_fold} predecessor); cold reload {new_fold}"
    );
    assert_eq!(
        callback_reads, one_fold,
        "the callback only needs its selected receipt after publication"
    );
}

#[tokio::test]
async fn callback_replays_selected_completion_after_readback_failure() {
    assert_selected_completion_replays(Fault::ReadBack).await;
}

#[tokio::test]
async fn callback_rejects_a_changed_selected_receipt_and_replays_the_exact_completion() {
    assert_selected_completion_replays(Fault::ChangeReceiptAfter).await;
}

async fn assert_selected_completion_replays(fault: Fault) {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let storage = backend.scope();
    let clock = ManualClock::new();
    let host = clock.attach(WorkerPull::new(storage.clone(), config()).unwrap());
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let first = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(
        callback(&host, &first, "started", &started_request(&first, "left"))
            .await
            .0,
        StatusCode::OK
    );
    let request = completed_request(&first, "left");
    backend.arm(fault);
    let (status, body) = callback(&host, &first, "completed", &request).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");

    let restarted = clock.attach(WorkerPull::new(storage, config()).unwrap());
    let (selected, fold) = restarted.compactor.load_state().await.unwrap();
    let authority = selected.worker_pull.unwrap();
    assert_eq!(authority.permits, [None]);
    assert_eq!(fold.runs["run"].tasks_completed, 1);
    let receipt = &authority.receipts["first"];
    assert_eq!(receipt.generation, first.generation);
    let completion = receipt.completion.as_ref().unwrap();
    assert_eq!(receipt.phase, Phase::Completed);
    let (status, body) = callback(&restarted, &first, "completed", &request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["finalState"], completion.state);
    assert_eq!(
        body["serverTime"],
        serde_json::json!(completion.accepted_at)
    );
    assert_eq!(
        restarted
            .compactor
            .load_state()
            .await
            .unwrap()
            .0
            .revision_ulid,
        selected.revision_ulid
    );
}

#[tokio::test(start_paused = true)]
async fn timeout_awaits_fenced_release_then_replays_the_selected_grant() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let storage = backend.scope();
    let host = WorkerPull::new(storage.clone(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    host.advance().await.unwrap();
    let lock = DistributedLock::new(
        storage.backend().clone(),
        orchestration_compaction_lock_path(),
    );

    backend.arm(Fault::TimeoutAfter);
    let router = host.router();
    let config = config();
    let payload = serde_json::json!({"requestId": "first"});
    let started = tokio::time::Instant::now();
    let call = post(
        &router,
        "/api/v1/tasks/claim",
        &config.sessions["left"],
        &payload,
    );
    tokio::pin!(call);
    tokio::select! {
        biased;
        () = backend.arrived.notified() => {}
        (status, body) = &mut call => {
            let lock = lock.read_lock_info().await.unwrap().unwrap();
            panic!("route returned {status} {body} before release; lock expired: {}", lock.is_expired());
        }
    }
    let held = lock.read_lock_info().await.unwrap().unwrap();
    assert!(!held.is_expired());
    let release_started = started.elapsed();
    assert!(release_started < OPERATION_TIMEOUT);
    tokio::time::advance(Duration::from_millis(100)).await;
    backend.resume.notify_one();
    let (status, body) = call.await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "worker-pull invocation timed out");
    let elapsed = started.elapsed();
    eprintln!("release began at {release_started:?}; timeout response at {elapsed:?}");
    assert!(elapsed <= OPERATION_TIMEOUT);
    let released = lock.read_lock_info().await.unwrap().unwrap();
    assert!(released.is_expired());
    assert_eq!(released.sequence_number, held.sequence_number);

    let (selected, _) = host.compactor.load_state().await.unwrap();
    let authority = selected.worker_pull.unwrap();
    let generation = authority.receipts["first"].generation;
    assert_eq!(authority.permits, vec![Some(generation)]);
    let replay = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(replay.generation, generation);
    assert_eq!(
        host.compactor.load_state().await.unwrap().0.revision_ulid,
        selected.revision_ulid
    );
}

#[tokio::test]
async fn callback_rechecks_changed_predecessor_before_using_loaded_projection() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let storage = backend.scope();
    let host = WorkerPull::new(storage.clone(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    let first = grant(host.claim("left", "first").await.unwrap());
    let initial = host.compactor.load_state().await.unwrap().0;
    let request = started_request(&first, "left");

    backend.arm(Fault::ArmPauseHead);
    let start = callback(&host, &first, "started", &request);
    tokio::pin!(start);
    tokio::select! {
        () = backend.arrived.notified() => {}
        result = &mut start => panic!("start returned before recheck: {result:?}"),
    }
    let lock = DistributedLock::new(
        storage.backend().clone(),
        orchestration_compaction_lock_path(),
    );
    lock.force_break().await.unwrap();
    let fresh = WorkerPull::new(storage, config()).unwrap();
    fresh
        .cancel_run(CancelRunRequest {
            run_id: "run".into(),
            reason: None,
            requested_by: "operator".into(),
        })
        .await
        .unwrap();
    let cancelled = fresh.compactor.load_state().await.unwrap().0;
    assert_ne!(cancelled.revision_ulid, initial.revision_ulid);
    backend.resume.notify_one();
    let (status, body) = start.await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    let (selected, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(selected.revision_ulid, cancelled.revision_ulid);
    assert!(fold.runs["run"].cancel_requested);
    let authority = selected.worker_pull.unwrap();
    assert_eq!(authority.permits, vec![None]);
    assert!(authority.receipts["first"].started_at.is_none());
    assert_eq!(authority.receipts["first"].phase, Phase::Cancelled);
}

#[tokio::test]
async fn claim_recovers_at_most_one_visible_batch_even_when_busy_or_replaying() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let host = WorkerPull::new(backend.scope(), config()).unwrap();
    host.accept_plan(&plan("run", 19)).await.unwrap();
    let (initial, fold) = host.compactor.load_state().await.unwrap();
    assert!(fold.dispatch_outbox.is_empty());

    let first = grant(host.claim("left", "first").await.unwrap());
    let (selected, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(fold.dispatch_outbox.len(), MAX_ADVANCE_ITEMS);
    assert_eq!(
        selected.worker_pull.unwrap().permits,
        vec![Some(first.generation)]
    );
    assert_ne!(initial.revision_ulid, selected.revision_ulid);
    assert!(matches!(
        host.claim("right", "second").await.unwrap(),
        ClaimReply::Busy
    ));
    let (_, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(fold.dispatch_outbox.len(), 2 * MAX_ADVANCE_ITEMS);

    let replay = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(replay.generation, first.generation);
    assert_eq!(replay.envelope, first.envelope);
    let (drained, fold) = host.compactor.load_state().await.unwrap();
    assert_eq!(fold.dispatch_outbox.len(), 19);
    backend.take_lock_acquisitions();
    assert!(matches!(
        host.claim("right", "second").await.unwrap(),
        ClaimReply::Busy
    ));
    assert_eq!(backend.take_lock_acquisitions(), 1);
    assert_eq!(
        host.compactor.load_state().await.unwrap().0.revision_ulid,
        drained.revision_ulid
    );
}

#[tokio::test]
async fn metadata_only_projection_still_requires_selected_readback() {
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new())));
    let host = WorkerPull::new(backend.scope(), config()).unwrap();
    host.accept_plan(&plan("run", 1)).await.unwrap();
    host.advance().await.unwrap();

    backend.arm(Fault::ReadBack);
    let error = host.claim("left", "first").await.unwrap_err();
    assert!(error.to_string().contains("selected pointer read failed"));
    let (selected, _) = host.compactor.load_state().await.unwrap();
    let generation = selected.worker_pull.unwrap().receipts["first"].generation;
    let replay = grant(host.claim("left", "first").await.unwrap());
    assert_eq!(replay.generation, generation);
    assert_eq!(
        host.compactor.load_state().await.unwrap().0.revision_ulid,
        selected.revision_ulid
    );
}
