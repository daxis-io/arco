//! Correctness regression tests for orchestration invariants.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};
use ulid::Ulid;

use arco_core::MemoryBackend;
use arco_core::ScopedStorage;
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{AssetId, RunId, TaskId};
use arco_flow::dispatch::memory::InMemoryTaskQueue;
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult, TaskEnvelope, TaskQueue};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::{FoldState, TaskRow, TaskState};
use arco_flow::orchestration::compactor::manifest::OrchestrationManifest;
use arco_flow::orchestration::controllers::{
    AntiEntropySweeper, DispatcherController, ReadyDispatchController,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, SourceRef, TaskDef, TaskOutcome, TriggerInfo,
};
use arco_flow::plan::{AssetKey, ResourceRequirements};
use arco_flow::task_key::{TaskKey, TaskOperation};

fn run_triggered_event(run_id: &str, plan_id: &str) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            trigger: TriggerInfo::Manual {
                user_id: "tester".to_string(),
            },
            root_assets: Vec::new(),
            run_key: None,
            labels: std::collections::HashMap::new(),
            code_version: None,
        },
    )
}

fn plan_created_event(run_id: &str, plan_id: &str, tasks: Vec<TaskDef>) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            tasks,
        },
    )
}

fn task_started_event(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    attempt_id: &str,
) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::TaskStarted {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-1".to_string(),
        },
    )
}

fn task_finished_event(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    attempt_id: &str,
    outcome: TaskOutcome,
) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::TaskFinished {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-1".to_string(),
            outcome,
            materialization_id: None,
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
            asset_key: None,
            partition_key: None,
            code_version: None,
        },
    )
}

fn default_task_def(key: &str, depends_on: Vec<&str>) -> TaskDef {
    TaskDef {
        key: key.to_string(),
        depends_on: depends_on.into_iter().map(|dep| dep.to_string()).collect(),
        asset_key: None,
        partition_key: None,
        max_attempts: 3,
        heartbeat_timeout_sec: 60,
    }
}

fn fresh_manifest() -> OrchestrationManifest {
    let mut manifest = OrchestrationManifest::new("01HQTESTREV");
    manifest.watermarks.last_processed_at = Utc::now() - Duration::seconds(5);
    manifest
}

fn fold_events_sorted(events: Vec<OrchestrationEvent>) -> FoldState {
    let mut sorted = events;
    sorted.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    let mut state = FoldState::new();
    for event in &sorted {
        state.fold_event(event);
    }
    state
}

#[test]
fn test_order_independence() -> Result<()> {
    let run_id = "run-order";
    let plan_id = "plan-order";
    let attempt_id = Ulid::new().to_string();

    let tasks = vec![
        default_task_def("extract", vec![]),
        default_task_def("transform", vec!["extract"]),
    ];

    let events = vec![
        run_triggered_event(run_id, plan_id),
        plan_created_event(run_id, plan_id, tasks),
        task_started_event(run_id, "extract", 1, &attempt_id),
        task_finished_event(run_id, "extract", 1, &attempt_id, TaskOutcome::Succeeded),
    ];

    let state_forward = fold_events_sorted(events.clone());
    let mut reverse = events;
    reverse.reverse();
    let state_reverse = fold_events_sorted(reverse);

    assert_eq!(state_forward.runs, state_reverse.runs);
    assert_eq!(state_forward.tasks, state_reverse.tasks);
    assert_eq!(
        state_forward.dep_satisfaction,
        state_reverse.dep_satisfaction
    );
    assert_eq!(state_forward.dispatch_outbox, state_reverse.dispatch_outbox);
    assert_eq!(state_forward.timers, state_reverse.timers);

    Ok(())
}

#[test]
fn test_duplicate_event_is_noop() -> Result<()> {
    let run_id = "run-dup";
    let plan_id = "plan-dup";
    let attempt_id = Ulid::new().to_string();

    let tasks = vec![default_task_def("extract", vec![])];

    let mut state = FoldState::new();
    state.fold_event(&run_triggered_event(run_id, plan_id));
    state.fold_event(&plan_created_event(run_id, plan_id, tasks));

    let event = task_finished_event(run_id, "extract", 1, &attempt_id, TaskOutcome::Succeeded);
    state.fold_event(&event);

    let snapshot = (
        state.runs.clone(),
        state.tasks.clone(),
        state.dep_satisfaction.clone(),
        state.dispatch_outbox.clone(),
        state.timers.clone(),
    );

    state.fold_event(&event);

    assert_eq!(snapshot.0, state.runs);
    assert_eq!(snapshot.1, state.tasks);
    assert_eq!(snapshot.2, state.dep_satisfaction);
    assert_eq!(snapshot.3, state.dispatch_outbox);
    assert_eq!(snapshot.4, state.timers);

    Ok(())
}

#[test]
fn test_controller_determinism_ready_dispatch() -> Result<()> {
    let run_id = "run-ready";
    let plan_id = "plan-ready";

    let tasks = vec![default_task_def("extract", vec![])];

    let mut state = FoldState::new();
    state.fold_event(&run_triggered_event(run_id, plan_id));
    state.fold_event(&plan_created_event(run_id, plan_id, tasks));

    let manifest = fresh_manifest();
    let controller = ReadyDispatchController::with_defaults();

    let actions1 = controller.reconcile(&manifest, &state);
    let actions2 = controller.reconcile(&manifest, &state);

    assert_eq!(actions1, actions2);
    Ok(())
}

#[test]
fn test_controller_determinism_dispatcher() -> Result<()> {
    let run_id = "run-dispatch";
    let plan_id = "plan-dispatch";
    let attempt_id = Ulid::new().to_string();

    let tasks = vec![default_task_def("extract", vec![])];

    let mut state = FoldState::new();
    state.fold_event(&run_triggered_event(run_id, plan_id));
    state.fold_event(&plan_created_event(run_id, plan_id, tasks));

    let dispatch_event = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::DispatchRequested {
            run_id: run_id.to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: attempt_id.clone(),
            worker_queue: "default-queue".to_string(),
            dispatch_id: format!("dispatch:{run_id}:extract:1"),
        },
    );
    state.fold_event(&dispatch_event);

    let manifest = fresh_manifest();
    let controller = DispatcherController::with_defaults();
    let outbox_rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();

    let actions1 = controller.reconcile(&manifest, &outbox_rows);
    let actions2 = controller.reconcile(&manifest, &outbox_rows);

    assert_eq!(actions1, actions2);
    Ok(())
}

#[tokio::test]
async fn test_task_queue_idempotency() -> Result<()> {
    let queue = InMemoryTaskQueue::new("test");
    let envelope = TaskEnvelope::new(
        TaskId::generate(),
        RunId::generate(),
        AssetId::generate(),
        TaskKey::new(AssetKey::new("raw", "events"), TaskOperation::Materialize),
        "tenant",
        "workspace",
        1,
        ResourceRequirements::default(),
    );

    let result1 = queue
        .enqueue(envelope.clone(), EnqueueOptions::default())
        .await?;
    let result2 = queue.enqueue(envelope, EnqueueOptions::default()).await?;

    assert!(matches!(result1, EnqueueResult::Enqueued { .. }));
    assert!(matches!(result2, EnqueueResult::Deduplicated { .. }));

    Ok(())
}

#[test]
fn test_anti_entropy_recovers_orphaned_tasks() -> Result<()> {
    let now = Utc::now();
    let sweeper = AntiEntropySweeper::with_defaults();

    let task = TaskRow {
        run_id: "run-orphan".to_string(),
        task_key: "extract".to_string(),
        state: TaskState::Ready,
        attempt: 0,
        attempt_id: Some("01HQ123ATT".to_string()),
        started_at: None,
        completed_at: None,
        error_message: None,
        deps_total: 0,
        deps_satisfied_count: 0,
        max_attempts: 3,
        heartbeat_timeout_sec: 60,
        last_heartbeat_at: None,
        ready_at: Some(now - Duration::minutes(10)),
        asset_key: None,
        partition_key: None,
        materialization_id: None,
        delta_table: None,
        delta_version: None,
        delta_partition: None,
        execution_lineage_ref: None,
        row_version: "01HQ123EVT".to_string(),
    };

    let repairs = sweeper.scan(&fresh_manifest().watermarks, &[task], &[], now);

    assert_eq!(repairs.len(), 1);
    assert!(matches!(
        repairs[0],
        arco_flow::orchestration::controllers::Repair::CreateDispatchOutbox { .. }
    ));

    Ok(())
}

#[test]
fn test_dependency_correctness() -> Result<()> {
    let run_id = "run-deps";
    let plan_id = "plan-deps";
    let attempt_id = Ulid::new().to_string();

    let tasks = vec![
        default_task_def("A", vec![]),
        default_task_def("B", vec!["A"]),
        default_task_def("C", vec!["B"]),
    ];

    let mut state = FoldState::new();
    state.fold_event(&run_triggered_event(run_id, plan_id));
    state.fold_event(&plan_created_event(run_id, plan_id, tasks));

    state.fold_event(&task_started_event(run_id, "A", 1, &attempt_id));

    let task_b_key = (run_id.to_string(), "B".to_string());
    let task_c_key = (run_id.to_string(), "C".to_string());

    let task_b = state
        .tasks
        .get(&task_b_key)
        .ok_or_else(|| Error::configuration("missing task B"))?;
    let task_c = state
        .tasks
        .get(&task_c_key)
        .ok_or_else(|| Error::configuration("missing task C"))?;

    assert_eq!(task_b.state, TaskState::Blocked);
    assert_eq!(task_c.state, TaskState::Blocked);

    state.fold_event(&task_finished_event(
        run_id,
        "A",
        1,
        &attempt_id,
        TaskOutcome::Succeeded,
    ));

    let task_b = state
        .tasks
        .get(&task_b_key)
        .ok_or_else(|| Error::configuration("missing task B"))?;
    let task_c = state
        .tasks
        .get(&task_c_key)
        .ok_or_else(|| Error::configuration("missing task C"))?;

    assert_eq!(task_b.state, TaskState::Ready);
    assert_eq!(task_c.state, TaskState::Blocked);

    Ok(())
}

#[tokio::test]
async fn test_controller_never_reads_ledger() -> Result<()> {
    let backend = Arc::new(CountingBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace")?;
    let ledger = LedgerWriter::new(storage.clone());
    let compactor = MicroCompactor::new(storage);

    let run_id = "run-ledger";
    let plan_id = "plan-ledger";
    let tasks = vec![default_task_def("extract", vec![])];

    let events = vec![
        run_triggered_event(run_id, plan_id),
        plan_created_event(run_id, plan_id, tasks),
    ];

    let mut paths = Vec::new();
    for event in events {
        let path = LedgerWriter::event_path(&event);
        ledger.append(event).await?;
        paths.push(path);
    }

    compactor.compact_events(paths).await?;
    backend.reset_ledger_reads();

    let _ = compactor.load_state().await?;

    assert_eq!(backend.ledger_reads(), 0);
    Ok(())
}

#[derive(Debug)]
struct CountingBackend {
    inner: MemoryBackend,
    ledger_reads: AtomicUsize,
}

impl CountingBackend {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            ledger_reads: AtomicUsize::new(0),
        }
    }

    fn ledger_reads(&self) -> usize {
        self.ledger_reads.load(Ordering::Relaxed)
    }

    fn reset_ledger_reads(&self) {
        self.ledger_reads.store(0, Ordering::Relaxed);
    }

    fn record_if_ledger(&self, path: &str) {
        if path.contains("/ledger/") {
            self.ledger_reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl StorageBackend for CountingBackend {
    async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
        self.record_if_ledger(path);
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::error::Result<Bytes> {
        self.record_if_ledger(path);
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::error::Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
        self.record_if_ledger(prefix);
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
        self.record_if_ledger(path);
        self.inner.head(path).await
    }

    async fn signed_url(
        &self,
        path: &str,
        expiry: std::time::Duration,
    ) -> arco_core::error::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

// ============================================================================
// Run Key Index + Conflict Detection Tests (Task 6.2)
// ============================================================================

fn run_requested_event(run_key: &str, fingerprint: &str) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: fingerprint.to_string(),
            asset_selection: vec!["analytics.daily".to_string()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "sched_01".to_string(),
                tick_id: "tick_01".to_string(),
            },
            labels: std::collections::HashMap::new(),
        },
    )
}

#[test]
fn test_fold_run_requested_creates_run_key_index() {
    let mut state = FoldState::new();

    let event = run_requested_event("sched:daily-etl:1736935200", "fingerprint_v1");

    state.fold_event(&event);

    // Should create run_key_index entry
    let index = state.run_key_index.get("sched:daily-etl:1736935200");
    assert!(index.is_some(), "run_key_index entry should exist");

    let index = index.unwrap();
    assert_eq!(index.request_fingerprint, "fingerprint_v1");
    assert!(!index.run_id.is_empty(), "run_id should be generated");
}

#[test]
fn test_fold_run_requested_detects_conflict_on_fingerprint_mismatch() {
    let mut state = FoldState::new();

    // First request
    let event1 = run_requested_event("sched:daily-etl:1736935200", "fingerprint_v1");
    state.fold_event(&event1);

    // Second request with same run_key but different fingerprint
    let event2 = run_requested_event("sched:daily-etl:1736935200", "fingerprint_v2");
    state.fold_event(&event2);

    // Should record conflict
    assert_eq!(state.run_key_conflicts.len(), 1, "should have one conflict");

    let conflict = state.run_key_conflicts.values().next().unwrap();
    assert_eq!(conflict.run_key, "sched:daily-etl:1736935200");
    assert_eq!(conflict.existing_fingerprint, "fingerprint_v1");
    assert_eq!(conflict.conflicting_fingerprint, "fingerprint_v2");
}

#[test]
fn test_fold_run_requested_is_idempotent_on_same_fingerprint() {
    let mut state = FoldState::new();

    let event = run_requested_event("sched:daily-etl:1736935200", "fingerprint_v1");

    // Process twice
    state.fold_event(&event.clone());
    let index_after_first = state.run_key_index.len();

    state.fold_event(&event);
    let index_after_second = state.run_key_index.len();

    // Should not create duplicate entry
    assert_eq!(
        index_after_first, index_after_second,
        "duplicate event should not create new index entry"
    );
    assert!(
        state.run_key_conflicts.is_empty(),
        "same fingerprint should not create conflict"
    );
}
