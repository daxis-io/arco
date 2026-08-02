//! Regression tests for the durable retry path (issues #250 / #337).
//!
//! These tests drive the deployed convergence machinery — the fold, the
//! ready-dispatch controller, and the anti-entropy sweeper — rather than
//! calling `RetryHandler::fire` directly, which is how the original defect
//! survived a green suite: nothing ever emitted the first retry deadline, so
//! every first task failure wedged its run in `RetryWait` forever.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Duration;

use arco_flow::orchestration::compactor::fold::{DispatchStatus, FoldState, TaskState};
use arco_flow::orchestration::compactor::manifest::OrchestrationManifest;
use arco_flow::orchestration::controllers::{
    AntiEntropySweeper, ReadyDispatchAction, ReadyDispatchController, Repair,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TaskOutcome, TriggerInfo,
};
use arco_flow::orchestration::ids::deterministic_attempt_id;

fn task_def(key: &str) -> TaskDef {
    TaskDef {
        key: key.to_string(),
        depends_on: Vec::new(),
        asset_key: Some(key.to_string()),
        partition_key: None,
        max_attempts: 3,
        heartbeat_timeout_sec: 300,
        requires_visible_output: false,
    }
}

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

fn dispatch_requested_event(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    attempt_id: &str,
    dispatch_id: &str,
) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::DispatchRequested {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_queue: "default-queue".to_string(),
            dispatch_id: dispatch_id.to_string(),
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

fn task_failed_event(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    attempt_id: &str,
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
            outcome: TaskOutcome::Failed,
            materialization_id: None,
            error_message: Some("transient failure".to_string()),
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress_json: None,
            asset_key: None,
            partition_key: None,
            code_version: None,
        },
    )
}

fn fresh_manifest(last_processed_at: chrono::DateTime<chrono::Utc>) -> OrchestrationManifest {
    let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
    manifest.watermarks.last_processed_at = last_processed_at;
    manifest
}

/// The #250 invariant, end to end at default configuration: a task whose
/// first attempt FAILS must get a second attempt dispatched by the deployed
/// controller loop — fold decides `RetryWait` and schedules the retry
/// deadline, the anti-entropy sweeper bootstraps the next attempt's dispatch
/// once the deadline passes, and folding that dispatch moves the task to
/// attempt 2.
#[test]
fn first_task_failure_yields_a_dispatched_retry_attempt() {
    let run_id = "run1";
    let task_key = "extract";
    let mut state = FoldState::new();

    state.fold_event(&run_triggered_event(run_id, "plan1"));
    state.fold_event(&plan_created_event(
        run_id,
        "plan1",
        vec![task_def(task_key)],
    ));

    // The ready-dispatch controller emits attempt 1.
    let controller = ReadyDispatchController::with_defaults();
    let manifest = fresh_manifest(chrono::Utc::now());
    let actions = controller.reconcile(&manifest, &state);
    assert_eq!(actions.len(), 1);
    let ReadyDispatchAction::EmitDispatchRequested {
        attempt,
        attempt_id,
        dispatch_id,
        ..
    } = actions[0].clone()
    else {
        panic!("expected EmitDispatchRequested for the planned task");
    };
    assert_eq!(attempt, 1);

    state.fold_event(&dispatch_requested_event(
        run_id,
        task_key,
        attempt,
        &attempt_id,
        &dispatch_id,
    ));
    state.fold_event(&task_started_event(run_id, task_key, attempt, &attempt_id));

    // Worker reports FAILED for the first attempt.
    let failure = task_failed_event(run_id, task_key, attempt, &attempt_id);
    let failed_at = failure.timestamp;
    state.fold_event(&failure);

    let task = state
        .tasks
        .get(&(run_id.to_string(), task_key.to_string()))
        .expect("task row");
    assert_eq!(task.state, TaskState::RetryWait);
    assert_eq!(task.attempt, 1);
    let deadline = task
        .retry_not_before
        .expect("fold must schedule the retry deadline (issue #337)");
    assert!(
        deadline > failed_at,
        "retry deadline must apply backoff after the failure"
    );

    // Anti-entropy sweeps after the deadline with a coherent clock: the
    // failure event was the last thing compacted.
    let now = deadline + Duration::seconds(1);
    let sweeper = AntiEntropySweeper::with_defaults();
    let tasks: Vec<_> = state.tasks.values().cloned().collect();
    let outbox: Vec<_> = state.dispatch_outbox.values().cloned().collect();
    let mut watermarks = fresh_manifest(failed_at).watermarks;
    watermarks.last_processed_at = failed_at;

    let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);
    assert_eq!(repairs.len(), 1, "expected exactly one repair: {repairs:?}");
    let Repair::CreateDispatchOutbox {
        attempt: retry_attempt,
        reason,
        ..
    } = &repairs[0]
    else {
        panic!("expected CreateDispatchOutbox, got {:?}", repairs[0]);
    };
    assert_eq!(*retry_attempt, 2);
    assert_eq!(reason, "retry_wait_bootstrap");

    // The sweeper service turns the repair into a DispatchRequested event
    // (see arco_flow_sweeper::run_handler); folding it dispatches attempt 2.
    let retry_dispatch_id = format!("dispatch:{run_id}:{task_key}:2");
    let retry_attempt_id = deterministic_attempt_id(&retry_dispatch_id);
    state.fold_event(&dispatch_requested_event(
        run_id,
        task_key,
        2,
        &retry_attempt_id,
        &retry_dispatch_id,
    ));

    let task = state
        .tasks
        .get(&(run_id.to_string(), task_key.to_string()))
        .expect("task row");
    assert_eq!(
        task.state,
        TaskState::Dispatched,
        "retry attempt must actually dispatch"
    );
    assert_eq!(task.attempt, 2);
    assert_eq!(task.attempt_id.as_deref(), Some(retry_attempt_id.as_str()));
    assert_eq!(
        task.retry_not_before, None,
        "dispatching the retry clears the deadline"
    );

    let outbox_row = state
        .dispatch_outbox
        .get(&retry_dispatch_id)
        .expect("retry dispatch outbox row");
    assert_eq!(outbox_row.attempt, 2);
    assert_eq!(outbox_row.status, DispatchStatus::Pending);
}

/// The sweeper must not bootstrap the retry before the fold-scheduled
/// deadline: backoff is preserved.
#[test]
fn retry_is_not_bootstrapped_before_the_backoff_deadline() {
    let run_id = "run1";
    let task_key = "extract";
    let mut state = FoldState::new();

    state.fold_event(&run_triggered_event(run_id, "plan1"));
    state.fold_event(&plan_created_event(
        run_id,
        "plan1",
        vec![task_def(task_key)],
    ));
    state.fold_event(&dispatch_requested_event(
        run_id,
        task_key,
        1,
        "att-1",
        "dispatch:run1:extract:1",
    ));
    state.fold_event(&task_started_event(run_id, task_key, 1, "att-1"));
    let failure = task_failed_event(run_id, task_key, 1, "att-1");
    let failed_at = failure.timestamp;
    state.fold_event(&failure);

    let tasks: Vec<_> = state.tasks.values().cloned().collect();
    let outbox: Vec<_> = state.dispatch_outbox.values().cloned().collect();
    let mut watermarks = fresh_manifest(failed_at).watermarks;
    watermarks.last_processed_at = failed_at;

    // One second after the failure the backoff window is still open.
    let sweeper = AntiEntropySweeper::with_defaults();
    let repairs = sweeper.scan(
        &watermarks,
        &tasks,
        &outbox,
        failed_at + Duration::seconds(1),
    );
    assert!(repairs.is_empty(), "backoff must be preserved: {repairs:?}");
}

/// Exhausted attempts must terminally fail instead of scheduling a retry.
#[test]
fn exhausted_attempts_fail_terminally_without_retry_deadline() {
    let run_id = "run1";
    let task_key = "extract";
    let mut state = FoldState::new();

    state.fold_event(&run_triggered_event(run_id, "plan1"));
    let mut def = task_def(task_key);
    def.max_attempts = 1;
    state.fold_event(&plan_created_event(run_id, "plan1", vec![def]));
    state.fold_event(&dispatch_requested_event(
        run_id,
        task_key,
        1,
        "att-1",
        "dispatch:run1:extract:1",
    ));
    state.fold_event(&task_started_event(run_id, task_key, 1, "att-1"));
    state.fold_event(&task_failed_event(run_id, task_key, 1, "att-1"));

    let task = state
        .tasks
        .get(&(run_id.to_string(), task_key.to_string()))
        .expect("task row");
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.retry_not_before, None);

    let run = state.runs.get(run_id).expect("run row");
    assert_eq!(run.tasks_failed, 1);
    assert_eq!(run.tasks_completed, 1);
}
