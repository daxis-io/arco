//! Custom assertion helpers for integration tests.

use arco_flow::events::EventEnvelope;
use arco_flow::plan::Plan;
use arco_flow::run::{Run, RunState};

use crate::storage::StorageOp;

/// Asserts that a run completed successfully.
///
/// # Panics
///
/// Panics if the run did not succeed or has failed tasks.
pub fn assert_run_succeeded(run: &Run) {
    assert_eq!(
        run.state,
        RunState::Succeeded,
        "Expected run to succeed, but state was {:?}",
        run.state
    );
    assert_eq!(
        run.tasks_failed(),
        0,
        "Expected no failed tasks, but {} failed",
        run.tasks_failed()
    );
}

/// Asserts that a run failed.
///
/// # Panics
///
/// Panics if the run did not fail.
pub fn assert_run_failed(run: &Run) {
    assert_eq!(
        run.state,
        RunState::Failed,
        "Expected run to fail, but state was {:?}",
        run.state
    );
    assert!(run.tasks_failed() > 0, "Expected at least one failed task");
}

/// Asserts that all tasks in a run are in terminal states.
///
/// # Panics
///
/// Panics if any task is not in a terminal state.
pub fn assert_all_tasks_terminal(run: &Run) {
    for exec in &run.task_executions {
        assert!(
            exec.is_terminal(),
            "Task {:?} is not terminal (state: {:?})",
            exec.task_id,
            exec.state
        );
    }
}

/// Asserts that tasks were executed in topological order.
///
/// # Panics
///
/// Panics if any task started before its dependencies completed.
pub fn assert_topological_order(run: &Run, plan: &Plan) {
    for task in &run.task_executions {
        if let Some(spec) = plan.get_task(&task.task_id) {
            for dep_id in &spec.upstream_task_ids {
                let dep_exec = run.get_task(dep_id).expect("dependency should exist");

                // If this task started, dependency must have completed first
                if let (Some(started), Some(dep_completed)) = (task.started_at, dep_exec.completed_at)
                {
                    assert!(
                        dep_completed <= started,
                        "Task {:?} started before dependency {:?} completed",
                        task.task_id,
                        dep_id
                    );
                }
            }
        }
    }
}

/// Asserts that events have monotonically increasing timestamps.
///
/// # Panics
///
/// Panics if events are not ordered by timestamp.
pub fn assert_events_ordered(events: &[EventEnvelope]) {
    for window in events.windows(2) {
        let time0 = window[0].time.expect("event should have time");
        let time1 = window[1].time.expect("event should have time");
        assert!(
            time0 <= time1,
            "Events not ordered: {:?} > {:?}",
            time0,
            time1
        );
    }
}

/// Asserts that all events have unique IDs.
///
/// # Panics
///
/// Panics if any duplicate event ID is found.
pub fn assert_events_unique(events: &[EventEnvelope]) {
    let mut seen = std::collections::HashSet::new();
    for event in events {
        assert!(seen.insert(&event.id), "Duplicate event ID: {}", event.id);
    }
}

/// Asserts that storage operations contain expected patterns.
///
/// # Panics
///
/// Panics if expected operations are not found.
pub fn assert_storage_ops_contain(ops: &[StorageOp], expected: &[(&str, &str)]) {
    for (op_type, path_prefix) in expected {
        let found = ops.iter().any(|op| {
            let (actual_type, actual_path) = match op {
                StorageOp::Get { path } => ("get", path.as_str()),
                StorageOp::GetRange { path, .. } => ("get_range", path.as_str()),
                StorageOp::Head { path } => ("head", path.as_str()),
                StorageOp::Put { path, .. } => ("put", path.as_str()),
                StorageOp::Delete { path } => ("delete", path.as_str()),
                StorageOp::List { prefix } => ("list", prefix.as_str()),
            };
            actual_type == *op_type && actual_path.starts_with(path_prefix)
        });
        assert!(
            found,
            "Expected {op_type} operation on path starting with '{path_prefix}', not found in {ops:?}",
        );
    }
}

/// Asserts that no storage operations accessed a given path prefix.
///
/// Useful for verifying invariants like "readers never access ledger".
///
/// # Panics
///
/// Panics if any operation accessed the given prefix.
pub fn assert_storage_ops_exclude(ops: &[StorageOp], forbidden_prefix: &str) {
    for op in ops {
        let path = match op {
            StorageOp::Get { path }
            | StorageOp::GetRange { path, .. }
            | StorageOp::Head { path }
            | StorageOp::Put { path, .. }
            | StorageOp::Delete { path }
            | StorageOp::List { prefix: path } => path,
        };
        assert!(
            !path.starts_with(forbidden_prefix),
            "Operation on forbidden path: {path} (prefix: {forbidden_prefix})",
        );
    }
}
