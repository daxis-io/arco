//! Planned execution scope carried in worker dispatch envelopes.

use arco_worker_contract::WorkerEnginePayload;

use crate::orchestration::compactor::fold::FoldState;

/// Builds the worker engine payload for a planned run task.
///
/// A missing task row returns `None` so dispatch producers fail closed instead
/// of executing an unscoped task and recording partition-specific success.
#[must_use]
pub fn engine_payload_for_task(
    state: &FoldState,
    run_id: &str,
    task_key: &str,
) -> Option<WorkerEnginePayload> {
    let task = state
        .tasks
        .get(&(run_id.to_string(), task_key.to_string()))?;
    Some(WorkerEnginePayload {
        partition_key: task
            .partition_key
            .clone()
            .filter(|partition_key| !partition_key.is_empty()),
        heartbeat_timeout_sec: (task.heartbeat_timeout_sec > 0)
            .then_some(task.heartbeat_timeout_sec),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::orchestration::compactor::fold::TaskRow;

    #[allow(clippy::expect_used)]
    fn task_row(partition_key: Option<&str>, heartbeat_timeout_sec: u32) -> TaskRow {
        serde_json::from_value(serde_json::json!({
            "run_id": "run_01",
            "task_key": "analytics.daily_sales",
            "state": "READY",
            "attempt": 1,
            "deps_total": 0,
            "deps_satisfied_count": 0,
            "max_attempts": 3,
            "heartbeat_timeout_sec": heartbeat_timeout_sec,
            "asset_key": "analytics.daily_sales",
            "partition_key": partition_key,
            "requires_visible_output": true,
            "row_version": "01HQ123EVT"
        }))
        .expect("task row fixture")
    }

    fn state_with(task: TaskRow) -> FoldState {
        let mut state = FoldState::default();
        state
            .tasks
            .insert((task.run_id.clone(), task.task_key.clone()), task);
        state
    }

    #[test]
    fn carries_partition_and_heartbeat_scope() {
        let state = state_with(task_row(Some("date=d:2026-01-01"), 600));

        let payload = engine_payload_for_task(&state, "run_01", "analytics.daily_sales")
            .expect("planned task payload");

        assert_eq!(payload.partition_key.as_deref(), Some("date=d:2026-01-01"));
        assert_eq!(payload.heartbeat_timeout_sec, Some(600));
    }

    #[test]
    fn omits_partition_for_unpartitioned_task() {
        let state = state_with(task_row(None, 300));

        let payload = engine_payload_for_task(&state, "run_01", "analytics.daily_sales")
            .expect("planned task payload");

        assert_eq!(payload.partition_key, None);
        assert_eq!(payload.heartbeat_timeout_sec, Some(300));
    }

    #[test]
    fn missing_task_row_fails_closed() {
        assert!(
            engine_payload_for_task(&FoldState::default(), "run_01", "analytics.daily_sales")
                .is_none()
        );
    }
}
