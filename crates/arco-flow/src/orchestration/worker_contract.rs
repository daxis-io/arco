//! Compatibility re-exports for the published worker dispatch contract, plus
//! the canonical construction path from planned orchestration state.

pub use arco_worker_contract::{WorkerDispatchEnvelope, callback_task_id, parse_callback_task_id};

use chrono::{DateTime, Utc};

use crate::orchestration::compactor::fold::TaskRow;

/// Inputs for building a [`WorkerDispatchEnvelope`] for one task attempt.
///
/// Dispatch producers (the dispatcher and the anti-entropy sweeper) must
/// build envelopes through [`dispatch_envelope_for_attempt`] so that
/// execution-scope fields sourced from the planned task row — the partition
/// key the catalog will record as materialized (issue #339) and the
/// heartbeat timeout the worker must beat (issue #367) — cannot silently be
/// dropped at any single construction site.
#[derive(Debug, Clone)]
pub struct DispatchEnvelopeSpec {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Semantic task key.
    pub task_key: String,
    /// Attempt number, starting at one.
    pub attempt: u32,
    /// Attempt identifier used for stale-worker protection.
    pub attempt_id: String,
    /// Dispatch identifier.
    pub dispatch_id: String,
    /// Target worker queue.
    pub worker_queue: String,
    /// Base URL workers use for task callbacks.
    pub callback_base_url: String,
    /// Per-task callback bearer token.
    pub task_token: String,
    /// Token expiry timestamp.
    pub token_expires_at: DateTime<Utc>,
}

/// Builds the canonical dispatch envelope for a planned task attempt.
///
/// `task` is the planned task row for this run/task key. When present, its
/// `partition_key` is copied verbatim into the envelope so the worker
/// executes exactly the partition identity the catalog records as
/// materialized, and its `heartbeat_timeout_sec` tells the worker how often
/// it must heartbeat before anti-entropy force-fails the attempt. A missing
/// row (repair paths racing state pruning) degrades to an envelope without
/// execution scope, matching the legacy wire shape.
#[must_use]
pub fn dispatch_envelope_for_attempt(
    spec: DispatchEnvelopeSpec,
    task: Option<&TaskRow>,
) -> WorkerDispatchEnvelope {
    WorkerDispatchEnvelope {
        tenant_id: spec.tenant_id,
        workspace_id: spec.workspace_id,
        task_id: callback_task_id(&spec.run_id, &spec.task_key),
        task_key: spec.task_key,
        run_id: spec.run_id,
        attempt: spec.attempt,
        attempt_id: spec.attempt_id,
        dispatch_id: spec.dispatch_id,
        execution_location_id: None,
        partition_key: task.and_then(|row| row.partition_key.clone()),
        heartbeat_timeout_sec: task.map(|row| row.heartbeat_timeout_sec),
        worker_queue: spec.worker_queue,
        callback_base_url: spec.callback_base_url,
        task_token: spec.task_token,
        token_expires_at: spec.token_expires_at,
        traceparent: None,
        payload: serde_json::Value::Object(serde_json::Map::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::compactor::fold::TaskState;

    fn spec() -> DispatchEnvelopeSpec {
        DispatchEnvelopeSpec {
            tenant_id: "tenant-a".to_string(),
            workspace_id: "workspace-b".to_string(),
            run_id: "run-123".to_string(),
            task_key: "analytics.daily_sales".to_string(),
            attempt: 1,
            attempt_id: "att-123".to_string(),
            dispatch_id: "dispatch:run-123:analytics.daily_sales:1".to_string(),
            worker_queue: "default-queue".to_string(),
            callback_base_url: "https://callbacks.example".to_string(),
            task_token: "token".to_string(),
            token_expires_at: Utc::now(),
        }
    }

    fn task_row(partition_key: Option<&str>) -> TaskRow {
        TaskRow {
            run_id: "run-123".to_string(),
            task_key: "analytics.daily_sales".to_string(),
            state: TaskState::Ready,
            attempt: 0,
            attempt_id: None,
            started_at: None,
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: None,
            ready_at: None,
            asset_key: Some("analytics.daily_sales".to_string()),
            partition_key: partition_key.map(ToString::to_string),
            requires_visible_output: false,
            materialization_id: None,
            output_visibility_state: None,
            published_at: None,
            publish_error: None,
            retry_not_before: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            execution_lineage_ref: None,
            row_version: "01HQ123EVT".to_string(),
        }
    }

    #[test]
    fn envelope_carries_planned_partition_key_and_heartbeat_timeout() {
        let row = task_row(Some("date=d:2026-01-01"));

        let envelope = dispatch_envelope_for_attempt(spec(), Some(&row));

        assert_eq!(envelope.partition_key.as_deref(), Some("date=d:2026-01-01"));
        assert_eq!(envelope.heartbeat_timeout_sec, Some(300));
        assert_eq!(envelope.task_key, "analytics.daily_sales");
        assert_eq!(
            envelope.task_id,
            callback_task_id("run-123", "analytics.daily_sales")
        );
    }

    #[test]
    fn envelope_omits_partition_key_for_unpartitioned_tasks() {
        let row = task_row(None);

        let envelope = dispatch_envelope_for_attempt(spec(), Some(&row));

        assert_eq!(envelope.partition_key, None);
        assert_eq!(envelope.heartbeat_timeout_sec, Some(300));
    }

    #[test]
    fn envelope_degrades_to_legacy_shape_without_task_row() {
        let envelope = dispatch_envelope_for_attempt(spec(), None);

        assert_eq!(envelope.partition_key, None);
        assert_eq!(envelope.heartbeat_timeout_sec, None);
    }
}
