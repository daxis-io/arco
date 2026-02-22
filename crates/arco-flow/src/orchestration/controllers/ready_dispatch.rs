//! Ready dispatch controller for initiating task dispatch.
//!
//! This controller reads tasks in READY state from Parquet projections and
//! emits `DispatchRequested` events to initiate the dispatch flow.
//!
//! ## Event Flow (ADR-020)
//!
//! ```text
//! PlanCreated → Compactor → TaskRow (Ready) → ReadyDispatchController
//!                                                    ↓
//!                                            DispatchRequested
//!                                                    ↓
//!                                            Compactor → dispatch_outbox (Pending)
//!                                                    ↓
//!                                            DispatcherController → Cloud Tasks
//!                                                    ↓
//!                                            DispatchEnqueued
//! ```
//!
//! ## Design Principles
//!
//! 1. **Stateless**: Can be scaled horizontally
//! 2. **Idempotent**: Same input produces same output (deterministic `dispatch_id`)
//! 3. **Parquet-only reads**: Never reads from the ledger
//! 4. **Watermark guard**: Skips when compaction is lagging

use chrono::Duration;
use metrics::{counter, histogram};

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::{
    DispatchOutboxRow, DispatchStatus, FoldState, TaskRow, TaskState,
};
use crate::orchestration::compactor::manifest::OrchestrationManifest;
use crate::orchestration::events::OrchestrationEventData;
use crate::orchestration::ids::deterministic_attempt_id;

/// Action returned by the ready dispatch reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadyDispatchAction {
    /// Emit a `DispatchRequested` event for this task.
    EmitDispatchRequested {
        /// Run identifier.
        run_id: String,
        /// Task key within run.
        task_key: String,
        /// Attempt number (1-indexed).
        attempt: u32,
        /// New attempt ID.
        attempt_id: String,
        /// Target worker queue.
        worker_queue: String,
        /// Internal dispatch ID.
        dispatch_id: String,
    },
    /// Skip this task (already has pending dispatch or wrong state).
    Skip {
        /// Run identifier.
        run_id: String,
        /// Task key.
        task_key: String,
        /// Reason for skipping.
        reason: String,
    },
}

impl ReadyDispatchAction {
    /// Converts the action to an `OrchestrationEventData` if it's a dispatch request.
    #[must_use]
    pub fn into_event_data(self) -> Option<OrchestrationEventData> {
        match self {
            Self::EmitDispatchRequested {
                run_id,
                task_key,
                attempt,
                attempt_id,
                worker_queue,
                dispatch_id,
            } => Some(OrchestrationEventData::DispatchRequested {
                run_id,
                task_key,
                attempt,
                attempt_id,
                worker_queue,
                dispatch_id,
            }),
            Self::Skip { .. } => None,
        }
    }
}

/// Ready dispatch controller for reconciling READY tasks to `DispatchRequested` events.
///
/// The controller follows the standard pattern:
/// 1. Read task state from Parquet (base + L0 deltas merged)
/// 2. For each READY task without a pending dispatch, emit `DispatchRequested`
/// 3. Events are written to ledger and compacted into `dispatch_outbox`
pub struct ReadyDispatchController {
    /// Maximum compaction lag before skipping dispatch.
    max_compaction_lag: Duration,
    /// Default worker queue for dispatch requests.
    worker_queue: String,
}

impl ReadyDispatchController {
    /// Creates a new ready dispatch controller.
    #[must_use]
    pub fn new(max_compaction_lag: Duration) -> Self {
        Self {
            max_compaction_lag,
            worker_queue: "default-queue".to_string(),
        }
    }

    /// Creates a controller with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(Duration::seconds(30))
    }

    /// Sets the default worker queue for dispatch requests.
    #[must_use]
    pub fn with_worker_queue(mut self, worker_queue: impl Into<String>) -> Self {
        self.worker_queue = worker_queue.into();
        self
    }

    /// Reconciles the task state and returns dispatch actions.
    ///
    /// # Arguments
    ///
    /// * `manifest` - The current orchestration manifest
    /// * `state` - Current fold state loaded from Parquet (base + L0 deltas merged)
    ///
    /// # Returns
    ///
    /// A list of dispatch actions to execute.
    #[must_use]
    pub fn reconcile(
        &self,
        manifest: &OrchestrationManifest,
        state: &FoldState,
    ) -> Vec<ReadyDispatchAction> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "ready_dispatch".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        // Check watermark freshness
        let actions: Vec<ReadyDispatchAction> =
            if manifest.watermarks.is_fresh(self.max_compaction_lag) {
                state
                    .tasks
                    .values()
                    .filter_map(|task| self.reconcile_task(task, state))
                    .collect()
            } else {
                state
                    .tasks
                    .values()
                    .filter(|task| task.state == TaskState::Ready)
                    .map(|task| ReadyDispatchAction::Skip {
                        run_id: task.run_id.clone(),
                        task_key: task.task_key.clone(),
                        reason: "compaction_lag".to_string(),
                    })
                    .collect()
            };

        let count = u64::try_from(actions.len()).unwrap_or(0);
        counter!(
            metrics_names::ORCH_CONTROLLER_ACTIONS_TOTAL,
            metrics_labels::CONTROLLER => "ready_dispatch".to_string(),
        )
        .increment(count);

        actions
    }

    /// Reconciles a single task.
    fn reconcile_task(&self, task: &TaskRow, state: &FoldState) -> Option<ReadyDispatchAction> {
        // Only process READY tasks
        if task.state != TaskState::Ready {
            return None;
        }

        if let Some(run) = state.runs.get(&task.run_id) {
            if run.cancel_requested {
                return Some(ReadyDispatchAction::Skip {
                    run_id: task.run_id.clone(),
                    task_key: task.task_key.clone(),
                    reason: "run_cancel_requested".to_string(),
                });
            }
        }

        // Determine the attempt number for dispatch
        // READY tasks start at attempt 0 (from PlanCreated), first dispatch is attempt 1
        let attempt = task.attempt.max(1);

        // Check if there's already a dispatch for this task/attempt
        let dispatch_id = DispatchOutboxRow::dispatch_id(&task.run_id, &task.task_key, attempt);

        if let Some(existing) = state.dispatch_outbox.get(&dispatch_id) {
            // Check the status of existing dispatch
            match existing.status {
                DispatchStatus::Pending | DispatchStatus::Created | DispatchStatus::Acked => {
                    // Already has an active dispatch - skip
                    return Some(ReadyDispatchAction::Skip {
                        run_id: task.run_id.clone(),
                        task_key: task.task_key.clone(),
                        reason: format!("existing_dispatch:{:?}", existing.status),
                    });
                }
                DispatchStatus::Failed => {
                    // Previous dispatch failed - will be handled by anti-entropy sweeper
                    return Some(ReadyDispatchAction::Skip {
                        run_id: task.run_id.clone(),
                        task_key: task.task_key.clone(),
                        reason: "dispatch_failed_anti_entropy".to_string(),
                    });
                }
            }
        }

        // Generate deterministic attempt_id from dispatch_id for controller idempotency
        let attempt_id = deterministic_attempt_id(&dispatch_id);

        Some(ReadyDispatchAction::EmitDispatchRequested {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            attempt,
            attempt_id,
            worker_queue: self.worker_queue.clone(),
            dispatch_id,
        })
    }

    /// Generates the idempotency key for a `DispatchRequested` event.
    #[must_use]
    pub fn dispatch_requested_idempotency_key(dispatch_id: &str) -> String {
        format!("dispatch_req:{dispatch_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::compactor::fold::{RunRow, RunState};
    use chrono::Utc;

    fn make_task_row(run_id: &str, task_key: &str, state: TaskState, attempt: u32) -> TaskRow {
        TaskRow {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            state,
            attempt,
            attempt_id: None,
            started_at: None,
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: None,
            ready_at: Some(Utc::now()),
            asset_key: None,
            partition_key: None,
            materialization_id: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            execution_lineage_ref: None,
            row_version: "01HQ123EVT".to_string(),
        }
    }

    fn make_outbox_row(
        run_id: &str,
        task_key: &str,
        attempt: u32,
        status: DispatchStatus,
    ) -> DispatchOutboxRow {
        let dispatch_id = DispatchOutboxRow::dispatch_id(run_id, task_key, attempt);
        DispatchOutboxRow {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            dispatch_id,
            cloud_task_id: None,
            status,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at: Utc::now(),
            row_version: "01HQ123EVT".to_string(),
        }
    }

    fn make_run_row(run_id: &str, cancel_requested: bool) -> RunRow {
        RunRow {
            run_id: run_id.to_string(),
            plan_id: "plan_01".to_string(),
            state: RunState::Running,
            run_key: None,
            labels: std::collections::HashMap::new(),
            code_version: None,
            cancel_requested,
            tasks_total: 1,
            tasks_completed: 0,
            tasks_succeeded: 0,
            tasks_failed: 0,
            tasks_skipped: 0,
            tasks_cancelled: 0,
            triggered_at: Utc::now(),
            completed_at: None,
            row_version: "01HQ123EVT".to_string(),
        }
    }

    fn fresh_manifest() -> OrchestrationManifest {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.watermarks.last_processed_at = Utc::now() - Duration::seconds(5);
        manifest
    }

    fn stale_manifest() -> OrchestrationManifest {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.watermarks.last_processed_at = Utc::now() - Duration::seconds(60);
        manifest
    }

    #[test]
    fn test_ready_dispatch_emits_for_ready_tasks() {
        let controller =
            ReadyDispatchController::with_defaults().with_worker_queue("priority-queue");
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );

        let actions = controller.reconcile(&manifest, &state);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ReadyDispatchAction::EmitDispatchRequested {
                run_id,
                task_key,
                attempt,
                worker_queue,
                dispatch_id,
                ..
            } => {
                assert_eq!(run_id, "run1");
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 1);
                assert_eq!(worker_queue, "priority-queue");
                assert_eq!(dispatch_id, "dispatch:run1:extract:1");
            }
            _ => panic!("Expected EmitDispatchRequested action"),
        }
    }

    #[test]
    fn test_ready_dispatch_skips_non_ready_tasks() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "blocked".to_string()),
            make_task_row("run1", "blocked", TaskState::Blocked, 0),
        );
        state.tasks.insert(
            ("run1".to_string(), "running".to_string()),
            make_task_row("run1", "running", TaskState::Running, 1),
        );
        state.tasks.insert(
            ("run1".to_string(), "succeeded".to_string()),
            make_task_row("run1", "succeeded", TaskState::Succeeded, 1),
        );

        let actions = controller.reconcile(&manifest, &state);

        // No actions for non-READY tasks
        assert!(actions.is_empty());
    }

    #[test]
    fn test_ready_dispatch_skips_tasks_with_pending_dispatch() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );

        // Add pending dispatch
        let outbox = make_outbox_row("run1", "extract", 1, DispatchStatus::Pending);
        state
            .dispatch_outbox
            .insert(outbox.dispatch_id.clone(), outbox);

        let actions = controller.reconcile(&manifest, &state);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ReadyDispatchAction::Skip { reason, .. } => {
                assert!(reason.contains("existing_dispatch"));
            }
            _ => panic!("Expected Skip action"),
        }
    }

    #[test]
    fn test_ready_dispatch_skips_tasks_with_created_dispatch() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );

        let outbox = make_outbox_row("run1", "extract", 1, DispatchStatus::Created);
        state
            .dispatch_outbox
            .insert(outbox.dispatch_id.clone(), outbox);

        let actions = controller.reconcile(&manifest, &state);

        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], ReadyDispatchAction::Skip { reason, .. } if reason.contains("Created"))
        );
    }

    #[test]
    fn test_ready_dispatch_guards_against_compaction_lag() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = stale_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );

        let actions = controller.reconcile(&manifest, &state);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ReadyDispatchAction::Skip { reason, .. } => {
                assert_eq!(reason, "compaction_lag");
            }
            _ => panic!("Expected Skip action due to compaction lag"),
        }
    }

    #[test]
    fn test_ready_dispatch_skips_cancel_requested_runs() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state
            .runs
            .insert("run1".to_string(), make_run_row("run1", true));
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );

        let actions = controller.reconcile(&manifest, &state);

        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            ReadyDispatchAction::Skip { reason, .. } if reason == "run_cancel_requested"
        ));
    }

    #[test]
    fn test_ready_dispatch_generates_deterministic_attempt_ids() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );

        let actions1 = controller.reconcile(&manifest, &state);
        let actions2 = controller.reconcile(&manifest, &state);

        // Both should emit dispatch with the SAME attempt_id (deterministic for idempotency)
        match (&actions1[0], &actions2[0]) {
            (
                ReadyDispatchAction::EmitDispatchRequested {
                    attempt_id: id1,
                    dispatch_id: d1,
                    ..
                },
                ReadyDispatchAction::EmitDispatchRequested {
                    attempt_id: id2,
                    dispatch_id: d2,
                    ..
                },
            ) => {
                assert_eq!(d1, d2, "Same dispatch_id for same task/attempt");
                assert_eq!(
                    id1, id2,
                    "Deterministic attempt_id for controller idempotency"
                );
            }
            _ => panic!("Expected EmitDispatchRequested actions"),
        }
    }

    #[test]
    fn test_ready_dispatch_different_tasks_get_different_attempt_ids() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "task_a".to_string()),
            make_task_row("run1", "task_a", TaskState::Ready, 0),
        );
        state.tasks.insert(
            ("run1".to_string(), "task_b".to_string()),
            make_task_row("run1", "task_b", TaskState::Ready, 0),
        );

        let actions = controller.reconcile(&manifest, &state);
        let attempt_ids: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                ReadyDispatchAction::EmitDispatchRequested { attempt_id, .. } => Some(attempt_id),
                _ => None,
            })
            .collect();

        assert_eq!(attempt_ids.len(), 2);
        assert_ne!(
            attempt_ids[0], attempt_ids[1],
            "Different tasks should have different attempt_ids"
        );
    }

    #[test]
    fn test_ready_dispatch_handles_multiple_ready_tasks() {
        let controller = ReadyDispatchController::with_defaults();
        let manifest = fresh_manifest();

        let mut state = FoldState::new();
        state.tasks.insert(
            ("run1".to_string(), "extract".to_string()),
            make_task_row("run1", "extract", TaskState::Ready, 0),
        );
        state.tasks.insert(
            ("run1".to_string(), "transform".to_string()),
            make_task_row("run1", "transform", TaskState::Ready, 0),
        );
        state.tasks.insert(
            ("run1".to_string(), "load".to_string()),
            make_task_row("run1", "load", TaskState::Blocked, 0),
        );

        let actions = controller.reconcile(&manifest, &state);

        // Should emit for 2 READY tasks, skip BLOCKED
        let emit_count = actions
            .iter()
            .filter(|a| matches!(a, ReadyDispatchAction::EmitDispatchRequested { .. }))
            .count();
        assert_eq!(emit_count, 2);
    }

    #[test]
    fn test_action_converts_to_event_data() {
        let action = ReadyDispatchAction::EmitDispatchRequested {
            run_id: "run1".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "priority-queue".to_string(),
            dispatch_id: "dispatch:run1:extract:1".to_string(),
        };

        let event_data = action.into_event_data();
        assert!(event_data.is_some());

        match event_data.unwrap() {
            OrchestrationEventData::DispatchRequested {
                run_id,
                task_key,
                attempt,
                worker_queue,
                ..
            } => {
                assert_eq!(run_id, "run1");
                assert_eq!(task_key, "extract");
                assert_eq!(attempt, 1);
                assert_eq!(worker_queue, "priority-queue");
            }
            _ => panic!("Expected DispatchRequested event data"),
        }
    }

    #[test]
    fn test_skip_action_does_not_convert_to_event() {
        let action = ReadyDispatchAction::Skip {
            run_id: "run1".to_string(),
            task_key: "extract".to_string(),
            reason: "test".to_string(),
        };

        assert!(action.into_event_data().is_none());
    }

    #[test]
    fn test_dispatch_requested_idempotency_key() {
        let key =
            ReadyDispatchController::dispatch_requested_idempotency_key("dispatch:run1:extract:1");
        assert_eq!(key, "dispatch_req:dispatch:run1:extract:1");
    }
}
