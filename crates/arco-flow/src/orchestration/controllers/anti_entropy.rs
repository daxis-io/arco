//! Anti-entropy sweeper for detecting and repairing stuck work.
//!
//! The anti-entropy sweeper scans for work that may have been lost due to:
//!
//! - Pub/Sub message loss (dispatch never created)
//! - Cloud Tasks failures (dispatch created but never started)
//! - Worker crashes (task started but never finished)
//!
//! ## Repair Actions
//!
//! The sweeper emits repair actions that are then executed by the dispatcher:
//!
//! - **`CreateDispatchOutbox`**: Task is READY but has no outbox entry
//! - **`RedispatchStuckTask`**: Task is DISPATCHED but hasn't started for too long
//!
//! ## Watermark Freshness Guard
//!
//! Like all controllers, the sweeper checks watermark freshness before
//! creating repair actions. This prevents false positives when compaction
//! is behind and the Parquet state doesn't reflect recent events.

use chrono::{DateTime, Duration, Utc};
use metrics::{counter, histogram};
use std::collections::HashSet;

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::{
    DispatchOutboxRow, DispatchStatus, TaskRow, TaskState,
};
use crate::orchestration::compactor::manifest::Watermarks;

/// Repair action emitted by the anti-entropy sweeper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Repair {
    /// Create a dispatch outbox entry for a stuck READY task.
    CreateDispatchOutbox {
        /// Run ID.
        run_id: String,
        /// Task key.
        task_key: String,
        /// Attempt number.
        attempt: u32,
        /// Reason the repair was triggered.
        reason: String,
    },
    /// Redispatch a stuck DISPATCHED task.
    RedispatchStuckTask {
        /// Run ID.
        run_id: String,
        /// Task key.
        task_key: String,
        /// Current attempt number.
        attempt: u32,
        /// Original dispatch ID.
        original_dispatch_id: String,
        /// Reason the repair was triggered.
        reason: String,
    },
    /// Skip repair due to compaction lag.
    SkippedDueToLag {
        /// Run ID.
        run_id: String,
        /// Task key.
        task_key: String,
        /// Current lag.
        compaction_lag_secs: i64,
    },
}

/// Anti-entropy sweeper for detecting stuck work.
///
/// The sweeper scans tasks and dispatch outbox to find work that may have
/// been lost. It uses timeouts to determine when work should be repaired.
pub struct AntiEntropySweeper {
    /// How long a READY task can be without dispatch before repair.
    ready_timeout: Duration,
    /// How long a DISPATCHED task can wait before starting.
    dispatch_timeout: Duration,
    /// Maximum compaction lag before skipping repairs.
    max_compaction_lag: Duration,
}

impl AntiEntropySweeper {
    /// Creates a new anti-entropy sweeper.
    #[must_use]
    pub fn new(
        ready_timeout: Duration,
        dispatch_timeout: Duration,
        max_compaction_lag: Duration,
    ) -> Self {
        Self {
            ready_timeout,
            dispatch_timeout,
            max_compaction_lag,
        }
    }

    /// Creates a sweeper with default settings.
    ///
    /// Default timeouts:
    /// - READY timeout: 5 minutes
    /// - DISPATCHED timeout: 10 minutes
    /// - Max compaction lag: 30 seconds
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(
            Duration::minutes(5),
            Duration::minutes(10),
            Duration::seconds(30),
        )
    }

    /// Scans for stuck work and returns repair actions.
    ///
    /// # Arguments
    ///
    /// * `watermarks` - Current compaction watermarks
    /// * `tasks` - Task rows from Parquet (base + L0 deltas merged)
    /// * `outbox` - Dispatch outbox rows from Parquet
    /// * `now` - Current timestamp
    ///
    /// # Returns
    ///
    /// A list of repair actions to execute.
    #[must_use]
    pub fn scan(
        &self,
        watermarks: &Watermarks,
        tasks: &[TaskRow],
        outbox: &[DispatchOutboxRow],
        now: DateTime<Utc>,
    ) -> Vec<Repair> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "anti_entropy".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        // Check watermark freshness first
        let compaction_lag = now - watermarks.last_processed_at;
        let repairs = if compaction_lag > self.max_compaction_lag {
            // Skip all repairs when compaction is lagging
            tasks
                .iter()
                .filter(|t| self.needs_repair(t, now))
                .map(|t| Repair::SkippedDueToLag {
                    run_id: t.run_id.clone(),
                    task_key: t.task_key.clone(),
                    compaction_lag_secs: compaction_lag.num_seconds(),
                })
                .collect()
        } else {
            // Build set of tasks with active dispatches
            let dispatched_tasks: HashSet<(String, String, u32)> = outbox
                .iter()
                .filter(|o| {
                    matches!(
                        o.status,
                        DispatchStatus::Pending | DispatchStatus::Created | DispatchStatus::Acked
                    )
                })
                .map(|o| (o.run_id.clone(), o.task_key.clone(), o.attempt))
                .collect();

            let mut repairs = Vec::new();

            for task in tasks {
                if let Some(repair) = self.check_task(task, &dispatched_tasks, outbox, now) {
                    repairs.push(repair);
                }
            }

            repairs
        };

        let count = u64::try_from(repairs.len()).unwrap_or(0);
        counter!(
            metrics_names::ORCH_CONTROLLER_ACTIONS_TOTAL,
            metrics_labels::CONTROLLER => "anti_entropy".to_string(),
        )
        .increment(count);

        repairs
    }

    /// Checks if a task might need repair (before watermark check).
    fn needs_repair(&self, task: &TaskRow, now: DateTime<Utc>) -> bool {
        match task.state {
            TaskState::Ready => task
                .ready_at
                .is_some_and(|ready_at| now - ready_at > self.ready_timeout),
            TaskState::Dispatched => true, // Will check dispatch age
            _ => false,
        }
    }

    /// Checks a single task for repair needs.
    fn check_task(
        &self,
        task: &TaskRow,
        dispatched_tasks: &HashSet<(String, String, u32)>,
        outbox: &[DispatchOutboxRow],
        now: DateTime<Utc>,
    ) -> Option<Repair> {
        match task.state {
            TaskState::Ready => self.check_ready_task(task, dispatched_tasks, now),
            TaskState::Dispatched => self.check_dispatched_task(task, outbox, now),
            _ => None,
        }
    }

    /// Checks a READY task for missing dispatch.
    fn check_ready_task(
        &self,
        task: &TaskRow,
        dispatched_tasks: &HashSet<(String, String, u32)>,
        now: DateTime<Utc>,
    ) -> Option<Repair> {
        // Check if task has been READY for too long
        let ready_at = task.ready_at?;
        if now - ready_at <= self.ready_timeout {
            return None;
        }

        // Check if there's a pending/created dispatch for this task
        // Note: attempt for READY task is 0, but dispatch uses attempt 1
        let next_attempt = task.attempt.max(1);
        let key = (task.run_id.clone(), task.task_key.clone(), next_attempt);
        if dispatched_tasks.contains(&key) {
            return None;
        }

        // Task is stuck READY with no dispatch - create one
        Some(Repair::CreateDispatchOutbox {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            attempt: next_attempt,
            reason: format!("stuck_ready_{}s", (now - ready_at).num_seconds()),
        })
    }

    /// Checks a DISPATCHED task for stuck dispatch.
    fn check_dispatched_task(
        &self,
        task: &TaskRow,
        outbox: &[DispatchOutboxRow],
        now: DateTime<Utc>,
    ) -> Option<Repair> {
        // Find the dispatch for this task
        let dispatch = outbox.iter().find(|o| {
            o.run_id == task.run_id && o.task_key == task.task_key && o.attempt == task.attempt
        });

        let Some(dispatch) = dispatch else {
            return Some(Repair::CreateDispatchOutbox {
                run_id: task.run_id.clone(),
                task_key: task.task_key.clone(),
                attempt: task.attempt,
                reason: "missing_dispatch_outbox".to_string(),
            });
        };

        if dispatch.status == DispatchStatus::Failed {
            return Some(Repair::RedispatchStuckTask {
                run_id: task.run_id.clone(),
                task_key: task.task_key.clone(),
                attempt: task.attempt,
                original_dispatch_id: dispatch.dispatch_id.clone(),
                reason: "dispatch_failed".to_string(),
            });
        }

        // Check if dispatch has been pending/created for too long
        if now - dispatch.created_at <= self.dispatch_timeout {
            return None;
        }

        // Dispatch is stuck - worker never picked it up
        Some(Repair::RedispatchStuckTask {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            attempt: task.attempt,
            original_dispatch_id: dispatch.dispatch_id.clone(),
            reason: format!(
                "stuck_dispatched_{}s",
                (now - dispatch.created_at).num_seconds()
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::orchestration_event_path;

    fn make_task_row(task_key: &str, state: TaskState, ready_at: Option<DateTime<Utc>>) -> TaskRow {
        TaskRow {
            run_id: "run1".to_string(),
            task_key: task_key.to_string(),
            state,
            attempt: if state == TaskState::Ready { 0 } else { 1 },
            attempt_id: Some("01HQ123ATT".to_string()),
            started_at: None,
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 60,
            last_heartbeat_at: None,
            ready_at,
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
        task_key: &str,
        attempt: u32,
        status: DispatchStatus,
        created_at: DateTime<Utc>,
    ) -> DispatchOutboxRow {
        DispatchOutboxRow {
            run_id: "run1".to_string(),
            task_key: task_key.to_string(),
            attempt,
            dispatch_id: format!("dispatch:run1:{}:{}", task_key, attempt),
            cloud_task_id: Some(format!("d_abc{}", attempt)),
            status,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at,
            row_version: "01HQ123EVT".to_string(),
        }
    }

    fn fresh_watermarks(now: DateTime<Utc>) -> Watermarks {
        Watermarks {
            events_processed_through: Some("01HQ123EVT".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "01HQ123")),
            last_processed_at: now - Duration::seconds(5),
        }
    }

    fn stale_watermarks(now: DateTime<Utc>) -> Watermarks {
        Watermarks {
            events_processed_through: Some("01HQ123EVT".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "01HQ123")),
            last_processed_at: now - Duration::seconds(60),
        }
    }

    #[test]
    fn test_anti_entropy_recovers_stuck_ready_tasks() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let tasks = vec![make_task_row(
            "extract",
            TaskState::Ready,
            Some(now - Duration::minutes(10)), // Stuck for 10 min
        )];

        let outbox = vec![]; // No dispatch outbox entry

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::CreateDispatchOutbox {
                task_key,
                attempt,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 1);
                assert!(reason.contains("stuck_ready"));
            }
            _ => panic!("Expected CreateDispatchOutbox repair"),
        }
    }

    #[test]
    fn test_anti_entropy_skips_recently_ready_tasks() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let tasks = vec![make_task_row(
            "extract",
            TaskState::Ready,
            Some(now - Duration::minutes(2)), // Only 2 min, not stuck yet
        )];

        let outbox = vec![];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        // Should not create repair - task hasn't been READY long enough
        assert!(repairs.is_empty());
    }

    #[test]
    fn test_anti_entropy_skips_tasks_with_pending_dispatch() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let tasks = vec![make_task_row(
            "extract",
            TaskState::Ready,
            Some(now - Duration::minutes(10)),
        )];

        let outbox = vec![make_outbox_row(
            "extract",
            1,
            DispatchStatus::Pending,
            now - Duration::minutes(5),
        )];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        // Should not create repair - there's already a pending dispatch
        assert!(repairs.is_empty());
    }

    #[test]
    fn test_anti_entropy_skips_tasks_with_acked_dispatch() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let tasks = vec![make_task_row(
            "extract",
            TaskState::Ready,
            Some(now - Duration::minutes(10)),
        )];

        let outbox = vec![make_outbox_row(
            "extract",
            1,
            DispatchStatus::Acked,
            now - Duration::minutes(5),
        )];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        // Should not create repair - there's already an acked dispatch
        assert!(repairs.is_empty());
    }

    #[test]
    fn test_anti_entropy_recovers_stuck_dispatched_tasks() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::Dispatched, None);
        task.attempt = 1;

        let tasks = vec![task];

        let outbox = vec![make_outbox_row(
            "extract",
            1,
            DispatchStatus::Created,
            now - Duration::minutes(15), // Stuck for 15 min
        )];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::RedispatchStuckTask {
                task_key,
                attempt,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 1);
                assert!(reason.contains("stuck_dispatched"));
            }
            _ => panic!("Expected RedispatchStuckTask repair"),
        }
    }

    #[test]
    fn test_anti_entropy_repairs_dispatched_without_outbox() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::Dispatched, None);
        task.attempt = 1;

        let tasks = vec![task];
        let outbox = vec![];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::CreateDispatchOutbox {
                task_key,
                attempt,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 1);
                assert_eq!(reason, "missing_dispatch_outbox");
            }
            _ => panic!("Expected CreateDispatchOutbox repair"),
        }
    }

    #[test]
    fn test_anti_entropy_redispatches_failed_outbox() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::Dispatched, None);
        task.attempt = 1;

        let tasks = vec![task];
        let outbox = vec![make_outbox_row(
            "extract",
            1,
            DispatchStatus::Failed,
            now - Duration::seconds(5),
        )];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::RedispatchStuckTask {
                task_key,
                attempt,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 1);
                assert_eq!(reason, "dispatch_failed");
            }
            _ => panic!("Expected RedispatchStuckTask repair"),
        }
    }

    #[test]
    fn test_anti_entropy_guards_against_compaction_lag() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = stale_watermarks(now);

        let tasks = vec![make_task_row(
            "extract",
            TaskState::Ready,
            Some(now - Duration::minutes(10)),
        )];

        let outbox = vec![];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        // Should skip repairs due to compaction lag
        assert_eq!(repairs.len(), 1);
        assert!(matches!(repairs[0], Repair::SkippedDueToLag { .. }));
    }

    #[test]
    fn test_anti_entropy_ignores_terminal_tasks() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let tasks = vec![
            make_task_row("extract", TaskState::Succeeded, None),
            make_task_row("transform", TaskState::Failed, None),
            make_task_row("load", TaskState::Skipped, None),
        ];

        let outbox = vec![];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        // Should not create repairs for terminal tasks
        assert!(repairs.is_empty());
    }

    #[test]
    fn test_anti_entropy_ignores_running_tasks() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let tasks = vec![make_task_row("extract", TaskState::Running, None)];

        let outbox = vec![];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        // Running tasks are handled by heartbeat, not anti-entropy
        assert!(repairs.is_empty());
    }
}
