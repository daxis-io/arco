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
//! - **`CreateDispatchOutbox`**: Task is in `RETRY_WAIT` past its retry deadline
//! - **`RedispatchStuckTask`**: Task is DISPATCHED but hasn't started for too long
//! - **`FailStaleRunningTask`**: Task is RUNNING past heartbeat timeout plus grace
//!
//! ## Watermark Freshness Guard
//!
//! Like all controllers, the sweeper checks watermark freshness before
//! creating repair actions. This prevents false positives when compaction
//! is behind and the Parquet state doesn't reflect recent events.
//!
//! The wall-clock watermark (`Watermarks::last_processed_at`) only advances
//! when a new ledger event is compacted, so an idle-but-healthy workspace
//! looks identical to a broken compactor. Callers that have checked the ledger
//! for unprocessed events (see [`LedgerFreshness::Current`]) bypass the
//! wall-clock guard for **non-destructive** repairs, which keeps dispatch
//! re-drive reachable in an idle workspace (issue #338): the task-staleness
//! threshold (300s heartbeat timeout + 30s grace = 330s) is larger than the
//! 300s compaction-lag guard, and a dead worker stops the event flow that
//! would keep the watermark fresh.
//!
//! That check compares event ids against the fold watermark; it cannot prove
//! the folded prefix is contiguous, so it is not a warrant for destroying
//! evidence. `FailStaleRunningTask` therefore still requires the wall-clock
//! guard — see [`AntiEntropySweeper::scan_with_ledger_freshness`]. Making the
//! zombie reaper reachable in a genuinely idle workspace needs the compactor
//! to refresh `last_processed_at` on idle ticks so that "idle" stops looking
//! like "lagging"; until then a stale-watermark workspace reports
//! [`Repair::SkippedDueToLag`] for its stale RUNNING tasks instead of
//! force-failing them on unproven evidence.

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
    /// Fail a stale RUNNING task so the fold can retry or terminally fail it.
    FailStaleRunningTask {
        /// Run ID.
        run_id: String,
        /// Task key.
        task_key: String,
        /// Current attempt number.
        attempt: u32,
        /// Current attempt ID.
        attempt_id: String,
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

impl Repair {
    /// Whether executing this repair destroys evidence that cannot be recovered.
    ///
    /// [`Repair::FailStaleRunningTask`] writes a terminal failure over an
    /// attempt that may still be running, or that may already have completed
    /// durably without the projection showing it. Nothing undoes that.
    ///
    /// The dispatch repairs are re-drives: they route work through
    /// deterministic dispatch ids that the control plane and the worker both
    /// deduplicate, so a repair issued on incomplete evidence costs at most a
    /// redundant delivery.
    #[must_use]
    pub const fn is_destructive(&self) -> bool {
        matches!(self, Self::FailStaleRunningTask { .. })
    }
}

/// What a caller has established about unfolded orchestration ledger events.
///
/// The three states are deliberately distinct: "checked and looks current",
/// "checked and found behind", and "not checked". Collapsing the last two lets
/// an unchecked caller inherit the permissions of a checked one, or the
/// reverse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerFreshness {
    /// No ledger event id exceeds the fold watermark, as far as a bounded scan
    /// of the ledger's date prefixes can tell.
    ///
    /// This is a maximum-id comparison, **not** a contiguous-frontier proof: an
    /// event that is durable but unfolded and whose id sits at or below the
    /// watermark is invisible to it. Non-destructive repairs may run on this
    /// evidence; destructive ones may not.
    Current,
    /// The check ran and did **not** find the ledger current: an appended
    /// event is missing from the projections, or the scan could not complete.
    ///
    /// This is positive evidence that the projection is behind, so it blocks
    /// destructive repair outright — the stale RUNNING row the sweeper is
    /// about to overwrite may be contradicted by the very event that has not
    /// been folded.
    Stale,
    /// The caller did not check. Only the wall-clock compaction-lag guard
    /// applies, which is the behaviour that predates the ledger scan.
    Unchecked,
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

const DEFAULT_MAX_COMPACTION_LAG: Duration = Duration::minutes(5);

/// Grace added on top of a task's `heartbeat_timeout_sec` before a RUNNING
/// task is considered stale.
///
/// Constant relationships (issues #338 and #367):
///
/// - The force-fail window for a RUNNING task is
///   `heartbeat_timeout_sec + RUNNING_TASK_STALENESS_GRACE` — 300s + 30s =
///   330s at the planner default
///   (`RunRequestProcessor::default_heartbeat_timeout_sec`).
/// - The Python reference worker heartbeats every
///   `heartbeat_timeout_sec / 5` seconds, clamped to at most 60s
///   (`python/arco/src/arco_flow/worker/server.py`), so several consecutive
///   heartbeats must be lost before the window can expire.
/// - `DEFAULT_MAX_COMPACTION_LAG` (300s) is *smaller* than the 330s force-fail
///   window, so a workspace whose watermark has gone stale reports
///   [`Repair::SkippedDueToLag`] for its stale RUNNING tasks.
///   [`LedgerFreshness::Current`] does not lift that guard for the force-fail,
///   because a maximum-id ledger scan cannot exclude a durable-but-unfolded
///   straggler carrying the task's terminal result.
pub const RUNNING_TASK_STALENESS_GRACE: Duration = Duration::seconds(30);

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
    /// - Max compaction lag: 5 minutes
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(
            Duration::minutes(5),
            Duration::minutes(10),
            DEFAULT_MAX_COMPACTION_LAG,
        )
    }

    /// Scans for stuck work and returns repair actions.
    ///
    /// Equivalent to [`Self::scan_with_ledger_freshness`] with
    /// [`LedgerFreshness::Unchecked`]: the wall-clock compaction-lag guard
    /// applies unconditionally.
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
        self.scan_with_ledger_freshness(watermarks, LedgerFreshness::Unchecked, tasks, outbox, now)
    }

    /// Scans for stuck work with explicit knowledge of ledger freshness.
    ///
    /// When `ledger_freshness` is [`LedgerFreshness::Current`], the caller has
    /// checked that no ledger event id exceeds the fold watermark, so the
    /// wall-clock compaction-lag guard is bypassed for **non-destructive**
    /// repairs. This keeps the dispatch re-drive reachable in an idle
    /// workspace whose only worker died (issue #338): with no event flow the
    /// watermark cannot stay fresh, and a redundant re-dispatch is bounded by
    /// deterministic dispatch ids.
    ///
    /// # Why freshness does not authorize destructive repair
    ///
    /// [`LedgerFreshness::Current`] is a maximum-id comparison, not a proof
    /// that the ledger prefix is contiguous. A straggler event `E1` that is
    /// durable but unfolded, and whose id is at or below the watermark
    /// (because a concurrent writer appended and folded a higher `E2` first),
    /// is invisible to that check. If `E1` carries the terminal completion or
    /// a protective heartbeat for a task the projection still shows as
    /// RUNNING, force-failing that task overwrites durable evidence with a
    /// fabricated failure.
    ///
    /// A re-dispatch made on the same incomplete evidence is recoverable; a
    /// force-fail is not. Destructive repairs therefore still require the
    /// wall-clock guard, and are reported as
    /// [`Repair::SkippedDueToLag`] when only freshness would have allowed
    /// them, so the condition is observable rather than silent.
    #[must_use]
    pub fn scan_with_ledger_freshness(
        &self,
        watermarks: &Watermarks,
        ledger_freshness: LedgerFreshness,
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

        // Check watermark freshness first. A current ledger scan lets
        // non-destructive repairs proceed despite wall-clock staleness;
        // destructive ones still need the wall-clock guard, because the scan
        // cannot prove a lower-id straggler was folded.
        let compaction_lag = now - watermarks.last_processed_at;
        let lag_exceeded = compaction_lag > self.max_compaction_lag;
        let skip_for_lag = ledger_freshness != LedgerFreshness::Current && lag_exceeded;
        // Destructive repair needs the wall-clock guard *and* the absence of
        // positive evidence that the ledger is behind. Freshness can only
        // withhold that permission, never grant it.
        let block_destructive = lag_exceeded || ledger_freshness == LedgerFreshness::Stale;
        let repairs = if skip_for_lag {
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
                .filter(|o| matches!(o.status, DispatchStatus::Pending | DispatchStatus::Created))
                .map(|o| (o.run_id.clone(), o.task_key.clone(), o.attempt))
                .collect();

            let mut repairs = Vec::new();

            for task in tasks {
                if let Some(repair) = self.check_task(task, &dispatched_tasks, outbox, now) {
                    if block_destructive && repair.is_destructive() {
                        repairs.push(Repair::SkippedDueToLag {
                            run_id: task.run_id.clone(),
                            task_key: task.task_key.clone(),
                            compaction_lag_secs: compaction_lag.num_seconds(),
                        });
                        continue;
                    }
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
            TaskState::RetryWait => {
                task.attempt < task.max_attempts
                    && task.retry_not_before.is_none_or(|deadline| now >= deadline)
            }
            TaskState::Running => Self::running_task_is_stale(task, now),
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
            TaskState::RetryWait => Self::check_retry_wait_task(task, dispatched_tasks, now),
            TaskState::Running => Self::check_running_task(task, now),
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

    fn check_retry_wait_task(
        task: &TaskRow,
        dispatched_tasks: &HashSet<(String, String, u32)>,
        now: DateTime<Utc>,
    ) -> Option<Repair> {
        if task.attempt >= task.max_attempts {
            return None;
        }
        // A missing deadline means the task predates the fold-time retry
        // scheduling introduced for issue #337 (nothing else ever set
        // `retry_not_before`). Such a task is already wedged, so bootstrap it
        // immediately; only a deadline still in the future defers the retry.
        if task.retry_not_before.is_some_and(|deadline| now < deadline) {
            return None;
        }
        let next_attempt = task.attempt + 1;
        let key = (task.run_id.clone(), task.task_key.clone(), next_attempt);
        if dispatched_tasks.contains(&key) {
            return None;
        }
        Some(Repair::CreateDispatchOutbox {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            attempt: next_attempt,
            reason: "retry_wait_bootstrap".to_string(),
        })
    }

    fn check_running_task(task: &TaskRow, now: DateTime<Utc>) -> Option<Repair> {
        if !Self::running_task_is_stale(task, now) {
            return None;
        }
        let attempt_id = task.attempt_id.clone()?;
        Some(Repair::FailStaleRunningTask {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            attempt: task.attempt,
            attempt_id,
            reason: "heartbeat_timeout_anti_entropy".to_string(),
        })
    }

    fn running_task_is_stale(task: &TaskRow, now: DateTime<Utc>) -> bool {
        let Some(reference_time) = task.last_heartbeat_at.or(task.started_at) else {
            return false;
        };
        let timeout = Duration::seconds(i64::from(task.heartbeat_timeout_sec));
        now - reference_time > timeout + RUNNING_TASK_STALENESS_GRACE
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
            last_committed_event_id: Some("01HQ123EVT".to_string()),
            last_visible_event_id: Some("01HQ123EVT".to_string()),
            events_processed_through: Some("01HQ123EVT".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "01HQ123")),
            last_processed_at: now - Duration::seconds(5),
        }
    }

    fn stale_watermarks(now: DateTime<Utc>) -> Watermarks {
        Watermarks {
            last_committed_event_id: Some("01HQ123EVT".to_string()),
            last_visible_event_id: Some("01HQ123EVT".to_string()),
            events_processed_through: Some("01HQ123EVT".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "01HQ123")),
            last_processed_at: now - Duration::minutes(10),
        }
    }

    fn scheduler_cadence_watermarks(now: DateTime<Utc>) -> Watermarks {
        Watermarks {
            last_committed_event_id: Some("01HQ123EVT".to_string()),
            last_visible_event_id: Some("01HQ123EVT".to_string()),
            events_processed_through: Some("01HQ123EVT".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "01HQ123")),
            last_processed_at: now - Duration::seconds(75),
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
    fn test_anti_entropy_bootstraps_retry_wait_tasks() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::RetryWait, None);
        task.attempt = 1;
        task.max_attempts = 3;
        task.retry_not_before = Some(now);

        let repairs = sweeper.scan(&watermarks, &[task], &[], now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::CreateDispatchOutbox {
                task_key,
                attempt,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 2);
                assert_eq!(reason, "retry_wait_bootstrap");
            }
            _ => panic!("Expected CreateDispatchOutbox repair"),
        }
    }

    #[test]
    fn test_anti_entropy_does_not_bootstrap_retry_wait_before_deadline() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::RetryWait, None);
        task.attempt = 1;
        task.max_attempts = 3;
        task.retry_not_before = Some(now + Duration::minutes(5));

        let repairs = sweeper.scan(&watermarks, &[task], &[], now);

        assert!(
            repairs.is_empty(),
            "anti-entropy must preserve retry backoff deadlines"
        );
    }

    #[test]
    fn test_anti_entropy_bootstraps_retry_wait_after_deadline() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::RetryWait, None);
        task.attempt = 1;
        task.max_attempts = 3;
        task.retry_not_before = Some(now - Duration::seconds(1));

        let repairs = sweeper.scan(&watermarks, &[task], &[], now);

        assert_eq!(repairs.len(), 1);
        assert!(matches!(
            repairs[0],
            Repair::CreateDispatchOutbox { attempt: 2, .. }
        ));
    }

    /// The bootstrap PR #303 intended (issue #337): a RetryWait task with no
    /// retry deadline at all — folded before fold-time retry scheduling
    /// existed — must be rescued immediately, not skipped forever.
    #[test]
    fn test_anti_entropy_bootstraps_retry_wait_without_deadline() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row("extract", TaskState::RetryWait, None);
        task.attempt = 1;
        task.max_attempts = 3;
        task.retry_not_before = None;

        let repairs = sweeper.scan(&watermarks, &[task], &[], now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::CreateDispatchOutbox {
                task_key,
                attempt,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 2);
                assert_eq!(reason, "retry_wait_bootstrap");
            }
            _ => panic!("Expected CreateDispatchOutbox repair"),
        }
    }

    #[test]
    fn test_anti_entropy_fails_stale_running_tasks() {
        // Single coherent clock: the task's last heartbeat was also the last
        // compacted event, so the watermark froze at the same instant the
        // task went silent. The 3-minute silence exceeds the 60s + 30s
        // staleness window while compaction lag (3 minutes) is still inside
        // the 5-minute guard.
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let silence_started = now - Duration::minutes(3);

        let mut watermarks = fresh_watermarks(now);
        watermarks.last_processed_at = silence_started;

        let mut task = make_task_row("extract", TaskState::Running, None);
        task.attempt = 1;
        task.last_heartbeat_at = Some(silence_started);

        let repairs = sweeper.scan(&watermarks, &[task], &[], now);

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::FailStaleRunningTask {
                task_key,
                attempt,
                attempt_id,
                reason,
                ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(*attempt, 1);
                assert_eq!(attempt_id, "01HQ123ATT");
                assert!(reason.contains("heartbeat_timeout"));
            }
            _ => panic!("Expected FailStaleRunningTask repair"),
        }
    }

    /// Issue #338 regression: at default configuration
    /// (`heartbeat_timeout_sec` = 300 from the planner default, 30s grace,
    /// 5-minute compaction-lag guard) a zombie task in an idle workspace MUST
    /// be reaped. The task's own last heartbeat is the last workspace event,
    /// so the watermark is exactly as old as the silence — the wall-clock
    /// guard alone would skip forever (330s staleness > 300s guard), which is
    /// why a proven-current ledger bypasses it.
    #[test]
    fn test_anti_entropy_reaps_zombie_task_at_default_configuration() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();

        // Default staleness window: 300s timeout + 30s grace.
        let silence_started = now - Duration::seconds(331);

        // Coherent clock: the dead worker's heartbeat was the last event the
        // compactor processed, so watermark age == task silence.
        let mut watermarks = fresh_watermarks(now);
        watermarks.last_processed_at = silence_started;

        let mut task = make_task_row("extract", TaskState::Running, None);
        task.attempt = 1;
        task.heartbeat_timeout_sec = 300;
        task.last_heartbeat_at = Some(silence_started);

        // Without any freshness signal the wall-clock guard skips.
        let unproven = sweeper.scan(&watermarks, std::slice::from_ref(&task), &[], now);
        assert_eq!(unproven.len(), 1);
        assert!(matches!(unproven[0], Repair::SkippedDueToLag { .. }));

        // A current ledger scan is a maximum-id comparison, not a proof that
        // the folded prefix is contiguous, so it must NOT authorise the
        // destructive reap while the watermark is stale.
        let with_freshness = sweeper.scan_with_ledger_freshness(
            &watermarks,
            LedgerFreshness::Current,
            std::slice::from_ref(&task),
            &[],
            now,
        );
        assert_eq!(with_freshness.len(), 1);
        assert!(
            matches!(with_freshness[0], Repair::SkippedDueToLag { .. }),
            "ledger freshness must not authorise force-failing a RUNNING task \
             on evidence that cannot exclude a lower-id straggler: {:?}",
            with_freshness[0]
        );

        // Once compaction is demonstrably current by wall clock, the reaper runs.
        let repairs = sweeper.scan_with_ledger_freshness(
            &fresh_watermarks(now),
            LedgerFreshness::Current,
            &[task],
            &[],
            now,
        );

        assert_eq!(repairs.len(), 1);
        match &repairs[0] {
            Repair::FailStaleRunningTask {
                task_key, reason, ..
            } => {
                assert_eq!(task_key, "extract");
                assert_eq!(reason, "heartbeat_timeout_anti_entropy");
            }
            _ => panic!("Expected FailStaleRunningTask repair"),
        }
    }

    /// H4: a durable-but-unfolded straggler below the watermark makes the
    /// projection's RUNNING row untrustworthy. Freshness must not turn that
    /// row into a fabricated terminal failure, while the non-destructive
    /// dispatch re-drive that issue #338 needs stays reachable.
    #[test]
    fn ledger_freshness_bypass_covers_redrive_but_not_force_fail() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();

        // Compaction looks stale by wall clock (idle workspace).
        let mut watermarks = fresh_watermarks(now);
        watermarks.last_processed_at = now - Duration::seconds(3_600);

        let mut running = make_task_row("publish", TaskState::Running, None);
        running.attempt = 1;
        running.heartbeat_timeout_sec = 300;
        running.last_heartbeat_at = Some(now - Duration::seconds(3_600));

        let mut ready = make_task_row("extract", TaskState::Ready, Some(now - Duration::hours(1)));
        ready.attempt = 0;

        let repairs = sweeper.scan_with_ledger_freshness(
            &watermarks,
            LedgerFreshness::Current,
            &[ready, running],
            &[],
            now,
        );

        assert!(
            repairs.iter().any(|repair| matches!(
                repair,
                Repair::CreateDispatchOutbox { task_key, .. } if task_key == "extract"
            )),
            "non-destructive re-drive must still bypass the wall-clock guard: {repairs:?}"
        );
        assert!(
            !repairs.iter().any(Repair::is_destructive),
            "no destructive repair may be emitted on freshness alone: {repairs:?}"
        );
        assert!(
            repairs.iter().any(|repair| matches!(
                repair,
                Repair::SkippedDueToLag { task_key, .. } if task_key == "publish"
            )),
            "the suppressed force-fail must be reported, not silently dropped: {repairs:?}"
        );
    }

    /// A RUNNING task inside its staleness window must not be reaped even
    /// when the ledger is proven current.
    #[test]
    fn test_anti_entropy_keeps_live_running_task_with_current_ledger() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();

        let heartbeat_at = now - Duration::seconds(329);
        let mut watermarks = fresh_watermarks(now);
        watermarks.last_processed_at = heartbeat_at;

        let mut task = make_task_row("extract", TaskState::Running, None);
        task.attempt = 1;
        task.heartbeat_timeout_sec = 300;
        task.last_heartbeat_at = Some(heartbeat_at);

        let repairs = sweeper.scan_with_ledger_freshness(
            &watermarks,
            LedgerFreshness::Current,
            &[task],
            &[],
            now,
        );

        assert!(repairs.is_empty(), "329s silence is inside the 330s window");
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
    fn test_anti_entropy_allows_one_minute_scheduler_cadence() {
        let now = Utc::now();
        let sweeper = AntiEntropySweeper::with_defaults();
        let watermarks = scheduler_cadence_watermarks(now);

        let tasks = vec![make_task_row(
            "extract",
            TaskState::Ready,
            Some(now - Duration::minutes(10)),
        )];

        let outbox = vec![];

        let repairs = sweeper.scan(&watermarks, &tasks, &outbox, now);

        assert_eq!(repairs.len(), 1);
        assert!(matches!(repairs[0], Repair::CreateDispatchOutbox { .. }));
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
