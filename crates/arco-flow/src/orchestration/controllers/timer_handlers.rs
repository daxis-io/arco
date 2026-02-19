//! Timer handlers for processing fired timers.
//!
//! When a Cloud Task timer fires, the appropriate handler processes it:
//!
//! - **`HeartbeatHandler`**: Checks if a running task is still healthy
//! - **`RetryHandler`**: Determines if a failed task should be retried
//!
//! ## Watermark Freshness Guard
//!
//! All timer handlers check watermark freshness before making decisions.
//! This is critical because:
//!
//! 1. If compaction is behind, the Parquet state may not reflect recent events
//! 2. A heartbeat check might see stale `last_heartbeat_at` and incorrectly fail a healthy task
//! 3. A retry timer might fire "early" if the task actually succeeded but compaction hasn't caught up
//!
//! The freshness guard ensures timers reschedule when compaction is lagging,
//! preventing incorrect decisions based on stale state.

use chrono::{DateTime, Duration, Utc};

use crate::orchestration::compactor::fold::{TaskRow, TaskState};
use crate::orchestration::compactor::manifest::Watermarks;
use crate::orchestration::ids::{deterministic_attempt_id, dispatch_internal_id};

/// Maximum compaction lag allowed for timer actions.
///
/// If compaction is behind by more than this amount, timer handlers
/// will reschedule instead of making decisions.
pub const MAX_COMPACTION_LAG_SECS: i64 = 30;

/// Action returned by the heartbeat handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatAction {
    /// Reschedule the heartbeat check.
    Reschedule {
        /// Delay before rescheduling.
        delay: Duration,
        /// Reason for rescheduling.
        reason: String,
    },
    /// Fail the task due to missed heartbeat.
    FailTask {
        /// Run ID.
        run_id: String,
        /// Task key.
        task_key: String,
        /// Attempt number.
        attempt: u32,
        /// Attempt ID (for concurrency guard).
        attempt_id: String,
        /// Reason for failure.
        reason: String,
    },
    /// No action needed (task already terminal).
    NoOp {
        /// Reason for no action.
        reason: String,
    },
}

/// Handler for heartbeat check timers.
///
/// When a heartbeat check timer fires, this handler determines whether:
/// 1. The task is still healthy (reschedule check)
/// 2. The task has missed its heartbeat (fail it)
/// 3. Compaction is lagging (reschedule to wait for fresh data)
pub struct HeartbeatHandler {
    /// Maximum compaction lag before rescheduling.
    max_compaction_lag: Duration,
    /// Grace period after heartbeat timeout.
    grace_period: Duration,
}

impl HeartbeatHandler {
    /// Creates a new heartbeat handler.
    #[must_use]
    pub fn new(max_compaction_lag_secs: i64) -> Self {
        Self {
            max_compaction_lag: Duration::seconds(max_compaction_lag_secs),
            grace_period: Duration::seconds(30),
        }
    }

    /// Creates a handler with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(MAX_COMPACTION_LAG_SECS)
    }

    /// Checks a task's heartbeat status.
    ///
    /// # Arguments
    ///
    /// * `watermarks` - Current compaction watermarks
    /// * `task` - The task to check
    /// * `now` - Current timestamp
    ///
    /// # Returns
    ///
    /// Action to take based on the heartbeat status.
    #[must_use]
    pub fn check(
        &self,
        watermarks: &Watermarks,
        task: &TaskRow,
        now: DateTime<Utc>,
    ) -> HeartbeatAction {
        // Check if task is still running
        if task.state.is_terminal() {
            return HeartbeatAction::NoOp {
                reason: format!("task is terminal: {:?}", task.state),
            };
        }

        if task.state != TaskState::Running {
            return HeartbeatAction::NoOp {
                reason: format!("task is not running: {:?}", task.state),
            };
        }

        // Check watermark freshness - critical guard!
        let compaction_lag = now - watermarks.last_processed_at;
        if compaction_lag > self.max_compaction_lag {
            return HeartbeatAction::Reschedule {
                delay: Duration::seconds(10),
                reason: format!(
                    "compaction_lag_guard: {}s lag (max {}s)",
                    compaction_lag.num_seconds(),
                    self.max_compaction_lag.num_seconds()
                ),
            };
        }

        // Check actual heartbeat freshness
        let Some(last_heartbeat) = task.last_heartbeat_at else {
            // No heartbeat received yet - reschedule with longer delay
            return HeartbeatAction::Reschedule {
                delay: Duration::seconds(30),
                reason: "no_heartbeat_yet".to_string(),
            };
        };

        let heartbeat_age = now - last_heartbeat;
        let timeout = Duration::seconds(i64::from(task.heartbeat_timeout_sec));
        let deadline = timeout + self.grace_period;

        if heartbeat_age > deadline {
            let Some(attempt_id) = task.attempt_id.clone() else {
                return HeartbeatAction::Reschedule {
                    delay: Duration::seconds(10),
                    reason: "missing_attempt_id".to_string(),
                };
            };

            // Task has genuinely missed heartbeat
            HeartbeatAction::FailTask {
                run_id: task.run_id.clone(),
                task_key: task.task_key.clone(),
                attempt: task.attempt,
                attempt_id,
                reason: format!(
                    "heartbeat_timeout: {}s since last heartbeat (timeout: {}s + {}s grace)",
                    heartbeat_age.num_seconds(),
                    timeout.num_seconds(),
                    self.grace_period.num_seconds()
                ),
            }
        } else {
            // Task is healthy, reschedule next check
            let next_check = timeout - heartbeat_age + self.grace_period;
            HeartbeatAction::Reschedule {
                delay: next_check.max(Duration::seconds(10)),
                reason: "task_healthy".to_string(),
            }
        }
    }
}

/// Action returned by the retry handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryAction {
    /// Reschedule the retry timer.
    Reschedule {
        /// Delay before rescheduling.
        delay: Duration,
        /// Reason for rescheduling.
        reason: String,
    },
    /// Increment the attempt and dispatch.
    IncrementAttempt {
        /// Run ID.
        run_id: String,
        /// Task key.
        task_key: String,
        /// New attempt number.
        new_attempt: u32,
        /// New attempt ID.
        new_attempt_id: String,
    },
    /// No action needed (task already terminal or wrong state).
    NoOp {
        /// Reason for no action.
        reason: String,
    },
}

/// Handler for retry timers.
///
/// When a retry timer fires, this handler determines whether:
/// 1. The task should be retried (increment attempt, dispatch)
/// 2. The task has already succeeded (no-op)
/// 3. Compaction is lagging (reschedule to wait for fresh data)
pub struct RetryHandler {
    /// Maximum compaction lag before rescheduling.
    max_compaction_lag: Duration,
}

impl RetryHandler {
    /// Creates a new retry handler.
    #[must_use]
    pub fn new(max_compaction_lag_secs: i64) -> Self {
        Self {
            max_compaction_lag: Duration::seconds(max_compaction_lag_secs),
        }
    }

    /// Creates a handler with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(MAX_COMPACTION_LAG_SECS)
    }

    /// Processes a retry timer fire.
    ///
    /// # Arguments
    ///
    /// * `watermarks` - Current compaction watermarks
    /// * `task` - The task to potentially retry
    /// * `now` - Current timestamp
    ///
    /// # Returns
    ///
    /// Action to take for the retry.
    #[must_use]
    pub fn fire(&self, watermarks: &Watermarks, task: &TaskRow, now: DateTime<Utc>) -> RetryAction {
        // Check if task is already terminal
        if task.state.is_terminal() {
            return RetryAction::NoOp {
                reason: format!("task is terminal: {:?}", task.state),
            };
        }

        // Only retry from RetryWait state
        if task.state != TaskState::RetryWait {
            return RetryAction::NoOp {
                reason: format!("task is not in RetryWait: {:?}", task.state),
            };
        }

        // Check watermark freshness - critical guard!
        // If compaction is behind, we might miss a success event and incorrectly retry
        let compaction_lag = now - watermarks.last_processed_at;
        if compaction_lag > self.max_compaction_lag {
            return RetryAction::Reschedule {
                delay: Duration::seconds(10),
                reason: format!(
                    "compaction_lag_guard: {}s lag (max {}s)",
                    compaction_lag.num_seconds(),
                    self.max_compaction_lag.num_seconds()
                ),
            };
        }

        // Check if we have retries left
        if task.attempt >= task.max_attempts {
            return RetryAction::NoOp {
                reason: format!(
                    "max_attempts_reached: {} >= {}",
                    task.attempt, task.max_attempts
                ),
            };
        }

        // Increment attempt and dispatch with deterministic attempt_id
        let new_attempt = task.attempt + 1;
        let dispatch_id = dispatch_internal_id(&task.run_id, &task.task_key, new_attempt);
        let new_attempt_id = deterministic_attempt_id(&dispatch_id);

        RetryAction::IncrementAttempt {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            new_attempt,
            new_attempt_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::orchestration_event_path;

    fn make_task_row(
        state: TaskState,
        attempt: u32,
        last_heartbeat: Option<DateTime<Utc>>,
    ) -> TaskRow {
        TaskRow {
            run_id: "run1".to_string(),
            task_key: "extract".to_string(),
            state,
            attempt,
            attempt_id: Some("01HQ123ATT".to_string()),
            started_at: None,
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 60,
            last_heartbeat_at: last_heartbeat,
            ready_at: None,
            asset_key: None,
            partition_key: None,
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
            last_processed_at: now - Duration::seconds(45),
        }
    }

    // ========================================================================
    // Heartbeat Handler Tests
    // ========================================================================

    #[test]
    fn test_heartbeat_check_guards_against_compaction_lag() {
        let now = Utc::now();
        let handler = HeartbeatHandler::with_defaults();
        let watermarks = stale_watermarks(now);

        let task = make_task_row(TaskState::Running, 1, Some(now - Duration::seconds(30)));

        let action = handler.check(&watermarks, &task, now);

        // Should reschedule, not fail (compaction lag too high)
        assert!(
            matches!(action, HeartbeatAction::Reschedule { reason, .. } if reason.contains("compaction_lag"))
        );
    }

    #[test]
    fn test_heartbeat_check_fails_when_compaction_fresh() {
        let now = Utc::now();
        let handler = HeartbeatHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(
            TaskState::Running,
            1,
            Some(now - Duration::seconds(120)), // 120s since heartbeat, timeout is 60s
        );

        let action = handler.check(&watermarks, &task, now);

        // Should fail (compaction is fresh, task is genuinely stale)
        assert!(
            matches!(action, HeartbeatAction::FailTask { reason, .. } if reason.contains("heartbeat_timeout"))
        );
    }

    #[test]
    fn test_heartbeat_reschedules_when_attempt_id_missing() {
        let now = Utc::now();
        let handler = HeartbeatHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row(TaskState::Running, 1, Some(now - Duration::seconds(120)));
        task.attempt_id = None;

        let action = handler.check(&watermarks, &task, now);

        assert!(
            matches!(action, HeartbeatAction::Reschedule { reason, .. } if reason == "missing_attempt_id")
        );
    }

    #[test]
    fn test_heartbeat_reschedules_for_healthy_task() {
        let now = Utc::now();
        let handler = HeartbeatHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(
            TaskState::Running,
            1,
            Some(now - Duration::seconds(10)), // Recent heartbeat
        );

        let action = handler.check(&watermarks, &task, now);

        // Should reschedule (task is healthy)
        assert!(
            matches!(action, HeartbeatAction::Reschedule { reason, .. } if reason == "task_healthy")
        );
    }

    #[test]
    fn test_heartbeat_noop_for_terminal_task() {
        let now = Utc::now();
        let handler = HeartbeatHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(TaskState::Succeeded, 1, Some(now - Duration::seconds(10)));

        let action = handler.check(&watermarks, &task, now);

        assert!(matches!(action, HeartbeatAction::NoOp { reason } if reason.contains("terminal")));
    }

    #[test]
    fn test_heartbeat_reschedules_when_no_heartbeat_yet() {
        let now = Utc::now();
        let handler = HeartbeatHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(TaskState::Running, 1, None);

        let action = handler.check(&watermarks, &task, now);

        assert!(
            matches!(action, HeartbeatAction::Reschedule { reason, .. } if reason == "no_heartbeat_yet")
        );
    }

    // ========================================================================
    // Retry Handler Tests
    // ========================================================================

    #[test]
    fn test_retry_timer_respects_watermark_freshness() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = stale_watermarks(now);

        let task = make_task_row(TaskState::RetryWait, 1, None);

        let action = handler.fire(&watermarks, &task, now);

        // Should reschedule, NOT increment attempt (compaction lag too high)
        assert!(
            matches!(action, RetryAction::Reschedule { reason, .. } if reason.contains("compaction_lag"))
        );
    }

    #[test]
    fn test_retry_timer_increments_when_compaction_fresh() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(TaskState::RetryWait, 1, None);

        let action = handler.fire(&watermarks, &task, now);

        // Compaction is fresh, safe to increment attempt
        assert!(matches!(
            action,
            RetryAction::IncrementAttempt { new_attempt: 2, .. }
        ));
    }

    #[test]
    fn test_retry_noop_for_terminal_task() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(TaskState::Succeeded, 1, None);

        let action = handler.fire(&watermarks, &task, now);

        assert!(matches!(action, RetryAction::NoOp { reason } if reason.contains("terminal")));
    }

    #[test]
    fn test_retry_noop_when_not_in_retry_wait() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(TaskState::Running, 1, None);

        let action = handler.fire(&watermarks, &task, now);

        assert!(
            matches!(action, RetryAction::NoOp { reason } if reason.contains("not in RetryWait"))
        );
    }

    #[test]
    fn test_retry_noop_when_max_attempts_reached() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let mut task = make_task_row(TaskState::RetryWait, 3, None);
        task.max_attempts = 3; // Already at max

        let action = handler.fire(&watermarks, &task, now);

        assert!(
            matches!(action, RetryAction::NoOp { reason } if reason.contains("max_attempts_reached"))
        );
    }

    #[test]
    fn test_retry_generates_deterministic_attempt_id() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task = make_task_row(TaskState::RetryWait, 1, None);

        let action1 = handler.fire(&watermarks, &task, now);
        let action2 = handler.fire(&watermarks, &task, now);

        // Same input should produce same attempt_id (deterministic for idempotency)
        match (action1, action2) {
            (
                RetryAction::IncrementAttempt {
                    new_attempt_id: id1,
                    new_attempt: a1,
                    ..
                },
                RetryAction::IncrementAttempt {
                    new_attempt_id: id2,
                    new_attempt: a2,
                    ..
                },
            ) => {
                assert_eq!(a1, a2, "Same attempt number");
                assert_eq!(
                    id1, id2,
                    "Deterministic attempt_id for controller idempotency"
                );
            }
            _ => panic!("Expected IncrementAttempt actions"),
        }
    }

    #[test]
    fn test_retry_different_attempts_get_different_attempt_ids() {
        let now = Utc::now();
        let handler = RetryHandler::with_defaults();
        let watermarks = fresh_watermarks(now);

        let task1 = make_task_row(TaskState::RetryWait, 1, None);
        let task2 = make_task_row(TaskState::RetryWait, 2, None);

        let action1 = handler.fire(&watermarks, &task1, now);
        let action2 = handler.fire(&watermarks, &task2, now);

        // Different attempt numbers should produce different attempt_ids
        match (action1, action2) {
            (
                RetryAction::IncrementAttempt {
                    new_attempt_id: id1,
                    new_attempt: a1,
                    ..
                },
                RetryAction::IncrementAttempt {
                    new_attempt_id: id2,
                    new_attempt: a2,
                    ..
                },
            ) => {
                assert_ne!(a1, a2, "Different attempt numbers");
                assert_ne!(
                    id1, id2,
                    "Different attempts should have different attempt_ids"
                );
            }
            _ => panic!("Expected IncrementAttempt actions"),
        }
    }
}
