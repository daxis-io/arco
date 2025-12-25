//! Timer controller for durable timers via Cloud Tasks.
//!
//! The timer controller reads the timers Parquet table and creates
//! Cloud Tasks timers for scheduled events like:
//!
//! - **Retry timers**: Fire when retry backoff expires
//! - **Heartbeat check timers**: Fire to check for zombie tasks
//! - **Cron timers**: Fire on schedule
//! - **SLA check timers**: Fire to check deadline violations
//!
//! ## Dual-Identifier Pattern (ADR-021)
//!
//! Each timer has two identifiers:
//! - `timer_id`: Human-readable internal ID for Parquet PKs and debugging
//! - `cloud_task_id`: Hash-based API-compliant ID for Cloud Tasks
//!
//! ## Watermark Freshness Guard
//!
//! All timer actions check watermark freshness before making decisions.
//! If compaction is lagging, timers reschedule instead of acting,
//! preventing incorrect decisions based on stale state.

use chrono::{DateTime, Duration, Utc};
use metrics::{counter, histogram};

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::{TimerRow, TimerState, TimerType};
use crate::orchestration::compactor::manifest::OrchestrationManifest;
use crate::orchestration::ids::cloud_task_id;

/// Action returned by the timer reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerAction {
    /// Create a Cloud Task timer.
    CreateTimer {
        /// Human-readable timer ID.
        timer_id: String,
        /// Cloud Tasks-compliant task ID.
        cloud_task_id: String,
        /// Timer type.
        timer_type: TimerType,
        /// When to fire.
        fire_at: DateTime<Utc>,
        /// Associated run (if any).
        run_id: Option<String>,
        /// Associated task (if any).
        task_key: Option<String>,
        /// Associated attempt (if any).
        attempt: Option<u32>,
    },
    /// Skip this timer (already created or cancelled).
    Skip {
        /// Timer ID that was skipped.
        timer_id: String,
        /// Reason for skipping.
        reason: String,
    },
}

/// Timer controller for reconciling timers to Cloud Tasks.
///
/// The timer controller follows the controller pattern:
/// 1. Read timers from Parquet (base + L0 deltas)
/// 2. For each SCHEDULED timer, compute the Cloud Tasks ID and create the timer
/// 3. Emit `TimerEnqueued` events to the ledger
pub struct TimerController {
    /// Maximum compaction lag before skipping timer creation.
    max_compaction_lag: Duration,
}

impl TimerController {
    /// Creates a new timer controller.
    #[must_use]
    pub fn new(max_compaction_lag: Duration) -> Self {
        Self { max_compaction_lag }
    }

    /// Creates a timer controller with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(Duration::seconds(30))
    }

    /// Reconciles the timers table and returns actions to execute.
    ///
    /// # Arguments
    ///
    /// * `manifest` - The current orchestration manifest
    /// * `timer_rows` - Timer rows from Parquet (base + L0 deltas merged)
    ///
    /// # Returns
    ///
    /// A list of timer actions to execute.
    #[must_use]
    pub fn reconcile(
        &self,
        manifest: &OrchestrationManifest,
        timer_rows: &[TimerRow],
    ) -> Vec<TimerAction> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "timer".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        // Check watermark freshness
        let actions: Vec<TimerAction> = if manifest.watermarks.is_fresh(self.max_compaction_lag) {
            timer_rows.iter().map(Self::reconcile_row).collect()
        } else {
            timer_rows
                .iter()
                .filter(|row| row.state == TimerState::Scheduled && row.cloud_task_id.is_none())
                .map(|row| TimerAction::Skip {
                    timer_id: row.timer_id.clone(),
                    reason: "compaction_lag".to_string(),
                })
                .collect()
        };

        let count = u64::try_from(actions.len()).unwrap_or(0);
        counter!(
            metrics_names::ORCH_CONTROLLER_ACTIONS_TOTAL,
            metrics_labels::CONTROLLER => "timer".to_string(),
        )
        .increment(count);

        actions
    }

    /// Reconciles a single timer row.
    fn reconcile_row(row: &TimerRow) -> TimerAction {
        match row.state {
            TimerState::Scheduled => {
                // Only create if not already created
                if row.cloud_task_id.is_some() {
                    return TimerAction::Skip {
                        timer_id: row.timer_id.clone(),
                        reason: "already_has_cloud_task_id".to_string(),
                    };
                }

                let cloud_id = cloud_task_id("t", &row.timer_id);

                TimerAction::CreateTimer {
                    timer_id: row.timer_id.clone(),
                    cloud_task_id: cloud_id,
                    timer_type: row.timer_type,
                    fire_at: row.fire_at,
                    run_id: row.run_id.clone(),
                    task_key: row.task_key.clone(),
                    attempt: row.attempt,
                }
            }
            TimerState::Fired => TimerAction::Skip {
                timer_id: row.timer_id.clone(),
                reason: "already_fired".to_string(),
            },
            TimerState::Cancelled => TimerAction::Skip {
                timer_id: row.timer_id.clone(),
                reason: "cancelled".to_string(),
            },
        }
    }

    /// Generates the idempotency key for a `TimerEnqueued` event.
    #[must_use]
    pub fn timer_enqueued_idempotency_key(timer_id: &str) -> String {
        format!("timer_enqueued:{timer_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timer_row(
        timer_id: &str,
        timer_type: TimerType,
        state: TimerState,
        cloud_task_id: Option<String>,
    ) -> TimerRow {
        TimerRow {
            timer_id: timer_id.to_string(),
            cloud_task_id,
            timer_type,
            run_id: Some("run1".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
            fire_at: Utc::now() + Duration::seconds(30),
            state,
            payload: None,
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
    fn test_timer_controller_creates_timers_for_scheduled() {
        let controller = TimerController::with_defaults();
        let manifest = fresh_manifest();

        let timer_rows = vec![make_timer_row(
            "timer:retry:run1:extract:1:1705320000",
            TimerType::Retry,
            TimerState::Scheduled,
            None,
        )];

        let actions = controller.reconcile(&manifest, &timer_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            TimerAction::CreateTimer {
                timer_id,
                cloud_task_id,
                timer_type,
                ..
            } => {
                assert_eq!(timer_id, "timer:retry:run1:extract:1:1705320000");
                assert!(cloud_task_id.starts_with("t_"));
                assert_eq!(*timer_type, TimerType::Retry);
            }
            _ => panic!("Expected CreateTimer action"),
        }
    }

    #[test]
    fn test_timer_controller_skips_already_created() {
        let controller = TimerController::with_defaults();
        let manifest = fresh_manifest();

        let timer_rows = vec![make_timer_row(
            "timer:retry:run1:extract:1:1705320000",
            TimerType::Retry,
            TimerState::Scheduled,
            Some("t_abc123".to_string()),
        )];

        let actions = controller.reconcile(&manifest, &timer_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            TimerAction::Skip { reason, .. } => {
                assert_eq!(reason, "already_has_cloud_task_id");
            }
            _ => panic!("Expected Skip action"),
        }
    }

    #[test]
    fn test_timer_controller_skips_fired() {
        let controller = TimerController::with_defaults();
        let manifest = fresh_manifest();

        let timer_rows = vec![make_timer_row(
            "timer:retry:run1:extract:1:1705320000",
            TimerType::Retry,
            TimerState::Fired,
            Some("t_abc123".to_string()),
        )];

        let actions = controller.reconcile(&manifest, &timer_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            TimerAction::Skip { reason, .. } => {
                assert_eq!(reason, "already_fired");
            }
            _ => panic!("Expected Skip action"),
        }
    }

    #[test]
    fn test_timer_controller_guards_against_compaction_lag() {
        let controller = TimerController::with_defaults();
        let manifest = stale_manifest();

        let timer_rows = vec![make_timer_row(
            "timer:retry:run1:extract:1:1705320000",
            TimerType::Retry,
            TimerState::Scheduled,
            None,
        )];

        let actions = controller.reconcile(&manifest, &timer_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            TimerAction::Skip { reason, .. } => {
                assert_eq!(reason, "compaction_lag");
            }
            _ => panic!("Expected Skip action due to compaction lag"),
        }
    }

    #[test]
    fn test_timer_controller_handles_different_timer_types() {
        let controller = TimerController::with_defaults();
        let manifest = fresh_manifest();

        let timer_rows = vec![
            make_timer_row(
                "timer:retry:run1:extract:1:1705320000",
                TimerType::Retry,
                TimerState::Scheduled,
                None,
            ),
            make_timer_row(
                "timer:heartbeat:run1:extract:1:1705320000",
                TimerType::HeartbeatCheck,
                TimerState::Scheduled,
                None,
            ),
            TimerRow {
                timer_id: "timer:cron:daily-etl:1705320000".to_string(),
                cloud_task_id: None,
                timer_type: TimerType::Cron,
                run_id: None,
                task_key: None,
                attempt: None,
                fire_at: Utc::now() + Duration::seconds(30),
                state: TimerState::Scheduled,
                payload: Some(r#"{"schedule_id":"daily-etl"}"#.to_string()),
                row_version: "01HQ123EVT".to_string(),
            },
        ];

        let actions = controller.reconcile(&manifest, &timer_rows);

        assert_eq!(actions.len(), 3);

        // All should be CreateTimer actions
        for action in &actions {
            assert!(matches!(action, TimerAction::CreateTimer { .. }));
        }
    }

    #[test]
    fn test_timer_enqueued_idempotency_key() {
        let key = TimerController::timer_enqueued_idempotency_key(
            "timer:retry:run1:extract:1:1705320000",
        );
        assert_eq!(key, "timer_enqueued:timer:retry:run1:extract:1:1705320000");
    }
}
