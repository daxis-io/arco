//! Dispatcher controller for Cloud Tasks integration.
//!
//! The dispatcher reads the `dispatch_outbox` Parquet table, creates Cloud Tasks
//! for pending dispatches, and emits `DispatchEnqueued` events to the ledger.
//!
//! ## Design Principles (ADR-020)
//!
//! 1. **Stateless**: Can be scaled horizontally
//! 2. **Idempotent**: Same input produces same output
//! 3. **Parquet-only reads**: Never reads from the ledger
//! 4. **Event emission**: Writes intent/acknowledgement facts to ledger
//!
//! ## Dual-Identifier Pattern (ADR-021)
//!
//! Each dispatch has two identifiers:
//! - `dispatch_id`: Human-readable internal ID for Parquet PKs and debugging
//! - `cloud_task_id`: Hash-based API-compliant ID for Cloud Tasks

use chrono::{DateTime, Utc};
use metrics::{counter, histogram};

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::{DispatchOutboxRow, DispatchStatus};
use crate::orchestration::compactor::manifest::OrchestrationManifest;
use crate::orchestration::ids::cloud_task_id;

/// Action returned by the dispatcher reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchAction {
    /// Create a Cloud Task for this dispatch.
    CreateCloudTask {
        /// Human-readable dispatch ID.
        dispatch_id: String,
        /// Cloud Tasks-compliant task ID.
        cloud_task_id: String,
        /// Run identifier.
        run_id: String,
        /// Task key within run.
        task_key: String,
        /// Attempt number.
        attempt: u32,
        /// Attempt ID for concurrency guard.
        attempt_id: String,
        /// Target worker queue.
        worker_queue: String,
    },
    /// Skip this dispatch (already created or acked).
    Skip {
        /// Dispatch ID that was skipped.
        dispatch_id: String,
        /// Reason for skipping.
        reason: String,
    },
}

/// Result of executing a dispatch action.
#[derive(Debug, Clone)]
pub enum DispatchResult {
    /// Cloud Task was created successfully.
    Enqueued {
        /// Dispatch ID.
        dispatch_id: String,
        /// Cloud Tasks task ID.
        cloud_task_id: String,
        /// When the task was created.
        created_at: DateTime<Utc>,
    },
    /// Cloud Task already existed (deduplicated).
    Deduplicated {
        /// Dispatch ID.
        dispatch_id: String,
        /// Cloud Tasks task ID.
        cloud_task_id: String,
    },
    /// Dispatch failed.
    Failed {
        /// Dispatch ID.
        dispatch_id: String,
        /// Error message.
        error: String,
    },
}

/// Dispatcher controller for reconciling `dispatch_outbox` to Cloud Tasks.
///
/// The dispatcher follows the controller pattern:
/// 1. Read `dispatch_outbox` from Parquet (base + L0 deltas)
/// 2. For each PENDING row, compute the Cloud Tasks ID and create the task
/// 3. Emit `DispatchEnqueued` events to the ledger
///
/// ## Idempotency
///
/// Cloud Tasks provides built-in deduplication for 24 hours based on task ID.
/// Our deterministic `cloud_task_id` ensures repeated reconciliation produces
/// the same Cloud Tasks, which are deduplicated by the Cloud Tasks API.
pub struct DispatcherController {
    /// Maximum compaction lag before skipping dispatch.
    ///
    /// If the Parquet projection is too stale, we skip dispatching to avoid
    /// making decisions based on outdated state.
    max_compaction_lag: chrono::Duration,
}

impl DispatcherController {
    /// Creates a new dispatcher controller.
    #[must_use]
    pub fn new(max_compaction_lag: chrono::Duration) -> Self {
        Self { max_compaction_lag }
    }

    /// Creates a dispatcher with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(chrono::Duration::seconds(30))
    }

    /// Reconciles the `dispatch_outbox` and returns actions to execute.
    ///
    /// This method reads the `dispatch_outbox` Parquet table and determines
    /// which dispatches need to be created in Cloud Tasks.
    ///
    /// # Arguments
    ///
    /// * `manifest` - The current orchestration manifest
    /// * `outbox_rows` - Dispatch outbox rows from Parquet (base + L0 deltas merged)
    ///
    /// # Returns
    ///
    /// A list of dispatch actions to execute.
    #[must_use]
    pub fn reconcile(
        &self,
        manifest: &OrchestrationManifest,
        outbox_rows: &[DispatchOutboxRow],
    ) -> Vec<DispatchAction> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "dispatcher".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        // Check watermark freshness
        let actions: Vec<DispatchAction> = if manifest.watermarks.is_fresh(self.max_compaction_lag)
        {
            outbox_rows.iter().map(Self::reconcile_row).collect()
        } else {
            // Compaction is behind - skip dispatching to avoid stale decisions
            outbox_rows
                .iter()
                .filter(|row| row.status == DispatchStatus::Pending)
                .map(|row| DispatchAction::Skip {
                    dispatch_id: row.dispatch_id.clone(),
                    reason: "compaction_lag".to_string(),
                })
                .collect()
        };

        let count = u64::try_from(actions.len()).unwrap_or(0);
        counter!(
            metrics_names::ORCH_CONTROLLER_ACTIONS_TOTAL,
            metrics_labels::CONTROLLER => "dispatcher".to_string(),
        )
        .increment(count);

        actions
    }

    /// Reconciles a single dispatch outbox row.
    fn reconcile_row(row: &DispatchOutboxRow) -> DispatchAction {
        match row.status {
            DispatchStatus::Pending => {
                // Generate Cloud Tasks ID from internal dispatch ID
                let cloud_id = cloud_task_id("d", &row.dispatch_id);

                DispatchAction::CreateCloudTask {
                    dispatch_id: row.dispatch_id.clone(),
                    cloud_task_id: cloud_id,
                    run_id: row.run_id.clone(),
                    task_key: row.task_key.clone(),
                    attempt: row.attempt,
                    attempt_id: row.attempt_id.clone(),
                    worker_queue: row.worker_queue.clone(),
                }
            }
            DispatchStatus::Created => DispatchAction::Skip {
                dispatch_id: row.dispatch_id.clone(),
                reason: "already_created".to_string(),
            },
            DispatchStatus::Acked => DispatchAction::Skip {
                dispatch_id: row.dispatch_id.clone(),
                reason: "already_acked".to_string(),
            },
            DispatchStatus::Failed => DispatchAction::Skip {
                dispatch_id: row.dispatch_id.clone(),
                reason: "previously_failed".to_string(),
            },
        }
    }

    /// Generates the idempotency key for a `DispatchEnqueued` event.
    ///
    /// This key ensures that duplicate event emissions are deduplicated
    /// in the ledger.
    #[must_use]
    pub fn dispatch_enqueued_idempotency_key(dispatch_id: &str) -> String {
        format!("dispatch_enqueued:{dispatch_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_outbox_row(dispatch_id: &str, status: DispatchStatus) -> DispatchOutboxRow {
        DispatchOutboxRow {
            run_id: "run1".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            dispatch_id: dispatch_id.to_string(),
            cloud_task_id: None,
            status,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at: Utc::now(),
            row_version: "01HQ123EVT".to_string(),
        }
    }

    fn fresh_manifest() -> OrchestrationManifest {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.watermarks.last_processed_at = Utc::now() - chrono::Duration::seconds(5);
        manifest
    }

    fn stale_manifest() -> OrchestrationManifest {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.watermarks.last_processed_at = Utc::now() - chrono::Duration::seconds(60);
        manifest
    }

    #[test]
    fn test_dispatcher_creates_cloud_tasks_for_pending_outbox() {
        let dispatcher = DispatcherController::with_defaults();
        let manifest = fresh_manifest();

        let outbox_rows = vec![make_outbox_row(
            "dispatch:run1:extract:1",
            DispatchStatus::Pending,
        )];

        let actions = dispatcher.reconcile(&manifest, &outbox_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            DispatchAction::CreateCloudTask {
                dispatch_id,
                cloud_task_id,
                ..
            } => {
                assert_eq!(dispatch_id, "dispatch:run1:extract:1");
                assert!(cloud_task_id.starts_with("d_"));
                assert_eq!(cloud_task_id.len(), 28);
            }
            _ => panic!("Expected CreateCloudTask action"),
        }
    }

    #[test]
    fn test_dispatcher_skips_already_created() {
        let dispatcher = DispatcherController::with_defaults();
        let manifest = fresh_manifest();

        let outbox_rows = vec![make_outbox_row(
            "dispatch:run1:extract:1",
            DispatchStatus::Created,
        )];

        let actions = dispatcher.reconcile(&manifest, &outbox_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            DispatchAction::Skip {
                dispatch_id,
                reason,
            } => {
                assert_eq!(dispatch_id, "dispatch:run1:extract:1");
                assert_eq!(reason, "already_created");
            }
            _ => panic!("Expected Skip action"),
        }
    }

    #[test]
    fn test_dispatcher_skips_acked() {
        let dispatcher = DispatcherController::with_defaults();
        let manifest = fresh_manifest();

        let outbox_rows = vec![make_outbox_row(
            "dispatch:run1:extract:1",
            DispatchStatus::Acked,
        )];

        let actions = dispatcher.reconcile(&manifest, &outbox_rows);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            DispatchAction::Skip { reason, .. } => {
                assert_eq!(reason, "already_acked");
            }
            _ => panic!("Expected Skip action"),
        }
    }

    #[test]
    fn test_dispatcher_guards_against_compaction_lag() {
        let dispatcher = DispatcherController::with_defaults();
        let manifest = stale_manifest();

        let outbox_rows = vec![make_outbox_row(
            "dispatch:run1:extract:1",
            DispatchStatus::Pending,
        )];

        let actions = dispatcher.reconcile(&manifest, &outbox_rows);

        // Should skip due to compaction lag
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            DispatchAction::Skip { reason, .. } => {
                assert_eq!(reason, "compaction_lag");
            }
            _ => panic!("Expected Skip action due to compaction lag"),
        }
    }

    #[test]
    fn test_dispatcher_is_deterministic() {
        let dispatcher = DispatcherController::with_defaults();
        let manifest = fresh_manifest();

        let outbox_rows = vec![make_outbox_row(
            "dispatch:run1:extract:1",
            DispatchStatus::Pending,
        )];

        let actions1 = dispatcher.reconcile(&manifest, &outbox_rows);
        let actions2 = dispatcher.reconcile(&manifest, &outbox_rows);

        // Same input = same output (determinism)
        assert_eq!(actions1, actions2);
    }

    #[test]
    fn test_dispatch_enqueued_idempotency_key() {
        let key =
            DispatcherController::dispatch_enqueued_idempotency_key("dispatch:run1:extract:1");
        assert_eq!(key, "dispatch_enqueued:dispatch:run1:extract:1");
    }

    #[test]
    fn test_cloud_task_id_different_for_different_attempts() {
        let dispatcher = DispatcherController::with_defaults();
        let manifest = fresh_manifest();

        let outbox_rows = vec![
            make_outbox_row("dispatch:run1:extract:1", DispatchStatus::Pending),
            DispatchOutboxRow {
                dispatch_id: "dispatch:run1:extract:2".to_string(),
                attempt: 2,
                ..make_outbox_row("dispatch:run1:extract:1", DispatchStatus::Pending)
            },
        ];

        let actions = dispatcher.reconcile(&manifest, &outbox_rows);

        assert_eq!(actions.len(), 2);

        // Extract cloud_task_ids
        let cloud_ids: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                DispatchAction::CreateCloudTask { cloud_task_id, .. } => {
                    Some(cloud_task_id.clone())
                }
                _ => None,
            })
            .collect();

        // Different attempts should have different Cloud Task IDs
        assert_eq!(cloud_ids.len(), 2);
        assert_ne!(cloud_ids[0], cloud_ids[1]);
    }
}
