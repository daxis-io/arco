//! Backfill controller for run-per-chunk backfill with pause/resume/cancel.
//!
//! This controller manages the backfill lifecycle:
//! - **Preview**: Calculate partition count and chunks before creating
//! - **Create**: Emit `BackfillCreated` with compact `PartitionSelector`
//! - **Reconcile**: Plan chunks respecting concurrency limits
//! - **State transitions**: Pause, resume, cancel
//! - **Retry-failed**: Create new backfill from failed partitions
//!
//! ## Event Flow
//!
//! ```text
//! User Request → BackfillController.create()
//!                       ↓
//!                 BackfillCreated (with PartitionSelector, not partition list)
//!                       ↓
//!                 Compactor → backfills projection
//!                       ↓
//!                 BackfillController.reconcile()
//!                       ↓
//!                 BackfillChunkPlanned + RunRequested (atomic batch)
//!                       ↓
//!                 Compactor → backfill_chunks, runs
//! ```
//!
//! ## Design Principles
//!
//! 1. **Compact payloads**: `BackfillCreated` uses `PartitionSelector`, not full partition list (P0-6)
//! 2. **Run-per-chunk**: Each chunk is a separate run for granular progress tracking
//! 3. **Concurrency control**: `max_concurrent_runs` limits active chunks
//! 4. **Atomic emission**: `BackfillChunkPlanned` and `RunRequested` in same batch (P0-1)

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, Utc};

use crate::orchestration::compactor::fold::{BackfillChunkRow, BackfillRow, RunRow};
use crate::orchestration::compactor::manifest::Watermarks;
use crate::orchestration::events::{
    BackfillState, ChunkState, OrchestrationEvent, OrchestrationEventData, PartitionSelector,
    SourceRef,
};

/// Resolves partitions for an asset across multiple partitioning schemes.
pub trait PartitionResolver: Send + Sync + std::fmt::Debug {
    /// Count partitions in a range.
    fn count_range(&self, asset_key: &str, start: &str, end: &str) -> u32;

    /// List partitions in a range with pagination.
    fn list_range_chunk(
        &self,
        asset_key: &str,
        start: &str,
        end: &str,
        offset: u32,
        limit: u32,
    ) -> Vec<String>;
}

/// Preview of a backfill before creation.
#[derive(Debug, Clone)]
pub struct BackfillPreview {
    /// Total number of partitions to materialize.
    pub total_partitions: u32,
    /// Total number of chunks (ceil(total_partitions / chunk_size)).
    pub total_chunks: u32,
    /// First chunk partition keys (for preview).
    pub first_chunk_partitions: Vec<String>,
    /// Estimated number of runs (equals total_chunks).
    pub estimated_runs: u32,
}

/// Request to create a new backfill.
#[derive(Debug, Clone)]
pub struct CreateBackfillRequest<'a> {
    /// Backfill identifier (ULID).
    pub backfill_id: &'a str,
    /// Tenant identifier.
    pub tenant_id: &'a str,
    /// Workspace identifier.
    pub workspace_id: &'a str,
    /// Assets to backfill.
    pub asset_selection: &'a [String],
    /// Start of partition range (inclusive).
    pub partition_start: &'a str,
    /// End of partition range (inclusive).
    pub partition_end: &'a str,
    /// Number of partitions per chunk (0 = use default).
    pub chunk_size: u32,
    /// Maximum concurrent chunk runs (0 = use default).
    pub max_concurrent_runs: u32,
    /// Client request ID for idempotency.
    pub client_request_id: &'a str,
}

/// Error type for backfill operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BackfillError {
    /// Version conflict during optimistic concurrency checks.
    #[error("Version conflict: expected {expected}, found {actual}")]
    VersionConflict {
        /// Expected version.
        expected: u32,
        /// Actual version observed.
        actual: u32,
    },
    /// Invalid state transition for a backfill.
    #[error("Invalid state transition from {from:?} to {to:?}")]
    InvalidTransition {
        /// Previous state.
        from: BackfillState,
        /// Target state.
        to: BackfillState,
    },
    /// Backfill record could not be found.
    #[error("Backfill not found: {0}")]
    NotFound(String),
}

/// Controller for backfill lifecycle management.
#[derive(Debug)]
pub struct BackfillController {
    /// Default chunk size if not specified.
    default_chunk_size: u32,
    /// Default max concurrent runs if not specified.
    default_max_concurrent: u32,
    /// Maximum acceptable compaction lag for reconcile actions.
    max_compaction_lag: Duration,
    /// Partition resolver for counting and listing partitions.
    partition_resolver: Arc<dyn PartitionResolver>,
}

impl BackfillController {
    /// Creates a new backfill controller with the given partition resolver.
    #[must_use]
    pub fn new(partition_resolver: Arc<dyn PartitionResolver>) -> Self {
        Self {
            default_chunk_size: 10,
            default_max_concurrent: 2,
            max_compaction_lag: Duration::seconds(30),
            partition_resolver,
        }
    }

    /// Creates a new backfill controller with custom defaults.
    #[must_use]
    pub fn with_defaults(
        partition_resolver: Arc<dyn PartitionResolver>,
        default_chunk_size: u32,
        default_max_concurrent: u32,
        max_compaction_lag: Duration,
    ) -> Self {
        Self {
            default_chunk_size,
            default_max_concurrent,
            max_compaction_lag,
            partition_resolver,
        }
    }

    /// Preview a backfill before creating it.
    ///
    /// Returns partition count and chunk breakdown without creating any events.
    #[must_use]
    pub fn preview(
        &self,
        asset_key: &str,
        partition_start: &str,
        partition_end: &str,
        chunk_size: u32,
    ) -> BackfillPreview {
        let total_partitions = self
            .partition_resolver
            .count_range(asset_key, partition_start, partition_end);
        let chunk_size = if chunk_size == 0 {
            self.default_chunk_size
        } else {
            chunk_size
        };
        let total_chunks = total_partitions.div_ceil(chunk_size);
        let first_chunk = self.partition_resolver.list_range_chunk(
            asset_key,
            partition_start,
            partition_end,
            0,
            chunk_size,
        );

        BackfillPreview {
            total_partitions,
            total_chunks,
            first_chunk_partitions: first_chunk,
            estimated_runs: total_chunks,
        }
    }

    /// Create a backfill from a partition range.
    ///
    /// Returns a `BackfillCreated` event with a compact `PartitionSelector`.
    /// Per P0-6, the event stores the range, not the full partition list.
    #[must_use]
    pub fn create(&self, request: &CreateBackfillRequest<'_>) -> OrchestrationEvent {
        let asset_key = request
            .asset_selection
            .first()
            .map_or("", String::as_str);
        let total_partitions = self.partition_resolver.count_range(
            asset_key,
            request.partition_start,
            request.partition_end,
        );

        let chunk_size = if request.chunk_size == 0 {
            self.default_chunk_size
        } else {
            request.chunk_size
        };
        let max_concurrent_runs = if request.max_concurrent_runs == 0 {
            self.default_max_concurrent
        } else {
            request.max_concurrent_runs
        };

        OrchestrationEvent::new(
            request.tenant_id,
            request.workspace_id,
            OrchestrationEventData::BackfillCreated {
                backfill_id: request.backfill_id.to_string(),
                client_request_id: request.client_request_id.to_string(),
                asset_selection: request.asset_selection.to_vec(),
                partition_selector: PartitionSelector::Range {
                    start: request.partition_start.to_string(),
                    end: request.partition_end.to_string(),
                },
                total_partitions,
                chunk_size,
                max_concurrent_runs,
                parent_backfill_id: None,
            },
        )
    }

    /// Reconcile backfills and plan next chunks.
    ///
    /// Returns events for chunks that should be planned, respecting concurrency limits.
    /// Per P0-1, each `BackfillChunkPlanned` is emitted with its `RunRequested` atomically.
    ///
    /// Checks watermark freshness before planning - if compaction is lagging, returns
    /// empty to avoid making decisions on stale state.
    #[must_use]
    pub fn reconcile(
        &self,
        watermarks: &Watermarks,
        backfills: &HashMap<String, BackfillRow>,
        chunks: &HashMap<String, BackfillChunkRow>,
        runs: &HashMap<String, RunRow>,
    ) -> Vec<OrchestrationEvent> {
        // Check watermark freshness - critical guard!
        if !watermarks.is_fresh(self.max_compaction_lag) {
            return Vec::new();
        }

        let mut events = Vec::new();

        for (backfill_id, backfill) in backfills {
            if backfill.state != BackfillState::Running {
                continue;
            }

            // Count active chunks
            let active_chunks = self.count_active_chunks(backfill_id, chunks, runs);

            // Plan more chunks if below concurrency limit
            let chunks_to_plan = backfill.max_concurrent_runs.saturating_sub(active_chunks);
            let total_chunks = self.total_chunks(backfill);

            for i in 0..chunks_to_plan {
                let next_chunk_index = backfill.planned_chunks + i;
                if next_chunk_index >= total_chunks {
                    break;
                }

                let partition_keys = self.get_chunk_partitions(backfill, next_chunk_index);
                let chunk_events = self.plan_chunk(
                    backfill_id,
                    next_chunk_index,
                    &partition_keys,
                    &backfill.asset_selection,
                    &backfill.tenant_id,
                    &backfill.workspace_id,
                );
                events.extend(chunk_events);
            }
        }

        events
    }

    /// Plan a single chunk.
    ///
    /// Returns `BackfillChunkPlanned` and `RunRequested` events for atomic emission.
    #[must_use]
    pub fn plan_chunk(
        &self,
        backfill_id: &str,
        chunk_index: u32,
        partition_keys: &[String],
        asset_selection: &[String],
        tenant_id: &str,
        workspace_id: &str,
    ) -> Vec<OrchestrationEvent> {
        let chunk_id = format!("{backfill_id}:{chunk_index}");
        let run_key = format!("backfill:{backfill_id}:chunk:{chunk_index}");
        let fingerprint = compute_request_fingerprint(partition_keys);
        let emitted_at = Utc::now();

        // Emit BackfillChunkPlanned + RunRequested atomically (P0-1)
        vec![
            OrchestrationEvent::new_with_timestamp(
                tenant_id,
                workspace_id,
                OrchestrationEventData::BackfillChunkPlanned {
                    backfill_id: backfill_id.to_string(),
                    chunk_id: chunk_id.clone(),
                    chunk_index,
                    partition_keys: partition_keys.to_vec(),
                    run_key: run_key.clone(),
                    request_fingerprint: fingerprint.clone(),
                },
                emitted_at,
            ),
            OrchestrationEvent::new_with_timestamp(
                tenant_id,
                workspace_id,
                OrchestrationEventData::RunRequested {
                    run_key,
                    request_fingerprint: fingerprint,
                    asset_selection: asset_selection.to_vec(),
                    partition_selection: Some(partition_keys.to_vec()),
                    trigger_source_ref: SourceRef::Backfill {
                        backfill_id: backfill_id.to_string(),
                        chunk_id,
                    },
                    labels: HashMap::new(),
                },
                emitted_at,
            ),
        ]
    }

    /// Pause a running backfill.
    #[must_use]
    pub fn pause(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        self.transition(
            backfill_id,
            current,
            BackfillState::Paused,
            tenant_id,
            workspace_id,
            None,
        )
    }

    /// Resume a paused backfill.
    #[must_use]
    pub fn resume(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        self.transition(
            backfill_id,
            current,
            BackfillState::Running,
            tenant_id,
            workspace_id,
            None,
        )
    }

    /// Cancel a backfill.
    #[must_use]
    pub fn cancel(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        self.transition(
            backfill_id,
            current,
            BackfillState::Cancelled,
            tenant_id,
            workspace_id,
            None,
        )
    }

    /// Pause with version check for idempotent operations.
    pub fn pause_with_version(
        &self,
        backfill_id: &str,
        expected_version: u32,
        current: &BackfillRow,
    ) -> Result<OrchestrationEvent, BackfillError> {
        if current.state_version != expected_version {
            return Err(BackfillError::VersionConflict {
                expected: expected_version,
                actual: current.state_version,
            });
        }

        Ok(self.pause(backfill_id, current, &current.tenant_id, &current.workspace_id))
    }

    /// Create a retry backfill from failed chunks.
    ///
    /// Creates a new backfill with `parent_backfill_id` set, containing only
    /// the failed partitions from the parent.
    #[must_use]
    pub fn retry_failed(
        &self,
        new_backfill_id: &str,
        parent: &BackfillRow,
        failed_chunks: &[BackfillChunkRow],
        retry_request_id: &str,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        // Collect only failed partitions
        let partition_keys: Vec<String> = failed_chunks
            .iter()
            .flat_map(|c| c.partition_keys.clone())
            .collect();

        let total_partitions = partition_keys.len() as u32;

        OrchestrationEvent::new_with_idempotency_key(
            tenant_id,
            workspace_id,
            OrchestrationEventData::BackfillCreated {
                backfill_id: new_backfill_id.to_string(),
                client_request_id: retry_request_id.to_string(),
                asset_selection: parent.asset_selection.clone(),
                partition_selector: PartitionSelector::Explicit { partition_keys },
                total_partitions,
                chunk_size: parent.chunk_size,
                max_concurrent_runs: parent.max_concurrent_runs,
                parent_backfill_id: Some(parent.backfill_id.clone()),
            },
            // Idempotency based on parent + retry_request_id
            format!("backfill_retry:{}:{}", parent.backfill_id, retry_request_id),
        )
    }

    fn transition(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        to_state: BackfillState,
        tenant_id: &str,
        workspace_id: &str,
        changed_by: Option<String>,
    ) -> OrchestrationEvent {
        let new_version = current.state_version + 1;

        OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::BackfillStateChanged {
                backfill_id: backfill_id.to_string(),
                from_state: current.state,
                to_state,
                state_version: new_version,
                changed_by,
            },
        )
    }

    fn count_active_chunks(
        &self,
        backfill_id: &str,
        chunks: &HashMap<String, BackfillChunkRow>,
        runs: &HashMap<String, RunRow>,
    ) -> u32 {
        chunks
            .values()
            .filter(|c| c.backfill_id == backfill_id)
            .filter(|c| {
                matches!(c.state, ChunkState::Planned | ChunkState::Running)
                    || c.run_id
                        .as_ref()
                        .map(|id| runs.get(id).map(|r| !r.state.is_terminal()).unwrap_or(false))
                        .unwrap_or(false)
            })
            .count() as u32
    }

    fn total_chunks(&self, backfill: &BackfillRow) -> u32 {
        backfill.total_partitions.div_ceil(backfill.chunk_size)
    }

    fn get_chunk_partitions(&self, backfill: &BackfillRow, chunk_index: u32) -> Vec<String> {
        let offset = chunk_index * backfill.chunk_size;
        let limit = backfill.chunk_size;
        let asset_key = backfill.asset_selection.first().map_or("", String::as_str);

        match &backfill.partition_selector {
            PartitionSelector::Range { start, end } => {
                self.partition_resolver
                    .list_range_chunk(asset_key, start, end, offset, limit)
            }
            PartitionSelector::Explicit { partition_keys } => partition_keys
                .iter()
                .skip(offset as usize)
                .take(limit as usize)
                .cloned()
                .collect(),
            PartitionSelector::Filter { .. } => {
                // Filter-based partition resolution would go here
                // For now, return empty (filters need external resolution)
                Vec::new()
            }
        }
    }
}

/// Compute a fingerprint for request deduplication.
fn compute_request_fingerprint(partition_keys: &[String]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    for key in partition_keys {
        hasher.update(key.as_bytes());
        hasher.update(b"|");
    }
    let result = hasher.finalize();
    // Use first 16 bytes (128 bits) of the hash - SHA-256 always produces 32 bytes
    let bytes: [u8; 16] = result
        .get(..16)
        .and_then(|s| s.try_into().ok())
        .unwrap_or([0u8; 16]);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, Utc};

    /// Test partition resolver that generates daily partitions.
    #[derive(Debug)]
    struct TestPartitionResolver;

    impl PartitionResolver for TestPartitionResolver {
        fn count_range(&self, _asset_key: &str, start: &str, end: &str) -> u32 {
            let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap();
            let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").unwrap();
            (end - start).num_days() as u32 + 1
        }

        fn list_range_chunk(
            &self,
            _asset_key: &str,
            start: &str,
            end: &str,
            offset: u32,
            limit: u32,
        ) -> Vec<String> {
            let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap();
            let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").unwrap();

            (0..)
                .map(|i| start + Duration::days(i))
                .take_while(|d| *d <= end)
                .skip(offset as usize)
                .take(limit as usize)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .collect()
        }
    }

    fn test_partition_resolver() -> Arc<dyn PartitionResolver> {
        Arc::new(TestPartitionResolver)
    }

    fn default_backfill_row() -> BackfillRow {
        BackfillRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            backfill_id: "bf_001".into(),
            asset_selection: vec!["analytics.daily".into()],
            partition_selector: PartitionSelector::Range {
                start: "2025-01-01".into(),
                end: "2025-01-10".into(),
            },
            chunk_size: 3,
            max_concurrent_runs: 2,
            state: BackfillState::Running,
            state_version: 1,
            total_partitions: 10,
            planned_chunks: 0,
            completed_chunks: 0,
            failed_chunks: 0,
            parent_backfill_id: None,
            created_at: Utc::now(),
            row_version: "row_01HQ".into(),
        }
    }

    fn fresh_watermarks() -> Watermarks {
        Watermarks {
            events_processed_through: Some("01HQ123".into()),
            last_processed_file: Some("ledger/2025-01-15/01HQ123.json".into()),
            last_processed_at: Utc::now() - Duration::seconds(5),
        }
    }

    fn stale_watermarks() -> Watermarks {
        Watermarks {
            events_processed_through: Some("01HQ100".into()),
            last_processed_file: Some("ledger/2025-01-15/01HQ100.json".into()),
            last_processed_at: Utc::now() - Duration::seconds(120), // 2 minutes stale
        }
    }

    // ========================================================================
    // Task 4.1: Backfill Preview Tests
    // ========================================================================

    #[test]
    fn test_backfill_preview_returns_partition_count_and_chunks() {
        let controller = BackfillController::new(test_partition_resolver());

        let preview = controller.preview("analytics.daily", "2025-01-01", "2025-01-31", 10);

        assert_eq!(preview.total_partitions, 31);
        assert_eq!(preview.total_chunks, 4); // ceil(31/10)
        assert_eq!(preview.first_chunk_partitions.len(), 10);
        assert_eq!(preview.estimated_runs, 4);
    }

    #[test]
    fn test_backfill_preview_uses_default_chunk_size() {
        let controller = BackfillController::new(test_partition_resolver());

        let preview = controller.preview("analytics.daily", "2025-01-01", "2025-01-10", 0);

        // Default chunk size is 10, so 10 partitions = 1 chunk
        assert_eq!(preview.total_partitions, 10);
        assert_eq!(preview.total_chunks, 1);
    }

    // ========================================================================
    // Task 4.2: Backfill Creation Tests
    // ========================================================================

    #[test]
    fn test_backfill_create_emits_backfill_created_event() {
        let controller = BackfillController::new(test_partition_resolver());
        let assets = vec!["analytics.daily".into()];

        let event = controller.create(&CreateBackfillRequest {
            backfill_id: "bf_01HQ123",
            tenant_id: "tenant-abc",
            workspace_id: "workspace-prod",
            asset_selection: &assets,
            partition_start: "2025-01-01",
            partition_end: "2025-01-31",
            chunk_size: 10,
            max_concurrent_runs: 2,
            client_request_id: "client_req_001",
        });

        if let OrchestrationEventData::BackfillCreated {
            backfill_id,
            partition_selector,
            total_partitions,
            chunk_size,
            max_concurrent_runs,
            parent_backfill_id,
            ..
        } = &event.data
        {
            assert_eq!(backfill_id, "bf_01HQ123");
            assert_eq!(*total_partitions, 31);
            assert!(matches!(
                partition_selector,
                PartitionSelector::Range { start, end }
                if start == "2025-01-01" && end == "2025-01-31"
            ));
            assert_eq!(*chunk_size, 10);
            assert_eq!(*max_concurrent_runs, 2);
            assert!(parent_backfill_id.is_none());
        } else {
            panic!("Expected BackfillCreated event");
        }
    }

    #[test]
    fn test_backfill_create_uses_compact_selector_not_partition_list() {
        let controller = BackfillController::new(test_partition_resolver());
        let assets = vec!["analytics.daily".into()];

        let event = controller.create(&CreateBackfillRequest {
            backfill_id: "bf_001",
            tenant_id: "tenant-abc",
            workspace_id: "workspace-prod",
            asset_selection: &assets,
            partition_start: "2025-01-01",
            partition_end: "2025-12-31", // 365 partitions
            chunk_size: 10,
            max_concurrent_runs: 2,
            client_request_id: "client_001",
        });

        // Per P0-6: should use Range selector, not Explicit with 365 partition keys
        if let OrchestrationEventData::BackfillCreated {
            partition_selector, ..
        } = &event.data
        {
            assert!(
                matches!(partition_selector, PartitionSelector::Range { .. }),
                "Should use Range selector for compact storage"
            );
        }
    }

    // ========================================================================
    // Task 4.3: Backfill Chunk Planning Tests
    // ========================================================================

    #[test]
    fn test_backfill_controller_plans_chunks_respecting_concurrency() {
        let controller = BackfillController::new(test_partition_resolver());

        let mut backfills = HashMap::new();
        backfills.insert("bf_001".into(), default_backfill_row());

        let chunks = HashMap::new();
        let runs = HashMap::new();
        let watermarks = fresh_watermarks();

        let events = controller.reconcile(&watermarks, &backfills, &chunks, &runs);

        // Should plan 2 chunks (max_concurrent) with 2 events each (chunk + run)
        let chunk_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(&e.data, OrchestrationEventData::BackfillChunkPlanned { .. }))
            .collect();

        assert_eq!(chunk_events.len(), 2, "Should plan max_concurrent chunks");
    }

    #[test]
    fn test_backfill_chunk_run_key_is_deterministic() {
        let controller = BackfillController::new(test_partition_resolver());

        let events = controller.plan_chunk(
            "bf_001",
            0,
            &["2025-01-01".into(), "2025-01-02".into()],
            &["analytics.daily".into()],
            "tenant-abc",
            "workspace-prod",
        );

        let chunk_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::BackfillChunkPlanned { .. }))
            .expect("Should have chunk event");

        if let OrchestrationEventData::BackfillChunkPlanned {
            run_key, chunk_id, ..
        } = &chunk_event.data
        {
            assert_eq!(chunk_id, "bf_001:0");
            assert_eq!(run_key, "backfill:bf_001:chunk:0");
        }
    }

    #[test]
    fn test_backfill_chunk_emits_run_requested_atomically() {
        let controller = BackfillController::new(test_partition_resolver());

        let events = controller.plan_chunk(
            "bf_001",
            0,
            &["2025-01-01".into()],
            &["analytics.daily".into()],
            "tenant-abc",
            "workspace-prod",
        );

        // Per P0-1: Both events should be in the same batch
        let has_chunk = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::BackfillChunkPlanned { .. }));
        let has_run = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }));

        assert!(has_chunk, "Should emit BackfillChunkPlanned");
        assert!(has_run, "Should emit RunRequested atomically");
        assert_eq!(events.len(), 2, "Should emit exactly 2 events");
    }

    // ========================================================================
    // Task 4.4: Backfill State Transitions Tests
    // ========================================================================

    #[test]
    fn test_backfill_pause_uses_state_version_for_idempotency() {
        let controller = BackfillController::new(test_partition_resolver());

        let current_state = BackfillRow {
            backfill_id: "bf_001".into(),
            state: BackfillState::Running,
            state_version: 2,
            ..default_backfill_row()
        };

        let event = controller.pause("bf_001", &current_state, "tenant-abc", "workspace-prod");

        if let OrchestrationEventData::BackfillStateChanged {
            from_state,
            to_state,
            state_version,
            ..
        } = &event.data
        {
            assert_eq!(*from_state, BackfillState::Running);
            assert_eq!(*to_state, BackfillState::Paused);
            assert_eq!(*state_version, 3); // Incremented
        } else {
            panic!("Expected BackfillStateChanged event");
        }

        // Idempotency key uses state_version
        assert!(
            event.idempotency_key.contains("backfill_state:bf_001:3"),
            "Idempotency key should contain version: {}",
            event.idempotency_key
        );
    }

    #[test]
    fn test_backfill_pause_is_idempotent_with_expected_version() {
        let controller = BackfillController::new(test_partition_resolver());

        let current = BackfillRow {
            state_version: 2,
            state: BackfillState::Running,
            ..default_backfill_row()
        };

        // Pause request with expected_version=2 (current is 2)
        let result = controller.pause_with_version("bf_001", 2, &current);
        assert!(result.is_ok());

        // Pause request with expected_version=1 (current is 2) - conflict
        let result = controller.pause_with_version("bf_001", 1, &current);
        assert!(matches!(result, Err(BackfillError::VersionConflict { .. })));
    }

    #[test]
    fn test_backfill_resume_from_paused() {
        let controller = BackfillController::new(test_partition_resolver());

        let current = BackfillRow {
            state: BackfillState::Paused,
            state_version: 3,
            ..default_backfill_row()
        };

        let event = controller.resume("bf_001", &current, "tenant-abc", "workspace-prod");

        if let OrchestrationEventData::BackfillStateChanged {
            from_state,
            to_state,
            state_version,
            ..
        } = &event.data
        {
            assert_eq!(*from_state, BackfillState::Paused);
            assert_eq!(*to_state, BackfillState::Running);
            assert_eq!(*state_version, 4);
        }
    }

    #[test]
    fn test_backfill_cancel() {
        let controller = BackfillController::new(test_partition_resolver());

        let current = BackfillRow {
            state: BackfillState::Running,
            state_version: 2,
            ..default_backfill_row()
        };

        let event = controller.cancel("bf_001", &current, "tenant-abc", "workspace-prod");

        if let OrchestrationEventData::BackfillStateChanged {
            to_state,
            state_version,
            ..
        } = &event.data
        {
            assert_eq!(*to_state, BackfillState::Cancelled);
            assert_eq!(*state_version, 3);
        }
    }

    // ========================================================================
    // Task 4.5: Retry-Failed Backfill Tests
    // ========================================================================

    #[test]
    fn test_retry_failed_creates_new_backfill_with_parent() {
        let controller = BackfillController::new(test_partition_resolver());

        let parent = BackfillRow {
            backfill_id: "bf_001".into(),
            state: BackfillState::Failed,
            ..default_backfill_row()
        };

        let failed_chunks = vec![
            BackfillChunkRow {
                tenant_id: "tenant-abc".into(),
                workspace_id: "workspace-prod".into(),
                chunk_id: "bf_001:2".into(),
                backfill_id: "bf_001".into(),
                chunk_index: 2,
                state: ChunkState::Failed,
                partition_keys: vec!["2025-01-03".into()],
                run_key: "backfill:bf_001:chunk:2".into(),
                run_id: None,
                row_version: "row_001".into(),
            },
            BackfillChunkRow {
                tenant_id: "tenant-abc".into(),
                workspace_id: "workspace-prod".into(),
                chunk_id: "bf_001:5".into(),
                backfill_id: "bf_001".into(),
                chunk_index: 5,
                state: ChunkState::Failed,
                partition_keys: vec!["2025-01-06".into(), "2025-01-07".into()],
                run_key: "backfill:bf_001:chunk:5".into(),
                run_id: None,
                row_version: "row_002".into(),
            },
        ];

        let event = controller.retry_failed(
            "bf_002",
            &parent,
            &failed_chunks,
            "retry_req_001",
            "tenant-abc",
            "workspace-prod",
        );

        if let OrchestrationEventData::BackfillCreated {
            backfill_id,
            partition_selector,
            parent_backfill_id,
            total_partitions,
            ..
        } = &event.data
        {
            assert_eq!(backfill_id, "bf_002");
            assert_eq!(*total_partitions, 3); // 1 + 2 failed partitions

            // Only failed partitions
            if let PartitionSelector::Explicit { partition_keys } = partition_selector {
                assert_eq!(partition_keys.len(), 3);
                assert!(partition_keys.contains(&"2025-01-03".to_string()));
                assert!(partition_keys.contains(&"2025-01-06".to_string()));
                assert!(partition_keys.contains(&"2025-01-07".to_string()));
            } else {
                panic!("Expected explicit partition selector");
            }

            // Links to parent
            assert_eq!(parent_backfill_id, &Some("bf_001".to_string()));
        }
    }

    #[test]
    fn test_retry_failed_is_idempotent_with_retry_request_id() {
        let controller = BackfillController::new(test_partition_resolver());

        let parent = BackfillRow {
            backfill_id: "bf_001".into(),
            state: BackfillState::Failed,
            ..default_backfill_row()
        };

        let failed_chunks = vec![BackfillChunkRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            chunk_id: "bf_001:2".into(),
            backfill_id: "bf_001".into(),
            chunk_index: 2,
            state: ChunkState::Failed,
            partition_keys: vec!["2025-01-03".into()],
            run_key: "backfill:bf_001:chunk:2".into(),
            run_id: None,
            row_version: "row_001".into(),
        }];

        let event1 = controller.retry_failed(
            "bf_002",
            &parent,
            &failed_chunks,
            "retry_req_001",
            "tenant-abc",
            "workspace-prod",
        );

        let event2 = controller.retry_failed(
            "bf_003", // Different ID, same request
            &parent,
            &failed_chunks,
            "retry_req_001", // Same request ID
            "tenant-abc",
            "workspace-prod",
        );

        // Idempotency key based on parent + retry_request_id
        assert!(event1.idempotency_key.contains("retry_req_001"));
        assert_eq!(event1.idempotency_key, event2.idempotency_key);
    }

    #[test]
    fn test_backfill_skip_non_running() {
        let controller = BackfillController::new(test_partition_resolver());

        let mut backfills = HashMap::new();
        backfills.insert(
            "bf_paused".into(),
            BackfillRow {
                backfill_id: "bf_paused".into(),
                state: BackfillState::Paused,
                ..default_backfill_row()
            },
        );

        let watermarks = fresh_watermarks();
        let events = controller.reconcile(&watermarks, &backfills, &HashMap::new(), &HashMap::new());

        assert!(events.is_empty(), "Should not plan chunks for paused backfill");
    }

    #[test]
    fn test_backfill_reconcile_skips_on_stale_watermarks() {
        let controller = BackfillController::new(test_partition_resolver());

        let mut backfills = HashMap::new();
        backfills.insert("bf_001".into(), default_backfill_row());

        let watermarks = stale_watermarks();
        let events = controller.reconcile(&watermarks, &backfills, &HashMap::new(), &HashMap::new());

        assert!(
            events.is_empty(),
            "Should not plan chunks when compaction is lagging"
        );
    }
}
