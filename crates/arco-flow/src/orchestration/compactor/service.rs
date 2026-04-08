//! Micro-compactor service for near-real-time Parquet visibility.
//!
//! The compactor processes orchestration events from the ledger and produces
//! Parquet projections that controllers read. This follows ADR-020's architecture:
//!
//! ```text
//! Ledger → MicroCompactor → Parquet (L0 deltas) → Manifest → Controllers
//! ```
//!
//! The compactor is the sole writer of Parquet files (IAM-enforced).

use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::time::{Duration, Instant};
use ulid::Ulid;

use arco_core::publish::{
    SnapshotPointerDurability, SnapshotPointerPublishOutcome, publish_snapshot_pointer_transaction,
};
use arco_core::{DistributedLock, ScopedStorage, WritePrecondition, WriteResult};
use metrics::{counter, gauge, histogram};

use crate::error::{Error, Result};
use crate::metrics::{labels as metric_labels, names as metric_names};
use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use crate::paths::{
    orchestration_base_snapshot_dir, orchestration_compaction_lock_path, orchestration_l0_dir,
    orchestration_manifest_path, orchestration_manifest_pointer_path,
    orchestration_manifest_snapshot_path,
};

use super::fold::{
    FoldState, merge_backfill_chunk_rows, merge_backfill_rows, merge_dep_satisfaction_rows,
    merge_dispatch_outbox_rows, merge_idempotency_key_rows, merge_partition_status_rows,
    merge_run_rows, merge_schedule_definition_rows, merge_schedule_state_rows,
    merge_schedule_tick_rows, merge_sensor_eval_rows, merge_sensor_state_rows, merge_task_rows,
    merge_timer_rows,
};
use super::manifest::{
    BaseSnapshot, EventRange, L0Delta, LedgerRebuildManifest, OrchestrationManifest,
    OrchestrationManifestPointer, RowCounts, TableArtifact, TablePaths, Watermarks,
    next_manifest_id,
};
use super::parquet_util::{
    read_partition_status, write_backfill_chunks, write_backfills, write_dep_satisfaction,
    write_dispatch_outbox, write_idempotency_keys, write_partition_status, write_run_key_conflicts,
    write_run_key_index, write_runs, write_schedule_definitions, write_schedule_state,
    write_schedule_ticks, write_sensor_evals, write_sensor_state, write_tasks, write_timers,
};

const SENSOR_EVAL_RETENTION_DAYS: i64 = 30;
const IDEMPOTENCY_KEY_RETENTION_DAYS: i64 = 30;
const COMPACTION_PUBLISH_RETRY_DELAYS_MS: [u64; 3] = [0, 5, 25];

/// Internal durability mode for orchestration compaction acknowledgements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// Acknowledge after pointer CAS publish (state is read-visible).
    Visible,
    /// Acknowledge after immutable manifest snapshot write (may not be visible yet).
    Persisted,
}

/// Visibility outcome for a compaction publish attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionVisibility {
    /// Pointer CAS publish succeeded; state is visible to readers.
    Visible,
    /// Immutable snapshot persisted but pointer CAS lost; state is not visible.
    PersistedNotVisible,
}

impl CompactionVisibility {
    /// Returns a stable string label for API responses and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::PersistedNotVisible => "persisted_not_visible",
        }
    }
}

type PublishOutcome = CompactionVisibility;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishRetryReason {
    SnapshotConflict,
    ConcurrentWrite,
}

impl PublishRetryReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotConflict => "snapshot_conflict",
            Self::ConcurrentWrite => "concurrent_write",
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum PublishManifestError {
    #[error("storage error: immutable manifest snapshot already exists: {snapshot_path}")]
    SnapshotConflict { snapshot_path: String },
    #[error("storage error: manifest publish failed - concurrent write")]
    ConcurrentWrite,
    #[error(transparent)]
    Other(#[from] Error),
}

const STALE_FENCING_PUBLISH_MARKER: &str = "orchestration_publish:stale_fencing";
const FENCING_LOCK_UNAVAILABLE_PUBLISH_MARKER: &str =
    "orchestration_publish:fencing_lock_unavailable";

impl PublishManifestError {
    const fn retry_reason(&self) -> Option<PublishRetryReason> {
        match self {
            Self::SnapshotConflict { .. } => Some(PublishRetryReason::SnapshotConflict),
            Self::ConcurrentWrite => Some(PublishRetryReason::ConcurrentWrite),
            Self::Other(_) => None,
        }
    }
}

impl From<PublishManifestError> for Error {
    fn from(value: PublishManifestError) -> Self {
        match value {
            PublishManifestError::SnapshotConflict { snapshot_path } => Self::storage(format!(
                "immutable manifest snapshot already exists: {snapshot_path}"
            )),
            PublishManifestError::ConcurrentWrite => {
                Self::storage("manifest publish failed - concurrent write")
            }
            PublishManifestError::Other(error) => error,
        }
    }
}

impl From<arco_core::error::Error> for PublishManifestError {
    fn from(value: arco_core::error::Error) -> Self {
        Self::Other(Error::from(value))
    }
}

/// Micro-compactor for orchestration events.
///
/// Processes events from the ledger and writes L0 delta Parquet files.
/// Controllers read base snapshot + L0 deltas from the manifest.
#[derive(Clone)]
pub struct MicroCompactor {
    storage: ScopedStorage,
    tenant_secret: Vec<u8>,
    durability_mode: DurabilityMode,
}

/// Result of a micro-compaction run.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of events processed.
    pub events_processed: u32,
    /// Delta ID if any state was written.
    pub delta_id: Option<String>,
    /// New manifest revision.
    pub manifest_revision: String,
    /// Visibility outcome for this compaction acknowledgement.
    pub visibility_status: CompactionVisibility,
}

impl MicroCompactor {
    /// Creates a new micro-compactor.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            tenant_secret: Vec::new(),
            durability_mode: DurabilityMode::Visible,
        }
    }

    /// Creates a new micro-compactor with a tenant HMAC secret.
    #[must_use]
    pub fn with_tenant_secret(storage: ScopedStorage, tenant_secret: Vec<u8>) -> Self {
        Self {
            storage,
            tenant_secret,
            durability_mode: DurabilityMode::Visible,
        }
    }

    /// Sets internal durability mode for compaction acknowledgements.
    #[must_use]
    pub const fn with_durability_mode(mut self, durability_mode: DurabilityMode) -> Self {
        self.durability_mode = durability_mode;
        self
    }

    /// Loads the current manifest and fold state (base snapshot + L0 deltas).
    ///
    /// This is intended for read-only consumers (API/controllers) that need a
    /// consistent view of orchestration state.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest or Parquet files cannot be read.
    pub async fn load_state(&self) -> Result<(OrchestrationManifest, FoldState)> {
        let (manifest, _, _, _) = self.read_manifest_with_version().await?;
        let mut state = self.load_current_state(&manifest).await?;
        state.tenant_secret.clone_from(&self.tenant_secret);
        Ok((manifest, state))
    }

    /// Runs micro-compaction for explicit event paths.
    ///
    /// This is the sync compaction path - events are passed explicitly,
    /// not discovered via listing. Used when API holds a distributed lock.
    ///
    /// # Arguments
    /// * `event_paths` - Explicit paths to event files to process
    ///
    /// # Returns
    /// Compaction result with events processed and new manifest revision.
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads/writes or Parquet encoding fail.
    pub async fn compact_events(&self, event_paths: Vec<String>) -> Result<CompactionResult> {
        self.compact_events_internal(event_paths, None, None).await
    }

    /// Rebuilds orchestration projection state from an explicit ledger manifest.
    ///
    /// This path is deterministic and manifest-driven:
    /// - callers provide explicit ledger event paths
    /// - replay floor is derived from current manifest watermarks
    /// - no object-store listing is required for correctness
    ///
    /// # Errors
    ///
    /// Returns an error if rebuild manifest paths are invalid or compaction fails.
    pub async fn rebuild_from_ledger_manifest(
        &self,
        rebuild_manifest: LedgerRebuildManifest,
        expected_epoch: Option<u64>,
    ) -> Result<CompactionResult> {
        let (manifest, _, _, _) = self.read_manifest_with_version().await?;
        let event_paths = rebuild_manifest
            .event_paths_after_watermark(&manifest.watermarks)
            .map_err(|message| Error::Core(arco_core::Error::InvalidInput(message)))?;
        self.compact_events_with_epoch(event_paths, expected_epoch)
            .await
    }

    /// Rebuilds orchestration projection state from a stored rebuild manifest JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the rebuild manifest cannot be read/parsed or replay fails.
    pub async fn rebuild_from_ledger_manifest_path(
        &self,
        rebuild_manifest_path: &str,
        expected_epoch: Option<u64>,
    ) -> Result<CompactionResult> {
        let bytes = self.storage.get_raw(rebuild_manifest_path).await?;
        let rebuild_manifest: LedgerRebuildManifest =
            serde_json::from_slice(&bytes).map_err(|e| {
                Error::Core(arco_core::Error::InvalidInput(format!(
                    "failed to parse ledger rebuild manifest at {rebuild_manifest_path}: {e}"
                )))
            })?;
        self.rebuild_from_ledger_manifest(rebuild_manifest, expected_epoch)
            .await
    }

    /// Fenced compaction path for callers that hold the orchestration lock.
    ///
    /// This validates the canonical lock path, current lock holder, and supplied
    /// fencing token before work starts and again immediately before pointer CAS.
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads/writes fail, Parquet encoding fails,
    /// or the supplied fencing token is stale.
    pub async fn compact_events_fenced(
        &self,
        event_paths: Vec<String>,
        fencing_token: u64,
        lock_path: &str,
    ) -> Result<CompactionResult> {
        self.compact_events_internal(event_paths, Some(fencing_token), Some(lock_path))
            .await
    }

    /// Runs micro-compaction and validates the caller's lock epoch when supplied.
    ///
    /// # Arguments
    /// * `event_paths` - Explicit paths to event files to process
    /// * `expected_epoch` - Optional lock epoch associated with this write attempt
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads/writes fail, Parquet encoding fails,
    /// or the supplied epoch is stale relative to the current pointer epoch.
    #[tracing::instrument(skip(self, event_paths), fields(event_count = event_paths.len()))]
    #[allow(clippy::too_many_lines)]
    pub async fn compact_events_with_epoch(
        &self,
        event_paths: Vec<String>,
        expected_epoch: Option<u64>,
    ) -> Result<CompactionResult> {
        self.compact_events_internal(event_paths, expected_epoch, None)
            .await
    }

    #[tracing::instrument(skip(self, event_paths), fields(event_count = event_paths.len()))]
    #[allow(clippy::too_many_lines)]
    async fn compact_events_internal(
        &self,
        event_paths: Vec<String>,
        expected_epoch: Option<u64>,
        lock_path: Option<&str>,
    ) -> Result<CompactionResult> {
        let ack_started = Instant::now();
        'retry: for retry_attempt in 0..=COMPACTION_PUBLISH_RETRY_DELAYS_MS.len() {
            self.validate_fencing_lock(expected_epoch, lock_path)
                .await?;

            // Load current manifest + version for CAS
            let (mut manifest, pointer_version, previous_pointer, previous_pointer_bytes) =
                self.read_manifest_with_version().await?;
            let previous_manifest = manifest.clone();
            validate_expected_epoch(expected_epoch, previous_pointer.as_ref())?;

            if event_paths.is_empty() {
                let mut visibility_status = CompactionVisibility::Visible;
                if pointer_version.is_none() {
                    match self
                        .publish_manifest(
                            &manifest,
                            Some(&previous_manifest),
                            None,
                            previous_pointer.as_ref(),
                            previous_pointer_bytes.as_ref(),
                            expected_epoch,
                            lock_path,
                        )
                        .await
                    {
                        Ok(outcome) => visibility_status = outcome,
                        Err(publish_error) => {
                            if let Some(delay_ms) =
                                should_retry_publish_error(&publish_error, retry_attempt)
                            {
                                let Some(reason) = publish_error.retry_reason() else {
                                    debug_assert!(
                                        false,
                                        "retryable publish error must carry a retry reason"
                                    );
                                    wait_for_publish_retry(delay_ms).await;
                                    continue 'retry;
                                };
                                record_compaction_publish_retry(
                                    self.durability_mode,
                                    reason,
                                    retry_attempt + 1,
                                    delay_ms,
                                );
                                wait_for_publish_retry(delay_ms).await;
                                continue 'retry;
                            }
                            let error: Error = publish_error.into();
                            record_compaction_metrics(
                                self.durability_mode,
                                ack_started.elapsed(),
                                &manifest,
                                CompactionVisibility::Visible,
                                "error",
                            );
                            return Err(error);
                        }
                    }
                }
                record_compaction_metrics(
                    self.durability_mode,
                    ack_started.elapsed(),
                    &manifest,
                    visibility_status,
                    "success",
                );
                return Ok(CompactionResult {
                    events_processed: 0,
                    delta_id: None,
                    manifest_revision: manifest.revision_ulid,
                    visibility_status,
                });
            }

            // Load current fold state from base + L0 deltas
            let mut state = self.load_current_state(&manifest).await?;
            state.tenant_secret.clone_from(&self.tenant_secret);
            let base_state = state.clone();

            // Process events
            let mut events = Vec::new();
            for path in &event_paths {
                let data = self.storage.get_raw(path).await?;
                let event: OrchestrationEvent =
                    serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                        message: format!("failed to parse event at {path}: {e}"),
                    })?;
                events.push((path.clone(), event));
            }

            // Sort by timestamp with a stable ordering for atomic batches.
            events.sort_by(|a, b| {
                let a_event = &a.1;
                let b_event = &b.1;
                let ordering = a_event.timestamp.cmp(&b_event.timestamp);
                if ordering != Ordering::Equal {
                    return ordering;
                }
                let ordering = event_priority(&a_event.data).cmp(&event_priority(&b_event.data));
                if ordering != Ordering::Equal {
                    return ordering;
                }
                a_event.event_id.cmp(&b_event.event_id)
            });

            // Fold events into state
            for (_, event) in &events {
                state.fold_event(event);
            }

            let retention_now = retention_reference_time_for_events(&events);
            prune_sensor_evals(&mut state, retention_now);
            prune_idempotency_keys(&mut state, retention_now);

            // Compute delta state (rows changed by this batch)
            let delta_state = delta_from_states(&base_state, &state);
            let has_delta = !delta_state_is_empty(&delta_state);

            // Count rows
            let row_counts = RowCounts {
                runs: u32::try_from(delta_state.runs.len()).unwrap_or(u32::MAX),
                tasks: u32::try_from(delta_state.tasks.len()).unwrap_or(u32::MAX),
                dep_satisfaction: u32::try_from(delta_state.dep_satisfaction.len())
                    .unwrap_or(u32::MAX),
                timers: u32::try_from(delta_state.timers.len()).unwrap_or(u32::MAX),
                dispatch_outbox: u32::try_from(delta_state.dispatch_outbox.len())
                    .unwrap_or(u32::MAX),
                sensor_state: u32::try_from(delta_state.sensor_state.len()).unwrap_or(u32::MAX),
                sensor_evals: u32::try_from(delta_state.sensor_evals.len()).unwrap_or(u32::MAX),
                run_key_index: u32::try_from(delta_state.run_key_index.len()).unwrap_or(u32::MAX),
                run_key_conflicts: u32::try_from(delta_state.run_key_conflicts.len())
                    .unwrap_or(u32::MAX),
                partition_status: u32::try_from(delta_state.partition_status.len())
                    .unwrap_or(u32::MAX),
                idempotency_keys: u32::try_from(delta_state.idempotency_keys.len())
                    .unwrap_or(u32::MAX),
                schedule_definitions: u32::try_from(delta_state.schedule_definitions.len())
                    .unwrap_or(u32::MAX),
                schedule_state: u32::try_from(delta_state.schedule_state.len()).unwrap_or(u32::MAX),
                schedule_ticks: u32::try_from(delta_state.schedule_ticks.len()).unwrap_or(u32::MAX),
                backfills: u32::try_from(delta_state.backfills.len()).unwrap_or(u32::MAX),
                backfill_chunks: u32::try_from(delta_state.backfill_chunks.len())
                    .unwrap_or(u32::MAX),
            };

            // Create L0 delta entry when changes exist
            let first_event = events
                .first()
                .map(|e| e.1.event_id.clone())
                .unwrap_or_default();
            let last_event = events
                .last()
                .map(|e| e.1.event_id.clone())
                .unwrap_or_default();
            let delta_created_at = Utc::now();
            let event_range = EventRange {
                from_event: first_event.clone(),
                to_event: last_event.clone(),
                event_count: u32::try_from(events.len()).unwrap_or(u32::MAX),
            };

            let should_merge_to_base = if has_delta {
                let mut projected_manifest = manifest.clone();
                projected_manifest.l0_deltas.push(L0Delta {
                    delta_id: "pending".to_string(),
                    created_at: delta_created_at,
                    event_range: event_range.clone(),
                    tables: TablePaths::default(),
                    row_counts: row_counts.clone(),
                });
                projected_manifest.l0_count += 1;
                projected_manifest.should_compact_l0()
            } else {
                manifest.should_compact_l0()
            };

            let had_existing_l0 = !manifest.l0_deltas.is_empty();
            let mut delta_id = None;
            let storage_changed = if should_merge_to_base {
                let snapshot_id = Ulid::new().to_string();
                let base_tables = match self.write_base_snapshot_parquet(&snapshot_id, &state).await
                {
                    Ok(paths) => paths,
                    Err(error) => {
                        if let Some(delay_ms) =
                            should_retry_l0_write_conflict(&error, retry_attempt)
                        {
                            wait_for_publish_retry(delay_ms).await;
                            continue 'retry;
                        }
                        return Err(error);
                    }
                };

                manifest.base_snapshot = BaseSnapshot {
                    snapshot_id: Some(snapshot_id),
                    published_at: Utc::now(),
                    tables: base_tables,
                };
                manifest.l0_deltas.clear();
                manifest.l0_count = 0;
                had_existing_l0 || has_delta
            } else if has_delta {
                let new_delta_id = Ulid::new().to_string();
                let delta_paths = match self.write_delta_parquet(&new_delta_id, &delta_state).await
                {
                    Ok(paths) => paths,
                    Err(error) => {
                        if let Some(delay_ms) =
                            should_retry_l0_write_conflict(&error, retry_attempt)
                        {
                            wait_for_publish_retry(delay_ms).await;
                            continue 'retry;
                        }
                        return Err(error);
                    }
                };
                manifest.l0_deltas.push(L0Delta {
                    delta_id: new_delta_id.clone(),
                    created_at: delta_created_at,
                    event_range,
                    tables: delta_paths,
                    row_counts,
                });
                manifest.l0_count += 1;
                delta_id = Some(new_delta_id);
                true
            } else {
                false
            };

            let watermark_changed =
                update_watermarks(&mut manifest, &events, &last_event, self.durability_mode);
            let manifest_changed = storage_changed || watermark_changed;

            let mut visibility_status = CompactionVisibility::Visible;
            let manifest_revision = if manifest_changed {
                let previous_manifest_path = previous_pointer.as_ref().map_or_else(
                    || orchestration_manifest_path().to_string(),
                    |p| p.manifest_path.clone(),
                );

                manifest.manifest_id =
                    next_manifest_id(&manifest.manifest_id).map_err(Error::serialization)?;
                manifest.previous_manifest_path = Some(previous_manifest_path);
                let pointer_epoch = previous_pointer.as_ref().map_or(0, |p| p.epoch);
                manifest.epoch = manifest.epoch.max(pointer_epoch);
                if let Some(expected_epoch) = expected_epoch {
                    manifest.epoch = manifest.epoch.max(expected_epoch);
                }

                let new_revision = Ulid::new().to_string();
                manifest.revision_ulid.clone_from(&new_revision);
                manifest.published_at = Utc::now();
                match self
                    .publish_manifest(
                        &manifest,
                        Some(&previous_manifest),
                        pointer_version.as_deref(),
                        previous_pointer.as_ref(),
                        previous_pointer_bytes.as_ref(),
                        expected_epoch,
                        lock_path,
                    )
                    .await
                {
                    Ok(outcome) => visibility_status = outcome,
                    Err(publish_error) => {
                        if let Some(delay_ms) =
                            should_retry_publish_error(&publish_error, retry_attempt)
                        {
                            let Some(reason) = publish_error.retry_reason() else {
                                debug_assert!(
                                    false,
                                    "retryable publish error must carry a retry reason"
                                );
                                wait_for_publish_retry(delay_ms).await;
                                continue 'retry;
                            };
                            record_compaction_publish_retry(
                                self.durability_mode,
                                reason,
                                retry_attempt + 1,
                                delay_ms,
                            );
                            wait_for_publish_retry(delay_ms).await;
                            continue 'retry;
                        }
                        let error: Error = publish_error.into();
                        record_compaction_metrics(
                            self.durability_mode,
                            ack_started.elapsed(),
                            &manifest,
                            CompactionVisibility::Visible,
                            "error",
                        );
                        return Err(error);
                    }
                }
                new_revision
            } else {
                manifest.revision_ulid.clone()
            };

            record_compaction_metrics(
                self.durability_mode,
                ack_started.elapsed(),
                &manifest,
                visibility_status,
                "success",
            );

            return Ok(CompactionResult {
                events_processed: u32::try_from(events.len()).unwrap_or(u32::MAX),
                delta_id,
                manifest_revision,
                visibility_status,
            });
        }

        unreachable!("bounded compaction publish retry loop must return or continue")
    }

    async fn validate_fencing_lock(
        &self,
        expected_epoch: Option<u64>,
        lock_path: Option<&str>,
    ) -> Result<()> {
        let (Some(expected_epoch), Some(lock_path)) = (expected_epoch, lock_path) else {
            return Ok(());
        };

        if lock_path != orchestration_compaction_lock_path() {
            return Err(Error::Core(arco_core::Error::InvalidInput(
                "invalid lock_path".to_string(),
            )));
        }

        let lock = DistributedLock::new(self.storage.backend().clone(), lock_path);
        let lock_info = lock.read_lock_info().await?;

        let Some(lock_info) = lock_info else {
            counter!(
                metric_names::ORCH_COMPACTOR_STALE_FENCING_REJECTS_TOTAL,
                metric_labels::REASON => "lock_missing".to_string(),
            )
            .increment(1);
            return Err(Error::FencingLockUnavailable {
                lock_path: lock_path.to_string(),
                provided: expected_epoch,
            });
        };

        if lock_info.is_expired() {
            counter!(
                metric_names::ORCH_COMPACTOR_STALE_FENCING_REJECTS_TOTAL,
                metric_labels::REASON => "lock_expired".to_string(),
            )
            .increment(1);
            return Err(Error::FencingLockUnavailable {
                lock_path: lock_path.to_string(),
                provided: expected_epoch,
            });
        }

        if lock_info.sequence_number != expected_epoch {
            counter!(
                metric_names::ORCH_COMPACTOR_STALE_FENCING_REJECTS_TOTAL,
                metric_labels::REASON => "stale_token".to_string(),
            )
            .increment(1);
            return Err(Error::StaleFencingToken {
                expected: lock_info.sequence_number,
                provided: expected_epoch,
            });
        }

        Ok(())
    }

    /// Reads the current manifest and pointer CAS version.
    async fn read_manifest_with_version(
        &self,
    ) -> Result<(
        OrchestrationManifest,
        Option<String>,
        Option<OrchestrationManifestPointer>,
        Option<Bytes>,
    )> {
        let pointer_path = orchestration_manifest_pointer_path();
        if let Some(meta) = self.storage.head_raw(pointer_path).await? {
            let pointer_data = self.storage.get_raw(pointer_path).await?;
            let pointer: OrchestrationManifestPointer = serde_json::from_slice(&pointer_data)
                .map_err(|e| Error::Serialization {
                    message: format!("failed to parse manifest pointer: {e}"),
                })?;

            let manifest_data = match self.storage.get_raw(&pointer.manifest_path).await {
                Ok(data) => data,
                Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                    self.storage.get_raw(orchestration_manifest_path()).await?
                }
                Err(e) => return Err(e.into()),
            };
            let mut manifest: OrchestrationManifest = serde_json::from_slice(&manifest_data)
                .map_err(|e| Error::Serialization {
                    message: format!("failed to parse manifest: {e}"),
                })?;

            manifest.manifest_id.clone_from(&pointer.manifest_id);
            manifest.epoch = manifest.epoch.max(pointer.epoch);

            return Ok((
                manifest,
                Some(meta.version),
                Some(pointer),
                Some(pointer_data),
            ));
        }

        let manifest_path = orchestration_manifest_path();
        match self.storage.head_raw(manifest_path).await? {
            Some(_) => {
                let data = self.storage.get_raw(manifest_path).await?;
                let manifest: OrchestrationManifest =
                    serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                        message: format!("failed to parse manifest: {e}"),
                    })?;
                Ok((manifest, None, None, None))
            }
            None => Ok((
                OrchestrationManifest::new(Ulid::new().to_string()),
                None,
                None,
                None,
            )),
        }
    }

    #[cfg(test)]
    async fn get_or_create_manifest(&self) -> Result<OrchestrationManifest> {
        let (manifest, _, _, _) = self.read_manifest_with_version().await?;
        Ok(manifest)
    }

    /// Loads current fold state from base snapshot + L0 deltas.
    async fn load_current_state(&self, manifest: &OrchestrationManifest) -> Result<FoldState> {
        let mut state = if let Some(ref snapshot_id) = manifest.base_snapshot.snapshot_id {
            self.load_snapshot_state(snapshot_id, &manifest.base_snapshot.tables)
                .await?
        } else {
            FoldState::new()
        };

        // Apply L0 deltas in order
        for delta in &manifest.l0_deltas {
            let delta_state = self
                .load_delta_state(&delta.delta_id, &delta.tables)
                .await?;
            state = merge_states(state, delta_state);
        }

        let retention_now = retention_reference_time_for_state(&state);
        if let Some(retention_now) = retention_now {
            prune_sensor_evals(&mut state, retention_now);
            prune_idempotency_keys(&mut state, retention_now);
        }
        state.rebuild_dependency_graph();

        Ok(state)
    }

    /// Loads state from a snapshot.
    #[allow(clippy::too_many_lines)]
    async fn load_snapshot_state(
        &self,
        _snapshot_id: &str,
        tables: &TablePaths,
    ) -> Result<FoldState> {
        let mut state = FoldState::new();

        if let Some(ref runs_path) = tables.runs {
            let data = self.storage.get_raw(runs_path.path()).await?;
            let rows = super::parquet_util::read_runs(&data)?;
            for row in rows {
                state.runs.insert(row.run_id.clone(), row);
            }
        }

        if let Some(ref tasks_path) = tables.tasks {
            let data = self.storage.get_raw(tasks_path.path()).await?;
            let rows = super::parquet_util::read_tasks(&data)?;
            for row in rows {
                state
                    .tasks
                    .insert((row.run_id.clone(), row.task_key.clone()), row);
            }
        }

        if let Some(ref deps_path) = tables.dep_satisfaction {
            let data = self.storage.get_raw(deps_path.path()).await?;
            let rows = super::parquet_util::read_dep_satisfaction(&data)?;
            for row in rows {
                state.dep_satisfaction.insert(
                    (
                        row.run_id.clone(),
                        row.upstream_task_key.clone(),
                        row.downstream_task_key.clone(),
                    ),
                    row,
                );
            }
        }

        if let Some(ref timers_path) = tables.timers {
            let data = self.storage.get_raw(timers_path.path()).await?;
            let rows = super::parquet_util::read_timers(&data)?;
            for row in rows {
                state.timers.insert(row.timer_id.clone(), row);
            }
        }

        if let Some(ref outbox_path) = tables.dispatch_outbox {
            let data = self.storage.get_raw(outbox_path.path()).await?;
            let rows = super::parquet_util::read_dispatch_outbox(&data)?;
            for row in rows {
                state.dispatch_outbox.insert(row.dispatch_id.clone(), row);
            }
        }

        if let Some(ref sensor_state_path) = tables.sensor_state {
            let data = self.storage.get_raw(sensor_state_path.path()).await?;
            let rows = super::parquet_util::read_sensor_state(&data)?;
            for row in rows {
                state.sensor_state.insert(row.sensor_id.clone(), row);
            }
        }

        if let Some(ref sensor_evals_path) = tables.sensor_evals {
            let data = self.storage.get_raw(sensor_evals_path.path()).await?;
            let rows = super::parquet_util::read_sensor_evals(&data)?;
            for row in rows {
                state.sensor_evals.insert(row.eval_id.clone(), row);
            }
        }

        if let Some(ref backfills_path) = tables.backfills {
            let data = self.storage.get_raw(backfills_path.path()).await?;
            let rows = super::parquet_util::read_backfills(&data)?;
            for row in rows {
                state.backfills.insert(row.backfill_id.clone(), row);
            }
        }

        if let Some(ref backfill_chunks_path) = tables.backfill_chunks {
            let data = self.storage.get_raw(backfill_chunks_path.path()).await?;
            let rows = super::parquet_util::read_backfill_chunks(&data)?;
            for row in rows {
                state.backfill_chunks.insert(row.chunk_id.clone(), row);
            }
        }

        if let Some(ref run_key_index_path) = tables.run_key_index {
            let data = self.storage.get_raw(run_key_index_path.path()).await?;
            let rows = super::parquet_util::read_run_key_index(&data)?;
            for row in rows {
                state.run_key_index.insert(row.run_key.clone(), row);
            }
        }

        if let Some(ref run_key_conflicts_path) = tables.run_key_conflicts {
            let data = self.storage.get_raw(run_key_conflicts_path.path()).await?;
            let rows = super::parquet_util::read_run_key_conflicts(&data)?;
            for row in rows {
                let conflict_id = format!("conflict:{}:{}", row.run_key, row.conflicting_event_id);
                state.run_key_conflicts.insert(conflict_id, row);
            }
        }

        if let Some(ref partition_status_path) = tables.partition_status {
            let data = self.storage.get_raw(partition_status_path.path()).await?;
            let rows = read_partition_status(&data)?;
            for row in rows {
                let key = (row.asset_key.clone(), row.partition_key.clone());
                state.partition_status.insert(key, row);
            }
        }

        if let Some(ref idempotency_keys_path) = tables.idempotency_keys {
            let data = self.storage.get_raw(idempotency_keys_path.path()).await?;
            let rows = super::parquet_util::read_idempotency_keys(&data)?;
            for row in rows {
                state
                    .idempotency_keys
                    .insert(row.idempotency_key.clone(), row);
            }
        }

        self.load_schedule_tables(&mut state, tables).await?;

        Ok(state)
    }

    async fn load_schedule_tables(&self, state: &mut FoldState, tables: &TablePaths) -> Result<()> {
        if let Some(ref schedule_definitions_path) = tables.schedule_definitions {
            let data = self
                .storage
                .get_raw(schedule_definitions_path.path())
                .await?;
            let rows = super::parquet_util::read_schedule_definitions(&data)?;
            for row in rows {
                state
                    .schedule_definitions
                    .insert(row.schedule_id.clone(), row);
            }
        }

        if let Some(ref schedule_state_path) = tables.schedule_state {
            let data = self.storage.get_raw(schedule_state_path.path()).await?;
            let rows = super::parquet_util::read_schedule_state(&data)?;
            for row in rows {
                state.schedule_state.insert(row.schedule_id.clone(), row);
            }
        }

        if let Some(ref schedule_ticks_path) = tables.schedule_ticks {
            let data = self.storage.get_raw(schedule_ticks_path.path()).await?;
            let rows = super::parquet_util::read_schedule_ticks(&data)?;
            for row in rows {
                state.schedule_ticks.insert(row.tick_id.clone(), row);
            }
        }

        Ok(())
    }

    /// Loads state from an L0 delta.
    async fn load_delta_state(&self, delta_id: &str, tables: &TablePaths) -> Result<FoldState> {
        // Same logic as snapshot loading
        self.load_snapshot_state(delta_id, tables).await
    }

    /// Writes fold state to L0 delta Parquet files.
    async fn write_delta_parquet(&self, delta_id: &str, state: &FoldState) -> Result<TablePaths> {
        self.write_state_parquet(&orchestration_l0_dir(delta_id), state)
            .await
    }

    /// Writes fold state to an immutable base snapshot directory.
    async fn write_base_snapshot_parquet(
        &self,
        snapshot_id: &str,
        state: &FoldState,
    ) -> Result<TablePaths> {
        self.write_state_parquet(&orchestration_base_snapshot_dir(snapshot_id), state)
            .await
    }

    async fn write_state_parquet(&self, base_path: &str, state: &FoldState) -> Result<TablePaths> {
        let mut paths = TablePaths::default();

        macro_rules! write_table {
            ($file:literal, $rows:expr, $encode:expr, $out:expr) => {
                self.write_map_table_if_nonempty(base_path, $file, $rows, $encode, $out)
                    .await?;
            };
        }

        write_table!("runs.parquet", &state.runs, write_runs, &mut paths.runs);
        write_table!("tasks.parquet", &state.tasks, write_tasks, &mut paths.tasks);
        write_table!(
            "dep_satisfaction.parquet",
            &state.dep_satisfaction,
            write_dep_satisfaction,
            &mut paths.dep_satisfaction
        );
        write_table!(
            "timers.parquet",
            &state.timers,
            write_timers,
            &mut paths.timers
        );
        write_table!(
            "dispatch_outbox.parquet",
            &state.dispatch_outbox,
            write_dispatch_outbox,
            &mut paths.dispatch_outbox
        );
        write_table!(
            "sensor_state.parquet",
            &state.sensor_state,
            write_sensor_state,
            &mut paths.sensor_state
        );
        write_table!(
            "sensor_evals.parquet",
            &state.sensor_evals,
            write_sensor_evals,
            &mut paths.sensor_evals
        );
        write_table!(
            "run_key_index.parquet",
            &state.run_key_index,
            write_run_key_index,
            &mut paths.run_key_index
        );
        write_table!(
            "run_key_conflicts.parquet",
            &state.run_key_conflicts,
            write_run_key_conflicts,
            &mut paths.run_key_conflicts
        );
        write_table!(
            "partition_status.parquet",
            &state.partition_status,
            write_partition_status,
            &mut paths.partition_status
        );
        write_table!(
            "idempotency_keys.parquet",
            &state.idempotency_keys,
            write_idempotency_keys,
            &mut paths.idempotency_keys
        );
        write_table!(
            "schedule_definitions.parquet",
            &state.schedule_definitions,
            write_schedule_definitions,
            &mut paths.schedule_definitions
        );
        write_table!(
            "schedule_state.parquet",
            &state.schedule_state,
            write_schedule_state,
            &mut paths.schedule_state
        );
        write_table!(
            "schedule_ticks.parquet",
            &state.schedule_ticks,
            write_schedule_ticks,
            &mut paths.schedule_ticks
        );
        write_table!(
            "backfills.parquet",
            &state.backfills,
            write_backfills,
            &mut paths.backfills
        );
        write_table!(
            "backfill_chunks.parquet",
            &state.backfill_chunks,
            write_backfill_chunks,
            &mut paths.backfill_chunks
        );

        Ok(paths)
    }

    async fn write_map_table_if_nonempty<K, R>(
        &self,
        base_path: &str,
        file: &str,
        rows: &std::collections::HashMap<K, R>,
        encode: fn(&[R]) -> Result<Bytes>,
        out: &mut Option<TableArtifact>,
    ) -> Result<()>
    where
        K: std::hash::Hash + Eq + Sync,
        R: Clone + Sync,
    {
        if rows.is_empty() {
            return Ok(());
        }

        let bytes = {
            let values: Vec<R> = rows.values().cloned().collect();
            encode(&values)?
        };

        let artifact = self.write_parquet_artifact(base_path, file, bytes).await?;
        *out = Some(artifact);
        Ok(())
    }

    async fn write_parquet_artifact(
        &self,
        base_path: &str,
        file: &str,
        bytes: Bytes,
    ) -> Result<TableArtifact> {
        let checksum_sha256 = sha256_prefixed(&bytes);
        let byte_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let path = hash_suffixed_artifact_path(base_path, file, &checksum_sha256);
        self.write_parquet_file(&path, bytes).await?;
        Ok(TableArtifact::new(path, checksum_sha256, byte_size))
    }

    /// Writes a Parquet file to storage.
    async fn write_parquet_file(&self, path: &str, bytes: Bytes) -> Result<()> {
        let result = self
            .storage
            .put_raw(path, bytes, WritePrecondition::DoesNotExist)
            .await?;

        match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                counter!(
                    metric_names::ORCH_COMPACTOR_OVERWRITE_REJECTS_TOTAL,
                    metric_labels::REASON => "artifact_exists".to_string(),
                )
                .increment(1);
                Err(Error::Core(arco_core::Error::PreconditionFailed {
                    message: format!("immutable parquet artifact already exists: {path}"),
                }))
            }
        }
    }

    /// Publishes a new manifest version.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn publish_manifest(
        &self,
        manifest: &OrchestrationManifest,
        previous_manifest: Option<&OrchestrationManifest>,
        current_pointer_version: Option<&str>,
        previous_pointer: Option<&OrchestrationManifestPointer>,
        previous_pointer_bytes: Option<&Bytes>,
        expected_epoch: Option<u64>,
        lock_path: Option<&str>,
    ) -> std::result::Result<PublishOutcome, PublishManifestError> {
        let snapshot_path = orchestration_manifest_snapshot_path(&manifest.manifest_id);
        let manifest_json =
            serde_json::to_string_pretty(manifest).map_err(|e| Error::Serialization {
                message: format!("failed to serialize manifest: {e}"),
            })?;

        let pointer_path = orchestration_manifest_pointer_path();
        let parent_pointer_hash = match (previous_pointer, previous_pointer_bytes) {
            (Some(_), Some(pointer_bytes)) => Some(sha256_prefixed(pointer_bytes)),
            (Some(_), None) => {
                return Err(Error::Core(arco_core::Error::Validation {
                    message: "missing raw pointer bytes for pointer succession validation"
                        .to_string(),
                })
                .into());
            }
            (None, _) => None,
        };

        let pointer = OrchestrationManifestPointer {
            manifest_id: manifest.manifest_id.clone(),
            manifest_path: snapshot_path.clone(),
            epoch: manifest.epoch,
            parent_pointer_hash: parent_pointer_hash.clone(),
            updated_at: Utc::now(),
        };
        let pointer_json =
            serde_json::to_string_pretty(&pointer).map_err(|e| Error::Serialization {
                message: format!("failed to serialize manifest pointer: {e}"),
            })?;

        let durability = match self.durability_mode {
            DurabilityMode::Visible => SnapshotPointerDurability::Visible,
            DurabilityMode::Persisted => SnapshotPointerDurability::Persisted,
        };
        let legacy_json =
            serde_json::to_string_pretty(manifest).map_err(|e| Error::Serialization {
                message: format!("failed to serialize legacy manifest: {e}"),
            })?;

        match publish_snapshot_pointer_transaction(
            &self.storage,
            &snapshot_path,
            Bytes::from(manifest_json.clone()),
            pointer_path,
            Bytes::from(pointer_json),
            current_pointer_version,
            Some((orchestration_manifest_path(), Bytes::from(legacy_json))),
            durability,
            async {
                self.validate_fencing_lock(expected_epoch, lock_path)
                    .await
                    .map_err(encode_pre_pointer_publish_error)?;

                if let (
                    Some(previous_manifest),
                    Some(previous_pointer),
                    Some(previous_pointer_hash),
                ) = (
                    previous_manifest,
                    previous_pointer,
                    parent_pointer_hash.as_deref(),
                ) {
                    manifest
                        .validate_succession(previous_manifest)
                        .map_err(|message| arco_core::Error::Validation { message })?;
                    pointer
                        .validate_succession(previous_pointer, previous_pointer_hash)
                        .map_err(|message| arco_core::Error::Validation { message })?;
                }

                Ok(())
            },
        )
        .await
        {
            Ok(SnapshotPointerPublishOutcome::Visible { .. }) => Ok(PublishOutcome::Visible),
            Ok(SnapshotPointerPublishOutcome::PersistedNotVisible) => {
                tracing::warn!(
                    manifest_id = %manifest.manifest_id,
                    "pointer CAS lost race in Persisted mode; returning persisted acknowledgement"
                );
                Ok(PublishOutcome::PersistedNotVisible)
            }
            Err(arco_core::Error::PreconditionFailed { message })
                if message
                    == format!("immutable manifest snapshot already exists: {snapshot_path}") =>
            {
                Err(PublishManifestError::SnapshotConflict { snapshot_path })
            }
            Err(arco_core::Error::PreconditionFailed { message })
                if message == "manifest publish failed - concurrent write" =>
            {
                Err(PublishManifestError::ConcurrentWrite)
            }
            Err(arco_core::Error::PreconditionFailed { message }) => decode_pre_pointer_publish_error(
                &message,
            )
            .map_or_else(
                || {
                    Err(PublishManifestError::Other(Error::Core(
                        arco_core::Error::PreconditionFailed { message },
                    )))
                },
                |error| Err(PublishManifestError::Other(error)),
            ),
            Err(error) => Err(PublishManifestError::Other(Error::from(error))),
        }
    }
}

/// Merges two fold states, preferring newer row versions.
#[allow(clippy::too_many_lines)]
fn merge_states(base: FoldState, delta: FoldState) -> FoldState {
    let mut merged = base;

    // Merge runs (newer row_version wins)
    for (run_id, row) in delta.runs {
        merged
            .runs
            .entry(run_id)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_run_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge tasks
    for (key, row) in delta.tasks {
        merged
            .tasks
            .entry(key)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_task_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge dep_satisfaction
    for (key, row) in delta.dep_satisfaction {
        merged
            .dep_satisfaction
            .entry(key)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_dep_satisfaction_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge timers
    for (timer_id, row) in delta.timers {
        merged
            .timers
            .entry(timer_id)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_timer_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge dispatch outbox
    for (dispatch_id, row) in delta.dispatch_outbox {
        merged
            .dispatch_outbox
            .entry(dispatch_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_dispatch_outbox_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge sensor state
    for (sensor_id, row) in delta.sensor_state {
        merged
            .sensor_state
            .entry(sensor_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_sensor_state_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge sensor evals
    for (eval_id, row) in delta.sensor_evals {
        merged
            .sensor_evals
            .entry(eval_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_sensor_eval_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    for (backfill_id, row) in delta.backfills {
        merged
            .backfills
            .entry(backfill_id)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_backfill_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    for (chunk_id, row) in delta.backfill_chunks {
        merged
            .backfill_chunks
            .entry(chunk_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_backfill_chunk_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    for (run_key, row) in delta.run_key_index {
        merged
            .run_key_index
            .entry(run_key)
            .and_modify(|existing| {
                if row.row_version > existing.row_version {
                    *existing = row.clone();
                }
            })
            .or_insert(row);
    }

    for (conflict_id, row) in delta.run_key_conflicts {
        merged.run_key_conflicts.entry(conflict_id).or_insert(row);
    }

    // Merge partition status
    for (key, row) in delta.partition_status {
        merged
            .partition_status
            .entry(key)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_partition_status_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge idempotency keys
    for (idempotency_key, row) in delta.idempotency_keys {
        merged
            .idempotency_keys
            .entry(idempotency_key)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_idempotency_key_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    for (schedule_id, row) in delta.schedule_definitions {
        merged
            .schedule_definitions
            .entry(schedule_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_schedule_definition_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    for (schedule_id, row) in delta.schedule_state {
        merged
            .schedule_state
            .entry(schedule_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_schedule_state_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    for (tick_id, row) in delta.schedule_ticks {
        merged
            .schedule_ticks
            .entry(tick_id)
            .and_modify(|existing| {
                if let Some(merged_row) =
                    merge_schedule_tick_rows(vec![existing.clone(), row.clone()])
                {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    merged
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    use sha2::Digest;
    let hash = sha2::Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(hash))
}

fn hash_suffixed_artifact_path(base_path: &str, file: &str, checksum_sha256: &str) -> String {
    let hash = checksum_sha256
        .strip_prefix("sha256:")
        .unwrap_or(checksum_sha256);
    let short_hash = &hash[..hash.len().min(12)];

    match file.rsplit_once('.') {
        Some((stem, ext)) => format!("{base_path}/{stem}.{short_hash}.{ext}"),
        None => format!("{base_path}/{file}.{short_hash}"),
    }
}

fn encode_pre_pointer_publish_error(error: Error) -> arco_core::Error {
    match error {
        Error::StaleFencingToken { expected, provided } => arco_core::Error::PreconditionFailed {
            message: format!("{STALE_FENCING_PUBLISH_MARKER}:{expected}:{provided}"),
        },
        Error::FencingLockUnavailable {
            lock_path,
            provided,
        } => arco_core::Error::PreconditionFailed {
            message: format!("{FENCING_LOCK_UNAVAILABLE_PUBLISH_MARKER}:{provided}:{lock_path}"),
        },
        Error::Core(error) => error,
        Error::Storage { message, source } => arco_core::Error::Storage { message, source },
        Error::Serialization { message } => arco_core::Error::Serialization { message },
        Error::Configuration { message }
        | Error::Dispatch { message }
        | Error::Parquet { message }
        | Error::PlanGenerationFailed { message } => arco_core::Error::Validation { message },
        other => arco_core::Error::Validation {
            message: other.to_string(),
        },
    }
}

fn decode_pre_pointer_publish_error(message: &str) -> Option<Error> {
    if let Some(rest) = message.strip_prefix(&format!("{STALE_FENCING_PUBLISH_MARKER}:")) {
        let (expected, provided) = rest.split_once(':')?;
        return Some(Error::StaleFencingToken {
            expected: expected.parse().ok()?,
            provided: provided.parse().ok()?,
        });
    }

    if let Some(rest) = message.strip_prefix(&format!("{FENCING_LOCK_UNAVAILABLE_PUBLISH_MARKER}:"))
    {
        let (provided, lock_path) = rest.split_once(':')?;
        return Some(Error::FencingLockUnavailable {
            lock_path: lock_path.to_string(),
            provided: provided.parse().ok()?,
        });
    }

    None
}

fn insert_changed<K, V>(
    delta: &mut std::collections::HashMap<K, V>,
    base: &std::collections::HashMap<K, V>,
    current: &std::collections::HashMap<K, V>,
) where
    K: std::hash::Hash + Eq + Clone,
    V: PartialEq + Clone,
{
    for (key, row) in current {
        if base.get(key) != Some(row) {
            delta.insert(key.clone(), row.clone());
        }
    }
}

fn delta_from_states(base: &FoldState, current: &FoldState) -> FoldState {
    let mut delta = FoldState::new();

    insert_changed(&mut delta.runs, &base.runs, &current.runs);
    insert_changed(&mut delta.tasks, &base.tasks, &current.tasks);
    insert_changed(
        &mut delta.dep_satisfaction,
        &base.dep_satisfaction,
        &current.dep_satisfaction,
    );
    insert_changed(&mut delta.timers, &base.timers, &current.timers);
    insert_changed(
        &mut delta.dispatch_outbox,
        &base.dispatch_outbox,
        &current.dispatch_outbox,
    );
    insert_changed(
        &mut delta.sensor_state,
        &base.sensor_state,
        &current.sensor_state,
    );
    insert_changed(
        &mut delta.sensor_evals,
        &base.sensor_evals,
        &current.sensor_evals,
    );

    insert_changed(&mut delta.backfills, &base.backfills, &current.backfills);
    insert_changed(
        &mut delta.backfill_chunks,
        &base.backfill_chunks,
        &current.backfill_chunks,
    );
    insert_changed(
        &mut delta.run_key_index,
        &base.run_key_index,
        &current.run_key_index,
    );
    insert_changed(
        &mut delta.run_key_conflicts,
        &base.run_key_conflicts,
        &current.run_key_conflicts,
    );
    insert_changed(
        &mut delta.partition_status,
        &base.partition_status,
        &current.partition_status,
    );
    insert_changed(
        &mut delta.idempotency_keys,
        &base.idempotency_keys,
        &current.idempotency_keys,
    );
    insert_changed(
        &mut delta.schedule_definitions,
        &base.schedule_definitions,
        &current.schedule_definitions,
    );
    insert_changed(
        &mut delta.schedule_state,
        &base.schedule_state,
        &current.schedule_state,
    );
    insert_changed(
        &mut delta.schedule_ticks,
        &base.schedule_ticks,
        &current.schedule_ticks,
    );

    delta
}

fn delta_state_is_empty(state: &FoldState) -> bool {
    state.runs.is_empty()
        && state.tasks.is_empty()
        && state.dep_satisfaction.is_empty()
        && state.timers.is_empty()
        && state.dispatch_outbox.is_empty()
        && state.sensor_state.is_empty()
        && state.sensor_evals.is_empty()
        && state.backfills.is_empty()
        && state.backfill_chunks.is_empty()
        && state.run_key_index.is_empty()
        && state.run_key_conflicts.is_empty()
        && state.partition_status.is_empty()
        && state.idempotency_keys.is_empty()
        && state.schedule_definitions.is_empty()
        && state.schedule_state.is_empty()
        && state.schedule_ticks.is_empty()
}

fn validate_expected_epoch(
    expected_epoch: Option<u64>,
    pointer: Option<&OrchestrationManifestPointer>,
) -> Result<()> {
    let (Some(request_epoch), Some(pointer)) = (expected_epoch, pointer) else {
        return Ok(());
    };

    if request_epoch < pointer.epoch {
        return Err(Error::StaleFencingToken {
            expected: pointer.epoch,
            provided: request_epoch,
        });
    }

    Ok(())
}

fn should_retry_l0_write_conflict(error: &Error, retry_attempt: usize) -> Option<u64> {
    match error {
        Error::Core(arco_core::Error::PreconditionFailed { message })
            if message.starts_with("immutable parquet artifact already exists:") =>
        {
            COMPACTION_PUBLISH_RETRY_DELAYS_MS
                .get(retry_attempt)
                .copied()
        }
        _ => None,
    }
}
fn retention_reference_time_for_events(events: &[(String, OrchestrationEvent)]) -> DateTime<Utc> {
    events
        .iter()
        .map(|(_, event)| event.timestamp)
        .max()
        .unwrap_or_else(Utc::now)
}

fn retention_reference_time_for_state(state: &FoldState) -> Option<DateTime<Utc>> {
    let max_sensor_eval = state
        .sensor_evals
        .values()
        .map(|row| row.evaluated_at)
        .max();
    let max_idempotency = state
        .idempotency_keys
        .values()
        .map(|row| row.recorded_at)
        .max();

    match (max_sensor_eval, max_idempotency) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn prune_sensor_evals(state: &mut FoldState, retention_now: DateTime<Utc>) {
    let cutoff = retention_now - chrono::Duration::days(SENSOR_EVAL_RETENTION_DAYS);
    state
        .sensor_evals
        .retain(|_, row| row.evaluated_at >= cutoff);
}

fn prune_idempotency_keys(state: &mut FoldState, retention_now: DateTime<Utc>) {
    let cutoff = retention_now - chrono::Duration::days(IDEMPOTENCY_KEY_RETENTION_DAYS);
    state
        .idempotency_keys
        .retain(|_, row| row.recorded_at >= cutoff);
}

fn update_watermarks(
    manifest: &mut OrchestrationManifest,
    events: &[(String, OrchestrationEvent)],
    last_event_id: &str,
    durability_mode: DurabilityMode,
) -> bool {
    if events.is_empty() {
        return false;
    }

    let should_advance_committed = manifest
        .watermarks
        .last_committed_event_id
        .as_deref()
        .is_none_or(|current| last_event_id > current);
    if !should_advance_committed {
        return false;
    }

    let last_path = events.last().map(|(path, _)| path.clone());
    let previous_visible = manifest.watermarks.last_visible_event_id.clone();
    let next_visible = if durability_mode == DurabilityMode::Visible {
        Some(last_event_id.to_string())
    } else {
        previous_visible
    };

    manifest.watermarks = Watermarks {
        last_committed_event_id: Some(last_event_id.to_string()),
        last_visible_event_id: next_visible.clone(),
        events_processed_through: next_visible,
        last_processed_file: last_path,
        last_processed_at: Utc::now(),
    };
    true
}

fn durability_mode_label(mode: DurabilityMode) -> &'static str {
    match mode {
        DurabilityMode::Visible => "visible",
        DurabilityMode::Persisted => "persisted",
    }
}

fn visibility_lag_events(watermarks: &Watermarks) -> f64 {
    match (
        watermarks.last_committed_event_id.as_deref(),
        watermarks.last_visible_event_id.as_deref(),
    ) {
        (Some(committed), Some(visible)) if committed == visible => 0.0,
        (Some(_), _) => 1.0,
        (None, _) => 0.0,
    }
}

fn record_compaction_metrics(
    durability_mode: DurabilityMode,
    ack_latency: Duration,
    manifest: &OrchestrationManifest,
    visibility_status: CompactionVisibility,
    result: &str,
) {
    let mode = durability_mode_label(durability_mode).to_string();
    counter!(
        metric_names::ORCH_COMPACTIONS_TOTAL,
        metric_labels::DURABILITY_MODE => mode.clone(),
        metric_labels::RESULT => result.to_string(),
    )
    .increment(1);
    histogram!(
        metric_names::ORCH_COMPACTOR_ACK_LATENCY_SECONDS,
        metric_labels::DURABILITY_MODE => mode.clone(),
    )
    .record(ack_latency.as_secs_f64());
    gauge!(
        metric_names::ORCH_COMPACTOR_VISIBILITY_LAG_EVENTS,
        metric_labels::DURABILITY_MODE => mode,
    )
    .set(match visibility_status {
        CompactionVisibility::Visible => visibility_lag_events(&manifest.watermarks),
        CompactionVisibility::PersistedNotVisible => 1.0,
    });
}

fn should_retry_publish_error(
    publish_error: &PublishManifestError,
    retry_attempt: usize,
) -> Option<u64> {
    match publish_error.retry_reason() {
        // Concurrent compactions deriving the next manifest ID from the same base
        // manifest collide on the immutable snapshot path before pointer CAS.
        Some(PublishRetryReason::SnapshotConflict) => COMPACTION_PUBLISH_RETRY_DELAYS_MS
            .get(retry_attempt)
            .copied(),
        Some(PublishRetryReason::ConcurrentWrite) | None => None,
    }
}

async fn wait_for_publish_retry(delay_ms: u64) {
    if delay_ms == 0 {
        tokio::task::yield_now().await;
    } else {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

fn record_compaction_publish_retry(
    durability_mode: DurabilityMode,
    reason: PublishRetryReason,
    attempt: usize,
    delay_ms: u64,
) {
    tracing::warn!(
        attempt,
        reason = reason.as_str(),
        delay_ms,
        "retrying compaction after concurrent manifest publish race"
    );
    counter!(
        metric_names::ORCH_COMPACTOR_PUBLISH_RETRIES_TOTAL,
        metric_labels::DURABILITY_MODE => durability_mode_label(durability_mode).to_string(),
        metric_labels::REASON => reason.as_str().to_string(),
        "attempt" => attempt.to_string(),
    )
    .increment(1);
}

fn event_priority(data: &OrchestrationEventData) -> u8 {
    match data {
        // Fold trigger evaluations before emitted RunRequested events at the same timestamp.
        OrchestrationEventData::ScheduleTicked { .. }
        | OrchestrationEventData::SensorEvaluated { .. }
        | OrchestrationEventData::BackfillChunkPlanned { .. } => 0,
        OrchestrationEventData::RunRequested { .. } => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::compactor::fold::{DispatchOutboxRow, TimerRow};
    use crate::orchestration::events::{
        OrchestrationEventData, PartitionSelector, RunRequest, SensorEvalStatus, SourceRef,
        TaskDef, TickStatus, TimerType, TriggerInfo, TriggerSource,
    };
    use crate::paths::{orchestration_compaction_lock_path, orchestration_event_path};
    use arco_core::{
        DistributedLock, MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use async_trait::async_trait;
    use chrono::DateTime;
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Barrier, Notify};
    use ulid::Ulid;

    async fn create_test_compactor() -> Result<(MicroCompactor, ScopedStorage)> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let compactor = MicroCompactor::new(storage.clone());
        Ok((compactor, storage))
    }

    async fn create_test_compactor_with_backend<B: StorageBackend>(
        backend: Arc<B>,
    ) -> Result<(MicroCompactor, ScopedStorage)> {
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let compactor = MicroCompactor::new(storage.clone());
        Ok((compactor, storage))
    }

    // Use deterministic event IDs to avoid parallel test flakiness.
    // Event IDs are ordered to match the expected sort order (by timestamp, then priority).
    // RunTriggered has priority 1, PlanCreated has priority 2, so PlanCreated sorts last.
    // The event_ids are ordered so that PlanCreated (sorted last) has the larger event_id,
    // which is what the compact_orders_watermarks_by_event_id test expects.
    fn make_run_triggered_event() -> OrchestrationEvent {
        OrchestrationEvent::new_with_event_id(
            "tenant",
            "workspace",
            OrchestrationEventData::RunTriggered {
                run_id: "run_01".to_string(),
                plan_id: "plan_01".to_string(),
                trigger: TriggerInfo::Manual {
                    user_id: "user@example.com".to_string(),
                },
                root_assets: vec!["analytics.report".to_string()],
                run_key: None,
                labels: HashMap::new(),
                code_version: None,
            },
            "evt_01_run_triggered",
        )
    }

    fn make_plan_created_event() -> OrchestrationEvent {
        OrchestrationEvent::new_with_event_id(
            "tenant",
            "workspace",
            OrchestrationEventData::PlanCreated {
                run_id: "run_01".to_string(),
                plan_id: "plan_01".to_string(),
                tasks: vec![
                    TaskDef {
                        key: "extract".to_string(),
                        depends_on: vec![],
                        asset_key: Some("analytics.extract".to_string()),
                        partition_key: None,
                        max_attempts: 3,
                        heartbeat_timeout_sec: 300,
                    },
                    TaskDef {
                        key: "transform".to_string(),
                        depends_on: vec!["extract".to_string()],
                        asset_key: Some("analytics.transform".to_string()),
                        partition_key: None,
                        max_attempts: 3,
                        heartbeat_timeout_sec: 300,
                    },
                ],
            },
            "evt_02_plan_created",
        )
    }

    fn make_task_started_event(attempt_id: &str) -> OrchestrationEvent {
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::TaskStarted {
                run_id: "run_01".to_string(),
                task_key: "extract".to_string(),
                attempt: 1,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-01".to_string(),
            },
        )
    }

    fn make_dispatch_requested_event(
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
    ) -> OrchestrationEvent {
        let dispatch_id = DispatchOutboxRow::dispatch_id(run_id, task_key, attempt);
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::DispatchRequested {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_queue: "default-queue".to_string(),
                dispatch_id,
            },
        )
    }

    fn make_dispatch_enqueued_event(dispatch_id: &str) -> OrchestrationEvent {
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::DispatchEnqueued {
                dispatch_id: dispatch_id.to_string(),
                run_id: Some("run_01".to_string()),
                task_key: Some("extract".to_string()),
                attempt: Some(1),
                cloud_task_id: "d_cloud123".to_string(),
            },
        )
    }

    fn make_timer_requested_event(timer_id: &str, fire_at: DateTime<Utc>) -> OrchestrationEvent {
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::TimerRequested {
                timer_id: timer_id.to_string(),
                timer_type: TimerType::Retry,
                run_id: Some("run_01".to_string()),
                task_key: Some("extract".to_string()),
                attempt: Some(1),
                fire_at,
            },
        )
    }

    fn make_timer_enqueued_event(timer_id: &str) -> OrchestrationEvent {
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::TimerEnqueued {
                timer_id: timer_id.to_string(),
                run_id: Some("run_01".to_string()),
                task_key: Some("extract".to_string()),
                attempt: Some(1),
                cloud_task_id: "t_cloud456".to_string(),
            },
        )
    }

    async fn write_basic_compaction_events(storage: &ScopedStorage) -> Result<Vec<String>> {
        let event1 = make_run_triggered_event();
        let event2 = make_plan_created_event();

        let path1 = orchestration_event_path("2025-01-15", &event1.event_id);
        let path2 = orchestration_event_path("2025-01-15", &event2.event_id);

        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&event1).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&event2).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        Ok(vec![path1, path2])
    }

    #[derive(Debug)]
    struct SnapshotRaceBackend {
        inner: MemoryBackend,
        manifest_suffix: String,
        race_barrier: Barrier,
        race_hits: AtomicUsize,
    }

    impl SnapshotRaceBackend {
        fn new(manifest_suffix: impl Into<String>) -> Self {
            Self {
                inner: MemoryBackend::new(),
                manifest_suffix: manifest_suffix.into(),
                race_barrier: Barrier::new(2),
                race_hits: AtomicUsize::new(0),
            }
        }

        fn should_race(&self, path: &str, precondition: &WritePrecondition) -> bool {
            path.ends_with(self.manifest_suffix.as_str())
                && matches!(precondition, WritePrecondition::DoesNotExist)
        }
    }

    #[async_trait]
    impl StorageBackend for SnapshotRaceBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            if self.should_race(path, &precondition)
                && self.race_hits.fetch_add(1, Ordering::SeqCst) < 2
            {
                self.race_barrier.wait().await;
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[derive(Debug)]
    struct PreferredEpochSnapshotRaceBackend {
        inner: MemoryBackend,
        manifest_suffix: String,
        preferred_epoch: u64,
        race_barrier: Barrier,
        preferred_committed: Notify,
        preferred_committed_flag: AtomicBool,
        race_hits: AtomicUsize,
    }

    impl PreferredEpochSnapshotRaceBackend {
        fn new(manifest_suffix: impl Into<String>, preferred_epoch: u64) -> Self {
            Self {
                inner: MemoryBackend::new(),
                manifest_suffix: manifest_suffix.into(),
                preferred_epoch,
                race_barrier: Barrier::new(2),
                preferred_committed: Notify::new(),
                preferred_committed_flag: AtomicBool::new(false),
                race_hits: AtomicUsize::new(0),
            }
        }

        fn race_epoch(
            &self,
            path: &str,
            precondition: &WritePrecondition,
            data: &Bytes,
        ) -> Option<u64> {
            if !path.ends_with(self.manifest_suffix.as_str())
                || !matches!(precondition, WritePrecondition::DoesNotExist)
            {
                return None;
            }

            serde_json::from_slice::<OrchestrationManifest>(data.as_ref())
                .ok()
                .map(|manifest| manifest.epoch)
        }
    }

    #[async_trait]
    impl StorageBackend for PreferredEpochSnapshotRaceBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            if let Some(epoch) = self.race_epoch(path, &precondition, &data) {
                if self.race_hits.fetch_add(1, Ordering::SeqCst) < 2 {
                    self.race_barrier.wait().await;
                    if epoch != self.preferred_epoch
                        && !self.preferred_committed_flag.load(Ordering::SeqCst)
                    {
                        self.preferred_committed.notified().await;
                    }
                    let result = self.inner.put(path, data, precondition).await;
                    if epoch == self.preferred_epoch {
                        self.preferred_committed_flag.store(true, Ordering::SeqCst);
                        self.preferred_committed.notify_waiters();
                    }
                    return result;
                }
            }

            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[derive(Debug)]
    struct OneShotL0WriteConflictBackend {
        inner: MemoryBackend,
        pending_conflict: AtomicBool,
        conflict_count: AtomicUsize,
    }

    impl OneShotL0WriteConflictBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                pending_conflict: AtomicBool::new(true),
                conflict_count: AtomicUsize::new(0),
            }
        }

        fn conflict_count(&self) -> usize {
            self.conflict_count.load(Ordering::SeqCst)
        }

        fn should_conflict(&self, path: &str, precondition: &WritePrecondition) -> bool {
            path.contains("state/orchestration/l0/")
                && matches!(precondition, WritePrecondition::DoesNotExist)
                && self.pending_conflict.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl StorageBackend for OneShotL0WriteConflictBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            if self.should_conflict(path, &precondition)
                && self.pending_conflict.swap(false, Ordering::SeqCst)
            {
                self.conflict_count.fetch_add(1, Ordering::SeqCst);
                return Ok(WriteResult::PreconditionFailed {
                    current_version: "synthetic-l0-conflict".to_string(),
                });
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn compact_events_retries_after_concurrent_manifest_snapshot_conflict() -> Result<()> {
        let backend = Arc::new(SnapshotRaceBackend::new(
            "state/orchestration/manifests/00000000000000000001.json",
        ));
        let (compactor_a, storage) = create_test_compactor_with_backend(backend.clone()).await?;
        let compactor_b = MicroCompactor::new(storage.clone());
        let event_paths = write_basic_compaction_events(&storage).await?;

        let (result_a, result_b) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                compactor_a.compact_events(event_paths.clone()),
                compactor_b.compact_events(event_paths),
            )
        })
        .await
        .expect("racing compactors should not deadlock");

        let result_a = result_a?;
        let result_b = result_b?;
        assert!(
            result_a.delta_id.is_some() ^ result_b.delta_id.is_some(),
            "exactly one racing compaction should publish a new delta"
        );

        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest: OrchestrationManifest =
            serde_json::from_slice(&manifest_data).expect("parse manifest");
        assert_eq!(manifest.manifest_id, "00000000000000000001");
        assert_eq!(manifest.l0_count, 1);
        assert_eq!(manifest.l0_deltas.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn compact_events_retry_revalidates_expected_epoch() -> Result<()> {
        let backend = Arc::new(PreferredEpochSnapshotRaceBackend::new(
            "state/orchestration/manifests/00000000000000000001.json",
            2,
        ));
        let (winning_compactor, storage) =
            create_test_compactor_with_backend(backend.clone()).await?;
        let stale_compactor = MicroCompactor::new(storage.clone());

        let mut manifest = OrchestrationManifest::new("01HQXYZ300REV");
        manifest.manifest_id = "00000000000000000000".to_string();
        manifest.epoch = 1;
        winning_compactor
            .publish_manifest(&manifest, None, None, None, None, None, None)
            .await?;

        let event_paths = write_basic_compaction_events(&storage).await?;

        let (stale_result, winning_result) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                stale_compactor.compact_events_with_epoch(event_paths.clone(), Some(1)),
                winning_compactor.compact_events_with_epoch(event_paths, Some(2)),
            )
        })
        .await
        .expect("epoch-race compactors should not deadlock");

        let stale_error = stale_result.expect_err("retry must revalidate a stale epoch");
        assert!(
            matches!(
                stale_error,
                Error::StaleFencingToken {
                    expected: 2,
                    provided: 1,
                }
            ),
            "expected typed stale token after retry, got {stale_error:?}"
        );
        assert!(
            winning_result.is_ok(),
            "higher epoch writer should publish successfully"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_empty_events_returns_current_manifest() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let result = compactor.compact_events(vec![]).await?;

        assert_eq!(result.events_processed, 0);
        assert!(result.delta_id.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_rejects_noncanonical_lock_path() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let error = compactor
            .compact_events_fenced(vec![], 1, "locks/not-the-orchestration-lock.json")
            .await
            .expect_err("noncanonical lock path must be rejected");

        assert!(
            matches!(error, Error::Core(arco_core::Error::InvalidInput(ref message)) if message == "invalid lock_path"),
            "expected invalid lock path error, got {error:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_rejects_missing_lock_holder() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let error = compactor
            .compact_events_fenced(vec![], 1, orchestration_compaction_lock_path())
            .await
            .expect_err("missing lock holder must be rejected");

        assert!(
            matches!(
                error,
                Error::FencingLockUnavailable { ref lock_path, provided: 1 }
                if lock_path == orchestration_compaction_lock_path()
            ),
            "expected missing lock holder error, got {error:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_rejects_stale_lock_holder() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let lock = DistributedLock::new(
            storage.backend().clone(),
            orchestration_compaction_lock_path(),
        );
        let guard = lock.acquire(Duration::from_secs(30), 1).await?;
        let stale_token = guard.fencing_token().sequence().saturating_sub(1);

        let error = compactor
            .compact_events_fenced(vec![], stale_token, orchestration_compaction_lock_path())
            .await
            .expect_err("stale fencing token must be rejected");

        assert!(
            matches!(
                error,
                Error::StaleFencingToken {
                    expected: 1,
                    provided: 0,
                }
            ),
            "expected stale fencing token, got {error:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn write_parquet_file_rejects_overwrite_attempts() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let path = "state/orchestration/l0/existing/runs.parquet";

        storage
            .put_raw(path, Bytes::from_static(b"old"), WritePrecondition::None)
            .await?;

        let error = compactor
            .write_parquet_file(path, Bytes::from_static(b"new"))
            .await
            .expect_err("immutable parquet writes must reject overwrite");

        assert!(
            matches!(
                error,
                Error::Core(arco_core::Error::PreconditionFailed { ref message })
                    if message.as_str()
                        == format!("immutable parquet artifact already exists: {path}")
            ),
            "expected immutable-write precondition failure, got {error:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_events_retries_after_l0_write_conflict() -> Result<()> {
        let backend = Arc::new(OneShotL0WriteConflictBackend::new());
        let (compactor, storage) = create_test_compactor_with_backend(backend.clone()).await?;
        let event_paths = write_basic_compaction_events(&storage).await?;

        let result = compactor.compact_events(event_paths).await?;

        assert!(
            result.delta_id.is_some(),
            "delta publish should still succeed"
        );
        assert_eq!(
            backend.conflict_count(),
            1,
            "expected one synthetic L0 conflict"
        );

        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest: OrchestrationManifest =
            serde_json::from_slice(&manifest_data).expect("parse manifest");
        assert_eq!(
            manifest.l0_count, 1,
            "retry should publish exactly one delta"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_manifest_records_artifact_metadata_and_hash_suffixed_names() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let event_paths = write_basic_compaction_events(&storage).await?;

        compactor.compact_events(event_paths).await?;

        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest_json: serde_json::Value =
            serde_json::from_slice(&manifest_data).expect("parse manifest json");

        let runs_artifact = &manifest_json["l0_deltas"][0]["tables"]["runs"];
        assert!(
            runs_artifact.is_object(),
            "artifact must be stored as metadata"
        );

        let path = runs_artifact["path"]
            .as_str()
            .expect("artifact path must be a string");
        assert!(
            path.contains("."),
            "artifact file should include a hash-derived suffix: {path}"
        );
        assert!(
            path.ends_with(".parquet"),
            "artifact file should remain parquet: {path}"
        );
        assert!(
            runs_artifact["checksum_sha256"]
                .as_str()
                .is_some_and(|value| !value.is_empty()),
            "artifact must store checksum_sha256"
        );
        assert!(
            runs_artifact["byte_size"]
                .as_u64()
                .is_some_and(|value| value > 0),
            "artifact must store byte_size"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_merges_l0_into_base_when_limits_are_exceeded() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let mut manifest = OrchestrationManifest::new("01HQXYZ400REV");
        manifest.manifest_id = "00000000000000000000".to_string();
        manifest.l0_limits.max_count = 1;
        manifest.l0_limits.max_rows = u32::MAX;
        manifest.l0_limits.max_age_seconds = u32::MAX;
        compactor
            .publish_manifest(&manifest, None, None, None, None, None, None)
            .await?;

        let event_paths = write_basic_compaction_events(&storage).await?;
        compactor.compact_events(event_paths).await?;

        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest: OrchestrationManifest =
            serde_json::from_slice(&manifest_data).expect("parse manifest");

        assert!(
            manifest.base_snapshot.snapshot_id.is_some(),
            "base snapshot should be materialized after L0 limit is exceeded"
        );
        assert_eq!(manifest.l0_count, 0, "L0 deltas should be merged into base");
        assert!(
            manifest.l0_deltas.is_empty(),
            "merged L0 deltas should not remain referenced"
        );

        let (_loaded_manifest, state) = compactor.load_state().await?;
        assert!(
            state.runs.contains_key("run_01"),
            "merged base snapshot must preserve state"
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_creates_l0_delta_with_state() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        // Write events to ledger
        let event1 = make_run_triggered_event();
        let event2 = make_plan_created_event();

        let path1 = orchestration_event_path("2025-01-15", &event1.event_id);
        let path2 = orchestration_event_path("2025-01-15", &event2.event_id);

        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&event1).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&event2).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        // Compact events
        let result = compactor.compact_events(vec![path1, path2]).await?;

        assert_eq!(result.events_processed, 2);
        assert!(result.delta_id.is_some());

        // Verify manifest was updated
        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest: OrchestrationManifest =
            serde_json::from_slice(&manifest_data).expect("parse");

        assert_eq!(manifest.l0_count, 1);
        assert_eq!(manifest.l0_deltas.len(), 1);
        assert_eq!(manifest.l0_deltas[0].event_range.event_count, 2);

        let pointer_data = storage
            .get_raw(orchestration_manifest_pointer_path())
            .await?;
        let pointer: OrchestrationManifestPointer =
            serde_json::from_slice(&pointer_data).expect("parse pointer");
        assert_eq!(pointer.manifest_id, manifest.manifest_id);
        assert!(
            pointer
                .manifest_path
                .contains("state/orchestration/manifests/")
        );
        let immutable_manifest = storage.get_raw(&pointer.manifest_path).await?;
        let immutable: OrchestrationManifest =
            serde_json::from_slice(&immutable_manifest).expect("parse immutable");
        assert_eq!(immutable.manifest_id, manifest.manifest_id);

        Ok(())
    }

    #[tokio::test]
    async fn fold_state_survives_roundtrip_through_parquet() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        // Compact initial events
        let event1 = make_run_triggered_event();
        let event2 = make_plan_created_event();

        let path1 = orchestration_event_path("2025-01-15", &event1.event_id);
        let path2 = orchestration_event_path("2025-01-15", &event2.event_id);

        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&event1).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&event2).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        compactor.compact_events(vec![path1, path2]).await?;

        // Load state from manifest
        let manifest = compactor.get_or_create_manifest().await?;
        let state = compactor.load_current_state(&manifest).await?;

        // Verify state
        assert_eq!(state.runs.len(), 1);
        assert!(state.runs.contains_key("run_01"));
        assert_eq!(state.tasks.len(), 2);
        assert!(
            state
                .tasks
                .contains_key(&("run_01".to_string(), "extract".to_string()))
        );
        assert!(
            state
                .tasks
                .contains_key(&("run_01".to_string(), "transform".to_string()))
        );
        assert_eq!(state.dep_satisfaction.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn cold_restart_preserves_layer2_backfills_ticks_and_sensors() -> Result<()> {
        let (_compactor, storage) = create_test_compactor().await?;

        let now = Utc::now();
        let scheduled_for = now + chrono::Duration::minutes(5);
        let schedule_id = "sched-01";
        let tick_id = format!("{schedule_id}:{}", scheduled_for.timestamp());

        let sensor_id = "sensor-01";
        let eval_id = "eval-01";

        let backfill_id = "bf-01";
        let chunk_id = "bf-01:0";

        fn event_with_id(
            event_id: &str,
            timestamp: DateTime<Utc>,
            data: OrchestrationEventData,
        ) -> OrchestrationEvent {
            let mut e =
                OrchestrationEvent::new_with_timestamp("tenant", "workspace", data, timestamp);
            e.event_id = event_id.to_string();
            e
        }

        let schedule_def = event_with_id(
            "evt_01",
            now,
            OrchestrationEventData::ScheduleDefinitionUpserted {
                schedule_id: schedule_id.to_string(),
                cron_expression: "* * * * *".to_string(),
                timezone: "UTC".to_string(),
                catchup_window_minutes: 60,
                asset_selection: vec!["asset.a".to_string()],
                max_catchup_ticks: 10,
                enabled: true,
            },
        );

        let schedule_tick = event_with_id(
            "evt_02",
            now,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: schedule_id.to_string(),
                scheduled_for,
                tick_id: tick_id.clone(),
                definition_version: "v1".to_string(),
                asset_selection: vec!["asset.a".to_string()],
                partition_selection: None,
                status: TickStatus::Triggered,
                run_key: Some("sched:1".to_string()),
                request_fingerprint: Some("fp-sched-1".to_string()),
            },
        );

        let schedule_run_requested = event_with_id(
            "evt_03",
            now,
            OrchestrationEventData::RunRequested {
                run_key: "sched:1".to_string(),
                request_fingerprint: "fp-sched-1".to_string(),
                asset_selection: vec!["asset.a".to_string()],
                partition_selection: None,
                trigger_source_ref: SourceRef::Schedule {
                    schedule_id: schedule_id.to_string(),
                    tick_id: tick_id.clone(),
                },
                labels: HashMap::new(),
            },
        );

        let sensor_eval = event_with_id(
            "evt_04",
            now,
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                eval_id: eval_id.to_string(),
                cursor_before: None,
                cursor_after: Some("c1".to_string()),
                expected_state_version: Some(0),
                trigger_source: TriggerSource::Poll {
                    poll_epoch: scheduled_for.timestamp(),
                },
                run_requests: vec![RunRequest {
                    run_key: "sensor:1".to_string(),
                    request_fingerprint: "fp-sensor-1".to_string(),
                    asset_selection: vec!["asset.b".to_string()],
                    partition_selection: None,
                }],
                status: SensorEvalStatus::Triggered,
            },
        );

        let sensor_run_requested = event_with_id(
            "evt_05",
            now,
            OrchestrationEventData::RunRequested {
                run_key: "sensor:1".to_string(),
                request_fingerprint: "fp-sensor-1".to_string(),
                asset_selection: vec!["asset.b".to_string()],
                partition_selection: None,
                trigger_source_ref: SourceRef::Sensor {
                    sensor_id: sensor_id.to_string(),
                    eval_id: eval_id.to_string(),
                },
                labels: HashMap::new(),
            },
        );

        let backfill_created = event_with_id(
            "evt_06",
            now,
            OrchestrationEventData::BackfillCreated {
                backfill_id: backfill_id.to_string(),
                client_request_id: "req-01".to_string(),
                asset_selection: vec!["asset.c".to_string()],
                partition_selector: PartitionSelector::Explicit {
                    partition_keys: vec![
                        "2025-01-01".to_string(),
                        "2025-01-02".to_string(),
                        "2025-01-03".to_string(),
                    ],
                },
                total_partitions: 3,
                chunk_size: 2,
                max_concurrent_runs: 2,
                parent_backfill_id: None,
            },
        );

        let backfill_chunk_planned = event_with_id(
            "evt_07",
            now,
            OrchestrationEventData::BackfillChunkPlanned {
                backfill_id: backfill_id.to_string(),
                chunk_id: chunk_id.to_string(),
                chunk_index: 0,
                partition_keys: vec!["2025-01-01".to_string(), "2025-01-02".to_string()],
                run_key: "backfill:bf-01:0".to_string(),
                request_fingerprint: "fp-bf-0".to_string(),
            },
        );

        let backfill_run_requested = event_with_id(
            "evt_08",
            now,
            OrchestrationEventData::RunRequested {
                run_key: "backfill:bf-01:0".to_string(),
                request_fingerprint: "fp-bf-0".to_string(),
                asset_selection: vec!["asset.c".to_string()],
                partition_selection: Some(vec!["2025-01-01".to_string(), "2025-01-02".to_string()]),
                trigger_source_ref: SourceRef::Backfill {
                    backfill_id: backfill_id.to_string(),
                    chunk_id: chunk_id.to_string(),
                },
                labels: HashMap::new(),
            },
        );

        let events = vec![
            schedule_def,
            schedule_tick,
            schedule_run_requested,
            sensor_eval,
            sensor_run_requested,
            backfill_created,
            backfill_chunk_planned,
            backfill_run_requested,
        ];

        let mut paths = Vec::new();
        for event in &events {
            let path = orchestration_event_path("2026-01-15", &event.event_id);
            storage
                .put_raw(
                    &path,
                    Bytes::from(serde_json::to_string(event).expect("serialize")),
                    WritePrecondition::None,
                )
                .await?;
            paths.push(path);
        }

        let compactor = MicroCompactor::new(storage.clone());
        compactor.compact_events(paths).await?;

        // Cold restart: create a fresh compactor instance and reload solely from manifest + Parquet.
        let reloaded = MicroCompactor::new(storage.clone());
        let (_manifest, state) = reloaded.load_state().await?;

        assert!(state.schedule_definitions.contains_key(schedule_id));
        assert!(state.schedule_state.contains_key(schedule_id));
        let tick = state.schedule_ticks.get(&tick_id).expect("tick");

        assert!(state.sensor_state.contains_key(sensor_id));
        assert!(state.sensor_evals.contains_key(eval_id));

        assert!(state.backfills.contains_key(backfill_id));
        let chunk = state.backfill_chunks.get(chunk_id).expect("chunk");

        let sched_run_id = tick.run_id.as_deref().expect("schedule run_id");
        let sched_index = state
            .run_key_index
            .get("sched:1")
            .expect("schedule run_key_index");
        assert_eq!(sched_index.run_id.as_str(), sched_run_id);

        let bf_run_id = chunk.run_id.as_deref().expect("backfill chunk run_id");
        let bf_index = state
            .run_key_index
            .get("backfill:bf-01:0")
            .expect("backfill run_key_index");
        assert_eq!(bf_index.run_id.as_str(), bf_run_id);

        Ok(())
    }

    #[tokio::test]
    async fn compact_orders_watermarks_by_event_id() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        let event1 = make_run_triggered_event();
        let event2 = make_plan_created_event();

        let event1_id = event1.event_id.clone();
        let event2_id = event2.event_id.clone();
        let path1 = orchestration_event_path("2025-01-15", &event1_id);
        let path2 = orchestration_event_path("2025-01-15", &event2_id);

        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&event1).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&event2).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        compactor
            .compact_events(vec![path2.clone(), path1.clone()])
            .await?;

        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest: OrchestrationManifest =
            serde_json::from_slice(&manifest_data).expect("parse");

        let (expected_event, expected_path) = if event1_id > event2_id {
            (event1_id, path1)
        } else {
            (event2_id, path2)
        };

        assert_eq!(
            manifest.watermarks.events_processed_through.as_deref(),
            Some(expected_event.as_str())
        );
        assert_eq!(
            manifest.watermarks.last_processed_file.as_deref(),
            Some(expected_path.as_str())
        );

        Ok(())
    }

    #[tokio::test]
    async fn compact_writes_delta_rows_only() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        let event1 = make_run_triggered_event();
        let event2 = make_plan_created_event();

        let path1 = orchestration_event_path("2025-01-15", &event1.event_id);
        let path2 = orchestration_event_path("2025-01-15", &event2.event_id);

        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&event1).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&event2).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        compactor.compact_events(vec![path1, path2]).await?;

        let attempt_id = Ulid::new().to_string();
        let event3 = make_task_started_event(&attempt_id);
        let path3 = orchestration_event_path("2025-01-15", &event3.event_id);
        storage
            .put_raw(
                &path3,
                Bytes::from(serde_json::to_string(&event3).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        compactor.compact_events(vec![path3]).await?;

        let manifest_data = storage.get_raw(orchestration_manifest_path()).await?;
        let manifest: OrchestrationManifest =
            serde_json::from_slice(&manifest_data).expect("parse");

        let latest = manifest.l0_deltas.last().expect("delta");
        assert_eq!(latest.row_counts.runs, 0);
        assert_eq!(latest.row_counts.tasks, 1);
        assert_eq!(latest.row_counts.dep_satisfaction, 0);
        assert_eq!(latest.row_counts.timers, 0);
        assert_eq!(latest.row_counts.dispatch_outbox, 0);
        assert_eq!(latest.row_counts.sensor_state, 0);
        assert_eq!(latest.row_counts.sensor_evals, 0);
        assert_eq!(latest.row_counts.partition_status, 0);
        // Each event records an idempotency key; this delta has 1 TaskStarted event
        assert_eq!(latest.row_counts.idempotency_keys, 1);

        Ok(())
    }

    #[tokio::test]
    async fn compact_persists_out_of_order_dispatch_fields() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        let dispatch_id = DispatchOutboxRow::dispatch_id("run_01", "extract", 1);

        // DispatchEnqueued arrives first (newer ULID)
        let mut enqueued = make_dispatch_enqueued_event(&dispatch_id);
        enqueued.event_id = "01B".to_string();
        let path1 = orchestration_event_path("2025-01-15", &enqueued.event_id);
        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&enqueued).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        compactor.compact_events(vec![path1]).await?;

        // DispatchRequested arrives later but with older ULID
        let mut requested = make_dispatch_requested_event("run_01", "extract", 1, "01HQ123ATT");
        requested.event_id = "01A".to_string();
        let path2 = orchestration_event_path("2025-01-15", &requested.event_id);
        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&requested).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        compactor.compact_events(vec![path2]).await?;

        // Load merged state and verify attempt_id was persisted
        let manifest = compactor.get_or_create_manifest().await?;
        let state = compactor.load_current_state(&manifest).await?;
        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");

        assert_eq!(row.attempt_id, "01HQ123ATT");
        assert_eq!(row.cloud_task_id.as_deref(), Some("d_cloud123"));

        Ok(())
    }

    #[tokio::test]
    async fn compact_persists_out_of_order_timer_fire_at() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        let fire_epoch = 1_705_320_000i64;
        let timer_id = TimerRow::retry_timer_id("run_01", "extract", 1, fire_epoch);

        // TimerEnqueued arrives first (newer ULID)
        let mut enqueued = make_timer_enqueued_event(&timer_id);
        enqueued.event_id = "01B".to_string();
        let path1 = orchestration_event_path("2025-01-15", &enqueued.event_id);
        storage
            .put_raw(
                &path1,
                Bytes::from(serde_json::to_string(&enqueued).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        compactor.compact_events(vec![path1]).await?;

        // TimerRequested arrives later but with older ULID (canonical fire_at differs)
        let canonical_fire_at = DateTime::from_timestamp(fire_epoch + 30, 0).unwrap();
        let mut requested = make_timer_requested_event(&timer_id, canonical_fire_at);
        requested.event_id = "01A".to_string();
        let path2 = orchestration_event_path("2025-01-15", &requested.event_id);
        storage
            .put_raw(
                &path2,
                Bytes::from(serde_json::to_string(&requested).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;
        compactor.compact_events(vec![path2]).await?;

        // Load merged state and verify fire_at was updated
        let manifest = compactor.get_or_create_manifest().await?;
        let state = compactor.load_current_state(&manifest).await?;
        let row = state.timers.get(&timer_id).expect("timer row");

        assert_eq!(row.fire_at, canonical_fire_at);
        assert_eq!(row.cloud_task_id.as_deref(), Some("t_cloud456"));

        Ok(())
    }

    #[tokio::test]
    async fn publish_manifest_uses_cas() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.manifest_id = "00000000000000000000".to_string();
        compactor
            .publish_manifest(&manifest, None, None, None, None, None, None)
            .await?;

        let (_current, version, pointer, pointer_bytes) =
            compactor.read_manifest_with_version().await?;
        let version = version.expect("pointer version");
        let pointer = pointer.expect("pointer");
        let pointer_bytes = pointer_bytes.expect("pointer bytes");
        let previous_manifest = manifest.clone();

        let mut competing = manifest.clone();
        competing.manifest_id = "00000000000000000001".to_string();
        competing.revision_ulid = "01HQXYZ124REV".to_string();
        compactor
            .publish_manifest(
                &competing,
                Some(&previous_manifest),
                Some(&version),
                Some(&pointer),
                Some(&pointer_bytes),
                None,
                None,
            )
            .await?;

        let mut stale = manifest;
        stale.manifest_id = "00000000000000000002".to_string();
        let err = compactor
            .publish_manifest(
                &stale,
                Some(&previous_manifest),
                Some(&version),
                Some(&pointer),
                Some(&pointer_bytes),
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("manifest publish failed"));

        Ok(())
    }

    #[tokio::test]
    async fn compact_with_stale_epoch_is_rejected() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let mut manifest = OrchestrationManifest::new("01HQXYZ200REV");
        manifest.manifest_id = "00000000000000000000".to_string();
        manifest.epoch = 5;
        compactor
            .publish_manifest(&manifest, None, None, None, None, None, None)
            .await?;

        let error = compactor
            .compact_events_with_epoch(vec![], Some(4))
            .await
            .expect_err("stale epoch must be rejected");
        assert!(
            matches!(
                error,
                Error::StaleFencingToken {
                    expected: 5,
                    provided: 4,
                }
            ),
            "expected typed stale token error, got {error:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn publish_manifest_reports_persisted_not_visible_on_cas_loss() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let persisted_a =
            MicroCompactor::new(storage.clone()).with_durability_mode(DurabilityMode::Persisted);
        let persisted_b =
            MicroCompactor::new(storage.clone()).with_durability_mode(DurabilityMode::Persisted);

        let mut base = OrchestrationManifest::new("01HQXYZ210REV");
        base.manifest_id = "00000000000000000000".to_string();
        base.epoch = 5;
        let base_outcome = persisted_a
            .publish_manifest(&base, None, None, None, None, None, None)
            .await?;
        assert_eq!(base_outcome, PublishOutcome::Visible);

        let (stale_manifest, stale_version, stale_pointer, stale_pointer_bytes) =
            persisted_a.read_manifest_with_version().await?;
        let stale_version = stale_version.expect("pointer version");
        let stale_pointer = stale_pointer.expect("pointer");
        let stale_pointer_bytes = stale_pointer_bytes.expect("pointer bytes");

        let mut winner = OrchestrationManifest::new("01HQXYZ211REV");
        winner.manifest_id = "00000000000000000001".to_string();
        winner.epoch = 5;
        let winner_outcome = persisted_a
            .publish_manifest(
                &winner,
                Some(&stale_manifest),
                Some(&stale_version),
                Some(&stale_pointer),
                Some(&stale_pointer_bytes),
                None,
                None,
            )
            .await?;
        assert_eq!(winner_outcome, PublishOutcome::Visible);

        let mut stale = OrchestrationManifest::new("01HQXYZ212REV");
        stale.manifest_id = "00000000000000000002".to_string();
        stale.epoch = 5;
        let stale_outcome = persisted_b
            .publish_manifest(
                &stale,
                Some(&stale_manifest),
                Some(&stale_version),
                Some(&stale_pointer),
                Some(&stale_pointer_bytes),
                None,
                None,
            )
            .await?;
        assert_eq!(stale_outcome, PublishOutcome::PersistedNotVisible);

        Ok(())
    }

    #[test]
    fn event_priority_orders_trigger_before_run_requested() {
        let tick = OrchestrationEventData::ScheduleTicked {
            schedule_id: "sched-01".to_string(),
            scheduled_for: Utc::now(),
            tick_id: "sched-01:1".to_string(),
            definition_version: "v1".to_string(),
            asset_selection: vec!["asset.a".to_string()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some("sched:1".to_string()),
            request_fingerprint: Some("fp-1".to_string()),
        };
        let run_requested = OrchestrationEventData::RunRequested {
            run_key: "sched:1".to_string(),
            request_fingerprint: "fp-1".to_string(),
            asset_selection: vec!["asset.a".to_string()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "sched-01".to_string(),
                tick_id: "sched-01:1".to_string(),
            },
            labels: HashMap::new(),
        };

        assert!(event_priority(&tick) < event_priority(&run_requested));
    }

    #[test]
    fn persisted_mode_advances_committed_watermark_without_advancing_visible() {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.watermarks = Watermarks {
            last_committed_event_id: Some("evt_01".to_string()),
            last_visible_event_id: Some("evt_01".to_string()),
            events_processed_through: Some("evt_01".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "evt_01")),
            last_processed_at: Utc::now(),
        };

        let mut event = make_plan_created_event();
        event.event_id = "evt_02".to_string();
        let path = orchestration_event_path("2025-01-15", &event.event_id);
        let events = vec![(path, event.clone())];

        let changed = update_watermarks(
            &mut manifest,
            &events,
            &event.event_id,
            DurabilityMode::Persisted,
        );

        assert!(changed);
        assert_eq!(
            manifest.watermarks.last_committed_event_id.as_deref(),
            Some("evt_02")
        );
        assert_eq!(
            manifest.watermarks.last_visible_event_id.as_deref(),
            Some("evt_01")
        );
        assert_eq!(
            manifest.watermarks.events_processed_through.as_deref(),
            Some("evt_01")
        );
    }

    #[test]
    fn visibility_lag_reports_zero_when_committed_is_visible() {
        let watermarks = Watermarks {
            last_committed_event_id: Some("evt_03".to_string()),
            last_visible_event_id: Some("evt_03".to_string()),
            events_processed_through: Some("evt_03".to_string()),
            last_processed_file: None,
            last_processed_at: Utc::now(),
        };
        assert_eq!(visibility_lag_events(&watermarks), 0.0);
    }

    #[test]
    fn visibility_lag_reports_one_when_visibility_trails_commit() {
        let watermarks = Watermarks {
            last_committed_event_id: Some("evt_03".to_string()),
            last_visible_event_id: Some("evt_02".to_string()),
            events_processed_through: Some("evt_02".to_string()),
            last_processed_file: None,
            last_processed_at: Utc::now(),
        };
        assert_eq!(visibility_lag_events(&watermarks), 1.0);
    }
}
