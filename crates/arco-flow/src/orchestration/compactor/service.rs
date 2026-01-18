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

use arco_core::{DistributedLock, ScopedStorage, WritePrecondition, WriteResult};
use metrics::{counter, gauge, histogram};

use crate::error::{Error, Result};
use crate::metrics::{labels as metric_labels, names as metric_names};
use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use crate::paths::{
    orchestration_compaction_lock_path, orchestration_l0_dir, orchestration_manifest_path,
    orchestration_manifest_pointer_path, orchestration_manifest_snapshot_path,
};

use arco_core::publish::{
    SnapshotPointerDurability, SnapshotPointerPublishOutcome, publish_snapshot_pointer_transaction,
};

use super::fold::{
    FoldState, merge_dep_satisfaction_rows, merge_dispatch_outbox_rows, merge_idempotency_key_rows,
    merge_partition_status_rows, merge_run_rows, merge_schedule_definition_rows,
    merge_schedule_state_rows, merge_schedule_tick_rows, merge_sensor_eval_rows,
    merge_sensor_state_rows, merge_task_rows, merge_timer_rows,
};
use super::manifest::{
    EventRange, L0Delta, OrchestrationManifest, OrchestrationManifestPointer, RowCounts,
    TablePaths, Watermarks, next_manifest_id,
};
use super::parquet_util::{
    read_partition_status, write_dep_satisfaction, write_dispatch_outbox, write_idempotency_keys,
    write_partition_status, write_runs, write_schedule_definitions, write_schedule_state,
    write_schedule_ticks, write_sensor_evals, write_sensor_state, write_tasks, write_timers,
};

const SENSOR_EVAL_RETENTION_DAYS: i64 = 30;
const IDEMPOTENCY_KEY_RETENTION_DAYS: i64 = 30;

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

#[derive(Debug, Clone)]
struct FencingValidation {
    lock_path: String,
    request_epoch: u64,
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
        let (manifest, _, _) = self.read_manifest_with_version().await?;
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
        self.compact_events_inner(event_paths, None).await
    }

    /// Runs micro-compaction with explicit fencing requirements.
    ///
    /// This is the preferred entrypoint for lock-protected callers. It enforces:
    /// - `request_epoch == current_lock_epoch`
    /// - `request_epoch >= pointer_epoch`
    /// - lock revalidation immediately before pointer CAS publish
    ///
    /// # Errors
    ///
    /// Returns `Error::StaleFencingToken` or `Error::FencingLockUnavailable`
    /// when the caller's epoch is stale or lock state is invalid.
    pub async fn compact_events_fenced(
        &self,
        event_paths: Vec<String>,
        fencing_token: u64,
        lock_path: &str,
    ) -> Result<CompactionResult> {
        self.compact_events_inner(
            event_paths,
            Some(FencingValidation {
                lock_path: lock_path.to_string(),
                request_epoch: fencing_token,
            }),
        )
        .await
    }

    /// Compatibility wrapper that applies default orchestration lock fencing.
    ///
    /// New callers should use `compact_events_fenced` to supply lock path explicitly.
    /// Existing callers may continue passing an epoch only.
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads/writes fail, Parquet encoding fails,
    /// or the supplied epoch is stale relative to lock/pointer epoch.
    pub async fn compact_events_with_epoch(
        &self,
        event_paths: Vec<String>,
        expected_epoch: Option<u64>,
    ) -> Result<CompactionResult> {
        match expected_epoch {
            Some(epoch) => {
                self.compact_events_fenced(event_paths, epoch, orchestration_compaction_lock_path())
                    .await
            }
            None => self.compact_events_inner(event_paths, None).await,
        }
    }

    /// Runs micro-compaction and validates fencing context when supplied.
    ///
    /// # Arguments
    /// * `event_paths` - Explicit paths to event files to process
    /// * `fencing` - Optional lock/epoch associated with this write attempt
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads/writes fail, Parquet encoding fails,
    /// or fencing validation fails.
    #[tracing::instrument(skip(self, event_paths), fields(event_count = event_paths.len()))]
    #[allow(clippy::too_many_lines)]
    async fn compact_events_inner(
        &self,
        event_paths: Vec<String>,
        fencing: Option<FencingValidation>,
    ) -> Result<CompactionResult> {
        let ack_started = Instant::now();

        if let Some(fencing) = fencing.as_ref() {
            self.validate_lock_epoch(fencing).await?;
        }

        // Load current manifest + version for CAS
        let (mut manifest, pointer_version, previous_pointer) =
            self.read_manifest_with_version().await?;
        validate_fencing_against_pointer(fencing.as_ref(), previous_pointer.as_ref())?;

        if event_paths.is_empty() {
            let mut visibility_status = CompactionVisibility::Visible;
            if pointer_version.is_none() {
                match self
                    .publish_manifest(&manifest, None, previous_pointer.as_ref(), fencing.as_ref())
                    .await
                {
                    Ok(outcome) => visibility_status = outcome,
                    Err(error) => {
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

        // Write L0 delta Parquet files if anything changed
        let mut delta_id = None;
        let mut delta_paths = TablePaths::default();
        if has_delta {
            let new_delta_id = Ulid::new().to_string();
            delta_paths = self
                .write_delta_parquet(&new_delta_id, &delta_state)
                .await?;
            delta_id = Some(new_delta_id);
        }

        // Count rows
        let row_counts = RowCounts {
            runs: u32::try_from(delta_state.runs.len()).unwrap_or(u32::MAX),
            tasks: u32::try_from(delta_state.tasks.len()).unwrap_or(u32::MAX),
            dep_satisfaction: u32::try_from(delta_state.dep_satisfaction.len()).unwrap_or(u32::MAX),
            timers: u32::try_from(delta_state.timers.len()).unwrap_or(u32::MAX),
            dispatch_outbox: u32::try_from(delta_state.dispatch_outbox.len()).unwrap_or(u32::MAX),
            sensor_state: u32::try_from(delta_state.sensor_state.len()).unwrap_or(u32::MAX),
            sensor_evals: u32::try_from(delta_state.sensor_evals.len()).unwrap_or(u32::MAX),
            run_key_index: u32::try_from(delta_state.run_key_index.len()).unwrap_or(u32::MAX),
            run_key_conflicts: u32::try_from(delta_state.run_key_conflicts.len())
                .unwrap_or(u32::MAX),
            partition_status: u32::try_from(delta_state.partition_status.len()).unwrap_or(u32::MAX),
            idempotency_keys: u32::try_from(delta_state.idempotency_keys.len()).unwrap_or(u32::MAX),
            schedule_definitions: u32::try_from(delta_state.schedule_definitions.len())
                .unwrap_or(u32::MAX),
            schedule_state: u32::try_from(delta_state.schedule_state.len()).unwrap_or(u32::MAX),
            schedule_ticks: u32::try_from(delta_state.schedule_ticks.len()).unwrap_or(u32::MAX),
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
        let delta_changed = if let Some(delta_id) = delta_id.clone() {
            let delta = L0Delta {
                delta_id,
                created_at: Utc::now(),
                event_range: EventRange {
                    from_event: first_event.clone(),
                    to_event: last_event.clone(),
                    event_count: u32::try_from(events.len()).unwrap_or(u32::MAX),
                },
                tables: delta_paths,
                row_counts,
            };

            // Update manifest
            manifest.l0_deltas.push(delta);
            manifest.l0_count += 1;
            true
        } else {
            false
        };

        let watermark_changed =
            update_watermarks(&mut manifest, &events, &last_event, self.durability_mode);
        let manifest_changed = delta_changed || watermark_changed;

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
            if let Some(fencing) = fencing.as_ref() {
                manifest.epoch = manifest.epoch.max(fencing.request_epoch);
            }

            let new_revision = Ulid::new().to_string();
            manifest.revision_ulid.clone_from(&new_revision);
            manifest.published_at = Utc::now();
            match self
                .publish_manifest(
                    &manifest,
                    pointer_version.as_deref(),
                    previous_pointer.as_ref(),
                    fencing.as_ref(),
                )
                .await
            {
                Ok(outcome) => visibility_status = outcome,
                Err(error) => {
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

        Ok(CompactionResult {
            events_processed: u32::try_from(events.len()).unwrap_or(u32::MAX),
            delta_id,
            manifest_revision,
            visibility_status,
        })
    }

    /// Reads the current manifest and pointer CAS version.
    async fn read_manifest_with_version(
        &self,
    ) -> Result<(
        OrchestrationManifest,
        Option<String>,
        Option<OrchestrationManifestPointer>,
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

            return Ok((manifest, Some(meta.version), Some(pointer)));
        }

        let manifest_path = orchestration_manifest_path();
        match self.storage.head_raw(manifest_path).await? {
            Some(_) => {
                let data = self.storage.get_raw(manifest_path).await?;
                let manifest: OrchestrationManifest =
                    serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                        message: format!("failed to parse manifest: {e}"),
                    })?;
                Ok((manifest, None, None))
            }
            None => Ok((
                OrchestrationManifest::new(Ulid::new().to_string()),
                None,
                None,
            )),
        }
    }

    #[cfg(test)]
    async fn get_or_create_manifest(&self) -> Result<OrchestrationManifest> {
        let (manifest, _, _) = self.read_manifest_with_version().await?;
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
            let data = self.storage.get_raw(runs_path).await?;
            let rows = super::parquet_util::read_runs(&data)?;
            for row in rows {
                state.runs.insert(row.run_id.clone(), row);
            }
        }

        if let Some(ref tasks_path) = tables.tasks {
            let data = self.storage.get_raw(tasks_path).await?;
            let rows = super::parquet_util::read_tasks(&data)?;
            for row in rows {
                state
                    .tasks
                    .insert((row.run_id.clone(), row.task_key.clone()), row);
            }
        }

        if let Some(ref deps_path) = tables.dep_satisfaction {
            let data = self.storage.get_raw(deps_path).await?;
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
            let data = self.storage.get_raw(timers_path).await?;
            let rows = super::parquet_util::read_timers(&data)?;
            for row in rows {
                state.timers.insert(row.timer_id.clone(), row);
            }
        }

        if let Some(ref outbox_path) = tables.dispatch_outbox {
            let data = self.storage.get_raw(outbox_path).await?;
            let rows = super::parquet_util::read_dispatch_outbox(&data)?;
            for row in rows {
                state.dispatch_outbox.insert(row.dispatch_id.clone(), row);
            }
        }

        if let Some(ref sensor_state_path) = tables.sensor_state {
            let data = self.storage.get_raw(sensor_state_path).await?;
            let rows = super::parquet_util::read_sensor_state(&data)?;
            for row in rows {
                state.sensor_state.insert(row.sensor_id.clone(), row);
            }
        }

        if let Some(ref sensor_evals_path) = tables.sensor_evals {
            let data = self.storage.get_raw(sensor_evals_path).await?;
            let rows = super::parquet_util::read_sensor_evals(&data)?;
            for row in rows {
                state.sensor_evals.insert(row.eval_id.clone(), row);
            }
        }

        if let Some(ref backfills_path) = tables.backfills {
            let data = self.storage.get_raw(backfills_path).await?;
            let rows = super::parquet_util::read_backfills(&data)?;
            for row in rows {
                state.backfills.insert(row.backfill_id.clone(), row);
            }
        }

        if let Some(ref backfill_chunks_path) = tables.backfill_chunks {
            let data = self.storage.get_raw(backfill_chunks_path).await?;
            let rows = super::parquet_util::read_backfill_chunks(&data)?;
            for row in rows {
                state.backfill_chunks.insert(row.chunk_id.clone(), row);
            }
        }

        if let Some(ref run_key_index_path) = tables.run_key_index {
            let data = self.storage.get_raw(run_key_index_path).await?;
            let rows = super::parquet_util::read_run_key_index(&data)?;
            for row in rows {
                state.run_key_index.insert(row.run_key.clone(), row);
            }
        }

        if let Some(ref run_key_conflicts_path) = tables.run_key_conflicts {
            let data = self.storage.get_raw(run_key_conflicts_path).await?;
            let rows = super::parquet_util::read_run_key_conflicts(&data)?;
            for row in rows {
                let conflict_id = format!("conflict:{}:{}", row.run_key, row.conflicting_event_id);
                state.run_key_conflicts.insert(conflict_id, row);
            }
        }

        if let Some(ref partition_status_path) = tables.partition_status {
            let data = self.storage.get_raw(partition_status_path).await?;
            let rows = read_partition_status(&data)?;
            for row in rows {
                let key = (row.asset_key.clone(), row.partition_key.clone());
                state.partition_status.insert(key, row);
            }
        }

        if let Some(ref idempotency_keys_path) = tables.idempotency_keys {
            let data = self.storage.get_raw(idempotency_keys_path).await?;
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
            let data = self.storage.get_raw(schedule_definitions_path).await?;
            let rows = super::parquet_util::read_schedule_definitions(&data)?;
            for row in rows {
                state
                    .schedule_definitions
                    .insert(row.schedule_id.clone(), row);
            }
        }

        if let Some(ref schedule_state_path) = tables.schedule_state {
            let data = self.storage.get_raw(schedule_state_path).await?;
            let rows = super::parquet_util::read_schedule_state(&data)?;
            for row in rows {
                state.schedule_state.insert(row.schedule_id.clone(), row);
            }
        }

        if let Some(ref schedule_ticks_path) = tables.schedule_ticks {
            let data = self.storage.get_raw(schedule_ticks_path).await?;
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
        let mut paths = TablePaths::default();
        let base_path = orchestration_l0_dir(delta_id);

        macro_rules! write_table {
            ($file:literal, $rows:expr, $encode:expr, $out:expr) => {
                self.write_map_table_if_nonempty(&base_path, $file, $rows, $encode, $out)
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

        if !state.schedule_definitions.is_empty() {
            let rows: Vec<_> = state.schedule_definitions.values().cloned().collect();
            let bytes = write_schedule_definitions(&rows)?;
            let path = format!("{base_path}/schedule_definitions.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.schedule_definitions = Some(path);
        }

        if !state.schedule_state.is_empty() {
            let rows: Vec<_> = state.schedule_state.values().cloned().collect();
            let bytes = write_schedule_state(&rows)?;
            let path = format!("{base_path}/schedule_state.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.schedule_state = Some(path);
        }

        if !state.schedule_ticks.is_empty() {
            let rows: Vec<_> = state.schedule_ticks.values().cloned().collect();
            let bytes = write_schedule_ticks(&rows)?;
            let path = format!("{base_path}/schedule_ticks.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.schedule_ticks = Some(path);
        }

        Ok(paths)
    }

    async fn write_map_table_if_nonempty<K, R>(
        &self,
        base_path: &str,
        file: &str,
        rows: &std::collections::HashMap<K, R>,
        encode: fn(&[R]) -> Result<Bytes>,
        out: &mut Option<String>,
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

        let path = format!("{base_path}/{file}");
        self.write_parquet_file(&path, bytes).await?;
        *out = Some(path);
        Ok(())
    }

    /// Writes a Parquet file to storage.
    async fn write_parquet_file(&self, path: &str, bytes: Bytes) -> Result<()> {
        let result = self
            .storage
            .put_raw(path, bytes, WritePrecondition::None)
            .await?;

        match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                // Shouldn't happen with None precondition
                Err(Error::storage("unexpected precondition failure"))
            }
        }
    }

    /// Publishes a new manifest version.
    async fn publish_manifest(
        &self,
        manifest: &OrchestrationManifest,
        current_pointer_version: Option<&str>,
        previous_pointer: Option<&OrchestrationManifestPointer>,
        fencing: Option<&FencingValidation>,
    ) -> Result<PublishOutcome> {
        let snapshot_path = orchestration_manifest_snapshot_path(&manifest.manifest_id);
        let manifest_json =
            serde_json::to_string_pretty(manifest).map_err(|e| Error::Serialization {
                message: format!("failed to serialize manifest: {e}"),
            })?;

        let pointer_path = orchestration_manifest_pointer_path();
        let parent_pointer_hash = match previous_pointer {
            Some(_) => match self.storage.get_raw(pointer_path).await {
                Ok(pointer_bytes) => Some(sha256_prefixed(&pointer_bytes)),
                Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                    None
                }
                Err(e) => return Err(e.into()),
            },
            None => None,
        };

        let pointer = OrchestrationManifestPointer {
            manifest_id: manifest.manifest_id.clone(),
            manifest_path: snapshot_path,
            epoch: manifest.epoch,
            parent_pointer_hash,
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

        let outcome = publish_snapshot_pointer_transaction(
            &self.storage,
            &pointer.manifest_path,
            Bytes::from(manifest_json.clone()),
            pointer_path,
            Bytes::from(pointer_json),
            current_pointer_version,
            Some((orchestration_manifest_path(), Bytes::from(manifest_json))),
            durability,
            async {
                if let Some(fencing) = fencing {
                    // Mandatory pre-publish lock revalidation for hard stale-writer fencing.
                    self.validate_lock_epoch_core(fencing).await?;
                }
                Ok(())
            },
        )
        .await?;

        match outcome {
            SnapshotPointerPublishOutcome::Visible { .. } => Ok(PublishOutcome::Visible),
            SnapshotPointerPublishOutcome::PersistedNotVisible => {
                tracing::warn!(
                    manifest_id = %manifest.manifest_id,
                    "pointer CAS lost race in Persisted mode; returning persisted acknowledgement"
                );
                Ok(PublishOutcome::PersistedNotVisible)
            }
        }
    }

    async fn validate_lock_epoch(&self, fencing: &FencingValidation) -> Result<()> {
        let lock = DistributedLock::new(self.storage.backend().clone(), &fencing.lock_path);
        let lock_info = lock.read_lock_info().await?;

        let Some(lock_info) = lock_info else {
            counter!(
                metric_names::ORCH_COMPACTOR_STALE_FENCE_REJECTS_TOTAL,
                "reason" => "lock_unavailable".to_string()
            )
            .increment(1);
            return Err(Error::FencingLockUnavailable {
                lock_path: fencing.lock_path.clone(),
                provided: fencing.request_epoch,
            });
        };

        if lock_info.is_expired() {
            counter!(
                metric_names::ORCH_COMPACTOR_STALE_FENCE_REJECTS_TOTAL,
                "reason" => "lock_expired".to_string()
            )
            .increment(1);
            return Err(Error::FencingLockUnavailable {
                lock_path: fencing.lock_path.clone(),
                provided: fencing.request_epoch,
            });
        }

        if lock_info.sequence_number != fencing.request_epoch {
            counter!(
                metric_names::ORCH_COMPACTOR_STALE_FENCE_REJECTS_TOTAL,
                "reason" => "lock_epoch_mismatch".to_string()
            )
            .increment(1);
            return Err(Error::StaleFencingToken {
                expected: lock_info.sequence_number,
                provided: fencing.request_epoch,
            });
        }

        Ok(())
    }

    async fn validate_lock_epoch_core(&self, fencing: &FencingValidation) -> arco_core::Result<()> {
        let lock = DistributedLock::new(self.storage.backend().clone(), &fencing.lock_path);
        let lock_info = lock.read_lock_info().await?;

        let Some(lock_info) = lock_info else {
            return Err(arco_core::Error::PreconditionFailed {
                message: format!(
                    "fencing lock unavailable at {} for provided epoch {}",
                    fencing.lock_path, fencing.request_epoch
                ),
            });
        };

        if lock_info.is_expired() {
            return Err(arco_core::Error::PreconditionFailed {
                message: format!(
                    "fencing lock unavailable at {} for provided epoch {}",
                    fencing.lock_path, fencing.request_epoch
                ),
            });
        }

        if lock_info.sequence_number != fencing.request_epoch {
            return Err(arco_core::Error::PreconditionFailed {
                message: format!(
                    "stale fencing token: expected {}, got {}",
                    lock_info.sequence_number, fencing.request_epoch
                ),
            });
        }

        Ok(())
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

    for (schedule_id, row) in &current.schedule_definitions {
        if base.schedule_definitions.get(schedule_id) != Some(row) {
            delta
                .schedule_definitions
                .insert(schedule_id.clone(), row.clone());
        }
    }

    for (schedule_id, row) in &current.schedule_state {
        if base.schedule_state.get(schedule_id) != Some(row) {
            delta
                .schedule_state
                .insert(schedule_id.clone(), row.clone());
        }
    }

    for (tick_id, row) in &current.schedule_ticks {
        if base.schedule_ticks.get(tick_id) != Some(row) {
            delta.schedule_ticks.insert(tick_id.clone(), row.clone());
        }
    }

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

fn validate_fencing_against_pointer(
    fencing: Option<&FencingValidation>,
    pointer: Option<&OrchestrationManifestPointer>,
) -> Result<()> {
    let (Some(fencing), Some(pointer)) = (fencing, pointer) else {
        return Ok(());
    };

    if fencing.request_epoch < pointer.epoch {
        counter!(
            metric_names::ORCH_COMPACTOR_STALE_FENCE_REJECTS_TOTAL,
            "reason" => "pointer_epoch_mismatch".to_string()
        )
        .increment(1);
        return Err(Error::StaleFencingToken {
            expected: pointer.epoch,
            provided: fencing.request_epoch,
        });
    }

    Ok(())
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

    let event_count = u64::try_from(events.len()).unwrap_or(u64::MAX);
    let (committed_count, visible_count) = watermark_event_counts(&manifest.watermarks);
    let next_committed_count = committed_count.saturating_add(event_count);

    let last_path = events.last().map(|(path, _)| path.clone());
    let previous_visible = manifest.watermarks.last_visible_event_id.clone();
    let next_visible = if durability_mode == DurabilityMode::Visible {
        Some(last_event_id.to_string())
    } else {
        previous_visible
    };

    manifest.watermarks = Watermarks {
        last_committed_event_id: Some(last_event_id.to_string()),
        committed_event_count: Some(next_committed_count),
        last_visible_event_id: next_visible.clone(),
        visible_event_count: Some(if durability_mode == DurabilityMode::Visible {
            next_committed_count
        } else {
            visible_count
        }),
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

#[allow(clippy::cast_precision_loss)]
fn visibility_lag_events(watermarks: &Watermarks) -> f64 {
    let (committed_count, visible_count) = watermark_event_counts(watermarks);
    committed_count.saturating_sub(visible_count) as f64
}

fn watermark_event_counts(watermarks: &Watermarks) -> (u64, u64) {
    let committed_count = watermarks.committed_event_count.unwrap_or({
        match watermarks.last_committed_event_id.as_deref() {
            Some(_) => 1,
            None => 0,
        }
    });

    let visible_count = watermarks.visible_event_count.unwrap_or_else(|| {
        match (
            watermarks.last_committed_event_id.as_deref(),
            watermarks.last_visible_event_id.as_deref(),
        ) {
            (Some(committed), Some(visible)) if committed == visible => committed_count,
            (Some(_), Some(_)) => committed_count.saturating_sub(1),
            (Some(_), None) | (None, _) => 0,
        }
    });

    (committed_count, visible_count.min(committed_count))
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
    use arco_core::{DistributedLock, MemoryBackend};
    use chrono::DateTime;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use ulid::Ulid;

    async fn create_test_compactor() -> Result<(MicroCompactor, ScopedStorage)> {
        let backend = Arc::new(MemoryBackend::new());
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

    #[tokio::test]
    async fn compact_empty_events_returns_current_manifest() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let result = compactor.compact_events(vec![]).await?;

        assert_eq!(result.events_processed, 0);
        assert!(result.delta_id.is_none());

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
            .publish_manifest(&manifest, None, None, None)
            .await?;

        let (_current, version, pointer) = compactor.read_manifest_with_version().await?;
        let version = version.expect("pointer version");
        let pointer = pointer.expect("pointer");

        let mut competing = manifest.clone();
        competing.manifest_id = "00000000000000000001".to_string();
        competing.revision_ulid = "01HQXYZ124REV".to_string();
        compactor
            .publish_manifest(&competing, Some(&version), Some(&pointer), None)
            .await?;

        let mut stale = manifest;
        stale.manifest_id = "00000000000000000002".to_string();
        let err = compactor
            .publish_manifest(&stale, Some(&version), Some(&pointer), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("manifest publish failed"));

        Ok(())
    }

    #[tokio::test]
    async fn compact_with_stale_epoch_is_rejected() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let lock_path = orchestration_compaction_lock_path();
        let lock = DistributedLock::new(storage.backend().clone(), lock_path);
        let guard = lock.acquire(Duration::from_secs(30), 1).await?;
        let request_epoch = guard.fencing_token().sequence();

        let mut manifest = OrchestrationManifest::new("01HQXYZ200REV");
        manifest.manifest_id = "00000000000000000000".to_string();
        manifest.epoch = request_epoch.saturating_add(1);
        compactor
            .publish_manifest(&manifest, None, None, None)
            .await?;

        let error = compactor
            .compact_events_fenced(vec![], request_epoch, lock_path)
            .await
            .expect_err("stale epoch must be rejected");
        assert!(error.to_string().contains("stale fencing token"));

        drop(guard);
        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_rejects_without_current_lock() -> Result<()> {
        let (compactor, _storage) = create_test_compactor().await?;

        let error = compactor
            .compact_events_fenced(vec![], 1, orchestration_compaction_lock_path())
            .await
            .expect_err("missing lock must be rejected");
        assert!(error.to_string().contains("fencing lock unavailable"));

        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_rejects_on_lock_epoch_mismatch() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let lock_path = orchestration_compaction_lock_path();
        let lock = DistributedLock::new(storage.backend().clone(), lock_path);
        let guard = lock.acquire(Duration::from_secs(30), 1).await?;
        let stale_epoch = guard.fencing_token().sequence().saturating_add(1);

        let error = compactor
            .compact_events_fenced(vec![], stale_epoch, lock_path)
            .await
            .expect_err("epoch mismatch must be rejected");
        assert!(error.to_string().contains("stale fencing token"));

        drop(guard);
        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_rejects_when_pointer_epoch_is_newer() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let lock_path = orchestration_compaction_lock_path();
        let lock = DistributedLock::new(storage.backend().clone(), lock_path);
        let guard = lock.acquire(Duration::from_secs(30), 1).await?;
        let epoch = guard.fencing_token().sequence();

        let mut manifest = OrchestrationManifest::new("01HQXYZ220REV");
        manifest.manifest_id = "00000000000000000000".to_string();
        manifest.epoch = epoch.saturating_add(5);
        compactor
            .publish_manifest(&manifest, None, None, None)
            .await?;

        let error = compactor
            .compact_events_fenced(vec![], epoch, lock_path)
            .await
            .expect_err("stale pointer epoch must be rejected");
        assert!(error.to_string().contains("stale fencing token"));

        drop(guard);
        Ok(())
    }

    #[tokio::test]
    async fn compact_events_fenced_accepts_matching_lock_epoch() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;
        let lock_path = orchestration_compaction_lock_path();
        let lock = DistributedLock::new(storage.backend().clone(), lock_path);
        let guard = lock.acquire(Duration::from_secs(30), 1).await?;
        let epoch = guard.fencing_token().sequence();

        let result = compactor
            .compact_events_fenced(vec![], epoch, lock_path)
            .await?;
        assert_eq!(result.events_processed, 0);

        drop(guard);
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
            .publish_manifest(&base, None, None, None)
            .await?;
        assert_eq!(base_outcome, PublishOutcome::Visible);

        let (_manifest, stale_version, stale_pointer) =
            persisted_a.read_manifest_with_version().await?;
        let stale_version = stale_version.expect("pointer version");
        let stale_pointer = stale_pointer.expect("pointer");

        let mut winner = OrchestrationManifest::new("01HQXYZ211REV");
        winner.manifest_id = "00000000000000000001".to_string();
        winner.epoch = 5;
        let winner_outcome = persisted_a
            .publish_manifest(&winner, Some(&stale_version), Some(&stale_pointer), None)
            .await?;
        assert_eq!(winner_outcome, PublishOutcome::Visible);

        let mut stale = OrchestrationManifest::new("01HQXYZ212REV");
        stale.manifest_id = "00000000000000000002".to_string();
        stale.epoch = 5;
        let stale_outcome = persisted_b
            .publish_manifest(&stale, Some(&stale_version), Some(&stale_pointer), None)
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
            committed_event_count: Some(1),
            last_visible_event_id: Some("evt_01".to_string()),
            visible_event_count: Some(1),
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
        assert_eq!(manifest.watermarks.committed_event_count, Some(2));
        assert_eq!(manifest.watermarks.visible_event_count, Some(1));
    }

    #[test]
    fn visibility_lag_reports_zero_when_committed_is_visible() {
        let watermarks = Watermarks {
            last_committed_event_id: Some("evt_03".to_string()),
            committed_event_count: Some(3),
            last_visible_event_id: Some("evt_03".to_string()),
            visible_event_count: Some(3),
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
            committed_event_count: Some(3),
            last_visible_event_id: Some("evt_02".to_string()),
            visible_event_count: Some(2),
            events_processed_through: Some("evt_02".to_string()),
            last_processed_file: None,
            last_processed_at: Utc::now(),
        };
        assert_eq!(visibility_lag_events(&watermarks), 1.0);
    }

    #[test]
    fn visibility_lag_reports_quantitative_gap_when_visibility_lags_multiple_events() {
        let watermarks = Watermarks {
            last_committed_event_id: Some("evt_42".to_string()),
            committed_event_count: Some(42),
            last_visible_event_id: Some("evt_37".to_string()),
            visible_event_count: Some(37),
            events_processed_through: Some("evt_37".to_string()),
            last_processed_file: None,
            last_processed_at: Utc::now(),
        };
        assert_eq!(visibility_lag_events(&watermarks), 5.0);
    }

    #[test]
    fn persisted_mode_advances_committed_counter_only() {
        let mut manifest = OrchestrationManifest::new("01HQXYZ124REV");
        manifest.watermarks = Watermarks {
            last_committed_event_id: Some("evt_10".to_string()),
            committed_event_count: Some(10),
            last_visible_event_id: Some("evt_10".to_string()),
            visible_event_count: Some(10),
            events_processed_through: Some("evt_10".to_string()),
            last_processed_file: Some(orchestration_event_path("2025-01-15", "evt_10")),
            last_processed_at: Utc::now(),
        };

        let mut event = make_plan_created_event();
        event.event_id = "evt_11".to_string();
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
            Some("evt_11")
        );
        assert_eq!(
            manifest.watermarks.last_visible_event_id.as_deref(),
            Some("evt_10")
        );
        assert_eq!(manifest.watermarks.committed_event_count, Some(11));
        assert_eq!(manifest.watermarks.visible_event_count, Some(10));
        assert_eq!(visibility_lag_events(&manifest.watermarks), 1.0);
    }
}
