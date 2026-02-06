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
use chrono::Utc;
use std::cmp::Ordering;
use ulid::Ulid;

use arco_core::{ScopedStorage, WritePrecondition, WriteResult};

use crate::error::{Error, Result};
use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use crate::paths::{orchestration_l0_dir, orchestration_manifest_path};

use super::fold::{
    FoldState, merge_dep_satisfaction_rows, merge_dispatch_outbox_rows, merge_idempotency_key_rows,
    merge_partition_status_rows, merge_run_rows, merge_schedule_definition_rows,
    merge_schedule_state_rows, merge_schedule_tick_rows, merge_sensor_eval_rows,
    merge_sensor_state_rows, merge_task_rows, merge_timer_rows,
};
use super::manifest::{
    EventRange, L0Delta, OrchestrationManifest, RowCounts, TablePaths, Watermarks,
};
use super::parquet_util::{
    read_partition_status, write_dep_satisfaction, write_dispatch_outbox, write_idempotency_keys,
    write_partition_status, write_run_key_conflicts, write_run_key_index, write_runs,
    write_schedule_definitions, write_schedule_state, write_schedule_ticks, write_sensor_evals,
    write_sensor_state, write_tasks, write_timers,
};

const SENSOR_EVAL_RETENTION_DAYS: i64 = 30;
const IDEMPOTENCY_KEY_RETENTION_DAYS: i64 = 30;

/// Micro-compactor for orchestration events.
///
/// Processes events from the ledger and writes L0 delta Parquet files.
/// Controllers read base snapshot + L0 deltas from the manifest.
#[derive(Clone)]
pub struct MicroCompactor {
    storage: ScopedStorage,
    tenant_secret: Vec<u8>,
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
}

impl MicroCompactor {
    /// Creates a new micro-compactor.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            tenant_secret: Vec::new(),
        }
    }

    /// Creates a new micro-compactor with a tenant HMAC secret.
    #[must_use]
    pub fn with_tenant_secret(storage: ScopedStorage, tenant_secret: Vec<u8>) -> Self {
        Self {
            storage,
            tenant_secret,
        }
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
        let (manifest, _) = self.read_manifest_with_version().await?;
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
    #[tracing::instrument(skip(self, event_paths), fields(event_count = event_paths.len()))]
    #[allow(clippy::too_many_lines)]
    pub async fn compact_events(&self, event_paths: Vec<String>) -> Result<CompactionResult> {
        // Load current manifest + version for CAS
        let (mut manifest, manifest_version) = self.read_manifest_with_version().await?;

        if event_paths.is_empty() {
            if manifest_version.is_none() {
                self.publish_manifest(&manifest, None).await?;
            }
            return Ok(CompactionResult {
                events_processed: 0,
                delta_id: None,
                manifest_revision: manifest.revision_ulid,
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

        prune_sensor_evals(&mut state);
        prune_idempotency_keys(&mut state);

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

        let watermark_changed = update_watermarks(&mut manifest, &events, &last_event);
        let manifest_changed = delta_changed || watermark_changed;

        let manifest_revision = if manifest_changed {
            let new_revision = Ulid::new().to_string();
            manifest.revision_ulid.clone_from(&new_revision);
            manifest.published_at = Utc::now();
            self.publish_manifest(&manifest, manifest_version.as_deref())
                .await?;
            new_revision
        } else {
            manifest.revision_ulid.clone()
        };

        Ok(CompactionResult {
            events_processed: u32::try_from(events.len()).unwrap_or(u32::MAX),
            delta_id,
            manifest_revision,
        })
    }

    /// Reads the current manifest and version for CAS publishing.
    async fn read_manifest_with_version(&self) -> Result<(OrchestrationManifest, Option<String>)> {
        let manifest_path = orchestration_manifest_path();

        match self.storage.head_raw(manifest_path).await? {
            Some(meta) => {
                let data = self.storage.get_raw(manifest_path).await?;
                let manifest: OrchestrationManifest =
                    serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                        message: format!("failed to parse manifest: {e}"),
                    })?;
                Ok((manifest, Some(meta.version)))
            }
            None => Ok((OrchestrationManifest::new(Ulid::new().to_string()), None)),
        }
    }

    #[cfg(test)]
    async fn get_or_create_manifest(&self) -> Result<OrchestrationManifest> {
        let (manifest, _) = self.read_manifest_with_version().await?;
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

        prune_sensor_evals(&mut state);
        prune_idempotency_keys(&mut state);
        state.rebuild_dependency_graph();

        Ok(state)
    }

    /// Loads state from a snapshot.
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
        current_version: Option<&str>,
    ) -> Result<()> {
        let manifest_path = orchestration_manifest_path();
        let json = serde_json::to_string_pretty(manifest).map_err(|e| Error::Serialization {
            message: format!("failed to serialize manifest: {e}"),
        })?;

        let precondition = current_version.map_or(WritePrecondition::DoesNotExist, |version| {
            WritePrecondition::MatchesVersion(version.to_string())
        });

        let result = self
            .storage
            .put_raw(manifest_path, Bytes::from(json), precondition)
            .await?;

        match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                Err(Error::storage("manifest publish failed - concurrent write"))
            }
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
        && state.run_key_index.is_empty()
        && state.run_key_conflicts.is_empty()
        && state.partition_status.is_empty()
        && state.idempotency_keys.is_empty()
        && state.schedule_definitions.is_empty()
        && state.schedule_state.is_empty()
        && state.schedule_ticks.is_empty()
}

fn prune_sensor_evals(state: &mut FoldState) {
    let cutoff = Utc::now() - chrono::Duration::days(SENSOR_EVAL_RETENTION_DAYS);
    state
        .sensor_evals
        .retain(|_, row| row.evaluated_at >= cutoff);
}

fn prune_idempotency_keys(state: &mut FoldState) {
    let cutoff = Utc::now() - chrono::Duration::days(IDEMPOTENCY_KEY_RETENTION_DAYS);
    state
        .idempotency_keys
        .retain(|_, row| row.recorded_at >= cutoff);
}

fn update_watermarks(
    manifest: &mut OrchestrationManifest,
    events: &[(String, OrchestrationEvent)],
    last_event_id: &str,
) -> bool {
    if events.is_empty() {
        return false;
    }

    let should_advance = manifest
        .watermarks
        .events_processed_through
        .as_deref()
        .is_none_or(|current| last_event_id > current);

    if should_advance {
        let last_path = events.last().map(|(path, _)| path.clone());
        manifest.watermarks = Watermarks {
            events_processed_through: Some(last_event_id.to_string()),
            last_processed_file: last_path,
            last_processed_at: Utc::now(),
        };
        return true;
    }

    false
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
        OrchestrationEventData, SourceRef, TaskDef, TickStatus, TimerType, TriggerInfo,
    };
    use crate::paths::orchestration_event_path;
    use arco_core::MemoryBackend;
    use chrono::DateTime;
    use std::collections::HashMap;
    use std::sync::Arc;
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
        let (compactor, storage) = create_test_compactor().await?;

        let manifest = OrchestrationManifest::new("01HQXYZ123REV");
        storage
            .put_raw(
                orchestration_manifest_path(),
                Bytes::from(serde_json::to_string(&manifest).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        let (_current, version) = compactor.read_manifest_with_version().await?;
        let version = version.expect("version");

        let bumped = OrchestrationManifest::new("01HQXYZ124REV");
        storage
            .put_raw(
                orchestration_manifest_path(),
                Bytes::from(serde_json::to_string(&bumped).expect("serialize")),
                WritePrecondition::None,
            )
            .await?;

        let err = compactor
            .publish_manifest(&manifest, Some(&version))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("manifest publish failed"));

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
}
