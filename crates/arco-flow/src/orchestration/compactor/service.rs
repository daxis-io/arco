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
use ulid::Ulid;

use arco_core::{ScopedStorage, WritePrecondition, WriteResult};

use crate::error::{Error, Result};
use crate::orchestration::events::OrchestrationEvent;

use super::fold::{
    merge_dep_satisfaction_rows,
    merge_dispatch_outbox_rows,
    merge_run_rows,
    merge_sensor_eval_rows,
    merge_sensor_state_rows,
    merge_task_rows,
    merge_timer_rows,
    FoldState,
};
use super::manifest::{EventRange, L0Delta, OrchestrationManifest, RowCounts, TablePaths, Watermarks};
use super::parquet_util::{
    write_dep_satisfaction,
    write_dispatch_outbox,
    write_runs,
    write_sensor_evals,
    write_sensor_state,
    write_tasks,
    write_timers,
};

const SENSOR_EVAL_RETENTION_DAYS: i64 = 30;

/// Micro-compactor for orchestration events.
///
/// Processes events from the ledger and writes L0 delta Parquet files.
/// Controllers read base snapshot + L0 deltas from the manifest.
#[derive(Clone)]
pub struct MicroCompactor {
    storage: ScopedStorage,
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
        Self { storage }
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
        let state = self.load_current_state(&manifest).await?;
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
        let base_state = state.clone();

        // Process events
        let mut events = Vec::new();
        for path in &event_paths {
            let data = self.storage.get_raw(path).await?;
            let event: OrchestrationEvent = serde_json::from_slice(&data)
                .map_err(|e| Error::Serialization {
                    message: format!("failed to parse event at {path}: {e}"),
                })?;
            events.push((path.clone(), event));
        }

        // Sort by event_id (ULID provides lexicographic time ordering)
        events.sort_by(|a, b| a.1.event_id.cmp(&b.1.event_id));

        // Fold events into state
        for (_, event) in &events {
            state.fold_event(event);
        }

        prune_sensor_evals(&mut state);

        // Compute delta state (rows changed by this batch)
        let delta_state = delta_from_states(&base_state, &state);
        let has_delta = !delta_state_is_empty(&delta_state);

        // Write L0 delta Parquet files if anything changed
        let mut delta_id = None;
        let mut delta_paths = TablePaths::default();
        if has_delta {
            let new_delta_id = Ulid::new().to_string();
            delta_paths = self.write_delta_parquet(&new_delta_id, &delta_state).await?;
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
        };

        // Create L0 delta entry when changes exist
        let first_event = events.first().map(|e| e.1.event_id.clone()).unwrap_or_default();
        let last_event = events.last().map(|e| e.1.event_id.clone()).unwrap_or_default();
        let mut manifest_changed = false;

        if let Some(delta_id) = delta_id.clone() {
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
            manifest_changed = true;
        }

        if update_watermarks(&mut manifest, &events, &last_event) {
            manifest_changed = true;
        }

        let manifest_revision = if manifest_changed {
            let new_revision = Ulid::new().to_string();
            manifest.revision_ulid.clone_from(&new_revision);
            manifest.published_at = Utc::now();
            self.publish_manifest(&manifest, manifest_version.as_deref()).await?;
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
    async fn read_manifest_with_version(
        &self,
    ) -> Result<(OrchestrationManifest, Option<String>)> {
        let manifest_path = "state/orchestration/manifest.json";

        match self.storage.head_raw(manifest_path).await? {
            Some(meta) => {
                let data = self.storage.get_raw(manifest_path).await?;
                let manifest: OrchestrationManifest = serde_json::from_slice(&data)
                    .map_err(|e| Error::Serialization {
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
        let mut state = FoldState::new();

        // Load base snapshot if exists
        if let Some(ref snapshot_id) = manifest.base_snapshot.snapshot_id {
            state = self.load_snapshot_state(snapshot_id, &manifest.base_snapshot.tables).await?;
        }

        // Apply L0 deltas in order
        for delta in &manifest.l0_deltas {
            let delta_state = self.load_delta_state(&delta.delta_id, &delta.tables).await?;
            state = merge_states(state, delta_state);
        }

        prune_sensor_evals(&mut state);
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
                state.tasks.insert((row.run_id.clone(), row.task_key.clone()), row);
            }
        }

        if let Some(ref deps_path) = tables.dep_satisfaction {
            let data = self.storage.get_raw(deps_path).await?;
            let rows = super::parquet_util::read_dep_satisfaction(&data)?;
            for row in rows {
                state.dep_satisfaction.insert(
                    (row.run_id.clone(), row.upstream_task_key.clone(), row.downstream_task_key.clone()),
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

        Ok(state)
    }

    /// Loads state from an L0 delta.
    async fn load_delta_state(&self, delta_id: &str, tables: &TablePaths) -> Result<FoldState> {
        // Same logic as snapshot loading
        self.load_snapshot_state(delta_id, tables).await
    }

    /// Writes fold state to L0 delta Parquet files.
    async fn write_delta_parquet(&self, delta_id: &str, state: &FoldState) -> Result<TablePaths> {
        let mut paths = TablePaths::default();
        let base_path = format!("state/orchestration/l0/{delta_id}");

        // Write runs
        if !state.runs.is_empty() {
            let rows: Vec<_> = state.runs.values().cloned().collect();
            let bytes = write_runs(&rows)?;
            let path = format!("{base_path}/runs.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.runs = Some(path);
        }

        // Write tasks
        if !state.tasks.is_empty() {
            let rows: Vec<_> = state.tasks.values().cloned().collect();
            let bytes = write_tasks(&rows)?;
            let path = format!("{base_path}/tasks.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.tasks = Some(path);
        }

        // Write dep_satisfaction
        if !state.dep_satisfaction.is_empty() {
            let rows: Vec<_> = state.dep_satisfaction.values().cloned().collect();
            let bytes = write_dep_satisfaction(&rows)?;
            let path = format!("{base_path}/dep_satisfaction.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.dep_satisfaction = Some(path);
        }

        if !state.timers.is_empty() {
            let rows: Vec<_> = state.timers.values().cloned().collect();
            let bytes = write_timers(&rows)?;
            let path = format!("{base_path}/timers.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.timers = Some(path);
        }

        if !state.dispatch_outbox.is_empty() {
            let rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();
            let bytes = write_dispatch_outbox(&rows)?;
            let path = format!("{base_path}/dispatch_outbox.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.dispatch_outbox = Some(path);
        }

        if !state.sensor_state.is_empty() {
            let rows: Vec<_> = state.sensor_state.values().cloned().collect();
            let bytes = write_sensor_state(&rows)?;
            let path = format!("{base_path}/sensor_state.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.sensor_state = Some(path);
        }

        if !state.sensor_evals.is_empty() {
            let rows: Vec<_> = state.sensor_evals.values().cloned().collect();
            let bytes = write_sensor_evals(&rows)?;
            let path = format!("{base_path}/sensor_evals.parquet");
            self.write_parquet_file(&path, bytes).await?;
            paths.sensor_evals = Some(path);
        }

        Ok(paths)
    }

    /// Writes a Parquet file to storage.
    async fn write_parquet_file(&self, path: &str, bytes: Bytes) -> Result<()> {
        let result = self.storage
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
        let manifest_path = "state/orchestration/manifest.json";
        let json = serde_json::to_string_pretty(manifest)
            .map_err(|e| Error::Serialization {
                message: format!("failed to serialize manifest: {e}"),
            })?;

        let precondition = current_version.map_or(
            WritePrecondition::DoesNotExist,
            |version| WritePrecondition::MatchesVersion(version.to_string()),
        );

        let result = self.storage
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
fn merge_states(base: FoldState, delta: FoldState) -> FoldState {
    let mut merged = base;

    // Merge runs (newer row_version wins)
    for (run_id, row) in delta.runs {
        merged.runs
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
        merged.tasks
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
        merged.dep_satisfaction
            .entry(key)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_dep_satisfaction_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge timers
    for (timer_id, row) in delta.timers {
        merged.timers
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
        merged.dispatch_outbox
            .entry(dispatch_id)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_dispatch_outbox_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge sensor state
    for (sensor_id, row) in delta.sensor_state {
        merged.sensor_state
            .entry(sensor_id)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_sensor_state_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    // Merge sensor evals
    for (eval_id, row) in delta.sensor_evals {
        merged.sensor_evals
            .entry(eval_id)
            .and_modify(|existing| {
                if let Some(merged_row) = merge_sensor_eval_rows(vec![existing.clone(), row.clone()]) {
                    *existing = merged_row;
                }
            })
            .or_insert(row);
    }

    merged
}

fn delta_from_states(base: &FoldState, current: &FoldState) -> FoldState {
    let mut delta = FoldState::new();

    for (run_id, row) in &current.runs {
        if base.runs.get(run_id) != Some(row) {
            delta.runs.insert(run_id.clone(), row.clone());
        }
    }

    for (key, row) in &current.tasks {
        if base.tasks.get(key) != Some(row) {
            delta.tasks.insert(key.clone(), row.clone());
        }
    }

    for (key, row) in &current.dep_satisfaction {
        if base.dep_satisfaction.get(key) != Some(row) {
            delta.dep_satisfaction.insert(key.clone(), row.clone());
        }
    }

    for (timer_id, row) in &current.timers {
        if base.timers.get(timer_id) != Some(row) {
            delta.timers.insert(timer_id.clone(), row.clone());
        }
    }

    for (dispatch_id, row) in &current.dispatch_outbox {
        if base.dispatch_outbox.get(dispatch_id) != Some(row) {
            delta.dispatch_outbox.insert(dispatch_id.clone(), row.clone());
        }
    }

    for (sensor_id, row) in &current.sensor_state {
        if base.sensor_state.get(sensor_id) != Some(row) {
            delta.sensor_state.insert(sensor_id.clone(), row.clone());
        }
    }

    for (eval_id, row) in &current.sensor_evals {
        if base.sensor_evals.get(eval_id) != Some(row) {
            delta.sensor_evals.insert(eval_id.clone(), row.clone());
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
}

fn prune_sensor_evals(state: &mut FoldState) {
    let cutoff = Utc::now() - chrono::Duration::days(SENSOR_EVAL_RETENTION_DAYS);
    state.sensor_evals.retain(|_, row| row.evaluated_at >= cutoff);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::compactor::fold::{DispatchOutboxRow, TimerRow};
    use crate::orchestration::events::{OrchestrationEventData, TaskDef, TimerType, TriggerInfo};
    use arco_core::MemoryBackend;
    use chrono::DateTime;
    use std::sync::Arc;
    use ulid::Ulid;

    async fn create_test_compactor() -> Result<(MicroCompactor, ScopedStorage)> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let compactor = MicroCompactor::new(storage.clone());
        Ok((compactor, storage))
    }

    fn make_run_triggered_event() -> OrchestrationEvent {
        OrchestrationEvent::new(
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
                labels: std::collections::HashMap::new(),
            },
        )
    }

    fn make_plan_created_event() -> OrchestrationEvent {
        OrchestrationEvent::new(
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

        let path1 = format!("ledger/orchestration/2025-01-15/{}.json", event1.event_id);
        let path2 = format!("ledger/orchestration/2025-01-15/{}.json", event2.event_id);

        storage.put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&event1).expect("serialize")),
            WritePrecondition::None,
        ).await?;

        storage.put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&event2).expect("serialize")),
            WritePrecondition::None,
        ).await?;

        // Compact events
        let result = compactor.compact_events(vec![path1, path2]).await?;

        assert_eq!(result.events_processed, 2);
        assert!(result.delta_id.is_some());

        // Verify manifest was updated
        let manifest_data = storage.get_raw("state/orchestration/manifest.json").await?;
        let manifest: OrchestrationManifest = serde_json::from_slice(&manifest_data).expect("parse");

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

        let path1 = format!("ledger/orchestration/2025-01-15/{}.json", event1.event_id);
        let path2 = format!("ledger/orchestration/2025-01-15/{}.json", event2.event_id);

        storage.put_raw(&path1, Bytes::from(serde_json::to_string(&event1).expect("serialize")), WritePrecondition::None).await?;
        storage.put_raw(&path2, Bytes::from(serde_json::to_string(&event2).expect("serialize")), WritePrecondition::None).await?;

        compactor.compact_events(vec![path1, path2]).await?;

        // Load state from manifest
        let manifest = compactor.get_or_create_manifest().await?;
        let state = compactor.load_current_state(&manifest).await?;

        // Verify state
        assert_eq!(state.runs.len(), 1);
        assert!(state.runs.contains_key("run_01"));
        assert_eq!(state.tasks.len(), 2);
        assert!(state.tasks.contains_key(&("run_01".to_string(), "extract".to_string())));
        assert!(state.tasks.contains_key(&("run_01".to_string(), "transform".to_string())));
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
        let path1 = format!("ledger/orchestration/2025-01-15/{}.json", event1_id);
        let path2 = format!("ledger/orchestration/2025-01-15/{}.json", event2_id);

        storage.put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&event1).expect("serialize")),
            WritePrecondition::None,
        ).await?;
        storage.put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&event2).expect("serialize")),
            WritePrecondition::None,
        ).await?;

        compactor.compact_events(vec![path2.clone(), path1.clone()]).await?;

        let manifest_data = storage.get_raw("state/orchestration/manifest.json").await?;
        let manifest: OrchestrationManifest = serde_json::from_slice(&manifest_data).expect("parse");

        let (expected_event, expected_path) = if event1_id > event2_id {
            (event1_id, path1)
        } else {
            (event2_id, path2)
        };

        assert_eq!(manifest.watermarks.events_processed_through.as_deref(), Some(expected_event.as_str()));
        assert_eq!(manifest.watermarks.last_processed_file.as_deref(), Some(expected_path.as_str()));

        Ok(())
    }

    #[tokio::test]
    async fn compact_writes_delta_rows_only() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        let event1 = make_run_triggered_event();
        let event2 = make_plan_created_event();

        let path1 = format!("ledger/orchestration/2025-01-15/{}.json", event1.event_id);
        let path2 = format!("ledger/orchestration/2025-01-15/{}.json", event2.event_id);

        storage.put_raw(&path1, Bytes::from(serde_json::to_string(&event1).expect("serialize")), WritePrecondition::None).await?;
        storage.put_raw(&path2, Bytes::from(serde_json::to_string(&event2).expect("serialize")), WritePrecondition::None).await?;

        compactor.compact_events(vec![path1, path2]).await?;

        let attempt_id = Ulid::new().to_string();
        let event3 = make_task_started_event(&attempt_id);
        let path3 = format!("ledger/orchestration/2025-01-15/{}.json", event3.event_id);
        storage.put_raw(&path3, Bytes::from(serde_json::to_string(&event3).expect("serialize")), WritePrecondition::None).await?;

        compactor.compact_events(vec![path3]).await?;

        let manifest_data = storage.get_raw("state/orchestration/manifest.json").await?;
        let manifest: OrchestrationManifest = serde_json::from_slice(&manifest_data).expect("parse");

        let latest = manifest.l0_deltas.last().expect("delta");
        assert_eq!(latest.row_counts.runs, 0);
        assert_eq!(latest.row_counts.tasks, 1);
        assert_eq!(latest.row_counts.dep_satisfaction, 0);
        assert_eq!(latest.row_counts.timers, 0);
        assert_eq!(latest.row_counts.dispatch_outbox, 0);
        assert_eq!(latest.row_counts.sensor_state, 0);
        assert_eq!(latest.row_counts.sensor_evals, 0);

        Ok(())
    }

    #[tokio::test]
    async fn compact_persists_out_of_order_dispatch_fields() -> Result<()> {
        let (compactor, storage) = create_test_compactor().await?;

        let dispatch_id = DispatchOutboxRow::dispatch_id("run_01", "extract", 1);

        // DispatchEnqueued arrives first (newer ULID)
        let mut enqueued = make_dispatch_enqueued_event(&dispatch_id);
        enqueued.event_id = "01B".to_string();
        let path1 = format!("ledger/orchestration/2025-01-15/{}.json", enqueued.event_id);
        storage.put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&enqueued).expect("serialize")),
            WritePrecondition::None,
        ).await?;
        compactor.compact_events(vec![path1]).await?;

        // DispatchRequested arrives later but with older ULID
        let mut requested = make_dispatch_requested_event("run_01", "extract", 1, "01HQ123ATT");
        requested.event_id = "01A".to_string();
        let path2 = format!("ledger/orchestration/2025-01-15/{}.json", requested.event_id);
        storage.put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&requested).expect("serialize")),
            WritePrecondition::None,
        ).await?;
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
        let path1 = format!("ledger/orchestration/2025-01-15/{}.json", enqueued.event_id);
        storage.put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&enqueued).expect("serialize")),
            WritePrecondition::None,
        ).await?;
        compactor.compact_events(vec![path1]).await?;

        // TimerRequested arrives later but with older ULID (canonical fire_at differs)
        let canonical_fire_at = DateTime::from_timestamp(fire_epoch + 30, 0).unwrap();
        let mut requested = make_timer_requested_event(&timer_id, canonical_fire_at);
        requested.event_id = "01A".to_string();
        let path2 = format!("ledger/orchestration/2025-01-15/{}.json", requested.event_id);
        storage.put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&requested).expect("serialize")),
            WritePrecondition::None,
        ).await?;
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
        storage.put_raw(
            "state/orchestration/manifest.json",
            Bytes::from(serde_json::to_string(&manifest).expect("serialize")),
            WritePrecondition::None,
        ).await?;

        let (_current, version) = compactor.read_manifest_with_version().await?;
        let version = version.expect("version");

        let bumped = OrchestrationManifest::new("01HQXYZ124REV");
        storage.put_raw(
            "state/orchestration/manifest.json",
            Bytes::from(serde_json::to_string(&bumped).expect("serialize")),
            WritePrecondition::None,
        ).await?;

        let err = compactor.publish_manifest(&manifest, Some(&version)).await.unwrap_err();
        assert!(err.to_string().contains("manifest publish failed"));

        Ok(())
    }
}
