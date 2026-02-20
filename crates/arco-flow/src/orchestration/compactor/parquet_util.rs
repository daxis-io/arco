//! Parquet encoding/decoding helpers for orchestration state tables.
//!
//! This module defines the canonical Parquet schemas for orchestration Parquet files:
//! - `runs.parquet`
//! - `tasks.parquet`
//! - `dep_satisfaction.parquet`
//! - `timers.parquet`
//! - `dispatch_outbox.parquet`
//! - `sensor_state.parquet`
//! - `sensor_evals.parquet`
//! - `run_key_index.parquet`
//! - `run_key_conflicts.parquet`
//! - `partition_status.parquet`
//! - `idempotency_keys.parquet`
//! - `schedule_definitions.parquet`
//! - `schedule_state.parquet`
//! - `schedule_ticks.parquet`
//! - `backfills.parquet`
//! - `backfill_chunks.parquet`
//!
//! These schemas are the contract for orchestration controllers reading state.
//! Keep changes backwards-compatible and gated by snapshot versioning.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array as _, BooleanArray, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;

use super::fold::{
    BackfillChunkRow, BackfillRow, DepResolution, DepSatisfactionRow, DispatchOutboxRow,
    DispatchStatus, IdempotencyKeyRow, PartitionStatusRow, RunKeyConflictRow, RunKeyIndexRow,
    RunRow, RunState, ScheduleDefinitionRow, ScheduleStateRow, ScheduleTickRow, SensorEvalRow,
    SensorStateRow, TaskRow, TaskState, TimerRow, TimerState, TimerType,
};
use crate::error::{Error, Result};
use crate::orchestration::events::{
    BackfillState, ChunkState, PartitionSelector, RunRequest, SensorEvalStatus, SensorStatus,
    TaskOutcome, TickStatus, TriggerSource,
};

// ============================================================================
// Schema Definitions
// ============================================================================

fn runs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("plan_id", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("run_key", DataType::Utf8, true),
        Field::new("cancel_requested", DataType::Boolean, false),
        Field::new("tasks_total", DataType::UInt32, false),
        Field::new("tasks_completed", DataType::UInt32, false),
        Field::new("tasks_succeeded", DataType::UInt32, false),
        Field::new("tasks_failed", DataType::UInt32, false),
        Field::new("tasks_skipped", DataType::UInt32, false),
        Field::new("tasks_cancelled", DataType::UInt32, false),
        Field::new("triggered_at", DataType::Int64, false),
        Field::new("completed_at", DataType::Int64, true),
        Field::new("labels", DataType::Utf8, true),
        Field::new("code_version", DataType::Utf8, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn tasks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("task_key", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("attempt", DataType::UInt32, false),
        Field::new("attempt_id", DataType::Utf8, true),
        Field::new("started_at", DataType::Int64, true),
        Field::new("completed_at", DataType::Int64, true),
        Field::new("error_message", DataType::Utf8, true),
        Field::new("deps_total", DataType::UInt32, false),
        Field::new("deps_satisfied_count", DataType::UInt32, false),
        Field::new("max_attempts", DataType::UInt32, false),
        Field::new("heartbeat_timeout_sec", DataType::UInt32, false),
        Field::new("last_heartbeat_at", DataType::Int64, true),
        Field::new("ready_at", DataType::Int64, true),
        Field::new("asset_key", DataType::Utf8, true),
        Field::new("partition_key", DataType::Utf8, true),
        Field::new("materialization_id", DataType::Utf8, true),
        Field::new("delta_table", DataType::Utf8, true),
        Field::new("delta_version", DataType::Int64, true),
        Field::new("delta_partition", DataType::Utf8, true),
        Field::new("execution_lineage_ref", DataType::Utf8, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn dep_satisfaction_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("upstream_task_key", DataType::Utf8, false),
        Field::new("downstream_task_key", DataType::Utf8, false),
        Field::new("satisfied", DataType::Boolean, false),
        Field::new("resolution", DataType::Utf8, true),
        Field::new("satisfied_at", DataType::Int64, true),
        Field::new("satisfying_attempt", DataType::UInt32, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn timers_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timer_id", DataType::Utf8, false),
        Field::new("cloud_task_id", DataType::Utf8, true),
        Field::new("timer_type", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("task_key", DataType::Utf8, true),
        Field::new("attempt", DataType::UInt32, true),
        Field::new("fire_at", DataType::Int64, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn dispatch_outbox_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("task_key", DataType::Utf8, false),
        Field::new("attempt", DataType::UInt32, false),
        Field::new("dispatch_id", DataType::Utf8, false),
        Field::new("cloud_task_id", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("attempt_id", DataType::Utf8, false),
        Field::new("worker_queue", DataType::Utf8, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn sensor_state_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("cursor", DataType::Utf8, true),
        Field::new("last_evaluation_at", DataType::Int64, true),
        Field::new("last_eval_id", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("state_version", DataType::UInt32, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn sensor_evals_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("eval_id", DataType::Utf8, false),
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("cursor_before", DataType::Utf8, true),
        Field::new("cursor_after", DataType::Utf8, true),
        Field::new("expected_state_version", DataType::UInt32, true),
        Field::new("trigger_source", DataType::Utf8, false),
        Field::new("run_requests", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("evaluated_at", DataType::Int64, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn run_key_index_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("run_key", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("request_fingerprint", DataType::Utf8, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn run_key_conflicts_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("run_key", DataType::Utf8, false),
        Field::new("existing_fingerprint", DataType::Utf8, false),
        Field::new("conflicting_fingerprint", DataType::Utf8, false),
        Field::new("conflicting_event_id", DataType::Utf8, false),
        Field::new("detected_at", DataType::Int64, false),
    ]))
}

fn partition_status_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("asset_key", DataType::Utf8, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("last_materialization_run_id", DataType::Utf8, true),
        Field::new("last_materialization_at", DataType::Int64, true),
        Field::new("last_materialization_code_version", DataType::Utf8, true),
        Field::new("last_attempt_run_id", DataType::Utf8, true),
        Field::new("last_attempt_at", DataType::Int64, true),
        Field::new("last_attempt_outcome", DataType::Utf8, true),
        Field::new("stale_since", DataType::Int64, true),
        Field::new("stale_reason_code", DataType::Utf8, true),
        Field::new("partition_values", DataType::Utf8, true),
        Field::new("delta_table", DataType::Utf8, true),
        Field::new("delta_version", DataType::Int64, true),
        Field::new("delta_partition", DataType::Utf8, true),
        Field::new("execution_lineage_ref", DataType::Utf8, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn idempotency_keys_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("idempotency_key", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("recorded_at", DataType::Int64, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn schedule_definitions_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("schedule_id", DataType::Utf8, false),
        Field::new("cron_expression", DataType::Utf8, false),
        Field::new("timezone", DataType::Utf8, false),
        Field::new("catchup_window_minutes", DataType::UInt32, false),
        Field::new("asset_selection", DataType::Utf8, true),
        Field::new("max_catchup_ticks", DataType::UInt32, false),
        Field::new("enabled", DataType::Boolean, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn schedule_state_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("schedule_id", DataType::Utf8, false),
        Field::new("last_scheduled_for", DataType::Int64, true),
        Field::new("last_tick_id", DataType::Utf8, true),
        Field::new("last_run_key", DataType::Utf8, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn schedule_ticks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("tick_id", DataType::Utf8, false),
        Field::new("schedule_id", DataType::Utf8, false),
        Field::new("scheduled_for", DataType::Int64, false),
        Field::new("definition_version", DataType::Utf8, false),
        Field::new("asset_selection", DataType::Utf8, true),
        Field::new("partition_selection", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("run_key", DataType::Utf8, true),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("request_fingerprint", DataType::Utf8, true),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn backfills_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("backfill_id", DataType::Utf8, false),
        Field::new("asset_selection", DataType::Utf8, false),
        Field::new("partition_selector", DataType::Utf8, false),
        Field::new("chunk_size", DataType::UInt32, false),
        Field::new("max_concurrent_runs", DataType::UInt32, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("state_version", DataType::UInt32, false),
        Field::new("total_partitions", DataType::UInt32, false),
        Field::new("planned_chunks", DataType::UInt32, false),
        Field::new("completed_chunks", DataType::UInt32, false),
        Field::new("failed_chunks", DataType::UInt32, false),
        Field::new("parent_backfill_id", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

fn backfill_chunks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("chunk_id", DataType::Utf8, false),
        Field::new("backfill_id", DataType::Utf8, false),
        Field::new("chunk_index", DataType::UInt32, false),
        Field::new("partition_keys", DataType::Utf8, false),
        Field::new("run_key", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, false),
        Field::new("row_version", DataType::Utf8, false),
    ]))
}

// ============================================================================
// Public Schema Accessors
// ============================================================================

/// Returns the runs schema for golden file comparison.
#[must_use]
pub fn run_schema() -> Schema {
    (*runs_schema()).clone()
}

/// Returns the tasks schema for golden file comparison.
#[must_use]
pub fn task_schema() -> Schema {
    (*tasks_schema()).clone()
}

/// Returns the `dep_satisfaction` schema for golden file comparison.
#[must_use]
pub fn dep_satisfaction_parquet_schema() -> Schema {
    (*dep_satisfaction_schema()).clone()
}

/// Returns the timers schema for golden file comparison.
#[must_use]
pub fn timer_schema() -> Schema {
    (*timers_schema()).clone()
}

/// Returns the `dispatch_outbox` schema for golden file comparison.
#[must_use]
pub fn dispatch_outbox_parquet_schema() -> Schema {
    (*dispatch_outbox_schema()).clone()
}

/// Returns the `sensor_state` schema for golden file comparison.
#[must_use]
pub fn sensor_state_parquet_schema() -> Schema {
    (*sensor_state_schema()).clone()
}

/// Returns the `sensor_evals` schema for golden file comparison.
#[must_use]
pub fn sensor_evals_parquet_schema() -> Schema {
    (*sensor_evals_schema()).clone()
}

/// Returns the `run_key_index` schema for golden file comparison.
#[must_use]
pub fn run_key_index_parquet_schema() -> Schema {
    (*run_key_index_schema()).clone()
}

/// Returns the `run_key_conflicts` schema for golden file comparison.
#[must_use]
pub fn run_key_conflicts_parquet_schema() -> Schema {
    (*run_key_conflicts_schema()).clone()
}

/// Returns the `partition_status` schema for golden file comparison.
#[must_use]
pub fn partition_status_parquet_schema() -> Schema {
    (*partition_status_schema()).clone()
}

/// Returns the Parquet schema for idempotency keys table.
#[must_use]
pub fn idempotency_keys_parquet_schema() -> Schema {
    (*idempotency_keys_schema()).clone()
}

// ============================================================================
// Writer Properties
// ============================================================================

fn writer_properties() -> WriterProperties {
    let created_by = KeyValue {
        key: "created_by".to_string(),
        value: Some("arco-flow".to_string()),
    };
    WriterProperties::builder()
        .set_key_value_metadata(Some(vec![created_by]))
        .build()
}

fn write_single_batch(schema: Arc<Schema>, batch: &RecordBatch) -> Result<Bytes> {
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let props = writer_properties();
    let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))
        .map_err(|e| Error::parquet(format!("parquet writer init failed: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| Error::parquet(format!("parquet write failed: {e}")))?;
    writer
        .close()
        .map_err(|e| Error::parquet(format!("parquet close failed: {e}")))?;
    Ok(Bytes::from(cursor.into_inner()))
}

// ============================================================================
// Writers
// ============================================================================

/// Writes `runs.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_runs(rows: &[RunRow]) -> Result<Bytes> {
    let schema = runs_schema();

    let run_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let plan_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.plan_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let states = StringArray::from(
        rows.iter()
            .map(|r| Some(run_state_to_str(r.state)))
            .collect::<Vec<_>>(),
    );
    let run_keys = StringArray::from(
        rows.iter()
            .map(|r| r.run_key.as_deref())
            .collect::<Vec<_>>(),
    );
    let cancel_requested =
        BooleanArray::from(rows.iter().map(|r| r.cancel_requested).collect::<Vec<_>>());
    let tasks_total = UInt32Array::from(rows.iter().map(|r| r.tasks_total).collect::<Vec<_>>());
    let tasks_completed =
        UInt32Array::from(rows.iter().map(|r| r.tasks_completed).collect::<Vec<_>>());
    let tasks_succeeded =
        UInt32Array::from(rows.iter().map(|r| r.tasks_succeeded).collect::<Vec<_>>());
    let tasks_failed = UInt32Array::from(rows.iter().map(|r| r.tasks_failed).collect::<Vec<_>>());
    let tasks_skipped = UInt32Array::from(rows.iter().map(|r| r.tasks_skipped).collect::<Vec<_>>());
    let tasks_cancelled =
        UInt32Array::from(rows.iter().map(|r| r.tasks_cancelled).collect::<Vec<_>>());
    let triggered_at = Int64Array::from(
        rows.iter()
            .map(|r| r.triggered_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let completed_at = Int64Array::from(
        rows.iter()
            .map(|r| r.completed_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let labels = rows
        .iter()
        .map(|r| {
            if r.labels.is_empty() {
                Ok(None)
            } else {
                serde_json::to_string(&r.labels).map(Some)
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize run labels: {e}")))?;
    let labels = StringArray::from(labels);
    let code_versions = StringArray::from(
        rows.iter()
            .map(|r| r.code_version.as_deref())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(run_ids),
            Arc::new(plan_ids),
            Arc::new(states),
            Arc::new(run_keys),
            Arc::new(cancel_requested),
            Arc::new(tasks_total),
            Arc::new(tasks_completed),
            Arc::new(tasks_succeeded),
            Arc::new(tasks_failed),
            Arc::new(tasks_skipped),
            Arc::new(tasks_cancelled),
            Arc::new(triggered_at),
            Arc::new(completed_at),
            Arc::new(labels),
            Arc::new(code_versions),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `tasks.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_tasks(rows: &[TaskRow]) -> Result<Bytes> {
    let schema = tasks_schema();

    let run_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let task_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.task_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let states = StringArray::from(
        rows.iter()
            .map(|r| Some(task_state_to_str(r.state)))
            .collect::<Vec<_>>(),
    );
    let attempts = UInt32Array::from(rows.iter().map(|r| r.attempt).collect::<Vec<_>>());
    let attempt_ids = StringArray::from(
        rows.iter()
            .map(|r| r.attempt_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let started_at = Int64Array::from(
        rows.iter()
            .map(|r| r.started_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let completed_at = Int64Array::from(
        rows.iter()
            .map(|r| r.completed_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let error_messages = StringArray::from(
        rows.iter()
            .map(|r| r.error_message.as_deref())
            .collect::<Vec<_>>(),
    );
    let deps_total = UInt32Array::from(rows.iter().map(|r| r.deps_total).collect::<Vec<_>>());
    let deps_satisfied_count = UInt32Array::from(
        rows.iter()
            .map(|r| r.deps_satisfied_count)
            .collect::<Vec<_>>(),
    );
    let max_attempts = UInt32Array::from(rows.iter().map(|r| r.max_attempts).collect::<Vec<_>>());
    let heartbeat_timeout_sec = UInt32Array::from(
        rows.iter()
            .map(|r| r.heartbeat_timeout_sec)
            .collect::<Vec<_>>(),
    );
    let last_heartbeat_at = Int64Array::from(
        rows.iter()
            .map(|r| r.last_heartbeat_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let ready_at = Int64Array::from(
        rows.iter()
            .map(|r| r.ready_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let asset_keys = StringArray::from(
        rows.iter()
            .map(|r| r.asset_key.as_deref())
            .collect::<Vec<_>>(),
    );
    let partition_keys = StringArray::from(
        rows.iter()
            .map(|r| r.partition_key.as_deref())
            .collect::<Vec<_>>(),
    );
    let materialization_ids = StringArray::from(
        rows.iter()
            .map(|r| r.materialization_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let delta_tables = StringArray::from(
        rows.iter()
            .map(|r| r.delta_table.as_deref())
            .collect::<Vec<_>>(),
    );
    let delta_versions = Int64Array::from(rows.iter().map(|r| r.delta_version).collect::<Vec<_>>());
    let delta_partitions = StringArray::from(
        rows.iter()
            .map(|r| r.delta_partition.as_deref())
            .collect::<Vec<_>>(),
    );
    let execution_lineage_refs = StringArray::from(
        rows.iter()
            .map(|r| r.execution_lineage_ref.as_deref())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(run_ids),
            Arc::new(task_keys),
            Arc::new(states),
            Arc::new(attempts),
            Arc::new(attempt_ids),
            Arc::new(started_at),
            Arc::new(completed_at),
            Arc::new(error_messages),
            Arc::new(deps_total),
            Arc::new(deps_satisfied_count),
            Arc::new(max_attempts),
            Arc::new(heartbeat_timeout_sec),
            Arc::new(last_heartbeat_at),
            Arc::new(ready_at),
            Arc::new(asset_keys),
            Arc::new(partition_keys),
            Arc::new(materialization_ids),
            Arc::new(delta_tables),
            Arc::new(delta_versions),
            Arc::new(delta_partitions),
            Arc::new(execution_lineage_refs),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `dep_satisfaction.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_dep_satisfaction(rows: &[DepSatisfactionRow]) -> Result<Bytes> {
    let schema = dep_satisfaction_schema();

    let run_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let upstream_task_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.upstream_task_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let downstream_task_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.downstream_task_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let satisfied = BooleanArray::from(rows.iter().map(|r| r.satisfied).collect::<Vec<_>>());
    let resolutions = StringArray::from(
        rows.iter()
            .map(|r| r.resolution.map(dep_resolution_to_str))
            .collect::<Vec<_>>(),
    );
    let satisfied_at = Int64Array::from(
        rows.iter()
            .map(|r| r.satisfied_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let satisfying_attempts = UInt32Array::from(
        rows.iter()
            .map(|r| r.satisfying_attempt)
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(run_ids),
            Arc::new(upstream_task_keys),
            Arc::new(downstream_task_keys),
            Arc::new(satisfied),
            Arc::new(resolutions),
            Arc::new(satisfied_at),
            Arc::new(satisfying_attempts),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `timers.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_timers(rows: &[TimerRow]) -> Result<Bytes> {
    let schema = timers_schema();

    let timer_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.timer_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let cloud_task_ids = StringArray::from(
        rows.iter()
            .map(|r| r.cloud_task_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let timer_types = StringArray::from(
        rows.iter()
            .map(|r| Some(timer_type_to_str(r.timer_type)))
            .collect::<Vec<_>>(),
    );
    let run_ids = StringArray::from(rows.iter().map(|r| r.run_id.as_deref()).collect::<Vec<_>>());
    let task_keys = StringArray::from(
        rows.iter()
            .map(|r| r.task_key.as_deref())
            .collect::<Vec<_>>(),
    );
    let attempts = UInt32Array::from(rows.iter().map(|r| r.attempt).collect::<Vec<_>>());
    let fire_at = Int64Array::from(
        rows.iter()
            .map(|r| r.fire_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let states = StringArray::from(
        rows.iter()
            .map(|r| Some(timer_state_to_str(r.state)))
            .collect::<Vec<_>>(),
    );
    let payloads = StringArray::from(
        rows.iter()
            .map(|r| r.payload.as_deref())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(timer_ids),
            Arc::new(cloud_task_ids),
            Arc::new(timer_types),
            Arc::new(run_ids),
            Arc::new(task_keys),
            Arc::new(attempts),
            Arc::new(fire_at),
            Arc::new(states),
            Arc::new(payloads),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `dispatch_outbox.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_dispatch_outbox(rows: &[DispatchOutboxRow]) -> Result<Bytes> {
    let schema = dispatch_outbox_schema();

    let run_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let task_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.task_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let attempts = UInt32Array::from(rows.iter().map(|r| r.attempt).collect::<Vec<_>>());
    let dispatch_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.dispatch_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let cloud_task_ids = StringArray::from(
        rows.iter()
            .map(|r| r.cloud_task_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let statuses = StringArray::from(
        rows.iter()
            .map(|r| Some(dispatch_status_to_str(r.status)))
            .collect::<Vec<_>>(),
    );
    let attempt_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.attempt_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let worker_queues = StringArray::from(
        rows.iter()
            .map(|r| Some(r.worker_queue.as_str()))
            .collect::<Vec<_>>(),
    );
    let created_at = Int64Array::from(
        rows.iter()
            .map(|r| r.created_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(run_ids),
            Arc::new(task_keys),
            Arc::new(attempts),
            Arc::new(dispatch_ids),
            Arc::new(cloud_task_ids),
            Arc::new(statuses),
            Arc::new(attempt_ids),
            Arc::new(worker_queues),
            Arc::new(created_at),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `sensor_state.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_sensor_state(rows: &[SensorStateRow]) -> Result<Bytes> {
    let schema = sensor_state_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let sensor_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.sensor_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let cursors = StringArray::from(rows.iter().map(|r| r.cursor.as_deref()).collect::<Vec<_>>());
    let last_evaluation_at = Int64Array::from(
        rows.iter()
            .map(|r| r.last_evaluation_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let last_eval_ids = StringArray::from(
        rows.iter()
            .map(|r| r.last_eval_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let statuses = StringArray::from(
        rows.iter()
            .map(|r| Some(sensor_status_to_str(r.status)))
            .collect::<Vec<_>>(),
    );
    let state_versions =
        UInt32Array::from(rows.iter().map(|r| r.state_version).collect::<Vec<_>>());
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(sensor_ids),
            Arc::new(cursors),
            Arc::new(last_evaluation_at),
            Arc::new(last_eval_ids),
            Arc::new(statuses),
            Arc::new(state_versions),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `sensor_evals.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_sensor_evals(rows: &[SensorEvalRow]) -> Result<Bytes> {
    let schema = sensor_evals_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let eval_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.eval_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let sensor_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.sensor_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let cursor_before = StringArray::from(
        rows.iter()
            .map(|r| r.cursor_before.as_deref())
            .collect::<Vec<_>>(),
    );
    let cursor_after = StringArray::from(
        rows.iter()
            .map(|r| r.cursor_after.as_deref())
            .collect::<Vec<_>>(),
    );
    let expected_state_versions = UInt32Array::from(
        rows.iter()
            .map(|r| r.expected_state_version)
            .collect::<Vec<_>>(),
    );
    let trigger_sources = rows
        .iter()
        .map(|r| serde_json::to_string(&r.trigger_source).map(Some))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize trigger source: {e}")))?;
    let trigger_sources = StringArray::from(trigger_sources);
    let run_requests = rows
        .iter()
        .map(|r| {
            if r.run_requests.is_empty() {
                Ok(None)
            } else {
                serde_json::to_string(&r.run_requests).map(Some)
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize sensor run requests: {e}")))?;
    let run_requests = StringArray::from(run_requests);
    let statuses = rows
        .iter()
        .map(|r| serde_json::to_string(&r.status).map(Some))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize sensor eval status: {e}")))?;
    let statuses = StringArray::from(statuses);
    let evaluated_at = Int64Array::from(
        rows.iter()
            .map(|r| r.evaluated_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(eval_ids),
            Arc::new(sensor_ids),
            Arc::new(cursor_before),
            Arc::new(cursor_after),
            Arc::new(expected_state_versions),
            Arc::new(trigger_sources),
            Arc::new(run_requests),
            Arc::new(statuses),
            Arc::new(evaluated_at),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `run_key_index.parquet`.
///
/// # Errors
/// Returns an error if Parquet encoding fails.
pub(super) fn write_run_key_index(rows: &[RunKeyIndexRow]) -> Result<Bytes> {
    let schema = run_key_index_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let run_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let run_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let request_fingerprints = StringArray::from(
        rows.iter()
            .map(|r| Some(r.request_fingerprint.as_str()))
            .collect::<Vec<_>>(),
    );
    let created_at = Int64Array::from(
        rows.iter()
            .map(|r| r.created_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(run_keys),
            Arc::new(run_ids),
            Arc::new(request_fingerprints),
            Arc::new(created_at),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `run_key_conflicts.parquet`.
///
/// # Errors
/// Returns an error if Parquet encoding fails.
pub(super) fn write_run_key_conflicts(rows: &[RunKeyConflictRow]) -> Result<Bytes> {
    let schema = run_key_conflicts_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let run_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let existing_fingerprints = StringArray::from(
        rows.iter()
            .map(|r| Some(r.existing_fingerprint.as_str()))
            .collect::<Vec<_>>(),
    );
    let conflicting_fingerprints = StringArray::from(
        rows.iter()
            .map(|r| Some(r.conflicting_fingerprint.as_str()))
            .collect::<Vec<_>>(),
    );
    let conflicting_event_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.conflicting_event_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let detected_at = Int64Array::from(
        rows.iter()
            .map(|r| r.detected_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(run_keys),
            Arc::new(existing_fingerprints),
            Arc::new(conflicting_fingerprints),
            Arc::new(conflicting_event_ids),
            Arc::new(detected_at),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `partition_status.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_partition_status(rows: &[PartitionStatusRow]) -> Result<Bytes> {
    let schema = partition_status_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let asset_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.asset_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let partition_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.partition_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let last_materialization_run_ids = StringArray::from(
        rows.iter()
            .map(|r| r.last_materialization_run_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let last_materialization_at = Int64Array::from(
        rows.iter()
            .map(|r| r.last_materialization_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let last_materialization_code_versions = StringArray::from(
        rows.iter()
            .map(|r| r.last_materialization_code_version.as_deref())
            .collect::<Vec<_>>(),
    );
    let last_attempt_run_ids = StringArray::from(
        rows.iter()
            .map(|r| r.last_attempt_run_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let last_attempt_at = Int64Array::from(
        rows.iter()
            .map(|r| r.last_attempt_at.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let last_attempt_outcomes = StringArray::from(
        rows.iter()
            .map(|r| r.last_attempt_outcome.map(task_outcome_to_str))
            .collect::<Vec<_>>(),
    );
    let stale_since = Int64Array::from(
        rows.iter()
            .map(|r| r.stale_since.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let stale_reason_codes = StringArray::from(
        rows.iter()
            .map(|r| r.stale_reason_code.as_deref())
            .collect::<Vec<_>>(),
    );
    let partition_values = rows
        .iter()
        .map(|r| {
            if r.partition_values.is_empty() {
                Ok(None)
            } else {
                serde_json::to_string(&r.partition_values).map(Some)
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize partition values: {e}")))?;
    let partition_values = StringArray::from(partition_values);
    let delta_tables = StringArray::from(
        rows.iter()
            .map(|r| r.delta_table.as_deref())
            .collect::<Vec<_>>(),
    );
    let delta_versions = Int64Array::from(rows.iter().map(|r| r.delta_version).collect::<Vec<_>>());
    let delta_partitions = StringArray::from(
        rows.iter()
            .map(|r| r.delta_partition.as_deref())
            .collect::<Vec<_>>(),
    );
    let execution_lineage_refs = StringArray::from(
        rows.iter()
            .map(|r| r.execution_lineage_ref.as_deref())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(asset_keys),
            Arc::new(partition_keys),
            Arc::new(last_materialization_run_ids),
            Arc::new(last_materialization_at),
            Arc::new(last_materialization_code_versions),
            Arc::new(last_attempt_run_ids),
            Arc::new(last_attempt_at),
            Arc::new(last_attempt_outcomes),
            Arc::new(stale_since),
            Arc::new(stale_reason_codes),
            Arc::new(partition_values),
            Arc::new(delta_tables),
            Arc::new(delta_versions),
            Arc::new(delta_partitions),
            Arc::new(execution_lineage_refs),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `idempotency_keys.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_idempotency_keys(rows: &[IdempotencyKeyRow]) -> Result<Bytes> {
    let schema = idempotency_keys_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let idempotency_keys = StringArray::from(
        rows.iter()
            .map(|r| Some(r.idempotency_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let event_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.event_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let event_types = StringArray::from(
        rows.iter()
            .map(|r| Some(r.event_type.as_str()))
            .collect::<Vec<_>>(),
    );
    let recorded_at = Int64Array::from(
        rows.iter()
            .map(|r| r.recorded_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(idempotency_keys),
            Arc::new(event_ids),
            Arc::new(event_types),
            Arc::new(recorded_at),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `schedule_definitions.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_schedule_definitions(rows: &[ScheduleDefinitionRow]) -> Result<Bytes> {
    let schema = schedule_definitions_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let schedule_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.schedule_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let cron_expressions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.cron_expression.as_str()))
            .collect::<Vec<_>>(),
    );
    let timezones = StringArray::from(
        rows.iter()
            .map(|r| Some(r.timezone.as_str()))
            .collect::<Vec<_>>(),
    );
    let catchup_window_minutes = UInt32Array::from(
        rows.iter()
            .map(|r| r.catchup_window_minutes)
            .collect::<Vec<_>>(),
    );
    let asset_selection = rows
        .iter()
        .map(|r| {
            if r.asset_selection.is_empty() {
                Ok(None)
            } else {
                serde_json::to_string(&r.asset_selection).map(Some)
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize asset selection: {e}")))?;
    let asset_selection = StringArray::from(asset_selection);
    let max_catchup_ticks =
        UInt32Array::from(rows.iter().map(|r| r.max_catchup_ticks).collect::<Vec<_>>());
    let enabled = BooleanArray::from(rows.iter().map(|r| r.enabled).collect::<Vec<_>>());
    let created_at = Int64Array::from(
        rows.iter()
            .map(|r| r.created_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(schedule_ids),
            Arc::new(cron_expressions),
            Arc::new(timezones),
            Arc::new(catchup_window_minutes),
            Arc::new(asset_selection),
            Arc::new(max_catchup_ticks),
            Arc::new(enabled),
            Arc::new(created_at),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `schedule_state.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_schedule_state(rows: &[ScheduleStateRow]) -> Result<Bytes> {
    let schema = schedule_state_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let schedule_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.schedule_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let last_scheduled_for = Int64Array::from(
        rows.iter()
            .map(|r| r.last_scheduled_for.map(|t| t.timestamp_millis()))
            .collect::<Vec<_>>(),
    );
    let last_tick_id = StringArray::from(
        rows.iter()
            .map(|r| r.last_tick_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let last_run_key = StringArray::from(
        rows.iter()
            .map(|r| r.last_run_key.as_deref())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(schedule_ids),
            Arc::new(last_scheduled_for),
            Arc::new(last_tick_id),
            Arc::new(last_run_key),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `schedule_ticks.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet serialization fails.
pub fn write_schedule_ticks(rows: &[ScheduleTickRow]) -> Result<Bytes> {
    let schema = schedule_ticks_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let tick_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tick_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let schedule_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.schedule_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let scheduled_for = Int64Array::from(
        rows.iter()
            .map(|r| r.scheduled_for.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let definition_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.definition_version.as_str()))
            .collect::<Vec<_>>(),
    );
    let asset_selection = rows
        .iter()
        .map(|r| {
            if r.asset_selection.is_empty() {
                Ok(None)
            } else {
                serde_json::to_string(&r.asset_selection).map(Some)
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize tick asset selection: {e}")))?;
    let asset_selection = StringArray::from(asset_selection);
    let partition_selection = rows
        .iter()
        .map(|r| {
            r.partition_selection
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize partition selection: {e}")))?;
    let partition_selection = StringArray::from(partition_selection);
    let status = rows
        .iter()
        .map(|r| serde_json::to_string(&r.status))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize tick status: {e}")))?;
    let status = StringArray::from(status.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>());
    let run_key = StringArray::from(
        rows.iter()
            .map(|r| r.run_key.as_deref())
            .collect::<Vec<_>>(),
    );
    let run_id = StringArray::from(rows.iter().map(|r| r.run_id.as_deref()).collect::<Vec<_>>());
    let request_fingerprint = StringArray::from(
        rows.iter()
            .map(|r| r.request_fingerprint.as_deref())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(tick_ids),
            Arc::new(schedule_ids),
            Arc::new(scheduled_for),
            Arc::new(definition_versions),
            Arc::new(asset_selection),
            Arc::new(partition_selection),
            Arc::new(status),
            Arc::new(run_key),
            Arc::new(run_id),
            Arc::new(request_fingerprint),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `backfills.parquet`.
///
/// # Errors
/// Returns an error if Parquet serialization fails.
#[allow(clippy::too_many_lines)]
pub fn write_backfills(rows: &[BackfillRow]) -> Result<Bytes> {
    let schema = backfills_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let backfill_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.backfill_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let asset_selection = rows
        .iter()
        .map(|r| serde_json::to_string(&r.asset_selection))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::parquet(format!("failed to serialize backfill asset_selection: {e}"))
        })?;
    let asset_selection = StringArray::from(
        asset_selection
            .iter()
            .map(|s| Some(s.as_str()))
            .collect::<Vec<_>>(),
    );
    let partition_selector = rows
        .iter()
        .map(|r| serde_json::to_string(&r.partition_selector))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::parquet(format!(
                "failed to serialize backfill partition_selector: {e}"
            ))
        })?;
    let partition_selector = StringArray::from(
        partition_selector
            .iter()
            .map(|s| Some(s.as_str()))
            .collect::<Vec<_>>(),
    );
    let chunk_size = UInt32Array::from(rows.iter().map(|r| r.chunk_size).collect::<Vec<_>>());
    let max_concurrent_runs = UInt32Array::from(
        rows.iter()
            .map(|r| r.max_concurrent_runs)
            .collect::<Vec<_>>(),
    );
    let state = StringArray::from(
        rows.iter()
            .map(|r| Some(backfill_state_to_str(r.state)))
            .collect::<Vec<_>>(),
    );
    let state_version = UInt32Array::from(rows.iter().map(|r| r.state_version).collect::<Vec<_>>());
    let total_partitions =
        UInt32Array::from(rows.iter().map(|r| r.total_partitions).collect::<Vec<_>>());
    let planned_chunks =
        UInt32Array::from(rows.iter().map(|r| r.planned_chunks).collect::<Vec<_>>());
    let completed_chunks =
        UInt32Array::from(rows.iter().map(|r| r.completed_chunks).collect::<Vec<_>>());
    let failed_chunks = UInt32Array::from(rows.iter().map(|r| r.failed_chunks).collect::<Vec<_>>());
    let parent_backfill_id = StringArray::from(
        rows.iter()
            .map(|r| r.parent_backfill_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let created_at = Int64Array::from(
        rows.iter()
            .map(|r| r.created_at.timestamp_millis())
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(backfill_ids),
            Arc::new(asset_selection),
            Arc::new(partition_selector),
            Arc::new(chunk_size),
            Arc::new(max_concurrent_runs),
            Arc::new(state),
            Arc::new(state_version),
            Arc::new(total_partitions),
            Arc::new(planned_chunks),
            Arc::new(completed_chunks),
            Arc::new(failed_chunks),
            Arc::new(parent_backfill_id),
            Arc::new(created_at),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

/// Writes `backfill_chunks.parquet`.
///
/// # Errors
/// Returns an error if Parquet serialization fails.
pub fn write_backfill_chunks(rows: &[BackfillChunkRow]) -> Result<Bytes> {
    let schema = backfill_chunks_schema();

    let tenant_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.tenant_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.workspace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let chunk_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.chunk_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let backfill_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.backfill_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let chunk_index = UInt32Array::from(rows.iter().map(|r| r.chunk_index).collect::<Vec<_>>());
    let partition_keys = rows
        .iter()
        .map(|r| serde_json::to_string(&r.partition_keys))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::parquet(format!("failed to serialize chunk partition_keys: {e}")))?;
    let partition_keys = StringArray::from(
        partition_keys
            .iter()
            .map(|s| Some(s.as_str()))
            .collect::<Vec<_>>(),
    );
    let run_key = StringArray::from(
        rows.iter()
            .map(|r| Some(r.run_key.as_str()))
            .collect::<Vec<_>>(),
    );
    let run_id = StringArray::from(rows.iter().map(|r| r.run_id.as_deref()).collect::<Vec<_>>());
    let state = StringArray::from(
        rows.iter()
            .map(|r| Some(chunk_state_to_str(r.state)))
            .collect::<Vec<_>>(),
    );
    let row_versions = StringArray::from(
        rows.iter()
            .map(|r| Some(r.row_version.as_str()))
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(workspace_ids),
            Arc::new(chunk_ids),
            Arc::new(backfill_ids),
            Arc::new(chunk_index),
            Arc::new(partition_keys),
            Arc::new(run_key),
            Arc::new(run_id),
            Arc::new(state),
            Arc::new(row_versions),
        ],
    )
    .map_err(|e| Error::parquet(format!("record batch build failed: {e}")))?;

    write_single_batch(schema, &batch)
}

// ============================================================================
// Readers
// ============================================================================

fn read_batches(bytes: &Bytes) -> Result<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|e| Error::parquet(format!("parquet reader init failed: {e}")))?
        .build()
        .map_err(|e| Error::parquet(format!("parquet reader build failed: {e}")))?;

    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| Error::parquet(format!("parquet read batch failed: {e}")))?;
        batches.push(batch);
    }
    Ok(batches)
}

fn col_string<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| Error::parquet(format!("missing column '{name}': {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::parquet(format!("column '{name}' is not StringArray")))
}

/// Returns None if column doesn't exist (backwards compatibility for new optional columns).
fn col_string_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    let idx = batch.schema().index_of(name).ok()?;
    batch.column(idx).as_any().downcast_ref::<StringArray>()
}

/// Returns None if column doesn't exist (backwards compatibility for new optional columns).
fn col_bool_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a BooleanArray> {
    let idx = batch.schema().index_of(name).ok()?;
    batch.column(idx).as_any().downcast_ref::<BooleanArray>()
}

fn col_u32<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| Error::parquet(format!("missing column '{name}': {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| Error::parquet(format!("column '{name}' is not UInt32Array")))
}

fn col_i64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| Error::parquet(format!("missing column '{name}': {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::parquet(format!("column '{name}' is not Int64Array")))
}

/// Returns None if column doesn't exist (backwards compatibility for new optional columns).
fn col_i64_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
    let idx = batch.schema().index_of(name).ok()?;
    batch.column(idx).as_any().downcast_ref::<Int64Array>()
}

fn col_bool<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| Error::parquet(format!("missing column '{name}': {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::parquet(format!("column '{name}' is not BooleanArray")))
}

fn millis_to_datetime(millis: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(millis).unwrap_or_else(chrono::Utc::now)
}

/// Reads `runs.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_runs(bytes: &Bytes) -> Result<Vec<RunRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let run_id = col_string(&batch, "run_id")?;
        let plan_id = col_string(&batch, "plan_id")?;
        let state = col_string(&batch, "state")?;
        let run_key = col_string_opt(&batch, "run_key");
        let cancel_requested = col_bool_opt(&batch, "cancel_requested");
        let tasks_total = col_u32(&batch, "tasks_total")?;
        let tasks_completed = col_u32(&batch, "tasks_completed")?;
        let tasks_succeeded = col_u32(&batch, "tasks_succeeded")?;
        let tasks_failed = col_u32(&batch, "tasks_failed")?;
        let tasks_skipped = col_u32(&batch, "tasks_skipped")?;
        let tasks_cancelled = col_u32(&batch, "tasks_cancelled")?;
        let triggered_at = col_i64(&batch, "triggered_at")?;
        let completed_at = col_i64(&batch, "completed_at")?;
        let labels = col_string_opt(&batch, "labels");
        let code_version = col_string_opt(&batch, "code_version");
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let labels = if let Some(col) = labels {
                if col.is_null(row) {
                    HashMap::new()
                } else {
                    serde_json::from_str::<HashMap<String, String>>(col.value(row))
                        .map_err(|e| Error::parquet(format!("failed to parse run labels: {e}")))?
                }
            } else {
                HashMap::new()
            };
            out.push(RunRow {
                run_id: run_id.value(row).to_string(),
                plan_id: plan_id.value(row).to_string(),
                state: str_to_run_state(state.value(row))?,
                run_key: run_key.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                labels,
                cancel_requested: cancel_requested
                    .is_some_and(|col| !col.is_null(row) && col.value(row)),
                tasks_total: tasks_total.value(row),
                tasks_completed: tasks_completed.value(row),
                tasks_succeeded: tasks_succeeded.value(row),
                tasks_failed: tasks_failed.value(row),
                tasks_skipped: tasks_skipped.value(row),
                tasks_cancelled: tasks_cancelled.value(row),
                triggered_at: millis_to_datetime(triggered_at.value(row)),
                completed_at: if completed_at.is_null(row) {
                    None
                } else {
                    Some(millis_to_datetime(completed_at.value(row)))
                },
                code_version: code_version.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `tasks.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_tasks(bytes: &Bytes) -> Result<Vec<TaskRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let run_id = col_string(&batch, "run_id")?;
        let task_key = col_string(&batch, "task_key")?;
        let state = col_string(&batch, "state")?;
        let attempt = col_u32(&batch, "attempt")?;
        let attempt_id = col_string(&batch, "attempt_id")?;
        let started_at = col_i64_opt(&batch, "started_at");
        let completed_at = col_i64_opt(&batch, "completed_at");
        let error_message = col_string_opt(&batch, "error_message");
        let deps_total = col_u32(&batch, "deps_total")?;
        let deps_satisfied_count = col_u32(&batch, "deps_satisfied_count")?;
        let max_attempts = col_u32(&batch, "max_attempts")?;
        let heartbeat_timeout_sec = col_u32(&batch, "heartbeat_timeout_sec")?;
        let last_heartbeat_at = col_i64(&batch, "last_heartbeat_at")?;
        let ready_at = col_i64(&batch, "ready_at")?;
        let asset_key = col_string_opt(&batch, "asset_key");
        let partition_key = col_string_opt(&batch, "partition_key");
        let materialization_id = col_string_opt(&batch, "materialization_id");
        let delta_table = col_string_opt(&batch, "delta_table");
        let delta_version = col_i64_opt(&batch, "delta_version");
        let delta_partition = col_string_opt(&batch, "delta_partition");
        let execution_lineage_ref = col_string_opt(&batch, "execution_lineage_ref");
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(TaskRow {
                run_id: run_id.value(row).to_string(),
                task_key: task_key.value(row).to_string(),
                state: str_to_task_state(state.value(row))?,
                attempt: attempt.value(row),
                attempt_id: if attempt_id.is_null(row) {
                    None
                } else {
                    Some(attempt_id.value(row).to_string())
                },
                started_at: started_at.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                completed_at: completed_at.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                error_message: error_message.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                deps_total: deps_total.value(row),
                deps_satisfied_count: deps_satisfied_count.value(row),
                max_attempts: max_attempts.value(row),
                heartbeat_timeout_sec: heartbeat_timeout_sec.value(row),
                last_heartbeat_at: if last_heartbeat_at.is_null(row) {
                    None
                } else {
                    Some(millis_to_datetime(last_heartbeat_at.value(row)))
                },
                ready_at: if ready_at.is_null(row) {
                    None
                } else {
                    Some(millis_to_datetime(ready_at.value(row)))
                },
                asset_key: asset_key.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                partition_key: partition_key.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                materialization_id: materialization_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                delta_table: delta_table.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                delta_version: delta_version.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row))
                    }
                }),
                delta_partition: delta_partition.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                execution_lineage_ref: execution_lineage_ref.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `dep_satisfaction.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_dep_satisfaction(bytes: &Bytes) -> Result<Vec<DepSatisfactionRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let run_id = col_string(&batch, "run_id")?;
        let upstream_task_key = col_string(&batch, "upstream_task_key")?;
        let downstream_task_key = col_string(&batch, "downstream_task_key")?;
        let satisfied = col_bool(&batch, "satisfied")?;
        let resolution = col_string(&batch, "resolution")?;
        let satisfied_at = col_i64(&batch, "satisfied_at")?;
        let satisfying_attempt = col_u32(&batch, "satisfying_attempt")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(DepSatisfactionRow {
                run_id: run_id.value(row).to_string(),
                upstream_task_key: upstream_task_key.value(row).to_string(),
                downstream_task_key: downstream_task_key.value(row).to_string(),
                satisfied: satisfied.value(row),
                resolution: if resolution.is_null(row) {
                    None
                } else {
                    Some(str_to_dep_resolution(resolution.value(row))?)
                },
                satisfied_at: if satisfied_at.is_null(row) {
                    None
                } else {
                    Some(millis_to_datetime(satisfied_at.value(row)))
                },
                satisfying_attempt: if satisfying_attempt.is_null(row) {
                    None
                } else {
                    Some(satisfying_attempt.value(row))
                },
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `timers.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_timers(bytes: &Bytes) -> Result<Vec<TimerRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let timer_id = col_string(&batch, "timer_id")?;
        let cloud_task_id = col_string(&batch, "cloud_task_id")?;
        let timer_type = col_string(&batch, "timer_type")?;
        let run_id = col_string(&batch, "run_id")?;
        let task_key = col_string(&batch, "task_key")?;
        let attempt = col_u32(&batch, "attempt")?;
        let fire_at = col_i64(&batch, "fire_at")?;
        let state = col_string(&batch, "state")?;
        let payload = col_string(&batch, "payload")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(TimerRow {
                timer_id: timer_id.value(row).to_string(),
                cloud_task_id: if cloud_task_id.is_null(row) {
                    None
                } else {
                    Some(cloud_task_id.value(row).to_string())
                },
                timer_type: str_to_timer_type(timer_type.value(row))?,
                run_id: if run_id.is_null(row) {
                    None
                } else {
                    Some(run_id.value(row).to_string())
                },
                task_key: if task_key.is_null(row) {
                    None
                } else {
                    Some(task_key.value(row).to_string())
                },
                attempt: if attempt.is_null(row) {
                    None
                } else {
                    Some(attempt.value(row))
                },
                fire_at: millis_to_datetime(fire_at.value(row)),
                state: str_to_timer_state(state.value(row))?,
                payload: if payload.is_null(row) {
                    None
                } else {
                    Some(payload.value(row).to_string())
                },
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `dispatch_outbox.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_dispatch_outbox(bytes: &Bytes) -> Result<Vec<DispatchOutboxRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let run_id = col_string(&batch, "run_id")?;
        let task_key = col_string(&batch, "task_key")?;
        let attempt = col_u32(&batch, "attempt")?;
        let dispatch_id = col_string(&batch, "dispatch_id")?;
        let cloud_task_id = col_string(&batch, "cloud_task_id")?;
        let status = col_string(&batch, "status")?;
        let attempt_id = col_string(&batch, "attempt_id")?;
        let worker_queue = col_string(&batch, "worker_queue")?;
        let created_at = col_i64(&batch, "created_at")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(DispatchOutboxRow {
                run_id: run_id.value(row).to_string(),
                task_key: task_key.value(row).to_string(),
                attempt: attempt.value(row),
                dispatch_id: dispatch_id.value(row).to_string(),
                cloud_task_id: if cloud_task_id.is_null(row) {
                    None
                } else {
                    Some(cloud_task_id.value(row).to_string())
                },
                status: str_to_dispatch_status(status.value(row))?,
                attempt_id: attempt_id.value(row).to_string(),
                worker_queue: worker_queue.value(row).to_string(),
                created_at: millis_to_datetime(created_at.value(row)),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `sensor_state.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_sensor_state(bytes: &Bytes) -> Result<Vec<SensorStateRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let sensor_id = col_string(&batch, "sensor_id")?;
        let cursor = col_string(&batch, "cursor")?;
        let last_evaluation_at = col_i64_opt(&batch, "last_evaluation_at");
        let last_eval_id = col_string(&batch, "last_eval_id")?;
        let status = col_string(&batch, "status")?;
        let state_version = col_u32(&batch, "state_version")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(SensorStateRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                sensor_id: sensor_id.value(row).to_string(),
                cursor: if cursor.is_null(row) {
                    None
                } else {
                    Some(cursor.value(row).to_string())
                },
                last_evaluation_at: last_evaluation_at.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                last_eval_id: if last_eval_id.is_null(row) {
                    None
                } else {
                    Some(last_eval_id.value(row).to_string())
                },
                status: str_to_sensor_status(status.value(row))?,
                state_version: state_version.value(row),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `sensor_evals.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_sensor_evals(bytes: &Bytes) -> Result<Vec<SensorEvalRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let eval_id = col_string(&batch, "eval_id")?;
        let sensor_id = col_string(&batch, "sensor_id")?;
        let cursor_before = col_string(&batch, "cursor_before")?;
        let cursor_after = col_string(&batch, "cursor_after")?;
        let expected_state_version = col_u32(&batch, "expected_state_version")?;
        let trigger_source = col_string(&batch, "trigger_source")?;
        let run_requests = col_string_opt(&batch, "run_requests");
        let status = col_string(&batch, "status")?;
        let evaluated_at = col_i64(&batch, "evaluated_at")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let trigger_source = if trigger_source.is_null(row) {
                return Err(Error::parquet(
                    "sensor eval missing trigger_source".to_string(),
                ));
            } else {
                serde_json::from_str::<TriggerSource>(trigger_source.value(row))
                    .map_err(|e| Error::parquet(format!("failed to parse trigger source: {e}")))?
            };
            let status = if status.is_null(row) {
                return Err(Error::parquet("sensor eval missing status".to_string()));
            } else {
                serde_json::from_str::<SensorEvalStatus>(status.value(row)).map_err(|e| {
                    Error::parquet(format!("failed to parse sensor eval status: {e}"))
                })?
            };
            let run_requests = match run_requests {
                Some(col) => {
                    if col.is_null(row) {
                        Vec::new()
                    } else {
                        serde_json::from_str::<Vec<RunRequest>>(col.value(row)).map_err(|e| {
                            Error::parquet(format!("failed to parse sensor run requests: {e}"))
                        })?
                    }
                }
                None => Vec::new(),
            };

            out.push(SensorEvalRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                eval_id: eval_id.value(row).to_string(),
                sensor_id: sensor_id.value(row).to_string(),
                cursor_before: if cursor_before.is_null(row) {
                    None
                } else {
                    Some(cursor_before.value(row).to_string())
                },
                cursor_after: if cursor_after.is_null(row) {
                    None
                } else {
                    Some(cursor_after.value(row).to_string())
                },
                expected_state_version: if expected_state_version.is_null(row) {
                    None
                } else {
                    Some(expected_state_version.value(row))
                },
                trigger_source,
                run_requests,
                status,
                evaluated_at: millis_to_datetime(evaluated_at.value(row)),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `run_key_index.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_run_key_index(bytes: &Bytes) -> Result<Vec<RunKeyIndexRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let run_key = col_string(&batch, "run_key")?;
        let run_id = col_string(&batch, "run_id")?;
        let request_fingerprint = col_string(&batch, "request_fingerprint")?;
        let created_at = col_i64(&batch, "created_at")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(RunKeyIndexRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                run_key: run_key.value(row).to_string(),
                run_id: run_id.value(row).to_string(),
                request_fingerprint: request_fingerprint.value(row).to_string(),
                created_at: millis_to_datetime(created_at.value(row)),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `run_key_conflicts.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_run_key_conflicts(bytes: &Bytes) -> Result<Vec<RunKeyConflictRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let run_key = col_string(&batch, "run_key")?;
        let existing_fingerprint = col_string(&batch, "existing_fingerprint")?;
        let conflicting_fingerprint = col_string(&batch, "conflicting_fingerprint")?;
        let conflicting_event_id = col_string(&batch, "conflicting_event_id")?;
        let detected_at = col_i64(&batch, "detected_at")?;

        for row in 0..batch.num_rows() {
            out.push(RunKeyConflictRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                run_key: run_key.value(row).to_string(),
                existing_fingerprint: existing_fingerprint.value(row).to_string(),
                conflicting_fingerprint: conflicting_fingerprint.value(row).to_string(),
                conflicting_event_id: conflicting_event_id.value(row).to_string(),
                detected_at: millis_to_datetime(detected_at.value(row)),
            });
        }
    }
    Ok(out)
}

/// Reads `partition_status.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
#[allow(clippy::too_many_lines)]
pub fn read_partition_status(bytes: &Bytes) -> Result<Vec<PartitionStatusRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let asset_key = col_string(&batch, "asset_key")?;
        let partition_key = col_string(&batch, "partition_key")?;
        let last_materialization_run_id = col_string_opt(&batch, "last_materialization_run_id");
        let last_materialization_at = col_i64_opt(&batch, "last_materialization_at");
        let last_materialization_code_version =
            col_string_opt(&batch, "last_materialization_code_version");
        let last_attempt_run_id = col_string_opt(&batch, "last_attempt_run_id");
        let last_attempt_at = col_i64_opt(&batch, "last_attempt_at");
        let last_attempt_outcome = col_string_opt(&batch, "last_attempt_outcome");
        let stale_since = col_i64_opt(&batch, "stale_since");
        let stale_reason_code = col_string_opt(&batch, "stale_reason_code");
        let partition_values = col_string_opt(&batch, "partition_values");
        let delta_table = col_string_opt(&batch, "delta_table");
        let delta_version = col_i64_opt(&batch, "delta_version");
        let delta_partition = col_string_opt(&batch, "delta_partition");
        let execution_lineage_ref = col_string_opt(&batch, "execution_lineage_ref");
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let partition_values = if let Some(col) = partition_values {
                if col.is_null(row) {
                    HashMap::new()
                } else {
                    serde_json::from_str::<HashMap<String, String>>(col.value(row)).map_err(
                        |e| Error::parquet(format!("failed to parse partition values: {e}")),
                    )?
                }
            } else {
                HashMap::new()
            };

            let last_attempt_outcome = if let Some(col) = last_attempt_outcome {
                if col.is_null(row) {
                    None
                } else {
                    Some(str_to_task_outcome(col.value(row))?)
                }
            } else {
                None
            };

            out.push(PartitionStatusRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                asset_key: asset_key.value(row).to_string(),
                partition_key: partition_key.value(row).to_string(),
                last_materialization_run_id: last_materialization_run_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                last_materialization_at: last_materialization_at.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                last_materialization_code_version: last_materialization_code_version
                    .as_ref()
                    .and_then(|col| {
                        if col.is_null(row) {
                            None
                        } else {
                            Some(col.value(row).to_string())
                        }
                    }),
                last_attempt_run_id: last_attempt_run_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                last_attempt_at: last_attempt_at.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                last_attempt_outcome,
                stale_since: stale_since.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                stale_reason_code: stale_reason_code.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                partition_values,
                delta_table: delta_table.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                delta_version: delta_version.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row))
                    }
                }),
                delta_partition: delta_partition.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                execution_lineage_ref: execution_lineage_ref.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `idempotency_keys.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_idempotency_keys(bytes: &Bytes) -> Result<Vec<IdempotencyKeyRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let idempotency_key = col_string(&batch, "idempotency_key")?;
        let event_id = col_string(&batch, "event_id")?;
        let event_type = col_string(&batch, "event_type")?;
        let recorded_at = col_i64(&batch, "recorded_at")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(IdempotencyKeyRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                idempotency_key: idempotency_key.value(row).to_string(),
                event_id: event_id.value(row).to_string(),
                event_type: event_type.value(row).to_string(),
                recorded_at: millis_to_datetime(recorded_at.value(row)),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `schedule_definitions.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_schedule_definitions(bytes: &Bytes) -> Result<Vec<ScheduleDefinitionRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let schedule_id = col_string(&batch, "schedule_id")?;
        let cron_expression = col_string(&batch, "cron_expression")?;
        let timezone = col_string(&batch, "timezone")?;
        let catchup_window_minutes = col_u32(&batch, "catchup_window_minutes")?;
        let asset_selection = col_string_opt(&batch, "asset_selection");
        let max_catchup_ticks = col_u32(&batch, "max_catchup_ticks")?;
        let enabled = col_bool(&batch, "enabled")?;
        let created_at = col_i64(&batch, "created_at")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let asset_selection = if let Some(col) = asset_selection {
                if col.is_null(row) {
                    Vec::new()
                } else {
                    serde_json::from_str::<Vec<String>>(col.value(row)).map_err(|e| {
                        Error::parquet(format!("failed to parse asset selection: {e}"))
                    })?
                }
            } else {
                Vec::new()
            };

            out.push(ScheduleDefinitionRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                schedule_id: schedule_id.value(row).to_string(),
                cron_expression: cron_expression.value(row).to_string(),
                timezone: timezone.value(row).to_string(),
                catchup_window_minutes: catchup_window_minutes.value(row),
                asset_selection,
                max_catchup_ticks: max_catchup_ticks.value(row),
                enabled: enabled.value(row),
                created_at: millis_to_datetime(created_at.value(row)),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `schedule_state.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_schedule_state(bytes: &Bytes) -> Result<Vec<ScheduleStateRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let schedule_id = col_string(&batch, "schedule_id")?;
        let last_scheduled_for = col_i64_opt(&batch, "last_scheduled_for");
        let last_tick_id = col_string_opt(&batch, "last_tick_id");
        let last_run_key = col_string_opt(&batch, "last_run_key");
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            out.push(ScheduleStateRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                schedule_id: schedule_id.value(row).to_string(),
                last_scheduled_for: last_scheduled_for.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(millis_to_datetime(col.value(row)))
                    }
                }),
                last_tick_id: last_tick_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                last_run_key: last_run_key.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `schedule_ticks.parquet`.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_schedule_ticks(bytes: &Bytes) -> Result<Vec<ScheduleTickRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let tick_id = col_string(&batch, "tick_id")?;
        let schedule_id = col_string(&batch, "schedule_id")?;
        let scheduled_for = col_i64(&batch, "scheduled_for")?;
        let definition_version = col_string(&batch, "definition_version")?;
        let asset_selection = col_string_opt(&batch, "asset_selection");
        let partition_selection = col_string_opt(&batch, "partition_selection");
        let status = col_string(&batch, "status")?;
        let run_key = col_string_opt(&batch, "run_key");
        let run_id = col_string_opt(&batch, "run_id");
        let request_fingerprint = col_string_opt(&batch, "request_fingerprint");
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let status = if status.is_null(row) {
                return Err(Error::parquet("schedule tick missing status".to_string()));
            } else {
                serde_json::from_str::<TickStatus>(status.value(row))
                    .map_err(|e| Error::parquet(format!("failed to parse tick status: {e}")))?
            };

            let asset_selection = if let Some(col) = asset_selection {
                if col.is_null(row) {
                    Vec::new()
                } else {
                    serde_json::from_str::<Vec<String>>(col.value(row)).map_err(|e| {
                        Error::parquet(format!("failed to parse tick asset selection: {e}"))
                    })?
                }
            } else {
                Vec::new()
            };

            let partition_selection = if let Some(col) = partition_selection {
                if col.is_null(row) {
                    None
                } else {
                    Some(
                        serde_json::from_str::<Vec<String>>(col.value(row)).map_err(|e| {
                            Error::parquet(format!("failed to parse partition selection: {e}"))
                        })?,
                    )
                }
            } else {
                None
            };

            out.push(ScheduleTickRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                tick_id: tick_id.value(row).to_string(),
                schedule_id: schedule_id.value(row).to_string(),
                scheduled_for: millis_to_datetime(scheduled_for.value(row)),
                definition_version: definition_version.value(row).to_string(),
                asset_selection,
                partition_selection,
                status,
                run_key: run_key.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                run_id: run_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                request_fingerprint: request_fingerprint.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `backfills.parquet`.
///
/// # Errors
/// Returns an error if Parquet decoding fails or required columns are missing.
#[allow(clippy::too_many_lines)]
pub fn read_backfills(bytes: &Bytes) -> Result<Vec<BackfillRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let backfill_id = col_string(&batch, "backfill_id")?;
        let asset_selection = col_string(&batch, "asset_selection")?;
        let partition_selector = col_string(&batch, "partition_selector")?;
        let chunk_size = col_u32(&batch, "chunk_size")?;
        let max_concurrent_runs = col_u32(&batch, "max_concurrent_runs")?;
        let state = col_string(&batch, "state")?;
        let state_version = col_u32(&batch, "state_version")?;
        let total_partitions = col_u32(&batch, "total_partitions")?;
        let planned_chunks = col_u32(&batch, "planned_chunks")?;
        let completed_chunks = col_u32(&batch, "completed_chunks")?;
        let failed_chunks = col_u32(&batch, "failed_chunks")?;
        let parent_backfill_id = col_string_opt(&batch, "parent_backfill_id");
        let created_at = col_i64(&batch, "created_at")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let asset_selection = serde_json::from_str::<Vec<String>>(asset_selection.value(row))
                .map_err(|e| {
                Error::parquet(format!("failed to parse backfill asset_selection: {e}"))
            })?;
            let partition_selector = serde_json::from_str::<PartitionSelector>(
                partition_selector.value(row),
            )
            .map_err(|e| Error::parquet(format!("failed to parse partition_selector: {e}")))?;

            out.push(BackfillRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                backfill_id: backfill_id.value(row).to_string(),
                asset_selection,
                partition_selector,
                chunk_size: chunk_size.value(row),
                max_concurrent_runs: max_concurrent_runs.value(row),
                state: str_to_backfill_state(state.value(row))?,
                state_version: state_version.value(row),
                total_partitions: total_partitions.value(row),
                planned_chunks: planned_chunks.value(row),
                completed_chunks: completed_chunks.value(row),
                failed_chunks: failed_chunks.value(row),
                parent_backfill_id: parent_backfill_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                created_at: millis_to_datetime(created_at.value(row)),
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

/// Reads `backfill_chunks.parquet`.
///
/// # Errors
/// Returns an error if Parquet decoding fails or required columns are missing.
pub fn read_backfill_chunks(bytes: &Bytes) -> Result<Vec<BackfillChunkRow>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let tenant_id = col_string(&batch, "tenant_id")?;
        let workspace_id = col_string(&batch, "workspace_id")?;
        let chunk_id = col_string(&batch, "chunk_id")?;
        let backfill_id = col_string(&batch, "backfill_id")?;
        let chunk_index = col_u32(&batch, "chunk_index")?;
        let partition_keys = col_string(&batch, "partition_keys")?;
        let run_key = col_string(&batch, "run_key")?;
        let run_id = col_string_opt(&batch, "run_id");
        let state = col_string(&batch, "state")?;
        let row_version = col_string(&batch, "row_version")?;

        for row in 0..batch.num_rows() {
            let partition_keys = serde_json::from_str::<Vec<String>>(partition_keys.value(row))
                .map_err(|e| {
                    Error::parquet(format!(
                        "failed to parse backfill chunk partition_keys: {e}"
                    ))
                })?;

            out.push(BackfillChunkRow {
                tenant_id: tenant_id.value(row).to_string(),
                workspace_id: workspace_id.value(row).to_string(),
                chunk_id: chunk_id.value(row).to_string(),
                backfill_id: backfill_id.value(row).to_string(),
                chunk_index: chunk_index.value(row),
                partition_keys,
                run_key: run_key.value(row).to_string(),
                run_id: run_id.as_ref().and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                state: str_to_chunk_state(state.value(row))?,
                row_version: row_version.value(row).to_string(),
            });
        }
    }
    Ok(out)
}

// ============================================================================
// State Conversion Helpers
// ============================================================================

fn run_state_to_str(state: RunState) -> &'static str {
    match state {
        RunState::Triggered => "TRIGGERED",
        RunState::Running => "RUNNING",
        RunState::Succeeded => "SUCCEEDED",
        RunState::Failed => "FAILED",
        RunState::Cancelled => "CANCELLED",
    }
}

fn str_to_run_state(s: &str) -> Result<RunState> {
    match s {
        "TRIGGERED" => Ok(RunState::Triggered),
        "RUNNING" => Ok(RunState::Running),
        "SUCCEEDED" => Ok(RunState::Succeeded),
        "FAILED" => Ok(RunState::Failed),
        "CANCELLED" => Ok(RunState::Cancelled),
        _ => Err(Error::parquet(format!("unknown run state '{s}'"))),
    }
}

fn task_state_to_str(state: TaskState) -> &'static str {
    match state {
        TaskState::Planned => "PLANNED",
        TaskState::Blocked => "BLOCKED",
        TaskState::Ready => "READY",
        TaskState::Dispatched => "DISPATCHED",
        TaskState::Running => "RUNNING",
        TaskState::RetryWait => "RETRY_WAIT",
        TaskState::Skipped => "SKIPPED",
        TaskState::Cancelled => "CANCELLED",
        TaskState::Failed => "FAILED",
        TaskState::Succeeded => "SUCCEEDED",
    }
}

fn str_to_task_state(s: &str) -> Result<TaskState> {
    match s {
        "PLANNED" => Ok(TaskState::Planned),
        "BLOCKED" => Ok(TaskState::Blocked),
        "READY" => Ok(TaskState::Ready),
        "DISPATCHED" => Ok(TaskState::Dispatched),
        "RUNNING" => Ok(TaskState::Running),
        "RETRY_WAIT" => Ok(TaskState::RetryWait),
        "SKIPPED" => Ok(TaskState::Skipped),
        "CANCELLED" => Ok(TaskState::Cancelled),
        "FAILED" => Ok(TaskState::Failed),
        "SUCCEEDED" => Ok(TaskState::Succeeded),
        _ => Err(Error::parquet(format!("unknown task state '{s}'"))),
    }
}

fn task_outcome_to_str(outcome: TaskOutcome) -> &'static str {
    match outcome {
        TaskOutcome::Succeeded => "SUCCEEDED",
        TaskOutcome::Failed => "FAILED",
        TaskOutcome::Skipped => "SKIPPED",
        TaskOutcome::Cancelled => "CANCELLED",
    }
}

fn str_to_task_outcome(s: &str) -> Result<TaskOutcome> {
    match s {
        "SUCCEEDED" => Ok(TaskOutcome::Succeeded),
        "FAILED" => Ok(TaskOutcome::Failed),
        "SKIPPED" => Ok(TaskOutcome::Skipped),
        "CANCELLED" => Ok(TaskOutcome::Cancelled),
        _ => Err(Error::parquet(format!("unknown task outcome '{s}'"))),
    }
}

fn dep_resolution_to_str(resolution: DepResolution) -> &'static str {
    match resolution {
        DepResolution::Success => "SUCCESS",
        DepResolution::Failed => "FAILED",
        DepResolution::Skipped => "SKIPPED",
        DepResolution::Cancelled => "CANCELLED",
    }
}

fn str_to_dep_resolution(s: &str) -> Result<DepResolution> {
    match s {
        "SUCCESS" => Ok(DepResolution::Success),
        "FAILED" => Ok(DepResolution::Failed),
        "SKIPPED" => Ok(DepResolution::Skipped),
        "CANCELLED" => Ok(DepResolution::Cancelled),
        _ => Err(Error::parquet(format!("unknown dep resolution '{s}'"))),
    }
}

fn timer_type_to_str(t: TimerType) -> &'static str {
    match t {
        TimerType::Retry => "RETRY",
        TimerType::HeartbeatCheck => "HEARTBEAT_CHECK",
        TimerType::Cron => "CRON",
        TimerType::SlaCheck => "SLA_CHECK",
    }
}

fn str_to_timer_type(s: &str) -> Result<TimerType> {
    match s {
        "RETRY" => Ok(TimerType::Retry),
        "HEARTBEAT_CHECK" => Ok(TimerType::HeartbeatCheck),
        "CRON" => Ok(TimerType::Cron),
        "SLA_CHECK" => Ok(TimerType::SlaCheck),
        _ => Err(Error::parquet(format!("unknown timer type '{s}'"))),
    }
}

fn timer_state_to_str(state: TimerState) -> &'static str {
    match state {
        TimerState::Scheduled => "SCHEDULED",
        TimerState::Fired => "FIRED",
        TimerState::Cancelled => "CANCELLED",
    }
}

fn str_to_timer_state(s: &str) -> Result<TimerState> {
    match s {
        "SCHEDULED" => Ok(TimerState::Scheduled),
        "FIRED" => Ok(TimerState::Fired),
        "CANCELLED" => Ok(TimerState::Cancelled),
        _ => Err(Error::parquet(format!("unknown timer state '{s}'"))),
    }
}

fn dispatch_status_to_str(status: DispatchStatus) -> &'static str {
    match status {
        DispatchStatus::Pending => "PENDING",
        DispatchStatus::Created => "CREATED",
        DispatchStatus::Acked => "ACKED",
        DispatchStatus::Failed => "FAILED",
    }
}

fn str_to_dispatch_status(s: &str) -> Result<DispatchStatus> {
    match s {
        "PENDING" => Ok(DispatchStatus::Pending),
        "CREATED" => Ok(DispatchStatus::Created),
        "ACKED" => Ok(DispatchStatus::Acked),
        "FAILED" => Ok(DispatchStatus::Failed),
        _ => Err(Error::parquet(format!("unknown dispatch status '{s}'"))),
    }
}

fn sensor_status_to_str(status: SensorStatus) -> &'static str {
    match status {
        SensorStatus::Active => "ACTIVE",
        SensorStatus::Paused => "PAUSED",
        SensorStatus::Error => "ERROR",
    }
}

fn str_to_sensor_status(s: &str) -> Result<SensorStatus> {
    match s {
        "ACTIVE" => Ok(SensorStatus::Active),
        "PAUSED" => Ok(SensorStatus::Paused),
        "ERROR" => Ok(SensorStatus::Error),
        _ => Err(Error::parquet(format!("unknown sensor status '{s}'"))),
    }
}

fn backfill_state_to_str(state: BackfillState) -> &'static str {
    match state {
        BackfillState::Pending => "PENDING",
        BackfillState::Running => "RUNNING",
        BackfillState::Paused => "PAUSED",
        BackfillState::Succeeded => "SUCCEEDED",
        BackfillState::Failed => "FAILED",
        BackfillState::Cancelled => "CANCELLED",
    }
}

fn str_to_backfill_state(s: &str) -> Result<BackfillState> {
    match s {
        "PENDING" => Ok(BackfillState::Pending),
        "RUNNING" => Ok(BackfillState::Running),
        "PAUSED" => Ok(BackfillState::Paused),
        "SUCCEEDED" => Ok(BackfillState::Succeeded),
        "FAILED" => Ok(BackfillState::Failed),
        "CANCELLED" => Ok(BackfillState::Cancelled),
        _ => Err(Error::parquet(format!("unknown backfill state '{s}'"))),
    }
}

fn chunk_state_to_str(state: ChunkState) -> &'static str {
    match state {
        ChunkState::Pending => "PENDING",
        ChunkState::Planned => "PLANNED",
        ChunkState::Running => "RUNNING",
        ChunkState::Succeeded => "SUCCEEDED",
        ChunkState::Failed => "FAILED",
    }
}

fn str_to_chunk_state(s: &str) -> Result<ChunkState> {
    match s {
        "PENDING" => Ok(ChunkState::Pending),
        "PLANNED" => Ok(ChunkState::Planned),
        "RUNNING" => Ok(ChunkState::Running),
        "SUCCEEDED" => Ok(ChunkState::Succeeded),
        "FAILED" => Ok(ChunkState::Failed),
        _ => Err(Error::parquet(format!("unknown chunk state '{s}'"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_runs_roundtrip() {
        let rows = vec![RunRow {
            run_id: "run_01HQXYZ123".to_string(),
            plan_id: "plan_01HQXYZ456".to_string(),
            state: RunState::Running,
            run_key: Some("daily-etl:2025-01-15".to_string()),
            labels: HashMap::from([("team".to_string(), "analytics".to_string())]),
            code_version: None,
            cancel_requested: false,
            tasks_total: 5,
            tasks_completed: 2,
            tasks_succeeded: 2,
            tasks_failed: 0,
            tasks_skipped: 0,
            tasks_cancelled: 0,
            triggered_at: Utc::now(),
            completed_at: None,
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_runs(&rows).expect("write");
        let parsed = read_runs(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].run_id, "run_01HQXYZ123");
        assert_eq!(parsed[0].state, RunState::Running);
        assert_eq!(parsed[0].run_key, Some("daily-etl:2025-01-15".to_string()));
        assert_eq!(parsed[0].tasks_total, 5);
        assert_eq!(
            parsed[0].labels.get("team").map(String::as_str),
            Some("analytics")
        );
    }

    #[test]
    fn test_partition_status_roundtrip() {
        let mut partition_values = HashMap::new();
        partition_values.insert("date".to_string(), "2025-01-15".to_string());

        let rows = vec![PartitionStatusRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            asset_key: "analytics.daily".to_string(),
            partition_key: "2025-01-15".to_string(),
            last_materialization_run_id: Some("run_01".to_string()),
            last_materialization_at: Some(Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap()),
            last_materialization_code_version: Some("v1".to_string()),
            last_attempt_run_id: Some("run_02".to_string()),
            last_attempt_at: Some(Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap()),
            last_attempt_outcome: Some(TaskOutcome::Failed),
            stale_since: None,
            stale_reason_code: None,
            partition_values,
            delta_table: Some("analytics.daily".to_string()),
            delta_version: Some(42),
            delta_partition: Some("date=2025-01-15".to_string()),
            execution_lineage_ref: Some(
                "{\"runId\":\"run_01\",\"taskKey\":\"analytics.daily\",\"attempt\":1}".to_string(),
            ),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_partition_status(&rows).expect("write");
        let parsed = read_partition_status(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].asset_key, "analytics.daily");
        assert_eq!(parsed[0].partition_key, "2025-01-15");
        assert_eq!(parsed[0].last_attempt_outcome, Some(TaskOutcome::Failed));
        assert_eq!(parsed[0].delta_version, Some(42));
        assert_eq!(parsed[0].delta_partition.as_deref(), Some("date=2025-01-15"));
        assert_eq!(
            parsed[0].partition_values.get("date").map(String::as_str),
            Some("2025-01-15")
        );
    }

    #[test]
    fn test_backfills_roundtrip() {
        let rows = vec![BackfillRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            backfill_id: "bf_01".to_string(),
            asset_selection: vec!["asset.a".to_string()],
            partition_selector: PartitionSelector::Explicit {
                partition_keys: vec!["2025-01-01".to_string(), "2025-01-02".to_string()],
            },
            chunk_size: 2,
            max_concurrent_runs: 4,
            state: BackfillState::Running,
            state_version: 1,
            total_partitions: 2,
            planned_chunks: 1,
            completed_chunks: 0,
            failed_chunks: 0,
            parent_backfill_id: None,
            created_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_backfills(&rows).expect("write");
        let parsed = read_backfills(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].backfill_id, "bf_01");
        assert_eq!(parsed[0].state, BackfillState::Running);
        assert_eq!(parsed[0].chunk_size, 2);
    }

    #[test]
    fn test_backfill_chunks_roundtrip() {
        let rows = vec![BackfillChunkRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            chunk_id: "bf_01:0".to_string(),
            backfill_id: "bf_01".to_string(),
            chunk_index: 0,
            partition_keys: vec!["2025-01-01".to_string(), "2025-01-02".to_string()],
            run_key: "backfill:bf_01:0".to_string(),
            run_id: Some("run_01".to_string()),
            state: ChunkState::Planned,
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_backfill_chunks(&rows).expect("write");
        let parsed = read_backfill_chunks(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].chunk_id, "bf_01:0");
        assert_eq!(parsed[0].state, ChunkState::Planned);
        assert_eq!(parsed[0].run_id.as_deref(), Some("run_01"));
    }

    #[test]
    fn test_read_runs_defaults_cancel_requested_when_missing_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("plan_id", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("tasks_total", DataType::UInt32, false),
            Field::new("tasks_completed", DataType::UInt32, false),
            Field::new("tasks_succeeded", DataType::UInt32, false),
            Field::new("tasks_failed", DataType::UInt32, false),
            Field::new("tasks_skipped", DataType::UInt32, false),
            Field::new("tasks_cancelled", DataType::UInt32, false),
            Field::new("triggered_at", DataType::Int64, false),
            Field::new("completed_at", DataType::Int64, true),
            Field::new("row_version", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("run_01")])),
                Arc::new(StringArray::from(vec![Some("plan_01")])),
                Arc::new(StringArray::from(vec![Some("RUNNING")])),
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Some("01A")])),
            ],
        )
        .expect("record batch");

        let bytes = write_single_batch(schema, &batch).expect("write");
        let parsed = read_runs(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].run_key.is_none());
        assert!(!parsed[0].cancel_requested);
        assert!(parsed[0].labels.is_empty());
    }

    #[test]
    fn test_tasks_roundtrip() {
        let rows = vec![TaskRow {
            run_id: "run_01HQXYZ123".to_string(),
            task_key: "extract".to_string(),
            state: TaskState::Ready,
            attempt: 1,
            attempt_id: Some("01HQXYZ456ATT".to_string()),
            started_at: Some(Utc::now()),
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: None,
            ready_at: Some(Utc::now()),
            asset_key: Some("analytics.extract".to_string()),
            partition_key: None,
            materialization_id: Some("mat_01".to_string()),
            delta_table: Some("analytics.extract".to_string()),
            delta_version: Some(9),
            delta_partition: Some("date=2025-01-15".to_string()),
            execution_lineage_ref: Some("{\"runId\":\"run_01HQXYZ123\"}".to_string()),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_tasks(&rows).expect("write");
        let parsed = read_tasks(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].task_key, "extract");
        assert_eq!(parsed[0].state, TaskState::Ready);
        assert_eq!(parsed[0].asset_key.as_deref(), Some("analytics.extract"));
        assert_eq!(parsed[0].delta_version, Some(9));
        assert_eq!(parsed[0].delta_partition.as_deref(), Some("date=2025-01-15"));
        assert!(parsed[0].started_at.is_some());
    }

    #[test]
    fn test_dep_satisfaction_roundtrip() {
        let rows = vec![DepSatisfactionRow {
            run_id: "run_01HQXYZ123".to_string(),
            upstream_task_key: "A".to_string(),
            downstream_task_key: "B".to_string(),
            satisfied: true,
            resolution: Some(DepResolution::Success),
            satisfied_at: Some(Utc::now()),
            satisfying_attempt: Some(1),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_dep_satisfaction(&rows).expect("write");
        let parsed = read_dep_satisfaction(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].satisfied);
        assert_eq!(parsed[0].resolution, Some(DepResolution::Success));
    }

    #[test]
    fn test_timers_roundtrip() {
        let rows = vec![TimerRow {
            timer_id: "timer:retry:run1:task1:1:1705320000".to_string(),
            cloud_task_id: Some("t_abc123".to_string()),
            timer_type: TimerType::Retry,
            run_id: Some("run_01".to_string()),
            task_key: Some("task_01".to_string()),
            attempt: Some(1),
            fire_at: Utc::now(),
            state: TimerState::Scheduled,
            payload: Some(r#"{"backoff":30}"#.to_string()),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_timers(&rows).expect("write");
        let parsed = read_timers(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].timer_type, TimerType::Retry);
        assert_eq!(parsed[0].state, TimerState::Scheduled);
    }

    #[test]
    fn test_dispatch_outbox_roundtrip() {
        let rows = vec![DispatchOutboxRow {
            run_id: "run_01".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            dispatch_id: "dispatch:run_01:extract:1".to_string(),
            cloud_task_id: Some("d_xyz789".to_string()),
            status: DispatchStatus::Created,
            attempt_id: "01HQXYZ456ATT".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at: Utc::now(),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_dispatch_outbox(&rows).expect("write");
        let parsed = read_dispatch_outbox(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].status, DispatchStatus::Created);
        assert_eq!(parsed[0].worker_queue, "default-queue");
    }

    #[test]
    fn test_sensor_state_roundtrip() {
        let rows = vec![SensorStateRow {
            tenant_id: "tenant-01".to_string(),
            workspace_id: "workspace-01".to_string(),
            sensor_id: "sensor-01".to_string(),
            cursor: Some("cursor_v1".to_string()),
            last_evaluation_at: Some(Utc::now()),
            last_eval_id: Some("eval_01".to_string()),
            status: SensorStatus::Paused,
            state_version: 3,
            row_version: "01HQXYZ123".to_string(),
        }];

        let bytes = write_sensor_state(&rows).expect("write");
        let parsed = read_sensor_state(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].sensor_id, "sensor-01");
        assert_eq!(parsed[0].status, SensorStatus::Paused);
        assert_eq!(parsed[0].cursor.as_deref(), Some("cursor_v1"));
    }

    #[test]
    fn test_sensor_evals_roundtrip() {
        let rows = vec![SensorEvalRow {
            tenant_id: "tenant-01".to_string(),
            workspace_id: "workspace-01".to_string(),
            eval_id: "eval-01".to_string(),
            sensor_id: "sensor-01".to_string(),
            cursor_before: Some("cursor_v0".to_string()),
            cursor_after: Some("cursor_v1".to_string()),
            expected_state_version: Some(2),
            trigger_source: TriggerSource::Poll {
                poll_epoch: 1_705_320_000,
            },
            run_requests: vec![RunRequest {
                run_key: "sensor:eval-01".to_string(),
                request_fingerprint: "fp_123".to_string(),
                asset_selection: vec!["asset.a".to_string()],
                partition_selection: None,
            }],
            status: SensorEvalStatus::Triggered,
            evaluated_at: Utc::now(),
            row_version: "01HQXYZ999".to_string(),
        }];

        let bytes = write_sensor_evals(&rows).expect("write");
        let parsed = read_sensor_evals(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].eval_id, "eval-01");
        assert_eq!(parsed[0].expected_state_version, Some(2));
        assert_eq!(parsed[0].run_requests.len(), 1);
        assert_eq!(parsed[0].status, SensorEvalStatus::Triggered);
    }

    #[test]
    fn test_idempotency_keys_roundtrip() {
        let rows = vec![IdempotencyKeyRow {
            tenant_id: "tenant-01".to_string(),
            workspace_id: "workspace-01".to_string(),
            idempotency_key: "sensor_eval:sensor-01:msg:abc123".to_string(),
            event_id: "01HQXYZ123".to_string(),
            event_type: "SensorEvaluated".to_string(),
            recorded_at: Utc::now(),
            row_version: "01HQXYZ123".to_string(),
        }];

        let bytes = write_idempotency_keys(&rows).expect("write");
        let parsed = read_idempotency_keys(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed[0].idempotency_key,
            "sensor_eval:sensor-01:msg:abc123"
        );
        assert_eq!(parsed[0].event_type, "SensorEvaluated");
    }

    #[test]
    fn test_schedule_definitions_roundtrip() {
        let rows = vec![ScheduleDefinitionRow {
            tenant_id: "tenant-01".to_string(),
            workspace_id: "workspace-01".to_string(),
            schedule_id: "daily-etl".to_string(),
            cron_expression: "0 10 * * *".to_string(),
            timezone: "UTC".to_string(),
            catchup_window_minutes: 60,
            asset_selection: vec!["analytics.summary".to_string()],
            max_catchup_ticks: 3,
            enabled: true,
            created_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_schedule_definitions(&rows).expect("write");
        let parsed = read_schedule_definitions(&bytes).expect("read");

        assert_eq!(parsed, rows);
    }

    #[test]
    fn test_schedule_state_roundtrip() {
        let rows = vec![ScheduleStateRow {
            tenant_id: "tenant-01".to_string(),
            workspace_id: "workspace-01".to_string(),
            schedule_id: "daily-etl".to_string(),
            last_scheduled_for: Some(Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap()),
            last_tick_id: Some("daily-etl:1736935200".to_string()),
            last_run_key: Some("sched:daily-etl:1736935200".to_string()),
            row_version: "01HQXYZ790".to_string(),
        }];

        let bytes = write_schedule_state(&rows).expect("write");
        let parsed = read_schedule_state(&bytes).expect("read");

        assert_eq!(parsed, rows);
    }

    #[test]
    fn test_schedule_ticks_roundtrip() {
        let rows = vec![ScheduleTickRow {
            tenant_id: "tenant-01".to_string(),
            workspace_id: "workspace-01".to_string(),
            tick_id: "daily-etl:1736935200".to_string(),
            schedule_id: "daily-etl".to_string(),
            scheduled_for: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            definition_version: "01HQDEF".to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: Some(vec!["2025-01-15".to_string()]),
            status: TickStatus::Triggered,
            run_key: Some("sched:daily-etl:1736935200".to_string()),
            run_id: Some("run_01HQXYZ123".to_string()),
            request_fingerprint: Some("fp_123".to_string()),
            row_version: "01HQXYZ791".to_string(),
        }];

        let bytes = write_schedule_ticks(&rows).expect("write");
        let parsed = read_schedule_ticks(&bytes).expect("read");

        assert_eq!(parsed, rows);
    }

    #[test]
    fn test_read_runs_rejects_unknown_state() {
        let schema = Arc::new(run_schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("run_01")])),
                Arc::new(StringArray::from(vec![Some("plan_01")])),
                Arc::new(StringArray::from(vec![Some("UNKNOWN_STATE")])),
                Arc::new(StringArray::from(vec![None::<&str>])), // run_key
                Arc::new(BooleanArray::from(vec![false])),       // cancel_requested
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Some("01A")])),
            ],
        )
        .expect("record batch");

        let bytes = write_single_batch(schema, &batch).expect("write");
        let err = read_runs(&bytes).unwrap_err();
        assert!(err.to_string().contains("unknown run state"));
    }

    #[test]
    fn test_read_tasks_rejects_unknown_state() {
        let schema = Arc::new(task_schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("run_01")])),
                Arc::new(StringArray::from(vec![Some("extract")])),
                Arc::new(StringArray::from(vec![Some("UNKNOWN_TASK")])),
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![3])),
                Arc::new(UInt32Array::from(vec![300])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(StringArray::from(vec![Some("01A")])),
            ],
        )
        .expect("record batch");

        let bytes = write_single_batch(schema, &batch).expect("write");
        let err = read_tasks(&bytes).unwrap_err();
        assert!(err.to_string().contains("unknown task state"));
    }

    #[test]
    fn test_read_tasks_defaults_asset_partition_when_missing_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("task_key", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("attempt", DataType::UInt32, false),
            Field::new("attempt_id", DataType::Utf8, true),
            Field::new("deps_total", DataType::UInt32, false),
            Field::new("deps_satisfied_count", DataType::UInt32, false),
            Field::new("max_attempts", DataType::UInt32, false),
            Field::new("heartbeat_timeout_sec", DataType::UInt32, false),
            Field::new("last_heartbeat_at", DataType::Int64, true),
            Field::new("ready_at", DataType::Int64, true),
            Field::new("row_version", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("run_01")])),
                Arc::new(StringArray::from(vec![Some("extract")])),
                Arc::new(StringArray::from(vec![Some("READY")])),
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("att_01")])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![3])),
                Arc::new(UInt32Array::from(vec![300])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Some("01A")])),
            ],
        )
        .expect("record batch");

        let bytes = write_single_batch(schema, &batch).expect("write");
        let parsed = read_tasks(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].asset_key.is_none());
        assert!(parsed[0].partition_key.is_none());
        assert!(parsed[0].materialization_id.is_none());
        assert!(parsed[0].delta_table.is_none());
        assert!(parsed[0].delta_version.is_none());
        assert!(parsed[0].delta_partition.is_none());
        assert!(parsed[0].execution_lineage_ref.is_none());
    }
}
