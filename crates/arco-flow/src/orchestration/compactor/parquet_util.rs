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
//! - `idempotency_keys.parquet`
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
    DepResolution, DepSatisfactionRow, DispatchOutboxRow, DispatchStatus, IdempotencyKeyRow,
    RunRow, RunState, SensorEvalRow, SensorStateRow, TaskRow, TaskState, TimerRow, TimerState,
    TimerType,
};
use crate::error::{Error, Result};
use crate::orchestration::events::{RunRequest, SensorEvalStatus, SensorStatus, TriggerSource};

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
        let asset_key = col_string(&batch, "asset_key")?;
        let partition_key = col_string(&batch, "partition_key")?;
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
                asset_key: if asset_key.is_null(row) {
                    None
                } else {
                    Some(asset_key.value(row).to_string())
                },
                partition_key: if partition_key.is_null(row) {
                    None
                } else {
                    Some(partition_key.value(row).to_string())
                },
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_runs_roundtrip() {
        let rows = vec![RunRow {
            run_id: "run_01HQXYZ123".to_string(),
            plan_id: "plan_01HQXYZ456".to_string(),
            state: RunState::Running,
            run_key: Some("daily-etl:2025-01-15".to_string()),
            labels: HashMap::from([("team".to_string(), "analytics".to_string())]),
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
            row_version: "01HQXYZ789".to_string(),
        }];

        let bytes = write_tasks(&rows).expect("write");
        let parsed = read_tasks(&bytes).expect("read");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].task_key, "extract");
        assert_eq!(parsed[0].state, TaskState::Ready);
        assert_eq!(parsed[0].asset_key.as_deref(), Some("analytics.extract"));
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
                Arc::new(StringArray::from(vec![Some("01A")])),
            ],
        )
        .expect("record batch");

        let bytes = write_single_batch(schema, &batch).expect("write");
        let err = read_tasks(&bytes).unwrap_err();
        assert!(err.to_string().contains("unknown task state"));
    }
}
