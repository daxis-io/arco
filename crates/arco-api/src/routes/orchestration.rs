//! Orchestration API routes for M1.
//!
//! Provides run management endpoints per the execution prompt.
//!
//! ## Routes
//!
//! - `POST   /workspaces/{workspace_id}/runs` - Trigger a new run
//! - `GET    /workspaces/{workspace_id}/runs/{run_id}` - Get run by ID
//! - `GET    /workspaces/{workspace_id}/runs` - List runs
//! - `POST   /workspaces/{workspace_id}/runs/{run_id}/cancel` - Cancel a run
//! - `POST   /workspaces/{workspace_id}/runs/{run_id}/logs` - Upload task logs
//! - `GET    /workspaces/{workspace_id}/runs/{run_id}/logs` - Get run logs
//! - `POST   /workspaces/{workspace_id}/sensors/{sensor_id}/evaluate` - Manual sensor evaluate
//!
//! ## Worker Callbacks
//!
//! - `POST   /tasks/{task_id}/started` - Worker started execution
//! - `POST   /tasks/{task_id}/heartbeat` - Worker heartbeat
//! - `POST   /tasks/{task_id}/completed` - Worker finished execution

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

use axum::extract::Query as AxumQuery;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::{IntoParams, ToSchema};

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::orchestration_compaction::compact_orchestration_events;
use crate::paths::{MANIFEST_IDEMPOTENCY_PREFIX, MANIFEST_PREFIX, backfill_idempotency_path};
use crate::routes::manifests::StoredManifest;
use crate::server::AppState;
use arco_core::{Error as CoreError, ScopedStorage, WritePrecondition, WriteResult};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{
    BackfillChunkRow, BackfillRow, FoldState, MicroCompactor, PartitionStatusRow, RunRow,
    RunState as FoldRunState, ScheduleDefinitionRow, ScheduleStateRow, ScheduleTickRow,
    SensorEvalRow, SensorStateRow, TaskRow, TaskState as FoldTaskState,
};
use arco_flow::orchestration::controllers::{PubSubMessage, PushSensorHandler};
use arco_flow::orchestration::events::{
    BackfillState, ChunkState, OrchestrationEvent, OrchestrationEventData, PartitionSelector,
    RunRequest, SensorEvalStatus, SensorStatus, TaskDef, TickStatus, TriggerInfo,
};
use arco_flow::orchestration::run_key::{
    FingerprintPolicy, ReservationResult, RunKeyReservation, get_reservation, reservation_path,
    reserve_run_key,
};
use ulid::Ulid;

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to trigger a new run.
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TriggerRunRequest {
    /// Asset selection (asset keys to materialize).
    #[serde(default)]
    pub selection: Vec<String>,
    /// Include upstream dependencies of the selection.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the selection.
    #[serde(default)]
    pub include_downstream: bool,
    /// Partition overrides (key=value pairs).
    #[serde(default)]
    pub partitions: Vec<PartitionValue>,
    /// Idempotency key for deduplication (409 if reused with different payload).
    /// Reservations created before the fingerprint cutoff remain lenient.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the run.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Rerun mode values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum RerunMode {
    /// Rerun all tasks that did not succeed in the parent run.
    FromFailure,
    /// Rerun a user-chosen subset of tasks from the parent plan.
    Subset,
}

/// Rerun kind values exposed in responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RerunKindResponse {
    /// Rerun tasks that did not succeed.
    FromFailure,
    /// Rerun a selected subset.
    Subset,
}

/// Request to rerun a prior run.
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RerunRunRequest {
    /// Rerun mode.
    pub mode: RerunMode,
    /// Selection roots for subset reruns.
    #[serde(default)]
    pub selection: Vec<String>,
    /// Include upstream dependencies of the subset.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the subset.
    #[serde(default)]
    pub include_downstream: bool,
    /// Optional run key override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the rerun.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Response after triggering a rerun.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RerunRunResponse {
    /// Run ID (ULID).
    pub run_id: String,
    /// Plan ID for this run.
    pub plan_id: String,
    /// Current run state.
    pub state: RunStateResponse,
    /// Whether this is a new run or an existing one (`run_key` deduplication).
    pub created: bool,
    /// Run creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Parent run ID.
    pub parent_run_id: String,
    /// Rerun kind.
    pub rerun_kind: RerunKindResponse,
}

/// Request to backfill a `run_key` reservation fingerprint.
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RunKeyBackfillRequest {
    /// Idempotency key to backfill.
    pub run_key: String,
    /// Asset selection (asset keys to materialize).
    #[serde(default)]
    pub selection: Vec<String>,
    /// Include upstream dependencies of the selection.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the selection.
    #[serde(default)]
    pub include_downstream: bool,
    /// Partition overrides (key=value pairs).
    #[serde(default)]
    pub partitions: Vec<PartitionValue>,
    /// Additional labels for the run.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Partition key-value pair.
#[derive(Debug, Deserialize, Serialize, ToSchema, Clone)]
pub struct PartitionValue {
    /// Partition dimension name.
    pub key: String,
    /// Partition value.
    pub value: String,
}

/// Response after triggering a run.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TriggerRunResponse {
    /// Run ID (ULID).
    pub run_id: String,
    /// Plan ID for this run.
    pub plan_id: String,
    /// Current run state.
    pub state: RunStateResponse,
    /// Whether this is a new run or an existing one (`run_key` deduplication).
    pub created: bool,
    /// Run creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Response after backfilling a `run_key` reservation.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunKeyBackfillResponse {
    /// Run key that was updated.
    pub run_key: String,
    /// Run ID associated with the reservation.
    pub run_id: String,
    /// Fingerprint stored on the reservation.
    pub request_fingerprint: String,
    /// Whether the reservation was updated.
    pub updated: bool,
    /// Reservation creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Run state values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunStateResponse {
    /// Run is pending (waiting to start).
    Pending,
    /// Run is actively executing.
    Running,
    /// Run cancellation has been requested.
    Cancelling,
    /// Run completed successfully.
    Succeeded,
    /// Run failed.
    Failed,
    /// Run was cancelled.
    Cancelled,
    /// Run timed out.
    TimedOut,
}

/// Full run details response.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunResponse {
    /// Run ID.
    pub run_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Plan ID.
    pub plan_id: String,
    /// Current state.
    pub state: RunStateResponse,
    /// Run creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Run start timestamp (when first task started).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// Run completion timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Task execution summaries.
    pub tasks: Vec<TaskSummary>,
    /// Task counts by state.
    pub task_counts: TaskCounts,
    /// Run labels.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Parent run ID (for reruns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    /// Rerun kind (for reruns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerun_kind: Option<RerunKindResponse>,
}

/// Task execution summary.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskSummary {
    /// Task key.
    pub task_key: String,
    /// Asset key (if asset task).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_key: Option<String>,
    /// Task state.
    pub state: TaskStateResponse,
    /// Current attempt number.
    pub attempt: u32,
    /// Task start time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// Task completion time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Task state values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStateResponse {
    /// Task is pending (waiting for dependencies).
    Pending,
    /// Task is queued (ready to execute).
    Queued,
    /// Task is running.
    Running,
    /// Task succeeded.
    Succeeded,
    /// Task failed.
    Failed,
    /// Task was skipped (upstream failure).
    Skipped,
    /// Task was cancelled.
    Cancelled,
}

/// Task counts by state.
#[derive(Debug, Default, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskCounts {
    /// Total tasks.
    pub total: u32,
    /// Pending tasks.
    pub pending: u32,
    /// Queued tasks.
    pub queued: u32,
    /// Running tasks.
    pub running: u32,
    /// Succeeded tasks.
    pub succeeded: u32,
    /// Failed tasks.
    pub failed: u32,
    /// Skipped tasks.
    pub skipped: u32,
    /// Cancelled tasks.
    pub cancelled: u32,
}

/// Query parameters for listing runs.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListRunsQuery {
    /// Maximum number of runs to return.
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Filter by state.
    pub state: Option<RunStateResponse>,
}

fn default_limit() -> u32 {
    20
}

const MAX_LIST_LIMIT: u32 = 200;
const DEFAULT_LIMIT: u32 = 50;
const MAX_LOG_BYTES: usize = 2 * 1024 * 1024;

/// Response for listing runs.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListRunsResponse {
    /// List of runs.
    pub runs: Vec<RunListItem>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Run list item (summary).
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunListItem {
    /// Run ID.
    pub run_id: String,
    /// Current state.
    pub state: RunStateResponse,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Completion timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Total task count.
    pub task_count: u32,
    /// Tasks succeeded.
    pub tasks_succeeded: u32,
    /// Tasks failed.
    pub tasks_failed: u32,
    /// Parent run ID (for reruns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    /// Rerun kind (for reruns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerun_kind: Option<RerunKindResponse>,
}

/// Request to cancel a run.
#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunRequest {
    /// Reason for cancellation.
    pub reason: Option<String>,
}

/// Response after cancelling a run.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunResponse {
    /// Run ID.
    pub run_id: String,
    /// New state (should be CANCELLED or CANCELLING).
    pub state: RunStateResponse,
    /// Cancellation timestamp.
    pub cancelled_at: DateTime<Utc>,
}

/// Request to upload logs for a task run.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunLogsRequest {
    /// Task key that produced the logs.
    pub task_key: String,
    /// Attempt number (1-indexed).
    pub attempt: u32,
    /// Captured stdout.
    #[serde(default)]
    pub stdout: String,
    /// Captured stderr.
    #[serde(default)]
    pub stderr: String,
}

/// Response after uploading logs.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunLogsResponse {
    /// Stored log path (relative to workspace scope).
    pub path: String,
    /// Size of the stored log payload in bytes.
    pub size_bytes: u64,
}

/// Query parameters for retrieving logs.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct RunLogsQuery {
    /// Filter by task key.
    pub task_key: Option<String>,
}

/// Request to manually evaluate a sensor.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualSensorEvaluateRequest {
    /// Raw payload to pass to the sensor evaluator.
    pub payload: serde_json::Value,
    /// Message attributes to pass to the evaluator.
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    /// Optional message identifier for idempotency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

/// Response after manually evaluating a sensor.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualSensorEvaluateResponse {
    /// Evaluation identifier.
    pub eval_id: String,
    /// Message identifier used for idempotency.
    pub message_id: String,
    /// Evaluation status.
    #[serde(flatten)]
    pub status: SensorEvalStatusResponse,
    /// Run requests produced by the evaluation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub run_requests: Vec<RunRequestResponse>,
    /// Number of events written to the ledger.
    pub events_written: u32,
    /// Warning if manual evaluate bypassed sensor status (paused/error).
    /// Manual evaluate always runs regardless of sensor operational state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Sensor evaluation status (API response).
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SensorEvalStatusResponse {
    /// Sensor triggered one or more runs.
    Triggered,
    /// Sensor evaluated but no new data found.
    NoNewData,
    /// Sensor evaluation failed.
    Error {
        /// Error message describing the failure.
        message: String,
    },
    /// Sensor evaluation was skipped due to stale cursor (CAS failed).
    SkippedStaleCursor,
}

/// Run request returned from sensor evaluation.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunRequestResponse {
    /// Stable run key for idempotency.
    pub run_key: String,
    /// Fingerprint of the request payload for conflict detection.
    pub request_fingerprint: String,
    /// Assets to materialize.
    pub asset_selection: Vec<String>,
    /// Optional partition selection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_selection: Option<Vec<String>>,
}

// ============================================================================
// Schedule API Types
// ============================================================================

/// Schedule state response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleResponse {
    /// Schedule identifier.
    pub schedule_id: String,
    /// Cron expression for the schedule.
    pub cron_expression: String,
    /// IANA timezone used for evaluation.
    pub timezone: String,
    /// Maximum time window for catchup.
    pub catchup_window_minutes: u32,
    /// Maximum number of ticks to catch up per evaluation.
    pub max_catchup_ticks: u32,
    /// Asset selection snapshot stored with the definition.
    pub asset_selection: Vec<String>,
    /// Whether the schedule is enabled.
    pub enabled: bool,
    /// Current definition version.
    pub definition_version: String,
    /// Last `scheduled_for` timestamp that was processed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scheduled_for: Option<DateTime<Utc>>,
    /// Last tick ID that was processed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tick_id: Option<String>,
    /// Last run key generated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_key: Option<String>,
}

/// Response for listing schedules.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListSchedulesResponse {
    /// List of schedules.
    pub schedules: Vec<ScheduleResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Request body for creating/updating a schedule definition.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertScheduleRequest {
    /// Cron expression for the schedule.
    pub cron_expression: String,
    /// IANA timezone used for evaluation.
    pub timezone: String,
    /// Maximum time window for catchup.
    pub catchup_window_minutes: u32,
    /// Maximum number of ticks to catch up per evaluation.
    pub max_catchup_ticks: u32,
    /// Asset selection snapshot stored with the definition.
    #[serde(default)]
    pub asset_selection: Vec<String>,
    /// Whether the schedule is enabled.
    pub enabled: bool,
}

/// Schedule tick response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleTickResponse {
    /// Unique tick ID.
    pub tick_id: String,
    /// Schedule identifier.
    pub schedule_id: String,
    /// When this tick was scheduled for.
    pub scheduled_for: DateTime<Utc>,
    /// Asset selection at tick time.
    pub asset_selection: Vec<String>,
    /// Tick status.
    pub status: TickStatusResponse,
    /// Skip reason when status is `SKIPPED`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    /// Error message when status is `FAILED`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /// Run key if a run was requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Run ID if resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

/// Tick status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TickStatusResponse {
    /// Tick triggered a run.
    Triggered,
    /// Tick was skipped.
    Skipped,
    /// Tick failed to evaluate.
    Failed,
}

/// Response for listing schedule ticks.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListScheduleTicksResponse {
    /// List of ticks.
    pub ticks: Vec<ScheduleTickResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Query parameters for listing schedule ticks.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListTicksQuery {
    /// Maximum ticks to return.
    pub limit: Option<u32>,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Filter by tick status.
    pub status: Option<TickStatusResponse>,
}

// ============================================================================
// Sensor API Types
// ============================================================================

/// Sensor state response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SensorResponse {
    /// Sensor identifier.
    pub sensor_id: String,
    /// Current cursor value (poll sensors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Last successful evaluation time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluation_at: Option<DateTime<Utc>>,
    /// Sensor status.
    pub status: SensorStatusResponse,
    /// State version (for CAS).
    pub state_version: u32,
}

/// Sensor status values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SensorStatusResponse {
    /// Sensor is active.
    Active,
    /// Sensor is paused.
    Paused,
    /// Sensor is in error state.
    Error,
}

/// Response for listing sensors.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListSensorsResponse {
    /// List of sensors.
    pub sensors: Vec<SensorResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Sensor evaluation history response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SensorEvalResponse {
    /// Evaluation identifier.
    pub eval_id: String,
    /// Sensor identifier.
    pub sensor_id: String,
    /// Cursor before evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_before: Option<String>,
    /// Cursor after evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_after: Option<String>,
    /// Evaluation timestamp.
    pub evaluated_at: DateTime<Utc>,
    /// Evaluation status.
    pub status: SensorEvalStatusResponse,
    /// Number of run requests generated.
    pub run_requests_count: u32,
}

/// Response for listing sensor evaluations.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListSensorEvalsResponse {
    /// List of evaluations.
    pub evals: Vec<SensorEvalResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// ============================================================================
// Backfill API Types
// ============================================================================

const DEFAULT_BACKFILL_CHUNK_SIZE: u32 = 10;
const DEFAULT_BACKFILL_MAX_CONCURRENT_RUNS: u32 = 2;

/// Request to create a backfill.
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateBackfillRequest {
    /// Assets to backfill.
    #[serde(default)]
    pub asset_selection: Vec<String>,
    /// Partition selector.
    pub partition_selector: PartitionSelectorRequest,
    /// Client request ID for idempotency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<String>,
    /// Number of partitions per chunk (0 uses default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u32>,
    /// Maximum concurrent chunk runs (0 uses default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_runs: Option<u32>,
}

/// Response after creating a backfill.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateBackfillResponse {
    /// Backfill identifier.
    pub backfill_id: String,
    /// Event ID that was accepted.
    pub accepted_event_id: String,
    /// Time the event was accepted.
    pub accepted_at: DateTime<Utc>,
}

/// Partition selector request.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionSelectorRequest {
    /// Range of partitions.
    Range {
        /// Start partition (inclusive).
        start: String,
        /// End partition (inclusive).
        end: String,
    },
    /// Explicit list of partitions.
    Explicit {
        /// Partition keys.
        partitions: Vec<String>,
    },
    /// Filter-based selection.
    Filter {
        /// Filter expressions.
        filters: HashMap<String, String>,
    },
}

/// Error response for unsupported partition selectors.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnprocessableEntityResponse {
    /// Error category.
    pub error: String,
    /// Human-readable error message.
    pub message: String,
    /// Stable error code.
    pub code: String,
}

/// Backfill response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackfillResponse {
    /// Backfill identifier.
    pub backfill_id: String,
    /// Assets to backfill.
    pub asset_selection: Vec<String>,
    /// Partition selector.
    pub partition_selector: PartitionSelectorResponse,
    /// Number of partitions per chunk.
    pub chunk_size: u32,
    /// Maximum concurrent chunk runs.
    pub max_concurrent_runs: u32,
    /// Current state.
    pub state: BackfillStateResponse,
    /// State version (for CAS).
    pub state_version: u32,
    /// When the backfill was created.
    pub created_at: DateTime<Utc>,
    /// Chunk counts by state.
    pub chunk_counts: ChunkCounts,
}

/// Partition selector response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionSelectorResponse {
    /// Range of partitions.
    Range {
        /// Start partition (inclusive).
        start: String,
        /// End partition (inclusive).
        end: String,
    },
    /// Explicit list of partitions.
    Explicit {
        /// Partition keys.
        partitions: Vec<String>,
    },
    /// Filter-based selection.
    Filter {
        /// Filter expressions.
        filters: HashMap<String, String>,
    },
}

/// Backfill state values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackfillStateResponse {
    /// Backfill created but not yet started.
    Pending,
    /// Backfill is actively processing chunks.
    Running,
    /// Backfill is paused.
    Paused,
    /// All chunks completed successfully.
    Succeeded,
    /// At least one chunk failed after retries.
    Failed,
    /// Backfill was cancelled.
    Cancelled,
}

/// Chunk counts by state.
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChunkCounts {
    /// Total chunks.
    pub total: u32,
    /// Pending chunks.
    pub pending: u32,
    /// Running chunks.
    pub running: u32,
    /// Succeeded chunks.
    pub succeeded: u32,
    /// Failed chunks.
    pub failed: u32,
}

/// Response for listing backfills.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListBackfillsResponse {
    /// List of backfills.
    pub backfills: Vec<BackfillResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Backfill chunk response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackfillChunkResponse {
    /// Chunk identifier.
    pub chunk_id: String,
    /// Backfill identifier.
    pub backfill_id: String,
    /// Zero-indexed chunk number.
    pub chunk_index: u32,
    /// Partition keys in this chunk.
    pub partition_keys: Vec<String>,
    /// Run key for this chunk.
    pub run_key: String,
    /// Run ID if resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Chunk state.
    pub state: ChunkStateResponse,
}

/// Chunk state values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChunkStateResponse {
    /// Chunk is pending.
    Pending,
    /// Chunk has been planned.
    Planned,
    /// Chunk run is in progress.
    Running,
    /// Chunk completed successfully.
    Succeeded,
    /// Chunk failed.
    Failed,
}

/// Response for listing backfill chunks.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListBackfillChunksResponse {
    /// List of chunks.
    pub chunks: Vec<BackfillChunkResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// ============================================================================
// Partition Status API Types
// ============================================================================

/// Partition status response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionStatusApiResponse {
    /// Asset key.
    pub asset_key: String,
    /// Partition key.
    pub partition_key: String,
    /// Run that last materialized (success only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_materialization_run_id: Option<String>,
    /// Timestamp of last successful materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_materialization_at: Option<DateTime<Utc>>,
    /// Whether this partition is stale.
    pub is_stale: bool,
    /// Why the partition is stale.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    /// Dimension key-values for the partition.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub partition_values: HashMap<String, String>,
}

/// Response for listing partitions.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListPartitionsResponse {
    /// List of partitions.
    pub partitions: Vec<PartitionStatusApiResponse>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Asset partition summary response.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssetPartitionSummaryResponse {
    /// Asset key.
    pub asset_key: String,
    /// Total partition count.
    pub total_partitions: u32,
    /// Materialized partition count.
    pub materialized_partitions: u32,
    /// Stale partition count.
    pub stale_partitions: u32,
    /// Missing partition count.
    pub missing_partitions: u32,
}

// ============================================================================
// Query Parameter Types
// ============================================================================

/// Common list query parameters.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery {
    /// Maximum items to return.
    pub limit: Option<u32>,
    /// Cursor for pagination.
    pub cursor: Option<String>,
}

/// Query parameters for listing backfills.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListBackfillsQuery {
    /// Maximum backfills to return.
    pub limit: Option<u32>,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Filter by state.
    pub state: Option<BackfillStateResponse>,
}

/// Query parameters for listing backfill chunks.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListChunksQuery {
    /// Maximum chunks to return.
    pub limit: Option<u32>,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Filter by state.
    pub state: Option<ChunkStateResponse>,
}

/// Query parameters for listing partitions.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListPartitionsQuery {
    /// Maximum partitions to return.
    pub limit: Option<u32>,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Filter by asset key.
    pub asset_key: Option<String>,
    /// Filter to stale partitions only.
    pub stale_only: Option<bool>,
}

// ============================================================================
// Helpers
// ============================================================================

fn ensure_workspace(ctx: &RequestContext, workspace_id: &str) -> Result<(), ApiError> {
    if workspace_id != ctx.workspace {
        return Err(ApiError::not_found("workspace not found"));
    }
    Ok(())
}

const MANIFEST_LATEST_INDEX_PATH: &str = "manifests/_index.json";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestManifestIndex {
    latest_manifest_id: String,
    deployed_at: DateTime<Utc>,
}

async fn load_latest_manifest(storage: &ScopedStorage) -> Result<Option<StoredManifest>, ApiError> {
    match storage.get_raw(MANIFEST_LATEST_INDEX_PATH).await {
        Ok(bytes) => match serde_json::from_slice::<LatestManifestIndex>(&bytes) {
            Ok(index) => {
                let path = crate::paths::manifest_path(&index.latest_manifest_id);
                match storage.get_raw(&path).await {
                    Ok(bytes) => match serde_json::from_slice::<StoredManifest>(&bytes) {
                        Ok(stored) => return Ok(Some(stored)),
                        Err(err) => {
                            tracing::warn!(
                                manifest_id = %index.latest_manifest_id,
                                error = ?err,
                                "failed to parse manifest referenced by latest index; falling back to scan"
                            );
                        }
                    },
                    Err(CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }) => {
                        tracing::warn!(
                            manifest_id = %index.latest_manifest_id,
                            "latest manifest index referenced missing manifest; falling back to scan"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            manifest_id = %index.latest_manifest_id,
                            error = ?err,
                            "failed to read manifest referenced by latest index; falling back to scan"
                        );
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    "failed to parse latest manifest index; falling back to scan"
                );
            }
        },
        Err(CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }) => {}
        Err(err) => {
            tracing::warn!(
                error = ?err,
                "failed to read latest manifest index; falling back to scan"
            );
        }
    }

    let entries = storage
        .list_meta(MANIFEST_PREFIX)
        .await
        .map_err(|e| ApiError::internal(format!("failed to list manifests: {e}")))?;

    let mut latest: Option<StoredManifest> = None;

    for entry in entries {
        let path = entry.path.as_str();
        if path.starts_with(MANIFEST_IDEMPOTENCY_PREFIX) || path.ends_with("_index.json") {
            continue;
        }
        if !path.ends_with(".json") {
            continue;
        }

        let bytes = match storage.get_raw(path).await {
            Ok(b) => b,
            Err(CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }) => continue,
            Err(err) => {
                tracing::warn!(path = %path, error = ?err, "failed to read manifest; skipping");
                continue;
            }
        };

        let stored: StoredManifest = match serde_json::from_slice(&bytes) {
            Ok(stored) => stored,
            Err(err) => {
                tracing::warn!(path = %path, error = ?err, "failed to parse manifest; skipping");
                continue;
            }
        };

        let should_replace = latest
            .as_ref()
            .is_none_or(|existing| stored.deployed_at > existing.deployed_at);

        if should_replace {
            latest = Some(stored);
        }
    }

    Ok(latest)
}

async fn plan_tasks_and_root_assets(
    storage: &ScopedStorage,
    request: &TriggerRunRequest,
) -> Result<(String, DateTime<Utc>, Vec<String>, Vec<TaskDef>), ApiError> {
    let stored = load_latest_manifest(storage).await?;
    let Some(stored) = stored else {
        return Err(ApiError::bad_request(
            "no manifest deployed for workspace; deploy a manifest before triggering a run",
        ));
    };

    let manifest_id = stored.manifest_id.clone();
    let deployed_at = stored.deployed_at;

    let mut known_assets = HashSet::new();
    let mut manifest_assets = Vec::with_capacity(stored.assets.len());

    for asset in &stored.assets {
        let key = arco_flow::orchestration::canonicalize_asset_key(&format!(
            "{}/{}",
            asset.key.namespace, asset.key.name
        ))
        .map_err(|err| {
            ApiError::internal(format!(
                "invalid asset key in manifest {}: {err}",
                stored.manifest_id
            ))
        })?;

        known_assets.insert(key.clone());
        manifest_assets.push((key, asset.dependencies.as_slice()));
    }

    let mut graph = arco_flow::orchestration::AssetGraph::new();
    for (asset_key, deps) in manifest_assets {
        let mut upstream = Vec::new();
        for dep in deps {
            let upstream_key = arco_flow::orchestration::canonicalize_asset_key(&format!(
                "{}/{}",
                dep.upstream_key.namespace, dep.upstream_key.name
            ))
            .map_err(|err| {
                ApiError::internal(format!(
                    "invalid asset dependency key in manifest {}: {err}",
                    stored.manifest_id
                ))
            })?;

            if known_assets.contains(&upstream_key) {
                upstream.push(upstream_key);
            } else {
                tracing::warn!(
                    asset_key = %asset_key,
                    upstream_key = %upstream_key,
                    manifest_id = %stored.manifest_id,
                    "dropping dependency edge to unknown upstream"
                );
            }
        }
        graph.insert_asset(asset_key, upstream);
    }

    let mut roots = BTreeSet::new();
    let mut unknown_roots = BTreeSet::new();
    for root in &request.selection {
        let canonical = arco_flow::orchestration::canonicalize_asset_key(root)
            .map_err(ApiError::bad_request)?;

        if known_assets.contains(&canonical) {
            roots.insert(canonical);
        } else {
            unknown_roots.insert(canonical);
        }
    }

    if !unknown_roots.is_empty() {
        return Err(ApiError::bad_request(format!(
            "unknown asset keys: {}",
            unknown_roots.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }

    let root_assets: Vec<String> = roots.iter().cloned().collect();

    let partition_key = build_partition_key(&request.partitions)?;
    let options = arco_flow::orchestration::SelectionOptions {
        include_upstream: request.include_upstream,
        include_downstream: request.include_downstream,
    };

    let tasks = arco_flow::orchestration::build_task_defs_for_selection(
        &graph,
        &root_assets,
        options,
        partition_key,
    )
    .map_err(ApiError::bad_request)?;

    Ok((manifest_id, deployed_at, root_assets, tasks))
}

type PlanGraph = (
    HashMap<String, TaskRow>,
    HashMap<String, Vec<String>>,
    HashMap<String, Vec<String>>,
);

fn build_plan_graph_for_run(fold_state: &FoldState, run_id: &str) -> PlanGraph {
    let mut tasks_by_key = HashMap::new();
    for row in fold_state.tasks.values().filter(|row| row.run_id == run_id) {
        tasks_by_key.insert(row.task_key.clone(), row.clone());
    }

    let mut upstream_by_task: HashMap<String, Vec<String>> = HashMap::new();
    let mut downstream_by_task: HashMap<String, Vec<String>> = HashMap::new();

    for task_key in tasks_by_key.keys() {
        upstream_by_task.insert(task_key.clone(), Vec::new());
        downstream_by_task.insert(task_key.clone(), Vec::new());
    }

    for edge in fold_state
        .dep_satisfaction
        .values()
        .filter(|edge| edge.run_id == run_id)
    {
        if !tasks_by_key.contains_key(&edge.upstream_task_key)
            || !tasks_by_key.contains_key(&edge.downstream_task_key)
        {
            continue;
        }

        upstream_by_task
            .entry(edge.downstream_task_key.clone())
            .or_default()
            .push(edge.upstream_task_key.clone());
        downstream_by_task
            .entry(edge.upstream_task_key.clone())
            .or_default()
            .push(edge.downstream_task_key.clone());
    }

    for value in upstream_by_task.values_mut() {
        value.sort();
        value.dedup();
    }
    for value in downstream_by_task.values_mut() {
        value.sort();
        value.dedup();
    }

    (tasks_by_key, upstream_by_task, downstream_by_task)
}

fn close_task_selection(
    roots: &[String],
    include_upstream: bool,
    include_downstream: bool,
    upstream_by_task: &HashMap<String, Vec<String>>,
    downstream_by_task: &HashMap<String, Vec<String>>,
) -> BTreeSet<String> {
    let mut selected: BTreeSet<String> = roots.iter().cloned().collect();

    if include_upstream {
        let mut queue: VecDeque<String> = roots.iter().cloned().collect();
        while let Some(current) = queue.pop_front() {
            let upstream = upstream_by_task.get(&current).cloned().unwrap_or_default();
            for upstream_key in upstream {
                if selected.insert(upstream_key.clone()) {
                    queue.push_back(upstream_key);
                }
            }
        }
    }

    if include_downstream {
        let mut queue: VecDeque<String> = roots.iter().cloned().collect();
        while let Some(current) = queue.pop_front() {
            let downstream = downstream_by_task
                .get(&current)
                .cloned()
                .unwrap_or_default();
            for downstream_key in downstream {
                if selected.insert(downstream_key.clone()) {
                    queue.push_back(downstream_key);
                }
            }
        }
    }

    selected
}

fn task_defs_for_rerun(
    tasks_by_key: &HashMap<String, TaskRow>,
    selected: &BTreeSet<String>,
    upstream_by_task: &HashMap<String, Vec<String>>,
) -> Vec<TaskDef> {
    let mut tasks = Vec::new();

    for task_key in selected {
        let Some(task_row) = tasks_by_key.get(task_key) else {
            continue;
        };

        let mut depends_on: Vec<String> = upstream_by_task
            .get(task_key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|upstream| selected.contains(upstream))
            .collect();
        depends_on.sort();

        tasks.push(TaskDef {
            key: task_key.clone(),
            depends_on,
            asset_key: task_row.asset_key.clone(),
            partition_key: task_row.partition_key.clone(),
            max_attempts: task_row.max_attempts,
            heartbeat_timeout_sec: task_row.heartbeat_timeout_sec,
        });
    }

    tasks.sort_by(|a, b| a.key.cmp(&b.key));
    tasks
}

fn compute_rerun_run_key(
    parent_run_id: &str,
    rerun_kind: RerunKindResponse,
    selected_tasks: &BTreeSet<String>,
    partition_keys: &BTreeSet<String>,
) -> Result<String, ApiError> {
    #[derive(Serialize)]
    struct Payload {
        parent_run_id: String,
        rerun_kind: RerunKindResponse,
        selected_tasks: Vec<String>,
        partition_keys: Vec<String>,
    }

    let payload = Payload {
        parent_run_id: parent_run_id.to_string(),
        rerun_kind,
        selected_tasks: selected_tasks.iter().cloned().collect(),
        partition_keys: partition_keys.iter().cloned().collect(),
    };

    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!("failed to serialize rerun run_key payload: {e}"))
    })?;
    let hash = Sha256::digest(&json);
    let hex_hash = hex::encode(hash);

    let kind = match rerun_kind {
        RerunKindResponse::FromFailure => "from_failure",
        RerunKindResponse::Subset => "subset",
    };

    Ok(format!("rerun:{parent_run_id}:{kind}:{}", &hex_hash[..16]))
}

#[allow(clippy::too_many_arguments)]
fn compute_rerun_request_fingerprint(
    parent_run_id: &str,
    rerun_kind: RerunKindResponse,
    roots: &[String],
    include_upstream: bool,
    include_downstream: bool,
    selected_tasks: &BTreeSet<String>,
    partition_keys: &BTreeSet<String>,
    labels: &HashMap<String, String>,
) -> Result<String, ApiError> {
    #[derive(Serialize)]
    struct Payload {
        parent_run_id: String,
        rerun_kind: RerunKindResponse,
        roots: Vec<String>,
        include_upstream: bool,
        include_downstream: bool,
        selected_tasks: Vec<String>,
        partition_keys: Vec<String>,
        labels: BTreeMap<String, String>,
    }

    let labels = labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    let payload = Payload {
        parent_run_id: parent_run_id.to_string(),
        rerun_kind,
        roots: roots.to_vec(),
        include_upstream,
        include_downstream,
        selected_tasks: selected_tasks.iter().cloned().collect(),
        partition_keys: partition_keys.iter().cloned().collect(),
        labels,
    };

    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!(
            "failed to serialize rerun request fingerprint: {e}"
        ))
    })?;
    let hash = Sha256::digest(&json);
    Ok(hex::encode(hash))
}

fn parse_pagination(limit: u32, cursor: Option<&str>) -> Result<(usize, usize), ApiError> {
    if limit == 0 || limit > MAX_LIST_LIMIT {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_LIST_LIMIT}"
        )));
    }

    let offset = cursor
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| ApiError::bad_request("invalid cursor"))
        })
        .transpose()?
        .unwrap_or(0);

    Ok((limit as usize, offset))
}

#[derive(Debug, Serialize, Deserialize)]
struct BackfillIdempotencyRecord {
    idempotency_key: String,
    backfill_id: String,
    accepted_event_id: String,
    accepted_at: DateTime<Utc>,
    fingerprint: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BackfillFingerprintPayload {
    asset_selection: Vec<String>,
    partitions: Vec<String>,
    chunk_size: u32,
    max_concurrent_runs: u32,
}

struct BackfillCreateInput {
    asset_selection: Vec<String>,
    partition_selector: PartitionSelector,
    partition_keys: Vec<String>,
    chunk_size: u32,
    max_concurrent_runs: u32,
}

fn compute_backfill_fingerprint(
    asset_selection: &[String],
    partitions: &[String],
    chunk_size: u32,
    max_concurrent_runs: u32,
) -> Result<String, ApiError> {
    let payload = BackfillFingerprintPayload {
        asset_selection: asset_selection.to_vec(),
        partitions: partitions.to_vec(),
        chunk_size,
        max_concurrent_runs,
    };
    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!("failed to serialize backfill fingerprint: {e}"))
    })?;
    let hash = Sha256::digest(&json);
    Ok(hex::encode(hash))
}

async fn load_backfill_idempotency_record(
    storage: &ScopedStorage,
    idempotency_key: &str,
) -> Result<Option<BackfillIdempotencyRecord>, ApiError> {
    let path = backfill_idempotency_path(idempotency_key);
    match storage.get_raw(&path).await {
        Ok(bytes) => {
            let record: BackfillIdempotencyRecord =
                serde_json::from_slice(&bytes).map_err(|e| {
                    ApiError::internal(format!("failed to parse backfill idempotency record: {e}"))
                })?;
            Ok(Some(record))
        }
        Err(CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }) => Ok(None),
        Err(err) => Err(ApiError::internal(format!(
            "failed to read backfill idempotency record: {err}"
        ))),
    }
}

async fn store_backfill_idempotency_record(
    storage: &ScopedStorage,
    record: &BackfillIdempotencyRecord,
) -> Result<(), ApiError> {
    let record_json = serde_json::to_string(record).map_err(|e| {
        ApiError::internal(format!(
            "failed to serialize backfill idempotency record: {e}"
        ))
    })?;
    let record_path = backfill_idempotency_path(&record.idempotency_key);
    let result = storage
        .put_raw(
            &record_path,
            Bytes::from(record_json),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|e| {
            ApiError::internal(format!("failed to store backfill idempotency record: {e}"))
        })?;

    if matches!(result, WriteResult::PreconditionFailed { .. }) {
        tracing::warn!(
            idempotency_key = %record.idempotency_key,
            backfill_id = %record.backfill_id,
            "backfill idempotency record already exists after write"
        );
    }

    Ok(())
}

fn explicit_partition_selector(
    partitions: Vec<String>,
) -> Result<(PartitionSelector, Vec<String>), ApiError> {
    if partitions.is_empty() {
        return Err(ApiError::bad_request("partition selector cannot be empty"));
    }
    if partitions.iter().any(String::is_empty) {
        return Err(ApiError::bad_request("partition key cannot be empty"));
    }

    Ok((
        PartitionSelector::Explicit {
            partition_keys: partitions.clone(),
        },
        partitions,
    ))
}

fn unsupported_partition_selector_response() -> UnprocessableEntityResponse {
    UnprocessableEntityResponse {
        error: "unprocessable_entity".to_string(),
        code: "PARTITION_SELECTOR_NOT_SUPPORTED".to_string(),
        message: "Only explicit partition lists are supported. Range and filter selectors require a PartitionResolver (not yet implemented).".to_string(),
    }
}

async fn resolve_backfill_idempotency(
    storage: &ScopedStorage,
    idempotency_key: &str,
    fingerprint: &str,
) -> Result<Option<CreateBackfillResponse>, ApiError> {
    if let Some(existing) = load_backfill_idempotency_record(storage, idempotency_key).await? {
        if existing.fingerprint == fingerprint {
            return Ok(Some(CreateBackfillResponse {
                backfill_id: existing.backfill_id,
                accepted_event_id: existing.accepted_event_id,
                accepted_at: existing.accepted_at,
            }));
        }

        return Err(ApiError::conflict(
            "idempotency key already used for a different backfill",
        ));
    }

    Ok(None)
}

async fn append_backfill_created_event(
    state: &AppState,
    ctx: &RequestContext,
    workspace_id: &str,
    input: BackfillCreateInput,
    idempotency_key: &str,
    storage: &ScopedStorage,
    fingerprint: String,
) -> Result<CreateBackfillResponse, ApiError> {
    let backfill_id = Ulid::new().to_string();
    let total_partitions = u32::try_from(input.partition_keys.len()).unwrap_or(u32::MAX);
    let event = OrchestrationEvent::new(
        &ctx.tenant,
        workspace_id,
        OrchestrationEventData::BackfillCreated {
            backfill_id: backfill_id.clone(),
            client_request_id: idempotency_key.to_string(),
            asset_selection: input.asset_selection,
            partition_selector: input.partition_selector,
            total_partitions,
            chunk_size: input.chunk_size,
            max_concurrent_runs: input.max_concurrent_runs,
            parent_backfill_id: None,
        },
    );

    let accepted_event_id = event.event_id.clone();
    let accepted_at = event.timestamp;

    let ledger = LedgerWriter::new(storage.clone());
    let event_path = LedgerWriter::event_path(&event);
    ledger
        .append(event)
        .await
        .map_err(|e| ApiError::internal(format!("failed to write BackfillCreated event: {e}")))?;

    compact_orchestration_events(&state.config, storage.clone(), vec![event_path]).await?;

    let record = BackfillIdempotencyRecord {
        idempotency_key: idempotency_key.to_string(),
        backfill_id: backfill_id.clone(),
        accepted_event_id: accepted_event_id.clone(),
        accepted_at,
        fingerprint,
    };
    store_backfill_idempotency_record(storage, &record).await?;

    Ok(CreateBackfillResponse {
        backfill_id,
        accepted_event_id,
        accepted_at,
    })
}

fn user_id_for_events(ctx: &RequestContext) -> String {
    ctx.user_id.clone().unwrap_or_else(|| "api".to_string())
}

async fn load_orchestration_state(
    ctx: &RequestContext,
    state: &AppState,
) -> Result<FoldState, ApiError> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = MicroCompactor::new(storage);
    let (_, fold_state) = compactor
        .load_state()
        .await
        .map_err(|e| ApiError::internal(format!("failed to load orchestration state: {e}")))?;
    Ok(fold_state)
}

struct RunEventOverrides {
    run_event_id: String,
    plan_event_id: String,
    created_at: DateTime<Utc>,
}

#[allow(clippy::too_many_arguments)]
async fn append_run_events(
    ledger: &LedgerWriter,
    tenant_id: &str,
    workspace_id: &str,
    user_id: String,
    run_id: &str,
    plan_id: &str,
    run_key: Option<String>,
    labels: HashMap<String, String>,
    code_version: Option<String>,
    root_assets: Vec<String>,
    tasks: Vec<TaskDef>,
    overrides: Option<RunEventOverrides>,
) -> Result<Vec<String>, ApiError> {
    let mut run_triggered = OrchestrationEvent::new(
        tenant_id,
        workspace_id,
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            trigger: TriggerInfo::Manual { user_id },
            root_assets,
            run_key,
            labels,
            code_version,
        },
    );

    if let Some(ref overrides) = overrides {
        apply_event_metadata(
            &mut run_triggered,
            &overrides.run_event_id,
            overrides.created_at,
        );
    }

    let run_event_path = LedgerWriter::event_path(&run_triggered);
    ledger
        .append(run_triggered)
        .await
        .map_err(|e| ApiError::internal(format!("failed to write RunTriggered event: {e}")))?;

    let mut plan_created = OrchestrationEvent::new(
        tenant_id,
        workspace_id,
        OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            tasks,
        },
    );

    if let Some(ref overrides) = overrides {
        apply_event_metadata(
            &mut plan_created,
            &overrides.plan_event_id,
            overrides.created_at,
        );
    }

    let plan_event_path = LedgerWriter::event_path(&plan_created);
    ledger
        .append(plan_created)
        .await
        .map_err(|e| ApiError::internal(format!("failed to write PlanCreated event: {e}")))?;

    Ok(vec![run_event_path, plan_event_path])
}

fn map_run_state(state: FoldRunState) -> RunStateResponse {
    match state {
        FoldRunState::Triggered => RunStateResponse::Pending,
        FoldRunState::Running => RunStateResponse::Running,
        FoldRunState::Succeeded => RunStateResponse::Succeeded,
        FoldRunState::Failed => RunStateResponse::Failed,
        FoldRunState::Cancelled => RunStateResponse::Cancelled,
    }
}

fn map_run_row_state(run: &RunRow) -> RunStateResponse {
    if run.cancel_requested && !run.state.is_terminal() {
        return RunStateResponse::Cancelling;
    }
    map_run_state(run.state)
}

fn map_task_state(state: FoldTaskState) -> TaskStateResponse {
    match state {
        FoldTaskState::Planned | FoldTaskState::Blocked | FoldTaskState::RetryWait => {
            TaskStateResponse::Pending
        }
        FoldTaskState::Ready | FoldTaskState::Dispatched => TaskStateResponse::Queued,
        FoldTaskState::Running => TaskStateResponse::Running,
        FoldTaskState::Succeeded => TaskStateResponse::Succeeded,
        FoldTaskState::Failed => TaskStateResponse::Failed,
        FoldTaskState::Skipped => TaskStateResponse::Skipped,
        FoldTaskState::Cancelled => TaskStateResponse::Cancelled,
    }
}

const LABEL_PARENT_RUN_ID: &str = "arco.parent_run_id";
const LABEL_RERUN_KIND: &str = "arco.rerun.kind";

fn rerun_kind_from_label(value: &str) -> Option<RerunKindResponse> {
    match value {
        "FROM_FAILURE" => Some(RerunKindResponse::FromFailure),
        "SUBSET" => Some(RerunKindResponse::Subset),
        _ => None,
    }
}

fn lineage_from_labels(
    labels: &HashMap<String, String>,
) -> (Option<String>, Option<RerunKindResponse>) {
    let parent = labels.get(LABEL_PARENT_RUN_ID).cloned();
    let kind = labels
        .get(LABEL_RERUN_KIND)
        .and_then(|value| rerun_kind_from_label(value));
    (parent, kind)
}

fn reject_reserved_lineage_labels(labels: &HashMap<String, String>) -> Result<(), ApiError> {
    let forbidden = [LABEL_PARENT_RUN_ID, LABEL_RERUN_KIND]
        .into_iter()
        .filter(|key| labels.contains_key(*key))
        .collect::<Vec<_>>();

    if forbidden.is_empty() {
        return Ok(());
    }

    Err(ApiError::bad_request(format!(
        "labels cannot include reserved keys: {}",
        forbidden.join(", ")
    )))
}

fn build_task_counts(run: &RunRow, tasks: &[&TaskRow]) -> TaskCounts {
    let mut pending = 0;
    let mut queued = 0;
    let mut running = 0;

    for task in tasks {
        match task.state {
            FoldTaskState::Planned | FoldTaskState::Blocked | FoldTaskState::RetryWait => {
                pending += 1;
            }
            FoldTaskState::Ready | FoldTaskState::Dispatched => queued += 1,
            FoldTaskState::Running => running += 1,
            _ => {}
        }
    }

    TaskCounts {
        total: run.tasks_total,
        pending,
        queued,
        running,
        succeeded: run.tasks_succeeded,
        failed: run.tasks_failed,
        skipped: run.tasks_skipped,
        cancelled: run.tasks_cancelled,
    }
}

fn build_partition_key(partitions: &[PartitionValue]) -> Result<Option<String>, ApiError> {
    if partitions.is_empty() {
        return Ok(None);
    }

    let mut seen = HashSet::new();
    let mut partition_key = arco_core::partition::PartitionKey::new();

    for partition in partitions {
        let key = partition.key.trim();
        if key.is_empty() {
            return Err(ApiError::bad_request("partition key cannot be empty"));
        }
        if !is_valid_partition_key(key) {
            return Err(ApiError::bad_request(format!(
                "invalid partition key: {key}"
            )));
        }
        if !seen.insert(key.to_string()) {
            return Err(ApiError::bad_request(format!(
                "duplicate partition key: {key}"
            )));
        }

        partition_key.insert(
            key.to_string(),
            arco_core::partition::ScalarValue::String(partition.value.clone()),
        );
    }

    Ok(Some(partition_key.canonical_string()))
}

fn is_valid_partition_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_valid_task_key(task_key: &str) -> bool {
    if task_key.is_empty() {
        return false;
    }
    task_key
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.')
}

fn build_request_fingerprint(request: &TriggerRunRequest) -> Result<String, ApiError> {
    #[derive(Serialize)]
    struct FingerprintPayload {
        selection: Vec<String>,
        include_upstream: bool,
        include_downstream: bool,
        partitions: Vec<PartitionValue>,
        labels: BTreeMap<String, String>,
    }

    let mut selection: Vec<String> = request
        .selection
        .iter()
        .map(|value| {
            arco_flow::orchestration::canonicalize_asset_key(value).map_err(ApiError::bad_request)
        })
        .collect::<Result<_, _>>()?;
    selection.sort();
    selection.dedup();

    let mut partitions = request.partitions.clone();
    partitions.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

    let labels = request
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    let payload = FingerprintPayload {
        selection,
        include_upstream: request.include_upstream,
        include_downstream: request.include_downstream,
        partitions,
        labels,
    };
    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!(
            "failed to serialize trigger request fingerprint: {e}"
        ))
    })?;
    let hash = Sha256::digest(&json);
    Ok(hex::encode(hash))
}

fn apply_event_metadata(event: &mut OrchestrationEvent, event_id: &str, timestamp: DateTime<Utc>) {
    event.event_id = event_id.to_string();
    event.timestamp = timestamp;
}

fn map_sensor_eval_status(status: &SensorEvalStatus) -> SensorEvalStatusResponse {
    match status {
        SensorEvalStatus::Triggered => SensorEvalStatusResponse::Triggered,
        SensorEvalStatus::NoNewData => SensorEvalStatusResponse::NoNewData,
        SensorEvalStatus::Error { message } => SensorEvalStatusResponse::Error {
            message: message.clone(),
        },
        SensorEvalStatus::SkippedStaleCursor => SensorEvalStatusResponse::SkippedStaleCursor,
    }
}

fn map_run_requests(run_requests: &[RunRequest]) -> Vec<RunRequestResponse> {
    run_requests
        .iter()
        .map(|req| RunRequestResponse {
            run_key: req.run_key.clone(),
            request_fingerprint: req.request_fingerprint.clone(),
            asset_selection: req.asset_selection.clone(),
            partition_selection: req.partition_selection.clone(),
        })
        .collect()
}

// ============================================================================
// Schedule/Sensor/Backfill/Partition Mapping Helpers
// ============================================================================

fn map_schedule(def: &ScheduleDefinitionRow, state: Option<&ScheduleStateRow>) -> ScheduleResponse {
    ScheduleResponse {
        schedule_id: def.schedule_id.clone(),
        cron_expression: def.cron_expression.clone(),
        timezone: def.timezone.clone(),
        catchup_window_minutes: def.catchup_window_minutes,
        max_catchup_ticks: def.max_catchup_ticks,
        asset_selection: def.asset_selection.clone(),
        enabled: def.enabled,
        definition_version: def.row_version.clone(),
        last_scheduled_for: state.and_then(|row| row.last_scheduled_for),
        last_tick_id: state.and_then(|row| row.last_tick_id.clone()),
        last_run_key: state.and_then(|row| row.last_run_key.clone()),
    }
}

fn normalize_asset_selection(values: &[String]) -> Vec<String> {
    let mut values = values.to_vec();
    values.sort();
    values
}

fn schedule_definition_matches_request(
    def: &ScheduleDefinitionRow,
    request: &UpsertScheduleRequest,
) -> bool {
    def.cron_expression == request.cron_expression
        && def.timezone == request.timezone
        && def.catchup_window_minutes == request.catchup_window_minutes
        && def.max_catchup_ticks == request.max_catchup_ticks
        && def.enabled == request.enabled
        && normalize_asset_selection(&def.asset_selection)
            == normalize_asset_selection(&request.asset_selection)
}

fn validate_schedule_upsert(
    schedule_id: &str,
    request: &UpsertScheduleRequest,
) -> Result<(), ApiError> {
    if schedule_id.is_empty() {
        return Err(ApiError::bad_request("schedule_id cannot be empty"));
    }

    if request.asset_selection.is_empty() {
        return Err(ApiError::bad_request("assetSelection cannot be empty"));
    }

    for asset in &request.asset_selection {
        if asset.is_empty() {
            return Err(ApiError::bad_request(
                "assetSelection items cannot be empty",
            ));
        }
    }

    if request.max_catchup_ticks == 0 {
        return Err(ApiError::bad_request("maxCatchupTicks must be >= 1"));
    }

    let field_count = request.cron_expression.split_whitespace().count();
    if field_count != 5 && field_count != 6 {
        return Err(ApiError::bad_request(
            "cronExpression must have 5 fields (min..dow) or 6 fields (sec..dow)",
        ));
    }

    let normalized = if field_count == 5 {
        format!("0 {}", request.cron_expression)
    } else {
        request.cron_expression.clone()
    };

    if cron::Schedule::from_str(&normalized).is_err() {
        return Err(ApiError::bad_request("invalid cronExpression"));
    }

    if request.timezone.parse::<chrono_tz::Tz>().is_err() {
        return Err(ApiError::bad_request("invalid timezone"));
    }

    Ok(())
}

fn map_tick_status(status: &TickStatus) -> TickStatusResponse {
    match status {
        TickStatus::Triggered => TickStatusResponse::Triggered,
        TickStatus::Skipped { .. } => TickStatusResponse::Skipped,
        TickStatus::Failed { .. } => TickStatusResponse::Failed,
    }
}

fn map_schedule_tick(row: &ScheduleTickRow) -> ScheduleTickResponse {
    let (skip_reason, error_message) = match &row.status {
        TickStatus::Skipped { reason } => (Some(reason.clone()), None),
        TickStatus::Failed { error } => (None, Some(error.clone())),
        TickStatus::Triggered => (None, None),
    };

    ScheduleTickResponse {
        tick_id: row.tick_id.clone(),
        schedule_id: row.schedule_id.clone(),
        scheduled_for: row.scheduled_for,
        asset_selection: row.asset_selection.clone(),
        status: map_tick_status(&row.status),
        skip_reason,
        error_message,
        run_key: row.run_key.clone(),
        run_id: row.run_id.clone(),
    }
}

fn map_sensor_status(status: SensorStatus) -> SensorStatusResponse {
    match status {
        SensorStatus::Active => SensorStatusResponse::Active,
        SensorStatus::Paused => SensorStatusResponse::Paused,
        SensorStatus::Error => SensorStatusResponse::Error,
    }
}

fn map_sensor_state(row: &SensorStateRow) -> SensorResponse {
    SensorResponse {
        sensor_id: row.sensor_id.clone(),
        cursor: row.cursor.clone(),
        last_evaluation_at: row.last_evaluation_at,
        status: map_sensor_status(row.status),
        state_version: row.state_version,
    }
}

fn map_sensor_eval(row: &SensorEvalRow) -> SensorEvalResponse {
    SensorEvalResponse {
        eval_id: row.eval_id.clone(),
        sensor_id: row.sensor_id.clone(),
        cursor_before: row.cursor_before.clone(),
        cursor_after: row.cursor_after.clone(),
        evaluated_at: row.evaluated_at,
        status: map_sensor_eval_status(&row.status),
        run_requests_count: u32::try_from(row.run_requests.len()).unwrap_or(u32::MAX),
    }
}

fn map_backfill_state(state: BackfillState) -> BackfillStateResponse {
    match state {
        BackfillState::Pending => BackfillStateResponse::Pending,
        BackfillState::Running => BackfillStateResponse::Running,
        BackfillState::Paused => BackfillStateResponse::Paused,
        BackfillState::Succeeded => BackfillStateResponse::Succeeded,
        BackfillState::Failed => BackfillStateResponse::Failed,
        BackfillState::Cancelled => BackfillStateResponse::Cancelled,
    }
}

fn map_partition_selector(selector: &PartitionSelector) -> PartitionSelectorResponse {
    match selector {
        PartitionSelector::Range { start, end } => PartitionSelectorResponse::Range {
            start: start.clone(),
            end: end.clone(),
        },
        PartitionSelector::Explicit { partition_keys } => PartitionSelectorResponse::Explicit {
            partitions: partition_keys.clone(),
        },
        PartitionSelector::Filter { filters } => PartitionSelectorResponse::Filter {
            filters: filters.clone(),
        },
    }
}

fn build_chunk_counts(chunks: &[&BackfillChunkRow]) -> ChunkCounts {
    let mut counts = ChunkCounts {
        total: u32::try_from(chunks.len()).unwrap_or(u32::MAX),
        ..Default::default()
    };

    for chunk in chunks {
        match chunk.state {
            ChunkState::Pending | ChunkState::Planned => counts.pending += 1,
            ChunkState::Running => counts.running += 1,
            ChunkState::Succeeded => counts.succeeded += 1,
            ChunkState::Failed => counts.failed += 1,
        }
    }

    counts
}

fn build_chunk_counts_index<'a, I>(chunks: I) -> HashMap<String, ChunkCounts>
where
    I: IntoIterator<Item = &'a BackfillChunkRow>,
{
    let mut counts = HashMap::new();

    for chunk in chunks {
        let entry = counts
            .entry(chunk.backfill_id.clone())
            .or_insert_with(ChunkCounts::default);

        entry.total = entry.total.saturating_add(1);

        match chunk.state {
            ChunkState::Pending | ChunkState::Planned => {
                entry.pending = entry.pending.saturating_add(1);
            }
            ChunkState::Running => {
                entry.running = entry.running.saturating_add(1);
            }
            ChunkState::Succeeded => {
                entry.succeeded = entry.succeeded.saturating_add(1);
            }
            ChunkState::Failed => {
                entry.failed = entry.failed.saturating_add(1);
            }
        }
    }

    counts
}

fn map_backfill_with_counts(row: &BackfillRow, counts: ChunkCounts) -> BackfillResponse {
    BackfillResponse {
        backfill_id: row.backfill_id.clone(),
        asset_selection: row.asset_selection.clone(),
        partition_selector: map_partition_selector(&row.partition_selector),
        chunk_size: row.chunk_size,
        max_concurrent_runs: row.max_concurrent_runs,
        state: map_backfill_state(row.state),
        state_version: row.state_version,
        created_at: row.created_at,
        chunk_counts: counts,
    }
}

fn map_backfill(row: &BackfillRow, chunks: &[&BackfillChunkRow]) -> BackfillResponse {
    map_backfill_with_counts(row, build_chunk_counts(chunks))
}

fn map_chunk_state(state: ChunkState) -> ChunkStateResponse {
    match state {
        ChunkState::Pending => ChunkStateResponse::Pending,
        ChunkState::Planned => ChunkStateResponse::Planned,
        ChunkState::Running => ChunkStateResponse::Running,
        ChunkState::Succeeded => ChunkStateResponse::Succeeded,
        ChunkState::Failed => ChunkStateResponse::Failed,
    }
}

fn map_backfill_chunk(row: &BackfillChunkRow) -> BackfillChunkResponse {
    BackfillChunkResponse {
        chunk_id: row.chunk_id.clone(),
        backfill_id: row.backfill_id.clone(),
        chunk_index: row.chunk_index,
        partition_keys: row.partition_keys.clone(),
        run_key: row.run_key.clone(),
        run_id: row.run_id.clone(),
        state: map_chunk_state(row.state),
    }
}

fn map_partition_status(row: &PartitionStatusRow) -> PartitionStatusApiResponse {
    PartitionStatusApiResponse {
        asset_key: row.asset_key.clone(),
        partition_key: row.partition_key.clone(),
        last_materialization_run_id: row.last_materialization_run_id.clone(),
        last_materialization_at: row.last_materialization_at,
        is_stale: row.stale_since.is_some(),
        stale_reason: row.stale_reason_code.clone(),
        partition_values: row.partition_values.clone(),
    }
}

fn paginate<T: Clone>(items: &[T], limit: usize, offset: usize) -> (Vec<T>, Option<String>) {
    let end = (offset + limit).min(items.len());
    let page = items.get(offset..end).unwrap_or_default().to_vec();
    let next_cursor = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next_cursor)
}

fn filter_ticks_by_status(
    ticks: &mut Vec<ScheduleTickResponse>,
    status: Option<TickStatusResponse>,
) {
    if let Some(filter) = status {
        ticks.retain(|tick| tick.status == filter);
    }
}

// ============================================================================
// Route Handlers
// ============================================================================

/// Trigger a new run.
///
/// Creates an execution plan and starts a new run for the selected assets.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/runs",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID")
    ),
    request_body = TriggerRunRequest,
    responses(
        (status = 201, description = "Run created", body = TriggerRunResponse),
        (status = 200, description = "Existing run returned (run_key match)", body = TriggerRunResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 409, description = "run_key conflict (payload mismatch)", body = ApiErrorBody),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn trigger_run(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    Json(request): Json<TriggerRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        selection = ?request.selection,
        run_key = ?request.run_key,
        "Triggering run"
    );

    ensure_workspace(&ctx, &workspace_id)?;
    let user_id = user_id_for_events(&ctx);

    // Validate selection
    if request.selection.is_empty() {
        return Err(ApiError::bad_request("selection cannot be empty"));
    }

    reject_reserved_lineage_labels(&request.labels)?;

    // Generate IDs upfront (needed for reservation)
    let run_id = Ulid::new().to_string();
    let plan_id = Ulid::new().to_string();
    let now = Utc::now();

    // Create storage for reservation and ledger
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let ledger = LedgerWriter::new(storage.clone());
    let mut run_event_overrides: Option<RunEventOverrides> = None;

    let request_fingerprint = if request.run_key.is_some() {
        Some(build_request_fingerprint(&request)?)
    } else {
        None
    };

    if let Some(ref run_key) = request.run_key {
        let fingerprint_policy =
            FingerprintPolicy::from_cutoff(state.config.run_key_fingerprint_cutoff);

        let existing = get_reservation(&storage, run_key)
            .await
            .map_err(|e| ApiError::internal(format!("failed to read run_key reservation: {e}")))?;

        if let Some(existing) = existing {
            let fingerprints_match = match (
                existing.request_fingerprint.as_deref(),
                request_fingerprint.as_deref(),
            ) {
                (Some(a), Some(b)) => a == b,
                (None, None) => true,
                (None, Some(_)) => match fingerprint_policy.cutoff {
                    None => true,
                    Some(cutoff) => existing.created_at < cutoff,
                },
                (Some(_), None) => false,
            };

            if !fingerprints_match {
                tracing::warn!(
                    run_key = %existing.run_key,
                    run_id = %existing.run_id,
                    existing_fingerprint = ?existing.request_fingerprint,
                    requested_fingerprint = ?request_fingerprint,
                    existing_created_at = %existing.created_at,
                    fingerprint_cutoff = ?fingerprint_policy.cutoff,
                    "run_key reused with different trigger payload (fingerprint mismatch)"
                );

                return Err(ApiError::conflict(format!(
                    "run_key '{}' already reserved with different trigger payload",
                    existing.run_key
                )));
            }

            let mut run_state = RunStateResponse::Pending;
            let mut run_found = false;

            match load_orchestration_state(&ctx, &state).await {
                Ok(fold_state) => {
                    if let Some(run) = fold_state.runs.get(&existing.run_id) {
                        run_state = map_run_row_state(run);
                        run_found = true;
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        run_key = %existing.run_key,
                        run_id = %existing.run_id,
                        error = ?err,
                        "failed to load orchestration state for run_key lookup"
                    );
                }
            }

            if !run_found {
                let (manifest_id, deployed_at, root_assets, tasks) =
                    plan_tasks_and_root_assets(&storage, &request).await?;

                let plan_event_id = existing.plan_event_id.clone().unwrap_or_else(|| {
                    let new_id = Ulid::new().to_string();
                    tracing::warn!(
                        run_key = %existing.run_key,
                        run_id = %existing.run_id,
                        "missing plan_event_id in reservation; generating new plan event id"
                    );
                    new_id
                });

                tracing::info!(
                    manifest_id = %manifest_id,
                    manifest_deployed_at = %deployed_at,
                    run_id = %existing.run_id,
                    "re-emitting run plan from latest manifest"
                );

                let event_paths = append_run_events(
                    &ledger,
                    &ctx.tenant,
                    &workspace_id,
                    user_id.clone(),
                    &existing.run_id,
                    &existing.plan_id,
                    Some(existing.run_key.clone()),
                    request.labels.clone(),
                    state.config.code_version.clone(),
                    root_assets,
                    tasks,
                    Some(RunEventOverrides {
                        run_event_id: existing.event_id.clone(),
                        plan_event_id,
                        created_at: existing.created_at,
                    }),
                )
                .await?;

                compact_orchestration_events(&state.config, storage.clone(), event_paths).await?;
            }

            return Ok((
                StatusCode::OK,
                Json(TriggerRunResponse {
                    run_id: existing.run_id,
                    plan_id: existing.plan_id,
                    state: run_state,
                    created: false,
                    created_at: existing.created_at,
                }),
            ));
        }
    }

    let (manifest_id, deployed_at, root_assets, tasks) =
        plan_tasks_and_root_assets(&storage, &request).await?;

    // If run_key provided, attempt to reserve it (strong idempotency)
    if let Some(ref run_key) = request.run_key {
        let run_event_id = Ulid::new().to_string();
        let plan_event_id = Ulid::new().to_string();
        let reservation = RunKeyReservation {
            run_key: run_key.clone(),
            run_id: run_id.clone(),
            plan_id: plan_id.clone(),
            event_id: run_event_id.clone(),
            plan_event_id: Some(plan_event_id.clone()),
            request_fingerprint: request_fingerprint.clone(),
            created_at: now,
        };

        let fingerprint_policy =
            FingerprintPolicy::from_cutoff(state.config.run_key_fingerprint_cutoff);
        match reserve_run_key(&storage, &reservation, fingerprint_policy)
            .await
            .map_err(|e| ApiError::internal(format!("failed to reserve run_key: {e}")))?
        {
            ReservationResult::Reserved => {
                // We won the race - proceed to emit events
                tracing::debug!(run_key = %run_key, "run_key reserved, proceeding with run creation");
                run_event_overrides = Some(RunEventOverrides {
                    run_event_id,
                    plan_event_id,
                    created_at: now,
                });
            }
            ReservationResult::AlreadyExists(existing) => {
                if let (Some(existing_fp), Some(current_fp)) = (
                    existing.request_fingerprint.as_deref(),
                    request_fingerprint.as_deref(),
                ) {
                    if existing_fp != current_fp {
                        tracing::warn!(
                            run_key = %existing.run_key,
                            run_id = %existing.run_id,
                            "run_key reused with different trigger payload"
                        );
                        return Err(ApiError::conflict(format!(
                            "run_key already reserved with different trigger payload: {}",
                            existing.run_key
                        )));
                    }
                } else if existing.request_fingerprint.is_none() && request_fingerprint.is_some() {
                    tracing::warn!(
                        run_key = %existing.run_key,
                        run_id = %existing.run_id,
                        "run_key reservation missing request_fingerprint; skipping payload validation"
                    );
                }

                // Return existing run - check FoldState for current state if available
                let mut run_state = RunStateResponse::Pending;
                let mut run_found = false;

                match load_orchestration_state(&ctx, &state).await {
                    Ok(fold_state) => {
                        if let Some(run) = fold_state.runs.get(&existing.run_id) {
                            run_state = map_run_row_state(run);
                            run_found = true;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            run_key = %existing.run_key,
                            run_id = %existing.run_id,
                            error = ?err,
                            "failed to load orchestration state for run_key lookup"
                        );
                    }
                }

                if !run_found {
                    let plan_event_id = existing.plan_event_id.clone().unwrap_or_else(|| {
                        let new_id = Ulid::new().to_string();
                        tracing::warn!(
                            run_key = %existing.run_key,
                            run_id = %existing.run_id,
                            "missing plan_event_id in reservation; generating new plan event id"
                        );
                        new_id
                    });

                    tracing::info!(
                        manifest_id = %manifest_id,
                        manifest_deployed_at = %deployed_at,
                        run_id = %existing.run_id,
                        "re-emitting run plan from latest manifest"
                    );

                    let event_paths = append_run_events(
                        &ledger,
                        &ctx.tenant,
                        &workspace_id,
                        user_id.clone(),
                        &existing.run_id,
                        &existing.plan_id,
                        Some(existing.run_key.clone()),
                        request.labels.clone(),
                        state.config.code_version.clone(),
                        root_assets.clone(),
                        tasks.clone(),
                        Some(RunEventOverrides {
                            run_event_id: existing.event_id.clone(),
                            plan_event_id,
                            created_at: existing.created_at,
                        }),
                    )
                    .await?;

                    compact_orchestration_events(&state.config, storage.clone(), event_paths)
                        .await?;
                }

                return Ok((
                    StatusCode::OK,
                    Json(TriggerRunResponse {
                        run_id: existing.run_id,
                        plan_id: existing.plan_id,
                        state: run_state,
                        created: false,
                        created_at: existing.created_at,
                    }),
                ));
            }
            ReservationResult::FingerprintMismatch {
                existing,
                requested_fingerprint,
            } => {
                // The run_key was reserved with a different trigger payload.
                // This is a client error - return 409 Conflict.
                tracing::warn!(
                    run_key = %existing.run_key,
                    run_id = %existing.run_id,
                    existing_fingerprint = ?existing.request_fingerprint,
                    requested_fingerprint = ?requested_fingerprint,
                    "run_key reused with different trigger payload (fingerprint mismatch)"
                );
                return Err(ApiError::conflict(format!(
                    "run_key '{}' already reserved with different trigger payload",
                    existing.run_key
                )));
            }
        }
    }

    tracing::info!(
        manifest_id = %manifest_id,
        manifest_deployed_at = %deployed_at,
        root_assets = ?root_assets,
        include_upstream = request.include_upstream,
        include_downstream = request.include_downstream,
        planned_tasks = tasks.len(),
        "planning run from latest manifest"
    );

    let event_paths = append_run_events(
        &ledger,
        &ctx.tenant,
        &workspace_id,
        user_id,
        &run_id,
        &plan_id,
        request.run_key.clone(),
        request.labels.clone(),
        state.config.code_version.clone(),
        root_assets,
        tasks,
        run_event_overrides,
    )
    .await?;

    compact_orchestration_events(&state.config, storage.clone(), event_paths).await?;

    Ok((
        StatusCode::CREATED,
        Json(TriggerRunResponse {
            run_id,
            plan_id,
            state: RunStateResponse::Pending,
            created: true,
            created_at: now,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/runs/{run_id}/rerun",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("run_id" = String, Path, description = "Run ID")
    ),
    request_body = RerunRunRequest,
    responses(
        (status = 201, description = "Rerun created", body = RerunRunResponse),
        (status = 200, description = "Existing rerun returned (run_key match)", body = RerunRunResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 409, description = "Conflict", body = ApiErrorBody),
        (status = 404, description = "Run not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn rerun_run(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, parent_run_id)): Path<(String, String)>,
    Json(request): Json<RerunRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    reject_reserved_lineage_labels(&request.labels)?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let parent_run = fold_state
        .runs
        .get(&parent_run_id)
        .ok_or_else(|| ApiError::not_found(format!("run not found: {parent_run_id}")))?;

    let rerun_kind = match request.mode {
        RerunMode::FromFailure => RerunKindResponse::FromFailure,
        RerunMode::Subset => RerunKindResponse::Subset,
    };

    if rerun_kind == RerunKindResponse::FromFailure && parent_run.state != FoldRunState::Failed {
        return Err(ApiError::conflict(
            "rerun-from-failure requires a FAILED parent run".to_string(),
        ));
    }

    let (tasks_by_key, upstream_by_task, downstream_by_task) =
        build_plan_graph_for_run(&fold_state, &parent_run_id);

    if tasks_by_key.is_empty() {
        return Err(ApiError::conflict(
            "parent run has no plan; cannot rerun".to_string(),
        ));
    }

    let (roots, selected, include_upstream, include_downstream) = match rerun_kind {
        RerunKindResponse::FromFailure => {
            let mut selected = BTreeSet::new();
            for (task_key, task_row) in &tasks_by_key {
                if task_row.state != FoldTaskState::Succeeded {
                    selected.insert(task_key.clone());
                }
            }

            let roots: Vec<String> = selected.iter().cloned().collect();
            (roots, selected, false, false)
        }
        RerunKindResponse::Subset => {
            if request.selection.is_empty() {
                return Err(ApiError::bad_request("selection cannot be empty"));
            }

            let mut roots = BTreeSet::new();
            let mut unknown = BTreeSet::new();
            for value in &request.selection {
                let canonical = arco_flow::orchestration::canonicalize_asset_key(value)
                    .map_err(ApiError::bad_request)?;

                if tasks_by_key.contains_key(&canonical) {
                    roots.insert(canonical);
                } else {
                    unknown.insert(canonical);
                }
            }

            if !unknown.is_empty() {
                return Err(ApiError::bad_request(format!(
                    "unknown task keys: {}",
                    unknown.into_iter().collect::<Vec<_>>().join(", ")
                )));
            }

            let roots_vec: Vec<String> = roots.iter().cloned().collect();
            let selected = close_task_selection(
                &roots_vec,
                request.include_upstream,
                request.include_downstream,
                &upstream_by_task,
                &downstream_by_task,
            );

            (
                roots_vec,
                selected,
                request.include_upstream,
                request.include_downstream,
            )
        }
    };

    if selected.is_empty() {
        return Err(ApiError::bad_request("rerun selection is empty"));
    }

    let mut partition_keys = BTreeSet::new();
    for task_key in &selected {
        let Some(row) = tasks_by_key.get(task_key) else {
            continue;
        };
        if let Some(partition_key) = row.partition_key.as_ref() {
            partition_keys.insert(partition_key.clone());
        }
    }

    let root_assets = roots.clone();
    let tasks = task_defs_for_rerun(&tasks_by_key, &selected, &upstream_by_task);

    let mut labels = request.labels.clone();
    labels.insert(LABEL_PARENT_RUN_ID.to_string(), parent_run_id.clone());
    labels.insert(
        LABEL_RERUN_KIND.to_string(),
        match rerun_kind {
            RerunKindResponse::FromFailure => "FROM_FAILURE".to_string(),
            RerunKindResponse::Subset => "SUBSET".to_string(),
        },
    );

    let run_key = match request.run_key.clone() {
        Some(value) => value,
        None => compute_rerun_run_key(&parent_run_id, rerun_kind, &selected, &partition_keys)?,
    };

    let request_fingerprint = Some(compute_rerun_request_fingerprint(
        &parent_run_id,
        rerun_kind,
        &root_assets,
        include_upstream,
        include_downstream,
        &selected,
        &partition_keys,
        &labels,
    )?);

    let now = Utc::now();
    let run_id = Ulid::new().to_string();
    let plan_id = Ulid::new().to_string();

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let ledger = LedgerWriter::new(storage.clone());

    let fingerprint_policy =
        FingerprintPolicy::from_cutoff(state.config.run_key_fingerprint_cutoff);

    let run_event_id = Ulid::new().to_string();
    let plan_event_id = Ulid::new().to_string();

    let reservation = RunKeyReservation {
        run_key: run_key.clone(),
        run_id: run_id.clone(),
        plan_id: plan_id.clone(),
        event_id: run_event_id.clone(),
        plan_event_id: Some(plan_event_id.clone()),
        request_fingerprint: request_fingerprint.clone(),
        created_at: now,
    };

    let run_event_overrides = match reserve_run_key(&storage, &reservation, fingerprint_policy)
        .await
        .map_err(|e| ApiError::internal(format!("failed to reserve run_key: {e}")))?
    {
        ReservationResult::Reserved => Some(RunEventOverrides {
            run_event_id,
            plan_event_id,
            created_at: now,
        }),
        ReservationResult::AlreadyExists(existing) => {
            let mut run_state = RunStateResponse::Pending;
            let mut run_found = false;

            if let Ok(fold_state) = load_orchestration_state(&ctx, &state).await {
                if let Some(run) = fold_state.runs.get(&existing.run_id) {
                    run_state = map_run_row_state(run);
                    run_found = true;
                }
            }

            if !run_found {
                let user_id = user_id_for_events(&ctx);
                let plan_event_id = existing
                    .plan_event_id
                    .clone()
                    .unwrap_or_else(|| Ulid::new().to_string());

                let event_paths = append_run_events(
                    &ledger,
                    &ctx.tenant,
                    &workspace_id,
                    user_id,
                    &existing.run_id,
                    &existing.plan_id,
                    Some(existing.run_key.clone()),
                    labels.clone(),
                    parent_run.code_version.clone(),
                    root_assets.clone(),
                    tasks.clone(),
                    Some(RunEventOverrides {
                        run_event_id: existing.event_id.clone(),
                        plan_event_id,
                        created_at: existing.created_at,
                    }),
                )
                .await?;

                compact_orchestration_events(&state.config, storage.clone(), event_paths).await?;
            }

            return Ok((
                StatusCode::OK,
                Json(RerunRunResponse {
                    run_id: existing.run_id,
                    plan_id: existing.plan_id,
                    state: run_state,
                    created: false,
                    created_at: existing.created_at,
                    parent_run_id,
                    rerun_kind,
                }),
            ));
        }
        ReservationResult::FingerprintMismatch { existing, .. } => {
            return Err(ApiError::conflict(format!(
                "run_key '{}' already reserved with different trigger payload",
                existing.run_key
            )));
        }
    };

    let user_id = user_id_for_events(&ctx);

    let event_paths = append_run_events(
        &ledger,
        &ctx.tenant,
        &workspace_id,
        user_id,
        &run_id,
        &plan_id,
        Some(run_key),
        labels,
        parent_run.code_version.clone(),
        root_assets,
        tasks,
        run_event_overrides,
    )
    .await?;

    compact_orchestration_events(&state.config, storage.clone(), event_paths).await?;

    Ok((
        StatusCode::CREATED,
        Json(RerunRunResponse {
            run_id,
            plan_id,
            state: RunStateResponse::Pending,
            created: true,
            created_at: now,
            parent_run_id,
            rerun_kind,
        }),
    ))
}

/// Manually evaluate a sensor with an arbitrary payload.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}/evaluate",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("sensor_id" = String, Path, description = "Sensor ID")
    ),
    request_body = ManualSensorEvaluateRequest,
    responses(
        (status = 200, description = "Sensor evaluated", body = ManualSensorEvaluateResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn manual_evaluate_sensor(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, sensor_id)): Path<(String, String)>,
    Json(request): Json<ManualSensorEvaluateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    let warning = match load_orchestration_state(&ctx, &state).await {
        Ok(fold_state) => {
            fold_state
                .sensor_state
                .get(&sensor_id)
                .and_then(|row| match row.status {
                    SensorStatus::Active => None,
                    SensorStatus::Paused => {
                        Some("Sensor is paused but manual evaluate bypasses status".to_string())
                    }
                    SensorStatus::Error => {
                        Some("Sensor is in error but manual evaluate bypasses status".to_string())
                    }
                })
        }
        Err(err) => {
            tracing::warn!(
                sensor_id = %sensor_id,
                error = ?err,
                "failed to load sensor state for manual evaluate warning"
            );
            None
        }
    };

    let message_id = request
        .message_id
        .clone()
        .or_else(|| ctx.idempotency_key.clone())
        .unwrap_or_else(|| format!("manual_{}", Ulid::new()));

    let data = serde_json::to_vec(&request.payload)
        .map_err(|e| ApiError::bad_request(format!("failed to serialize sensor payload: {e}")))?;

    let message = PubSubMessage {
        message_id: message_id.clone(),
        data,
        attributes: request.attributes.clone(),
        publish_time: Utc::now(),
    };

    let handler = PushSensorHandler::with_evaluator(state.sensor_evaluator());
    let events = handler.handle_message(
        &sensor_id,
        &ctx.tenant,
        &workspace_id,
        SensorStatus::Active,
        &message,
    );

    let eval_summary = events.iter().find_map(|event| {
        if let OrchestrationEventData::SensorEvaluated {
            eval_id,
            status,
            run_requests,
            ..
        } = &event.data
        {
            Some((eval_id.clone(), status.clone(), run_requests.clone()))
        } else {
            None
        }
    });

    let Some((eval_id, status, run_requests)) = eval_summary else {
        return Err(ApiError::internal(
            "sensor evaluation did not emit SensorEvaluated",
        ));
    };

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let ledger = LedgerWriter::new(storage.clone());
    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    let events_written = event_paths.len();

    ledger
        .append_all(events)
        .await
        .map_err(|e| ApiError::internal(format!("failed to append sensor events: {e}")))?;

    compact_orchestration_events(&state.config, storage, event_paths).await?;

    Ok((
        StatusCode::OK,
        Json(ManualSensorEvaluateResponse {
            eval_id,
            message_id,
            status: map_sensor_eval_status(&status),
            run_requests: map_run_requests(&run_requests),
            events_written: u32::try_from(events_written).unwrap_or(u32::MAX),
            warning,
        }),
    ))
}

/// Backfill missing `run_key` fingerprints.
///
/// Allows updating legacy reservations that were created before fingerprints
/// were introduced, enabling strict payload validation moving forward.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/runs/run-key/backfill",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID")
    ),
    request_body = RunKeyBackfillRequest,
    responses(
        (status = 200, description = "Backfill result", body = RunKeyBackfillResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 404, description = "run_key not found", body = ApiErrorBody),
        (status = 409, description = "run_key fingerprint conflict", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn backfill_run_key(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    Json(request): Json<RunKeyBackfillRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    if request.selection.is_empty() {
        return Err(ApiError::bad_request("selection cannot be empty"));
    }

    let fingerprint_request = TriggerRunRequest {
        selection: request.selection.clone(),
        include_upstream: request.include_upstream,
        include_downstream: request.include_downstream,
        partitions: request.partitions.clone(),
        run_key: Some(request.run_key.clone()),
        labels: request.labels.clone(),
    };
    let request_fingerprint = build_request_fingerprint(&fingerprint_request)?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let existing = get_reservation(&storage, &request.run_key)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read run_key reservation: {e}")))?;
    let Some(existing) = existing else {
        return Err(ApiError::not_found(format!(
            "run_key reservation not found: {}",
            request.run_key
        )));
    };

    if let Some(existing_fingerprint) = existing.request_fingerprint.as_deref() {
        if existing_fingerprint == request_fingerprint {
            return Ok(Json(RunKeyBackfillResponse {
                run_key: existing.run_key,
                run_id: existing.run_id,
                request_fingerprint,
                updated: false,
                created_at: existing.created_at,
            }));
        }

        return Err(ApiError::conflict(format!(
            "run_key '{}' already reserved with different fingerprint",
            existing.run_key
        )));
    }

    if let Some(cutoff) = state.config.run_key_fingerprint_cutoff {
        if existing.created_at >= cutoff {
            return Err(ApiError::conflict(format!(
                "run_key '{}' missing fingerprint after cutoff",
                existing.run_key
            )));
        }
    }

    let path = reservation_path(&request.run_key);
    let meta = storage
        .head_raw(&path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read run_key metadata: {e}")))?;
    let Some(meta) = meta else {
        return Err(ApiError::not_found(format!(
            "run_key reservation not found: {}",
            request.run_key
        )));
    };

    let mut updated = existing.clone();
    updated.request_fingerprint = Some(request_fingerprint.clone());
    let json = serde_json::to_string(&updated)
        .map_err(|e| ApiError::internal(format!("failed to serialize run_key reservation: {e}")))?;

    let result = storage
        .put_raw(
            &path,
            Bytes::from(json),
            WritePrecondition::MatchesVersion(meta.version),
        )
        .await
        .map_err(|e| ApiError::internal(format!("failed to update run_key reservation: {e}")))?;

    match result {
        WriteResult::Success { .. } => Ok(Json(RunKeyBackfillResponse {
            run_key: updated.run_key,
            run_id: updated.run_id,
            request_fingerprint,
            updated: true,
            created_at: updated.created_at,
        })),
        WriteResult::PreconditionFailed { .. } => Err(ApiError::conflict(
            "run_key reservation updated concurrently; retry".to_string(),
        )),
    }
}

/// Get run by ID.
///
/// Returns the current state and task details for a run.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/runs/{run_id}",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("run_id" = String, Path, description = "Run ID")
    ),
    responses(
        (status = 200, description = "Run details", body = RunResponse),
        (status = 404, description = "Run not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_run(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, run_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        run_id = %run_id,
        "Getting run"
    );

    ensure_workspace(&ctx, &workspace_id)?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let run = fold_state
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found(format!("run not found: {run_id}")))?;

    let tasks: Vec<&TaskRow> = fold_state
        .tasks
        .values()
        .filter(|row| row.run_id == run_id)
        .collect();

    let task_counts = build_task_counts(run, &tasks);
    let started_at = tasks.iter().filter_map(|row| row.started_at).min();
    let task_summaries = tasks
        .iter()
        .map(|row| TaskSummary {
            task_key: row.task_key.clone(),
            asset_key: row.asset_key.clone(),
            state: map_task_state(row.state),
            attempt: row.attempt,
            started_at: row.started_at,
            completed_at: row.completed_at,
            error_message: row.error_message.clone(),
        })
        .collect::<Vec<_>>();

    let (parent_run_id, rerun_kind) = lineage_from_labels(&run.labels);

    let response = RunResponse {
        run_id: run.run_id.clone(),
        workspace_id,
        plan_id: run.plan_id.clone(),
        state: map_run_row_state(run),
        created_at: run.triggered_at,
        started_at,
        completed_at: run.completed_at,
        tasks: task_summaries,
        task_counts,
        labels: run.labels.clone(),
        parent_run_id,
        rerun_kind,
    };

    Ok(Json(response))
}

/// List runs.
///
/// Returns a paginated list of runs for the workspace.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/runs",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("limit" = Option<u32>, Query, description = "Maximum runs to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
        ("state" = Option<RunStateResponse>, Query, description = "Filter by state"),
    ),
    responses(
        (status = 200, description = "List of runs", body = ListRunsResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_runs(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    AxumQuery(query): AxumQuery<ListRunsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        limit = query.limit,
        cursor = ?query.cursor,
        state_filter = ?query.state,
        "Listing runs"
    );

    ensure_workspace(&ctx, &workspace_id)?;
    if query.limit == 0 || query.limit > MAX_LIST_LIMIT {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_LIST_LIMIT}"
        )));
    }
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let mut runs: Vec<&RunRow> = fold_state.runs.values().collect();
    if let Some(filter_state) = query.state {
        runs.retain(|row| map_run_row_state(row) == filter_state);
    }

    runs.sort_by(|a, b| b.triggered_at.cmp(&a.triggered_at));

    let offset = query
        .cursor
        .as_deref()
        .map(|cursor| {
            cursor
                .parse::<usize>()
                .map_err(|_| ApiError::bad_request("invalid cursor"))
        })
        .transpose()?
        .unwrap_or(0);

    let limit = query.limit as usize;
    let end = (offset + limit).min(runs.len());
    let page = runs.get(offset..end).unwrap_or_default();

    let next_cursor = if end < runs.len() {
        Some(end.to_string())
    } else {
        None
    };

    let items = page
        .iter()
        .map(|row| {
            let (parent_run_id, rerun_kind) = lineage_from_labels(&row.labels);
            RunListItem {
                run_id: row.run_id.clone(),
                state: map_run_row_state(row),
                created_at: row.triggered_at,
                completed_at: row.completed_at,
                task_count: row.tasks_total,
                tasks_succeeded: row.tasks_succeeded,
                tasks_failed: row.tasks_failed,
                parent_run_id,
                rerun_kind,
            }
        })
        .collect::<Vec<_>>();

    Ok(Json(ListRunsResponse {
        runs: items,
        next_cursor,
    }))
}

/// Cancel a run.
///
/// Initiates cancellation of a running run.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/runs/{run_id}/cancel",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("run_id" = String, Path, description = "Run ID")
    ),
    request_body = CancelRunRequest,
    responses(
        (status = 200, description = "Run cancelled", body = CancelRunResponse),
        (status = 404, description = "Run not found", body = ApiErrorBody),
        (status = 409, description = "Run already completed", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn cancel_run(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, run_id)): Path<(String, String)>,
    Json(request): Json<CancelRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        run_id = %run_id,
        reason = ?request.reason,
        "Cancelling run"
    );

    ensure_workspace(&ctx, &workspace_id)?;

    // Load current state to check if run exists and is cancellable
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let run = fold_state
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found(format!("run not found: {run_id}")))?;

    // Check if run is already in a terminal state
    if run.state.is_terminal() {
        return Err(ApiError::conflict(format!(
            "run {} is already in terminal state {:?}",
            run_id, run.state
        )));
    }

    // Check if cancellation was already requested
    if run.cancel_requested {
        // Idempotent - return success
        return Ok(Json(CancelRunResponse {
            run_id,
            state: RunStateResponse::Cancelling,
            cancelled_at: Utc::now(),
        }));
    }

    // Create ledger writer
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let ledger = LedgerWriter::new(storage.clone());

    // Emit RunCancelRequested event
    let cancel_event = OrchestrationEvent::new(
        &ctx.tenant,
        &workspace_id,
        OrchestrationEventData::RunCancelRequested {
            run_id: run_id.clone(),
            reason: request.reason,
            requested_by: user_id_for_events(&ctx),
        },
    );

    let event_path = LedgerWriter::event_path(&cancel_event);
    ledger.append(cancel_event).await.map_err(|e| {
        ApiError::internal(format!("failed to write RunCancelRequested event: {e}"))
    })?;

    compact_orchestration_events(&state.config, storage, vec![event_path]).await?;

    Ok(Json(CancelRunResponse {
        run_id,
        state: RunStateResponse::Cancelling,
        cancelled_at: Utc::now(),
    }))
}

/// Upload task logs for a run.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/runs/{run_id}/logs",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("run_id" = String, Path, description = "Run ID")
    ),
    request_body = RunLogsRequest,
    responses(
        (status = 200, description = "Logs stored", body = RunLogsResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
        (status = 409, description = "Log already exists", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn upload_run_logs(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, run_id)): Path<(String, String)>,
    Json(request): Json<RunLogsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    if request.attempt == 0 {
        return Err(ApiError::bad_request("attempt must be >= 1"));
    }
    if !is_valid_task_key(&request.task_key) {
        return Err(ApiError::bad_request("invalid task_key"));
    }

    let stdout_bytes = request.stdout.len();
    let stderr_bytes = request.stderr.len();
    let total_bytes = stdout_bytes + stderr_bytes;
    if total_bytes > MAX_LOG_BYTES {
        return Err(ApiError::bad_request(format!(
            "logs exceed {MAX_LOG_BYTES} bytes"
        )));
    }

    let body = format!(
        "=== stdout ===\n{}\n=== stderr ===\n{}\n",
        request.stdout, request.stderr
    );
    let size_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let path = format!(
        "logs/{}/{}/attempt-{}.log",
        run_id, request.task_key, request.attempt
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let result = storage
        .put_raw(&path, Bytes::from(body), WritePrecondition::DoesNotExist)
        .await
        .map_err(|e| ApiError::internal(format!("failed to store logs: {e}")))?;

    match result {
        WriteResult::Success { .. } => Ok(Json(RunLogsResponse { path, size_bytes })),
        WriteResult::PreconditionFailed { .. } => Err(ApiError::conflict(
            "logs already exist for task attempt".to_string(),
        )),
    }
}

/// Get logs for a run.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/runs/{run_id}/logs",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("run_id" = String, Path, description = "Run ID"),
        ("taskKey" = Option<String>, Query, description = "Filter by task key")
    ),
    responses(
        (status = 200, description = "Aggregated logs", body = String, content_type = "text/plain"),
        (status = 404, description = "Logs not found", body = ApiErrorBody),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_run_logs(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, run_id)): Path<(String, String)>,
    AxumQuery(query): AxumQuery<RunLogsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    if let Some(ref task_key) = query.task_key {
        if !is_valid_task_key(task_key) {
            return Err(ApiError::bad_request("invalid task_key"));
        }
    }

    let prefix = query.task_key.map_or_else(
        || format!("logs/{run_id}/"),
        |task_key| format!("logs/{run_id}/{task_key}/"),
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let mut paths = storage
        .list(&prefix)
        .await
        .map_err(|e| ApiError::internal(format!("failed to list logs: {e}")))?;

    if paths.is_empty() {
        return Err(ApiError::not_found("logs not found"));
    }

    paths.sort_by(|a, b| a.as_str().cmp(b.as_str()));

    let mut output = String::new();
    for path in paths {
        let bytes = storage
            .get_raw(path.as_str())
            .await
            .map_err(|e| ApiError::internal(format!("failed to read log: {e}")))?;
        let chunk = String::from_utf8_lossy(&bytes);
        output.push_str("--- ");
        output.push_str(path.as_str());
        output.push_str(" ---\n");
        output.push_str(&chunk);
        if !chunk.ends_with('\n') {
            output.push('\n');
        }
    }

    Ok((StatusCode::OK, output))
}

// ============================================================================
// Schedule Routes
// ============================================================================

/// List schedules.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/schedules",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("limit" = Option<u32>, Query, description = "Maximum schedules to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
    ),
    responses(
        (status = 200, description = "List of schedules", body = ListSchedulesResponse),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_schedules(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    AxumQuery(query): AxumQuery<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let mut schedules: Vec<ScheduleResponse> = fold_state
        .schedule_definitions
        .values()
        .map(|def| {
            let state = fold_state.schedule_state.get(def.schedule_id.as_str());
            map_schedule(def, state)
        })
        .collect();
    schedules.sort_by(|a, b| a.schedule_id.cmp(&b.schedule_id));

    let (page, next_cursor) = paginate(&schedules, limit, offset);

    Ok(Json(ListSchedulesResponse {
        schedules: page,
        next_cursor,
    }))
}

/// Get schedule by ID.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("schedule_id" = String, Path, description = "Schedule ID")
    ),
    responses(
        (status = 200, description = "Schedule details", body = ScheduleResponse),
        (status = 404, description = "Schedule not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn get_schedule(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, schedule_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let definition = fold_state
        .schedule_definitions
        .get(schedule_id.as_str())
        .ok_or_else(|| ApiError::not_found(format!("schedule not found: {schedule_id}")))?;

    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    Ok(Json(map_schedule(definition, schedule_state)))
}

pub(crate) async fn upsert_schedule(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, schedule_id)): Path<(String, String)>,
    Json(request): Json<UpsertScheduleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    validate_schedule_upsert(&schedule_id, &request)?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let existing = fold_state.schedule_definitions.get(schedule_id.as_str());
    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    if let Some(existing) = existing {
        if schedule_definition_matches_request(existing, &request) {
            return Ok((StatusCode::OK, Json(map_schedule(existing, schedule_state))));
        }
    }

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let ledger = LedgerWriter::new(storage.clone());
    let existed = existing.is_some();

    let mut event = OrchestrationEvent::new(
        &ctx.tenant,
        &workspace_id,
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: request.cron_expression,
            timezone: request.timezone,
            catchup_window_minutes: request.catchup_window_minutes,
            asset_selection: request.asset_selection,
            max_catchup_ticks: request.max_catchup_ticks,
            enabled: request.enabled,
        },
    );

    if let Some(existing) = existing {
        while event.event_id <= existing.row_version {
            event.event_id = Ulid::new().to_string();
        }
    }

    let request_key = ctx
        .idempotency_key
        .clone()
        .unwrap_or_else(|| event.event_id.clone());
    event.idempotency_key = format!("sched_def_api:{schedule_id}:{request_key}");

    let event_path = LedgerWriter::event_path(&event);

    ledger.append(event).await.map_err(|e| {
        ApiError::internal(format!("failed to append schedule definition event: {e}"))
    })?;

    compact_orchestration_events(&state.config, storage, vec![event_path]).await?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let definition = fold_state
        .schedule_definitions
        .get(schedule_id.as_str())
        .ok_or_else(|| ApiError::internal("schedule definition missing after upsert"))?;

    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    let status_code = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };

    Ok((status_code, Json(map_schedule(definition, schedule_state))))
}

pub(crate) async fn enable_schedule(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, schedule_id)): Path<(String, String)>,
    Json(_): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let existing = fold_state
        .schedule_definitions
        .get(schedule_id.as_str())
        .ok_or_else(|| ApiError::not_found(format!("schedule not found: {schedule_id}")))?;
    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    if existing.enabled {
        return Ok((StatusCode::OK, Json(map_schedule(existing, schedule_state))));
    }

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let ledger = LedgerWriter::new(storage.clone());
    let mut event = OrchestrationEvent::new(
        &ctx.tenant,
        &workspace_id,
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: existing.cron_expression.clone(),
            timezone: existing.timezone.clone(),
            catchup_window_minutes: existing.catchup_window_minutes,
            asset_selection: existing.asset_selection.clone(),
            max_catchup_ticks: existing.max_catchup_ticks,
            enabled: true,
        },
    );

    while event.event_id <= existing.row_version {
        event.event_id = Ulid::new().to_string();
    }

    let request_key = ctx
        .idempotency_key
        .clone()
        .unwrap_or_else(|| event.event_id.clone());
    event.idempotency_key = format!("sched_enable:{schedule_id}:{request_key}");

    let event_path = LedgerWriter::event_path(&event);

    ledger.append(event).await.map_err(|e| {
        ApiError::internal(format!("failed to append schedule definition event: {e}"))
    })?;

    compact_orchestration_events(&state.config, storage, vec![event_path]).await?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let definition = fold_state
        .schedule_definitions
        .get(schedule_id.as_str())
        .ok_or_else(|| ApiError::internal("schedule definition missing after enable"))?;

    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    Ok((
        StatusCode::OK,
        Json(map_schedule(definition, schedule_state)),
    ))
}

pub(crate) async fn disable_schedule(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, schedule_id)): Path<(String, String)>,
    Json(_): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let existing = fold_state
        .schedule_definitions
        .get(schedule_id.as_str())
        .ok_or_else(|| ApiError::not_found(format!("schedule not found: {schedule_id}")))?;
    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    if !existing.enabled {
        return Ok((StatusCode::OK, Json(map_schedule(existing, schedule_state))));
    }

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let ledger = LedgerWriter::new(storage.clone());
    let mut event = OrchestrationEvent::new(
        &ctx.tenant,
        &workspace_id,
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: existing.cron_expression.clone(),
            timezone: existing.timezone.clone(),
            catchup_window_minutes: existing.catchup_window_minutes,
            asset_selection: existing.asset_selection.clone(),
            max_catchup_ticks: existing.max_catchup_ticks,
            enabled: false,
        },
    );

    while event.event_id <= existing.row_version {
        event.event_id = Ulid::new().to_string();
    }

    let request_key = ctx
        .idempotency_key
        .clone()
        .unwrap_or_else(|| event.event_id.clone());
    event.idempotency_key = format!("sched_disable:{schedule_id}:{request_key}");

    let event_path = LedgerWriter::event_path(&event);

    ledger.append(event).await.map_err(|e| {
        ApiError::internal(format!("failed to append schedule definition event: {e}"))
    })?;

    compact_orchestration_events(&state.config, storage, vec![event_path]).await?;

    let fold_state = load_orchestration_state(&ctx, &state).await?;
    let definition = fold_state
        .schedule_definitions
        .get(schedule_id.as_str())
        .ok_or_else(|| ApiError::internal("schedule definition missing after disable"))?;

    let schedule_state = fold_state.schedule_state.get(schedule_id.as_str());

    Ok((
        StatusCode::OK,
        Json(map_schedule(definition, schedule_state)),
    ))
}

pub(crate) async fn get_schedule_tick(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, schedule_id, tick_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    if !fold_state
        .schedule_definitions
        .contains_key(schedule_id.as_str())
    {
        return Err(ApiError::not_found(format!(
            "schedule not found: {schedule_id}"
        )));
    }

    let tick = fold_state
        .schedule_ticks
        .get(tick_id.as_str())
        .ok_or_else(|| ApiError::not_found(format!("tick not found: {tick_id}")))?;

    if tick.schedule_id != schedule_id {
        return Err(ApiError::not_found(format!("tick not found: {tick_id}")));
    }

    Ok(Json(map_schedule_tick(tick)))
}

/// List schedule ticks.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}/ticks",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("schedule_id" = String, Path, description = "Schedule ID"),
        ("limit" = Option<u32>, Query, description = "Maximum ticks to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
        ("status" = Option<TickStatusResponse>, Query, description = "Filter by status"),
    ),
    responses(
        (status = 200, description = "List of ticks", body = ListScheduleTicksResponse),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_schedule_ticks(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, schedule_id)): Path<(String, String)>,
    AxumQuery(query): AxumQuery<ListTicksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let mut ticks: Vec<ScheduleTickResponse> = fold_state
        .schedule_ticks
        .values()
        .filter(|t| t.schedule_id == schedule_id)
        .map(map_schedule_tick)
        .collect();
    filter_ticks_by_status(&mut ticks, query.status);
    ticks.sort_by(|a, b| {
        b.scheduled_for
            .cmp(&a.scheduled_for)
            .then_with(|| b.tick_id.cmp(&a.tick_id))
    });

    let (page, next_cursor) = paginate(&ticks, limit, offset);

    Ok(Json(ListScheduleTicksResponse {
        ticks: page,
        next_cursor,
    }))
}

// ============================================================================
// Sensor Routes
// ============================================================================

/// List sensors.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/sensors",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("limit" = Option<u32>, Query, description = "Maximum sensors to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
    ),
    responses(
        (status = 200, description = "List of sensors", body = ListSensorsResponse),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_sensors(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    AxumQuery(query): AxumQuery<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let mut sensors: Vec<SensorResponse> = fold_state
        .sensor_state
        .values()
        .map(map_sensor_state)
        .collect();
    sensors.sort_by(|a, b| a.sensor_id.cmp(&b.sensor_id));

    let (page, next_cursor) = paginate(&sensors, limit, offset);

    Ok(Json(ListSensorsResponse {
        sensors: page,
        next_cursor,
    }))
}

/// Get sensor by ID.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("sensor_id" = String, Path, description = "Sensor ID")
    ),
    responses(
        (status = 200, description = "Sensor details", body = SensorResponse),
        (status = 404, description = "Sensor not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn get_sensor(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, sensor_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let sensor = fold_state
        .sensor_state
        .get(&sensor_id)
        .ok_or_else(|| ApiError::not_found(format!("sensor not found: {sensor_id}")))?;

    Ok(Json(map_sensor_state(sensor)))
}

/// List sensor evaluations.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}/evals",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("sensor_id" = String, Path, description = "Sensor ID"),
        ("limit" = Option<u32>, Query, description = "Maximum evaluations to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
    ),
    responses(
        (status = 200, description = "List of evaluations", body = ListSensorEvalsResponse),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_sensor_evals(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, sensor_id)): Path<(String, String)>,
    AxumQuery(query): AxumQuery<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let mut evals: Vec<SensorEvalResponse> = fold_state
        .sensor_evals
        .values()
        .filter(|e| e.sensor_id == sensor_id)
        .map(map_sensor_eval)
        .collect();
    evals.sort_by(|a, b| b.evaluated_at.cmp(&a.evaluated_at));

    let (page, next_cursor) = paginate(&evals, limit, offset);

    Ok(Json(ListSensorEvalsResponse {
        evals: page,
        next_cursor,
    }))
}

// ============================================================================
// Backfill Routes
// ============================================================================

/// Create a new backfill.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/backfills",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID")
    ),
    request_body = CreateBackfillRequest,
    responses(
        (status = 202, description = "Backfill accepted", body = CreateBackfillResponse),
        (status = 200, description = "Existing backfill returned", body = CreateBackfillResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 409, description = "Idempotency conflict", body = ApiErrorBody),
        (status = 422, description = "Partition selector not supported", body = UnprocessableEntityResponse),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn create_backfill(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    Json(request): Json<CreateBackfillRequest>,
) -> Result<Response, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;

    let CreateBackfillRequest {
        asset_selection,
        partition_selector,
        client_request_id,
        chunk_size,
        max_concurrent_runs,
    } = request;

    if asset_selection.is_empty() {
        return Err(ApiError::bad_request("assetSelection cannot be empty"));
    }

    if asset_selection.len() > 1 {
        return Err(ApiError::bad_request(
            "multi-asset backfills are not supported yet",
        ));
    }

    let idempotency_key = client_request_id
        .or_else(|| ctx.idempotency_key.clone())
        .ok_or_else(|| ApiError::bad_request("clientRequestId or Idempotency-Key is required"))?;

    let (partition_selector, partition_keys) = match partition_selector {
        PartitionSelectorRequest::Explicit { partitions } => {
            explicit_partition_selector(partitions)?
        }
        PartitionSelectorRequest::Range { .. } | PartitionSelectorRequest::Filter { .. } => {
            let response = unsupported_partition_selector_response();
            return Ok((StatusCode::UNPROCESSABLE_ENTITY, Json(response)).into_response());
        }
    };

    let chunk_size = match chunk_size {
        Some(0) | None => DEFAULT_BACKFILL_CHUNK_SIZE,
        Some(value) => value,
    };
    let max_concurrent_runs = match max_concurrent_runs {
        Some(0) | None => DEFAULT_BACKFILL_MAX_CONCURRENT_RUNS,
        Some(value) => value,
    };
    let input = BackfillCreateInput {
        asset_selection,
        partition_selector,
        partition_keys,
        chunk_size,
        max_concurrent_runs,
    };

    let fingerprint = compute_backfill_fingerprint(
        &input.asset_selection,
        &input.partition_keys,
        input.chunk_size,
        input.max_concurrent_runs,
    )?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    if let Some(existing) =
        resolve_backfill_idempotency(&storage, &idempotency_key, &fingerprint).await?
    {
        return Ok((StatusCode::OK, Json(existing)).into_response());
    }

    let response = append_backfill_created_event(
        state.as_ref(),
        &ctx,
        &workspace_id,
        input,
        &idempotency_key,
        &storage,
        fingerprint,
    )
    .await?;

    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

/// List backfills.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/backfills",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("limit" = Option<u32>, Query, description = "Maximum backfills to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
        ("state" = Option<BackfillStateResponse>, Query, description = "Filter by state"),
    ),
    responses(
        (status = 200, description = "List of backfills", body = ListBackfillsResponse),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_backfills(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    AxumQuery(query): AxumQuery<ListBackfillsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let counts_by_backfill = build_chunk_counts_index(fold_state.backfill_chunks.values());
    let mut backfills: Vec<BackfillResponse> = fold_state
        .backfills
        .values()
        .map(|b| {
            let counts = counts_by_backfill
                .get(&b.backfill_id)
                .cloned()
                .unwrap_or_default();
            map_backfill_with_counts(b, counts)
        })
        .collect();

    if let Some(state_filter) = query.state {
        backfills.retain(|b| b.state == state_filter);
    }

    backfills.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let (page, next_cursor) = paginate(&backfills, limit, offset);

    Ok(Json(ListBackfillsResponse {
        backfills: page,
        next_cursor,
    }))
}

/// Get backfill by ID.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/backfills/{backfill_id}",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("backfill_id" = String, Path, description = "Backfill ID")
    ),
    responses(
        (status = 200, description = "Backfill details", body = BackfillResponse),
        (status = 404, description = "Backfill not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn get_backfill(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, backfill_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let backfill = fold_state
        .backfills
        .get(&backfill_id)
        .ok_or_else(|| ApiError::not_found(format!("backfill not found: {backfill_id}")))?;

    let chunks: Vec<&BackfillChunkRow> = fold_state
        .backfill_chunks
        .values()
        .filter(|c| c.backfill_id == backfill_id)
        .collect();

    Ok(Json(map_backfill(backfill, &chunks)))
}

/// List backfill chunks.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/backfills/{backfill_id}/chunks",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("backfill_id" = String, Path, description = "Backfill ID"),
        ("limit" = Option<u32>, Query, description = "Maximum chunks to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
        ("state" = Option<ChunkStateResponse>, Query, description = "Filter by state"),
    ),
    responses(
        (status = 200, description = "List of chunks", body = ListBackfillChunksResponse),
        (status = 404, description = "Backfill not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_backfill_chunks(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, backfill_id)): Path<(String, String)>,
    AxumQuery(query): AxumQuery<ListChunksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    // Verify backfill exists
    if !fold_state.backfills.contains_key(&backfill_id) {
        return Err(ApiError::not_found(format!(
            "backfill not found: {backfill_id}"
        )));
    }

    let mut chunks: Vec<BackfillChunkResponse> = fold_state
        .backfill_chunks
        .values()
        .filter(|c| c.backfill_id == backfill_id)
        .map(map_backfill_chunk)
        .collect();

    if let Some(state_filter) = query.state {
        chunks.retain(|c| c.state as u8 == state_filter as u8);
    }

    chunks.sort_by(|a, b| a.chunk_index.cmp(&b.chunk_index));

    let (page, next_cursor) = paginate(&chunks, limit, offset);

    Ok(Json(ListBackfillChunksResponse {
        chunks: page,
        next_cursor,
    }))
}

// ============================================================================
// Partition Routes
// ============================================================================

/// List partitions.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/partitions",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("limit" = Option<u32>, Query, description = "Maximum partitions to return"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
        ("assetKey" = Option<String>, Query, description = "Filter by asset key"),
        ("staleOnly" = Option<bool>, Query, description = "Filter to stale partitions only"),
    ),
    responses(
        (status = 200, description = "List of partitions", body = ListPartitionsResponse),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn list_partitions(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    AxumQuery(query): AxumQuery<ListPartitionsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let (limit, offset) = parse_pagination(
        query.limit.unwrap_or(DEFAULT_LIMIT),
        query.cursor.as_deref(),
    )?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let mut partitions: Vec<PartitionStatusApiResponse> = fold_state
        .partition_status
        .values()
        .filter(|p| {
            if let Some(ref asset_key) = query.asset_key {
                if &p.asset_key != asset_key {
                    return false;
                }
            }
            if query.stale_only.unwrap_or(false) && p.stale_since.is_none() {
                return false;
            }
            true
        })
        .map(map_partition_status)
        .collect();

    partitions.sort_by(|a, b| {
        a.asset_key
            .cmp(&b.asset_key)
            .then_with(|| a.partition_key.cmp(&b.partition_key))
    });

    let (page, next_cursor) = paginate(&partitions, limit, offset);

    Ok(Json(ListPartitionsResponse {
        partitions: page,
        next_cursor,
    }))
}

/// Get partition summary for an asset.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/assets/{asset_key}/partitions/summary",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("asset_key" = String, Path, description = "Asset key (URL-encoded)")
    ),
    responses(
        (status = 200, description = "Partition summary", body = AssetPartitionSummaryResponse),
        (status = 404, description = "Asset not found", body = ApiErrorBody),
    ),
    tag = "Orchestration",
    security(("bearerAuth" = []))
)]
pub(crate) async fn get_asset_partition_summary(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, asset_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_workspace(&ctx, &workspace_id)?;
    let fold_state = load_orchestration_state(&ctx, &state).await?;

    let partitions: Vec<&PartitionStatusRow> = fold_state
        .partition_status
        .values()
        .filter(|p| p.asset_key == asset_key)
        .collect();

    if partitions.is_empty() {
        return Err(ApiError::not_found(format!(
            "no partitions found for asset: {asset_key}"
        )));
    }

    let total = u32::try_from(partitions.len()).unwrap_or(u32::MAX);
    let materialized = u32::try_from(
        partitions
            .iter()
            .filter(|p| p.last_materialization_run_id.is_some())
            .count(),
    )
    .unwrap_or(u32::MAX);
    let stale_count = u32::try_from(
        partitions
            .iter()
            .filter(|p| p.stale_since.is_some())
            .count(),
    )
    .unwrap_or(u32::MAX);
    let missing = total.saturating_sub(materialized);

    Ok(Json(AssetPartitionSummaryResponse {
        asset_key,
        total_partitions: total,
        materialized_partitions: materialized,
        stale_partitions: stale_count,
        missing_partitions: missing,
    }))
}

// ============================================================================
// Router
// ============================================================================

/// Creates the orchestration routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        // Runs
        .route("/workspaces/:workspace_id/runs", post(trigger_run))
        .route("/workspaces/:workspace_id/runs", get(list_runs))
        .route("/workspaces/:workspace_id/runs/:run_id", get(get_run))
        .route(
            "/workspaces/:workspace_id/runs/:run_id/rerun",
            post(rerun_run),
        )
        .route(
            "/workspaces/:workspace_id/runs/run-key/backfill",
            post(backfill_run_key),
        )
        .route(
            "/workspaces/:workspace_id/runs/:run_id/cancel",
            post(cancel_run),
        )
        .route(
            "/workspaces/:workspace_id/runs/:run_id/logs",
            post(upload_run_logs),
        )
        .route(
            "/workspaces/:workspace_id/runs/:run_id/logs",
            get(get_run_logs),
        )
        // Schedules
        .route("/workspaces/:workspace_id/schedules", get(list_schedules))
        .route(
            "/workspaces/:workspace_id/schedules/:schedule_id",
            get(get_schedule).put(upsert_schedule),
        )
        .route(
            "/workspaces/:workspace_id/schedules/:schedule_id/enable",
            post(enable_schedule),
        )
        .route(
            "/workspaces/:workspace_id/schedules/:schedule_id/disable",
            post(disable_schedule),
        )
        .route(
            "/workspaces/:workspace_id/schedules/:schedule_id/ticks",
            get(list_schedule_ticks),
        )
        .route(
            "/workspaces/:workspace_id/schedules/:schedule_id/ticks/:tick_id",
            get(get_schedule_tick),
        )
        // Sensors
        .route("/workspaces/:workspace_id/sensors", get(list_sensors))
        .route(
            "/workspaces/:workspace_id/sensors/:sensor_id",
            get(get_sensor),
        )
        .route(
            "/workspaces/:workspace_id/sensors/:sensor_id/evaluate",
            post(manual_evaluate_sensor),
        )
        .route(
            "/workspaces/:workspace_id/sensors/:sensor_id/evals",
            get(list_sensor_evals),
        )
        // Backfills
        .route(
            "/workspaces/:workspace_id/backfills",
            post(create_backfill).get(list_backfills),
        )
        .route(
            "/workspaces/:workspace_id/backfills/:backfill_id",
            get(get_backfill),
        )
        .route(
            "/workspaces/:workspace_id/backfills/:backfill_id/chunks",
            get(list_backfill_chunks),
        )
        // Partitions
        .route("/workspaces/:workspace_id/partitions", get(list_partitions))
        .route(
            "/workspaces/:workspace_id/assets/:asset_key/partitions/summary",
            get(get_asset_partition_summary),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::manifests::{AssetEntry, AssetKey, GitContext};
    use anyhow::{Result, anyhow};
    use arco_core::partition::{PartitionKey, ScalarValue};
    use axum::http::StatusCode;
    use chrono::Duration;

    #[test]
    fn test_trigger_request_deserialization() {
        let json = r#"{
            "selection": ["analytics/users"],
            "partitions": [{"key": "date", "value": "2024-01-15"}],
            "runKey": "daily-etl:2024-01-15"
        }"#;

        let request: TriggerRunRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.selection, vec!["analytics/users"]);
        assert_eq!(request.run_key, Some("daily-etl:2024-01-15".to_string()));
        assert_eq!(request.partitions.len(), 1);
    }

    #[test]
    fn test_run_response_serialization() {
        let response = RunResponse {
            run_id: "run_123".to_string(),
            workspace_id: "ws_456".to_string(),
            plan_id: "plan_789".to_string(),
            state: RunStateResponse::Running,
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            completed_at: None,
            tasks: vec![],
            task_counts: TaskCounts::default(),
            labels: HashMap::new(),
            parent_run_id: None,
            rerun_kind: None,
        };

        let json = serde_json::to_string(&response).expect("serialize");
        assert!(json.contains("\"runId\":\"run_123\""));
        assert!(json.contains("\"state\":\"RUNNING\""));
    }

    #[test]
    fn test_parse_pagination_rejects_zero_limit() {
        let err = parse_pagination(0, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.message(),
            format!("limit must be between 1 and {MAX_LIST_LIMIT}")
        );
    }

    #[test]
    fn test_parse_pagination_rejects_invalid_cursor() {
        let err = parse_pagination(1, Some("abc")).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.message(), "invalid cursor");
    }

    #[test]
    fn test_parse_pagination_parses_cursor() {
        let (limit, offset) = parse_pagination(5, Some("10")).expect("parse");
        assert_eq!(limit, 5);
        assert_eq!(offset, 10);
    }

    #[test]
    fn test_partition_selector_filter_maps_to_response() {
        let mut filters = HashMap::new();
        filters.insert("region".to_string(), "us-*".to_string());

        let selector = PartitionSelector::Filter {
            filters: filters.clone(),
        };
        let response = map_partition_selector(&selector);
        let json = serde_json::to_value(&response).expect("serialize");

        assert_eq!(json["type"], "filter");
        assert_eq!(json["filters"]["region"], "us-*");
    }

    #[test]
    fn test_build_chunk_counts_index_groups_by_backfill() {
        let rows = vec![
            BackfillChunkRow {
                tenant_id: "tenant".to_string(),
                workspace_id: "workspace".to_string(),
                chunk_id: "bf_a:0".to_string(),
                backfill_id: "bf_a".to_string(),
                chunk_index: 0,
                partition_keys: vec!["2025-01-01".to_string()],
                run_key: "rk_a0".to_string(),
                run_id: None,
                state: ChunkState::Pending,
                row_version: "01HQ1".to_string(),
            },
            BackfillChunkRow {
                tenant_id: "tenant".to_string(),
                workspace_id: "workspace".to_string(),
                chunk_id: "bf_a:1".to_string(),
                backfill_id: "bf_a".to_string(),
                chunk_index: 1,
                partition_keys: vec!["2025-01-02".to_string()],
                run_key: "rk_a1".to_string(),
                run_id: None,
                state: ChunkState::Planned,
                row_version: "01HQ2".to_string(),
            },
            BackfillChunkRow {
                tenant_id: "tenant".to_string(),
                workspace_id: "workspace".to_string(),
                chunk_id: "bf_a:2".to_string(),
                backfill_id: "bf_a".to_string(),
                chunk_index: 2,
                partition_keys: vec!["2025-01-03".to_string()],
                run_key: "rk_a2".to_string(),
                run_id: None,
                state: ChunkState::Running,
                row_version: "01HQ3".to_string(),
            },
            BackfillChunkRow {
                tenant_id: "tenant".to_string(),
                workspace_id: "workspace".to_string(),
                chunk_id: "bf_b:0".to_string(),
                backfill_id: "bf_b".to_string(),
                chunk_index: 0,
                partition_keys: vec!["2025-01-01".to_string()],
                run_key: "rk_b0".to_string(),
                run_id: None,
                state: ChunkState::Succeeded,
                row_version: "01HQ4".to_string(),
            },
            BackfillChunkRow {
                tenant_id: "tenant".to_string(),
                workspace_id: "workspace".to_string(),
                chunk_id: "bf_b:1".to_string(),
                backfill_id: "bf_b".to_string(),
                chunk_index: 1,
                partition_keys: vec!["2025-01-02".to_string()],
                run_key: "rk_b1".to_string(),
                run_id: None,
                state: ChunkState::Failed,
                row_version: "01HQ5".to_string(),
            },
        ];

        let counts = build_chunk_counts_index(rows.iter());

        let a = counts.get("bf_a").expect("bf_a");
        assert_eq!(a.total, 3);
        assert_eq!(a.pending, 2);
        assert_eq!(a.running, 1);
        assert_eq!(a.succeeded, 0);
        assert_eq!(a.failed, 0);

        let b = counts.get("bf_b").expect("bf_b");
        assert_eq!(b.total, 2);
        assert_eq!(b.pending, 0);
        assert_eq!(b.running, 0);
        assert_eq!(b.succeeded, 1);
        assert_eq!(b.failed, 1);
    }

    #[test]
    fn test_filter_ticks_by_status() {
        let mut ticks = vec![
            ScheduleTickResponse {
                tick_id: "tick_1".to_string(),
                schedule_id: "sched".to_string(),
                scheduled_for: Utc::now(),
                asset_selection: vec![],
                status: TickStatusResponse::Triggered,
                skip_reason: None,
                error_message: None,
                run_key: None,
                run_id: None,
            },
            ScheduleTickResponse {
                tick_id: "tick_2".to_string(),
                schedule_id: "sched".to_string(),
                scheduled_for: Utc::now(),
                asset_selection: vec![],
                status: TickStatusResponse::Failed,
                skip_reason: None,
                error_message: Some("boom".to_string()),
                run_key: None,
                run_id: None,
            },
        ];

        filter_ticks_by_status(&mut ticks, Some(TickStatusResponse::Failed));

        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].status, TickStatusResponse::Failed);
    }

    #[test]
    fn test_request_fingerprint_is_order_independent() -> Result<()> {
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("team".to_string(), "infra".to_string());

        let request_a = TriggerRunRequest {
            selection: vec!["analytics.b".to_string(), "analytics.a".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![
                PartitionValue {
                    key: "date".to_string(),
                    value: "2024-01-01".to_string(),
                },
                PartitionValue {
                    key: "region".to_string(),
                    value: "us-east-1".to_string(),
                },
            ],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: labels.clone(),
        };

        let mut labels_reordered = HashMap::new();
        labels_reordered.insert("team".to_string(), "infra".to_string());
        labels_reordered.insert("env".to_string(), "prod".to_string());

        let request_b = TriggerRunRequest {
            selection: vec!["analytics.a".to_string(), "analytics.b".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![
                PartitionValue {
                    key: "region".to_string(),
                    value: "us-east-1".to_string(),
                },
                PartitionValue {
                    key: "date".to_string(),
                    value: "2024-01-01".to_string(),
                },
            ],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: labels_reordered,
        };

        let fingerprint_a =
            build_request_fingerprint(&request_a).map_err(|err| anyhow!("{err:?}"))?;
        let fingerprint_b =
            build_request_fingerprint(&request_b).map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(fingerprint_a, fingerprint_b);
        Ok(())
    }

    #[test]
    fn test_build_partition_key_canonical_string() -> Result<()> {
        let partitions = vec![
            PartitionValue {
                key: "region".to_string(),
                value: "us-east-1".to_string(),
            },
            PartitionValue {
                key: "date".to_string(),
                value: "2024-01-15".to_string(),
            },
        ];

        let partition_key = build_partition_key(&partitions).map_err(|err| anyhow!("{err:?}"))?;

        let mut pk = PartitionKey::new();
        pk.insert("region", ScalarValue::String("us-east-1".to_string()));
        pk.insert("date", ScalarValue::String("2024-01-15".to_string()));
        let expected = pk.canonical_string();

        assert_eq!(partition_key.as_deref(), Some(expected.as_str()));
        Ok(())
    }

    #[test]
    fn test_build_partition_key_rejects_invalid_partition_key() {
        let partitions = vec![PartitionValue {
            key: "Date".to_string(),
            value: "2024-01-15".to_string(),
        }];

        assert!(build_partition_key(&partitions).is_err());
    }

    #[tokio::test]
    async fn test_trigger_run_reemits_when_reservation_exists() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;

        let manifest_id = Ulid::new().to_string();
        let stored_manifest = StoredManifest {
            manifest_id: manifest_id.clone(),
            tenant_id: ctx.tenant.clone(),
            workspace_id: ctx.workspace.clone(),
            manifest_version: "1.0".to_string(),
            code_version_id: "abc123".to_string(),
            fingerprint: "fp".to_string(),
            git: GitContext::default(),
            assets: vec![AssetEntry {
                key: AssetKey {
                    namespace: "analytics".to_string(),
                    name: "users".to_string(),
                },
                id: "01HQXYZ123".to_string(),
                description: String::new(),
                owners: vec![],
                tags: serde_json::Value::Null,
                partitioning: serde_json::Value::Null,
                dependencies: vec![],
                code: serde_json::Value::Null,
                checks: vec![],
                execution: serde_json::Value::Null,
                resources: serde_json::Value::Null,
                io: serde_json::Value::Null,
                transform_fingerprint: String::new(),
            }],
            schedules: vec![],
            deployed_at: Utc::now(),
            deployed_by: "test".to_string(),
            metadata: serde_json::Value::Null,
        };

        let manifest_path = crate::paths::manifest_path(&manifest_id);
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&stored_manifest)?),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                MANIFEST_LATEST_INDEX_PATH,
                Bytes::from(serde_json::to_vec(&LatestManifestIndex {
                    latest_manifest_id: manifest_id,
                    deployed_at: stored_manifest.deployed_at,
                })?),
                WritePrecondition::None,
            )
            .await?;

        let reservation = RunKeyReservation {
            run_key: "daily:2024-01-01".to_string(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(
                build_request_fingerprint(&request).map_err(|err| anyhow!("{err:?}"))?,
            ),
            created_at: Utc::now(),
        };

        let reserved =
            reserve_run_key(&storage, &reservation, FingerprintPolicy::lenient()).await?;
        assert!(matches!(reserved, ReservationResult::Reserved));

        let response = trigger_run(
            State(state.clone()),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request.clone()),
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        let payload: TriggerRunResponse = serde_json::from_slice(&body)?;

        assert_eq!(payload.run_id, reservation.run_id);
        assert_eq!(payload.plan_id, reservation.plan_id);
        assert!(!payload.created);

        let mut run_event = OrchestrationEvent::new(
            &ctx.tenant,
            &ctx.workspace,
            OrchestrationEventData::RunTriggered {
                run_id: reservation.run_id.clone(),
                plan_id: reservation.plan_id.clone(),
                trigger: TriggerInfo::Manual {
                    user_id: user_id_for_events(&ctx),
                },
                root_assets: vec!["analytics.users".to_string()],
                run_key: Some(reservation.run_key.clone()),
                labels: request.labels.clone(),
                code_version: None,
            },
        );
        apply_event_metadata(
            &mut run_event,
            &reservation.event_id,
            reservation.created_at,
        );
        let run_path = LedgerWriter::event_path(&run_event);
        let run_bytes = storage.get_raw(&run_path).await?;
        let stored_run: OrchestrationEvent = serde_json::from_slice(&run_bytes)?;
        assert_eq!(stored_run.event_id, reservation.event_id);
        assert_eq!(stored_run.event_type, "RunTriggered");

        let plan_event_id = reservation.plan_event_id.clone().expect("plan_event_id");
        let mut plan_event = OrchestrationEvent::new(
            &ctx.tenant,
            &ctx.workspace,
            OrchestrationEventData::PlanCreated {
                run_id: reservation.run_id.clone(),
                plan_id: reservation.plan_id.clone(),
                tasks: {
                    let graph = arco_flow::orchestration::AssetGraph::new();
                    let partition_key = build_partition_key(&request.partitions)
                        .map_err(|err| anyhow!("{err:?}"))?;
                    let options = arco_flow::orchestration::SelectionOptions {
                        include_upstream: request.include_upstream,
                        include_downstream: request.include_downstream,
                    };
                    arco_flow::orchestration::build_task_defs_for_selection(
                        &graph,
                        &request.selection,
                        options,
                        partition_key,
                    )
                    .map_err(|err| anyhow!("{err}"))?
                },
            },
        );
        apply_event_metadata(&mut plan_event, &plan_event_id, reservation.created_at);
        let plan_path = LedgerWriter::event_path(&plan_event);
        let plan_bytes = storage.get_raw(&plan_path).await?;
        let stored_plan: OrchestrationEvent = serde_json::from_slice(&plan_bytes)?;
        assert_eq!(stored_plan.event_id, plan_event_id);
        assert_eq!(stored_plan.event_type, "PlanCreated");

        Ok(())
    }

    #[tokio::test]
    async fn test_trigger_run_conflicts_on_fingerprint_mismatch() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request_a = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let request_b = TriggerRunRequest {
            selection: vec!["analytics/orders".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;

        let reservation = RunKeyReservation {
            run_key: "daily:2024-01-01".to_string(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(
                build_request_fingerprint(&request_a).map_err(|err| anyhow!("{err:?}"))?,
            ),
            created_at: Utc::now(),
        };

        let reserved =
            reserve_run_key(&storage, &reservation, FingerprintPolicy::lenient()).await?;
        assert!(matches!(reserved, ReservationResult::Reserved));

        let response = match trigger_run(
            State(state.clone()),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request_b),
        )
        .await
        {
            Ok(_) => panic!("expected conflict for mismatched run_key payload"),
            Err(err) => err.into_response(),
        };

        assert_eq!(response.status(), StatusCode::CONFLICT);
        Ok(())
    }

    #[tokio::test]
    async fn test_trigger_run_falls_back_to_scan_when_index_points_to_missing_manifest()
    -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;

        let manifest_id = Ulid::new().to_string();
        let stored_manifest = StoredManifest {
            manifest_id: manifest_id.clone(),
            tenant_id: ctx.tenant.clone(),
            workspace_id: ctx.workspace.clone(),
            manifest_version: "1.0".to_string(),
            code_version_id: "abc123".to_string(),
            fingerprint: "fp".to_string(),
            git: GitContext::default(),
            assets: vec![AssetEntry {
                key: AssetKey {
                    namespace: "analytics".to_string(),
                    name: "orders".to_string(),
                },
                id: "01HQXYZ123".to_string(),
                description: String::new(),
                owners: vec![],
                tags: serde_json::Value::Null,
                partitioning: serde_json::Value::Null,
                dependencies: vec![],
                code: serde_json::Value::Null,
                checks: vec![],
                execution: serde_json::Value::Null,
                resources: serde_json::Value::Null,
                io: serde_json::Value::Null,
                transform_fingerprint: String::new(),
            }],
            schedules: vec![],
            deployed_at: Utc::now(),
            deployed_by: "test".to_string(),
            metadata: serde_json::Value::Null,
        };

        let manifest_path = crate::paths::manifest_path(&manifest_id);
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&stored_manifest)?),
                WritePrecondition::None,
            )
            .await?;

        storage
            .put_raw(
                MANIFEST_LATEST_INDEX_PATH,
                Bytes::from(serde_json::to_vec(&LatestManifestIndex {
                    latest_manifest_id: "missing".to_string(),
                    deployed_at: stored_manifest.deployed_at,
                })?),
                WritePrecondition::None,
            )
            .await?;

        let request = TriggerRunRequest {
            selection: vec!["analytics/orders".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            run_key: None,
            labels: HashMap::new(),
        };

        let response = trigger_run(
            State(state),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request),
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?
        .into_response();

        assert_eq!(response.status(), StatusCode::CREATED);
        Ok(())
    }

    #[tokio::test]
    async fn test_backfill_run_key_updates_missing_fingerprint() -> Result<()> {
        let cutoff = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let mut config = crate::config::Config::default();
        config.debug = true;
        config.run_key_fingerprint_cutoff = Some(cutoff);
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request = RunKeyBackfillRequest {
            run_key: "daily:2024-01-01".to_string(),
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            labels: HashMap::new(),
        };

        let fingerprint_request = TriggerRunRequest {
            selection: request.selection.clone(),
            include_upstream: request.include_upstream,
            include_downstream: request.include_downstream,
            partitions: request.partitions.clone(),
            run_key: Some(request.run_key.clone()),
            labels: request.labels.clone(),
        };
        let expected_fingerprint =
            build_request_fingerprint(&fingerprint_request).map_err(|err| anyhow!("{err:?}"))?;

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;

        let reservation = RunKeyReservation {
            run_key: request.run_key.clone(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: None,
            created_at: cutoff - Duration::hours(1),
        };

        let reserved =
            reserve_run_key(&storage, &reservation, FingerprintPolicy::lenient()).await?;
        assert!(matches!(reserved, ReservationResult::Reserved));

        let response = backfill_run_key(
            State(state.clone()),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request),
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        let payload: RunKeyBackfillResponse = serde_json::from_slice(&body)?;
        assert!(payload.updated);
        assert_eq!(payload.request_fingerprint, expected_fingerprint);

        let updated = get_reservation(&storage, &payload.run_key).await?.unwrap();
        assert_eq!(updated.request_fingerprint, Some(expected_fingerprint));

        Ok(())
    }

    #[tokio::test]
    async fn test_backfill_run_key_conflicts_on_mismatch() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request_a = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let request_b = RunKeyBackfillRequest {
            run_key: "daily:2024-01-01".to_string(),
            selection: vec!["analytics/orders".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            labels: HashMap::new(),
        };

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;

        let reservation = RunKeyReservation {
            run_key: "daily:2024-01-01".to_string(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(
                build_request_fingerprint(&request_a).map_err(|err| anyhow!("{err:?}"))?,
            ),
            created_at: Utc::now(),
        };

        let reserved =
            reserve_run_key(&storage, &reservation, FingerprintPolicy::lenient()).await?;
        assert!(matches!(reserved, ReservationResult::Reserved));

        let response = match backfill_run_key(
            State(state.clone()),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request_b),
        )
        .await
        {
            Ok(_) => panic!("expected conflict for mismatched fingerprint"),
            Err(err) => err.into_response(),
        };

        assert_eq!(response.status(), StatusCode::CONFLICT);
        Ok(())
    }
}
