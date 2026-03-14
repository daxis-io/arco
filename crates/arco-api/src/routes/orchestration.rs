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
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::{IntoParams, ToSchema};

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::orchestration_compaction::compact_orchestration_events;
use crate::paths::{
    MANIFEST_IDEMPOTENCY_PREFIX, MANIFEST_LATEST_INDEX_PATH, MANIFEST_PREFIX,
    backfill_idempotency_path,
};
use crate::routes::manifests::StoredManifest;
use crate::server::AppState;
use arco_core::{Error as CoreError, ScopedStorage, WritePrecondition, WriteResult};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{
    BackfillChunkRow, BackfillRow, DepResolution, FoldState, MicroCompactor, PartitionStatusRow,
    RunRow, RunState as FoldRunState, ScheduleDefinitionRow, ScheduleStateRow, ScheduleTickRow,
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
    /// Partition overrides (key=value strings). Backward-compatible with older clients.
    #[serde(default)]
    pub partitions: Vec<PartitionValue>,
    /// Canonical partition key string (ADR-011). Preferred; cannot be combined with `partitions`.
    #[schema(min_length = 1)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    /// Idempotency key for deduplication (409 if reused with different payload).
    /// Reservations created before the fingerprint cutoff remain lenient.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the run.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TriggerRunRequestWithPartitionKey {
    /// Asset selection (asset keys to materialize).
    #[serde(default)]
    pub selection: Vec<String>,
    /// Include upstream dependencies of the selection.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the selection.
    #[serde(default)]
    pub include_downstream: bool,
    /// Canonical partition key string (ADR-011).
    #[schema(min_length = 1)]
    pub partition_key: String,
    /// Idempotency key for deduplication (409 if reused with different payload).
    /// Reservations created before the fingerprint cutoff remain lenient.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the run.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TriggerRunRequestWithPartitions {
    /// Asset selection (asset keys to materialize).
    #[serde(default)]
    pub selection: Vec<String>,
    /// Include upstream dependencies of the selection.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the selection.
    #[serde(default)]
    pub include_downstream: bool,
    /// Partition overrides (key=value strings). Backward-compatible with older clients.
    pub partitions: Vec<PartitionValue>,
    /// Idempotency key for deduplication (409 if reused with different payload).
    /// Reservations created before the fingerprint cutoff remain lenient.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the run.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TriggerRunRequestUnpartitioned {
    /// Asset selection (asset keys to materialize).
    #[serde(default)]
    pub selection: Vec<String>,
    /// Include upstream dependencies of the selection.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the selection.
    #[serde(default)]
    pub include_downstream: bool,
    /// Idempotency key for deduplication (409 if reused with different payload).
    /// Reservations created before the fingerprint cutoff remain lenient.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the run.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(untagged)]
pub(crate) enum TriggerRunRequestOpenApi {
    WithPartitionKey(TriggerRunRequestWithPartitionKey),
    WithPartitions(TriggerRunRequestWithPartitions),
    Unpartitioned(TriggerRunRequestUnpartitioned),
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
    /// Deterministic operator-facing reason for this rerun.
    pub rerun_reason: String,
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
    /// Partition overrides (key=value pairs). Backward-compatible with older clients.
    /// Cannot be combined with `partitionKey`.
    #[serde(default)]
    pub partitions: Vec<PartitionValue>,
    /// Canonical partition key string (ADR-011). Preferred; cannot be combined with `partitions`.
    #[schema(min_length = 1)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
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
    /// Deterministic operator-facing reason for rerun creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerun_reason: Option<String>,
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
    /// Deterministic execution lineage reference for successful materializations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_lineage_ref: Option<String>,
    /// Delta table recorded for this materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_table: Option<String>,
    /// Delta version recorded for this materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_version: Option<i64>,
    /// Delta partition recorded for this materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_partition: Option<String>,
    /// Retry attribution for tasks that consumed retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_attribution: Option<TaskRetryAttributionResponse>,
    /// Skip attribution for tasks skipped due to upstream outcomes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_attribution: Option<TaskSkipAttributionResponse>,
}

/// Retry attribution details for a task.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskRetryAttributionResponse {
    /// Number of retries consumed before current attempt.
    pub retries: u32,
    /// Deterministic reason code.
    pub reason: String,
}

/// Skip attribution details for a task.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskSkipAttributionResponse {
    /// Upstream task that caused this skip.
    pub upstream_task_key: String,
    /// Upstream resolution that propagated the skip.
    pub upstream_resolution: String,
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

// Input size limits (defense-in-depth; avoid pathological CPU/memory in validation/fingerprinting).
const MAX_RUN_KEY_LEN: usize = 256;
const MAX_PARTITION_KEY_LEN: usize = 1024;
const MAX_SELECTION_ITEMS: usize = 10_000;
const MAX_SELECTION_ITEM_LEN: usize = 256;
const MAX_LABELS: usize = 128;
const MAX_LABEL_KEY_LEN: usize = 128;
const MAX_LABEL_VALUE_LEN: usize = 2048;
const MAX_PARTITIONS: usize = 128;
const MAX_PARTITION_DIM_LEN: usize = 64;
const MAX_PARTITION_VALUE_LEN: usize = 256;

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

/// Sensor evaluation status filter (query parameter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SensorEvalStatusFilter {
    /// Sensor triggered one or more runs.
    Triggered,
    /// Sensor evaluated but no new data found.
    NoNewData,
    /// Sensor evaluation failed.
    Error,
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

/// Query parameters for listing sensor evaluations.
#[derive(Debug, Default, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListSensorEvalsQuery {
    /// Maximum evaluations to return.
    pub limit: Option<u32>,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Filter evaluations after this timestamp (inclusive).
    pub since: Option<DateTime<Utc>>,
    /// Filter evaluations before this timestamp (inclusive).
    pub until: Option<DateTime<Utc>>,
    /// Filter by evaluation status.
    pub status: Option<SensorEvalStatusFilter>,
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
        /// Filter bounds. Supported keys: `start`, `end`, `partition_start`, `partition_end`.
        filters: HashMap<String, String>,
    },
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
        /// Filter bounds (`start`/`end`) captured on the backfill.
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
    /// Delta table recorded for the latest successful materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_table: Option<String>,
    /// Delta version recorded for the latest successful materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_version: Option<i64>,
    /// Delta partition recorded for the latest successful materialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_partition: Option<String>,
    /// Deterministic execution lineage reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_lineage_ref: Option<String>,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestManifestIndex {
    latest_manifest_id: String,
    deployed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct PartitioningSpec {
    is_partitioned: bool,
    dimensions: Vec<DimensionSpec>,
}

impl PartitioningSpec {
    fn from_value(value: &serde_json::Value) -> Self {
        let Some(obj) = value.as_object() else {
            return Self::default();
        };

        let dimensions: Vec<DimensionSpec> = obj
            .get("dimensions")
            .and_then(|dims| dims.as_array())
            .map(|dims| dims.iter().filter_map(DimensionSpec::from_value).collect())
            .unwrap_or_default();

        let is_partitioned_value = obj
            .get("is_partitioned")
            .or_else(|| obj.get("isPartitioned"))
            .and_then(serde_json::Value::as_bool);
        let is_partitioned = if is_partitioned_value == Some(false) && !dimensions.is_empty() {
            let dimension_names = dimensions
                .iter()
                .map(|dim| dim.name.clone())
                .collect::<Vec<_>>();
            tracing::warn!(
                dimensions = ?dimension_names,
                "manifest partitioning marked unpartitioned despite dimensions; treating as partitioned"
            );
            true
        } else {
            is_partitioned_value.unwrap_or(!dimensions.is_empty())
        };

        Self {
            is_partitioned,
            dimensions,
        }
    }

    fn matches(&self, other: &Self) -> bool {
        if self.is_partitioned != other.is_partitioned {
            return false;
        }
        if !self.is_partitioned {
            return true;
        }
        partitioning_signature(self) == partitioning_signature(other)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DimensionSignature {
    kind: String,
    granularity: Option<String>,
    values: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DimensionSpec {
    name: String,
    kind: String,
    granularity: Option<String>,
    values: Option<Vec<String>>,
}

impl DimensionSpec {
    fn from_value(value: &serde_json::Value) -> Option<Self> {
        let obj = value.as_object()?;
        let name = obj.get("name")?.as_str()?.to_string();
        let kind = obj.get("kind")?.as_str()?.to_string();
        let granularity = obj
            .get("granularity")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        let values = obj
            .get("values")
            .and_then(|value| value.as_array())
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            });

        Some(Self {
            name,
            kind,
            granularity,
            values,
        })
    }

    fn signature(&self) -> DimensionSignature {
        let values = self
            .values
            .as_ref()
            .filter(|values| !values.is_empty())
            .map(|values| values.iter().cloned().collect::<BTreeSet<_>>());

        DimensionSignature {
            kind: self.kind.clone(),
            granularity: self.granularity.clone(),
            values,
        }
    }
}

fn partitioning_signature(spec: &PartitioningSpec) -> BTreeMap<String, DimensionSignature> {
    spec.dimensions
        .iter()
        .map(|dim| (dim.name.clone(), dim.signature()))
        .collect()
}

#[derive(Debug, Clone)]
struct ResolvedPartitionKey {
    canonical: Option<String>,
    legacy_canonical: Option<String>,
}

impl ResolvedPartitionKey {
    fn canonical(&self) -> Option<&str> {
        self.canonical.as_deref()
    }
}

struct ManifestContext {
    manifest_id: String,
    deployed_at: DateTime<Utc>,
    graph: arco_flow::orchestration::AssetGraph,
    known_assets: HashSet<String>,
    partitioning_specs: HashMap<String, PartitioningSpec>,
}

struct RunPlanContext {
    manifest_id: String,
    deployed_at: DateTime<Utc>,
    graph: arco_flow::orchestration::AssetGraph,
    root_assets: Vec<String>,
    partitioning_spec: PartitioningSpec,
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
        if !std::path::Path::new(path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
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

fn build_manifest_context(stored: &StoredManifest) -> Result<ManifestContext, ApiError> {
    let mut known_assets = HashSet::new();
    let mut manifest_assets = Vec::with_capacity(stored.assets.len());
    let mut partitioning_specs = HashMap::with_capacity(stored.assets.len());

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
        manifest_assets.push((key.clone(), asset.dependencies.as_slice()));
        partitioning_specs.insert(key, PartitioningSpec::from_value(&asset.partitioning));
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

    Ok(ManifestContext {
        manifest_id: stored.manifest_id.clone(),
        deployed_at: stored.deployed_at,
        graph,
        known_assets,
        partitioning_specs,
    })
}

fn resolve_root_assets(
    request: &TriggerRunRequest,
    known_assets: &HashSet<String>,
) -> Result<Vec<String>, ApiError> {
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

    Ok(roots.into_iter().collect())
}

fn derive_run_partitioning_spec(
    root_assets: &[String],
    partitioning_specs: &HashMap<String, PartitioningSpec>,
    has_partition_request: bool,
) -> Result<PartitioningSpec, ApiError> {
    let mut base_spec: Option<PartitioningSpec> = None;

    for root in root_assets {
        let spec = partitioning_specs.get(root).cloned().unwrap_or_default();

        if has_partition_request && !spec.is_partitioned {
            return Err(ApiError::bad_request(format!(
                "partition key provided for unpartitioned asset: {root}"
            )));
        }

        if let Some(existing) = base_spec.as_ref() {
            if has_partition_request && !existing.matches(&spec) {
                return Err(ApiError::bad_request(
                    "partitioned root assets must share identical partitioning".to_string(),
                ));
            }
        } else {
            base_spec = Some(spec);
        }
    }

    Ok(base_spec.unwrap_or_default())
}

async fn load_run_plan_context(
    storage: &ScopedStorage,
    request: &TriggerRunRequest,
) -> Result<RunPlanContext, ApiError> {
    let stored = load_latest_manifest(storage).await?;
    let Some(stored) = stored else {
        return Err(ApiError::bad_request(
            "no manifest deployed for workspace; deploy a manifest before triggering a run",
        ));
    };

    let manifest_context = build_manifest_context(&stored)?;
    let root_assets = resolve_root_assets(request, &manifest_context.known_assets)?;
    let has_partition_request = request.partition_key.is_some() || !request.partitions.is_empty();
    let partitioning_spec = derive_run_partitioning_spec(
        &root_assets,
        &manifest_context.partitioning_specs,
        has_partition_request,
    )?;

    Ok(RunPlanContext {
        manifest_id: manifest_context.manifest_id,
        deployed_at: manifest_context.deployed_at,
        graph: manifest_context.graph,
        root_assets,
        partitioning_spec,
    })
}

fn build_task_defs_for_request(
    context: &RunPlanContext,
    request: &TriggerRunRequest,
    partition_key: Option<&str>,
) -> Result<Vec<TaskDef>, ApiError> {
    let options = arco_flow::orchestration::SelectionOptions {
        include_upstream: request.include_upstream,
        include_downstream: request.include_downstream,
    };

    arco_flow::orchestration::build_task_defs_for_selection(
        &context.graph,
        &context.root_assets,
        options,
        partition_key,
    )
    .map_err(ApiError::bad_request)
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
    partition_selector: BackfillFingerprintPartitionSelector,
    chunk_size: u32,
    max_concurrent_runs: u32,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BackfillFingerprintPartitionSelector {
    Range { start: String, end: String },
    Explicit { partition_keys: Vec<String> },
    Filter { filters: BTreeMap<String, String> },
}

impl From<&PartitionSelector> for BackfillFingerprintPartitionSelector {
    fn from(selector: &PartitionSelector) -> Self {
        match selector {
            PartitionSelector::Range { start, end } => Self::Range {
                start: start.clone(),
                end: end.clone(),
            },
            PartitionSelector::Explicit { partition_keys } => Self::Explicit {
                partition_keys: partition_keys.clone(),
            },
            PartitionSelector::Filter { filters } => Self::Filter {
                filters: filters
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyBackfillFingerprintPayload {
    asset_selection: Vec<String>,
    partitions: Vec<String>,
    chunk_size: u32,
    max_concurrent_runs: u32,
}

struct BackfillCreateInput {
    asset_selection: Vec<String>,
    partition_selector: PartitionSelector,
    total_partitions: u32,
    chunk_size: u32,
    max_concurrent_runs: u32,
}

fn compute_backfill_fingerprint(
    asset_selection: &[String],
    partition_selector: &PartitionSelector,
    chunk_size: u32,
    max_concurrent_runs: u32,
) -> Result<String, ApiError> {
    let payload = BackfillFingerprintPayload {
        asset_selection: asset_selection.to_vec(),
        partition_selector: partition_selector.into(),
        chunk_size,
        max_concurrent_runs,
    };
    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!("failed to serialize backfill fingerprint: {e}"))
    })?;
    let hash = Sha256::digest(&json);
    Ok(hex::encode(hash))
}

fn compute_legacy_backfill_fingerprint(
    asset_selection: &[String],
    partition_selector: &PartitionSelector,
    chunk_size: u32,
    max_concurrent_runs: u32,
) -> Result<Option<String>, ApiError> {
    let PartitionSelector::Explicit { partition_keys } = partition_selector else {
        return Ok(None);
    };
    let payload = LegacyBackfillFingerprintPayload {
        asset_selection: asset_selection.to_vec(),
        partitions: partition_keys.clone(),
        chunk_size,
        max_concurrent_runs,
    };
    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!(
            "failed to serialize legacy backfill fingerprint: {e}"
        ))
    })?;
    let hash = Sha256::digest(&json);
    Ok(Some(hex::encode(hash)))
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
) -> Result<WriteResult, ApiError> {
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

    Ok(result)
}

fn parse_partition_bound_date(raw: &str) -> Result<NaiveDate, ApiError> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map_err(|_| ApiError::bad_request("partition selector bounds must use YYYY-MM-DD"))
}

fn normalize_partition_bounds(start: &str, end: &str) -> Result<(String, String), ApiError> {
    let start_date = parse_partition_bound_date(start)?;
    let end_date = parse_partition_bound_date(end)?;
    if start_date > end_date {
        return Err(ApiError::bad_request(
            "partition selector start must be less than or equal to end",
        ));
    }
    Ok((
        start_date.format("%Y-%m-%d").to_string(),
        end_date.format("%Y-%m-%d").to_string(),
    ))
}

fn normalize_filter_bounds(
    filters: &HashMap<String, String>,
) -> Result<(String, String), ApiError> {
    const START: &str = "start";
    const END: &str = "end";
    const PARTITION_START: &str = "partition_start";
    const PARTITION_END: &str = "partition_end";

    if filters
        .keys()
        .any(|key| !matches!(key.as_str(), START | END | PARTITION_START | PARTITION_END))
    {
        return Err(ApiError::bad_request(
            "filter selector supports only start/end bounds",
        ));
    }

    let start = filters.get(START).map(|value| value.trim());
    let start_alias = filters.get(PARTITION_START).map(|value| value.trim());
    let end = filters.get(END).map(|value| value.trim());
    let end_alias = filters.get(PARTITION_END).map(|value| value.trim());

    if let (Some(primary), Some(alias)) = (start, start_alias) {
        if primary != alias {
            return Err(ApiError::bad_request(
                "filter selector start bounds are conflicting",
            ));
        }
    }
    if let (Some(primary), Some(alias)) = (end, end_alias) {
        if primary != alias {
            return Err(ApiError::bad_request(
                "filter selector end bounds are conflicting",
            ));
        }
    }

    let start = start
        .or(start_alias)
        .ok_or_else(|| ApiError::bad_request("filter selector requires start/end bounds"))?;
    let end = end
        .or(end_alias)
        .ok_or_else(|| ApiError::bad_request("filter selector requires start/end bounds"))?;
    normalize_partition_bounds(start, end)
}

fn parse_partition_selector(
    selector: PartitionSelectorRequest,
) -> Result<(PartitionSelector, u32), ApiError> {
    match selector {
        PartitionSelectorRequest::Explicit { partitions } => {
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
                u32::try_from(partitions.len()).unwrap_or(u32::MAX),
            ))
        }
        PartitionSelectorRequest::Range { start, end } => {
            if start.trim().is_empty() || end.trim().is_empty() {
                return Err(ApiError::bad_request(
                    "range selector requires start and end",
                ));
            }
            let (start, end) = normalize_partition_bounds(start.trim(), end.trim())?;
            Ok((PartitionSelector::Range { start, end }, 0))
        }
        PartitionSelectorRequest::Filter { filters } => {
            if filters.is_empty() {
                return Err(ApiError::bad_request("filter selector cannot be empty"));
            }
            if filters
                .iter()
                .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
            {
                return Err(ApiError::bad_request(
                    "filter selector keys and values must be non-empty",
                ));
            }
            let (start, end) = normalize_filter_bounds(&filters)?;
            let filters = HashMap::from([("start".to_string(), start), ("end".to_string(), end)]);
            Ok((PartitionSelector::Filter { filters }, 0))
        }
    }
}

async fn resolve_backfill_idempotency(
    storage: &ScopedStorage,
    idempotency_key: &str,
    fingerprint: &str,
    compatibility_fingerprints: &[String],
) -> Result<Option<CreateBackfillResponse>, ApiError> {
    if let Some(existing) = load_backfill_idempotency_record(storage, idempotency_key).await? {
        if existing.fingerprint == fingerprint
            || compatibility_fingerprints
                .iter()
                .any(|value| value == &existing.fingerprint)
        {
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
    let event = OrchestrationEvent::new(
        &ctx.tenant,
        workspace_id,
        OrchestrationEventData::BackfillCreated {
            backfill_id: backfill_id.clone(),
            client_request_id: idempotency_key.to_string(),
            asset_selection: input.asset_selection,
            partition_selector: input.partition_selector,
            total_partitions: input.total_partitions,
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

    let store_result = store_backfill_idempotency_record(storage, &record).await?;
    if matches!(store_result, WriteResult::PreconditionFailed { .. }) {
        if let Some(existing) =
            resolve_backfill_idempotency(storage, idempotency_key, &record.fingerprint, &[]).await?
        {
            return Ok(existing);
        }
        return Err(ApiError::conflict(
            "idempotency key was claimed concurrently; retry request",
        ));
    }

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

fn rerun_reason_for_kind(kind: RerunKindResponse) -> &'static str {
    match kind {
        RerunKindResponse::FromFailure => "from_failure_unsucceeded_tasks",
        RerunKindResponse::Subset => "subset_selection",
    }
}

fn task_retry_attribution(task: &TaskRow) -> Option<TaskRetryAttributionResponse> {
    if task.attempt <= 1 {
        return None;
    }

    Some(TaskRetryAttributionResponse {
        retries: task.attempt.saturating_sub(1),
        reason: "prior_attempt_failed".to_string(),
    })
}

fn dep_resolution_str(resolution: DepResolution) -> &'static str {
    match resolution {
        DepResolution::Success => "SUCCESS",
        DepResolution::Failed => "FAILED",
        DepResolution::Skipped => "SKIPPED",
        DepResolution::Cancelled => "CANCELLED",
    }
}

fn build_skip_attribution_index(
    fold_state: &FoldState,
    run_id: &str,
) -> HashMap<String, TaskSkipAttributionResponse> {
    let mut first_causes: HashMap<String, (i64, String, String, DepResolution)> = HashMap::new();

    for edge in fold_state
        .dep_satisfaction
        .values()
        .filter(|edge| edge.run_id == run_id)
    {
        let Some(resolution) = edge.resolution else {
            continue;
        };
        if matches!(resolution, DepResolution::Success) {
            continue;
        }

        let satisfied_at = edge.satisfied_at.map_or(0, |t| t.timestamp_millis());
        let upstream_task_key = edge.upstream_task_key.clone();
        let row_version = edge.row_version.clone();

        let replace = first_causes
            .get(&edge.downstream_task_key)
            .is_none_or(|current| {
                (
                    satisfied_at,
                    upstream_task_key.as_str(),
                    row_version.as_str(),
                ) < (current.0, current.1.as_str(), current.2.as_str())
            });

        if replace {
            first_causes.insert(
                edge.downstream_task_key.clone(),
                (satisfied_at, upstream_task_key, row_version, resolution),
            );
        }
    }

    first_causes
        .into_iter()
        .map(
            |(downstream_task_key, (_, upstream_task_key, _, resolution))| {
                (
                    downstream_task_key,
                    TaskSkipAttributionResponse {
                        upstream_task_key,
                        upstream_resolution: dep_resolution_str(resolution).to_string(),
                    },
                )
            },
        )
        .collect()
}

fn task_skip_attribution(
    task: &TaskRow,
    skip_attribution_index: &HashMap<String, TaskSkipAttributionResponse>,
) -> Option<TaskSkipAttributionResponse> {
    if task.state != FoldTaskState::Skipped {
        return None;
    }

    skip_attribution_index
        .get(&task.task_key)
        .map(|attr| TaskSkipAttributionResponse {
            upstream_task_key: attr.upstream_task_key.clone(),
            upstream_resolution: attr.upstream_resolution.clone(),
        })
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

fn validate_selection_limits(selection: &[String]) -> Result<(), ApiError> {
    if selection.len() > MAX_SELECTION_ITEMS {
        return Err(ApiError::bad_request(format!(
            "selection exceeds max items ({MAX_SELECTION_ITEMS})"
        )));
    }
    for value in selection {
        if value.len() > MAX_SELECTION_ITEM_LEN {
            return Err(ApiError::bad_request(format!(
                "selection item exceeds max length ({MAX_SELECTION_ITEM_LEN})"
            )));
        }
    }
    Ok(())
}

fn validate_labels_limits(labels: &HashMap<String, String>) -> Result<(), ApiError> {
    if labels.len() > MAX_LABELS {
        return Err(ApiError::bad_request(format!(
            "labels exceed max properties ({MAX_LABELS})"
        )));
    }

    for (key, value) in labels {
        if key.len() > MAX_LABEL_KEY_LEN {
            return Err(ApiError::bad_request(format!(
                "label key exceeds max length ({MAX_LABEL_KEY_LEN})"
            )));
        }
        if value.len() > MAX_LABEL_VALUE_LEN {
            return Err(ApiError::bad_request(format!(
                "label value exceeds max length ({MAX_LABEL_VALUE_LEN})"
            )));
        }
    }

    Ok(())
}

fn validate_partitions_limits(partitions: &[PartitionValue]) -> Result<(), ApiError> {
    if partitions.len() > MAX_PARTITIONS {
        return Err(ApiError::bad_request(format!(
            "partitions exceed max items ({MAX_PARTITIONS})"
        )));
    }

    for partition in partitions {
        if partition.key.len() > MAX_PARTITION_DIM_LEN {
            return Err(ApiError::bad_request(format!(
                "partition key exceeds max length ({MAX_PARTITION_DIM_LEN})"
            )));
        }
        if partition.value.len() > MAX_PARTITION_VALUE_LEN {
            return Err(ApiError::bad_request(format!(
                "partition value exceeds max length ({MAX_PARTITION_VALUE_LEN})"
            )));
        }
    }

    Ok(())
}

fn validate_trigger_run_request_limits(request: &TriggerRunRequest) -> Result<(), ApiError> {
    validate_selection_limits(&request.selection)?;
    validate_labels_limits(&request.labels)?;
    validate_partitions_limits(&request.partitions)?;

    if let Some(run_key) = request.run_key.as_deref() {
        if run_key.len() > MAX_RUN_KEY_LEN {
            return Err(ApiError::bad_request(format!(
                "runKey exceeds max length ({MAX_RUN_KEY_LEN})"
            )));
        }
    }

    if let Some(partition_key) = request.partition_key.as_deref() {
        if partition_key.len() > MAX_PARTITION_KEY_LEN {
            return Err(ApiError::bad_request(format!(
                "partitionKey exceeds max length ({MAX_PARTITION_KEY_LEN})"
            )));
        }
    }

    Ok(())
}

fn parse_partition_values(
    partitions: &[PartitionValue],
) -> Result<BTreeMap<String, String>, ApiError> {
    if partitions.is_empty() {
        return Ok(BTreeMap::new());
    }

    if partitions.len() > MAX_PARTITIONS {
        return Err(ApiError::bad_request(format!(
            "partitions exceed max items ({MAX_PARTITIONS})"
        )));
    }

    let mut seen = HashSet::new();
    let mut values = BTreeMap::new();

    for partition in partitions {
        let key = partition.key.trim();
        if key.is_empty() {
            return Err(ApiError::bad_request("partition key cannot be empty"));
        }
        if key.len() > MAX_PARTITION_DIM_LEN {
            return Err(ApiError::bad_request(format!(
                "partition key exceeds max length ({MAX_PARTITION_DIM_LEN})"
            )));
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

        if partition.value.len() > MAX_PARTITION_VALUE_LEN {
            return Err(ApiError::bad_request(format!(
                "partition value exceeds max length ({MAX_PARTITION_VALUE_LEN})"
            )));
        }

        values.insert(key.to_string(), partition.value.clone());
    }

    Ok(values)
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

fn resolve_partition_key(
    request: &TriggerRunRequest,
    partitioning: &PartitioningSpec,
    cutoff: Option<DateTime<Utc>>,
) -> Result<ResolvedPartitionKey, ApiError> {
    if request.partition_key.is_some() && !request.partitions.is_empty() {
        return Err(ApiError::bad_request(
            "use either partitionKey or partitions, not both",
        ));
    }

    let has_partition_request = request.partition_key.is_some() || !request.partitions.is_empty();
    if has_partition_request && !partitioning.is_partitioned {
        return Err(ApiError::bad_request(
            "partition key provided for unpartitioned assets",
        ));
    }

    if let Some(raw) = request.partition_key.as_deref() {
        if raw.is_empty() {
            return Err(ApiError::bad_request("partitionKey cannot be empty"));
        }

        let parsed = arco_core::partition::PartitionKey::parse(raw)
            .map_err(|err| ApiError::bad_request(format!("invalid partitionKey: {err}")))?;
        let canonical = parsed.canonical_string();
        if canonical != raw {
            return Err(ApiError::bad_request(format!(
                "partitionKey must be canonical: expected '{canonical}'"
            )));
        }

        let raw_dimensions = parsed
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        let (normalized_key, normalized) =
            normalize_partition_key(&raw_dimensions, partitioning, cutoff)?;
        let normalized_canonical = normalized_key.canonical_string();

        // Validate that normalization produced a fully canonical, ADR-011 compliant key.
        arco_core::partition::PartitionKey::parse(&normalized_canonical)
            .map_err(|err| ApiError::bad_request(format!("invalid partitionKey: {err}")))?;

        if normalized_canonical.len() > MAX_PARTITION_KEY_LEN {
            return Err(ApiError::bad_request(format!(
                "partitionKey exceeds max length ({MAX_PARTITION_KEY_LEN})"
            )));
        }

        let legacy_canonical = if normalized && normalized_canonical != raw {
            Some(raw.to_string())
        } else {
            None
        };

        return Ok(ResolvedPartitionKey {
            canonical: Some(normalized_canonical),
            legacy_canonical,
        });
    }

    let raw_values = parse_partition_values(&request.partitions)?;
    if raw_values.is_empty() {
        return Ok(ResolvedPartitionKey {
            canonical: None,
            legacy_canonical: None,
        });
    }

    let raw_dimensions = raw_values
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                arco_core::partition::ScalarValue::String(value.clone()),
            )
        })
        .collect::<HashMap<_, _>>();
    let (normalized_key, normalized) =
        normalize_partition_key(&raw_dimensions, partitioning, cutoff)?;
    let normalized_canonical = normalized_key.canonical_string();

    // Validate that normalization produced a fully canonical, ADR-011 compliant key.
    arco_core::partition::PartitionKey::parse(&normalized_canonical)
        .map_err(|err| ApiError::bad_request(format!("invalid partitionKey: {err}")))?;

    if normalized_canonical.len() > MAX_PARTITION_KEY_LEN {
        return Err(ApiError::bad_request(format!(
            "partitionKey exceeds max length ({MAX_PARTITION_KEY_LEN})"
        )));
    }

    let legacy_canonical = if normalized {
        let legacy = build_partition_key(&request.partitions)?;
        legacy.filter(|legacy| legacy != &normalized_canonical)
    } else {
        None
    };

    Ok(ResolvedPartitionKey {
        canonical: Some(normalized_canonical),
        legacy_canonical,
    })
}

fn build_partition_key(partitions: &[PartitionValue]) -> Result<Option<String>, ApiError> {
    let values = parse_partition_values(partitions)?;
    if values.is_empty() {
        return Ok(None);
    }

    let mut partition_key = arco_core::partition::PartitionKey::new();
    for (key, value) in values {
        partition_key.insert(key, arco_core::partition::ScalarValue::String(value));
    }

    let canonical = partition_key.canonical_string();
    if canonical.len() > MAX_PARTITION_KEY_LEN {
        return Err(ApiError::bad_request(format!(
            "partitionKey exceeds max length ({MAX_PARTITION_KEY_LEN})"
        )));
    }

    Ok(Some(canonical))
}

fn normalize_partition_key(
    raw_dimensions: &HashMap<String, arco_core::partition::ScalarValue>,
    partitioning: &PartitioningSpec,
    cutoff: Option<DateTime<Utc>>,
) -> Result<(arco_core::partition::PartitionKey, bool), ApiError> {
    let provided_keys: HashSet<&str> = raw_dimensions.keys().map(String::as_str).collect();
    let expected_keys: HashSet<&str> = partitioning
        .dimensions
        .iter()
        .map(|dim| dim.name.as_str())
        .collect();

    if partitioning.is_partitioned && provided_keys != expected_keys {
        let expected = partitioning
            .dimensions
            .iter()
            .map(|dim| dim.name.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        let provided = provided_keys
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::bad_request(format!(
            "partitionKey dimensions must match manifest dimensions: expected [{expected}], got [{provided}]"
        )));
    }

    let cutoff_active = cutoff.is_some_and(|cutoff| Utc::now() >= cutoff);
    let mut normalized = arco_core::partition::PartitionKey::new();
    let mut normalized_time_string = false;

    for dim in &partitioning.dimensions {
        let Some(value) = raw_dimensions.get(&dim.name) else {
            continue;
        };

        let (normalized_value, did_normalize) = match dim.kind.as_str() {
            "time" => normalize_time_dimension_value(dim, value, cutoff_active)?,
            "static" => normalize_static_dimension_value(dim, value)?,
            "tenant" => normalize_tenant_dimension_value(dim, value)?,
            _ => {
                return Err(ApiError::bad_request(format!(
                    "unsupported partition dimension kind '{}' for '{}'",
                    dim.kind, dim.name
                )));
            }
        };

        if did_normalize {
            normalized_time_string = true;
        }

        normalized.insert(dim.name.clone(), normalized_value);
    }

    Ok((normalized, normalized_time_string))
}

fn normalize_time_dimension_value(
    dim: &DimensionSpec,
    value: &arco_core::partition::ScalarValue,
    cutoff_active: bool,
) -> Result<(arco_core::partition::ScalarValue, bool), ApiError> {
    match value {
        arco_core::partition::ScalarValue::Date(date) => {
            enforce_time_granularity(dim, "day")?;
            Ok((arco_core::partition::ScalarValue::Date(date.clone()), false))
        }
        arco_core::partition::ScalarValue::Timestamp(timestamp) => {
            enforce_time_granularity(dim, "hour")?;

            if dim.granularity.as_deref() == Some("hour") {
                // Hour-partitioned dimensions must be aligned to the hour.
                let parsed = DateTime::parse_from_rfc3339(timestamp).map_err(|_| {
                    ApiError::bad_request(format!(
                        "time dimension '{}' must use canonical UTC timestamp",
                        dim.name
                    ))
                })?;
                let utc = parsed.with_timezone(&Utc);
                if utc.minute() != 0 || utc.second() != 0 || utc.nanosecond() != 0 {
                    return Err(ApiError::bad_request(format!(
                        "time dimension '{}' must be aligned to the hour",
                        dim.name
                    )));
                }
            }

            Ok((
                arco_core::partition::ScalarValue::Timestamp(timestamp.clone()),
                false,
            ))
        }
        arco_core::partition::ScalarValue::String(raw) => {
            if cutoff_active {
                return Err(ApiError::bad_request(format!(
                    "time partition values must use d:/t: tags for '{}'",
                    dim.name
                )));
            }

            let normalized = match dim.granularity.as_deref() {
                Some("day") => {
                    let parsed = parse_date(raw).ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "time dimension '{}' expects YYYY-MM-DD",
                            dim.name
                        ))
                    })?;
                    arco_core::partition::ScalarValue::Date(parsed)
                }
                Some("hour") => {
                    let parsed = parse_rfc3339_timestamp(raw, true).ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "time dimension '{}' expects RFC3339 timestamp",
                            dim.name
                        ))
                    })?;
                    arco_core::partition::ScalarValue::Timestamp(parsed)
                }
                Some(granularity) => {
                    return Err(ApiError::bad_request(format!(
                        "unsupported time granularity '{}' for '{}'",
                        granularity, dim.name
                    )));
                }
                None => {
                    if let Some(parsed) = parse_date(raw) {
                        arco_core::partition::ScalarValue::Date(parsed)
                    } else if let Some(parsed) = parse_rfc3339_timestamp(raw, false) {
                        arco_core::partition::ScalarValue::Timestamp(parsed)
                    } else {
                        return Err(ApiError::bad_request(format!(
                            "time dimension '{}' expects YYYY-MM-DD or RFC3339 timestamp",
                            dim.name
                        )));
                    }
                }
            };

            tracing::warn!(
                dimension = %dim.name,
                value = %raw,
                "normalized time partition string to tagged value"
            );

            Ok((normalized, true))
        }
        _ => Err(ApiError::bad_request(format!(
            "time dimension '{}' must use d:/t: or s: value",
            dim.name
        ))),
    }
}

fn normalize_static_dimension_value(
    dim: &DimensionSpec,
    value: &arco_core::partition::ScalarValue,
) -> Result<(arco_core::partition::ScalarValue, bool), ApiError> {
    let arco_core::partition::ScalarValue::String(raw) = value else {
        return Err(ApiError::bad_request(format!(
            "dimension '{}' must use string values",
            dim.name
        )));
    };

    if let Some(values) = dim.values.as_ref().filter(|values| !values.is_empty()) {
        if !values.contains(raw) {
            return Err(ApiError::bad_request(format!(
                "dimension '{}' value '{}' is not allowed",
                dim.name, raw
            )));
        }
    }

    Ok((
        arco_core::partition::ScalarValue::String(raw.clone()),
        false,
    ))
}

fn normalize_tenant_dimension_value(
    dim: &DimensionSpec,
    value: &arco_core::partition::ScalarValue,
) -> Result<(arco_core::partition::ScalarValue, bool), ApiError> {
    let arco_core::partition::ScalarValue::String(raw) = value else {
        return Err(ApiError::bad_request(format!(
            "dimension '{}' must use string values",
            dim.name
        )));
    };

    Ok((
        arco_core::partition::ScalarValue::String(raw.clone()),
        false,
    ))
}

fn enforce_time_granularity(dim: &DimensionSpec, expected: &str) -> Result<(), ApiError> {
    match dim.granularity.as_deref() {
        Some(granularity) if granularity == expected => Ok(()),
        Some(_granularity) => Err(ApiError::bad_request(format!(
            "time dimension '{}' expects {} granularity",
            dim.name, expected
        ))),
        None => Ok(()),
    }
}

fn parse_date(value: &str) -> Option<String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .map(|date| date.format("%Y-%m-%d").to_string())
}

fn parse_rfc3339_timestamp(value: &str, require_hour_boundary: bool) -> Option<String> {
    let parsed = DateTime::parse_from_rfc3339(value).ok()?;
    let utc = parsed.with_timezone(&Utc);

    if require_hour_boundary && (utc.minute() != 0 || utc.second() != 0 || utc.nanosecond() != 0) {
        return None;
    }

    Some(utc.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
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

fn build_request_fingerprint(
    request: &TriggerRunRequest,
    resolved_partition_key: &ResolvedPartitionKey,
) -> Result<String, ApiError> {
    #[derive(Serialize)]
    struct FingerprintPayload {
        selection: Vec<String>,
        include_upstream: bool,
        include_downstream: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_key: Option<String>,
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

    let partition_key = resolved_partition_key.canonical.clone();

    let labels = request
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    let payload = FingerprintPayload {
        selection,
        include_upstream: request.include_upstream,
        include_downstream: request.include_downstream,
        partition_key,
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

#[allow(clippy::too_many_lines)]
fn build_request_fingerprint_variants(
    request: &TriggerRunRequest,
    resolved_partition_key: &ResolvedPartitionKey,
) -> Result<(String, Vec<String>), ApiError> {
    #[derive(Serialize)]
    struct FingerprintPayload {
        selection: Vec<String>,
        include_upstream: bool,
        include_downstream: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_key: Option<String>,
        labels: BTreeMap<String, String>,
    }

    #[derive(Serialize)]
    struct LegacyFingerprintPayload {
        selection: Vec<String>,
        include_upstream: bool,
        include_downstream: bool,
        partitions: Vec<PartitionValue>,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_key: Option<String>,
        labels: BTreeMap<String, String>,
    }

    fn hash_payload<T: Serialize>(payload: &T) -> Result<String, ApiError> {
        let json = serde_json::to_vec(payload).map_err(|e| {
            ApiError::internal(format!(
                "failed to serialize trigger request fingerprint: {e}"
            ))
        })?;
        let hash = Sha256::digest(&json);
        Ok(hex::encode(hash))
    }

    let primary = build_request_fingerprint(request, resolved_partition_key)?;

    let mut selection: Vec<String> = request
        .selection
        .iter()
        .map(|value| {
            arco_flow::orchestration::canonicalize_asset_key(value).map_err(ApiError::bad_request)
        })
        .collect::<Result<_, _>>()?;
    selection.sort();
    selection.dedup();

    let labels = request
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    let partition_key = resolved_partition_key.canonical.as_ref();
    let legacy_partition_key = resolved_partition_key.legacy_canonical.as_ref();
    let mut partition_key_variants: Vec<String> = Vec::new();
    if let Some(pk) = partition_key {
        partition_key_variants.push(pk.clone());
        partition_key_variants.extend(time_string_partition_key_variants(pk)?);
    }
    if let Some(pk) = legacy_partition_key {
        partition_key_variants.push(pk.clone());
    }
    partition_key_variants.sort();
    partition_key_variants.dedup();

    let mut variants = vec![primary.clone()];

    for partition_key in &partition_key_variants {
        let fingerprint_payload = FingerprintPayload {
            selection: selection.clone(),
            include_upstream: request.include_upstream,
            include_downstream: request.include_downstream,
            partition_key: Some(partition_key.clone()),
            labels: labels.clone(),
        };
        variants.push(hash_payload(&fingerprint_payload)?);

        let legacy_partition_key_payload = LegacyFingerprintPayload {
            selection: selection.clone(),
            include_upstream: request.include_upstream,
            include_downstream: request.include_downstream,
            partitions: Vec::new(),
            partition_key: Some(partition_key.clone()),
            labels: labels.clone(),
        };
        variants.push(hash_payload(&legacy_partition_key_payload)?);
    }

    if request.partitions.is_empty() {
        for partition_key in &partition_key_variants {
            let Some(mut partitions) = partitions_from_string_only_partition_key(partition_key)?
            else {
                continue;
            };

            partitions.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

            let legacy_partitions_payload = LegacyFingerprintPayload {
                selection: selection.clone(),
                include_upstream: request.include_upstream,
                include_downstream: request.include_downstream,
                partitions,
                partition_key: None,
                labels: labels.clone(),
            };
            variants.push(hash_payload(&legacy_partitions_payload)?);
        }
    } else {
        let mut partitions = request.partitions.clone();
        partitions.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

        let legacy_partitions_payload = LegacyFingerprintPayload {
            selection,
            include_upstream: request.include_upstream,
            include_downstream: request.include_downstream,
            partitions,
            partition_key: None,
            labels,
        };
        variants.push(hash_payload(&legacy_partitions_payload)?);
    }

    variants.sort();
    variants.dedup();

    Ok((primary, variants))
}

fn time_string_partition_key_variants(partition_key: &str) -> Result<Vec<String>, ApiError> {
    let parsed = arco_core::partition::PartitionKey::parse(partition_key).map_err(|err| {
        ApiError::internal(format!(
            "failed to parse canonical partitionKey for fingerprint normalization: {err}"
        ))
    })?;

    let mut base = arco_core::partition::PartitionKey::new();
    let mut trimmed_timestamp_keys: Vec<(String, String)> = Vec::new();

    for (key, value) in parsed.iter() {
        match value {
            arco_core::partition::ScalarValue::String(s) => {
                base.insert(
                    key.clone(),
                    arco_core::partition::ScalarValue::String(s.clone()),
                );
            }
            arco_core::partition::ScalarValue::Date(date) => {
                base.insert(
                    key.clone(),
                    arco_core::partition::ScalarValue::String(date.clone()),
                );
            }
            arco_core::partition::ScalarValue::Timestamp(timestamp) => {
                base.insert(
                    key.clone(),
                    arco_core::partition::ScalarValue::String(timestamp.clone()),
                );
                if let Some(prefix) = timestamp.strip_suffix(".000000Z") {
                    trimmed_timestamp_keys.push((key.clone(), format!("{prefix}Z")));
                }
            }
            _ => return Ok(Vec::new()),
        }
    }

    let mut variants = vec![base];
    for (key, trimmed) in trimmed_timestamp_keys {
        let mut next = Vec::with_capacity(variants.len() * 2);
        for existing in &variants {
            next.push(existing.clone());

            let mut adjusted = existing.clone();
            adjusted.insert(
                key.clone(),
                arco_core::partition::ScalarValue::String(trimmed.clone()),
            );
            next.push(adjusted);
        }
        variants = next;
    }

    let mut rendered: Vec<String> = variants
        .into_iter()
        .map(|pk| pk.canonical_string())
        .collect();
    rendered.sort();
    rendered.dedup();
    Ok(rendered)
}

fn partitions_from_string_only_partition_key(
    partition_key: &str,
) -> Result<Option<Vec<PartitionValue>>, ApiError> {
    let parsed = arco_core::partition::PartitionKey::parse(partition_key).map_err(|err| {
        ApiError::internal(format!(
            "failed to parse canonical partitionKey for fingerprint normalization: {err}"
        ))
    })?;

    let mut result = Vec::with_capacity(parsed.len());
    for (key, value) in parsed.iter() {
        match value {
            arco_core::partition::ScalarValue::String(s) => result.push(PartitionValue {
                key: key.clone(),
                value: s.clone(),
            }),
            _ => return Ok(None),
        }
    }

    Ok(Some(result))
}

fn normalize_trigger_run_reservation_result(
    result: ReservationResult,
    fingerprint_variants: &[String],
) -> ReservationResult {
    match result {
        ReservationResult::FingerprintMismatch {
            existing,
            requested_fingerprint,
        } => {
            let is_equivalent = existing
                .request_fingerprint
                .as_deref()
                .is_some_and(|fp| fingerprint_variants.iter().any(|v| v == fp));
            if is_equivalent {
                ReservationResult::AlreadyExists(existing)
            } else {
                ReservationResult::FingerprintMismatch {
                    existing,
                    requested_fingerprint,
                }
            }
        }
        other => other,
    }
}

fn run_key_conflict_details(
    existing: &RunKeyReservation,
    requested_fingerprint: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "conflictType": "RUN_KEY_FINGERPRINT_MISMATCH",
        "runKey": existing.run_key,
        "existingRunId": existing.run_id,
        "existingPlanId": existing.plan_id,
        "existingFingerprint": existing.request_fingerprint,
        "requestedFingerprint": requested_fingerprint,
        "existingCreatedAt": existing.created_at.to_rfc3339(),
    })
}

fn run_key_conflict_error(
    existing: &RunKeyReservation,
    requested_fingerprint: Option<&str>,
) -> ApiError {
    ApiError::conflict(format!(
        "run_key '{}' already reserved with different trigger payload",
        existing.run_key
    ))
    .with_details(run_key_conflict_details(existing, requested_fingerprint))
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
        delta_table: row.delta_table.clone(),
        delta_version: row.delta_version,
        delta_partition: row.delta_partition.clone(),
        execution_lineage_ref: row.execution_lineage_ref.clone(),
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

fn filter_sensor_evals_by_status(
    evals: &mut Vec<SensorEvalResponse>,
    status: Option<SensorEvalStatusFilter>,
) {
    let Some(filter) = status else {
        return;
    };

    evals.retain(|eval| {
        matches!(
            (&eval.status, filter),
            (
                SensorEvalStatusResponse::Triggered,
                SensorEvalStatusFilter::Triggered
            ) | (
                SensorEvalStatusResponse::NoNewData,
                SensorEvalStatusFilter::NoNewData
            ) | (
                SensorEvalStatusResponse::Error { .. },
                SensorEvalStatusFilter::Error
            ) | (
                SensorEvalStatusResponse::SkippedStaleCursor,
                SensorEvalStatusFilter::SkippedStaleCursor
            )
        )
    });
}

fn filter_sensor_evals_by_time_range(
    evals: &mut Vec<SensorEvalResponse>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
) {
    if since.is_none() && until.is_none() {
        return;
    }

    evals.retain(|eval| {
        let after_since = since.is_none_or(|since| eval.evaluated_at >= since);
        let before_until = until.is_none_or(|until| eval.evaluated_at <= until);
        after_since && before_until
    });
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
    request_body = TriggerRunRequestOpenApi,
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

    validate_trigger_run_request_limits(&request)?;

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

    let has_partition_request = request.partition_key.is_some() || !request.partitions.is_empty();
    let mut plan_context: Option<RunPlanContext> = None;
    let partitioning_spec = if has_partition_request {
        let context = load_run_plan_context(&storage, &request).await?;
        let spec = context.partitioning_spec.clone();
        plan_context = Some(context);
        spec
    } else {
        PartitioningSpec::default()
    };

    let resolved_partition_key = resolve_partition_key(
        &request,
        &partitioning_spec,
        state.config.partition_time_string_cutoff,
    )?;

    let (request_fingerprint, request_fingerprint_variants) = if request.run_key.is_some() {
        let (primary, variants) =
            build_request_fingerprint_variants(&request, &resolved_partition_key)?;
        (Some(primary), variants)
    } else {
        (None, Vec::new())
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
                (Some(existing_fp), Some(_)) => request_fingerprint_variants
                    .iter()
                    .any(|fp| fp == existing_fp),
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

                return Err(run_key_conflict_error(
                    &existing,
                    request_fingerprint.as_deref(),
                ));
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
                if plan_context.is_none() {
                    plan_context = Some(load_run_plan_context(&storage, &request).await?);
                }
                let Some(plan_context_ref) = plan_context.as_ref() else {
                    return Err(ApiError::internal("internal error: missing plan context"));
                };

                let tasks = build_task_defs_for_request(
                    plan_context_ref,
                    &request,
                    resolved_partition_key.canonical(),
                )?;

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
                    manifest_id = %plan_context_ref.manifest_id,
                    manifest_deployed_at = %plan_context_ref.deployed_at,
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
                    plan_context_ref.root_assets.clone(),
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

    // If run_key provided, attempt to reserve it (strong idempotency)
    if let Some(ref run_key) = request.run_key {
        // Ensure request is valid against the deployed manifest before reserving.
        // This preserves the invariant: run_key is not reserved on BAD_REQUEST.
        if plan_context.is_none() {
            plan_context = Some(load_run_plan_context(&storage, &request).await?);
        }

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
        let reservation_result = reserve_run_key(&storage, &reservation, fingerprint_policy)
            .await
            .map_err(|e| ApiError::internal(format!("failed to reserve run_key: {e}")))?;
        let reservation_result = normalize_trigger_run_reservation_result(
            reservation_result,
            &request_fingerprint_variants,
        );

        match reservation_result {
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
                if let (Some(existing_fp), Some(_)) = (
                    existing.request_fingerprint.as_deref(),
                    request_fingerprint.as_deref(),
                ) {
                    if !request_fingerprint_variants
                        .iter()
                        .any(|fp| fp == existing_fp)
                    {
                        tracing::warn!(
                            run_key = %existing.run_key,
                            run_id = %existing.run_id,
                            "run_key reused with different trigger payload"
                        );
                        return Err(run_key_conflict_error(
                            &existing,
                            request_fingerprint.as_deref(),
                        ));
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
                    if plan_context.is_none() {
                        plan_context = Some(load_run_plan_context(&storage, &request).await?);
                    }
                    let Some(plan_context_ref) = plan_context.as_ref() else {
                        return Err(ApiError::internal("internal error: missing plan context"));
                    };
                    let tasks = build_task_defs_for_request(
                        plan_context_ref,
                        &request,
                        resolved_partition_key.canonical(),
                    )?;

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
                        manifest_id = %plan_context_ref.manifest_id,
                        manifest_deployed_at = %plan_context_ref.deployed_at,
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
                        plan_context_ref.root_assets.clone(),
                        tasks,
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
                return Err(run_key_conflict_error(
                    &existing,
                    requested_fingerprint.as_deref(),
                ));
            }
        }
    }

    if plan_context.is_none() {
        plan_context = Some(load_run_plan_context(&storage, &request).await?);
    }
    let Some(plan_context_ref) = plan_context.as_ref() else {
        return Err(ApiError::internal("internal error: missing plan context"));
    };
    let tasks = build_task_defs_for_request(
        plan_context_ref,
        &request,
        resolved_partition_key.canonical(),
    )?;

    tracing::info!(
        manifest_id = %plan_context_ref.manifest_id,
        manifest_deployed_at = %plan_context_ref.deployed_at,
        root_assets = ?plan_context_ref.root_assets,
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
        plan_context_ref.root_assets.clone(),
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

    validate_labels_limits(&request.labels)?;
    validate_selection_limits(&request.selection)?;
    if let Some(run_key) = request.run_key.as_deref() {
        if run_key.len() > MAX_RUN_KEY_LEN {
            return Err(ApiError::bad_request(format!(
                "runKey exceeds max length ({MAX_RUN_KEY_LEN})"
            )));
        }
    }

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
                    rerun_reason: rerun_reason_for_kind(rerun_kind).to_string(),
                }),
            ));
        }
        ReservationResult::FingerprintMismatch {
            existing,
            requested_fingerprint,
        } => {
            return Err(run_key_conflict_error(
                &existing,
                requested_fingerprint.as_deref(),
            ));
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
            rerun_reason: rerun_reason_for_kind(rerun_kind).to_string(),
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
#[allow(clippy::too_many_lines)]
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
        partition_key: request.partition_key.clone(),
        run_key: Some(request.run_key.clone()),
        labels: request.labels.clone(),
    };

    validate_trigger_run_request_limits(&fingerprint_request)?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let has_partition_request =
        fingerprint_request.partition_key.is_some() || !fingerprint_request.partitions.is_empty();
    let partitioning_spec = if has_partition_request {
        load_run_plan_context(&storage, &fingerprint_request)
            .await?
            .partitioning_spec
    } else {
        PartitioningSpec::default()
    };
    let resolved_partition_key = resolve_partition_key(
        &fingerprint_request,
        &partitioning_spec,
        state.config.partition_time_string_cutoff,
    )?;
    let request_fingerprint =
        build_request_fingerprint(&fingerprint_request, &resolved_partition_key)?;

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
    let skip_attribution_index = build_skip_attribution_index(&fold_state, &run_id);

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
            execution_lineage_ref: row.execution_lineage_ref.clone(),
            delta_table: row.delta_table.clone(),
            delta_version: row.delta_version,
            delta_partition: row.delta_partition.clone(),
            retry_attribution: task_retry_attribution(row),
            skip_attribution: task_skip_attribution(row, &skip_attribution_index),
        })
        .collect::<Vec<_>>();

    let (parent_run_id, rerun_kind) = lineage_from_labels(&run.labels);
    let rerun_reason = rerun_kind.map(|kind| rerun_reason_for_kind(kind).to_string());

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
        rerun_reason,
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
        ("since" = Option<DateTime<Utc>>, Query, description = "Filter evaluations after this timestamp (inclusive)"),
        ("until" = Option<DateTime<Utc>>, Query, description = "Filter evaluations before this timestamp (inclusive)"),
        ("status" = Option<SensorEvalStatusFilter>, Query, description = "Filter by status"),
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
    AxumQuery(query): AxumQuery<ListSensorEvalsQuery>,
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
    filter_sensor_evals_by_status(&mut evals, query.status);
    filter_sensor_evals_by_time_range(&mut evals, query.since, query.until);
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

    let (partition_selector, total_partitions) = parse_partition_selector(partition_selector)?;

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
        total_partitions,
        chunk_size,
        max_concurrent_runs,
    };

    let fingerprint = compute_backfill_fingerprint(
        &input.asset_selection,
        &input.partition_selector,
        input.chunk_size,
        input.max_concurrent_runs,
    )?;
    let compatibility_fingerprints = compute_legacy_backfill_fingerprint(
        &input.asset_selection,
        &input.partition_selector,
        input.chunk_size,
        input.max_concurrent_runs,
    )?
    .into_iter()
    .collect::<Vec<_>>();

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    if let Some(existing) = resolve_backfill_idempotency(
        &storage,
        &idempotency_key,
        &fingerprint,
        &compatibility_fingerprints,
    )
    .await?
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
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use chrono::Duration;
    use serde_json::json;

    fn hash_trigger_fingerprint_payload(
        request: &TriggerRunRequest,
        partition_key: Option<String>,
    ) -> Result<String> {
        #[derive(Serialize)]
        struct FingerprintPayload {
            selection: Vec<String>,
            include_upstream: bool,
            include_downstream: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            partition_key: Option<String>,
            labels: BTreeMap<String, String>,
        }

        let mut selection: Vec<String> = request
            .selection
            .iter()
            .map(|value| {
                arco_flow::orchestration::canonicalize_asset_key(value).map_err(anyhow::Error::msg)
            })
            .collect::<Result<_, _>>()?;
        selection.sort();
        selection.dedup();

        let labels = request
            .labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        let payload = FingerprintPayload {
            selection,
            include_upstream: request.include_upstream,
            include_downstream: request.include_downstream,
            partition_key,
            labels,
        };

        let json = serde_json::to_vec(&payload)?;
        Ok(hex::encode(Sha256::digest(&json)))
    }

    fn partitioning_spec(dimensions: Vec<DimensionSpec>) -> PartitioningSpec {
        PartitioningSpec {
            is_partitioned: !dimensions.is_empty(),
            dimensions,
        }
    }

    fn time_dimension(name: &str, granularity: Option<&str>) -> DimensionSpec {
        DimensionSpec {
            name: name.to_string(),
            kind: "time".to_string(),
            granularity: granularity.map(str::to_string),
            values: None,
        }
    }

    fn static_dimension(name: &str) -> DimensionSpec {
        DimensionSpec {
            name: name.to_string(),
            kind: "static".to_string(),
            granularity: None,
            values: None,
        }
    }

    fn static_dimension_with_values(name: &str, values: Vec<&str>) -> DimensionSpec {
        DimensionSpec {
            name: name.to_string(),
            kind: "static".to_string(),
            granularity: None,
            values: Some(values.into_iter().map(str::to_string).collect()),
        }
    }

    fn tenant_dimension(name: &str) -> DimensionSpec {
        DimensionSpec {
            name: name.to_string(),
            kind: "tenant".to_string(),
            granularity: None,
            values: None,
        }
    }

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
    fn test_trigger_request_deserialization_partition_key() {
        let json = r#"{
            "selection": ["analytics/users"],
            "partitionKey": "date=s:MjAyNC0wMS0xNQ",
            "runKey": "daily-etl:2024-01-15"
        }"#;

        let request: TriggerRunRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.selection, vec!["analytics/users"]);
        assert_eq!(
            request.partition_key.as_deref(),
            Some("date=s:MjAyNC0wMS0xNQ")
        );
        assert!(request.partitions.is_empty());
    }

    #[test]
    fn test_trigger_request_limits_rejects_too_long_partition_key() {
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("x".repeat(MAX_PARTITION_KEY_LEN + 1)),
            run_key: None,
            labels: HashMap::new(),
        };

        let err = validate_trigger_run_request_limits(&request).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("partitionKey"));
    }

    #[test]
    fn test_trigger_request_limits_rejects_too_many_selection_items() {
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string(); MAX_SELECTION_ITEMS + 1],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
            run_key: None,
            labels: HashMap::new(),
        };

        let err = validate_trigger_run_request_limits(&request).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("selection"));
    }

    #[test]
    fn test_build_partition_key_rejects_canonical_string_too_long() {
        let partitions = vec![
            PartitionValue {
                key: "a".to_string(),
                value: "x".repeat(MAX_PARTITION_VALUE_LEN),
            },
            PartitionValue {
                key: "b".to_string(),
                value: "x".repeat(MAX_PARTITION_VALUE_LEN),
            },
            PartitionValue {
                key: "c".to_string(),
                value: "x".repeat(MAX_PARTITION_VALUE_LEN),
            },
        ];

        let err = build_partition_key(&partitions).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("partitionKey exceeds max length"));
    }

    #[test]
    fn test_resolve_partition_key_rejects_both_partition_key_and_partitions() {
        let partitioning = partitioning_spec(vec![time_dimension("date", Some("day"))]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![PartitionValue {
                key: "date".to_string(),
                value: "2024-01-15".to_string(),
            }],
            partition_key: Some("date=s:MjAyNC0wMS0xNQ".to_string()),
            run_key: None,
            labels: HashMap::new(),
        };

        let err = resolve_partition_key(&request, &partitioning, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_resolve_partition_key_rejects_noncanonical_partition_key() {
        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            tenant_dimension("tenant"),
        ]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("tenant=s:YWNtZQ,date=s:MjAyNC0wMS0xNQ".to_string()),
            run_key: None,
            labels: HashMap::new(),
        };

        let err = resolve_partition_key(&request, &partitioning, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("canonical"));
    }

    #[test]
    fn test_resolve_partition_key_rejects_missing_dimension() {
        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            static_dimension("region"),
        ]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![PartitionValue {
                key: "date".to_string(),
                value: "2024-01-15".to_string(),
            }],
            partition_key: None,
            run_key: None,
            labels: HashMap::new(),
        };

        let err = resolve_partition_key(&request, &partitioning, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("expected"));
        assert!(err.message().contains("got"));
    }

    #[test]
    fn test_resolve_partition_key_rejects_extra_dimension() {
        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            static_dimension("region"),
        ]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![
                PartitionValue {
                    key: "date".to_string(),
                    value: "2024-01-15".to_string(),
                },
                PartitionValue {
                    key: "region".to_string(),
                    value: "us".to_string(),
                },
                PartitionValue {
                    key: "tenant".to_string(),
                    value: "acme".to_string(),
                },
            ],
            partition_key: None,
            run_key: None,
            labels: HashMap::new(),
        };

        let err = resolve_partition_key(&request, &partitioning, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_resolve_partition_key_rejects_disallowed_static_value() {
        let partitioning = partitioning_spec(vec![static_dimension_with_values(
            "region",
            vec!["us", "eu"],
        )]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![PartitionValue {
                key: "region".to_string(),
                value: "apac".to_string(),
            }],
            partition_key: None,
            run_key: None,
            labels: HashMap::new(),
        };

        let err = resolve_partition_key(&request, &partitioning, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("not allowed"));
    }

    #[test]
    fn test_resolve_partition_key_accepts_canonical_partition_key() -> Result<()> {
        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            tenant_dimension("tenant"),
        ]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("date=s:MjAyNC0wMS0xNQ,tenant=s:YWNtZQ".to_string()),
            run_key: None,
            labels: HashMap::new(),
        };

        let resolved = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        assert_eq!(
            resolved.canonical.as_deref(),
            Some("date=d:2024-01-15,tenant=s:YWNtZQ")
        );
        Ok(())
    }

    #[test]
    fn test_resolve_partition_key_normalizes_day_time_string() -> Result<()> {
        let partitioning = partitioning_spec(vec![time_dimension("date", Some("day"))]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("date=s:MjAyNC0wMS0xNQ".to_string()),
            run_key: None,
            labels: HashMap::new(),
        };

        let resolved = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        assert_eq!(resolved.canonical.as_deref(), Some("date=d:2024-01-15"));
        Ok(())
    }

    #[test]
    fn test_request_fingerprint_variants_include_legacy_time_string_partition_key() -> Result<()> {
        let partitioning = partitioning_spec(vec![time_dimension("date", Some("day"))]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("date=d:2024-01-15".to_string()),
            run_key: Some("daily:2024-01-15".to_string()),
            labels: HashMap::new(),
        };

        let resolved = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let (_primary, variants) = build_request_fingerprint_variants(&request, &resolved)
            .map_err(|err| anyhow!("{err:?}"))?;

        let encoded = URL_SAFE_NO_PAD.encode("2024-01-15".as_bytes());
        let legacy_partition_key = format!("date=s:{encoded}");
        let legacy_fingerprint =
            hash_trigger_fingerprint_payload(&request, Some(legacy_partition_key))?;

        assert!(
            variants.contains(&legacy_fingerprint),
            "expected variants to include legacy time-string fingerprint"
        );

        Ok(())
    }

    #[test]
    fn test_request_fingerprint_variants_include_legacy_hour_time_string_partition_key()
    -> Result<()> {
        let partitioning = partitioning_spec(vec![time_dimension("hour", Some("hour"))]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("hour=t:2024-01-15T15:00:00.000000Z".to_string()),
            run_key: Some("hourly:2024-01-15T15:00:00Z".to_string()),
            labels: HashMap::new(),
        };

        let resolved = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let (_primary, variants) = build_request_fingerprint_variants(&request, &resolved)
            .map_err(|err| anyhow!("{err:?}"))?;

        let encoded = URL_SAFE_NO_PAD.encode("2024-01-15T15:00:00Z".as_bytes());
        let legacy_partition_key = format!("hour=s:{encoded}");
        let legacy_fingerprint =
            hash_trigger_fingerprint_payload(&request, Some(legacy_partition_key))?;

        assert!(
            variants.contains(&legacy_fingerprint),
            "expected variants to include legacy hour time-string fingerprint"
        );

        Ok(())
    }

    #[test]
    fn test_resolve_partition_key_normalizes_hour_time_string() -> Result<()> {
        let partitioning = partitioning_spec(vec![time_dimension("hour", Some("hour"))]);
        let encoded = URL_SAFE_NO_PAD.encode("2024-01-15T10:00:00-05:00".as_bytes());
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some(format!("hour=s:{encoded}")),
            run_key: None,
            labels: HashMap::new(),
        };

        let resolved = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        assert_eq!(
            resolved.canonical.as_deref(),
            Some("hour=t:2024-01-15T15:00:00.000000Z")
        );
        Ok(())
    }

    #[test]
    fn test_resolve_partition_key_rejects_time_string_after_cutoff() {
        let partitioning = partitioning_spec(vec![time_dimension("date", Some("day"))]);
        let cutoff = Some(Utc::now() - Duration::days(1));
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("date=s:MjAyNC0wMS0xNQ".to_string()),
            run_key: None,
            labels: HashMap::new(),
        };

        let err =
            resolve_partition_key(&request, &partitioning, cutoff).expect_err("expected error");
        assert!(err.message().contains("d:/t:"));
    }

    #[test]
    fn test_resolve_partition_key_rejects_partitions_time_string_after_cutoff() {
        let partitioning = partitioning_spec(vec![time_dimension("date", Some("day"))]);
        let cutoff = Some(Utc::now() - Duration::days(1));
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![PartitionValue {
                key: "date".to_string(),
                value: "2024-01-15".to_string(),
            }],
            partition_key: None,
            run_key: None,
            labels: HashMap::new(),
        };

        let err =
            resolve_partition_key(&request, &partitioning, cutoff).expect_err("expected error");
        assert!(err.message().contains("d:/t:"));
    }

    #[test]
    fn test_resolve_partition_key_rejects_empty_partition_key() {
        let partitioning = partitioning_spec(vec![time_dimension("date", Some("day"))]);
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: Some("".to_string()),
            run_key: None,
            labels: HashMap::new(),
        };

        let err = resolve_partition_key(&request, &partitioning, None).expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_request_fingerprint_equivalent_for_partition_key_and_partitions() -> Result<()> {
        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            static_dimension("region"),
        ]);
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

        let request_with_partitions = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: partitions.clone(),
            partition_key: None,
            run_key: Some("daily-etl:2024-01-15".to_string()),
            labels: HashMap::new(),
        };

        let canonical = build_partition_key(&partitions).map_err(|err| anyhow!("{err:?}"))?;
        let request_with_partition_key = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: canonical,
            run_key: Some("daily-etl:2024-01-15".to_string()),
            labels: HashMap::new(),
        };

        let resolved_partitions =
            resolve_partition_key(&request_with_partitions, &partitioning, None)
                .map_err(|err| anyhow!("{err:?}"))?;
        let resolved_partition_key =
            resolve_partition_key(&request_with_partition_key, &partitioning, None)
                .map_err(|err| anyhow!("{err:?}"))?;
        let fingerprint_partitions =
            build_request_fingerprint(&request_with_partitions, &resolved_partitions)
                .map_err(|err| anyhow!("{err:?}"))?;
        let fingerprint_partition_key =
            build_request_fingerprint(&request_with_partition_key, &resolved_partition_key)
                .map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(fingerprint_partitions, fingerprint_partition_key);
        Ok(())
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
            rerun_reason: None,
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
    fn test_parse_partition_selector_rejects_filter_without_bounds() {
        let mut filters = HashMap::new();
        filters.insert("start".to_string(), "2025-01-01".to_string());

        let err = parse_partition_selector(PartitionSelectorRequest::Filter { filters })
            .expect_err("expected bounds validation error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.message(), "filter selector requires start/end bounds");
    }

    #[test]
    fn test_parse_partition_selector_rejects_filter_with_unsupported_keys() {
        let mut filters = HashMap::new();
        filters.insert("start".to_string(), "2025-01-01".to_string());
        filters.insert("end".to_string(), "2025-01-03".to_string());
        filters.insert("region".to_string(), "us-*".to_string());

        let err = parse_partition_selector(PartitionSelectorRequest::Filter { filters })
            .expect_err("expected unsupported filter key error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.message(),
            "filter selector supports only start/end bounds"
        );
    }

    #[test]
    fn test_parse_partition_selector_rejects_invalid_range_date_format() {
        let err = parse_partition_selector(PartitionSelectorRequest::Range {
            start: "2025/01/01".to_string(),
            end: "2025-01-03".to_string(),
        })
        .expect_err("expected invalid date format");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.message(),
            "partition selector bounds must use YYYY-MM-DD"
        );
    }

    #[test]
    fn test_parse_partition_selector_rejects_range_with_start_after_end() {
        let err = parse_partition_selector(PartitionSelectorRequest::Range {
            start: "2025-01-10".to_string(),
            end: "2025-01-03".to_string(),
        })
        .expect_err("expected start/end ordering error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            err.message(),
            "partition selector start must be less than or equal to end"
        );
    }

    #[test]
    fn test_parse_partition_selector_canonicalizes_filter_alias_bounds() -> Result<()> {
        let mut filters = HashMap::new();
        filters.insert("partition_start".to_string(), "2025-01-01".to_string());
        filters.insert("partition_end".to_string(), "2025-01-03".to_string());

        let (selector, _) = parse_partition_selector(PartitionSelectorRequest::Filter { filters })
            .map_err(|err| anyhow!("{err:?}"))?;

        let PartitionSelector::Filter { filters } = selector else {
            panic!("expected filter selector");
        };
        assert_eq!(filters.len(), 2);
        assert_eq!(filters.get("start"), Some(&"2025-01-01".to_string()));
        assert_eq!(filters.get("end"), Some(&"2025-01-03".to_string()));
        Ok(())
    }

    #[test]
    fn test_compute_backfill_fingerprint_is_deterministic_for_filter_selector() -> Result<()> {
        let mut first = HashMap::new();
        first.insert("partition_end".to_string(), "2025-01-10".to_string());
        first.insert("region".to_string(), "us-*".to_string());
        first.insert("partition_start".to_string(), "2025-01-01".to_string());

        let mut second = HashMap::new();
        second.insert("partition_start".to_string(), "2025-01-01".to_string());
        second.insert("region".to_string(), "us-*".to_string());
        second.insert("partition_end".to_string(), "2025-01-10".to_string());

        let selector_a = PartitionSelector::Filter { filters: first };
        let selector_b = PartitionSelector::Filter { filters: second };
        let asset_selection = vec!["analytics.daily".to_string()];

        let fingerprint_a = compute_backfill_fingerprint(&asset_selection, &selector_a, 10, 2)
            .map_err(|err| anyhow!("{err:?}"))?;
        let fingerprint_b = compute_backfill_fingerprint(&asset_selection, &selector_b, 10, 2)
            .map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(
            fingerprint_a, fingerprint_b,
            "fingerprint must be stable across equivalent filter map orderings"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_create_backfill_accepts_legacy_explicit_idempotency_fingerprint() -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct LegacyBackfillFingerprintPayload {
            asset_selection: Vec<String>,
            partitions: Vec<String>,
            chunk_size: u32,
            max_concurrent_runs: u32,
        }

        fn legacy_backfill_fingerprint(
            asset_selection: &[String],
            partitions: &[String],
            chunk_size: u32,
            max_concurrent_runs: u32,
        ) -> Result<String> {
            let payload = LegacyBackfillFingerprintPayload {
                asset_selection: asset_selection.to_vec(),
                partitions: partitions.to_vec(),
                chunk_size,
                max_concurrent_runs,
            };
            let json = serde_json::to_vec(&payload)?;
            Ok(hex::encode(Sha256::digest(&json)))
        }

        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            groups: vec![],
            request_id: "req_legacy_01".to_string(),
            idempotency_key: None,
        };

        let asset_selection = vec!["analytics.daily".to_string()];
        let partitions = vec!["2025-01-01".to_string(), "2025-01-02".to_string()];
        let legacy_fingerprint = legacy_backfill_fingerprint(&asset_selection, &partitions, 10, 2)?;
        let accepted_at = Utc::now();

        let record = BackfillIdempotencyRecord {
            idempotency_key: "legacy_idem_01".to_string(),
            backfill_id: "bf_legacy_existing".to_string(),
            accepted_event_id: "evt_legacy_existing".to_string(),
            accepted_at,
            fingerprint: legacy_fingerprint,
        };

        let backend = state.storage_backend().map_err(|err| anyhow!("{err:?}"))?;
        let storage = ctx
            .scoped_storage(backend)
            .map_err(|err| anyhow!("{err:?}"))?;
        store_backfill_idempotency_record(&storage, &record)
            .await
            .map_err(|err| anyhow!("{err:?}"))?;

        let request = CreateBackfillRequest {
            asset_selection,
            partition_selector: PartitionSelectorRequest::Explicit { partitions },
            client_request_id: Some("legacy_idem_01".to_string()),
            chunk_size: None,
            max_concurrent_runs: None,
        };

        let response = create_backfill(
            State(state),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request),
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
        let payload: CreateBackfillResponse = serde_json::from_slice(&body)?;
        assert_eq!(payload.backfill_id, "bf_legacy_existing");
        assert_eq!(payload.accepted_event_id, "evt_legacy_existing");
        assert_eq!(payload.accepted_at, accepted_at);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_backfill_accepts_range_selector() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request = CreateBackfillRequest {
            asset_selection: vec!["analytics.daily".to_string()],
            partition_selector: PartitionSelectorRequest::Range {
                start: "2025-01-01".to_string(),
                end: "2025-01-03".to_string(),
            },
            client_request_id: Some("bf_range_001".to_string()),
            chunk_size: Some(2),
            max_concurrent_runs: Some(1),
        };

        let response = create_backfill(
            State(state),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request),
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_backfill_accepts_filter_selector() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let mut filters = HashMap::new();
        filters.insert("start".to_string(), "2025-01-01".to_string());
        filters.insert("end".to_string(), "2025-01-03".to_string());

        let request = CreateBackfillRequest {
            asset_selection: vec!["analytics.daily".to_string()],
            partition_selector: PartitionSelectorRequest::Filter { filters },
            client_request_id: Some("bf_filter_001".to_string()),
            chunk_size: Some(2),
            max_concurrent_runs: Some(1),
        };

        let response = create_backfill(
            State(state),
            ctx.clone(),
            Path(ctx.workspace.clone()),
            Json(request),
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        Ok(())
    }

    #[tokio::test]
    async fn test_append_backfill_created_event_replays_existing_after_claim_race() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            groups: vec![],
            request_id: "req_race_01".to_string(),
            idempotency_key: None,
        };

        let backend = state.storage_backend().map_err(|err| anyhow!("{err:?}"))?;
        let storage = ctx
            .scoped_storage(backend)
            .map_err(|err| anyhow!("{err:?}"))?;

        let partition_selector = PartitionSelector::Range {
            start: "2025-01-01".to_string(),
            end: "2025-01-03".to_string(),
        };
        let input = BackfillCreateInput {
            asset_selection: vec!["analytics.daily".to_string()],
            partition_selector: partition_selector.clone(),
            total_partitions: 3,
            chunk_size: 2,
            max_concurrent_runs: 1,
        };
        let fingerprint =
            compute_backfill_fingerprint(&input.asset_selection, &partition_selector, 2, 1)
                .map_err(|err| anyhow!("{err:?}"))?;
        let accepted_at = Utc::now();

        let existing_record = BackfillIdempotencyRecord {
            idempotency_key: "bf_race_idem_01".to_string(),
            backfill_id: "bf_existing".to_string(),
            accepted_event_id: "evt_existing".to_string(),
            accepted_at,
            fingerprint: fingerprint.clone(),
        };
        store_backfill_idempotency_record(&storage, &existing_record)
            .await
            .map_err(|err| anyhow!("{err:?}"))?;

        let replay = append_backfill_created_event(
            state.as_ref(),
            &ctx,
            &ctx.workspace,
            input,
            "bf_race_idem_01",
            &storage,
            fingerprint,
        )
        .await
        .map_err(|err| anyhow!("{err:?}"))?;

        assert_eq!(replay.backfill_id, "bf_existing");
        assert_eq!(replay.accepted_event_id, "evt_existing");
        assert_eq!(replay.accepted_at, accepted_at);
        Ok(())
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
        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            static_dimension("region"),
        ]);
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
            partition_key: None,
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
            partition_key: None,
            run_key: Some("daily:2024-01-01".to_string()),
            labels: labels_reordered,
        };

        let resolved_a = resolve_partition_key(&request_a, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let resolved_b = resolve_partition_key(&request_b, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let fingerprint_a =
            build_request_fingerprint(&request_a, &resolved_a).map_err(|err| anyhow!("{err:?}"))?;
        let fingerprint_b =
            build_request_fingerprint(&request_b, &resolved_b).map_err(|err| anyhow!("{err:?}"))?;

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

    #[test]
    fn test_derive_run_partitioning_spec_rejects_mismatched_root_partitioning() -> Result<()> {
        let stored_manifest = StoredManifest {
            manifest_id: Ulid::new().to_string(),
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            manifest_version: "1.0".to_string(),
            code_version_id: "abc123".to_string(),
            fingerprint: "fp".to_string(),
            git: GitContext::default(),
            assets: vec![
                AssetEntry {
                    key: AssetKey {
                        namespace: "analytics".to_string(),
                        name: "users".to_string(),
                    },
                    id: "01HQXYZ123".to_string(),
                    description: String::new(),
                    owners: vec![],
                    tags: serde_json::Value::Null,
                    partitioning: json!({
                        "is_partitioned": true,
                        "dimensions": [{
                            "name": "date",
                            "kind": "time",
                            "granularity": "day"
                        }]
                    }),
                    dependencies: vec![],
                    code: serde_json::Value::Null,
                    checks: vec![],
                    execution: serde_json::Value::Null,
                    resources: serde_json::Value::Null,
                    io: serde_json::Value::Null,
                    transform_fingerprint: String::new(),
                },
                AssetEntry {
                    key: AssetKey {
                        namespace: "analytics".to_string(),
                        name: "orders".to_string(),
                    },
                    id: "01HQXYZ124".to_string(),
                    description: String::new(),
                    owners: vec![],
                    tags: serde_json::Value::Null,
                    partitioning: json!({
                        "is_partitioned": true,
                        "dimensions": [{
                            "name": "date",
                            "kind": "time",
                            "granularity": "hour"
                        }]
                    }),
                    dependencies: vec![],
                    code: serde_json::Value::Null,
                    checks: vec![],
                    execution: serde_json::Value::Null,
                    resources: serde_json::Value::Null,
                    io: serde_json::Value::Null,
                    transform_fingerprint: String::new(),
                },
            ],
            schedules: vec![],
            deployed_at: Utc::now(),
            deployed_by: "test".to_string(),
            metadata: serde_json::Value::Null,
        };

        let manifest_context =
            build_manifest_context(&stored_manifest).map_err(|err| anyhow!("{err:?}"))?;
        let root_assets = vec![
            arco_flow::orchestration::canonicalize_asset_key("analytics/users")
                .map_err(anyhow::Error::msg)?,
            arco_flow::orchestration::canonicalize_asset_key("analytics/orders")
                .map_err(anyhow::Error::msg)?,
        ];

        let err =
            derive_run_partitioning_spec(&root_assets, &manifest_context.partitioning_specs, true)
                .expect_err("expected error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(err.message().contains("identical partitioning"));
        Ok(())
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
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
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

        let partitioning = PartitioningSpec::default();
        let resolved_partition_key = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let reservation = RunKeyReservation {
            run_key: "daily:2024-01-01".to_string(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some({
                let (primary, variants) =
                    build_request_fingerprint_variants(&request, &resolved_partition_key)
                        .map_err(|err| anyhow!("{err:?}"))?;
                variants
                    .into_iter()
                    .find(|fp| fp != &primary)
                    .unwrap_or(primary)
            }),
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
                    let options = arco_flow::orchestration::SelectionOptions {
                        include_upstream: request.include_upstream,
                        include_downstream: request.include_downstream,
                    };
                    arco_flow::orchestration::build_task_defs_for_selection(
                        &graph,
                        &request.selection,
                        options,
                        resolved_partition_key.canonical.as_deref(),
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
    async fn test_trigger_run_normalizes_reserve_run_key_fingerprint_mismatch_for_equivalent_variants()
    -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));

        let ctx = RequestContext {
            tenant: "tenant".to_string(),
            workspace: "workspace".to_string(),
            user_id: Some("user@example.com".to_string()),
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

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

        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions,
            partition_key: None,
            run_key: Some("daily:2024-01-15".to_string()),
            labels: HashMap::new(),
        };

        let partitioning = partitioning_spec(vec![
            time_dimension("date", Some("day")),
            static_dimension("region"),
        ]);
        let resolved_partition_key = resolve_partition_key(&request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let (primary, variants) =
            build_request_fingerprint_variants(&request, &resolved_partition_key)
                .map_err(|err| anyhow!("{err:?}"))?;
        let legacy = variants
            .iter()
            .find(|fp| *fp != &primary)
            .cloned()
            .expect("expected legacy fingerprint variant");

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;

        let run_key = request.run_key.clone().expect("run_key");

        let legacy_reservation = RunKeyReservation {
            run_key: run_key.clone(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(legacy),
            created_at: Utc::now(),
        };

        let reserved =
            reserve_run_key(&storage, &legacy_reservation, FingerprintPolicy::lenient()).await?;
        assert!(matches!(reserved, ReservationResult::Reserved));

        let new_reservation = RunKeyReservation {
            run_key: run_key.clone(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(primary),
            created_at: Utc::now(),
        };

        let result =
            reserve_run_key(&storage, &new_reservation, FingerprintPolicy::lenient()).await?;
        assert!(matches!(
            result,
            ReservationResult::FingerprintMismatch { .. }
        ));

        let normalized = normalize_trigger_run_reservation_result(result, &variants);
        assert!(matches!(normalized, ReservationResult::AlreadyExists(_)));

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
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request_a = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let request_b = TriggerRunRequest {
            selection: vec!["analytics/orders".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;
        let partitioning = PartitioningSpec::default();
        let resolved_a = resolve_partition_key(&request_a, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;

        let reservation = RunKeyReservation {
            run_key: "daily:2024-01-01".to_string(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(
                build_request_fingerprint(&request_a, &resolved_a)
                    .map_err(|err| anyhow!("{err:?}"))?,
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
            groups: vec![],
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
            partition_key: None,
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
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request = RunKeyBackfillRequest {
            run_key: "daily:2024-01-01".to_string(),
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
            labels: HashMap::new(),
        };

        let fingerprint_request = TriggerRunRequest {
            selection: request.selection.clone(),
            include_upstream: request.include_upstream,
            include_downstream: request.include_downstream,
            partitions: request.partitions.clone(),
            partition_key: None,
            run_key: Some(request.run_key.clone()),
            labels: request.labels.clone(),
        };
        let partitioning = PartitioningSpec::default();
        let resolved = resolve_partition_key(&fingerprint_request, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;
        let expected_fingerprint = build_request_fingerprint(&fingerprint_request, &resolved)
            .map_err(|err| anyhow!("{err:?}"))?;

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
            groups: vec![],
            request_id: "req_01".to_string(),
            idempotency_key: None,
        };

        let request_a = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let request_b = RunKeyBackfillRequest {
            run_key: "daily:2024-01-01".to_string(),
            selection: vec!["analytics/orders".to_string()],
            include_upstream: false,
            include_downstream: false,
            partitions: vec![],
            partition_key: None,
            labels: HashMap::new(),
        };

        let backend = state.storage_backend()?;
        let storage = ctx.scoped_storage(backend)?;
        let partitioning = PartitioningSpec::default();
        let resolved_a = resolve_partition_key(&request_a, &partitioning, None)
            .map_err(|err| anyhow!("{err:?}"))?;

        let reservation = RunKeyReservation {
            run_key: "daily:2024-01-01".to_string(),
            run_id: Ulid::new().to_string(),
            plan_id: Ulid::new().to_string(),
            event_id: Ulid::new().to_string(),
            plan_event_id: Some(Ulid::new().to_string()),
            request_fingerprint: Some(
                build_request_fingerprint(&request_a, &resolved_a)
                    .map_err(|err| anyhow!("{err:?}"))?,
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
