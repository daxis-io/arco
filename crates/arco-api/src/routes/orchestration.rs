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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use axum::extract::Query as AxumQuery;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
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
use crate::server::AppState;
use arco_core::{WritePrecondition, WriteResult};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{
    FoldState, MicroCompactor, RunRow, RunState as FoldRunState, TaskRow,
    TaskState as FoldTaskState,
};
use arco_flow::orchestration::controllers::{PubSubMessage, PushSensorHandler};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, RunRequest, SensorEvalStatus, SensorStatus,
    TaskDef, TriggerInfo,
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

/// Request to backfill a `run_key` reservation fingerprint.
#[derive(Debug, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RunKeyBackfillRequest {
    /// Idempotency key to backfill.
    pub run_key: String,
    /// Asset selection (asset keys to materialize).
    #[serde(default)]
    pub selection: Vec<String>,
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
#[derive(Debug, Serialize, ToSchema)]
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
// Helpers
// ============================================================================

fn ensure_workspace(ctx: &RequestContext, workspace_id: &str) -> Result<(), ApiError> {
    if workspace_id != ctx.workspace {
        return Err(ApiError::not_found("workspace not found"));
    }
    Ok(())
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

fn build_task_defs(request: &TriggerRunRequest) -> Result<Vec<TaskDef>, ApiError> {
    let partition_key = build_partition_key(&request.partitions)?;

    Ok(request
        .selection
        .iter()
        .map(|asset_key| TaskDef {
            key: asset_key.clone(), // task_key = asset_key for simple case
            depends_on: vec![],
            asset_key: Some(asset_key.clone()),
            partition_key: partition_key.clone(),
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
        })
        .collect())
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
        partitions: Vec<PartitionValue>,
        labels: BTreeMap<String, String>,
    }

    let mut selection = request.selection.clone();
    selection.sort();

    let mut partitions = request.partitions.clone();
    partitions.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

    let labels = request
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    let payload = FingerprintPayload {
        selection,
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

    let tasks = build_task_defs(&request)?;
    let root_assets = request.selection.clone();

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
        .map(|row| RunListItem {
            run_id: row.run_id.clone(),
            state: map_run_row_state(row),
            created_at: row.triggered_at,
            completed_at: row.completed_at,
            task_count: row.tasks_total,
            tasks_succeeded: row.tasks_succeeded,
            tasks_failed: row.tasks_failed,
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
// Router
// ============================================================================

/// Creates the orchestration routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/workspaces/:workspace_id/runs", post(trigger_run))
        .route("/workspaces/:workspace_id/runs", get(list_runs))
        .route("/workspaces/:workspace_id/runs/:run_id", get(get_run))
        .route(
            "/workspaces/:workspace_id/sensors/:sensor_id/evaluate",
            post(manual_evaluate_sensor),
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
        };

        let json = serde_json::to_string(&response).expect("serialize");
        assert!(json.contains("\"runId\":\"run_123\""));
        assert!(json.contains("\"state\":\"RUNNING\""));
    }

    #[test]
    fn test_request_fingerprint_is_order_independent() -> Result<()> {
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("team".to_string(), "infra".to_string());

        let request_a = TriggerRunRequest {
            selection: vec!["b".to_string(), "a".to_string()],
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
            selection: vec!["a".to_string(), "b".to_string()],
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
    fn test_build_task_defs_canonical_partition_key() -> Result<()> {
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            partitions: vec![
                PartitionValue {
                    key: "region".to_string(),
                    value: "us-east-1".to_string(),
                },
                PartitionValue {
                    key: "date".to_string(),
                    value: "2024-01-15".to_string(),
                },
            ],
            run_key: None,
            labels: HashMap::new(),
        };

        let tasks = build_task_defs(&request).map_err(|err| anyhow!("{err:?}"))?;
        let mut pk = PartitionKey::new();
        pk.insert("region", ScalarValue::String("us-east-1".to_string()));
        pk.insert("date", ScalarValue::String("2024-01-15".to_string()));
        let expected = pk.canonical_string();

        assert_eq!(tasks[0].partition_key.as_deref(), Some(expected.as_str()));
        Ok(())
    }

    #[test]
    fn test_build_task_defs_rejects_invalid_partition_key() {
        let request = TriggerRunRequest {
            selection: vec!["analytics/users".to_string()],
            partitions: vec![PartitionValue {
                key: "Date".to_string(),
                value: "2024-01-15".to_string(),
            }],
            run_key: None,
            labels: HashMap::new(),
        };

        assert!(build_task_defs(&request).is_err());
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
                root_assets: vec!["analytics/users".to_string()],
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
                tasks: build_task_defs(&request).map_err(|err| anyhow!("{err:?}"))?,
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
            partitions: vec![],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let request_b = TriggerRunRequest {
            selection: vec!["analytics/orders".to_string()],
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
            partitions: vec![],
            labels: HashMap::new(),
        };

        let fingerprint_request = TriggerRunRequest {
            selection: request.selection.clone(),
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
            partitions: vec![],
            run_key: Some("daily:2024-01-01".to_string()),
            labels: HashMap::new(),
        };

        let request_b = RunKeyBackfillRequest {
            run_key: "daily:2024-01-01".to_string(),
            selection: vec!["analytics/orders".to_string()],
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
