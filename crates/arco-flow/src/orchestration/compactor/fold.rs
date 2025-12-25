//! Event fold logic for compacting orchestration events into Parquet state.
//!
//! The fold processes events in ULID order and builds up the state tables:
//! - `runs`: Run state and counters
//! - `tasks`: Task state machine with dependencies
//! - `dep_satisfaction`: Per-edge dependency facts
//! - `timers`: Active durable timers
//! - `dispatch_outbox`: Pending dispatch intents
//!
//! Key invariants:
//! - Events are processed exactly once (idempotency key index + `row_version`)
//! - State transitions are monotonic (can't go backward)
//! - Duplicate events are no-ops

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use metrics::counter;

use crate::metrics::{labels as metrics_labels, names as metrics_names};
use crate::orchestration::events::{
    BackfillState, ChunkState, OrchestrationEvent, OrchestrationEventData, PartitionSelector,
    RunRequest, SensorEvalStatus, SensorStatus, SourceRef, TaskDef, TaskOutcome, TickStatus,
    TimerType as EventTimerType, TriggerSource,
};

/// Task state machine states.
///
/// States have a rank for monotonic ordering - higher rank = more terminal.
/// This prevents state regression when events arrive out of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    /// Task is planned but dependencies not yet satisfied.
    Planned,
    /// Task is blocked waiting for dependencies.
    Blocked,
    /// Task is ready to be dispatched.
    Ready,
    /// Task has been dispatched but not yet started.
    Dispatched,
    /// Task is currently running.
    Running,
    /// Task is waiting for retry.
    RetryWait,
    /// Task was skipped (upstream failed).
    Skipped,
    /// Task was cancelled.
    Cancelled,
    /// Task failed after all retries.
    Failed,
    /// Task completed successfully.
    Succeeded,
}

impl TaskState {
    /// Returns the state rank for monotonic ordering.
    ///
    /// Higher rank = more terminal. Used as tiebreaker when `row_versions` are equal.
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::Blocked => 1,
            Self::Ready => 2,
            Self::Dispatched => 3,
            Self::Running => 4,
            Self::RetryWait => 5,
            Self::Skipped => 10,
            Self::Cancelled => 11,
            Self::Failed => 12,
            Self::Succeeded => 13,
        }
    }

    /// Returns true if this is a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }
}

use serde::{Deserialize, Serialize};

/// Dependency edge resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DepResolution {
    /// Upstream succeeded, dependency satisfied.
    Success,
    /// Upstream failed, downstream should be skipped.
    Failed,
    /// Upstream was skipped, downstream should be skipped.
    Skipped,
    /// Upstream was cancelled, downstream should be cancelled.
    Cancelled,
}

/// Dispatch outbox status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DispatchStatus {
    /// Ready to be dispatched to Cloud Tasks.
    Pending,
    /// Cloud Task has been created.
    Created,
    /// Worker has acknowledged the dispatch.
    Acked,
    /// Dispatch failed (Cloud Tasks API error).
    Failed,
}

/// Dispatch outbox row (dual-identifier pattern per ADR-021).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchOutboxRow {
    /// Run identifier.
    pub run_id: String,
    /// Task name.
    pub task_key: String,
    /// Attempt number.
    pub attempt: u32,
    /// Internal ID (human-readable): `dispatch:{run_id}:{task_key}:{attempt}`.
    pub dispatch_id: String,
    /// Cloud Tasks-safe ID (hash-based): `d_{hex(sha256(dispatch_id))[..40]}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_task_id: Option<String>,
    /// Current status.
    pub status: DispatchStatus,
    /// Attempt ID (included in worker payload for concurrency guard).
    pub attempt_id: String,
    /// Target worker queue name.
    pub worker_queue: String,
    /// When outbox entry was created.
    pub created_at: DateTime<Utc>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl DispatchOutboxRow {
    /// Generates the internal dispatch ID.
    #[must_use]
    pub fn dispatch_id(run_id: &str, task_key: &str, attempt: u32) -> String {
        format!("dispatch:{run_id}:{task_key}:{attempt}")
    }
}

/// Timer type for durable timers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimerType {
    /// Retry timer - fires when retry backoff expires.
    Retry,
    /// Heartbeat check - fires to check for zombie tasks.
    HeartbeatCheck,
    /// Cron timer - fires on schedule.
    Cron,
    /// SLA check - fires to check for deadline violations.
    SlaCheck,
}

/// Timer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimerState {
    /// Timer is scheduled to fire.
    Scheduled,
    /// Timer has fired.
    Fired,
    /// Timer was cancelled.
    Cancelled,
}

fn map_timer_type(timer_type: EventTimerType) -> TimerType {
    match timer_type {
        EventTimerType::Retry => TimerType::Retry,
        EventTimerType::HeartbeatCheck => TimerType::HeartbeatCheck,
        EventTimerType::Cron => TimerType::Cron,
        EventTimerType::SlaCheck => TimerType::SlaCheck,
    }
}

/// Timer row (dual-identifier pattern per ADR-021).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerRow {
    /// Internal ID (human-readable): `timer:{type}:{run_id}:{task_key}:{attempt}:{epoch}`.
    pub timer_id: String,
    /// Cloud Tasks-safe ID (hash-based): `t_{hex(sha256(timer_id))[..40]}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_task_id: Option<String>,
    /// Timer type.
    pub timer_type: TimerType,
    /// Associated run (optional for cron timers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Associated task (optional for cron timers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_key: Option<String>,
    /// Associated attempt (for retry/heartbeat timers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Scheduled fire time.
    pub fire_at: DateTime<Utc>,
    /// Current state.
    pub state: TimerState,
    /// JSON payload for timer handler.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

/// Parsed timer ID components.
#[derive(Debug, Clone)]
pub struct ParsedTimerId {
    /// The type of timer (retry, heartbeat, cron, or SLA).
    pub timer_type: TimerType,
    /// The scheduled fire time as Unix epoch seconds.
    pub fire_epoch: i64,
    /// Associated run (if present in the ID).
    pub run_id: Option<String>,
    /// Associated task (if present in the ID).
    pub task_key: Option<String>,
    /// Associated attempt (if present in the ID).
    pub attempt: Option<u32>,
}

impl TimerRow {
    /// Generates the internal timer ID for retry timers.
    #[must_use]
    pub fn retry_timer_id(run_id: &str, task_key: &str, attempt: u32, fire_epoch: i64) -> String {
        format!("timer:retry:{run_id}:{task_key}:{attempt}:{fire_epoch}")
    }

    /// Generates the internal timer ID for heartbeat check timers.
    #[must_use]
    pub fn heartbeat_timer_id(
        run_id: &str,
        task_key: &str,
        attempt: u32,
        fire_epoch: i64,
    ) -> String {
        format!("timer:heartbeat:{run_id}:{task_key}:{attempt}:{fire_epoch}")
    }

    /// Generates the internal timer ID for cron timers.
    #[must_use]
    pub fn cron_timer_id(schedule_id: &str, fire_epoch: i64) -> String {
        format!("timer:cron:{schedule_id}:{fire_epoch}")
    }

    /// Parses a timer ID to extract timer type, fire epoch, and optional run metadata.
    ///
    /// Timer ID formats:
    /// - `timer:retry:{run_id}:{task_key}:{attempt}:{fire_epoch}`
    /// - `timer:heartbeat:{run_id}:{task_key}:{attempt}:{fire_epoch}`
    /// - `timer:cron:{schedule_id}:{fire_epoch}`
    /// - `timer:sla:{run_id}:{task_key}:{attempt}:{fire_epoch}`
    #[must_use]
    pub fn parse_timer_id(timer_id: &str) -> Option<ParsedTimerId> {
        let parts: Vec<&str> = timer_id.split(':').collect();
        let prefix = parts.first()?;
        if *prefix != "timer" {
            return None;
        }

        let timer_type = match *parts.get(1)? {
            "retry" => TimerType::Retry,
            "heartbeat" => TimerType::HeartbeatCheck,
            "cron" => TimerType::Cron,
            "sla" => TimerType::SlaCheck,
            _ => return None,
        };

        // fire_epoch is always the last component
        let fire_epoch = parts.last()?.parse::<i64>().ok()?;

        let (run_id, task_key, attempt) = match timer_type {
            TimerType::Cron => (None, None, None),
            TimerType::Retry | TimerType::HeartbeatCheck | TimerType::SlaCheck => {
                let run_id = parts.get(2)?;
                let task_key = parts.get(3)?;
                let attempt = parts.get(4)?.parse::<u32>().ok()?;
                (
                    Some((*run_id).to_string()),
                    Some((*task_key).to_string()),
                    Some(attempt),
                )
            }
        };

        Some(ParsedTimerId {
            timer_type,
            fire_epoch,
            run_id,
            task_key,
            attempt,
        })
    }
}

// ============================================================================
// Cloud Tasks ID Generation (ADR-021)
// ============================================================================

use crate::orchestration::ids::cloud_task_id;

/// Generates a Cloud Tasks-safe dispatch ID.
#[must_use]
pub fn dispatch_cloud_task_id(run_id: &str, task_key: &str, attempt: u32) -> String {
    let internal_id = DispatchOutboxRow::dispatch_id(run_id, task_key, attempt);
    cloud_task_id("d", &internal_id)
}

/// Generates a Cloud Tasks-safe timer ID.
#[must_use]
pub fn timer_cloud_task_id(timer_id: &str) -> String {
    cloud_task_id("t", timer_id)
}

/// Task row in the fold state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRow {
    /// Run identifier.
    pub run_id: String,
    /// Task name within run.
    pub task_key: String,
    /// Current state.
    pub state: TaskState,
    /// Current attempt number (1-indexed).
    pub attempt: u32,
    /// Current attempt ID - concurrency guard.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// When the task started (first attempt of current run).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the task completed (terminal outcome).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message for the terminal attempt (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /// Total number of dependencies.
    pub deps_total: u32,
    /// Derived count of satisfied dependencies.
    pub deps_satisfied_count: u32,
    /// Maximum retry attempts.
    pub max_attempts: u32,
    /// Heartbeat timeout in seconds.
    pub heartbeat_timeout_sec: u32,
    /// Last heartbeat timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// When task became READY (for anti-entropy).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<DateTime<Utc>>,
    /// Asset key (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_key: Option<String>,
    /// Partition key (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

/// Dependency satisfaction row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepSatisfactionRow {
    /// Run identifier.
    pub run_id: String,
    /// Upstream task key.
    pub upstream_task_key: String,
    /// Downstream task key.
    pub downstream_task_key: String,
    /// Whether edge is satisfied.
    pub satisfied: bool,
    /// Edge resolution (SUCCESS, FAILED, SKIPPED, CANCELLED).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<DepResolution>,
    /// When edge was satisfied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub satisfied_at: Option<DateTime<Utc>>,
    /// Which attempt satisfied the edge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub satisfying_attempt: Option<u32>,
    /// ULID of satisfying event.
    pub row_version: String,
}

/// Run row in the fold state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRow {
    /// Run identifier.
    pub run_id: String,
    /// Plan identifier.
    pub plan_id: String,
    /// Current run state.
    pub state: RunState,
    /// Idempotency key for run deduplication (e.g., "daily-etl:2025-01-15").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Run labels (optional metadata).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    /// Whether cancellation has been requested.
    #[serde(default)]
    pub cancel_requested: bool,
    /// Total tasks in run.
    pub tasks_total: u32,
    /// Tasks completed (succeeded + failed + skipped).
    pub tasks_completed: u32,
    /// Tasks succeeded.
    pub tasks_succeeded: u32,
    /// Tasks failed.
    pub tasks_failed: u32,
    /// Tasks skipped.
    pub tasks_skipped: u32,
    /// Tasks cancelled.
    pub tasks_cancelled: u32,
    /// When run was triggered.
    pub triggered_at: DateTime<Utc>,
    /// When run completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

/// Run state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    /// Run has been triggered.
    Triggered,
    /// Run is in progress.
    Running,
    /// Run completed successfully.
    Succeeded,
    /// Run failed.
    Failed,
    /// Run was cancelled.
    Cancelled,
}

impl RunState {
    /// Returns true if this is a terminal state.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

// ============================================================================
// Layer 2: Schedule Schemas
// ============================================================================

/// Schedule definition (configuration, not state).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleDefinitionRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Schedule identifier (ULID).
    pub schedule_id: String,
    /// Cron expression (5- or 6-field, e.g., "0 10 * * *" or "0 0 10 * * *").
    pub cron_expression: String,
    /// IANA timezone (e.g., `UTC`, `America/New_York`).
    pub timezone: String,
    /// Maximum minutes to look back for missed ticks.
    pub catchup_window_minutes: u32,
    /// Assets to materialize when schedule fires.
    pub asset_selection: Vec<String>,
    /// Maximum catch-up ticks to emit.
    pub max_catchup_ticks: u32,
    /// Whether the schedule is enabled.
    pub enabled: bool,
    /// When the schedule was created.
    pub created_at: DateTime<Utc>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl ScheduleDefinitionRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.schedule_id)
    }
}

/// Schedule runtime state (separate from definition).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleStateRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Schedule identifier.
    pub schedule_id: String,
    /// Last `scheduled_for` timestamp that was processed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scheduled_for: Option<DateTime<Utc>>,
    /// Last tick ID that was processed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tick_id: Option<String>,
    /// Last run key generated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_key: Option<String>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl ScheduleStateRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.schedule_id)
    }
}

/// Schedule tick history (one row per tick).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleTickRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Unique tick ID: `{schedule_id}:{scheduled_for_epoch}`.
    pub tick_id: String,
    /// Schedule identifier.
    pub schedule_id: String,
    /// When this tick was scheduled for.
    pub scheduled_for: DateTime<Utc>,
    /// Definition version used for this tick.
    pub definition_version: String,
    /// Snapshot of asset selection at tick time.
    pub asset_selection: Vec<String>,
    /// Optional partition selection snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_selection: Option<Vec<String>>,
    /// Tick evaluation status.
    pub status: TickStatus,
    /// Run key if a run was requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Run ID (correlated from `RunRequested` during fold).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Request fingerprint for conflict detection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_fingerprint: Option<String>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl ScheduleTickRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.tick_id)
    }
}

// ============================================================================
// Layer 2: Sensor Schemas
// ============================================================================

/// Sensor runtime state (universal for push + poll).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SensorStateRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Sensor identifier (ULID).
    pub sensor_id: String,
    /// Current cursor value (poll sensors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Last evaluation timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluation_at: Option<DateTime<Utc>>,
    /// Last evaluation ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_eval_id: Option<String>,
    /// Sensor status (ACTIVE/PAUSED/ERROR).
    pub status: SensorStatus,
    /// State version for CAS (poll sensors).
    pub state_version: u32,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl SensorStateRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.sensor_id)
    }
}

/// Sensor evaluation history row.
///
/// This projection tracks all sensor evaluations including stale ones (CAS failures).
/// Used for:
/// - Debugging overlapping poll sensor issues
/// - Correlating `RunRequested` events back to their source evaluation
/// - Observability into sensor evaluation patterns
///
/// ## Persistence Strategy
///
/// When persisted to Parquet, entries should be retained for 7-30 days (configurable).
/// The `evaluated_at` field enables time-based cleanup during compaction.
/// Stale evaluations (`status = SkippedStaleCursor`) are particularly useful for
/// diagnosing concurrent poll overlap issues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SensorEvalRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Evaluation identifier.
    pub eval_id: String,
    /// Sensor identifier.
    pub sensor_id: String,
    /// Cursor before evaluation (poll sensors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_before: Option<String>,
    /// Cursor after evaluation (poll sensors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_after: Option<String>,
    /// Expected state version for CAS (poll sensors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_state_version: Option<u32>,
    /// Trigger source for this evaluation.
    pub trigger_source: TriggerSource,
    /// Run requests generated by this evaluation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub run_requests: Vec<RunRequest>,
    /// Evaluation status.
    pub status: SensorEvalStatus,
    /// When the evaluation occurred.
    pub evaluated_at: DateTime<Utc>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl SensorEvalRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.eval_id)
    }
}

// ============================================================================
// Layer 2: Backfill Schemas
// ============================================================================

/// Backfill entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Backfill identifier (ULID).
    pub backfill_id: String,
    /// Assets to backfill.
    pub asset_selection: Vec<String>,
    /// Partition selector (compact, per P0-6).
    pub partition_selector: PartitionSelector,
    /// Number of partitions per chunk.
    pub chunk_size: u32,
    /// Maximum concurrent chunk runs.
    pub max_concurrent_runs: u32,
    /// Current state.
    pub state: BackfillState,
    /// State version (monotonic).
    pub state_version: u32,
    /// Total partition count.
    pub total_partitions: u32,
    /// Chunks that have been planned.
    pub planned_chunks: u32,
    /// Chunks that completed successfully.
    pub completed_chunks: u32,
    /// Chunks that failed.
    pub failed_chunks: u32,
    /// Parent backfill ID (for retry-failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_backfill_id: Option<String>,
    /// When the backfill was created.
    pub created_at: DateTime<Utc>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl BackfillRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.backfill_id)
    }
}

/// Backfill chunk tracking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillChunkRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Chunk ID: `{backfill_id}:{chunk_index}`.
    pub chunk_id: String,
    /// Backfill identifier.
    pub backfill_id: String,
    /// Zero-indexed chunk number.
    pub chunk_index: u32,
    /// Partition keys in this chunk.
    pub partition_keys: Vec<String>,
    /// Run key for this chunk.
    pub run_key: String,
    /// Run ID (filled when run resolves).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Chunk state.
    pub state: ChunkState,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl BackfillChunkRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.chunk_id)
    }
}

// ============================================================================
// Layer 2: Partition Status Schemas (per ADR-026)
// ============================================================================

/// Partition materialization status.
///
/// Computed display status that separates execution from data freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PartitionMaterializationStatus {
    /// Partition has never been materialized.
    NeverMaterialized,
    /// Partition is materialized and fresh.
    Materialized,
    /// Partition is materialized but stale.
    Stale,
    /// Partition is materialized but the last attempt failed.
    MaterializedButLastAttemptFailed,
}

impl PartitionMaterializationStatus {
    /// Returns true if the partition has ever been successfully materialized.
    #[must_use]
    pub const fn is_materialized(self) -> bool {
        !matches!(self, Self::NeverMaterialized)
    }
}

/// Partition status tracking (per ADR-026).
///
/// Separates data freshness (materialization) from execution status (attempts).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStatusRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Asset key.
    pub asset_key: String,
    /// Partition key.
    pub partition_key: String,
    /// Run that last materialized (success only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_materialization_run_id: Option<String>,
    /// When last materialized (success only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_materialization_at: Option<DateTime<Utc>>,
    /// Code version used (success only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_materialization_code_version: Option<String>,
    /// Most recent attempt run (any outcome).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_run_id: Option<String>,
    /// When last attempted (any outcome).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<DateTime<Utc>>,
    /// Last attempt outcome (`TaskOutcome`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_outcome: Option<TaskOutcome>,
    /// When partition became stale (nullable; derived or precomputed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_since: Option<DateTime<Utc>>,
    /// Stale reason code (`FRESHNESS_POLICY/UPSTREAM_CHANGED/CODE_CHANGED`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason_code: Option<String>,
    /// Dimension key-values for the partition.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub partition_values: HashMap<String, String>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl PartitionStatusRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.tenant_id,
            &self.workspace_id,
            &self.asset_key,
            &self.partition_key,
        )
    }
}

// ============================================================================
// Layer 2: Run Key Index Schemas
// ============================================================================

/// Run key index for deduplication and conflict detection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunKeyIndexRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Stable run key (e.g., "sched:daily-etl:1736935200").
    pub run_key: String,
    /// Computed run ID.
    pub run_id: String,
    /// Request fingerprint for conflict detection.
    pub request_fingerprint: String,
    /// When the run was created.
    pub created_at: DateTime<Utc>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl RunKeyIndexRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (&self.tenant_id, &self.workspace_id, &self.run_key)
    }
}

/// Run key conflict (when same `run_key` has different fingerprint).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunKeyConflictRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Run key that had a conflict.
    pub run_key: String,
    /// Fingerprint of the existing run.
    pub existing_fingerprint: String,
    /// Fingerprint that conflicted.
    pub conflicting_fingerprint: String,
    /// Event ID that caused the conflict.
    pub conflicting_event_id: String,
    /// When the conflict was detected.
    pub detected_at: DateTime<Utc>,
}

impl RunKeyConflictRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.tenant_id,
            &self.workspace_id,
            &self.run_key,
            &self.conflicting_event_id,
        )
    }
}

/// Idempotency key index row.
///
/// Tracks processed idempotency keys to drop duplicate events across compaction runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyKeyRow {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Idempotency key (unique).
    pub idempotency_key: String,
    /// Event identifier that claimed this idempotency key.
    pub event_id: String,
    /// Event type that produced this idempotency key.
    pub event_type: String,
    /// When the event was recorded.
    pub recorded_at: DateTime<Utc>,
    /// ULID of last event that modified this row.
    pub row_version: String,
}

impl IdempotencyKeyRow {
    /// Returns the primary key tuple.
    #[must_use]
    pub fn primary_key(&self) -> (&str, &str, &str) {
        (
            &self.tenant_id,
            &self.workspace_id,
            &self.idempotency_key,
        )
    }
}

/// Fold state accumulator.
///
/// Holds the current state being built from events.
#[derive(Debug, Clone, Default)]
pub struct FoldState {
    /// Run rows keyed by `run_id`.
    pub runs: HashMap<String, RunRow>,

    /// Task rows keyed by (`run_id`, `task_key`).
    pub tasks: HashMap<(String, String), TaskRow>,

    /// Dependency edges keyed by (`run_id`, `upstream_task_key`, `downstream_task_key`).
    pub dep_satisfaction: HashMap<(String, String, String), DepSatisfactionRow>,

    /// Timer rows keyed by `timer_id`.
    pub timers: HashMap<String, TimerRow>,

    /// Dispatch outbox rows keyed by `dispatch_id`.
    pub dispatch_outbox: HashMap<String, DispatchOutboxRow>,

    /// Task dependencies (downstream -> list of upstreams).
    task_dependencies: HashMap<(String, String), Vec<String>>,

    /// Task dependents (upstream -> list of downstreams).
    task_dependents: HashMap<(String, String), Vec<String>>,

    // ========================================================================
    // Layer 2: Schedule/Sensor Automation
    // ========================================================================
    /// Schedule state rows keyed by `schedule_id`.
    pub schedule_state: HashMap<String, ScheduleStateRow>,

    /// Schedule tick rows keyed by `tick_id`.
    pub schedule_ticks: HashMap<String, ScheduleTickRow>,

    /// Sensor state rows keyed by `sensor_id`.
    pub sensor_state: HashMap<String, SensorStateRow>,

    /// Sensor evaluation history rows keyed by `eval_id`.
    pub sensor_evals: HashMap<String, SensorEvalRow>,

    // ========================================================================
    // Layer 2: Backfill
    // ========================================================================
    /// Backfill rows keyed by `backfill_id`.
    pub backfills: HashMap<String, BackfillRow>,

    /// Backfill chunk rows keyed by `chunk_id`.
    pub backfill_chunks: HashMap<String, BackfillChunkRow>,

    // ========================================================================
    // Layer 2: Partition Status
    // ========================================================================
    /// Partition status rows keyed by (`asset_key`, `partition_key`).
    pub partition_status: HashMap<(String, String), PartitionStatusRow>,

    // ========================================================================
    // Layer 2: Run Key Index
    // ========================================================================
    /// Run key index keyed by `run_key`.
    pub run_key_index: HashMap<String, RunKeyIndexRow>,

    /// Run key conflicts keyed by `conflict_id`.
    pub run_key_conflicts: HashMap<String, RunKeyConflictRow>,

    // ========================================================================
    // Global Idempotency Index
    // ========================================================================
    /// Idempotency key index keyed by `idempotency_key`.
    pub idempotency_keys: HashMap<String, IdempotencyKeyRow>,
}

impl FoldState {
    /// Creates a new empty fold state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds dependency graphs from `dep_satisfaction` rows.
    ///
    /// Used after loading state from Parquet where graphs are not persisted.
    pub fn rebuild_dependency_graph(&mut self) {
        self.task_dependencies.clear();
        self.task_dependents.clear();

        for key in self.tasks.keys() {
            self.task_dependencies.entry(key.clone()).or_default();
        }

        for edge in self.dep_satisfaction.values() {
            let upstream_key = (edge.run_id.clone(), edge.upstream_task_key.clone());
            let downstream_key = (edge.run_id.clone(), edge.downstream_task_key.clone());

            let deps = self
                .task_dependencies
                .entry(downstream_key.clone())
                .or_default();
            if !deps.contains(&edge.upstream_task_key) {
                deps.push(edge.upstream_task_key.clone());
            }

            let dependents = self.task_dependents.entry(upstream_key).or_default();
            if !dependents.contains(&edge.downstream_task_key) {
                dependents.push(edge.downstream_task_key.clone());
            }
        }
    }

    /// Processes an orchestration event and updates state.
    #[allow(clippy::too_many_lines)]
    pub fn fold_event(&mut self, event: &OrchestrationEvent) {
        if self
            .idempotency_keys
            .contains_key(event.idempotency_key.as_str())
        {
            return;
        }

        self.record_idempotency_key(event);

        match &event.data {
            OrchestrationEventData::RunTriggered {
                run_id,
                plan_id,
                run_key,
                labels,
                ..
            } => {
                self.fold_run_triggered(
                    run_id,
                    plan_id,
                    run_key.clone(),
                    labels.clone(),
                    &event.event_id,
                    event.timestamp,
                );
            }
            OrchestrationEventData::PlanCreated { run_id, tasks, .. } => {
                self.fold_plan_created(run_id, tasks, &event.event_id, event.timestamp);
            }
            OrchestrationEventData::TaskStarted {
                run_id,
                task_key,
                attempt,
                attempt_id,
                ..
            } => {
                self.fold_task_started(
                    run_id,
                    task_key,
                    *attempt,
                    attempt_id,
                    event.timestamp,
                    &event.event_id,
                );
            }
            OrchestrationEventData::TaskHeartbeat {
                run_id,
                task_key,
                attempt,
                attempt_id,
                heartbeat_at,
                ..
            } => {
                // heartbeat_at is optional; use event timestamp as fallback
                let ts = heartbeat_at.unwrap_or(event.timestamp);
                self.fold_task_heartbeat(
                    run_id,
                    task_key,
                    *attempt,
                    attempt_id,
                    ts,
                    &event.event_id,
                );
            }
            OrchestrationEventData::TaskFinished {
                run_id,
                task_key,
                attempt,
                attempt_id,
                outcome,
                error_message,
                ..
            } => {
                self.fold_task_finished(
                    run_id,
                    task_key,
                    *attempt,
                    attempt_id,
                    *outcome,
                    error_message.clone(),
                    &event.event_id,
                    event.timestamp,
                );
            }
            OrchestrationEventData::DispatchRequested {
                run_id,
                task_key,
                attempt,
                attempt_id,
                worker_queue,
                dispatch_id,
            } => {
                self.fold_dispatch_requested(
                    run_id,
                    task_key,
                    *attempt,
                    attempt_id,
                    worker_queue,
                    dispatch_id,
                    &event.event_id,
                    event.timestamp,
                );
            }
            OrchestrationEventData::DispatchEnqueued {
                dispatch_id,
                run_id,
                task_key,
                attempt,
                cloud_task_id,
            } => {
                self.fold_dispatch_enqueued(
                    dispatch_id,
                    run_id.as_deref(),
                    task_key.as_deref(),
                    *attempt,
                    cloud_task_id,
                    &event.event_id,
                    event.timestamp,
                );
            }
            OrchestrationEventData::TimerRequested {
                timer_id,
                timer_type,
                run_id,
                task_key,
                attempt,
                fire_at,
            } => {
                self.fold_timer_requested(
                    timer_id,
                    *timer_type,
                    run_id.as_deref(),
                    task_key.as_deref(),
                    *attempt,
                    *fire_at,
                    &event.event_id,
                );
            }
            OrchestrationEventData::TimerEnqueued {
                timer_id,
                run_id,
                task_key,
                attempt,
                cloud_task_id,
            } => {
                self.fold_timer_enqueued(
                    timer_id,
                    run_id.as_deref(),
                    task_key.as_deref(),
                    *attempt,
                    cloud_task_id,
                    &event.event_id,
                );
            }
            OrchestrationEventData::TimerFired {
                timer_id,
                timer_type,
                run_id,
                task_key,
                attempt,
            } => {
                self.fold_timer_fired(
                    timer_id,
                    *timer_type,
                    run_id.as_deref(),
                    task_key.as_deref(),
                    *attempt,
                    &event.event_id,
                );
            }
            OrchestrationEventData::RunCancelRequested { run_id, .. } => {
                self.fold_run_cancel_requested(run_id, &event.event_id, event.timestamp);
            }

            // Layer 2 automation events
            OrchestrationEventData::ScheduleTicked {
                schedule_id,
                scheduled_for,
                tick_id,
                definition_version,
                asset_selection,
                partition_selection,
                status,
                run_key,
                request_fingerprint,
            } => {
                self.fold_schedule_ticked(
                    &event.tenant_id,
                    &event.workspace_id,
                    schedule_id,
                    *scheduled_for,
                    tick_id,
                    definition_version,
                    asset_selection,
                    partition_selection.clone(),
                    status.clone(),
                    run_key.clone(),
                    request_fingerprint.clone(),
                    &event.event_id,
                );
            }

            // Layer 2 sensor events
            OrchestrationEventData::SensorEvaluated {
                sensor_id,
                eval_id,
                cursor_before,
                cursor_after,
                expected_state_version,
                trigger_source,
                run_requests,
                status,
            } => {
                self.fold_sensor_evaluated(
                    &event.tenant_id,
                    &event.workspace_id,
                    sensor_id,
                    eval_id,
                    cursor_before.clone(),
                    cursor_after.clone(),
                    *expected_state_version,
                    trigger_source,
                    run_requests,
                    status,
                    event.timestamp,
                    &event.event_id,
                );
            }

            // Layer 2 automation events - stubs for remaining events
            OrchestrationEventData::RunRequested {
                trigger_source_ref, ..
            } => {
                if !self.should_drop_run_requested(trigger_source_ref) {
                    // TODO(Task 6.1): Implement fold_run_requested (run_id + projection).
                }
            }
            OrchestrationEventData::BackfillCreated {
                backfill_id,
                client_request_id: _,
                asset_selection,
                partition_selector,
                total_partitions,
                chunk_size,
                max_concurrent_runs,
                parent_backfill_id,
            } => {
                self.fold_backfill_created(
                    &event.tenant_id,
                    &event.workspace_id,
                    backfill_id,
                    asset_selection,
                    partition_selector,
                    *total_partitions,
                    *chunk_size,
                    *max_concurrent_runs,
                    parent_backfill_id.as_ref(),
                    event.timestamp,
                    &event.event_id,
                );
            }
            OrchestrationEventData::BackfillChunkPlanned {
                backfill_id,
                chunk_id,
                chunk_index,
                partition_keys,
                run_key,
                request_fingerprint: _,
            } => {
                self.fold_backfill_chunk_planned(
                    &event.tenant_id,
                    &event.workspace_id,
                    backfill_id,
                    chunk_id,
                    *chunk_index,
                    partition_keys,
                    run_key,
                    &event.event_id,
                );
            }
            OrchestrationEventData::BackfillStateChanged {
                backfill_id,
                from_state,
                to_state,
                state_version,
                changed_by: _,
            } => {
                self.fold_backfill_state_changed(
                    backfill_id,
                    *from_state,
                    *to_state,
                    *state_version,
                    &event.event_id,
                );
            }
        }
    }

    fn fold_run_cancel_requested(
        &mut self,
        run_id: &str,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        // Check if run exists and is not already terminal
        let run_is_terminal = self.runs.get(run_id).is_some_and(|r| r.state.is_terminal());
        if run_is_terminal {
            return;
        }

        // Set cancel_requested flag on run
        if let Some(run) = self.runs.get_mut(run_id) {
            run.cancel_requested = true;
            run.row_version = event_id.to_string();
        }

        // Collect task keys to cancel (non-terminal, non-running)
        // Running tasks will check cancel_requested on heartbeat and be signaled by workers
        let tasks_to_cancel: Vec<(String, String)> = self
            .tasks
            .iter()
            .filter(|((r, _), task)| {
                r == run_id && !task.state.is_terminal() && task.state != TaskState::Running
            })
            .map(|(key, _)| key.clone())
            .collect();

        // Cancel each task and update run counters
        for task_key in tasks_to_cancel {
            if let Some(task) = self.tasks.get_mut(&task_key) {
                task.state = TaskState::Cancelled;
                task.row_version = event_id.to_string();
            }

            if let Some(run) = self.runs.get_mut(run_id) {
                run.tasks_cancelled += 1;
                run.tasks_completed += 1;
                run.row_version = event_id.to_string();
            }
        }

        // Check if run is now complete (all tasks terminal)
        if let Some(run) = self.runs.get_mut(run_id) {
            if run.tasks_completed == run.tasks_total {
                run.state = RunState::Cancelled;
                run.completed_at = Some(timestamp);
                run.row_version = event_id.to_string();
            }
        }
    }

    fn fold_run_triggered(
        &mut self,
        run_id: &str,
        plan_id: &str,
        run_key: Option<String>,
        labels: HashMap<String, String>,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        if self.runs.contains_key(run_id) {
            return;
        }
        self.runs.insert(
            run_id.to_string(),
            RunRow {
                run_id: run_id.to_string(),
                plan_id: plan_id.to_string(),
                state: RunState::Triggered,
                run_key,
                labels,
                cancel_requested: false,
                tasks_total: 0,
                tasks_completed: 0,
                tasks_succeeded: 0,
                tasks_failed: 0,
                tasks_skipped: 0,
                tasks_cancelled: 0,
                triggered_at: timestamp,
                completed_at: None,
                row_version: event_id.to_string(),
            },
        );
    }

    fn fold_plan_created(
        &mut self,
        run_id: &str,
        task_defs: &[TaskDef],
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        if self.tasks.keys().any(|(r, _)| r == run_id) {
            return;
        }
        // Update run with task count
        if let Some(run) = self.runs.get_mut(run_id) {
            run.tasks_total = u32::try_from(task_defs.len()).unwrap_or(u32::MAX);
            run.state = RunState::Running;
            run.row_version = event_id.to_string();
        }

        // Build dependency graph
        for task_def in task_defs {
            let task_key = (run_id.to_string(), task_def.key.clone());

            // Store dependencies
            self.task_dependencies
                .insert(task_key.clone(), task_def.depends_on.clone());

            // Store dependents (reverse lookup)
            for upstream in &task_def.depends_on {
                self.task_dependents
                    .entry((run_id.to_string(), upstream.clone()))
                    .or_default()
                    .push(task_def.key.clone());
            }
        }

        // Create task rows and dependency edges
        for task_def in task_defs {
            let deps_total = u32::try_from(task_def.depends_on.len()).unwrap_or(u32::MAX);
            let initial_state = if deps_total == 0 {
                TaskState::Ready
            } else {
                TaskState::Blocked
            };

            let task_row = TaskRow {
                run_id: run_id.to_string(),
                task_key: task_def.key.clone(),
                state: initial_state,
                attempt: 0,
                attempt_id: None,
                started_at: None,
                completed_at: None,
                error_message: None,
                deps_total,
                deps_satisfied_count: 0,
                max_attempts: task_def.max_attempts,
                heartbeat_timeout_sec: task_def.heartbeat_timeout_sec,
                last_heartbeat_at: None,
                ready_at: if initial_state == TaskState::Ready {
                    Some(timestamp)
                } else {
                    None
                },
                asset_key: task_def.asset_key.clone(),
                partition_key: task_def.partition_key.clone(),
                row_version: event_id.to_string(),
            };

            self.tasks
                .insert((run_id.to_string(), task_def.key.clone()), task_row);

            // Create dependency satisfaction edges
            for upstream in &task_def.depends_on {
                let edge_key = (run_id.to_string(), upstream.clone(), task_def.key.clone());
                self.dep_satisfaction.insert(
                    edge_key,
                    DepSatisfactionRow {
                        run_id: run_id.to_string(),
                        upstream_task_key: upstream.clone(),
                        downstream_task_key: task_def.key.clone(),
                        satisfied: false,
                        resolution: None,
                        satisfied_at: None,
                        satisfying_attempt: None,
                        row_version: event_id.to_string(),
                    },
                );
            }
        }
    }

    fn fold_task_started(
        &mut self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        timestamp: DateTime<Utc>,
        event_id: &str,
    ) {
        let key = (run_id.to_string(), task_key.to_string());
        if let Some(task) = self.tasks.get_mut(&key) {
            if task.state.is_terminal() {
                return;
            }
            // Update if this is a newer or equal attempt (handles retries).
            // A TaskStarted for attempt N should always override attempt < N.
            if attempt >= task.attempt {
                if attempt == task.attempt {
                    if let Some(current_attempt_id) = task.attempt_id.as_deref() {
                        if current_attempt_id != attempt_id {
                            return;
                        }
                    }
                }
                task.state = TaskState::Running;
                task.attempt = attempt;
                task.attempt_id = Some(attempt_id.to_string());
                task.started_at = Some(timestamp);
                task.row_version = event_id.to_string();
            }
        }
    }

    fn fold_task_heartbeat(
        &mut self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        heartbeat_at: DateTime<Utc>,
        event_id: &str,
    ) {
        let key = (run_id.to_string(), task_key.to_string());
        if let Some(task) = self.tasks.get_mut(&key) {
            if task.state.is_terminal() {
                return;
            }
            // Only update if attempt_id matches (concurrency guard per ADR-022)
            if task.attempt_id.as_deref() == Some(attempt_id) && task.attempt == attempt {
                task.last_heartbeat_at = Some(heartbeat_at);
                task.row_version = event_id.to_string();
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fold_task_finished(
        &mut self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        outcome: TaskOutcome,
        error_message: Option<String>,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        let key = (run_id.to_string(), task_key.to_string());

        if let Some(task) = self.tasks.get(&key) {
            if task.state.is_terminal()
                && task.attempt == attempt
                && task.attempt_id.as_deref() == Some(attempt_id)
            {
                return;
            }
        }

        // Check if this is a stale attempt event (INV-8)
        let is_current_attempt = self
            .tasks
            .get(&key)
            .is_some_and(|t| t.attempt_id.as_deref() == Some(attempt_id) && t.attempt == attempt);

        if !is_current_attempt && self.tasks.contains_key(&key) {
            // Stale attempt event - reject to prevent state regression
            return;
        }

        if let Some(task) = self.tasks.get_mut(&key) {
            let new_state = match outcome {
                TaskOutcome::Succeeded => TaskState::Succeeded,
                TaskOutcome::Failed => {
                    if task.attempt < task.max_attempts {
                        TaskState::RetryWait
                    } else {
                        TaskState::Failed
                    }
                }
                TaskOutcome::Skipped => TaskState::Skipped,
                TaskOutcome::Cancelled => TaskState::Cancelled,
            };

            task.state = new_state;
            task.row_version = event_id.to_string();
            if new_state.is_terminal() {
                task.completed_at = Some(timestamp);
            }
            match outcome {
                TaskOutcome::Succeeded => {
                    task.error_message = None;
                }
                TaskOutcome::Failed | TaskOutcome::Skipped | TaskOutcome::Cancelled => {
                    task.error_message = error_message;
                }
            }

            // Update run counters
            if new_state.is_terminal() {
                if let Some(run) = self.runs.get_mut(run_id) {
                    run.tasks_completed += 1;
                    match new_state {
                        TaskState::Succeeded => run.tasks_succeeded += 1,
                        TaskState::Failed => run.tasks_failed += 1,
                        TaskState::Skipped => run.tasks_skipped += 1,
                        TaskState::Cancelled => run.tasks_cancelled += 1,
                        _ => {}
                    }
                    run.row_version = event_id.to_string();

                    // Check if run is complete
                    if run.tasks_completed == run.tasks_total {
                        run.state = if run.tasks_cancelled > 0 {
                            RunState::Cancelled
                        } else if run.tasks_failed > 0 {
                            RunState::Failed
                        } else {
                            RunState::Succeeded
                        };
                        run.completed_at = Some(timestamp);
                    }
                }
            }

            // Satisfy downstream dependencies (for success)
            if outcome == TaskOutcome::Succeeded {
                self.satisfy_downstream_edges(
                    run_id,
                    task_key,
                    DepResolution::Success,
                    attempt,
                    event_id,
                    timestamp,
                );
            } else if outcome == TaskOutcome::Failed && new_state == TaskState::Failed {
                // Terminal failure - propagate to downstream
                self.propagate_failure(
                    run_id,
                    task_key,
                    DepResolution::Failed,
                    event_id,
                    timestamp,
                );
            } else if outcome == TaskOutcome::Skipped {
                self.propagate_failure(
                    run_id,
                    task_key,
                    DepResolution::Skipped,
                    event_id,
                    timestamp,
                );
            } else if outcome == TaskOutcome::Cancelled {
                self.propagate_failure(
                    run_id,
                    task_key,
                    DepResolution::Cancelled,
                    event_id,
                    timestamp,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fold_dispatch_requested(
        &mut self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        worker_queue: &str,
        dispatch_id: &str,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        let task_lookup_key = (run_id.to_string(), task_key.to_string());
        let mut outbox_attempt_id: Option<String> = None;
        let mut outbox_exists = false;

        if let Some(existing) = self.dispatch_outbox.get_mut(dispatch_id) {
            outbox_exists = true;
            if existing.attempt_id.is_empty() {
                existing.attempt_id = attempt_id.to_string();
            }
            if existing.worker_queue.is_empty()
                || (existing.worker_queue == "default-queue" && worker_queue != "default-queue")
            {
                existing.worker_queue = worker_queue.to_string();
            }
            outbox_attempt_id = Some(existing.attempt_id.clone());
        }

        if let Some(task) = self.tasks.get_mut(&task_lookup_key) {
            if !task.state.is_terminal()
                && matches!(
                    task.state,
                    TaskState::Ready | TaskState::RetryWait | TaskState::Dispatched
                )
                && attempt >= task.attempt
            {
                let desired_attempt_id = outbox_attempt_id.as_deref().unwrap_or(attempt_id);
                let should_update = task.state != TaskState::Dispatched
                    || task.attempt != attempt
                    || task.attempt_id.as_deref() != Some(desired_attempt_id);

                if should_update {
                    task.state = TaskState::Dispatched;
                    task.attempt = attempt;
                    task.attempt_id = Some(desired_attempt_id.to_string());
                    task.row_version = event_id.to_string();
                }
            }
        }

        if outbox_exists {
            // Row already exists (e.g., DispatchEnqueued arrived first out-of-order)
            // Fill in missing critical fields regardless of event ordering
            // Don't update row_version or status - the existing row has newer state
            return;
        }

        self.dispatch_outbox.insert(
            dispatch_id.to_string(),
            DispatchOutboxRow {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                dispatch_id: dispatch_id.to_string(),
                cloud_task_id: None,
                status: DispatchStatus::Pending,
                attempt_id: attempt_id.to_string(),
                worker_queue: worker_queue.to_string(),
                created_at: timestamp,
                row_version: event_id.to_string(),
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn fold_dispatch_enqueued(
        &mut self,
        dispatch_id: &str,
        run_id: Option<&str>,
        task_key: Option<&str>,
        attempt: Option<u32>,
        cloud_task_id: &str,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        if let Some(existing) = self.dispatch_outbox.get_mut(dispatch_id) {
            if event_id <= existing.row_version.as_str() {
                return;
            }
            existing.status = DispatchStatus::Created;
            existing.cloud_task_id = Some(cloud_task_id.to_string());
            existing.row_version = event_id.to_string();
            return;
        }

        // Handle out-of-order event: create row if it doesn't exist
        // This handles the case where DispatchEnqueued arrives before DispatchRequested
        if let (Some(run_id), Some(task_key), Some(attempt)) = (run_id, task_key, attempt) {
            self.dispatch_outbox.insert(
                dispatch_id.to_string(),
                DispatchOutboxRow {
                    run_id: run_id.to_string(),
                    task_key: task_key.to_string(),
                    attempt,
                    dispatch_id: dispatch_id.to_string(),
                    cloud_task_id: Some(cloud_task_id.to_string()),
                    status: DispatchStatus::Created,
                    attempt_id: String::new(), // Will be filled by DispatchRequested when it arrives
                    worker_queue: "default-queue".to_string(),
                    created_at: timestamp,
                    row_version: event_id.to_string(),
                },
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fold_timer_requested(
        &mut self,
        timer_id: &str,
        timer_type: EventTimerType,
        run_id: Option<&str>,
        task_key: Option<&str>,
        attempt: Option<u32>,
        fire_at: DateTime<Utc>,
        event_id: &str,
    ) {
        if let Some(existing) = self.timers.get_mut(timer_id) {
            // Row already exists (e.g., TimerEnqueued arrived first out-of-order)
            // Fill in the authoritative fire_at from TimerRequested
            // The existing row may have a fire_at derived from parsing timer_id,
            // but TimerRequested has the canonical value
            existing.fire_at = fire_at;
            if existing.run_id.is_none() {
                existing.run_id = run_id.map(ToString::to_string);
            }
            if existing.task_key.is_none() {
                existing.task_key = task_key.map(ToString::to_string);
            }
            if existing.attempt.is_none() {
                existing.attempt = attempt;
            }
            // Don't update row_version or cloud_task_id - the existing row has newer state
            return;
        }

        self.timers.insert(
            timer_id.to_string(),
            TimerRow {
                timer_id: timer_id.to_string(),
                cloud_task_id: None,
                timer_type: map_timer_type(timer_type),
                run_id: run_id.map(ToString::to_string),
                task_key: task_key.map(ToString::to_string),
                attempt,
                fire_at,
                state: TimerState::Scheduled,
                payload: None,
                row_version: event_id.to_string(),
            },
        );
    }

    fn fold_timer_enqueued(
        &mut self,
        timer_id: &str,
        run_id: Option<&str>,
        task_key: Option<&str>,
        attempt: Option<u32>,
        cloud_task_id: &str,
        event_id: &str,
    ) {
        if let Some(existing) = self.timers.get_mut(timer_id) {
            if event_id <= existing.row_version.as_str() {
                return;
            }
            existing.cloud_task_id = Some(cloud_task_id.to_string());
            existing.row_version = event_id.to_string();
            return;
        }

        // Handle out-of-order event: create row if it doesn't exist
        // This handles the case where TimerEnqueued arrives before TimerRequested
        // Parse timer_id to extract timer_type and fire_epoch
        if let Some(parsed) = TimerRow::parse_timer_id(timer_id) {
            let ParsedTimerId {
                timer_type,
                fire_epoch,
                run_id: parsed_run_id,
                task_key: parsed_task_key,
                attempt: parsed_attempt,
            } = parsed;

            let fire_at = DateTime::from_timestamp(fire_epoch, 0).unwrap_or_else(Utc::now);
            let resolved_run_id = run_id.map(ToString::to_string).or(parsed_run_id);
            let resolved_task_key = task_key.map(ToString::to_string).or(parsed_task_key);
            let resolved_attempt = attempt.or(parsed_attempt);

            self.timers.insert(
                timer_id.to_string(),
                TimerRow {
                    timer_id: timer_id.to_string(),
                    cloud_task_id: Some(cloud_task_id.to_string()),
                    timer_type,
                    run_id: resolved_run_id,
                    task_key: resolved_task_key,
                    attempt: resolved_attempt,
                    fire_at,
                    state: TimerState::Scheduled,
                    payload: None,
                    row_version: event_id.to_string(),
                },
            );
        }
    }

    fn fold_timer_fired(
        &mut self,
        timer_id: &str,
        _timer_type: EventTimerType,
        run_id: Option<&str>,
        task_key: Option<&str>,
        attempt: Option<u32>,
        event_id: &str,
    ) {
        if let Some(existing) = self.timers.get_mut(timer_id) {
            if event_id <= existing.row_version.as_str() {
                return;
            }
            existing.state = TimerState::Fired;
            existing.row_version = event_id.to_string();
            return;
        }

        if let Some(parsed) = TimerRow::parse_timer_id(timer_id) {
            let ParsedTimerId {
                timer_type,
                fire_epoch,
                run_id: parsed_run_id,
                task_key: parsed_task_key,
                attempt: parsed_attempt,
            } = parsed;
            let fire_at = DateTime::from_timestamp(fire_epoch, 0).unwrap_or_else(Utc::now);
            let resolved_run_id = run_id.map(ToString::to_string).or(parsed_run_id);
            let resolved_task_key = task_key.map(ToString::to_string).or(parsed_task_key);
            let resolved_attempt = attempt.or(parsed_attempt);

            self.timers.insert(
                timer_id.to_string(),
                TimerRow {
                    timer_id: timer_id.to_string(),
                    cloud_task_id: None,
                    timer_type,
                    run_id: resolved_run_id,
                    task_key: resolved_task_key,
                    attempt: resolved_attempt,
                    fire_at,
                    state: TimerState::Fired,
                    payload: None,
                    row_version: event_id.to_string(),
                },
            );
        }
    }

    // ========================================================================
    // Layer 2: Schedule Fold Logic
    // ========================================================================

    /// Folds a `ScheduleTicked` event into state.
    ///
    /// Updates:
    /// 1. `schedule_state` - Last tick info for the schedule
    /// 2. `schedule_ticks` - Tick history row
    ///
    /// Per P0-1, this is pure projection - fold does NOT emit new events.
    /// The controller emits `ScheduleTicked` + `RunRequested` atomically.
    #[allow(clippy::too_many_arguments)]
    fn fold_schedule_ticked(
        &mut self,
        tenant_id: &str,
        workspace_id: &str,
        schedule_id: &str,
        scheduled_for: DateTime<Utc>,
        tick_id: &str,
        definition_version: &str,
        asset_selection: &[String],
        partition_selection: Option<Vec<String>>,
        status: TickStatus,
        run_key: Option<String>,
        request_fingerprint: Option<String>,
        event_id: &str,
    ) {
        // Update schedule state
        let state = self
            .schedule_state
            .entry(schedule_id.to_string())
            .or_insert_with(|| ScheduleStateRow {
                tenant_id: tenant_id.to_string(),
                workspace_id: workspace_id.to_string(),
                schedule_id: schedule_id.to_string(),
                last_scheduled_for: None,
                last_tick_id: None,
                last_run_key: None,
                row_version: String::new(),
            });

        // Only update if this event is newer
        if event_id > state.row_version.as_str() {
            state.last_scheduled_for = Some(scheduled_for);
            state.last_tick_id = Some(tick_id.to_string());
            state.last_run_key.clone_from(&run_key);
            state.row_version = event_id.to_string();
        }

        // Create or update tick history row
        // Check for duplicate tick_id (idempotency)
        if let Some(existing) = self.schedule_ticks.get(tick_id) {
            // Duplicate event - only update if newer event_id
            if event_id <= existing.row_version.as_str() {
                return;
            }
        }

        self.schedule_ticks.insert(
            tick_id.to_string(),
            ScheduleTickRow {
                tenant_id: tenant_id.to_string(),
                workspace_id: workspace_id.to_string(),
                tick_id: tick_id.to_string(),
                schedule_id: schedule_id.to_string(),
                scheduled_for,
                definition_version: definition_version.to_string(),
                asset_selection: asset_selection.to_vec(),
                partition_selection,
                status,
                run_key,
                run_id: None, // Filled by fold_run_requested correlation
                request_fingerprint,
                row_version: event_id.to_string(),
            },
        );
    }

    /// Fold a `SensorEvaluated` event.
    ///
    /// Updates projections:
    /// 1. `sensor_state` - Cursor and evaluation info (with CAS check)
    /// 2. `sensor_evals` - Evaluation history (including stale evals)
    ///
    /// Per P0-1, this is pure projection - fold does NOT emit new events.
    /// The controller emits `SensorEvaluated` + `RunRequested`(s) atomically.
    ///
    /// Per P0-2, poll sensors include `expected_state_version` for CAS.
    /// If the expected version doesn't match, the evaluation is stale and dropped.
    #[allow(clippy::too_many_arguments)]
    fn fold_sensor_evaluated(
        &mut self,
        tenant_id: &str,
        workspace_id: &str,
        sensor_id: &str,
        eval_id: &str,
        cursor_before: Option<String>,
        cursor_after: Option<String>,
        expected_state_version: Option<u32>,
        trigger_source: &TriggerSource,
        run_requests: &[RunRequest],
        status: &SensorEvalStatus,
        timestamp: DateTime<Utc>,
        event_id: &str,
    ) {
        let new_state = || SensorStateRow {
            tenant_id: tenant_id.to_string(),
            workspace_id: workspace_id.to_string(),
            sensor_id: sensor_id.to_string(),
            cursor: None,
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 0,
            row_version: String::new(),
        };

        // Get current state version without holding a mutable borrow.
        let state_version = {
            let state = self
                .sensor_state
                .entry(sensor_id.to_string())
                .or_insert_with(new_state);
            state.state_version
        };

        // CAS check for poll sensors (P0-2)
        // If expected_state_version is set and doesn't match, drop this stale evaluation
        let is_stale = expected_state_version.is_some_and(|expected| state_version != expected);

        let eval_status = if is_stale {
            SensorEvalStatus::SkippedStaleCursor
        } else {
            status.clone()
        };

        self.upsert_sensor_eval(SensorEvalRow {
            tenant_id: tenant_id.to_string(),
            workspace_id: workspace_id.to_string(),
            eval_id: eval_id.to_string(),
            sensor_id: sensor_id.to_string(),
            cursor_before,
            cursor_after: cursor_after.clone(),
            expected_state_version,
            trigger_source: trigger_source.clone(),
            run_requests: run_requests.to_vec(),
            status: eval_status,
            evaluated_at: timestamp,
            row_version: event_id.to_string(),
        });

        if is_stale {
            counter!(
                metrics_names::SENSOR_EVALS_TOTAL,
                metrics_labels::SENSOR_TYPE => "poll".to_string(),
                metrics_labels::STATUS => "skipped_stale".to_string(),
            )
            .increment(1);
            return;
        }

        let state = self
            .sensor_state
            .entry(sensor_id.to_string())
            .or_insert_with(new_state);

        // Only update if this event is newer (idempotency)
        if event_id <= state.row_version.as_str() {
            return;
        }

        // Update sensor state
        state.cursor = cursor_after;
        state.last_evaluation_at = Some(timestamp);
        state.last_eval_id = Some(eval_id.to_string());
        state.row_version = event_id.to_string();

        // Increment state_version for CAS (poll sensors)
        state.state_version += 1;

        // Update status if evaluation errored
        if matches!(status, SensorEvalStatus::Error { .. }) {
            state.status = SensorStatus::Error;
        }

        // NOTE: Per P0-1, fold does NOT emit RunRequested events.
        // The controller already emitted SensorEvaluated + RunRequested(s) atomically.
        // fold_run_requested handles each RunRequested event separately.
    }

    fn upsert_sensor_eval(&mut self, row: SensorEvalRow) {
        if let Some(existing) = self.sensor_evals.get(row.eval_id.as_str()) {
            if row.row_version <= existing.row_version {
                return;
            }
        }
        self.sensor_evals.insert(row.eval_id.clone(), row);
    }

    // ========================================================================
    // Layer 2: Backfill Fold Logic
    // ========================================================================

    #[allow(clippy::too_many_arguments)]
    fn fold_backfill_created(
        &mut self,
        tenant_id: &str,
        workspace_id: &str,
        backfill_id: &str,
        asset_selection: &[String],
        partition_selector: &PartitionSelector,
        total_partitions: u32,
        chunk_size: u32,
        max_concurrent_runs: u32,
        parent_backfill_id: Option<&String>,
        timestamp: DateTime<Utc>,
        event_id: &str,
    ) {
        if let Some(existing) = self.backfills.get(backfill_id) {
            if event_id <= existing.row_version.as_str() {
                return;
            }
            // Do not overwrite existing backfills with the same ID.
            return;
        }

        let mut planned_chunks = 0_u32;
        if chunk_size > 0 {
            loop {
                let chunk_id = format!("{backfill_id}:{planned_chunks}");
                if self.backfill_chunks.contains_key(&chunk_id) {
                    planned_chunks += 1;
                } else {
                    break;
                }
            }
        }

        self.backfills.insert(
            backfill_id.to_string(),
            BackfillRow {
                tenant_id: tenant_id.to_string(),
                workspace_id: workspace_id.to_string(),
                backfill_id: backfill_id.to_string(),
                asset_selection: asset_selection.to_vec(),
                partition_selector: partition_selector.clone(),
                chunk_size,
                max_concurrent_runs,
                state: BackfillState::Running,
                state_version: 1,
                total_partitions,
                planned_chunks,
                completed_chunks: 0,
                failed_chunks: 0,
                parent_backfill_id: parent_backfill_id.cloned(),
                created_at: timestamp,
                row_version: event_id.to_string(),
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn fold_backfill_chunk_planned(
        &mut self,
        tenant_id: &str,
        workspace_id: &str,
        backfill_id: &str,
        chunk_id: &str,
        chunk_index: u32,
        partition_keys: &[String],
        run_key: &str,
        event_id: &str,
    ) {
        let existing_run_id = self
            .backfill_chunks
            .get(chunk_id)
            .and_then(|row| row.run_id.clone());

        if let Some(existing) = self.backfill_chunks.get(chunk_id) {
            if event_id <= existing.row_version.as_str() {
                return;
            }
        }

        self.backfill_chunks.insert(
            chunk_id.to_string(),
            BackfillChunkRow {
                tenant_id: tenant_id.to_string(),
                workspace_id: workspace_id.to_string(),
                chunk_id: chunk_id.to_string(),
                backfill_id: backfill_id.to_string(),
                chunk_index,
                partition_keys: partition_keys.to_vec(),
                run_key: run_key.to_string(),
                run_id: existing_run_id,
                state: ChunkState::Planned,
                row_version: event_id.to_string(),
            },
        );

        if let Some(backfill) = self.backfills.get_mut(backfill_id) {
            let mut next_index = backfill.planned_chunks;
            loop {
                let contiguous_id = format!("{backfill_id}:{next_index}");
                if self.backfill_chunks.contains_key(&contiguous_id) {
                    next_index += 1;
                } else {
                    break;
                }
            }

            if next_index != backfill.planned_chunks {
                backfill.planned_chunks = next_index;
                backfill.row_version = event_id.to_string();
            }
        }
    }

    fn fold_backfill_state_changed(
        &mut self,
        backfill_id: &str,
        from_state: BackfillState,
        to_state: BackfillState,
        state_version: u32,
        event_id: &str,
    ) {
        let Some(backfill) = self.backfills.get_mut(backfill_id) else {
            return;
        };

        if event_id <= backfill.row_version.as_str() {
            return;
        }

        if backfill.state != from_state {
            return;
        }

        if !BackfillState::is_valid_transition(backfill.state, to_state) {
            return;
        }

        let expected_version = backfill.state_version.saturating_add(1);
        if state_version != expected_version {
            return;
        }

        backfill.state = to_state;
        backfill.state_version = state_version;
        backfill.row_version = event_id.to_string();
    }

    fn record_idempotency_key(&mut self, event: &OrchestrationEvent) {
        self.idempotency_keys
            .entry(event.idempotency_key.clone())
            .or_insert_with(|| IdempotencyKeyRow {
                tenant_id: event.tenant_id.clone(),
                workspace_id: event.workspace_id.clone(),
                idempotency_key: event.idempotency_key.clone(),
                event_id: event.event_id.clone(),
                event_type: event.event_type.clone(),
                recorded_at: event.timestamp,
                row_version: event.event_id.clone(),
            });
    }

    fn should_drop_run_requested(&self, trigger_source_ref: &SourceRef) -> bool {
        if let SourceRef::Sensor { eval_id, .. } = trigger_source_ref {
            if let Some(eval) = self.sensor_evals.get(eval_id) {
                return matches!(eval.status, SensorEvalStatus::SkippedStaleCursor);
            }
        }
        false
    }

    fn satisfy_downstream_edges(
        &mut self,
        run_id: &str,
        upstream_key: &str,
        resolution: DepResolution,
        attempt: u32,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        let dependents_key = (run_id.to_string(), upstream_key.to_string());
        let dependents = self
            .task_dependents
            .get(&dependents_key)
            .cloned()
            .unwrap_or_default();

        for downstream_key in dependents {
            let edge_key = (
                run_id.to_string(),
                upstream_key.to_string(),
                downstream_key.clone(),
            );

            // Check if edge was already satisfied (duplicate-safe per ADR-022)
            let was_satisfied = self
                .dep_satisfaction
                .get(&edge_key)
                .is_some_and(|e| e.satisfied);

            // Upsert edge (idempotent)
            self.dep_satisfaction.insert(
                edge_key.clone(),
                DepSatisfactionRow {
                    run_id: run_id.to_string(),
                    upstream_task_key: upstream_key.to_string(),
                    downstream_task_key: downstream_key.clone(),
                    satisfied: true,
                    resolution: Some(resolution),
                    satisfied_at: Some(timestamp),
                    satisfying_attempt: Some(attempt),
                    row_version: event_id.to_string(),
                },
            );

            // Only increment if newly satisfied (critical for duplicate safety)
            if !was_satisfied {
                let task_key = (run_id.to_string(), downstream_key.clone());
                if let Some(downstream_task) = self.tasks.get_mut(&task_key) {
                    downstream_task.deps_satisfied_count += 1;

                    // Check if downstream is now ready
                    if downstream_task.deps_satisfied_count == downstream_task.deps_total
                        && downstream_task.state == TaskState::Blocked
                    {
                        downstream_task.state = TaskState::Ready;
                        downstream_task.ready_at = Some(timestamp);
                        downstream_task.row_version = event_id.to_string();
                    }
                }
            }
        }
    }

    fn propagate_failure(
        &mut self,
        run_id: &str,
        failed_task_key: &str,
        initial_resolution: DepResolution,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        // Queue of (upstream_key, resolution_for_this_edge)
        // The first task uses the initial resolution (FAILED/SKIPPED/CANCELLED)
        // Transitive resolution depends on the initial outcome.
        let mut to_propagate: VecDeque<(String, DepResolution)> = VecDeque::new();
        to_propagate.push_back((failed_task_key.to_string(), initial_resolution));

        while let Some((upstream_key, edge_resolution)) = to_propagate.pop_front() {
            let dependents_key = (run_id.to_string(), upstream_key.clone());
            let dependents = self
                .task_dependents
                .get(&dependents_key)
                .cloned()
                .unwrap_or_default();

            for downstream_key in dependents {
                let edge_key = (
                    run_id.to_string(),
                    upstream_key.clone(),
                    downstream_key.clone(),
                );

                // Mark edge as resolved (not satisfied)
                self.dep_satisfaction.insert(
                    edge_key,
                    DepSatisfactionRow {
                        run_id: run_id.to_string(),
                        upstream_task_key: upstream_key.clone(),
                        downstream_task_key: downstream_key.clone(),
                        satisfied: false,
                        resolution: Some(edge_resolution),
                        satisfied_at: Some(timestamp),
                        satisfying_attempt: None,
                        row_version: event_id.to_string(),
                    },
                );

                // Skip/cancel downstream if not already terminal
                let task_key = (run_id.to_string(), downstream_key.clone());
                if let Some(downstream_task) = self.tasks.get_mut(&task_key) {
                    if !downstream_task.state.is_terminal() {
                        let (new_state, next_resolution) = match edge_resolution {
                            DepResolution::Cancelled => {
                                (TaskState::Cancelled, DepResolution::Cancelled)
                            }
                            _ => (TaskState::Skipped, DepResolution::Skipped),
                        };

                        downstream_task.state = new_state;
                        downstream_task.row_version = event_id.to_string();
                        to_propagate.push_back((downstream_key, next_resolution));

                        // Update run counters
                        if let Some(run) = self.runs.get_mut(run_id) {
                            run.tasks_completed += 1;
                            match new_state {
                                TaskState::Skipped => run.tasks_skipped += 1,
                                TaskState::Cancelled => run.tasks_cancelled += 1,
                                _ => {}
                            }
                            run.row_version = event_id.to_string();

                            // Check if run is complete
                            if run.tasks_completed == run.tasks_total {
                                run.state = if run.tasks_cancelled > 0 {
                                    RunState::Cancelled
                                } else if run.tasks_failed > 0 {
                                    RunState::Failed
                                } else {
                                    RunState::Succeeded
                                };
                                run.completed_at = Some(timestamp);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns all tasks that are ready but not yet dispatched.
    #[must_use]
    pub fn ready_tasks(&self) -> Vec<&TaskRow> {
        self.tasks
            .values()
            .filter(|t| t.state == TaskState::Ready)
            .collect()
    }

    /// Returns the dependents of a task.
    #[must_use]
    pub fn get_dependents(&self, run_id: &str, task_key: &str) -> Vec<String> {
        self.task_dependents
            .get(&(run_id.to_string(), task_key.to_string()))
            .cloned()
            .unwrap_or_default()
    }
}

// ============================================================================
// Merge Functions for Base + L0 Delta Compaction
// ============================================================================

/// Merges task rows from base snapshot and L0 deltas.
///
/// Uses `row_version` (ULID) as primary ordering key, with `state_rank` as tiebreaker.
/// If both are equal, prefer the later row to preserve updates from newer deltas.
#[must_use]
pub fn merge_task_rows(rows: Vec<TaskRow>) -> Option<TaskRow> {
    rows.into_iter().reduce(|best, row| {
        // Compare by row_version first (ULID lexicographic ordering)
        match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Greater => row,
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal => {
                // Same row_version: use state_rank as tiebreaker
                // Higher rank = more terminal = preferred
                match row.state.rank().cmp(&best.state.rank()) {
                    std::cmp::Ordering::Less => best,
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
                }
            }
        }
    })
}

/// Merges run rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_run_rows(rows: Vec<RunRow>) -> Option<RunRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

/// Merges dependency satisfaction rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_dep_satisfaction_rows(rows: Vec<DepSatisfactionRow>) -> Option<DepSatisfactionRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

/// Merges dispatch outbox rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_dispatch_outbox_rows(rows: Vec<DispatchOutboxRow>) -> Option<DispatchOutboxRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

/// Merges timer rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_timer_rows(rows: Vec<TimerRow>) -> Option<TimerRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

/// Merges sensor state rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_sensor_state_rows(rows: Vec<SensorStateRow>) -> Option<SensorStateRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

/// Merges sensor evaluation rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_sensor_eval_rows(rows: Vec<SensorEvalRow>) -> Option<SensorEvalRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

/// Merges idempotency key rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_idempotency_key_rows(rows: Vec<IdempotencyKeyRow>) -> Option<IdempotencyKeyRow> {
    rows.into_iter()
        .reduce(|best, row| match row.row_version.cmp(&best.row_version) {
            std::cmp::Ordering::Less => best,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => row,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::events::{
        OrchestrationEventData, SourceRef, TimerType as EventTimerType, TriggerInfo, TriggerSource,
    };
    use ulid::Ulid;

    fn make_event(data: OrchestrationEventData) -> OrchestrationEvent {
        OrchestrationEvent::new("tenant-test", "workspace-test", data)
    }

    fn run_triggered_event(run_id: &str) -> OrchestrationEvent {
        make_event(OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: "plan-01".to_string(),
            trigger: TriggerInfo::Manual {
                user_id: "user@test.com".to_string(),
            },
            root_assets: vec![],
            run_key: None,
            labels: HashMap::new(),
        })
    }

    fn plan_created_event(run_id: &str, tasks: Vec<TaskDef>) -> OrchestrationEvent {
        make_event(OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: "plan-01".to_string(),
            tasks,
        })
    }

    fn task_finished_event(
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        outcome: TaskOutcome,
    ) -> OrchestrationEvent {
        make_event(OrchestrationEventData::TaskFinished {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-01".to_string(),
            outcome,
            materialization_id: None,
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        })
    }

    fn task_started_event(
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
    ) -> OrchestrationEvent {
        make_event(OrchestrationEventData::TaskStarted {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-01".to_string(),
        })
    }

    fn dispatch_requested_event(
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
    ) -> OrchestrationEvent {
        let dispatch_id = DispatchOutboxRow::dispatch_id(run_id, task_key, attempt);
        make_event(OrchestrationEventData::DispatchRequested {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_queue: "default-queue".to_string(),
            dispatch_id,
        })
    }

    fn dispatch_enqueued_event(dispatch_id: &str) -> OrchestrationEvent {
        make_event(OrchestrationEventData::DispatchEnqueued {
            dispatch_id: dispatch_id.to_string(),
            run_id: Some("run1".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
            cloud_task_id: "d_cloud123".to_string(),
        })
    }

    fn timer_requested_event(timer_id: &str, fire_at: DateTime<Utc>) -> OrchestrationEvent {
        make_event(OrchestrationEventData::TimerRequested {
            timer_id: timer_id.to_string(),
            timer_type: EventTimerType::Retry,
            run_id: Some("run1".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
            fire_at,
        })
    }

    fn timer_fired_event(timer_id: &str) -> OrchestrationEvent {
        make_event(OrchestrationEventData::TimerFired {
            timer_id: timer_id.to_string(),
            timer_type: EventTimerType::Retry,
            run_id: Some("run1".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
        })
    }

    #[test]
    fn test_fold_plan_created_initializes_tasks() {
        let mut state = FoldState::new();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "extract".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "transform".into(),
                    depends_on: vec!["extract".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "load".into(),
                    depends_on: vec!["transform".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        // Extract has no deps, should be READY
        let extract = state.tasks.get(&("run1".into(), "extract".into())).unwrap();
        assert_eq!(extract.state, TaskState::Ready);
        assert_eq!(extract.deps_total, 0);

        // Transform depends on extract, should be BLOCKED
        let transform = state
            .tasks
            .get(&("run1".into(), "transform".into()))
            .unwrap();
        assert_eq!(transform.state, TaskState::Blocked);
        assert_eq!(transform.deps_total, 1);

        // Load depends on transform, should be BLOCKED
        let load = state.tasks.get(&("run1".into(), "load".into())).unwrap();
        assert_eq!(load.state, TaskState::Blocked);
        assert_eq!(load.deps_total, 1);

        // Dep satisfaction edges should be created
        assert!(state.dep_satisfaction.contains_key(&(
            "run1".into(),
            "extract".into(),
            "transform".into()
        )));
        assert!(state.dep_satisfaction.contains_key(&(
            "run1".into(),
            "transform".into(),
            "load".into()
        )));
    }

    #[test]
    fn test_run_triggered_is_idempotent() {
        let mut state = FoldState::new();

        let first = run_triggered_event("run1");
        let second = run_triggered_event("run1");

        state.fold_event(&first);
        state.fold_event(&second);

        let run = state.runs.get("run1").unwrap();
        assert_eq!(run.row_version, first.event_id);
        assert_eq!(run.state, RunState::Triggered);
    }

    #[test]
    fn test_plan_created_is_idempotent() {
        let mut state = FoldState::new();

        state.fold_event(&run_triggered_event("run1"));
        let plan = plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
            ],
        );

        state.fold_event(&plan);
        let task_count = state.tasks.len();
        let edge_count = state.dep_satisfaction.len();

        state.fold_event(&plan);

        assert_eq!(state.tasks.len(), task_count);
        assert_eq!(state.dep_satisfaction.len(), edge_count);
    }

    #[test]
    fn test_duplicate_task_finished_does_not_double_increment() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        // Start task A
        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));

        // First TaskFinished(A, succeeded)
        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Succeeded,
        ));

        let b_after_first = state.tasks.get(&("run1".into(), "B".into())).unwrap();
        assert_eq!(b_after_first.deps_satisfied_count, 1);
        assert_eq!(b_after_first.state, TaskState::Ready);

        // Duplicate TaskFinished(A, succeeded) - should be no-op
        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Succeeded,
        ));

        // Should still be 1, not 2
        let b_after_dup = state.tasks.get(&("run1".into(), "B".into())).unwrap();
        assert_eq!(b_after_dup.deps_satisfied_count, 1);
    }

    #[test]
    fn test_duplicate_task_finished_does_not_double_increment_run_counters() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![TaskDef {
                key: "A".into(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 1,
                heartbeat_timeout_sec: 300,
            }],
        ));

        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));
        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Succeeded,
        ));

        let run_after_first = state.runs.get("run1").unwrap();
        assert_eq!(run_after_first.tasks_completed, 1);
        assert_eq!(run_after_first.tasks_succeeded, 1);

        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Succeeded,
        ));

        let run_after_dup = state.runs.get("run1").unwrap();
        assert_eq!(run_after_dup.tasks_completed, 1);
        assert_eq!(run_after_dup.tasks_succeeded, 1);
    }

    #[test]
    fn test_stale_attempt_event_is_rejected() {
        let mut state = FoldState::new();
        let attempt_1_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![TaskDef {
                key: "extract".into(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
            }],
        ));

        // Start attempt 1
        state.fold_event(&task_started_event("run1", "extract", 1, &attempt_1_id));
        assert_eq!(
            state
                .tasks
                .get(&("run1".into(), "extract".into()))
                .unwrap()
                .attempt,
            1
        );

        // Start attempt 2 (retry)
        let attempt_2_id = Ulid::new().to_string();
        state.fold_event(&task_started_event("run1", "extract", 2, &attempt_2_id));
        assert_eq!(
            state
                .tasks
                .get(&("run1".into(), "extract".into()))
                .unwrap()
                .attempt,
            2
        );
        assert_eq!(
            state
                .tasks
                .get(&("run1".into(), "extract".into()))
                .unwrap()
                .state,
            TaskState::Running
        );

        // Late TaskFinished for attempt 1 arrives (out-of-order) - should be rejected
        state.fold_event(&task_finished_event(
            "run1",
            "extract",
            1,
            &attempt_1_id,
            TaskOutcome::Failed,
        ));

        // State should NOT regress - attempt 2 is still running
        let task = state.tasks.get(&("run1".into(), "extract".into())).unwrap();
        assert_eq!(task.attempt, 2);
        assert_eq!(task.attempt_id.as_deref(), Some(attempt_2_id.as_str()));
        assert_eq!(task.state, TaskState::Running);
    }

    #[test]
    fn test_task_started_attempt_id_mismatch_is_ignored() {
        let mut state = FoldState::new();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![TaskDef {
                key: "extract".into(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
            }],
        ));

        state.fold_event(&dispatch_requested_event("run1", "extract", 1, "att-1"));
        state.fold_event(&task_started_event("run1", "extract", 1, "att-2"));

        let task = state.tasks.get(&("run1".into(), "extract".into())).unwrap();
        assert_eq!(task.state, TaskState::Dispatched);
        assert_eq!(task.attempt_id.as_deref(), Some("att-1"));
    }

    #[test]
    fn test_upstream_failure_skips_downstream() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "C".into(),
                    depends_on: vec!["B".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        // Start A
        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));

        // A fails terminally (max_attempts = 1)
        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Failed,
        ));

        // A should be FAILED
        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Failed
        );

        // B should be SKIPPED (direct downstream)
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Skipped
        );

        // C should also be SKIPPED (transitive)
        assert_eq!(
            state.tasks.get(&("run1".into(), "C".into())).unwrap().state,
            TaskState::Skipped
        );

        // dep_satisfaction edges should have correct resolution
        let ab_edge = state
            .dep_satisfaction
            .get(&("run1".into(), "A".into(), "B".into()))
            .unwrap();
        assert_eq!(ab_edge.resolution, Some(DepResolution::Failed));
        assert!(!ab_edge.satisfied);

        let bc_edge = state
            .dep_satisfaction
            .get(&("run1".into(), "B".into(), "C".into()))
            .unwrap();
        assert_eq!(bc_edge.resolution, Some(DepResolution::Skipped));
    }

    #[test]
    fn test_cancelled_task_cascades_and_cancels_run() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));
        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Cancelled,
        ));

        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Cancelled
        );
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Cancelled
        );

        let run = state.runs.get("run1").unwrap();
        assert_eq!(run.tasks_completed, 2);
        assert_eq!(run.tasks_cancelled, 2);
        assert_eq!(run.state, RunState::Cancelled);
        assert!(run.completed_at.is_some());

        let ab_edge = state
            .dep_satisfaction
            .get(&("run1".into(), "A".into(), "B".into()))
            .unwrap();
        assert_eq!(ab_edge.resolution, Some(DepResolution::Cancelled));
        assert!(!ab_edge.satisfied);
    }

    #[test]
    fn test_task_state_ranks_are_monotonic() {
        assert!(TaskState::Planned.rank() < TaskState::Ready.rank());
        assert!(TaskState::Ready.rank() < TaskState::Running.rank());
        assert!(TaskState::Running.rank() < TaskState::Succeeded.rank());
        assert!(TaskState::Failed.rank() > TaskState::Running.rank());
        assert!(TaskState::Skipped.rank() > TaskState::Running.rank());
    }

    #[test]
    fn test_terminal_states() {
        assert!(!TaskState::Ready.is_terminal());
        assert!(!TaskState::Running.is_terminal());
        assert!(TaskState::Succeeded.is_terminal());
        assert!(TaskState::Failed.is_terminal());
        assert!(TaskState::Skipped.is_terminal());
        assert!(TaskState::Cancelled.is_terminal());
    }

    // ========================================================================
    // Merge Function Tests
    // ========================================================================

    fn make_task_row(task_key: &str, state: TaskState, row_version: &str) -> TaskRow {
        TaskRow {
            run_id: "run1".into(),
            task_key: task_key.into(),
            state,
            attempt: 1,
            attempt_id: None,
            started_at: None,
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: None,
            ready_at: None,
            asset_key: None,
            partition_key: None,
            row_version: row_version.into(),
        }
    }

    #[test]
    fn test_merge_uses_row_version_not_file_order() {
        // Simulate base + L0 delta with out-of-order row_version
        let base_row = make_task_row("extract", TaskState::Running, "01A");
        let delta_row = make_task_row("extract", TaskState::Succeeded, "01B");

        // Pass in "wrong" order (newer first)
        let merged = merge_task_rows(vec![delta_row.clone(), base_row]).unwrap();

        // Should pick row with max row_version
        assert_eq!(merged.state, TaskState::Succeeded);
        assert_eq!(merged.row_version, "01B");

        // Also test correct order
        let base_row2 = make_task_row("extract", TaskState::Running, "01A");
        let delta_row2 = make_task_row("extract", TaskState::Succeeded, "01B");
        let merged2 = merge_task_rows(vec![base_row2, delta_row2]).unwrap();
        assert_eq!(merged2.state, TaskState::Succeeded);
    }

    #[test]
    fn test_merge_uses_state_rank_as_tiebreaker() {
        // Same row_version, different states
        let row1 = make_task_row("extract", TaskState::Running, "01A");
        let row2 = make_task_row("extract", TaskState::Succeeded, "01A");

        let merged = merge_task_rows(vec![row1, row2]).unwrap();

        // Should pick higher state rank (Succeeded > Running)
        assert_eq!(merged.state, TaskState::Succeeded);
    }

    #[test]
    fn test_merge_prefers_later_row_on_equal_rank() {
        let mut row1 = make_task_row("extract", TaskState::Running, "01A");
        row1.attempt_id = Some("attempt-1".to_string());

        let mut row2 = row1.clone();
        row2.attempt_id = Some("attempt-2".to_string());

        let merged = merge_task_rows(vec![row1, row2]).unwrap();
        assert_eq!(merged.attempt_id.as_deref(), Some("attempt-2"));
    }

    #[test]
    fn test_merge_empty_returns_none() {
        let result = merge_task_rows(vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_single_row_returns_it() {
        let row = make_task_row("extract", TaskState::Running, "01A");
        let merged = merge_task_rows(vec![row]).unwrap();
        assert_eq!(merged.state, TaskState::Running);
    }

    #[test]
    fn test_merge_multiple_deltas() {
        // Simulate base + multiple L0 deltas
        let base = make_task_row("extract", TaskState::Ready, "01A");
        let delta1 = make_task_row("extract", TaskState::Running, "01B");
        let delta2 = make_task_row("extract", TaskState::Succeeded, "01C");

        let merged = merge_task_rows(vec![base, delta1, delta2]).unwrap();

        // Should pick the newest (01C)
        assert_eq!(merged.state, TaskState::Succeeded);
        assert_eq!(merged.row_version, "01C");
    }

    #[test]
    fn test_rebuild_dependency_graph_from_edges() {
        let mut state = FoldState::new();

        state.tasks.insert(
            ("run1".into(), "A".into()),
            make_task_row("A", TaskState::Ready, "01A"),
        );
        state.tasks.insert(
            ("run1".into(), "B".into()),
            make_task_row("B", TaskState::Blocked, "01A"),
        );
        state.dep_satisfaction.insert(
            ("run1".into(), "A".into(), "B".into()),
            DepSatisfactionRow {
                run_id: "run1".into(),
                upstream_task_key: "A".into(),
                downstream_task_key: "B".into(),
                satisfied: false,
                resolution: None,
                satisfied_at: None,
                satisfying_attempt: None,
                row_version: "01A".into(),
            },
        );

        state.rebuild_dependency_graph();

        let dependents = state.get_dependents("run1", "A");
        assert_eq!(dependents, vec!["B".to_string()]);
    }

    // ========================================================================
    // Dispatch Outbox Tests
    // ========================================================================

    #[test]
    fn test_dispatch_id_format() {
        let id = DispatchOutboxRow::dispatch_id("run123", "extract", 1);
        assert_eq!(id, "dispatch:run123:extract:1");

        let id2 = DispatchOutboxRow::dispatch_id("run123", "extract", 2);
        assert_eq!(id2, "dispatch:run123:extract:2");
        assert_ne!(id, id2);
    }

    #[test]
    fn test_dispatch_outbox_serialization() {
        let row = DispatchOutboxRow {
            run_id: "run123".into(),
            task_key: "extract".into(),
            attempt: 1,
            dispatch_id: "dispatch:run123:extract:1".into(),
            cloud_task_id: Some("d_abc123".into()),
            status: DispatchStatus::Pending,
            attempt_id: "01HQ123ATT".into(),
            worker_queue: "default-queue".into(),
            created_at: Utc::now(),
            row_version: "01HQ123EVT".into(),
        };

        let json = serde_json::to_string(&row).unwrap();
        assert!(json.contains("PENDING"));
        assert!(json.contains("dispatch:run123:extract:1"));

        let parsed: DispatchOutboxRow = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.status, DispatchStatus::Pending);
    }

    #[test]
    fn test_dispatch_requested_creates_outbox_row() {
        let mut state = FoldState::new();
        let dispatch_id = DispatchOutboxRow::dispatch_id("run1", "extract", 1);
        let event = make_event(OrchestrationEventData::DispatchRequested {
            run_id: "run1".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "priority-queue".to_string(),
            dispatch_id: dispatch_id.clone(),
        });

        state.fold_event(&event);

        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
        assert_eq!(row.status, DispatchStatus::Pending);
        assert_eq!(row.attempt_id, "01HQ123ATT");
        assert_eq!(row.worker_queue, "priority-queue");
    }

    #[test]
    fn test_dispatch_enqueued_updates_outbox_row() {
        let mut state = FoldState::new();
        let mut requested = dispatch_requested_event("run1", "extract", 1, "01HQ123ATT");
        let dispatch_id = DispatchOutboxRow::dispatch_id("run1", "extract", 1);
        let mut enqueued = dispatch_enqueued_event(&dispatch_id);
        requested.event_id = "01A".to_string();
        enqueued.event_id = "01B".to_string();

        state.fold_event(&requested);
        state.fold_event(&enqueued);

        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
        assert_eq!(row.status, DispatchStatus::Created);
        assert_eq!(row.cloud_task_id.as_deref(), Some("d_cloud123"));
    }

    #[test]
    fn test_dispatch_out_of_order_events_do_not_regress() {
        let mut state = FoldState::new();
        let mut requested = dispatch_requested_event("run1", "extract", 1, "01HQ123ATT");
        let dispatch_id = DispatchOutboxRow::dispatch_id("run1", "extract", 1);
        let mut enqueued = dispatch_enqueued_event(&dispatch_id);
        requested.event_id = "01A".to_string();
        enqueued.event_id = "01B".to_string();

        // Apply newer event first
        state.fold_event(&enqueued);
        state.fold_event(&requested);

        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
        assert_eq!(row.status, DispatchStatus::Created);
    }

    /// P0 regression test: out-of-order DispatchEnqueued followed by DispatchRequested
    /// must populate attempt_id (critical for worker concurrency guard).
    #[test]
    fn test_dispatch_out_of_order_fills_attempt_id() {
        let mut state = FoldState::new();
        let dispatch_id = DispatchOutboxRow::dispatch_id("run1", "extract", 1);

        // DispatchEnqueued arrives FIRST (out-of-order)
        let mut enqueued = dispatch_enqueued_event(&dispatch_id);
        enqueued.event_id = "01B".to_string(); // Newer ULID

        state.fold_event(&enqueued);

        // Row should exist but with empty attempt_id
        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
        assert_eq!(row.status, DispatchStatus::Created);
        assert!(
            row.attempt_id.is_empty(),
            "attempt_id should be empty before DispatchRequested"
        );

        // DispatchRequested arrives SECOND (but has older ULID)
        let mut requested = make_event(OrchestrationEventData::DispatchRequested {
            run_id: "run1".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "priority-queue".to_string(),
            dispatch_id: dispatch_id.clone(),
        });
        requested.event_id = "01A".to_string(); // Older ULID

        state.fold_event(&requested);

        // Row should now have attempt_id filled in
        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
        assert_eq!(
            row.attempt_id, "01HQ123ATT",
            "attempt_id should be filled by DispatchRequested"
        );
        assert_eq!(row.worker_queue, "priority-queue");
        // Status should NOT regress from CREATED to PENDING
        assert_eq!(row.status, DispatchStatus::Created);
        // cloud_task_id should be preserved
        assert_eq!(row.cloud_task_id.as_deref(), Some("d_cloud123"));
    }

    #[test]
    fn test_duplicate_dispatch_requested_does_not_override_attempt_id() {
        let mut state = FoldState::new();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![TaskDef {
                key: "extract".into(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
            }],
        ));

        let dispatch_id = DispatchOutboxRow::dispatch_id("run1", "extract", 1);
        let mut first = dispatch_requested_event("run1", "extract", 1, "att-1");
        let mut second = dispatch_requested_event("run1", "extract", 1, "att-2");
        first.event_id = "01A".to_string();
        second.event_id = "01B".to_string();

        state.fold_event(&first);
        state.fold_event(&second);

        let task = state.tasks.get(&("run1".into(), "extract".into())).unwrap();
        assert_eq!(task.state, TaskState::Dispatched);
        assert_eq!(task.attempt_id.as_deref(), Some("att-1"));

        let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
        assert_eq!(row.attempt_id, "att-1");
    }

    // ========================================================================
    // Timer Tests
    // ========================================================================

    /// Test that parse_timer_id correctly extracts timer type and fire epoch.
    #[test]
    fn test_parse_timer_id() {
        // Retry timer
        let parsed = TimerRow::parse_timer_id("timer:retry:run1:extract:1:1705320000").unwrap();
        assert_eq!(parsed.timer_type, TimerType::Retry);
        assert_eq!(parsed.fire_epoch, 1705320000);
        assert_eq!(parsed.run_id.as_deref(), Some("run1"));
        assert_eq!(parsed.task_key.as_deref(), Some("extract"));
        assert_eq!(parsed.attempt, Some(1));

        // Heartbeat timer
        let parsed = TimerRow::parse_timer_id("timer:heartbeat:run1:extract:1:1705320000").unwrap();
        assert_eq!(parsed.timer_type, TimerType::HeartbeatCheck);
        assert_eq!(parsed.fire_epoch, 1705320000);
        assert_eq!(parsed.run_id.as_deref(), Some("run1"));
        assert_eq!(parsed.task_key.as_deref(), Some("extract"));
        assert_eq!(parsed.attempt, Some(1));

        // Cron timer
        let parsed = TimerRow::parse_timer_id("timer:cron:daily-etl:1705320000").unwrap();
        assert_eq!(parsed.timer_type, TimerType::Cron);
        assert_eq!(parsed.fire_epoch, 1705320000);
        assert!(parsed.run_id.is_none());
        assert!(parsed.task_key.is_none());
        assert!(parsed.attempt.is_none());

        // SLA timer
        let parsed = TimerRow::parse_timer_id("timer:sla:run1:extract:1:1705320000").unwrap();
        assert_eq!(parsed.timer_type, TimerType::SlaCheck);
        assert_eq!(parsed.fire_epoch, 1705320000);
        assert_eq!(parsed.run_id.as_deref(), Some("run1"));
        assert_eq!(parsed.task_key.as_deref(), Some("extract"));
        assert_eq!(parsed.attempt, Some(1));

        // Invalid timer ID
        assert!(TimerRow::parse_timer_id("invalid:format").is_none());
        assert!(TimerRow::parse_timer_id("timer:unknown:1705320000").is_none());
    }

    /// P1 regression test: out-of-order TimerEnqueued followed by TimerRequested
    /// must create a timer row and then fill in canonical fire_at.
    #[test]
    fn test_timer_out_of_order_enqueued_before_requested() {
        let mut state = FoldState::new();
        let fire_epoch = 1705320000i64;
        let timer_id = TimerRow::retry_timer_id("run1", "extract", 1, fire_epoch);

        // Create TimerEnqueued event (arrives FIRST, out-of-order)
        let mut enqueued = make_event(OrchestrationEventData::TimerEnqueued {
            timer_id: timer_id.clone(),
            run_id: Some("run1".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
            cloud_task_id: "t_cloud456".to_string(),
        });
        enqueued.event_id = "01B".to_string(); // Newer ULID

        state.fold_event(&enqueued);

        // Row should be created with parsed fire_at
        let row = state.timers.get(&timer_id).expect("timer row should exist");
        assert_eq!(row.timer_type, TimerType::Retry);
        assert_eq!(row.cloud_task_id.as_deref(), Some("t_cloud456"));
        assert_eq!(row.state, TimerState::Scheduled);
        // fire_at should be derived from fire_epoch
        assert_eq!(row.fire_at.timestamp(), fire_epoch);

        // TimerRequested arrives SECOND (but has older ULID)
        let canonical_fire_at = DateTime::from_timestamp(fire_epoch + 30, 0).unwrap(); // Slightly different
        let mut requested = timer_requested_event(&timer_id, canonical_fire_at);
        requested.event_id = "01A".to_string(); // Older ULID

        state.fold_event(&requested);

        // fire_at should be updated to canonical value from TimerRequested
        let row = state.timers.get(&timer_id).expect("timer row");
        assert_eq!(
            row.fire_at, canonical_fire_at,
            "fire_at should be updated to canonical value"
        );
        // cloud_task_id should be preserved
        assert_eq!(row.cloud_task_id.as_deref(), Some("t_cloud456"));
        // State should remain SCHEDULED
        assert_eq!(row.state, TimerState::Scheduled);
    }

    #[test]
    fn test_timer_fired_creates_row_from_timer_id() {
        let mut state = FoldState::new();
        let fire_epoch = 1705320000i64;
        let timer_id = TimerRow::retry_timer_id("run1", "extract", 1, fire_epoch);

        let mut fired = make_event(OrchestrationEventData::TimerFired {
            timer_id: timer_id.clone(),
            timer_type: EventTimerType::Retry,
            run_id: None,
            task_key: None,
            attempt: None,
        });
        fired.event_id = "01B".to_string();

        state.fold_event(&fired);

        let row = state.timers.get(&timer_id).expect("timer row");
        assert_eq!(row.timer_type, TimerType::Retry);
        assert_eq!(row.state, TimerState::Fired);
        assert_eq!(row.run_id.as_deref(), Some("run1"));
        assert_eq!(row.task_key.as_deref(), Some("extract"));
        assert_eq!(row.attempt, Some(1));
        assert_eq!(row.fire_at.timestamp(), fire_epoch);
    }

    /// Test that timer events in normal order work correctly.
    #[test]
    fn test_timer_normal_order_requested_then_enqueued() {
        let mut state = FoldState::new();
        let fire_epoch = 1705320000i64;
        let timer_id = TimerRow::retry_timer_id("run1", "extract", 1, fire_epoch);
        let fire_at = DateTime::from_timestamp(fire_epoch, 0).unwrap();

        // TimerRequested first (normal order)
        let mut requested = timer_requested_event(&timer_id, fire_at);
        requested.event_id = "01A".to_string();
        state.fold_event(&requested);

        let row = state.timers.get(&timer_id).expect("timer row");
        assert_eq!(row.state, TimerState::Scheduled);
        assert!(row.cloud_task_id.is_none());

        // TimerEnqueued second
        let mut enqueued = make_event(OrchestrationEventData::TimerEnqueued {
            timer_id: timer_id.clone(),
            run_id: Some("run1".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
            cloud_task_id: "t_cloud456".to_string(),
        });
        enqueued.event_id = "01B".to_string();
        state.fold_event(&enqueued);

        let row = state.timers.get(&timer_id).expect("timer row");
        assert_eq!(row.cloud_task_id.as_deref(), Some("t_cloud456"));
    }

    #[test]
    fn test_timer_id_includes_epoch_for_uniqueness() {
        let fire_epoch1 = 1705320000i64;
        let fire_epoch2 = 1705320060i64;

        let id1 = TimerRow::retry_timer_id("run1", "task1", 1, fire_epoch1);
        let id2 = TimerRow::retry_timer_id("run1", "task1", 1, fire_epoch2);

        // Different fire times = different timer IDs
        assert_ne!(id1, id2);
        assert!(id1.starts_with("timer:retry:run1:task1:1:"));
        assert!(id2.starts_with("timer:retry:run1:task1:1:"));
    }

    #[test]
    fn test_heartbeat_timer_id() {
        let id = TimerRow::heartbeat_timer_id("run1", "extract", 1, 1705320000);
        assert_eq!(id, "timer:heartbeat:run1:extract:1:1705320000");
    }

    #[test]
    fn test_cron_timer_id() {
        let id = TimerRow::cron_timer_id("daily-etl", 1705320000);
        assert_eq!(id, "timer:cron:daily-etl:1705320000");
    }

    #[test]
    fn test_timer_row_serialization() {
        let row = TimerRow {
            timer_id: "timer:retry:run1:extract:1:1705320000".into(),
            cloud_task_id: Some("t_xyz789".into()),
            timer_type: TimerType::Retry,
            run_id: Some("run1".into()),
            task_key: Some("extract".into()),
            attempt: Some(1),
            fire_at: Utc::now(),
            state: TimerState::Scheduled,
            payload: Some(r#"{"backoff_seconds":30}"#.into()),
            row_version: "01HQ123EVT".into(),
        };

        let json = serde_json::to_string(&row).unwrap();
        assert!(json.contains("SCHEDULED"));
        assert!(json.contains("RETRY"));

        let parsed: TimerRow = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.timer_type, TimerType::Retry);
        assert_eq!(parsed.state, TimerState::Scheduled);
    }

    #[test]
    fn test_timer_requested_and_fired_updates_state() {
        let mut state = FoldState::new();
        let timer_id = TimerRow::retry_timer_id("run1", "extract", 1, 1705320000);
        let fire_at = Utc::now();

        let mut requested = timer_requested_event(&timer_id, fire_at);
        requested.event_id = "01A".to_string();
        state.fold_event(&requested);

        let row = state.timers.get(&timer_id).expect("timer row");
        assert_eq!(row.timer_type, TimerType::Retry);
        assert_eq!(row.state, TimerState::Scheduled);
        assert_eq!(row.fire_at, fire_at);

        let mut fired = timer_fired_event(&timer_id);
        fired.event_id = "01B".to_string();
        state.fold_event(&fired);

        let row = state.timers.get(&timer_id).expect("timer row");
        assert_eq!(row.state, TimerState::Fired);
    }

    #[test]
    fn test_merge_dispatch_outbox_rows() {
        let row1 = DispatchOutboxRow {
            run_id: "run1".into(),
            task_key: "extract".into(),
            attempt: 1,
            dispatch_id: "dispatch:run1:extract:1".into(),
            cloud_task_id: None,
            status: DispatchStatus::Pending,
            attempt_id: "01HQ123ATT".into(),
            worker_queue: "default-queue".into(),
            created_at: Utc::now(),
            row_version: "01A".into(),
        };

        let row2 = DispatchOutboxRow {
            status: DispatchStatus::Created,
            cloud_task_id: Some("d_abc123".into()),
            row_version: "01B".into(),
            ..row1.clone()
        };

        let merged = merge_dispatch_outbox_rows(vec![row1, row2]).unwrap();
        assert_eq!(merged.status, DispatchStatus::Created);
        assert_eq!(merged.row_version, "01B");
    }

    #[test]
    fn test_merge_dispatch_outbox_rows_prefers_later_on_equal_row_version() {
        let row1 = DispatchOutboxRow {
            run_id: "run1".into(),
            task_key: "extract".into(),
            attempt: 1,
            dispatch_id: "dispatch:run1:extract:1".into(),
            cloud_task_id: Some("d_abc123".into()),
            status: DispatchStatus::Created,
            attempt_id: String::new(),
            worker_queue: "default-queue".into(),
            created_at: Utc::now(),
            row_version: "01A".into(),
        };

        let row2 = DispatchOutboxRow {
            attempt_id: "01HQ123ATT".into(),
            ..row1.clone()
        };

        let merged = merge_dispatch_outbox_rows(vec![row1, row2]).unwrap();
        assert_eq!(merged.attempt_id, "01HQ123ATT");
        assert_eq!(merged.row_version, "01A");
    }

    #[test]
    fn test_merge_timer_rows() {
        let row1 = TimerRow {
            timer_id: "timer:retry:run1:extract:1:1705320000".into(),
            cloud_task_id: None,
            timer_type: TimerType::Retry,
            run_id: Some("run1".into()),
            task_key: Some("extract".into()),
            attempt: Some(1),
            fire_at: Utc::now(),
            state: TimerState::Scheduled,
            payload: None,
            row_version: "01A".into(),
        };

        let row2 = TimerRow {
            state: TimerState::Fired,
            row_version: "01B".into(),
            ..row1.clone()
        };

        let merged = merge_timer_rows(vec![row1, row2]).unwrap();
        assert_eq!(merged.state, TimerState::Fired);
        assert_eq!(merged.row_version, "01B");
    }

    #[test]
    fn test_merge_timer_rows_prefers_later_on_equal_row_version() {
        let row1 = TimerRow {
            timer_id: "timer:retry:run1:extract:1:1705320000".into(),
            cloud_task_id: Some("t_abc123".into()),
            timer_type: TimerType::Retry,
            run_id: Some("run1".into()),
            task_key: Some("extract".into()),
            attempt: Some(1),
            fire_at: DateTime::from_timestamp(1705320000, 0).unwrap(),
            state: TimerState::Scheduled,
            payload: None,
            row_version: "01A".into(),
        };

        let row2 = TimerRow {
            fire_at: DateTime::from_timestamp(1705320030, 0).unwrap(),
            ..row1.clone()
        };

        let merged = merge_timer_rows(vec![row1, row2]).unwrap();
        assert_eq!(merged.fire_at.timestamp(), 1705320030);
        assert_eq!(merged.row_version, "01A");
    }

    // ========================================================================
    // Cloud Tasks ID Generation Tests (ADR-021)
    // ========================================================================

    #[test]
    fn test_cloud_task_id_is_deterministic() {
        let internal_id = "dispatch:run123:extract:1";

        let id1 = cloud_task_id("d", internal_id);
        let id2 = cloud_task_id("d", internal_id);

        // Same input = same output
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_cloud_task_id_is_unique() {
        let id1 = cloud_task_id("d", "dispatch:run123:extract:1");
        let id2 = cloud_task_id("d", "dispatch:run123:extract:2");

        // Different input = different output
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_cloud_task_id_format() {
        let id = cloud_task_id("d", "dispatch:run123:extract:1");

        // Starts with prefix + underscore
        assert!(id.starts_with("d_"));

        // All chars are alphanumeric or underscore (Cloud Tasks compliant)
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));

        // Total length: 1 (prefix) + 1 (_) + 26 (hash) = 28
        assert_eq!(id.len(), 28);
    }

    #[test]
    fn test_dispatch_cloud_task_id() {
        let id = dispatch_cloud_task_id("run123", "extract", 1);

        assert!(id.starts_with("d_"));
        assert_eq!(id.len(), 28);

        // Different attempts produce different IDs
        let id2 = dispatch_cloud_task_id("run123", "extract", 2);
        assert_ne!(id, id2);
    }

    #[test]
    fn test_timer_cloud_task_id() {
        let timer_id = TimerRow::retry_timer_id("run1", "extract", 1, 1705320000);
        let cloud_id = timer_cloud_task_id(&timer_id);

        assert!(cloud_id.starts_with("t_"));
        assert_eq!(cloud_id.len(), 28);
    }

    // ========================================================================
    // Run Cancellation Tests (P0)
    // ========================================================================

    fn run_cancel_requested_event(run_id: &str, reason: &str) -> OrchestrationEvent {
        make_event(OrchestrationEventData::RunCancelRequested {
            run_id: run_id.to_string(),
            reason: Some(reason.to_string()),
            requested_by: "user@test.com".to_string(),
        })
    }

    /// P0: Run cancellation must transition pending tasks to CANCELLED.
    /// A cancelled run with non-terminal tasks is inconsistent and can strand work.
    #[test]
    fn test_run_cancel_requested_cancels_pending_tasks() {
        let mut state = FoldState::new();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "C".into(),
                    depends_on: vec!["B".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        // A is READY, B and C are BLOCKED
        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Ready
        );
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Blocked
        );
        assert_eq!(
            state.tasks.get(&("run1".into(), "C".into())).unwrap().state,
            TaskState::Blocked
        );

        // Request cancellation
        state.fold_event(&run_cancel_requested_event("run1", "user_requested"));

        // All non-terminal tasks should be CANCELLED
        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Cancelled
        );
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Cancelled
        );
        assert_eq!(
            state.tasks.get(&("run1".into(), "C".into())).unwrap().state,
            TaskState::Cancelled
        );

        // Run should be CANCELLED with correct counters
        let run = state.runs.get("run1").unwrap();
        assert!(run.cancel_requested);
        assert_eq!(run.state, RunState::Cancelled);
        assert_eq!(run.tasks_cancelled, 3);
        assert_eq!(run.tasks_completed, 3);
        assert!(run.completed_at.is_some());
    }

    /// P0: Running tasks should NOT be immediately cancelled - they check cancel_requested on heartbeat.
    #[test]
    fn test_run_cancel_requested_does_not_cancel_running_tasks() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        // Start task A (transitions to RUNNING)
        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));
        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Running
        );

        // Request cancellation
        state.fold_event(&run_cancel_requested_event("run1", "user_requested"));

        // Running task should remain RUNNING (worker will check cancel_requested on heartbeat)
        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Running
        );

        // Blocked task should be CANCELLED
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Cancelled
        );

        // Run should have cancel_requested but NOT be terminal yet (A is still running)
        let run = state.runs.get("run1").unwrap();
        assert!(run.cancel_requested);
        assert_eq!(run.state, RunState::Running); // Not CANCELLED yet
        assert_eq!(run.tasks_cancelled, 1); // Only B was cancelled
        assert_eq!(run.tasks_completed, 1);
    }

    /// P0: Already terminal tasks should not be re-cancelled.
    #[test]
    fn test_run_cancel_requested_preserves_terminal_tasks() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event(
            "run1",
            vec![
                TaskDef {
                    key: "A".into(),
                    depends_on: vec![],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
                TaskDef {
                    key: "B".into(),
                    depends_on: vec!["A".into()],
                    asset_key: None,
                    partition_key: None,
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                },
            ],
        ));

        // A succeeds
        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));
        state.fold_event(&task_finished_event(
            "run1",
            "A",
            1,
            &attempt_id,
            TaskOutcome::Succeeded,
        ));

        // B is now READY
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Ready
        );

        // Request cancellation
        state.fold_event(&run_cancel_requested_event("run1", "user_requested"));

        // A should remain SUCCEEDED (not re-cancelled)
        assert_eq!(
            state.tasks.get(&("run1".into(), "A".into())).unwrap().state,
            TaskState::Succeeded
        );

        // B should be CANCELLED (was READY)
        assert_eq!(
            state.tasks.get(&("run1".into(), "B".into())).unwrap().state,
            TaskState::Cancelled
        );

        // Run counters should reflect both states
        let run = state.runs.get("run1").unwrap();
        assert!(run.cancel_requested);
        assert_eq!(run.tasks_succeeded, 1);
        assert_eq!(run.tasks_cancelled, 1);
        assert_eq!(run.tasks_completed, 2);
        assert_eq!(run.state, RunState::Cancelled); // Still cancelled because at least one task was cancelled
    }

    // ========================================================================
    // Layer 2: Schedule Fold Tests
    // ========================================================================

    fn schedule_ticked_event(
        schedule_id: &str,
        scheduled_for: DateTime<Utc>,
        status: TickStatus,
        run_key: Option<&str>,
    ) -> OrchestrationEvent {
        let tick_id = format!("{}:{}", schedule_id, scheduled_for.timestamp());
        OrchestrationEvent::new(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::ScheduleTicked {
                schedule_id: schedule_id.to_string(),
                scheduled_for,
                tick_id,
                definition_version: "def_01HQ123".into(),
                asset_selection: vec!["analytics.summary".into()],
                partition_selection: None,
                status,
                run_key: run_key.map(ToString::to_string),
                request_fingerprint: Some("fp_abc123".into()),
            },
        )
    }

    #[test]
    fn test_fold_schedule_ticked_updates_state_and_creates_tick_row() {
        let mut state = FoldState::new();
        let scheduled_for = Utc::now();

        let event = schedule_ticked_event(
            "daily-etl",
            scheduled_for,
            TickStatus::Triggered,
            Some("sched:daily-etl:1736935200"),
        );

        state.fold_event(&event);

        // Should update schedule_state
        let schedule_state = state.schedule_state.get("daily-etl").unwrap();
        assert!(schedule_state.last_scheduled_for.is_some());
        assert_eq!(
            schedule_state.last_run_key,
            Some("sched:daily-etl:1736935200".into())
        );

        // Should create tick history row
        let tick_id = format!("daily-etl:{}", scheduled_for.timestamp());
        let tick = state.schedule_ticks.get(&tick_id).unwrap();
        assert!(matches!(tick.status, TickStatus::Triggered));
        assert_eq!(tick.schedule_id, "daily-etl");
        assert_eq!(tick.run_key, Some("sched:daily-etl:1736935200".to_string()));
    }

    /// NOTE: Per P0-1, fold does NOT emit events. Controllers emit all events atomically.
    /// This test verifies fold only updates projections.
    #[test]
    fn test_fold_schedule_ticked_is_pure_projection() {
        let mut state = FoldState::new();

        let event = schedule_ticked_event(
            "daily-etl",
            Utc::now(),
            TickStatus::Triggered,
            Some("sched:daily-etl:1736935200"),
        );

        // fold_event returns nothing (pure projection)
        state.fold_event(&event);

        // Verify projections updated
        assert!(state.schedule_state.contains_key("daily-etl"));
        // RunRequested should come from controller, not fold - no runs should be created
        assert!(state.runs.is_empty());
    }

    #[test]
    fn test_fold_schedule_ticked_idempotent_with_same_tick_id() {
        let mut state = FoldState::new();
        let scheduled_for = Utc::now();

        // First tick
        let event1 = schedule_ticked_event(
            "daily-etl",
            scheduled_for,
            TickStatus::Triggered,
            Some("sched:daily-etl:run1"),
        );
        state.fold_event(&event1);

        let tick_id = format!("daily-etl:{}", scheduled_for.timestamp());
        let first_version = state
            .schedule_ticks
            .get(&tick_id)
            .unwrap()
            .row_version
            .clone();

        // Duplicate event with older event_id (simulating replay)
        // Create event with older ULID
        let mut event2 = schedule_ticked_event(
            "daily-etl",
            scheduled_for,
            TickStatus::Triggered,
            Some("sched:daily-etl:run1"),
        );
        // Manually set to older event_id - normally events are newer, but this simulates duplicate
        // The fold logic should ignore events with equal or older event_id
        event2.event_id = String::new(); // Empty string is "older" than any ULID

        state.fold_event(&event2);

        // Version should NOT change (duplicate was ignored)
        assert_eq!(
            state.schedule_ticks.get(&tick_id).unwrap().row_version,
            first_version
        );
    }

    #[test]
    fn test_fold_schedule_ticked_failed_status_has_no_run_key() {
        let mut state = FoldState::new();

        let event = schedule_ticked_event(
            "daily-etl",
            Utc::now(),
            TickStatus::Failed {
                error: "invalid cron".into(),
            },
            None, // Failed ticks have no run_key
        );

        state.fold_event(&event);

        let schedule_state = state.schedule_state.get("daily-etl").unwrap();
        assert!(schedule_state.last_scheduled_for.is_some());
        assert!(schedule_state.last_run_key.is_none());

        // Tick row should show failed status
        let tick = state.schedule_ticks.values().next().unwrap();
        assert!(matches!(tick.status, TickStatus::Failed { .. }));
        assert!(tick.run_key.is_none());
    }

    #[test]
    fn test_fold_schedule_ticked_skipped_status_has_no_run_key() {
        let mut state = FoldState::new();

        let event = schedule_ticked_event(
            "daily-etl",
            Utc::now(),
            TickStatus::Skipped {
                reason: "paused".into(),
            },
            None,
        );

        state.fold_event(&event);

        let schedule_state = state.schedule_state.get("daily-etl").unwrap();
        assert!(schedule_state.last_scheduled_for.is_some());
        assert!(schedule_state.last_run_key.is_none());

        let tick = state.schedule_ticks.values().next().unwrap();
        assert!(matches!(tick.status, TickStatus::Skipped { .. }));
        assert!(tick.run_key.is_none());
    }

    #[test]
    fn test_fold_schedule_ticked_updates_existing_state() {
        let mut state = FoldState::new();

        // First tick
        let event1 = schedule_ticked_event(
            "daily-etl",
            Utc::now() - chrono::Duration::hours(1),
            TickStatus::Triggered,
            Some("run1"),
        );
        state.fold_event(&event1);

        // Ensure second event has a larger ULID (wait for next millisecond)
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Second tick (newer)
        let event2 =
            schedule_ticked_event("daily-etl", Utc::now(), TickStatus::Triggered, Some("run2"));
        state.fold_event(&event2);

        // State should reflect the latest tick
        let schedule_state = state.schedule_state.get("daily-etl").unwrap();
        assert_eq!(schedule_state.last_run_key, Some("run2".into()));

        // Both ticks should be in history
        assert_eq!(state.schedule_ticks.len(), 2);
    }

    // ========================================================================
    // Layer 2: Backfill Fold Tests
    // ========================================================================

    // Use deterministic event IDs to avoid parallel test flakiness.
    // Event IDs incorporate backfill_id and a sequence number to ensure
    // proper ordering within a test and isolation across tests.
    fn backfill_created_event(backfill_id: &str) -> OrchestrationEvent {
        OrchestrationEvent::new_with_event_id(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::BackfillCreated {
                backfill_id: backfill_id.to_string(),
                client_request_id: "client_req_001".into(),
                asset_selection: vec!["analytics.daily".into()],
                partition_selector: PartitionSelector::Range {
                    start: "2025-01-01".into(),
                    end: "2025-01-03".into(),
                },
                total_partitions: 3,
                chunk_size: 2,
                max_concurrent_runs: 2,
                parent_backfill_id: None,
            },
            format!("evt_{backfill_id}_01_created"),
        )
    }

    fn backfill_chunk_planned_event(backfill_id: &str, chunk_index: u32) -> OrchestrationEvent {
        OrchestrationEvent::new_with_event_id(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::BackfillChunkPlanned {
                backfill_id: backfill_id.to_string(),
                chunk_id: format!("{backfill_id}:{chunk_index}"),
                chunk_index,
                partition_keys: vec!["2025-01-01".into(), "2025-01-02".into()],
                run_key: format!("backfill:{backfill_id}:chunk:{chunk_index}"),
                request_fingerprint: "fp_backfill_chunk".into(),
            },
            format!("evt_{backfill_id}_02_chunk_{chunk_index}"),
        )
    }

    fn backfill_state_changed_event(
        backfill_id: &str,
        from_state: BackfillState,
        to_state: BackfillState,
        state_version: u32,
    ) -> OrchestrationEvent {
        OrchestrationEvent::new_with_event_id(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::BackfillStateChanged {
                backfill_id: backfill_id.to_string(),
                from_state,
                to_state,
                state_version,
                changed_by: None,
            },
            // Use state_version to ensure ordering: state changes come after creation
            format!("evt_{backfill_id}_03_state_v{state_version}"),
        )
    }

    #[test]
    fn test_fold_backfill_created_inserts_backfill() {
        let mut state = FoldState::new();

        let event = backfill_created_event("bf_001");
        state.fold_event(&event);

        let backfill = state.backfills.get("bf_001").expect("backfill should exist");
        assert_eq!(backfill.state, BackfillState::Running);
        assert_eq!(backfill.state_version, 1);
        assert_eq!(backfill.total_partitions, 3);
        assert_eq!(backfill.chunk_size, 2);
        assert_eq!(backfill.max_concurrent_runs, 2);
    }

    #[test]
    fn test_fold_backfill_chunk_planned_updates_chunks_and_backfill() {
        let mut state = FoldState::new();

        state.fold_event(&backfill_created_event("bf_002"));
        state.fold_event(&backfill_chunk_planned_event("bf_002", 0));

        let chunk = state
            .backfill_chunks
            .get("bf_002:0")
            .expect("chunk should exist");
        assert_eq!(chunk.state, ChunkState::Planned);
        assert_eq!(chunk.run_key, "backfill:bf_002:chunk:0");

        let backfill = state.backfills.get("bf_002").expect("backfill should exist");
        assert_eq!(backfill.planned_chunks, 1);
    }

    #[test]
    fn test_fold_backfill_state_changed_updates_state() {
        let mut state = FoldState::new();

        state.fold_event(&backfill_created_event("bf_003"));
        let event = backfill_state_changed_event(
            "bf_003",
            BackfillState::Running,
            BackfillState::Paused,
            2,
        );
        state.fold_event(&event);

        let backfill = state.backfills.get("bf_003").expect("backfill should exist");
        assert_eq!(backfill.state, BackfillState::Paused);
        assert_eq!(backfill.state_version, 2);
    }

    #[test]
    fn test_fold_backfill_state_changed_rejects_invalid_version() {
        let mut state = FoldState::new();

        state.fold_event(&backfill_created_event("bf_004"));
        let event = backfill_state_changed_event(
            "bf_004",
            BackfillState::Running,
            BackfillState::Paused,
            4,
        );
        state.fold_event(&event);

        let backfill = state.backfills.get("bf_004").expect("backfill should exist");
        assert_eq!(backfill.state, BackfillState::Running);
        assert_eq!(backfill.state_version, 1);
    }

    // ========================================================================
    // Layer 2: Sensor Fold Tests
    // ========================================================================

    fn sensor_evaluated_event(
        sensor_id: &str,
        cursor_before: Option<&str>,
        cursor_after: Option<&str>,
        expected_state_version: Option<u32>,
        trigger_source: TriggerSource,
    ) -> OrchestrationEvent {
        OrchestrationEvent::new(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                eval_id: format!("eval_{}_{}", sensor_id, Ulid::new()),
                cursor_before: cursor_before.map(ToString::to_string),
                cursor_after: cursor_after.map(ToString::to_string),
                expected_state_version,
                trigger_source,
                run_requests: vec![],
                status: SensorEvalStatus::NoNewData,
            },
        )
    }

    #[test]
    fn test_fold_sensor_evaluated_updates_state() {
        let mut state = FoldState::new();

        let event = sensor_evaluated_event(
            "01HQ123SENSORXYZ",
            None,
            Some("cursor_v1"),
            None, // Push sensor - no CAS
            TriggerSource::Push {
                message_id: "msg_001".into(),
            },
        );

        state.fold_event(&event);

        // Should create sensor state
        let sensor_state = state.sensor_state.get("01HQ123SENSORXYZ").unwrap();
        assert_eq!(sensor_state.cursor, Some("cursor_v1".into()));
        assert!(sensor_state.last_evaluation_at.is_some());
        assert!(sensor_state.last_eval_id.is_some());
        assert_eq!(sensor_state.state_version, 1);
        assert_eq!(sensor_state.status, SensorStatus::Active);

        // Should record evaluation history
        let eval_id = match &event.data {
            OrchestrationEventData::SensorEvaluated { eval_id, .. } => eval_id,
            _ => unreachable!("expected SensorEvaluated"),
        };
        let eval_row = state.sensor_evals.get(eval_id).unwrap();
        assert_eq!(eval_row.cursor_after, Some("cursor_v1".into()));
        assert!(matches!(eval_row.status, SensorEvalStatus::NoNewData));
    }

    /// Per P0-1, fold does NOT emit events. Controllers emit all events atomically.
    /// This test verifies fold only updates projections.
    #[test]
    fn test_fold_sensor_evaluated_is_pure_projection() {
        let mut state = FoldState::new();

        let event = sensor_evaluated_event(
            "01HQ123SENSORXYZ",
            None,
            Some("cursor_v1"),
            None,
            TriggerSource::Push {
                message_id: "msg_001".into(),
            },
        );

        // fold_event returns nothing (pure projection)
        state.fold_event(&event);

        // Verify projections updated
        assert!(state.sensor_state.contains_key("01HQ123SENSORXYZ"));
        // RunRequested should come from controller, not fold
    }

    /// Per P0-2, poll sensors use CAS for cursor serialization.
    /// If expected_state_version doesn't match, the stale eval is dropped.
    #[test]
    fn test_fold_sensor_evaluated_drops_on_version_mismatch() {
        let mut state = FoldState::new();

        // Create initial sensor state with version 2
        state.sensor_state.insert(
            "01HQ456POLLSENSOR".into(),
            SensorStateRow {
                tenant_id: "tenant-abc".into(),
                workspace_id: "workspace-prod".into(),
                sensor_id: "01HQ456POLLSENSOR".into(),
                cursor: Some("cursor_v1".into()),
                last_evaluation_at: None,
                last_eval_id: None,
                status: SensorStatus::Active,
                state_version: 2,
                row_version: "initial".into(),
            },
        );

        // Try to evaluate with stale expected_state_version (1 != 2)
        let event = sensor_evaluated_event(
            "01HQ456POLLSENSOR",
            Some("cursor_v1"),
            Some("cursor_v2"), // Would update cursor if accepted
            Some(1),           // Stale! Current is 2
            TriggerSource::Poll {
                poll_epoch: 1736935200,
            },
        );

        state.fold_event(&event);

        // Stale eval should be dropped - cursor unchanged
        let sensor_state = state.sensor_state.get("01HQ456POLLSENSOR").unwrap();
        assert_eq!(
            sensor_state.cursor,
            Some("cursor_v1".into()),
            "Cursor should NOT be updated for stale eval"
        );
        assert_eq!(
            sensor_state.state_version, 2,
            "state_version should NOT increment for stale eval"
        );

        let eval_id = match &event.data {
            OrchestrationEventData::SensorEvaluated { eval_id, .. } => eval_id,
            _ => unreachable!("expected SensorEvaluated"),
        };
        let eval_row = state.sensor_evals.get(eval_id).unwrap();
        assert!(
            matches!(eval_row.status, SensorEvalStatus::SkippedStaleCursor),
            "stale eval should be recorded as SkippedStaleCursor"
        );
    }

    #[test]
    fn test_run_requested_is_dropped_for_stale_sensor_eval() {
        let mut state = FoldState::new();

        state.sensor_state.insert(
            "01HQ456POLLSENSOR".into(),
            SensorStateRow {
                tenant_id: "tenant-abc".into(),
                workspace_id: "workspace-prod".into(),
                sensor_id: "01HQ456POLLSENSOR".into(),
                cursor: Some("cursor_v1".into()),
                last_evaluation_at: None,
                last_eval_id: None,
                status: SensorStatus::Active,
                state_version: 2,
                row_version: "initial".into(),
            },
        );

        let event = sensor_evaluated_event(
            "01HQ456POLLSENSOR",
            Some("cursor_v1"),
            Some("cursor_v2"),
            Some(1),
            TriggerSource::Poll {
                poll_epoch: 1736935200,
            },
        );
        state.fold_event(&event);

        let eval_id = match &event.data {
            OrchestrationEventData::SensorEvaluated { eval_id, .. } => eval_id,
            _ => unreachable!("expected SensorEvaluated"),
        };

        let source_ref = SourceRef::Sensor {
            sensor_id: "01HQ456POLLSENSOR".into(),
            eval_id: eval_id.clone(),
        };

        assert!(
            state.should_drop_run_requested(&source_ref),
            "RunRequested should be dropped when eval is stale"
        );
    }

    /// CAS should accept eval when expected_state_version matches.
    #[test]
    fn test_fold_sensor_evaluated_accepts_on_version_match() {
        let mut state = FoldState::new();

        // Create initial sensor state with version 2
        state.sensor_state.insert(
            "01HQ456POLLSENSOR".into(),
            SensorStateRow {
                tenant_id: "tenant-abc".into(),
                workspace_id: "workspace-prod".into(),
                sensor_id: "01HQ456POLLSENSOR".into(),
                cursor: Some("cursor_v1".into()),
                last_evaluation_at: None,
                last_eval_id: None,
                status: SensorStatus::Active,
                state_version: 2,
                row_version: String::new(),
            },
        );

        // Evaluate with matching expected_state_version
        let event = sensor_evaluated_event(
            "01HQ456POLLSENSOR",
            Some("cursor_v1"),
            Some("cursor_v2"),
            Some(2), // Matches current state_version
            TriggerSource::Poll {
                poll_epoch: 1736935200,
            },
        );

        state.fold_event(&event);

        // Eval should be accepted - cursor updated
        let sensor_state = state.sensor_state.get("01HQ456POLLSENSOR").unwrap();
        assert_eq!(
            sensor_state.cursor,
            Some("cursor_v2".into()),
            "Cursor should be updated for valid eval"
        );
        assert_eq!(
            sensor_state.state_version, 3,
            "state_version should increment after valid eval"
        );
    }

    /// Push sensors (no expected_state_version) should always be accepted.
    #[test]
    fn test_fold_sensor_evaluated_push_bypasses_cas() {
        let mut state = FoldState::new();

        // Create initial sensor state with version 5
        state.sensor_state.insert(
            "01HQ789PUSHSENSOR".into(),
            SensorStateRow {
                tenant_id: "tenant-abc".into(),
                workspace_id: "workspace-prod".into(),
                sensor_id: "01HQ789PUSHSENSOR".into(),
                cursor: None,
                last_evaluation_at: None,
                last_eval_id: None,
                status: SensorStatus::Active,
                state_version: 5,
                row_version: String::new(),
            },
        );

        // Push sensor eval (no expected_state_version)
        let event = sensor_evaluated_event(
            "01HQ789PUSHSENSOR",
            None,
            Some("msg_001"),
            None, // No CAS for push sensors
            TriggerSource::Push {
                message_id: "msg_001".into(),
            },
        );

        state.fold_event(&event);

        // Push eval should be accepted regardless of state_version
        let sensor_state = state.sensor_state.get("01HQ789PUSHSENSOR").unwrap();
        assert_eq!(sensor_state.cursor, Some("msg_001".into()));
        assert_eq!(
            sensor_state.state_version, 6,
            "state_version should increment after push eval"
        );
    }

    /// Error status should update sensor status to Error.
    #[test]
    fn test_fold_sensor_evaluated_error_updates_status() {
        let mut state = FoldState::new();

        // Create error event
        let event = OrchestrationEvent::new(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::SensorEvaluated {
                sensor_id: "01HQ123ERRORSENSOR".to_string(),
                eval_id: format!("eval_error_{}", Ulid::new()),
                cursor_before: None,
                cursor_after: None,
                expected_state_version: None,
                trigger_source: TriggerSource::Push {
                    message_id: "msg_error".into(),
                },
                run_requests: vec![],
                status: SensorEvalStatus::Error {
                    message: "Connection failed".into(),
                },
            },
        );

        state.fold_event(&event);

        let sensor_state = state.sensor_state.get("01HQ123ERRORSENSOR").unwrap();
        assert_eq!(
            sensor_state.status,
            SensorStatus::Error,
            "Sensor status should be Error after error eval"
        );

        let eval_id = match &event.data {
            OrchestrationEventData::SensorEvaluated { eval_id, .. } => eval_id,
            _ => unreachable!("expected SensorEvaluated"),
        };
        let eval_row = state.sensor_evals.get(eval_id).unwrap();
        assert!(matches!(eval_row.status, SensorEvalStatus::Error { .. }));
    }

    /// State version should increment on each evaluation.
    #[test]
    fn test_fold_sensor_evaluated_increments_state_version() {
        let mut state = FoldState::new();

        // First evaluation
        let event1 = sensor_evaluated_event(
            "01HQ123SENSORXYZ",
            None,
            Some("cursor_v1"),
            None,
            TriggerSource::Push {
                message_id: "msg_001".into(),
            },
        );
        state.fold_event(&event1);

        assert_eq!(
            state
                .sensor_state
                .get("01HQ123SENSORXYZ")
                .unwrap()
                .state_version,
            1
        );

        // Ensure second event has a larger ULID (wait for next millisecond)
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Second evaluation
        let event2 = sensor_evaluated_event(
            "01HQ123SENSORXYZ",
            None,
            Some("cursor_v2"),
            None,
            TriggerSource::Push {
                message_id: "msg_002".into(),
            },
        );
        state.fold_event(&event2);

        assert_eq!(
            state
                .sensor_state
                .get("01HQ123SENSORXYZ")
                .unwrap()
                .state_version,
            2
        );
    }
}
