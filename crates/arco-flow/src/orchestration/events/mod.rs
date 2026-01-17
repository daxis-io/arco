//! Orchestration event types for the event-driven architecture.
//!
//! This module defines the event envelope and payload types for orchestration
//! events. Events are appended to the ledger and processed by the compactor.
//!
//! ## Event Categories (per ADR-020)
//!
//! | Category | Events | Description |
//! |----------|--------|-------------|
//! | Intent | `DispatchRequested`, `TimerRequested` | "I want this to happen" |
//! | Acknowledgement | `DispatchEnqueued`, `TimerEnqueued` | "External system accepted" |
//! | Worker Facts | `TaskStarted`, `TaskHeartbeat`, `TaskFinished` | "This happened" |
//! | Automation | `ScheduleTicked`, `SensorEvaluated`, `RunRequested` | "Trigger evaluated" |
//! | Backfill | `BackfillCreated`, `BackfillChunkPlanned`, `BackfillStateChanged` | "Backfill lifecycle" |
//!
//! Derived state changes (`TaskBecameReady`, `TaskSkipped`, `RunCompleted`) are
//! projection-only - computed during compaction fold and intentionally excluded
//! from the ledger event envelope.
//!
//! ## Idempotency
//!
//! Every event includes an `idempotency_key` for deduplication. The compactor
//! uses this to ensure duplicate events are no-ops.
//!
//! ## Attempt ID (per ADR-022)
//!
//! Task events include `attempt_id` as a concurrency guard. This prevents
//! state regression when out-of-order events arrive (e.g., "attempt 1 finished"
//! arriving after "attempt 2 started").

pub mod automation_events;
pub mod backfill_events;

pub use automation_events::{
    RunRequest, SensorEvalStatus, SensorStatus, SourceRef, TickStatus, TriggerSource, sha256_hex,
};
pub use backfill_events::{BackfillState, ChunkState, PartitionSelector};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use ulid::Ulid;

/// Orchestration event envelope.
///
/// Wraps event data with metadata for the ledger. The envelope follows the
/// CloudEvents-inspired pattern from ADR-004.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationEvent {
    /// Unique event identifier (ULID).
    pub event_id: String,

    /// Event type (e.g., "`TaskFinished`", "`DispatchRequested`").
    pub event_type: String,

    /// Schema version for forward compatibility.
    pub event_version: u32,

    /// When the event was created.
    pub timestamp: DateTime<Utc>,

    /// Event origin URI (e.g., "arco-flow/tenant-abc/workspace-prod").
    pub source: String,

    /// Tenant identifier.
    pub tenant_id: String,

    /// Workspace identifier.
    pub workspace_id: String,

    /// Idempotency key for deduplication.
    pub idempotency_key: String,

    /// Correlation identifier (typically `run_id`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// Causation identifier (the event that caused this one).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,

    /// Event payload.
    pub data: OrchestrationEventData,
}

impl OrchestrationEvent {
    /// Creates a new orchestration event with auto-generated ID and timestamp.
    #[must_use]
    pub fn new(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: OrchestrationEventData,
    ) -> Self {
        Self::new_with_timestamp(tenant_id, workspace_id, data, Utc::now())
    }

    /// Creates a new orchestration event with a custom idempotency key.
    ///
    /// Use this when you need to override the auto-generated idempotency key,
    /// such as for retry-failed backfills where idempotency should be based on
    /// the parent backfill + request ID rather than the event data.
    #[must_use]
    pub fn new_with_idempotency_key(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: OrchestrationEventData,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self::new_with_timestamp_and_idempotency_key(
            tenant_id,
            workspace_id,
            data,
            idempotency_key,
            Utc::now(),
        )
    }

    /// Creates a new orchestration event with a fixed timestamp.
    #[must_use]
    pub fn new_with_timestamp(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: OrchestrationEventData,
        timestamp: DateTime<Utc>,
    ) -> Self {
        let tenant = tenant_id.into();
        let workspace = workspace_id.into();
        let event_id = Ulid::new().to_string();
        let event_type = data.event_type().to_string();
        let idempotency_key = data.idempotency_key();
        let correlation_id = data.run_id().map(ToString::to_string);

        Self {
            event_id,
            event_type,
            event_version: 1,
            timestamp,
            source: format!("arco-flow/{tenant}/{workspace}"),
            tenant_id: tenant,
            workspace_id: workspace,
            idempotency_key,
            correlation_id,
            causation_id: None,
            data,
        }
    }

    /// Creates a new orchestration event with a custom idempotency key and timestamp.
    #[must_use]
    pub fn new_with_timestamp_and_idempotency_key(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: OrchestrationEventData,
        idempotency_key: impl Into<String>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        let tenant = tenant_id.into();
        let workspace = workspace_id.into();
        let event_id = Ulid::new().to_string();
        let event_type = data.event_type().to_string();
        let correlation_id = data.run_id().map(ToString::to_string);

        Self {
            event_id,
            event_type,
            event_version: 1,
            timestamp,
            source: format!("arco-flow/{tenant}/{workspace}"),
            tenant_id: tenant,
            workspace_id: workspace,
            idempotency_key: idempotency_key.into(),
            correlation_id,
            causation_id: None,
            data,
        }
    }

    /// Creates an event with explicit event_id for deterministic testing.
    ///
    /// In production, use `new()` or `new_with_timestamp()` which generate ULIDs.
    /// This constructor is useful for tests where event ordering must be
    /// deterministic regardless of parallel execution timing.
    #[cfg(test)]
    #[must_use]
    pub fn new_with_event_id(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: OrchestrationEventData,
        event_id: impl Into<String>,
    ) -> Self {
        let tenant = tenant_id.into();
        let workspace = workspace_id.into();
        let event_type = data.event_type().to_string();
        let idempotency_key = data.idempotency_key();
        let correlation_id = data.run_id().map(ToString::to_string);

        Self {
            event_id: event_id.into(),
            event_type,
            event_version: 1,
            timestamp: Utc::now(),
            source: format!("arco-flow/{tenant}/{workspace}"),
            tenant_id: tenant,
            workspace_id: workspace,
            idempotency_key,
            correlation_id,
            causation_id: None,
            data,
        }
    }

    /// Sets the causation identifier.
    #[must_use]
    pub fn with_causation_id(mut self, causation_id: impl Into<String>) -> Self {
        self.causation_id = Some(causation_id.into());
        self
    }
}

/// Orchestration event payload types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrchestrationEventData {
    // ========================================================================
    // Run Lifecycle Events
    // ========================================================================
    /// A run has been triggered.
    RunTriggered {
        /// Run identifier.
        run_id: String,
        /// Plan identifier.
        plan_id: String,
        /// What triggered this run.
        trigger: TriggerInfo,
        /// Root assets to materialize.
        root_assets: Vec<String>,
        /// Optional run key for idempotency (e.g., "daily-etl:2025-01-15").
        #[serde(skip_serializing_if = "Option::is_none")]
        run_key: Option<String>,
        /// Optional labels for the run.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        labels: HashMap<String, String>,
        /// Code version for this run (e.g., deployment version or git SHA).
        #[serde(skip_serializing_if = "Option::is_none")]
        code_version: Option<String>,
    },

    /// Plan has been created with task graph.
    PlanCreated {
        /// Run identifier.
        run_id: String,
        /// Plan identifier.
        plan_id: String,
        /// Task definitions.
        tasks: Vec<TaskDef>,
    },

    /// Cancellation has been requested for a run.
    RunCancelRequested {
        /// Run identifier.
        run_id: String,
        /// Optional reason for cancellation.
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Who requested the cancellation.
        requested_by: String,
    },

    // ========================================================================
    // Task Lifecycle Events (Worker Facts)
    // ========================================================================
    /// Task has started executing.
    TaskStarted {
        /// Run identifier.
        run_id: String,
        /// Task name within run.
        task_key: String,
        /// Attempt number (1-indexed).
        attempt: u32,
        /// Attempt identifier - concurrency guard per ADR-022.
        attempt_id: String,
        /// Worker that started execution.
        worker_id: String,
    },

    /// Task heartbeat received.
    TaskHeartbeat {
        /// Run identifier.
        run_id: String,
        /// Task name within run.
        task_key: String,
        /// Attempt number.
        attempt: u32,
        /// Attempt identifier - must match active attempt.
        attempt_id: String,
        /// Worker identifier.
        worker_id: String,
        /// Heartbeat timestamp from worker (UTC).
        #[serde(skip_serializing_if = "Option::is_none")]
        heartbeat_at: Option<DateTime<Utc>>,
        /// Optional progress percentage (0-100).
        #[serde(skip_serializing_if = "Option::is_none")]
        progress_pct: Option<u8>,
        /// Optional status message.
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },

    /// Task has finished executing.
    TaskFinished {
        /// Run identifier.
        run_id: String,
        /// Task name within run.
        task_key: String,
        /// Attempt number.
        attempt: u32,
        /// Attempt identifier - must match active attempt for state update.
        attempt_id: String,
        /// Worker identifier.
        worker_id: String,
        /// Task outcome.
        outcome: TaskOutcome,
        /// Materialization ID if succeeded.
        #[serde(skip_serializing_if = "Option::is_none")]
        materialization_id: Option<String>,
        /// Error message if failed.
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
        /// Full output payload (serialized).
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<Value>,
        /// Full error payload (serialized).
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<Value>,
        /// Execution metrics payload (serialized).
        #[serde(skip_serializing_if = "Option::is_none")]
        metrics: Option<Value>,
        /// Phase when cancellation occurred (if CANCELLED).
        #[serde(skip_serializing_if = "Option::is_none")]
        cancelled_during_phase: Option<String>,
        /// Partial progress payload for cancellation.
        #[serde(skip_serializing_if = "Option::is_none")]
        partial_progress: Option<Value>,
        /// Asset key if this task materialized an asset partition.
        #[serde(skip_serializing_if = "Option::is_none")]
        asset_key: Option<String>,
        /// Partition key if this task materialized a partition.
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_key: Option<String>,
        /// Code version (e.g., git SHA) used for this materialization.
        #[serde(skip_serializing_if = "Option::is_none")]
        code_version: Option<String>,
    },

    // ========================================================================
    // Intent Events
    // ========================================================================
    /// Request to dispatch a task.
    DispatchRequested {
        /// Run identifier.
        run_id: String,
        /// Task name within run.
        task_key: String,
        /// Attempt number.
        attempt: u32,
        /// Attempt identifier to include in dispatch payload.
        attempt_id: String,
        /// Target worker queue name.
        #[serde(default = "default_worker_queue")]
        worker_queue: String,
        /// Internal dispatch ID.
        dispatch_id: String,
    },

    /// Request to create a timer.
    TimerRequested {
        /// Internal timer ID.
        timer_id: String,
        /// Timer type.
        timer_type: TimerType,
        /// Associated run (nullable for cron timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        /// Associated task (nullable for cron timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        task_key: Option<String>,
        /// Associated attempt (for retry/heartbeat timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
        /// When the timer should fire.
        fire_at: DateTime<Utc>,
    },

    // ========================================================================
    // Acknowledgement Events
    // ========================================================================
    /// Dispatch has been enqueued in Cloud Tasks.
    DispatchEnqueued {
        /// Internal dispatch ID.
        dispatch_id: String,
        /// Run identifier.
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        /// Task name within run.
        #[serde(skip_serializing_if = "Option::is_none")]
        task_key: Option<String>,
        /// Attempt number.
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
        /// Cloud Tasks ID (hash-based per ADR-021).
        cloud_task_id: String,
    },

    /// Timer has been enqueued in Cloud Tasks.
    TimerEnqueued {
        /// Internal timer ID.
        timer_id: String,
        /// Associated run (nullable for cron timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        /// Associated task (nullable for cron timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        task_key: Option<String>,
        /// Associated attempt (for retry/heartbeat timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
        /// Cloud Tasks ID (hash-based per ADR-021).
        cloud_task_id: String,
    },

    /// Timer has fired.
    TimerFired {
        /// Internal timer ID.
        timer_id: String,
        /// Timer type.
        timer_type: TimerType,
        /// Associated run (nullable for cron timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        /// Associated task (nullable for cron timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        task_key: Option<String>,
        /// Associated attempt (for retry/heartbeat timers).
        #[serde(skip_serializing_if = "Option::is_none")]
        attempt: Option<u32>,
    },

    // ========================================================================
    // Automation Events (Layer 2)
    // ========================================================================
    /// A schedule tick has been evaluated.
    ScheduleTicked {
        /// Schedule identifier (ULID).
        schedule_id: String,
        /// When this tick was scheduled for.
        scheduled_for: DateTime<Utc>,
        /// Unique tick identifier: `{schedule_id}:{scheduled_for_epoch}`.
        tick_id: String,
        /// Definition version used for this tick (for replay determinism).
        definition_version: String,
        /// Snapshot of asset selection at tick time.
        asset_selection: Vec<String>,
        /// Optional partition selection snapshot.
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_selection: Option<Vec<String>>,
        /// Tick evaluation status.
        status: TickStatus,
        /// Run key if a run was requested (None if skipped).
        #[serde(skip_serializing_if = "Option::is_none")]
        run_key: Option<String>,
        /// Request fingerprint for conflict detection.
        #[serde(skip_serializing_if = "Option::is_none")]
        request_fingerprint: Option<String>,
    },

    /// A sensor has been evaluated.
    SensorEvaluated {
        /// Sensor identifier (ULID).
        sensor_id: String,
        /// Unique evaluation identifier.
        eval_id: String,
        /// Cursor value before evaluation (for poll sensors).
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor_before: Option<String>,
        /// Cursor value after evaluation (for poll sensors).
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor_after: Option<String>,
        /// Expected state version for CAS (poll sensors only).
        #[serde(skip_serializing_if = "Option::is_none")]
        expected_state_version: Option<u32>,
        /// What triggered this evaluation.
        trigger_source: TriggerSource,
        /// Run requests generated by this evaluation.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        run_requests: Vec<RunRequest>,
        /// Evaluation status.
        status: SensorEvalStatus,
    },

    /// A run has been requested by an automation trigger.
    ///
    /// Emitted atomically with `ScheduleTicked`, `SensorEvaluated`, or `BackfillChunkPlanned`.
    /// The `run_id` is computed deterministically from `run_key` at fold time.
    RunRequested {
        /// Stable run key for idempotency (e.g., "sched:01HQ123:1736935200").
        run_key: String,
        /// Fingerprint of the request payload for conflict detection.
        request_fingerprint: String,
        /// Assets to materialize.
        asset_selection: Vec<String>,
        /// Optional partition selection.
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_selection: Option<Vec<String>>,
        /// Reference to what triggered this request.
        trigger_source_ref: SourceRef,
        /// Optional labels for the run.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        labels: HashMap<String, String>,
    },

    // ========================================================================
    // Backfill Events (Layer 2)
    // ========================================================================
    /// A backfill has been created.
    BackfillCreated {
        /// Backfill identifier (ULID).
        backfill_id: String,
        /// Client request ID for idempotency.
        client_request_id: String,
        /// Assets to backfill.
        asset_selection: Vec<String>,
        /// Partition selector (compact, per P0-6).
        partition_selector: PartitionSelector,
        /// Pre-computed total partition count.
        total_partitions: u32,
        /// Number of partitions per chunk.
        chunk_size: u32,
        /// Maximum concurrent chunk runs.
        max_concurrent_runs: u32,
        /// Parent backfill ID (for retry-failed).
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_backfill_id: Option<String>,
    },

    /// A backfill chunk has been planned.
    BackfillChunkPlanned {
        /// Backfill identifier.
        backfill_id: String,
        /// Chunk identifier: `{backfill_id}:{chunk_index}`.
        chunk_id: String,
        /// Zero-indexed chunk number.
        chunk_index: u32,
        /// Partition keys in this chunk.
        partition_keys: Vec<String>,
        /// Run key for this chunk.
        run_key: String,
        /// Request fingerprint for conflict detection.
        request_fingerprint: String,
    },

    /// Backfill state has changed.
    BackfillStateChanged {
        /// Backfill identifier.
        backfill_id: String,
        /// Previous state.
        from_state: BackfillState,
        /// New state.
        to_state: BackfillState,
        /// State version (monotonic, for idempotency).
        state_version: u32,
        /// Who initiated the change (user ID or "system").
        #[serde(skip_serializing_if = "Option::is_none")]
        changed_by: Option<String>,
    },
}

impl OrchestrationEventData {
    /// Returns the event type name.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::RunTriggered { .. } => "RunTriggered",
            Self::PlanCreated { .. } => "PlanCreated",
            Self::RunCancelRequested { .. } => "RunCancelRequested",
            Self::TaskStarted { .. } => "TaskStarted",
            Self::TaskHeartbeat { .. } => "TaskHeartbeat",
            Self::TaskFinished { .. } => "TaskFinished",
            Self::DispatchRequested { .. } => "DispatchRequested",
            Self::TimerRequested { .. } => "TimerRequested",
            Self::DispatchEnqueued { .. } => "DispatchEnqueued",
            Self::TimerEnqueued { .. } => "TimerEnqueued",
            Self::TimerFired { .. } => "TimerFired",
            Self::ScheduleTicked { .. } => "ScheduleTicked",
            Self::SensorEvaluated { .. } => "SensorEvaluated",
            Self::RunRequested { .. } => "RunRequested",
            Self::BackfillCreated { .. } => "BackfillCreated",
            Self::BackfillChunkPlanned { .. } => "BackfillChunkPlanned",
            Self::BackfillStateChanged { .. } => "BackfillStateChanged",
        }
    }

    /// Returns the idempotency key for this event.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        match self {
            Self::RunTriggered {
                run_id, run_key, ..
            } => run_key
                .as_ref()
                .map_or_else(|| format!("run:{run_id}"), |key| format!("run:{key}")),
            Self::PlanCreated { run_id, .. } => format!("plan:{run_id}"),
            Self::RunCancelRequested { run_id, .. } => format!("cancel_req:{run_id}"),

            Self::TaskStarted {
                run_id,
                task_key,
                attempt,
                ..
            } => format!("started:{run_id}:{task_key}:{attempt}"),
            Self::TaskHeartbeat {
                run_id,
                task_key,
                attempt,
                attempt_id,
                heartbeat_at,
                ..
            } => heartbeat_at.as_ref().map_or_else(
                || format!("heartbeat:{run_id}:{task_key}:{attempt}:{attempt_id}"),
                |ts| {
                    format!(
                        "heartbeat:{run_id}:{task_key}:{attempt}:{attempt_id}:{}",
                        ts.timestamp_millis()
                    )
                },
            ),
            Self::TaskFinished {
                run_id,
                task_key,
                attempt,
                ..
            } => format!("finished:{run_id}:{task_key}:{attempt}"),

            Self::DispatchRequested { dispatch_id, .. } => format!("dispatch_req:{dispatch_id}"),
            Self::TimerRequested { timer_id, .. } => format!("timer_req:{timer_id}"),
            Self::DispatchEnqueued { dispatch_id, .. } => format!("dispatch_ack:{dispatch_id}"),
            Self::TimerEnqueued { timer_id, .. } => format!("timer_ack:{timer_id}"),
            Self::TimerFired { timer_id, .. } => format!("timer_fired:{timer_id}"),

            Self::ScheduleTicked { tick_id, .. } => format!("sched_tick:{tick_id}"),

            Self::SensorEvaluated {
                sensor_id,
                trigger_source,
                cursor_before,
                ..
            } => match trigger_source {
                TriggerSource::Push { message_id } => {
                    format!("sensor_eval:{sensor_id}:msg:{message_id}")
                }
                TriggerSource::Poll { poll_epoch } => {
                    let cursor_hash = cursor_before
                        .as_ref()
                        .map_or_else(|| "none".to_string(), |c| sha256_hex(c));
                    format!("sensor_eval:{sensor_id}:poll:{poll_epoch}:{cursor_hash}")
                }
            },

            Self::RunRequested {
                run_key,
                request_fingerprint,
                ..
            } => {
                let fp_hash = sha256_hex(request_fingerprint);
                format!("runreq:{run_key}:{fp_hash}")
            }

            Self::BackfillCreated {
                client_request_id, ..
            } => format!("backfill_create:{client_request_id}"),

            Self::BackfillChunkPlanned {
                backfill_id,
                chunk_index,
                ..
            } => format!("backfill_chunk:{backfill_id}:{chunk_index}"),

            Self::BackfillStateChanged {
                backfill_id,
                state_version,
                to_state,
                ..
            } => {
                let to_state = match to_state {
                    BackfillState::Pending => "PENDING",
                    BackfillState::Running => "RUNNING",
                    BackfillState::Paused => "PAUSED",
                    BackfillState::Succeeded => "SUCCEEDED",
                    BackfillState::Failed => "FAILED",
                    BackfillState::Cancelled => "CANCELLED",
                };
                format!("backfill_state:{backfill_id}:{state_version}:{to_state}")
            }
        }
    }

    /// Returns the run ID if this event is associated with a run.
    #[must_use]
    pub fn run_id(&self) -> Option<&str> {
        match self {
            Self::RunTriggered { run_id, .. }
            | Self::PlanCreated { run_id, .. }
            | Self::RunCancelRequested { run_id, .. }
            | Self::TaskStarted { run_id, .. }
            | Self::TaskHeartbeat { run_id, .. }
            | Self::TaskFinished { run_id, .. }
            | Self::DispatchRequested { run_id, .. } => Some(run_id),

            Self::TimerRequested { run_id, .. }
            | Self::DispatchEnqueued { run_id, .. }
            | Self::TimerEnqueued { run_id, .. }
            | Self::TimerFired { run_id, .. } => run_id.as_deref(),

            // Automation events don't have direct run_id (runs are created at fold time)
            Self::ScheduleTicked { .. }
            | Self::SensorEvaluated { .. }
            | Self::RunRequested { .. }
            | Self::BackfillCreated { .. }
            | Self::BackfillChunkPlanned { .. }
            | Self::BackfillStateChanged { .. } => None,
        }
    }
}

/// Trigger information for a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerInfo {
    /// Triggered by cron schedule.
    Cron {
        /// Schedule identifier.
        schedule_id: String,
    },
    /// Triggered manually by user.
    Manual {
        /// User identifier.
        user_id: String,
    },
    /// Triggered by upstream materialization.
    Materialization {
        /// Upstream materialization ID that triggered this run.
        upstream_materialization_id: String,
    },
    /// Triggered by webhook.
    Webhook {
        /// Webhook identifier.
        webhook_id: String,
    },
    /// Triggered by sensor.
    Sensor {
        /// Sensor identifier.
        sensor_id: String,
        /// Sensor cursor at trigger time.
        cursor: String,
    },
}

/// Task definition within a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDef {
    /// Task name within run.
    pub key: String,
    /// Dependencies (upstream task keys).
    pub depends_on: Vec<String>,
    /// Asset key (optional for non-asset tasks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_key: Option<String>,
    /// Partition key (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    /// Maximum retry attempts.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Heartbeat timeout in seconds.
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_sec: u32,
}

fn default_max_attempts() -> u32 {
    3
}

fn default_heartbeat_timeout() -> u32 {
    300
}

fn default_worker_queue() -> String {
    "default-queue".to_string()
}

/// Task completion outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOutcome {
    /// Task completed successfully.
    Succeeded,
    /// Task failed (may retry).
    Failed,
    /// Task was skipped (upstream failure).
    Skipped,
    /// Task was cancelled.
    Cancelled,
}

/// Run completion outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    /// All tasks succeeded.
    Succeeded,
    /// At least one task failed.
    Failed,
    /// Run was cancelled.
    Cancelled,
}

/// Timer type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerType {
    /// Retry timer for failed task.
    Retry,
    /// Heartbeat timeout check.
    HeartbeatCheck,
    /// Cron schedule tick.
    Cron,
    /// SLA deadline check.
    SlaCheck,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_envelope_creation() {
        let event = OrchestrationEvent::new(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::RunTriggered {
                run_id: "run123".into(),
                plan_id: "plan456".into(),
                trigger: TriggerInfo::Manual {
                    user_id: "user@example.com".into(),
                },
                root_assets: vec!["asset1".into()],
                run_key: None,
                labels: HashMap::new(),
                code_version: None,
            },
        );

        assert!(!event.event_id.is_empty());
        assert_eq!(event.event_type, "RunTriggered");
        assert_eq!(event.event_version, 1);
        assert_eq!(event.tenant_id, "tenant-abc");
        assert_eq!(event.workspace_id, "workspace-prod");
    }

    #[test]
    fn test_idempotency_key_with_run_key() {
        let data = OrchestrationEventData::RunTriggered {
            run_id: "run123".into(),
            plan_id: "plan456".into(),
            trigger: TriggerInfo::Cron {
                schedule_id: "daily".into(),
            },
            root_assets: vec![],
            run_key: Some("daily:2025-01-15".into()),
            labels: HashMap::new(),
            code_version: None,
        };

        assert_eq!(data.idempotency_key(), "run:daily:2025-01-15");
    }

    #[test]
    fn test_task_finished_idempotency_key() {
        let data = OrchestrationEventData::TaskFinished {
            run_id: "run123".into(),
            task_key: "extract".into(),
            attempt: 1,
            attempt_id: "att456".into(),
            worker_id: "worker-01".into(),
            outcome: TaskOutcome::Succeeded,
            materialization_id: None,
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
            asset_key: None,
            partition_key: None,
            code_version: None,
        };

        assert_eq!(data.idempotency_key(), "finished:run123:extract:1");
    }
}
