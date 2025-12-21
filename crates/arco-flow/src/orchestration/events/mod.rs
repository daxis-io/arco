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
//!
//! Derived state changes (`TaskBecameReady`, `TaskSkipped`, `RunCompleted`) are
//! projection-only - computed during compaction fold, not emitted as ledger events.
//!
//! ## Idempotency
//!
//! Every event includes an `idempotency_key` for deduplication. The compactor
//! uses this to ensure duplicate events are no-ops.
//!
//! ## Attempt ID (per ADR-022)
//!
//! Task events include `attempt_id` (ULID) as a concurrency guard. This prevents
//! state regression when out-of-order events arrive (e.g., "attempt 1 finished"
//! arriving after "attempt 2 started").

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Orchestration event envelope.
///
/// Wraps event data with metadata for the ledger. The envelope follows the
/// CloudEvents-inspired pattern from ADR-004.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationEvent {
    /// Unique event identifier (ULID).
    pub event_id: String,

    /// Event type (e.g., "TaskFinished", "DispatchRequested").
    pub event_type: String,

    /// Schema version for forward compatibility.
    pub event_version: u32,

    /// When the event was created.
    pub timestamp: DateTime<Utc>,

    /// Event origin URI (e.g., "servo/tenant-abc/workspace-prod").
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
            timestamp: Utc::now(),
            source: format!("servo/{tenant}/{workspace}"),
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

    /// Run has completed (projection-only, but can be emitted for observability).
    RunCompleted {
        /// Run identifier.
        run_id: String,
        /// Final outcome.
        outcome: RunOutcome,
        /// Tasks succeeded count.
        tasks_succeeded: u32,
        /// Tasks failed count.
        tasks_failed: u32,
        /// Tasks skipped count.
        tasks_skipped: u32,
        /// Total duration in milliseconds.
        duration_ms: u64,
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
        /// Attempt identifier (ULID) - concurrency guard per ADR-022.
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
        /// Heartbeat timestamp from worker (UTC).
        #[serde(skip_serializing_if = "Option::is_none")]
        heartbeat_at: Option<DateTime<Utc>>,
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
        /// Task outcome.
        outcome: TaskOutcome,
        /// Materialization ID if succeeded.
        #[serde(skip_serializing_if = "Option::is_none")]
        materialization_id: Option<String>,
        /// Error message if failed.
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
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
}

impl OrchestrationEventData {
    /// Returns the event type name.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::RunTriggered { .. } => "RunTriggered",
            Self::PlanCreated { .. } => "PlanCreated",
            Self::RunCompleted { .. } => "RunCompleted",
            Self::TaskStarted { .. } => "TaskStarted",
            Self::TaskHeartbeat { .. } => "TaskHeartbeat",
            Self::TaskFinished { .. } => "TaskFinished",
            Self::DispatchRequested { .. } => "DispatchRequested",
            Self::TimerRequested { .. } => "TimerRequested",
            Self::DispatchEnqueued { .. } => "DispatchEnqueued",
            Self::TimerEnqueued { .. } => "TimerEnqueued",
            Self::TimerFired { .. } => "TimerFired",
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
            Self::RunCompleted { run_id, .. } => format!("run_completed:{run_id}"),

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
        }
    }

    /// Returns the run ID if this event is associated with a run.
    #[must_use]
    pub fn run_id(&self) -> Option<&str> {
        match self {
            Self::RunTriggered { run_id, .. }
            | Self::PlanCreated { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::TaskStarted { run_id, .. }
            | Self::TaskHeartbeat { run_id, .. }
            | Self::TaskFinished { run_id, .. }
            | Self::DispatchRequested { run_id, .. } => Some(run_id),

            Self::TimerRequested { run_id, .. } => run_id.as_deref(),

            Self::DispatchEnqueued { run_id, .. } => run_id.as_deref(),

            Self::TimerEnqueued { run_id, .. } | Self::TimerFired { run_id, .. } => {
                run_id.as_deref()
            }
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
            outcome: TaskOutcome::Succeeded,
            materialization_id: None,
            error_message: None,
        };

        assert_eq!(data.idempotency_key(), "finished:run123:extract:1");
    }
}
