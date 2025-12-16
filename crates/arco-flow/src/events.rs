//! Execution events for Tier 2 persistence.
//!
//! This module provides `CloudEvents`-compatible event envelopes for run and task
//! state changes. Events are appended to the Tier 2 event log for eventual
//! consistency processing.
//!
//! ## `CloudEvents` Compatibility
//!
//! Event envelopes conform to the [`CloudEvents` v1.0 specification](https://cloudevents.io/):
//! - `id`: Unique event identifier (ULID)
//! - `source`: Event origin URI (`/arco/flow/{tenant}/{workspace}`)
//! - `specversion`: `CloudEvents` spec version ("1.0")
//! - `type`: Event type (`arco.flow.{event_name}`)
//! - `time`: Event timestamp (ISO 8601)
//! - `datacontenttype`: Content type of data ("application/json")
//! - `data`: The actual event payload
//!
//! ## Why ULID for Event IDs
//!
//! We use [ULID](https://github.com/ulid/spec) instead of UUID v4 for event identifiers because:
//! - **Lexicographically sortable**: ULIDs sort chronologically when compared as strings
//! - **Timestamp-based**: The first 48 bits encode millisecond precision time
//! - **Monotonic**: ULIDs generated in the same millisecond are monotonically increasing
//! - **Compatible**: Same 128-bit space as UUID, works with existing infrastructure
//!
//! These properties enable efficient event ordering without relying on separate timestamp
//! fields, which is critical for the Tier 2 append-only ledger where lexicographic file
//! ordering must equal chronological ordering.
//!
//! ## Idempotency
//!
//! Events include an `idempotency_key` for deduplication. For the same logical
//! event (e.g., task completion), use a deterministic key derived from the
//! event's identity (`run_id`, `task_id`, `attempt`). Different envelope instances
//! with the same idempotency key represent the same logical event.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use arco_core::{MaterializationId, RunId, TaskId};

use crate::run::RunState;
use crate::task::{TaskError, TaskMetrics, TaskState};

/// `CloudEvents`-compatible event envelope for execution events.
///
/// Wraps execution event data with `CloudEvents` standard attributes for
/// interoperability with event streaming systems.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    // --- CloudEvents Required Attributes ---
    /// Unique event identifier (ULID).
    pub id: String,

    /// Event origin URI.
    /// Format: `/arco/flow/{tenant_id}/{workspace_id}`
    pub source: String,

    /// `CloudEvents` specification version.
    pub specversion: String,

    /// Event type.
    /// Format: `arco.flow.{event_name}` (e.g., `arco.flow.run_started`)
    #[serde(rename = "type")]
    pub event_type: String,

    // --- CloudEvents Optional Attributes ---
    /// Event timestamp (ISO 8601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<DateTime<Utc>>,

    /// Content type of data field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datacontenttype: Option<String>,

    // --- Arco Extension Attributes ---
    /// Tenant scope.
    pub tenant_id: String,

    /// Workspace scope.
    pub workspace_id: String,

    /// Idempotency key for deduplication.
    /// Use a deterministic key for the same logical event.
    pub idempotency_key: String,

    /// Per-stream sequence number (optional).
    ///
    /// When present, this provides a total order within a stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,

    /// Stream identifier for ordering (optional).
    ///
    /// Example: `run:{run_id}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,

    /// Correlation identifier (optional).
    ///
    /// Typically a run ID for all related events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// Causation identifier (optional).
    ///
    /// Typically the ID of the event that caused this one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,

    /// Schema version for the envelope + payload.
    pub schema_version: u32,

    // --- Event Data ---
    /// Event payload.
    pub data: ExecutionEventData,
}

impl EventEnvelope {
    /// Creates a new event envelope with auto-generated ID and timestamp.
    #[must_use]
    pub fn new(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: ExecutionEventData,
    ) -> Self {
        let tenant = tenant_id.into();
        let workspace = workspace_id.into();

        let id = Ulid::new().to_string();
        let stream_id = data.run_id().map(|run_id| format!("run:{run_id}"));
        let correlation_id = data.run_id().map(ToString::to_string);
        let idempotency_key = data.idempotency_key().unwrap_or_else(|| id.clone());

        Self {
            id,
            source: format!("/arco/flow/{tenant}/{workspace}"),
            specversion: "1.0".into(),
            event_type: format!("arco.flow.{}", data.event_name()),
            time: Some(Utc::now()),
            datacontenttype: Some("application/json".into()),
            tenant_id: tenant,
            workspace_id: workspace,
            idempotency_key,
            sequence: None,
            stream_id,
            correlation_id,
            causation_id: None,
            schema_version: 1,
            data,
        }
    }

    /// Creates a new event envelope with a custom idempotency key.
    #[must_use]
    pub fn with_idempotency_key(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: ExecutionEventData,
        idempotency_key: impl Into<String>,
    ) -> Self {
        let mut envelope = Self::new(tenant_id, workspace_id, data);
        envelope.idempotency_key = idempotency_key.into();
        envelope
    }

    /// Sets stream ordering metadata.
    #[must_use]
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    /// Sets the causation identifier.
    #[must_use]
    pub fn with_causation_id(mut self, causation_id: impl Into<String>) -> Self {
        self.causation_id = Some(causation_id.into());
        self
    }

    /// Returns the run ID associated with this event (if any).
    #[must_use]
    pub fn run_id(&self) -> Option<&RunId> {
        self.data.run_id()
    }

    /// Returns the task ID associated with this event (if any).
    #[must_use]
    pub fn task_id(&self) -> Option<&TaskId> {
        self.data.task_id()
    }
}

/// Execution event data payloads.
///
/// This enum contains the actual event data, wrapped by `EventEnvelope`
/// for `CloudEvents` compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum ExecutionEventData {
    /// A run has been created.
    RunCreated {
        /// Run identifier.
        run_id: RunId,
        /// Plan identifier.
        plan_id: String,
        /// Trigger type.
        trigger_type: String,
        /// Triggered by (user or schedule).
        #[serde(skip_serializing_if = "Option::is_none")]
        triggered_by: Option<String>,
    },

    /// A run has started executing.
    RunStarted {
        /// Run identifier.
        run_id: RunId,
        /// Plan identifier.
        plan_id: String,
    },

    /// A run has completed (succeeded, failed, or cancelled).
    RunCompleted {
        /// Run identifier.
        run_id: RunId,
        /// Final run state.
        state: RunState,
        /// Tasks succeeded count.
        tasks_succeeded: usize,
        /// Tasks failed count.
        tasks_failed: usize,
        /// Tasks skipped count.
        tasks_skipped: usize,
        /// Tasks cancelled count.
        tasks_cancelled: usize,
    },

    /// A task has been queued for execution.
    TaskQueued {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
    },

    /// A task has been dispatched to a worker.
    TaskDispatched {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
        /// Worker identifier.
        worker_id: String,
    },

    /// A task has started executing (worker acknowledged).
    TaskStarted {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
        /// Worker identifier.
        worker_id: String,
    },

    /// A task has completed (succeeded, failed, skipped, or cancelled).
    TaskCompleted {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Final task state.
        state: TaskState,
        /// Attempt number.
        attempt: u32,
    },

    /// A task has produced output (on success).
    TaskOutput {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Materialization identifier.
        materialization_id: MaterializationId,
        /// Row count produced.
        row_count: i64,
        /// Bytes produced.
        byte_size: i64,
    },

    /// A task has failed with an error.
    TaskFailed {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Error information.
        error: TaskError,
        /// Attempt number.
        attempt: u32,
    },

    /// A retry has been scheduled for a failed task.
    TaskRetryScheduled {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Failed attempt number.
        attempt: u32,
        /// When the retry is eligible to run again.
        retry_at: DateTime<Utc>,
        /// Backoff delay in milliseconds.
        backoff_ms: i64,
    },

    /// A task has been retried.
    TaskRetried {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Previous attempt number.
        previous_attempt: u32,
        /// New attempt number.
        new_attempt: u32,
    },

    /// Task heartbeat received.
    TaskHeartbeat {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
    },

    /// Task execution metrics have been recorded.
    TaskMetricsRecorded {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Execution metrics.
        metrics: TaskMetrics,
    },
}

impl ExecutionEventData {
    /// Returns the event name (`snake_case`) for the `CloudEvents` type field.
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::RunCreated { .. } => "run_created",
            Self::RunStarted { .. } => "run_started",
            Self::RunCompleted { .. } => "run_completed",
            Self::TaskQueued { .. } => "task_queued",
            Self::TaskDispatched { .. } => "task_dispatched",
            Self::TaskStarted { .. } => "task_started",
            Self::TaskCompleted { .. } => "task_completed",
            Self::TaskOutput { .. } => "task_output",
            Self::TaskFailed { .. } => "task_failed",
            Self::TaskRetryScheduled { .. } => "task_retry_scheduled",
            Self::TaskRetried { .. } => "task_retried",
            Self::TaskHeartbeat { .. } => "task_heartbeat",
            Self::TaskMetricsRecorded { .. } => "task_metrics_recorded",
        }
    }

    /// Returns a deterministic idempotency key for events that represent durable state transitions.
    ///
    /// For high-frequency events (e.g., heartbeats) this returns `None` and callers should fall
    /// back to per-envelope uniqueness (e.g., `event_id`).
    #[must_use]
    pub fn idempotency_key(&self) -> Option<String> {
        match self {
            Self::RunCreated { run_id, .. } => Some(format!("run_created:{run_id}")),
            Self::RunStarted { run_id, .. } => Some(format!("run_started:{run_id}")),
            Self::RunCompleted { run_id, .. } => Some(format!("run_completed:{run_id}")),

            Self::TaskQueued {
                run_id,
                task_id,
                attempt,
            } => Some(format!("task_queued:{run_id}:{task_id}:{attempt}")),
            Self::TaskDispatched {
                run_id,
                task_id,
                attempt,
                ..
            } => Some(format!("task_dispatched:{run_id}:{task_id}:{attempt}")),
            Self::TaskStarted {
                run_id,
                task_id,
                attempt,
                ..
            } => Some(format!("task_started:{run_id}:{task_id}:{attempt}")),
            Self::TaskCompleted {
                run_id,
                task_id,
                state,
                attempt,
            } => Some(format!(
                "task_completed:{run_id}:{task_id}:{attempt}:{state}"
            )),
            Self::TaskOutput {
                materialization_id, ..
            } => Some(format!("task_output:{materialization_id}")),
            Self::TaskFailed {
                run_id,
                task_id,
                attempt,
                ..
            } => Some(format!("task_failed:{run_id}:{task_id}:{attempt}")),
            Self::TaskRetryScheduled {
                run_id,
                task_id,
                attempt,
                ..
            } => Some(format!("task_retry_scheduled:{run_id}:{task_id}:{attempt}")),
            Self::TaskRetried {
                run_id,
                task_id,
                previous_attempt,
                new_attempt,
            } => Some(format!(
                "task_retried:{run_id}:{task_id}:{previous_attempt}:{new_attempt}"
            )),
            Self::TaskMetricsRecorded { .. } | Self::TaskHeartbeat { .. } => None,
        }
    }

    /// Returns the run ID associated with this event.
    #[must_use]
    pub fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::RunCreated { run_id, .. }
            | Self::RunStarted { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::TaskQueued { run_id, .. }
            | Self::TaskDispatched { run_id, .. }
            | Self::TaskStarted { run_id, .. }
            | Self::TaskCompleted { run_id, .. }
            | Self::TaskOutput { run_id, .. }
            | Self::TaskFailed { run_id, .. }
            | Self::TaskRetryScheduled { run_id, .. }
            | Self::TaskRetried { run_id, .. }
            | Self::TaskHeartbeat { run_id, .. }
            | Self::TaskMetricsRecorded { run_id, .. } => Some(run_id),
        }
    }

    /// Returns the task ID associated with this event (if any).
    #[must_use]
    pub fn task_id(&self) -> Option<&TaskId> {
        match self {
            Self::TaskQueued { task_id, .. }
            | Self::TaskDispatched { task_id, .. }
            | Self::TaskStarted { task_id, .. }
            | Self::TaskCompleted { task_id, .. }
            | Self::TaskOutput { task_id, .. }
            | Self::TaskFailed { task_id, .. }
            | Self::TaskRetryScheduled { task_id, .. }
            | Self::TaskRetried { task_id, .. }
            | Self::TaskHeartbeat { task_id, .. }
            | Self::TaskMetricsRecorded { task_id, .. } => Some(task_id),
            Self::RunCreated { .. } | Self::RunStarted { .. } | Self::RunCompleted { .. } => None,
        }
    }
}

/// Builder for creating event envelopes from run state.
pub struct EventBuilder;

impl EventBuilder {
    /// Creates a `RunCreated` event envelope.
    #[must_use]
    pub fn run_created(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        plan_id: impl Into<String>,
        trigger_type: impl Into<String>,
        triggered_by: Option<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::RunCreated {
                run_id,
                plan_id: plan_id.into(),
                trigger_type: trigger_type.into(),
                triggered_by,
            },
        )
    }

    /// Creates a `RunStarted` event envelope.
    #[must_use]
    pub fn run_started(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        plan_id: impl Into<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::RunStarted {
                run_id,
                plan_id: plan_id.into(),
            },
        )
    }

    /// Creates a `RunCompleted` event envelope.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn run_completed(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        state: RunState,
        tasks_succeeded: usize,
        tasks_failed: usize,
        tasks_skipped: usize,
        tasks_cancelled: usize,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::RunCompleted {
                run_id,
                state,
                tasks_succeeded,
                tasks_failed,
                tasks_skipped,
                tasks_cancelled,
            },
        )
    }

    /// Creates a `TaskQueued` event envelope.
    #[must_use]
    pub fn task_queued(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskQueued {
                run_id,
                task_id,
                attempt,
            },
        )
    }

    /// Creates a `TaskDispatched` event envelope.
    #[must_use]
    pub fn task_dispatched(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
        worker_id: impl Into<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskDispatched {
                run_id,
                task_id,
                attempt,
                worker_id: worker_id.into(),
            },
        )
    }

    /// Creates a `TaskStarted` event envelope.
    #[must_use]
    pub fn task_started(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
        worker_id: impl Into<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskStarted {
                run_id,
                task_id,
                attempt,
                worker_id: worker_id.into(),
            },
        )
    }

    /// Creates a `TaskCompleted` event envelope.
    #[must_use]
    pub fn task_completed(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        state: TaskState,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskCompleted {
                run_id,
                task_id,
                state,
                attempt,
            },
        )
    }

    /// Creates a `TaskFailed` event envelope.
    #[must_use]
    pub fn task_failed(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        error: TaskError,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskFailed {
                run_id,
                task_id,
                error,
                attempt,
            },
        )
    }

    /// Creates a `TaskRetryScheduled` event envelope.
    #[must_use]
    pub fn task_retry_scheduled(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
        retry_at: DateTime<Utc>,
        backoff: Duration,
    ) -> EventEnvelope {
        let backoff_ms = i64::try_from(backoff.as_millis()).unwrap_or(i64::MAX);

        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskRetryScheduled {
                run_id,
                task_id,
                attempt,
                retry_at,
                backoff_ms,
            },
        )
    }

    /// Creates a `TaskRetried` event envelope.
    #[must_use]
    pub fn task_retried(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        previous_attempt: u32,
        new_attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskRetried {
                run_id,
                task_id,
                previous_attempt,
                new_attempt,
            },
        )
    }

    /// Creates a `TaskOutput` event envelope.
    #[must_use]
    pub fn task_output(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        materialization_id: MaterializationId,
        row_count: i64,
        byte_size: i64,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskOutput {
                run_id,
                task_id,
                materialization_id,
                row_count,
                byte_size,
            },
        )
    }

    /// Creates a `TaskHeartbeat` event envelope.
    #[must_use]
    pub fn task_heartbeat(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskHeartbeat {
                run_id,
                task_id,
                attempt,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_envelope_has_cloudevents_attributes() {
        let run_id = RunId::generate();
        let expected_stream_id = format!("run:{run_id}");
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::RunStarted {
                run_id,
                plan_id: "plan-123".into(),
            },
        );

        assert!(!envelope.id.is_empty());
        assert_eq!(envelope.specversion, "1.0");
        assert!(envelope.source.contains("arco"));
        assert!(envelope.event_type.starts_with("arco.flow."));
        assert!(envelope.time.is_some());
        assert!(!envelope.idempotency_key.is_empty());

        assert_eq!(envelope.tenant_id, "tenant");
        assert_eq!(envelope.workspace_id, "workspace");
        assert_eq!(
            envelope.stream_id.as_deref(),
            Some(expected_stream_id.as_str())
        );
    }

    #[test]
    fn event_envelope_serializes_cloudevents_format() -> serde_json::Result<()> {
        let run_id = RunId::generate();
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::RunStarted {
                run_id,
                plan_id: "plan-123".into(),
            },
        );

        let json = serde_json::to_string(&envelope)?;

        assert!(json.contains("\"specversion\":\"1.0\""));
        assert!(json.contains("\"type\":\"arco.flow.run_started\""));
        assert!(json.contains("\"source\":"));
        assert!(json.contains("\"id\":"));
        assert!(json.contains("\"data\":"));

        Ok(())
    }

    #[test]
    fn run_started_event_serializes() -> serde_json::Result<()> {
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::RunStarted {
                run_id: RunId::generate(),
                plan_id: "plan-123".into(),
            },
        );

        let json = serde_json::to_string(&envelope)?;
        assert!(json.contains("run_started"));

        Ok(())
    }

    #[test]
    fn task_completed_event_serializes() -> serde_json::Result<()> {
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::TaskCompleted {
                run_id: RunId::generate(),
                task_id: TaskId::generate(),
                state: TaskState::Succeeded,
                attempt: 1,
            },
        );

        let json = serde_json::to_string(&envelope)?;
        assert!(json.contains("task_completed"));

        Ok(())
    }

    #[test]
    fn event_idempotency_key_is_deterministic() {
        let run_id = RunId::generate();
        let task_id = TaskId::generate();

        let envelope1 = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::TaskCompleted {
                run_id,
                task_id,
                state: TaskState::Succeeded,
                attempt: 1,
            },
        );

        let envelope2 = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::TaskCompleted {
                run_id,
                task_id,
                state: TaskState::Succeeded,
                attempt: 1,
            },
        );

        assert_eq!(envelope1.idempotency_key, envelope2.idempotency_key);
        assert_ne!(envelope1.id, envelope2.id);
    }

    #[test]
    fn run_started_envelope_serializes() -> serde_json::Result<()> {
        let envelope =
            EventBuilder::run_started("tenant", "workspace", RunId::generate(), "plan-123");

        let json = serde_json::to_string(&envelope)?;

        assert!(json.contains("\"specversion\":\"1.0\""));
        assert!(json.contains("\"type\":\"arco.flow.run_started\""));
        assert!(json.contains("/arco/flow/tenant/workspace"));
        assert!(json.contains("run_started"));

        Ok(())
    }

    #[test]
    fn task_completed_envelope_serializes() -> serde_json::Result<()> {
        let envelope = EventBuilder::task_completed(
            "tenant",
            "workspace",
            RunId::generate(),
            TaskId::generate(),
            TaskState::Succeeded,
            1,
        );

        let json = serde_json::to_string(&envelope)?;

        assert!(json.contains("\"specversion\":\"1.0\""));
        assert!(json.contains("\"type\":\"arco.flow.task_completed\""));
        assert!(json.contains("\"SUCCEEDED\""));

        Ok(())
    }

    #[test]
    fn event_builder_creates_valid_envelopes() {
        let run_id = RunId::generate();
        let task_id = TaskId::generate();

        let created = EventBuilder::run_created(
            "tenant",
            "workspace",
            run_id,
            "plan-1",
            "Manual",
            Some("user@example.com".into()),
        );
        assert_eq!(created.event_type, "arco.flow.run_created");
        assert_eq!(created.specversion, "1.0");

        let queued = EventBuilder::task_queued("tenant", "workspace", run_id, task_id, 1);
        assert_eq!(queued.event_type, "arco.flow.task_queued");
        assert_eq!(queued.id.len(), 26); // ULID length
    }

    #[test]
    fn envelopes_roundtrip_through_json() -> serde_json::Result<()> {
        let envelope = EventBuilder::run_completed(
            "tenant",
            "workspace",
            RunId::generate(),
            RunState::Succeeded,
            5,
            0,
            0,
            0,
        );

        let json = serde_json::to_string(&envelope)?;
        let parsed: EventEnvelope = serde_json::from_str(&json)?;

        assert_eq!(envelope.id, parsed.id);
        assert_eq!(envelope.event_type, parsed.event_type);
        assert_eq!(envelope.specversion, parsed.specversion);

        Ok(())
    }

    #[test]
    fn event_data_extracts_ids() {
        let run_id = RunId::generate();
        let task_id = TaskId::generate();

        let data = ExecutionEventData::TaskStarted {
            run_id,
            task_id,
            attempt: 1,
            worker_id: "worker-1".into(),
        };

        assert_eq!(data.run_id(), Some(&run_id));
        assert_eq!(data.task_id(), Some(&task_id));
    }

    #[test]
    fn heartbeat_event_creates_correctly() {
        let envelope = EventBuilder::task_heartbeat(
            "tenant",
            "workspace",
            RunId::generate(),
            TaskId::generate(),
            2,
        );

        assert_eq!(envelope.event_type, "arco.flow.task_heartbeat");
        matches!(envelope.data, ExecutionEventData::TaskHeartbeat { .. });
    }
}
