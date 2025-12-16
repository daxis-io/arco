//! Task execution state and lifecycle management.
//!
//! This module provides:
//! - `TaskState`: The state machine for task execution
//! - `TaskExecution`: Execution tracking for a single task
//! - `TaskOutput`: Output metadata from successful execution
//! - `TaskError`: Error information from failed execution

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{MaterializationId, TaskId};

use crate::error::{Error, Result};

/// Default heartbeat timeout (5 minutes).
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 300;

/// Task execution state machine.
///
/// States follow a directed graph matching the orchestration design:
/// ```text
/// ┌─────────┐  run starts  ┌─────────┐  deps met   ┌───────┐  quota   ┌────────┐
/// │ PLANNED │─────────────►│ PENDING │────────────►│ READY │─────────►│ QUEUED │
/// └─────────┘              └─────────┘             └───────┘          └────────┘
///                               │                      │                   │
///                          upstream                    │              dispatched
///                          failed                      │                   │
///                               │                      │                   ▼
///                               ▼                      │             ┌────────────┐
///                          ┌─────────┐                 │             │ DISPATCHED │
///                          │ SKIPPED │                 │             └────────────┘
///                          └─────────┘                 │                   │
///                               ▲                      │               ack received
///                               │                      │                   │
///                               │                      │                   ▼
///                               │                      │              ┌─────────┐
///                               └──────────────────────┴──────────────│ RUNNING │
///                                       (cancelled)                   └─────────┘
///                                                                          │
///                                                            ┌─────────────┼─────────────┐
///                                                            │             │             │
///                                                            ▼             ▼             ▼
///                                                      ┌───────────┐ ┌──────────┐ ┌───────────┐
///                                                      │ SUCCEEDED │ │  FAILED  │ │ CANCELLED │
///                                                      └───────────┘ └──────────┘ └───────────┘
///                                                                          │
///                                                                     retry?
///                                                                          │
///                                                                          ▼
///                                                                    ┌────────────┐
///                                                                    │ RETRY_WAIT │
///                                                                    └────────────┘
///                                                                          │
///                                                                     backoff expires
///                                                                          │
///                                                                          ▼
///                                                                      ┌───────┐
///                                                                      │ READY │
///                                                                      └───────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    /// Exists in plan, scheduler hasn't evaluated yet.
    Planned,
    /// Scheduler evaluated, blocked on dependencies.
    Pending,
    /// Dependencies met, waiting for quota.
    Ready,
    /// Quota acquired, pushed to queue.
    Queued,
    /// Sent to worker, awaiting acknowledgment.
    Dispatched,
    /// Worker acknowledged, actively executing.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Failed (may retry).
    Failed,
    /// Skipped (upstream failed).
    Skipped,
    /// Cancelled by user or system.
    Cancelled,
    /// Waiting for retry backoff.
    RetryWait,
}

impl TaskState {
    /// Returns true if this is a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }

    /// Returns true if this state allows retry.
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(self, Self::Failed | Self::RetryWait)
    }

    /// Returns true if the task is actively executing or pending execution.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Planned
                | Self::Pending
                | Self::Ready
                | Self::Queued
                | Self::Dispatched
                | Self::Running
                | Self::RetryWait
        )
    }

    /// Returns true if the transition from self to target is valid.
    #[must_use]
    pub fn can_transition_to(&self, target: Self) -> bool {
        match self {
            Self::Planned => matches!(target, Self::Pending | Self::Cancelled),
            Self::Pending => matches!(target, Self::Ready | Self::Skipped | Self::Cancelled),
            Self::Ready => matches!(target, Self::Queued | Self::Cancelled),
            Self::Queued => matches!(target, Self::Dispatched | Self::Cancelled),
            Self::Dispatched => matches!(target, Self::Running | Self::Cancelled),
            Self::Running => {
                matches!(target, Self::Succeeded | Self::Failed | Self::Cancelled)
            }
            Self::Failed => matches!(target, Self::RetryWait | Self::Cancelled),
            Self::RetryWait => matches!(target, Self::Ready | Self::Cancelled),
            Self::Succeeded | Self::Skipped | Self::Cancelled => false,
        }
    }

    /// Returns all valid target states from the current state.
    #[must_use]
    pub fn valid_transitions(&self) -> Vec<Self> {
        match self {
            Self::Planned => vec![Self::Pending, Self::Cancelled],
            Self::Pending => vec![Self::Ready, Self::Skipped, Self::Cancelled],
            Self::Ready => vec![Self::Queued, Self::Cancelled],
            Self::Queued => vec![Self::Dispatched, Self::Cancelled],
            Self::Dispatched => vec![Self::Running, Self::Cancelled],
            Self::Running => vec![Self::Succeeded, Self::Failed, Self::Cancelled],
            Self::Failed => vec![Self::RetryWait, Self::Cancelled],
            Self::RetryWait => vec![Self::Ready, Self::Cancelled],
            Self::Succeeded | Self::Skipped | Self::Cancelled => vec![],
        }
    }
}

impl Default for TaskState {
    fn default() -> Self {
        Self::Planned
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planned => write!(f, "PLANNED"),
            Self::Pending => write!(f, "PENDING"),
            Self::Ready => write!(f, "READY"),
            Self::Queued => write!(f, "QUEUED"),
            Self::Dispatched => write!(f, "DISPATCHED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Succeeded => write!(f, "SUCCEEDED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Skipped => write!(f, "SKIPPED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::RetryWait => write!(f, "RETRY_WAIT"),
        }
    }
}

/// Task error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskErrorCategory {
    /// Error in user asset code.
    UserCode,
    /// Schema mismatch or constraint violation.
    DataQuality,
    /// Network, storage, or timeout.
    Infrastructure,
    /// Invalid configuration or missing secrets.
    Configuration,
    /// Unknown error category.
    Unknown,
}

impl Default for TaskErrorCategory {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Task error information.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    /// Error category.
    pub category: TaskErrorCategory,
    /// Error message.
    pub message: String,
    /// Stack trace or detail (truncated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Whether the error is retryable.
    pub retryable: bool,
}

impl TaskError {
    /// Creates a new task error.
    #[must_use]
    pub fn new(category: TaskErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            detail: None,
            retryable: matches!(category, TaskErrorCategory::Infrastructure),
        }
    }

    /// Sets the error detail.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Sets whether the error is retryable.
    #[must_use]
    pub const fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }
}

/// Task output reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutput {
    /// Materialization ID for output tracking.
    pub materialization_id: MaterializationId,
    /// Output files.
    #[serde(default)]
    pub files: Vec<FileEntry>,
    /// Output row count.
    #[serde(default)]
    pub row_count: i64,
    /// Output size in bytes.
    #[serde(default)]
    pub byte_size: i64,
}

/// File entry for output tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    /// Storage path.
    pub path: String,
    /// File size in bytes.
    #[serde(default)]
    pub size_bytes: i64,
    /// Row count (for tabular data).
    #[serde(default)]
    pub row_count: i64,
    /// Content hash (SHA-256).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    /// File format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Task execution metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetrics {
    /// Wall clock duration in milliseconds.
    #[serde(default)]
    pub duration_ms: i64,
    /// CPU time in milliseconds.
    #[serde(default)]
    pub cpu_time_ms: i64,
    /// Peak memory usage in bytes.
    #[serde(default)]
    pub peak_memory_bytes: i64,
    /// Bytes read from storage.
    #[serde(default)]
    pub bytes_read: i64,
    /// Bytes written to storage.
    #[serde(default)]
    pub bytes_written: i64,
}

/// Execution state for a single task within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskExecution {
    /// Task being executed.
    pub task_id: TaskId,
    /// Execution state.
    pub state: TaskState,
    /// Attempt number (1-indexed, increments on retry).
    pub attempt: u32,
    /// Maximum retry attempts.
    pub max_attempts: u32,
    /// When the task was queued.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<DateTime<Utc>>,
    /// When the task was dispatched to a worker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<DateTime<Utc>>,
    /// When execution started (worker acknowledged).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When execution completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Last heartbeat from worker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// Heartbeat timeout duration.
    #[serde(with = "humantime_serde", default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: Duration,
    /// Worker that executed this task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Output reference (if succeeded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<TaskOutput>,
    /// Error information (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    /// Execution metrics.
    #[serde(default)]
    pub metrics: TaskMetrics,
}

fn default_heartbeat_timeout() -> Duration {
    Duration::from_secs(DEFAULT_HEARTBEAT_TIMEOUT_SECS)
}

impl TaskExecution {
    /// Creates a new task execution in PLANNED state.
    #[must_use]
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            state: TaskState::Planned,
            attempt: 1,
            max_attempts: 3,
            queued_at: None,
            dispatched_at: None,
            started_at: None,
            completed_at: None,
            last_heartbeat: None,
            heartbeat_timeout: default_heartbeat_timeout(),
            worker_id: None,
            output: None,
            error: None,
            metrics: TaskMetrics::default(),
        }
    }

    /// Creates a task execution with custom max attempts.
    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Creates a task execution with custom heartbeat timeout.
    #[must_use]
    pub const fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    /// Returns true if the task is in a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Returns true if the task can be retried.
    #[must_use]
    pub fn can_retry(&self) -> bool {
        self.state.is_retriable() && self.attempt < self.max_attempts
    }

    /// Records a heartbeat from the worker.
    pub fn record_heartbeat(&mut self) {
        self.last_heartbeat = Some(Utc::now());
    }

    /// Returns true if the heartbeat is stale (exceeded timeout).
    ///
    /// A task is considered stale if:
    /// - It is in RUNNING or DISPATCHED state
    /// - AND no heartbeat has been received
    /// - OR the last heartbeat exceeds the timeout
    #[must_use]
    pub fn is_heartbeat_stale(&self) -> bool {
        if !matches!(self.state, TaskState::Running | TaskState::Dispatched) {
            return false;
        }

        self.last_heartbeat.is_none_or(|last| {
            let elapsed = Utc::now().signed_duration_since(last);
            elapsed
                > chrono::Duration::from_std(self.heartbeat_timeout)
                    .unwrap_or(chrono::Duration::MAX)
        })
    }

    /// Transitions to a new state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn transition_to(&mut self, target: TaskState) -> Result<()> {
        if !self.state.can_transition_to(target) {
            return Err(Error::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
                reason: format!(
                    "valid transitions from {}: {:?}",
                    self.state,
                    self.state.valid_transitions()
                ),
            });
        }

        let now = Utc::now();

        // Update timestamps based on transition
        match target {
            TaskState::Queued => {
                self.queued_at = Some(now);
            }
            TaskState::Dispatched => {
                self.dispatched_at = Some(now);
            }
            TaskState::Running => {
                self.started_at = Some(now);
                // Record initial heartbeat when task starts
                self.last_heartbeat = Some(now);
            }
            TaskState::Ready => {
                // On retry, increment attempt counter
                if self.state == TaskState::RetryWait {
                    self.attempt += 1;
                }
            }
            TaskState::Succeeded
            | TaskState::Failed
            | TaskState::Skipped
            | TaskState::Cancelled => {
                self.completed_at = Some(now);
                if let Some(started) = self.started_at {
                    self.metrics.duration_ms = (now - started).num_milliseconds();
                }
            }
            _ => {}
        }

        self.state = target;
        Ok(())
    }

    /// Marks the task as succeeded with output.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn succeed(&mut self, output: TaskOutput) -> Result<()> {
        self.output = Some(output);
        self.transition_to(TaskState::Succeeded)
    }

    /// Marks the task as failed with error.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn fail(&mut self, error: TaskError) -> Result<()> {
        self.error = Some(error);
        self.transition_to(TaskState::Failed)
    }

    /// Marks the task as skipped (upstream failed).
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn skip(&mut self) -> Result<()> {
        self.transition_to(TaskState::Skipped)
    }

    /// Marks the task as cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn cancel(&mut self) -> Result<()> {
        self.transition_to(TaskState::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_state_full_lifecycle() {
        // Test the full state machine: PLANNED -> PENDING -> READY -> QUEUED -> DISPATCHED -> RUNNING -> SUCCEEDED
        let state = TaskState::Planned;
        assert!(state.can_transition_to(TaskState::Pending));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Pending;
        assert!(state.can_transition_to(TaskState::Ready));
        assert!(state.can_transition_to(TaskState::Skipped));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Ready;
        assert!(state.can_transition_to(TaskState::Queued));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Queued;
        assert!(state.can_transition_to(TaskState::Dispatched));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Dispatched;
        assert!(state.can_transition_to(TaskState::Running));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Succeeded));
    }

    #[test]
    fn task_state_transitions_running_to_terminal() {
        let state = TaskState::Running;
        assert!(state.can_transition_to(TaskState::Succeeded));
        assert!(state.can_transition_to(TaskState::Failed));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Pending));
    }

    #[test]
    fn task_execution_state_machine() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        assert_eq!(exec.state, TaskState::Planned);

        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        assert!(exec.queued_at.is_some());

        exec.transition_to(TaskState::Dispatched).unwrap();
        assert!(exec.dispatched_at.is_some());

        exec.transition_to(TaskState::Running).unwrap();
        assert!(exec.started_at.is_some());

        exec.transition_to(TaskState::Succeeded).unwrap();
        assert_eq!(exec.state, TaskState::Succeeded);
        assert!(exec.completed_at.is_some());
    }

    #[test]
    fn task_execution_invalid_transition_fails() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Cannot jump from PLANNED to SUCCEEDED
        let result = exec.transition_to(TaskState::Succeeded);
        assert!(result.is_err());
    }

    #[test]
    fn task_execution_heartbeat() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id).with_heartbeat_timeout(Duration::from_secs(30));

        // Progress to running state
        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        exec.transition_to(TaskState::Dispatched).unwrap();
        exec.transition_to(TaskState::Running).unwrap();

        // Initial heartbeat is set when transitioning to Running
        assert!(exec.last_heartbeat.is_some());
        assert!(!exec.is_heartbeat_stale()); // Fresh heartbeat

        // Record new heartbeat
        exec.record_heartbeat();
        assert!(!exec.is_heartbeat_stale());
    }
}
