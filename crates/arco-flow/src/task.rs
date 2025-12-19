//! Task execution state and lifecycle management.
//!
//! This module provides:
//! - `TaskState`: The state machine for task execution
//! - `TaskExecution`: Execution tracking for a single task
//! - `TransitionReason`: Explicit reasons for all state transitions
//! - `TaskOutput`: Output metadata from successful execution
//! - `TaskError`: Error information from failed execution

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{MaterializationId, TaskId};

use crate::error::{Error, Result};
use crate::task_key::TaskKey;

/// Reason for a task state transition.
///
/// Every state transition must have an explicit reason for:
/// - Auditing and debugging
/// - Metrics and alerting
/// - Replay and recovery decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionReason {
    // --- Happy path ---
    /// Run started, task moved from Planned to Pending.
    RunStarted,
    /// All upstream dependencies completed successfully.
    DependenciesSatisfied,
    /// Quota acquired, task moved to Queued.
    QuotaAcquired,
    /// Task sent to worker.
    DispatchedToWorker,
    /// Worker acknowledged task receipt.
    WorkerAcknowledged,
    /// Worker began execution.
    ExecutionStarted,
    /// Task completed successfully.
    ExecutionSucceeded,

    // --- Failure path ---
    /// Worker reported failure.
    ExecutionFailed,
    /// Task exceeded timeout.
    TimedOut,
    /// Worker missed heartbeat deadline.
    HeartbeatTimeout,
    /// Worker missed dispatch-ack deadline.
    DispatchAckTimeout,
    /// Upstream task failed.
    UpstreamFailed,

    // --- Recovery path ---
    /// Task queued for retry after failure.
    RetryScheduled,
    /// Retry timer expired, task ready again.
    RetryTimerExpired,

    // --- Cancellation path ---
    /// User requested cancellation.
    UserCancelled,
    /// System cancelled (e.g., run timeout).
    SystemCancelled,
    /// Parent run was cancelled.
    RunCancelled,

    // --- Skip path ---
    /// Skipped because upstream failed.
    SkippedDueToUpstreamFailure,
    /// Skipped because asset is up-to-date.
    SkippedUpToDate,
}

impl std::fmt::Display for TransitionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RunStarted => write!(f, "run_started"),
            Self::DependenciesSatisfied => write!(f, "dependencies_satisfied"),
            Self::QuotaAcquired => write!(f, "quota_acquired"),
            Self::DispatchedToWorker => write!(f, "dispatched_to_worker"),
            Self::WorkerAcknowledged => write!(f, "worker_acknowledged"),
            Self::ExecutionStarted => write!(f, "execution_started"),
            Self::ExecutionSucceeded => write!(f, "execution_succeeded"),
            Self::ExecutionFailed => write!(f, "execution_failed"),
            Self::TimedOut => write!(f, "timed_out"),
            Self::HeartbeatTimeout => write!(f, "heartbeat_timeout"),
            Self::DispatchAckTimeout => write!(f, "dispatch_ack_timeout"),
            Self::UpstreamFailed => write!(f, "upstream_failed"),
            Self::RetryScheduled => write!(f, "retry_scheduled"),
            Self::RetryTimerExpired => write!(f, "retry_timer_expired"),
            Self::UserCancelled => write!(f, "user_cancelled"),
            Self::SystemCancelled => write!(f, "system_cancelled"),
            Self::RunCancelled => write!(f, "run_cancelled"),
            Self::SkippedDueToUpstreamFailure => write!(f, "skipped_due_to_upstream_failure"),
            Self::SkippedUpToDate => write!(f, "skipped_up_to_date"),
        }
    }
}

/// Reason for a run state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTransitionReason {
    /// Run created.
    Created,
    /// Run started executing.
    Started,
    /// All tasks completed successfully.
    AllTasksSucceeded,
    /// One or more tasks failed.
    TasksFailed,
    /// User requested cancellation.
    UserCancelled,
    /// System requested cancellation.
    SystemCancelled,
    /// Run exceeded maximum duration.
    TimedOut,
}

impl std::fmt::Display for RunTransitionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "created"),
            Self::Started => write!(f, "started"),
            Self::AllTasksSucceeded => write!(f, "all_tasks_succeeded"),
            Self::TasksFailed => write!(f, "tasks_failed"),
            Self::UserCancelled => write!(f, "user_cancelled"),
            Self::SystemCancelled => write!(f, "system_cancelled"),
            Self::TimedOut => write!(f, "timed_out"),
        }
    }
}

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
            Self::Dispatched => matches!(target, Self::Running | Self::Failed | Self::Cancelled),
            Self::Running => {
                matches!(target, Self::Succeeded | Self::Failed | Self::Cancelled)
            }
            Self::Failed => matches!(target, Self::RetryWait | Self::Cancelled),
            Self::RetryWait => matches!(target, Self::Ready | Self::Cancelled),
            Self::Succeeded | Self::Skipped | Self::Cancelled => false,
        }
    }

    /// Returns a lowercase label suitable for metrics and logs.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Queued => "queued",
            Self::Dispatched => "dispatched",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
            Self::RetryWait => "retry_wait",
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
            Self::Dispatched => vec![Self::Running, Self::Failed, Self::Cancelled],
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

/// Timeout configuration for task execution.
///
/// Defines separate timeouts for dispatch acknowledgment and heartbeat monitoring.
/// This two-timeout model prevents zombie tasks from blocking the scheduler.
#[derive(Debug, Clone, Copy)]
pub struct TaskTimeoutConfig {
    /// Time for worker to acknowledge dispatch (default: 30s).
    /// If exceeded, task is requeued for dispatch.
    pub dispatch_ack_timeout: Duration,

    /// Time between heartbeats during execution (default: 60s).
    /// If exceeded, task is marked as zombie and may be requeued.
    pub heartbeat_timeout: Duration,

    /// Maximum execution time for a task (default: from `ResourceRequirements`).
    pub max_execution_time: Duration,
}

impl Default for TaskTimeoutConfig {
    fn default() -> Self {
        Self {
            dispatch_ack_timeout: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(60),
            max_execution_time: Duration::from_secs(3600),
        }
    }
}

/// Execution state for a single task within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskExecution {
    /// Task being executed.
    pub task_id: TaskId,
    /// Semantic task key for deterministic ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_key: Option<TaskKey>,
    /// Execution priority (lower = higher priority).
    #[serde(default)]
    pub priority: i32,
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
    /// When a retry is eligible to run again.
    ///
    /// Set when transitioning into `RetryWait`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<DateTime<Utc>>,
    /// Last heartbeat from worker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// Heartbeat timeout duration.
    #[serde(with = "humantime_serde", default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: Duration,
    /// Reason for the most recent state transition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_reason: Option<TransitionReason>,
    /// Timestamp of the most recent state transition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_at: Option<DateTime<Utc>>,
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
            task_key: None,
            priority: 0,
            state: TaskState::Planned,
            attempt: 1,
            max_attempts: 3,
            queued_at: None,
            dispatched_at: None,
            started_at: None,
            completed_at: None,
            retry_at: None,
            last_heartbeat: None,
            heartbeat_timeout: default_heartbeat_timeout(),
            last_transition_reason: None,
            last_transition_at: None,
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

    /// Sets task metadata used for deterministic scheduling.
    #[must_use]
    pub fn with_metadata(mut self, task_key: TaskKey, priority: i32) -> Self {
        self.task_key = Some(task_key);
        self.priority = priority;
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
        self.record_heartbeat_at(Utc::now());
    }

    /// Records a heartbeat from the worker at a given time.
    pub fn record_heartbeat_at(&mut self, now: DateTime<Utc>) {
        self.last_heartbeat = Some(now);
    }

    /// Returns true if the heartbeat is stale (exceeded timeout).
    ///
    /// A task is considered stale if:
    /// - It is in RUNNING or DISPATCHED state
    /// - AND no heartbeat has been received
    /// - OR the last heartbeat exceeds the timeout
    #[must_use]
    pub fn is_heartbeat_stale(&self) -> bool {
        self.is_heartbeat_stale_at(Utc::now())
    }

    /// Returns true if the heartbeat is stale (exceeded timeout) at a given time.
    #[must_use]
    pub fn is_heartbeat_stale_at(&self, now: DateTime<Utc>) -> bool {
        if !matches!(self.state, TaskState::Running | TaskState::Dispatched) {
            return false;
        }

        self.last_heartbeat.is_none_or(|last| {
            let elapsed = now.signed_duration_since(last);
            elapsed
                > chrono::Duration::from_std(self.heartbeat_timeout)
                    .unwrap_or(chrono::Duration::MAX)
        })
    }

    /// Returns true if a retry wait has expired at a given time.
    #[must_use]
    pub fn is_retry_due_at(&self, now: DateTime<Utc>) -> bool {
        if self.state != TaskState::RetryWait {
            return false;
        }

        self.retry_at.is_none_or(|retry_at| now >= retry_at)
    }

    /// Returns true if this task is a "zombie" (missed heartbeat/dispatch-ack deadline).
    ///
    /// Zombie tasks should be requeued for dispatch. Uses separate timeouts for:
    /// - `Dispatched` state: dispatch-ack timeout (worker didn't acknowledge in time)
    /// - `Running` state: heartbeat timeout (worker stopped sending heartbeats)
    ///
    /// **Two-timeout model (PR-7):** Dispatch-ack is typically shorter (30s) than
    /// heartbeat timeout (60s) because ack should be immediate, while running tasks
    /// may have brief communication gaps.
    #[must_use]
    pub fn is_zombie(&self, now: DateTime<Utc>, config: &TaskTimeoutConfig) -> bool {
        let dispatch_ack_duration = chrono::Duration::from_std(config.dispatch_ack_timeout)
            .unwrap_or_else(|_| chrono::Duration::seconds(30));
        let heartbeat_duration = chrono::Duration::from_std(config.heartbeat_timeout)
            .unwrap_or_else(|_| chrono::Duration::seconds(60));

        match self.state {
            TaskState::Dispatched => {
                // Check dispatch-ack timeout
                self.dispatched_at
                    .is_some_and(|dispatched_at| now > dispatched_at + dispatch_ack_duration)
            }
            TaskState::Running => {
                // Check heartbeat timeout: prefer last_heartbeat, fallback to dispatched_at
                self.last_heartbeat
                    .or(self.dispatched_at)
                    .is_some_and(|timestamp| now > timestamp + heartbeat_duration)
            }
            _ => false,
        }
    }

    /// Attempts to transition to a terminal state idempotently.
    ///
    /// Returns `Ok(true)` if transition was applied.
    /// Returns `Ok(false)` if already in terminal state (idempotent no-op).
    /// Returns `Err` if transition is invalid.
    ///
    /// **Key invariant (PR-8):** Late/duplicate callbacks for attempt N cannot
    /// affect attempt N+1's state. The `attempt` parameter ensures callbacks
    /// for old attempts are rejected.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid (e.g., transitioning to
    /// a non-terminal state or to a different terminal state).
    pub fn try_terminal_transition(
        &mut self,
        target: TaskState,
        reason: TransitionReason,
        attempt: u32,
    ) -> Result<bool> {
        // Validate target is terminal
        if !target.is_terminal() {
            return Err(Error::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
                reason: "try_terminal_transition requires a terminal state".to_string(),
            });
        }

        // Reject callbacks for old attempts
        if attempt < self.attempt {
            // Late callback from previous attempt - ignore
            return Ok(false);
        }

        // Already in terminal state?
        if self.state.is_terminal() {
            // Idempotent: same state = no-op
            if self.state == target {
                return Ok(false);
            }
            // Different terminal state = error (shouldn't happen)
            return Err(Error::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
                reason: "cannot transition from one terminal state to another".to_string(),
            });
        }

        // Apply transition
        let now = Utc::now();
        self.state = target;
        self.last_transition_reason = Some(reason);
        self.last_transition_at = Some(now);
        self.completed_at = Some(now);

        if let Some(started) = self.started_at {
            self.metrics.duration_ms = (now - started).num_milliseconds();
        }

        Ok(true)
    }

    /// Transitions to a new state with an explicit reason.
    ///
    /// This is the preferred method for transitions as it captures the reason
    /// for auditing and observability.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    #[tracing::instrument(
        skip(self),
        fields(task_id = %self.task_id, from = %self.state, to = %target, reason = %reason, attempt = self.attempt)
    )]
    pub fn transition_to_with_reason(
        &mut self,
        target: TaskState,
        reason: TransitionReason,
    ) -> Result<()> {
        self.transition_to(target)?;
        self.last_transition_reason = Some(reason);
        self.last_transition_at = Some(Utc::now());
        Ok(())
    }

    /// Transitions to a new state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    #[tracing::instrument(
        skip(self),
        fields(task_id = %self.task_id, from = %self.state, to = %target, attempt = self.attempt)
    )]
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
                    self.queued_at = None;
                    self.dispatched_at = None;
                    self.started_at = None;
                    self.completed_at = None;
                    self.last_heartbeat = None;
                    self.worker_id = None;
                    self.output = None;
                    self.error = None;
                    self.metrics = TaskMetrics::default();
                    self.retry_at = None;
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
        self.error = None;
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
        self.output = None;
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
        assert!(state.can_transition_to(TaskState::Failed));
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
    fn task_execution_state_machine() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        assert_eq!(exec.state, TaskState::Planned);

        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        assert!(exec.queued_at.is_some());

        exec.transition_to(TaskState::Dispatched)?;
        assert!(exec.dispatched_at.is_some());

        exec.transition_to(TaskState::Running)?;
        assert!(exec.started_at.is_some());

        exec.transition_to(TaskState::Succeeded)?;
        assert_eq!(exec.state, TaskState::Succeeded);
        assert!(exec.completed_at.is_some());

        Ok(())
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
    fn task_execution_heartbeat() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id).with_heartbeat_timeout(Duration::from_secs(30));

        // Progress to running state
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;

        // Initial heartbeat is set when transitioning to Running
        assert!(exec.last_heartbeat.is_some());
        assert!(!exec.is_heartbeat_stale()); // Fresh heartbeat

        // Record new heartbeat
        exec.record_heartbeat();
        assert!(!exec.is_heartbeat_stale());

        Ok(())
    }

    #[test]
    fn transition_reason_display() {
        assert_eq!(TransitionReason::RunStarted.to_string(), "run_started");
        assert_eq!(
            TransitionReason::DependenciesSatisfied.to_string(),
            "dependencies_satisfied"
        );
        assert_eq!(
            TransitionReason::HeartbeatTimeout.to_string(),
            "heartbeat_timeout"
        );
        assert_eq!(
            TransitionReason::UserCancelled.to_string(),
            "user_cancelled"
        );
    }

    #[test]
    fn run_transition_reason_display() {
        assert_eq!(RunTransitionReason::Created.to_string(), "created");
        assert_eq!(
            RunTransitionReason::AllTasksSucceeded.to_string(),
            "all_tasks_succeeded"
        );
    }

    #[test]
    fn transition_to_with_reason_tracks_reason() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        exec.transition_to_with_reason(TaskState::Pending, TransitionReason::RunStarted)?;

        assert_eq!(exec.state, TaskState::Pending);
        assert_eq!(
            exec.last_transition_reason,
            Some(TransitionReason::RunStarted)
        );
        assert!(exec.last_transition_at.is_some());

        Ok(())
    }

    #[test]
    fn is_zombie_detects_dispatch_ack_timeout() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);
        let config = TaskTimeoutConfig {
            dispatch_ack_timeout: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(60),
            max_execution_time: Duration::from_secs(3600),
        };

        // Progress to dispatched state
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;

        // Immediately after dispatch - not a zombie
        let now = exec.dispatched_at.expect("dispatched_at should be set");
        assert!(!exec.is_zombie(now, &config));

        // 31 seconds later - zombie (exceeded 30s dispatch-ack timeout)
        let later = now + chrono::Duration::seconds(31);
        assert!(exec.is_zombie(later, &config));

        Ok(())
    }

    #[test]
    fn is_zombie_detects_heartbeat_timeout() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);
        let config = TaskTimeoutConfig {
            dispatch_ack_timeout: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(60),
            max_execution_time: Duration::from_secs(3600),
        };

        // Progress to running state
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;

        // Immediately after start - not a zombie
        let now = exec.last_heartbeat.expect("last_heartbeat should be set");
        assert!(!exec.is_zombie(now, &config));

        // 61 seconds later - zombie (exceeded 60s heartbeat timeout)
        let later = now + chrono::Duration::seconds(61);
        assert!(exec.is_zombie(later, &config));

        Ok(())
    }

    #[test]
    fn is_zombie_not_for_other_states() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);
        let config = TaskTimeoutConfig::default();

        // Planned state - never a zombie
        let now = Utc::now();
        assert!(!exec.is_zombie(now, &config));

        // Pending state - never a zombie
        exec.transition_to(TaskState::Pending)?;
        assert!(!exec.is_zombie(now, &config));

        // Ready state - never a zombie
        exec.transition_to(TaskState::Ready)?;
        assert!(!exec.is_zombie(now, &config));

        Ok(())
    }

    #[test]
    fn try_terminal_transition_applies_on_valid_transition() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Progress to running state
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;

        // Successful terminal transition
        let applied = exec.try_terminal_transition(
            TaskState::Succeeded,
            TransitionReason::ExecutionSucceeded,
            1,
        )?;

        assert!(applied);
        assert_eq!(exec.state, TaskState::Succeeded);
        assert_eq!(
            exec.last_transition_reason,
            Some(TransitionReason::ExecutionSucceeded)
        );
        assert!(exec.last_transition_at.is_some());
        assert!(exec.completed_at.is_some());

        Ok(())
    }

    #[test]
    fn try_terminal_transition_idempotent_on_same_state() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Progress to running and succeed
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;
        exec.transition_to(TaskState::Succeeded)?;

        // Idempotent: same state returns false (no-op)
        let applied = exec.try_terminal_transition(
            TaskState::Succeeded,
            TransitionReason::ExecutionSucceeded,
            1,
        )?;

        assert!(!applied);
        assert_eq!(exec.state, TaskState::Succeeded);

        Ok(())
    }

    #[test]
    fn try_terminal_transition_rejects_old_attempt() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Progress to running
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;

        // Fail and retry
        exec.transition_to(TaskState::Failed)?;
        exec.transition_to(TaskState::RetryWait)?;
        exec.transition_to(TaskState::Ready)?; // This increments attempt to 2

        // Progress to running again
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;

        assert_eq!(exec.attempt, 2);

        // Late callback from attempt 1 should be rejected
        let applied = exec.try_terminal_transition(
            TaskState::Succeeded,
            TransitionReason::ExecutionSucceeded,
            1, // Old attempt
        )?;

        assert!(!applied);
        assert_eq!(exec.state, TaskState::Running); // State unchanged

        Ok(())
    }

    #[test]
    fn try_terminal_transition_rejects_non_terminal_state() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Try to use try_terminal_transition with a non-terminal state
        let result =
            exec.try_terminal_transition(TaskState::Running, TransitionReason::ExecutionStarted, 1);

        assert!(result.is_err());
    }

    #[test]
    fn try_terminal_transition_rejects_different_terminal_state() -> Result<()> {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Progress to succeeded
        exec.transition_to(TaskState::Pending)?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;
        exec.transition_to(TaskState::Succeeded)?;

        // Try to transition to a different terminal state
        let result =
            exec.try_terminal_transition(TaskState::Failed, TransitionReason::ExecutionFailed, 1);

        assert!(result.is_err());
        assert_eq!(exec.state, TaskState::Succeeded); // State unchanged

        Ok(())
    }

    #[test]
    fn task_timeout_config_default_values() {
        let config = TaskTimeoutConfig::default();
        assert_eq!(config.dispatch_ack_timeout, Duration::from_secs(30));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(60));
        assert_eq!(config.max_execution_time, Duration::from_secs(3600));
    }
}
