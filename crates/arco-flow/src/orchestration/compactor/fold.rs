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
//! - Events are processed exactly once (idempotency via `row_version`)
//! - State transitions are monotonic (can't go backward)
//! - Duplicate events are no-ops

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};

use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskDef, TaskOutcome};

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
    /// Higher rank = more terminal. Used as tiebreaker when row_versions are equal.
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
        matches!(self, Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled)
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchOutboxRow {
    /// Run identifier.
    pub run_id: String,
    /// Task name.
    pub task_key: String,
    /// Attempt number.
    pub attempt: u32,
    /// Internal ID (human-readable): `dispatch:{run_id}:{task_key}:{attempt}`.
    pub dispatch_id: String,
    /// Cloud Tasks-safe ID (hash-based): `d_{base32(sha256(dispatch_id))[..26]}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_task_id: Option<String>,
    /// Current status.
    pub status: DispatchStatus,
    /// Attempt ULID (included in worker payload for concurrency guard).
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
        format!("dispatch:{}:{}:{}", run_id, task_key, attempt)
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

/// Timer row (dual-identifier pattern per ADR-021).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerRow {
    /// Internal ID (human-readable): `timer:{type}:{run_id}:{task_key}:{attempt}:{epoch}`.
    pub timer_id: String,
    /// Cloud Tasks-safe ID (hash-based): `t_{base32(sha256(timer_id))[..26]}`.
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

impl TimerRow {
    /// Generates the internal timer ID for retry timers.
    #[must_use]
    pub fn retry_timer_id(run_id: &str, task_key: &str, attempt: u32, fire_epoch: i64) -> String {
        format!("timer:retry:{}:{}:{}:{}", run_id, task_key, attempt, fire_epoch)
    }

    /// Generates the internal timer ID for heartbeat check timers.
    #[must_use]
    pub fn heartbeat_timer_id(run_id: &str, task_key: &str, attempt: u32, fire_epoch: i64) -> String {
        format!("timer:heartbeat:{}:{}:{}:{}", run_id, task_key, attempt, fire_epoch)
    }

    /// Generates the internal timer ID for cron timers.
    #[must_use]
    pub fn cron_timer_id(schedule_id: &str, fire_epoch: i64) -> String {
        format!("timer:cron:{}:{}", schedule_id, fire_epoch)
    }
}

/// Task row in the fold state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRow {
    /// Run identifier.
    pub run_id: String,
    /// Task name within run.
    pub task_key: String,
    /// Current state.
    pub state: TaskState,
    /// Current attempt number (1-indexed).
    pub attempt: u32,
    /// Current attempt ID (ULID) - concurrency guard.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRow {
    /// Run identifier.
    pub run_id: String,
    /// Plan identifier.
    pub plan_id: String,
    /// Current run state.
    pub state: RunState,
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

/// Fold state accumulator.
///
/// Holds the current state being built from events.
#[derive(Debug, Clone, Default)]
pub struct FoldState {
    /// Run rows keyed by run_id.
    pub runs: HashMap<String, RunRow>,

    /// Task rows keyed by (run_id, task_key).
    pub tasks: HashMap<(String, String), TaskRow>,

    /// Dependency edges keyed by (run_id, upstream_task_key, downstream_task_key).
    pub dep_satisfaction: HashMap<(String, String, String), DepSatisfactionRow>,

    /// Task dependencies (downstream -> list of upstreams).
    task_dependencies: HashMap<(String, String), Vec<String>>,

    /// Task dependents (upstream -> list of downstreams).
    task_dependents: HashMap<(String, String), Vec<String>>,
}

impl FoldState {
    /// Creates a new empty fold state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Processes an orchestration event and updates state.
    pub fn fold_event(&mut self, event: &OrchestrationEvent) {
        match &event.data {
            OrchestrationEventData::RunTriggered { run_id, plan_id, .. } => {
                self.fold_run_triggered(run_id, plan_id, &event.event_id, event.timestamp);
            }
            OrchestrationEventData::PlanCreated { run_id, tasks, .. } => {
                self.fold_plan_created(run_id, tasks, &event.event_id, event.timestamp);
            }
            OrchestrationEventData::TaskStarted {
                run_id, task_key, attempt, attempt_id, ..
            } => {
                self.fold_task_started(run_id, task_key, *attempt, attempt_id, &event.event_id);
            }
            OrchestrationEventData::TaskHeartbeat {
                run_id, task_key, attempt, attempt_id, heartbeat_at
            } => {
                // heartbeat_at is optional; use event timestamp as fallback
                let ts = heartbeat_at.unwrap_or(event.timestamp);
                self.fold_task_heartbeat(run_id, task_key, *attempt, attempt_id, ts, &event.event_id);
            }
            OrchestrationEventData::TaskFinished {
                run_id, task_key, attempt, attempt_id, outcome, ..
            } => {
                self.fold_task_finished(run_id, task_key, *attempt, attempt_id, *outcome, &event.event_id, event.timestamp);
            }
            // Other events handled by specific controllers
            _ => {}
        }
    }

    fn fold_run_triggered(
        &mut self,
        run_id: &str,
        plan_id: &str,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        self.runs.insert(run_id.to_string(), RunRow {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            state: RunState::Triggered,
            tasks_total: 0,
            tasks_completed: 0,
            tasks_succeeded: 0,
            tasks_failed: 0,
            tasks_skipped: 0,
            triggered_at: timestamp,
            completed_at: None,
            row_version: event_id.to_string(),
        });
    }

    fn fold_plan_created(
        &mut self,
        run_id: &str,
        task_defs: &[TaskDef],
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        // Update run with task count
        if let Some(run) = self.runs.get_mut(run_id) {
            run.tasks_total = task_defs.len() as u32;
            run.state = RunState::Running;
            run.row_version = event_id.to_string();
        }

        // Build dependency graph
        for task_def in task_defs {
            let task_key = (run_id.to_string(), task_def.key.clone());

            // Store dependencies
            self.task_dependencies.insert(
                task_key.clone(),
                task_def.depends_on.clone(),
            );

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
            let deps_total = task_def.depends_on.len() as u32;
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
                deps_total,
                deps_satisfied_count: 0,
                max_attempts: task_def.max_attempts,
                heartbeat_timeout_sec: task_def.heartbeat_timeout_sec,
                last_heartbeat_at: None,
                ready_at: if initial_state == TaskState::Ready { Some(timestamp) } else { None },
                asset_key: task_def.asset_key.clone(),
                partition_key: task_def.partition_key.clone(),
                row_version: event_id.to_string(),
            };

            self.tasks.insert(
                (run_id.to_string(), task_def.key.clone()),
                task_row,
            );

            // Create dependency satisfaction edges
            for upstream in &task_def.depends_on {
                let edge_key = (
                    run_id.to_string(),
                    upstream.clone(),
                    task_def.key.clone(),
                );
                self.dep_satisfaction.insert(edge_key, DepSatisfactionRow {
                    run_id: run_id.to_string(),
                    upstream_task_key: upstream.clone(),
                    downstream_task_key: task_def.key.clone(),
                    satisfied: false,
                    resolution: None,
                    satisfied_at: None,
                    satisfying_attempt: None,
                    row_version: event_id.to_string(),
                });
            }
        }
    }

    fn fold_task_started(
        &mut self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        event_id: &str,
    ) {
        let key = (run_id.to_string(), task_key.to_string());
        if let Some(task) = self.tasks.get_mut(&key) {
            // Update if this is a newer or equal attempt (handles retries)
            // A TaskStarted for attempt N should always override attempt < N
            if attempt >= task.attempt {
                task.state = TaskState::Running;
                task.attempt = attempt;
                task.attempt_id = Some(attempt_id.to_string());
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
            // Only update if attempt_id matches (concurrency guard per ADR-022)
            if task.attempt_id.as_deref() == Some(attempt_id) && task.attempt == attempt {
                task.last_heartbeat_at = Some(heartbeat_at);
                task.row_version = event_id.to_string();
            }
        }
    }

    fn fold_task_finished(
        &mut self,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        outcome: TaskOutcome,
        event_id: &str,
        timestamp: DateTime<Utc>,
    ) {
        let key = (run_id.to_string(), task_key.to_string());

        // Check if this is a stale attempt event (INV-8)
        let is_current_attempt = self.tasks.get(&key)
            .map(|t| t.attempt_id.as_deref() == Some(attempt_id) && t.attempt == attempt)
            .unwrap_or(false);

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

            // Update run counters
            if new_state.is_terminal() {
                if let Some(run) = self.runs.get_mut(run_id) {
                    run.tasks_completed += 1;
                    match new_state {
                        TaskState::Succeeded => run.tasks_succeeded += 1,
                        TaskState::Failed => run.tasks_failed += 1,
                        TaskState::Skipped => run.tasks_skipped += 1,
                        _ => {}
                    }
                    run.row_version = event_id.to_string();

                    // Check if run is complete
                    if run.tasks_completed == run.tasks_total {
                        run.state = if run.tasks_failed > 0 {
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
                self.satisfy_downstream_edges(run_id, task_key, DepResolution::Success, attempt, event_id, timestamp);
            } else if outcome == TaskOutcome::Failed && new_state == TaskState::Failed {
                // Terminal failure - propagate to downstream
                self.propagate_failure(run_id, task_key, DepResolution::Failed, event_id, timestamp);
            } else if outcome == TaskOutcome::Skipped {
                self.propagate_failure(run_id, task_key, DepResolution::Skipped, event_id, timestamp);
            } else if outcome == TaskOutcome::Cancelled {
                self.propagate_failure(run_id, task_key, DepResolution::Cancelled, event_id, timestamp);
            }
        }
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
        let dependents = self.task_dependents.get(&dependents_key).cloned().unwrap_or_default();

        for downstream_key in dependents {
            let edge_key = (run_id.to_string(), upstream_key.to_string(), downstream_key.clone());

            // Check if edge was already satisfied (duplicate-safe per ADR-022)
            let was_satisfied = self.dep_satisfaction
                .get(&edge_key)
                .map(|e| e.satisfied)
                .unwrap_or(false);

            // Upsert edge (idempotent)
            self.dep_satisfaction.insert(edge_key.clone(), DepSatisfactionRow {
                run_id: run_id.to_string(),
                upstream_task_key: upstream_key.to_string(),
                downstream_task_key: downstream_key.clone(),
                satisfied: true,
                resolution: Some(resolution),
                satisfied_at: Some(timestamp),
                satisfying_attempt: Some(attempt),
                row_version: event_id.to_string(),
            });

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
        // The first task uses the initial resolution (FAILED/CANCELLED)
        // Transitive skips use SKIPPED resolution
        let mut to_skip: VecDeque<(String, DepResolution)> = VecDeque::new();
        to_skip.push_back((failed_task_key.to_string(), initial_resolution));

        while let Some((upstream_key, edge_resolution)) = to_skip.pop_front() {
            let dependents_key = (run_id.to_string(), upstream_key.clone());
            let dependents = self.task_dependents.get(&dependents_key).cloned().unwrap_or_default();

            for downstream_key in dependents {
                let edge_key = (run_id.to_string(), upstream_key.clone(), downstream_key.clone());

                // Mark edge as resolved (not satisfied)
                self.dep_satisfaction.insert(edge_key, DepSatisfactionRow {
                    run_id: run_id.to_string(),
                    upstream_task_key: upstream_key.clone(),
                    downstream_task_key: downstream_key.clone(),
                    satisfied: false,
                    resolution: Some(edge_resolution),
                    satisfied_at: Some(timestamp),
                    satisfying_attempt: None,
                    row_version: event_id.to_string(),
                });

                // Skip downstream if not already terminal
                let task_key = (run_id.to_string(), downstream_key.clone());
                if let Some(downstream_task) = self.tasks.get_mut(&task_key) {
                    if !downstream_task.state.is_terminal() {
                        downstream_task.state = TaskState::Skipped;
                        downstream_task.row_version = event_id.to_string();
                        // Transitive skips always use SKIPPED resolution
                        to_skip.push_back((downstream_key, DepResolution::Skipped));

                        // Update run counters
                        if let Some(run) = self.runs.get_mut(run_id) {
                            run.tasks_completed += 1;
                            run.tasks_skipped += 1;
                            run.row_version = event_id.to_string();

                            // Check if run is complete
                            if run.tasks_completed == run.tasks_total {
                                run.state = RunState::Failed;
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
/// Uses row_version (ULID) as primary ordering key, with state_rank as tiebreaker.
/// This ensures deterministic merge regardless of file order.
#[must_use]
pub fn merge_task_rows(rows: Vec<TaskRow>) -> Option<TaskRow> {
    rows.into_iter()
        .reduce(|best, row| {
            // Compare by row_version first (ULID lexicographic ordering)
            match row.row_version.cmp(&best.row_version) {
                std::cmp::Ordering::Greater => row,
                std::cmp::Ordering::Less => best,
                std::cmp::Ordering::Equal => {
                    // Same row_version: use state_rank as tiebreaker
                    // Higher rank = more terminal = preferred
                    if row.state.rank() > best.state.rank() {
                        row
                    } else {
                        best
                    }
                }
            }
        })
}

/// Merges run rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_run_rows(rows: Vec<RunRow>) -> Option<RunRow> {
    rows.into_iter()
        .max_by(|a, b| a.row_version.cmp(&b.row_version))
}

/// Merges dependency satisfaction rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_dep_satisfaction_rows(rows: Vec<DepSatisfactionRow>) -> Option<DepSatisfactionRow> {
    rows.into_iter()
        .max_by(|a, b| a.row_version.cmp(&b.row_version))
}

/// Merges dispatch outbox rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_dispatch_outbox_rows(rows: Vec<DispatchOutboxRow>) -> Option<DispatchOutboxRow> {
    rows.into_iter()
        .max_by(|a, b| a.row_version.cmp(&b.row_version))
}

/// Merges timer rows from base snapshot and L0 deltas.
#[must_use]
pub fn merge_timer_rows(rows: Vec<TimerRow>) -> Option<TimerRow> {
    rows.into_iter()
        .max_by(|a, b| a.row_version.cmp(&b.row_version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::events::{OrchestrationEventData, TriggerInfo};
    use ulid::Ulid;

    fn make_event(data: OrchestrationEventData) -> OrchestrationEvent {
        OrchestrationEvent::new("tenant-test", "workspace-test", data)
    }

    fn run_triggered_event(run_id: &str) -> OrchestrationEvent {
        make_event(OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: "plan-01".to_string(),
            trigger: TriggerInfo::Manual { user_id: "user@test.com".to_string() },
            root_assets: vec![],
            run_key: None,
        })
    }

    fn plan_created_event(run_id: &str, tasks: Vec<TaskDef>) -> OrchestrationEvent {
        make_event(OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: "plan-01".to_string(),
            tasks,
        })
    }

    fn task_finished_event(run_id: &str, task_key: &str, attempt: u32, attempt_id: &str, outcome: TaskOutcome) -> OrchestrationEvent {
        make_event(OrchestrationEventData::TaskFinished {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            outcome,
            materialization_id: None,
            error_message: None,
        })
    }

    fn task_started_event(run_id: &str, task_key: &str, attempt: u32, attempt_id: &str) -> OrchestrationEvent {
        make_event(OrchestrationEventData::TaskStarted {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-01".to_string(),
        })
    }

    #[test]
    fn test_fold_plan_created_initializes_tasks() {
        let mut state = FoldState::new();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event("run1", vec![
            TaskDef { key: "extract".into(), depends_on: vec![], asset_key: None, partition_key: None, max_attempts: 3, heartbeat_timeout_sec: 300 },
            TaskDef { key: "transform".into(), depends_on: vec!["extract".into()], asset_key: None, partition_key: None, max_attempts: 3, heartbeat_timeout_sec: 300 },
            TaskDef { key: "load".into(), depends_on: vec!["transform".into()], asset_key: None, partition_key: None, max_attempts: 3, heartbeat_timeout_sec: 300 },
        ]));

        // Extract has no deps, should be READY
        let extract = state.tasks.get(&("run1".into(), "extract".into())).unwrap();
        assert_eq!(extract.state, TaskState::Ready);
        assert_eq!(extract.deps_total, 0);

        // Transform depends on extract, should be BLOCKED
        let transform = state.tasks.get(&("run1".into(), "transform".into())).unwrap();
        assert_eq!(transform.state, TaskState::Blocked);
        assert_eq!(transform.deps_total, 1);

        // Load depends on transform, should be BLOCKED
        let load = state.tasks.get(&("run1".into(), "load".into())).unwrap();
        assert_eq!(load.state, TaskState::Blocked);
        assert_eq!(load.deps_total, 1);

        // Dep satisfaction edges should be created
        assert!(state.dep_satisfaction.contains_key(&("run1".into(), "extract".into(), "transform".into())));
        assert!(state.dep_satisfaction.contains_key(&("run1".into(), "transform".into(), "load".into())));
    }

    #[test]
    fn test_duplicate_task_finished_does_not_double_increment() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event("run1", vec![
            TaskDef { key: "A".into(), depends_on: vec![], asset_key: None, partition_key: None, max_attempts: 3, heartbeat_timeout_sec: 300 },
            TaskDef { key: "B".into(), depends_on: vec!["A".into()], asset_key: None, partition_key: None, max_attempts: 3, heartbeat_timeout_sec: 300 },
        ]));

        // Start task A
        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));

        // First TaskFinished(A, succeeded)
        state.fold_event(&task_finished_event("run1", "A", 1, &attempt_id, TaskOutcome::Succeeded));

        let b_after_first = state.tasks.get(&("run1".into(), "B".into())).unwrap();
        assert_eq!(b_after_first.deps_satisfied_count, 1);
        assert_eq!(b_after_first.state, TaskState::Ready);

        // Duplicate TaskFinished(A, succeeded) - should be no-op
        state.fold_event(&task_finished_event("run1", "A", 1, &attempt_id, TaskOutcome::Succeeded));

        // Should still be 1, not 2
        let b_after_dup = state.tasks.get(&("run1".into(), "B".into())).unwrap();
        assert_eq!(b_after_dup.deps_satisfied_count, 1);
    }

    #[test]
    fn test_stale_attempt_event_is_rejected() {
        let mut state = FoldState::new();
        let attempt_1_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event("run1", vec![
            TaskDef { key: "extract".into(), depends_on: vec![], asset_key: None, partition_key: None, max_attempts: 3, heartbeat_timeout_sec: 300 },
        ]));

        // Start attempt 1
        state.fold_event(&task_started_event("run1", "extract", 1, &attempt_1_id));
        assert_eq!(state.tasks.get(&("run1".into(), "extract".into())).unwrap().attempt, 1);

        // Start attempt 2 (retry)
        let attempt_2_id = Ulid::new().to_string();
        state.fold_event(&task_started_event("run1", "extract", 2, &attempt_2_id));
        assert_eq!(state.tasks.get(&("run1".into(), "extract".into())).unwrap().attempt, 2);
        assert_eq!(state.tasks.get(&("run1".into(), "extract".into())).unwrap().state, TaskState::Running);

        // Late TaskFinished for attempt 1 arrives (out-of-order) - should be rejected
        state.fold_event(&task_finished_event("run1", "extract", 1, &attempt_1_id, TaskOutcome::Failed));

        // State should NOT regress - attempt 2 is still running
        let task = state.tasks.get(&("run1".into(), "extract".into())).unwrap();
        assert_eq!(task.attempt, 2);
        assert_eq!(task.attempt_id.as_deref(), Some(attempt_2_id.as_str()));
        assert_eq!(task.state, TaskState::Running);
    }

    #[test]
    fn test_upstream_failure_skips_downstream() {
        let mut state = FoldState::new();
        let attempt_id = Ulid::new().to_string();

        state.fold_event(&run_triggered_event("run1"));
        state.fold_event(&plan_created_event("run1", vec![
            TaskDef { key: "A".into(), depends_on: vec![], asset_key: None, partition_key: None, max_attempts: 1, heartbeat_timeout_sec: 300 },
            TaskDef { key: "B".into(), depends_on: vec!["A".into()], asset_key: None, partition_key: None, max_attempts: 1, heartbeat_timeout_sec: 300 },
            TaskDef { key: "C".into(), depends_on: vec!["B".into()], asset_key: None, partition_key: None, max_attempts: 1, heartbeat_timeout_sec: 300 },
        ]));

        // Start A
        state.fold_event(&task_started_event("run1", "A", 1, &attempt_id));

        // A fails terminally (max_attempts = 1)
        state.fold_event(&task_finished_event("run1", "A", 1, &attempt_id, TaskOutcome::Failed));

        // A should be FAILED
        assert_eq!(state.tasks.get(&("run1".into(), "A".into())).unwrap().state, TaskState::Failed);

        // B should be SKIPPED (direct downstream)
        assert_eq!(state.tasks.get(&("run1".into(), "B".into())).unwrap().state, TaskState::Skipped);

        // C should also be SKIPPED (transitive)
        assert_eq!(state.tasks.get(&("run1".into(), "C".into())).unwrap().state, TaskState::Skipped);

        // dep_satisfaction edges should have correct resolution
        let ab_edge = state.dep_satisfaction.get(&("run1".into(), "A".into(), "B".into())).unwrap();
        assert_eq!(ab_edge.resolution, Some(DepResolution::Failed));
        assert!(!ab_edge.satisfied);

        let bc_edge = state.dep_satisfaction.get(&("run1".into(), "B".into(), "C".into())).unwrap();
        assert_eq!(bc_edge.resolution, Some(DepResolution::Skipped));
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

    // ========================================================================
    // Timer Tests
    // ========================================================================

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
    fn test_merge_dispatch_outbox_rows() {
        let row1 = DispatchOutboxRow {
            run_id: "run1".into(),
            task_key: "extract".into(),
            attempt: 1,
            dispatch_id: "dispatch:run1:extract:1".into(),
            cloud_task_id: None,
            status: DispatchStatus::Pending,
            attempt_id: "01HQ123ATT".into(),
            worker_queue: "default".into(),
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
}
