//! Execution run tracking.
//!
//! A run represents a single execution of a plan, capturing:
//!
//! - **Inputs**: What data was read
//! - **Outputs**: What data was produced
//! - **Timing**: When each task started and completed
//! - **State**: Current status and any errors
//!
//! ## Event Replay
//!
//! Runs are designed to be rebuilt from the Tier 2 ledger by replaying execution events:
//!
//! 1. Load all events for a run stream (e.g., `run:{run_id}`).
//! 2. Sort by `sequence` (the total order within the run). If `sequence` is absent, fall back to
//!    a stable tie-breaker such as `(time, id)`.
//! 3. Deduplicate by `idempotency_key` for durable transitions (run/task lifecycle events).
//! 4. Apply events to an in-memory `Run` + per-task `TaskExecution` using the same state machine
//!    validation (`can_transition_to`) to guarantee consistency.
//!
//! High-frequency events such as heartbeats and metrics are intentionally not deduplicated via
//! deterministic idempotency keys; consumers should treat them as "last write wins" observations.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{RunId, TaskId};

use crate::error::{Error, Result};
use crate::plan::Plan;
use crate::task::{TaskExecution, TaskState};
use crate::task_key::TaskKey;

/// Run state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    /// Created, waiting to start.
    Pending,
    /// Actively executing tasks.
    Running,
    /// All tasks completed successfully.
    Succeeded,
    /// One or more tasks failed.
    Failed,
    /// Being cancelled (waiting for in-flight tasks).
    Cancelling,
    /// Cancelled by user or system.
    Cancelled,
    /// Exceeded maximum duration.
    TimedOut,
}

impl RunState {
    /// Returns true if this is a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    /// Returns true if the transition from self to target is valid.
    #[must_use]
    pub fn can_transition_to(&self, target: Self) -> bool {
        match self {
            Self::Pending => matches!(target, Self::Running | Self::Cancelling | Self::Cancelled),
            Self::Running => matches!(
                target,
                Self::Succeeded | Self::Failed | Self::Cancelling | Self::TimedOut
            ),
            Self::Cancelling => matches!(target, Self::Cancelled),
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut => false,
        }
    }
}

impl Default for RunState {
    fn default() -> Self {
        Self::Pending
    }
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Succeeded => write!(f, "SUCCEEDED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelling => write!(f, "CANCELLING"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::TimedOut => write!(f, "TIMED_OUT"),
        }
    }
}

/// Trigger type enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TriggerType {
    /// User-initiated.
    Manual,
    /// Cron/schedule-based.
    Scheduled,
    /// Event-driven (e.g., file arrival).
    Sensor,
    /// Historical data backfill.
    Backfill,
}

impl Default for TriggerType {
    fn default() -> Self {
        Self::Manual
    }
}

impl std::fmt::Display for TriggerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manual => write!(f, "Manual"),
            Self::Scheduled => write!(f, "Scheduled"),
            Self::Sensor => write!(f, "Sensor"),
            Self::Backfill => write!(f, "Backfill"),
        }
    }
}

/// How the run was triggered.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunTrigger {
    /// Trigger type.
    #[serde(rename = "type")]
    pub trigger_type: TriggerType,
    /// User who triggered (if manual).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<String>,
    /// Trigger timestamp.
    pub triggered_at: DateTime<Utc>,
    /// Associated schedule name (if scheduled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_name: Option<String>,
}

impl RunTrigger {
    /// Creates a manual trigger.
    #[must_use]
    pub fn manual(user: impl Into<String>) -> Self {
        Self {
            trigger_type: TriggerType::Manual,
            triggered_by: Some(user.into()),
            triggered_at: Utc::now(),
            schedule_name: None,
        }
    }

    /// Creates a scheduled trigger.
    #[must_use]
    pub fn scheduled(schedule_name: impl Into<String>) -> Self {
        Self {
            trigger_type: TriggerType::Scheduled,
            triggered_by: None,
            triggered_at: Utc::now(),
            schedule_name: Some(schedule_name.into()),
        }
    }

    /// Creates a sensor trigger.
    #[must_use]
    pub fn sensor() -> Self {
        Self {
            trigger_type: TriggerType::Sensor,
            triggered_by: None,
            triggered_at: Utc::now(),
            schedule_name: None,
        }
    }

    /// Creates a backfill trigger.
    #[must_use]
    pub fn backfill(user: impl Into<String>) -> Self {
        Self {
            trigger_type: TriggerType::Backfill,
            triggered_by: Some(user.into()),
            triggered_at: Utc::now(),
            schedule_name: None,
        }
    }
}

/// A pipeline execution run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    /// Unique run identifier.
    pub id: RunId,
    /// Plan being executed.
    pub plan_id: String,
    /// Tenant scope.
    pub tenant_id: String,
    /// Workspace scope.
    pub workspace_id: String,
    /// Current state of the run.
    pub state: RunState,
    /// When the run was created.
    pub created_at: DateTime<Utc>,
    /// When the run started executing (if started).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the run completed (if completed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Task execution states.
    pub task_executions: Vec<TaskExecution>,
    /// Run-level labels.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Trigger information.
    pub trigger: RunTrigger,

    /// Monotonic sequence for events emitted within this run.
    ///
    /// This provides deterministic ordering for Tier 2 replay when used as the
    /// `sequence` field in execution events.
    #[serde(default)]
    pub run_sequence: u64,
}

impl Run {
    /// Creates a new run from a plan.
    #[must_use]
    pub fn from_plan(plan: &Plan, trigger: RunTrigger) -> Self {
        let task_executions = plan
            .tasks
            .iter()
            .map(|task| {
                let task_key = TaskKey {
                    asset_key: task.asset_key.clone(),
                    partition_key: task.partition_key.clone(),
                    operation: task.operation,
                };
                let mut exec =
                    TaskExecution::new(task.task_id).with_metadata(task_key, task.priority);
                exec.state = TaskState::Pending;
                exec
            })
            .collect();

        Self {
            id: RunId::generate(),
            plan_id: plan.plan_id.clone(),
            tenant_id: plan.tenant_id.clone(),
            workspace_id: plan.workspace_id.clone(),
            state: RunState::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            task_executions,
            labels: HashMap::new(),
            trigger,
            run_sequence: 0,
        }
    }

    /// Returns the next monotonic per-run event sequence number.
    ///
    /// Sequence numbers are 1-indexed.
    #[must_use]
    pub fn next_sequence(&mut self) -> u64 {
        self.run_sequence = self.run_sequence.saturating_add(1);
        self.run_sequence
    }

    /// Returns true if the run is in a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Transitions to a new state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    #[tracing::instrument(skip(self), fields(run_id = %self.id, from = %self.state, to = %target))]
    pub fn transition_to(&mut self, target: RunState) -> Result<()> {
        if !self.state.can_transition_to(target) {
            return Err(Error::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
                reason: "invalid run state transition".into(),
            });
        }

        let now = Utc::now();

        match target {
            RunState::Running => {
                self.started_at = Some(now);
            }
            RunState::Succeeded | RunState::Failed | RunState::Cancelled | RunState::TimedOut => {
                self.completed_at = Some(now);
            }
            _ => {}
        }

        self.state = target;
        Ok(())
    }

    /// Returns the task execution for a given task ID.
    #[must_use]
    pub fn get_task(&self, task_id: &TaskId) -> Option<&TaskExecution> {
        self.task_executions.iter().find(|t| &t.task_id == task_id)
    }

    /// Returns mutable task execution for a given task ID.
    pub fn get_task_mut(&mut self, task_id: &TaskId) -> Option<&mut TaskExecution> {
        self.task_executions
            .iter_mut()
            .find(|t| &t.task_id == task_id)
    }

    /// Returns the count of tasks in pending state.
    #[must_use]
    pub fn tasks_pending(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Pending)
            .count()
    }

    /// Returns the count of tasks in queued state.
    #[must_use]
    pub fn tasks_queued(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Queued)
            .count()
    }

    /// Returns the count of tasks in running state.
    #[must_use]
    pub fn tasks_running(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Running)
            .count()
    }

    /// Returns the count of succeeded tasks.
    #[must_use]
    pub fn tasks_succeeded(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Succeeded)
            .count()
    }

    /// Returns the count of failed tasks.
    #[must_use]
    pub fn tasks_failed(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Failed)
            .count()
    }

    /// Returns the count of skipped tasks.
    #[must_use]
    pub fn tasks_skipped(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Skipped)
            .count()
    }

    /// Returns the count of cancelled tasks.
    #[must_use]
    pub fn tasks_cancelled(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Cancelled)
            .count()
    }

    /// Returns tasks ready to execute (all dependencies satisfied).
    #[must_use]
    pub fn ready_tasks(&self, plan: &Plan) -> Vec<TaskId> {
        self.task_executions
            .iter()
            .filter(|exec| matches!(exec.state, TaskState::Pending | TaskState::Ready))
            .filter(|exec| {
                plan.get_task(&exec.task_id).is_some_and(|spec| {
                    spec.upstream_task_ids.iter().all(|dep_id| {
                        self.get_task(dep_id)
                            .is_some_and(|dep| dep.state == TaskState::Succeeded)
                    })
                })
            })
            .map(|exec| exec.task_id)
            .collect()
    }

    /// Returns true if all tasks are in terminal states.
    #[must_use]
    pub fn all_tasks_terminal(&self) -> bool {
        self.task_executions.iter().all(TaskExecution::is_terminal)
    }

    /// Computes the final run state based on task states.
    ///
    /// Call this after all tasks are terminal to determine the run outcome.
    #[must_use]
    pub fn compute_final_state(&self) -> RunState {
        if !self.all_tasks_terminal() {
            return self.state;
        }

        if self.tasks_cancelled() > 0 {
            return RunState::Cancelled;
        }

        if self.tasks_failed() > 0 {
            return RunState::Failed;
        }

        RunState::Succeeded
    }

    /// Adds a label to the run.
    pub fn add_label(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.labels.insert(key.into(), value.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use crate::task::TaskState;
    use crate::task_key::TaskOperation;
    use arco_core::AssetId;

    #[test]
    fn run_initializes_from_plan() -> Result<()> {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        let run = Run::from_plan(&plan, RunTrigger::manual("user@example.com"));

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.tenant_id, "tenant");
        assert_eq!(run.task_executions.len(), 1);
        assert_eq!(
            run.get_task(&task_id)
                .ok_or(Error::TaskNotFound { task_id })?
                .state,
            TaskState::Pending
        );

        Ok(())
    }

    #[test]
    fn run_state_transitions() {
        let state = RunState::Pending;
        assert!(state.can_transition_to(RunState::Running));
        assert!(!state.can_transition_to(RunState::Succeeded));
    }

    #[test]
    fn run_tracks_task_progress() -> Result<()> {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        assert_eq!(run.tasks_pending(), 1);
        assert_eq!(run.tasks_succeeded(), 0);

        let exec = run
            .get_task_mut(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;
        exec.transition_to(TaskState::Succeeded)?;

        assert_eq!(run.tasks_pending(), 0);
        assert_eq!(run.tasks_succeeded(), 1);

        Ok(())
    }

    #[test]
    fn run_ready_tasks_respects_dependencies() -> Result<()> {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        // Only task_a should be ready initially
        let ready = run.ready_tasks(&plan);
        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&task_a));

        // Mark task_a as succeeded
        let exec = run
            .get_task_mut(&task_a)
            .ok_or(Error::TaskNotFound { task_id: task_a })?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;
        exec.transition_to(TaskState::Succeeded)?;

        // Now task_b should be ready
        let ready = run.ready_tasks(&plan);
        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&task_b));

        Ok(())
    }

    #[test]
    fn run_compute_final_state() -> Result<()> {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        let exec = run
            .get_task_mut(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?;
        exec.transition_to(TaskState::Ready)?;
        exec.transition_to(TaskState::Queued)?;
        exec.transition_to(TaskState::Dispatched)?;
        exec.transition_to(TaskState::Running)?;
        exec.transition_to(TaskState::Succeeded)?;

        assert_eq!(run.compute_final_state(), RunState::Succeeded);

        Ok(())
    }
}
