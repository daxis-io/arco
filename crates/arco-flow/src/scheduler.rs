//! Dependency-aware task scheduling.
//!
//! The scheduler executes plans with:
//!
//! - **Parallelism**: Independent tasks run concurrently
//! - **Dependency ordering**: Tasks wait for their dependencies
//! - **Fault tolerance**: Failed tasks skip downstream, continue independent branches

use std::collections::HashSet;

use arco_core::TaskId;

use crate::error::{Error, Result};
use crate::events::EventBuilder;
use crate::outbox::EventSink;
use crate::plan::{Plan, TaskSpec};
use crate::run::{Run, RunState, RunTrigger};
use crate::runner::TaskResult;
use crate::task::TaskState;

/// Scheduler configuration.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum concurrent tasks.
    pub max_parallelism: usize,
    /// Whether to continue on task failure (skip downstream only).
    pub continue_on_failure: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_parallelism: 10,
            continue_on_failure: true,
        }
    }
}

/// Scheduler for executing plans.
///
/// The scheduler is responsible for:
/// - Creating runs from plans
/// - Determining which tasks are ready to execute
/// - Processing task completions and cascading effects
/// - Computing final run state
#[derive(Debug)]
pub struct Scheduler {
    plan: Plan,
    config: SchedulerConfig,
}

impl Scheduler {
    /// Creates a new scheduler for the given plan.
    #[must_use]
    pub fn new(plan: Plan) -> Self {
        Self {
            plan,
            config: SchedulerConfig::default(),
        }
    }

    /// Creates a scheduler with custom configuration.
    #[must_use]
    pub const fn with_config(plan: Plan, config: SchedulerConfig) -> Self {
        Self { plan, config }
    }

    /// Returns the plan being scheduled.
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Returns the scheduler configuration.
    #[must_use]
    pub const fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Creates a new run from the plan.
    #[must_use]
    pub fn create_run(&self, trigger: RunTrigger, outbox: &mut impl EventSink) -> Run {
        let mut run = Run::from_plan(&self.plan, trigger);

        let event = EventBuilder::run_created(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            run.plan_id.clone(),
            run.trigger.trigger_type.to_string(),
            run.trigger.triggered_by.clone(),
        );
        emit_sequenced(&mut run, outbox, event);

        run
    }

    /// Returns tasks that are ready to execute.
    ///
    /// A task is ready if:
    /// - It is in Pending state
    /// - All its upstream dependencies have Succeeded
    /// - We haven't exceeded `max_parallelism` (considering queued/running tasks)
    #[must_use]
    pub fn get_ready_tasks(&self, run: &Run) -> Vec<&TaskSpec> {
        let running_count = run.tasks_running() + run.tasks_queued();
        let available_slots = self.config.max_parallelism.saturating_sub(running_count);

        if available_slots == 0 {
            return Vec::new();
        }

        let ready_ids = run.ready_tasks(&self.plan);

        // Sort by priority (lower = higher priority), then by task order in plan
        let mut ready_tasks: Vec<_> = ready_ids
            .iter()
            .filter_map(|id| self.plan.get_task(id))
            .collect();

        ready_tasks.sort_by(|a, b| {
            a.priority.cmp(&b.priority).then_with(|| {
                let pos_a = self.plan.tasks.iter().position(|t| t.task_id == a.task_id);
                let pos_b = self.plan.tasks.iter().position(|t| t.task_id == b.task_id);
                pos_a.cmp(&pos_b)
            })
        });

        ready_tasks.into_iter().take(available_slots).collect()
    }

    /// Processes task completion and cascades effects to downstream tasks.
    ///
    /// If a task fails and `continue_on_failure` is true, all downstream
    /// tasks that depend on it (directly or transitively) will be skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if `task_id` is not present in the run.
    pub fn process_task_completion(
        &self,
        run: &mut Run,
        task_id: &TaskId,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let execution = run
            .get_task(task_id)
            .ok_or(Error::TaskNotFound { task_id: *task_id })?;

        if matches!(execution.state, TaskState::Failed | TaskState::Cancelled)
            && self.config.continue_on_failure
        {
            let skipped = self.skip_downstream_tasks(run, task_id);
            for skipped_task_id in skipped {
                let attempt = run.get_task(&skipped_task_id).map_or(1, |t| t.attempt);
                emit_sequenced(
                    run,
                    outbox,
                    EventBuilder::task_completed(
                        &run.tenant_id,
                        &run.workspace_id,
                        run.id,
                        skipped_task_id,
                        TaskState::Skipped,
                        attempt,
                    ),
                );
            }
        }

        Ok(())
    }

    /// Skips all tasks that depend on the given task (directly or transitively).
    fn skip_downstream_tasks(&self, run: &mut Run, failed_task_id: &TaskId) -> Vec<TaskId> {
        let mut visited: HashSet<TaskId> = HashSet::new();
        let mut frontier: Vec<TaskId> = vec![*failed_task_id];
        let mut skipped_in_order = Vec::new();

        // Deterministic traversal: scan plan.tasks in plan order for dependents.
        while let Some(current) = frontier.pop() {
            for task in &self.plan.tasks {
                if !task.upstream_task_ids.contains(&current) {
                    continue;
                }
                if !visited.insert(task.task_id) {
                    continue;
                }

                if let Some(exec) = run.get_task_mut(&task.task_id) {
                    if exec.state == TaskState::Pending {
                        let _ = exec.skip();
                        skipped_in_order.push(task.task_id);
                    }
                }

                frontier.push(task.task_id);
            }
        }

        skipped_in_order
    }

    /// Queues a task for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is not ready to queue.
    pub fn queue_task(
        &self,
        run: &mut Run,
        task_id: &TaskId,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let is_ready = run.ready_tasks(&self.plan).contains(task_id);

        let attempt = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;

            // Enforce dependency readiness before queueing.
            if exec.state == TaskState::Pending && !is_ready {
                return Err(Error::InvalidStateTransition {
                    from: exec.state.to_string(),
                    to: TaskState::Queued.to_string(),
                    reason: "dependencies not satisfied".into(),
                });
            }

            if exec.state == TaskState::Pending {
                exec.transition_to(TaskState::Ready)?;
            }

            exec.transition_to(TaskState::Queued)?;
            exec.attempt
        };

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_queued(&run.tenant_id, &run.workspace_id, run.id, *task_id, attempt),
        );

        Ok(())
    }

    /// Starts task execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is not in queued state.
    pub fn start_task(
        &self,
        run: &mut Run,
        task_id: &TaskId,
        worker_id: &str,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let attempt = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;

            exec.transition_to(TaskState::Dispatched)?;
            exec.worker_id = Some(worker_id.to_string());
            exec.attempt
        };

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_dispatched(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                attempt,
                worker_id,
            ),
        );

        {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;
            exec.transition_to(TaskState::Running)?;
        }

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_started(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                attempt,
                worker_id,
            ),
        );

        Ok(())
    }

    /// Checks if the run should transition to a terminal state.
    ///
    /// Returns the new state if a transition should occur.
    #[must_use]
    pub fn check_run_completion(&self, run: &Run) -> Option<RunState> {
        if !run.all_tasks_terminal() {
            return None;
        }

        Some(run.compute_final_state())
    }

    /// Starts the run (transitions from Pending to Running).
    ///
    /// # Errors
    ///
    /// Returns an error if the run is not in Pending state.
    pub fn start_run(&self, run: &mut Run, outbox: &mut impl EventSink) -> Result<()> {
        run.transition_to(RunState::Running)?;

        emit_sequenced(
            run,
            outbox,
            EventBuilder::run_started(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                run.plan_id.clone(),
            ),
        );

        Ok(())
    }

    /// Completes the run with the final state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn complete_run(
        &self,
        run: &mut Run,
        final_state: RunState,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        run.transition_to(final_state)?;

        emit_sequenced(
            run,
            outbox,
            EventBuilder::run_completed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                run.state,
                run.tasks_succeeded(),
                run.tasks_failed(),
                run.tasks_skipped(),
                run.tasks_cancelled(),
            ),
        );

        Ok(())
    }

    /// Cancels the run and all pending tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the run cannot be cancelled.
    pub fn cancel_run(&self, run: &mut Run, outbox: &mut impl EventSink) -> Result<()> {
        if run.state != RunState::Cancelling {
            run.transition_to(RunState::Cancelling)?;
        }

        let mut cancelled = Vec::new();
        for exec in &mut run.task_executions {
            if matches!(
                exec.state,
                TaskState::Pending | TaskState::Ready | TaskState::Queued
            ) {
                cancelled.push((exec.task_id, exec.attempt));
                let _ = exec.cancel();
            }
        }

        for (task_id, attempt) in cancelled {
            emit_sequenced(
                run,
                outbox,
                EventBuilder::task_completed(
                    &run.tenant_id,
                    &run.workspace_id,
                    run.id,
                    task_id,
                    TaskState::Cancelled,
                    attempt,
                ),
            );
        }

        if run.tasks_running() == 0 {
            run.transition_to(RunState::Cancelled)?;

            emit_sequenced(
                run,
                outbox,
                EventBuilder::run_completed(
                    &run.tenant_id,
                    &run.workspace_id,
                    run.id,
                    run.state,
                    run.tasks_succeeded(),
                    run.tasks_failed(),
                    run.tasks_skipped(),
                    run.tasks_cancelled(),
                ),
            );
        }

        Ok(())
    }

    /// Records the result of a task execution and emits the corresponding events.
    ///
    /// This also applies downstream skipping (if enabled) on failure/cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if the task does not exist or the transition is invalid.
    pub fn record_task_result(
        &self,
        run: &mut Run,
        task_id: &TaskId,
        result: TaskResult,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        match result {
            TaskResult::Succeeded(output) => {
                Self::record_task_success(run, task_id, output, outbox)?;
            }
            TaskResult::Failed(error) => {
                Self::record_task_failure(run, task_id, error, outbox)?;
            }
            TaskResult::Cancelled => {
                Self::record_task_cancelled(run, task_id, outbox)?;
            }
        };

        self.process_task_completion(run, task_id, outbox)
    }

    fn record_task_success(
        run: &mut Run,
        task_id: &TaskId,
        output: crate::task::TaskOutput,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let (attempt, materialization_id, row_count, byte_size) = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;
            let attempt = exec.attempt;
            let materialization_id = output.materialization_id;
            let row_count = output.row_count;
            let byte_size = output.byte_size;
            exec.succeed(output)?;
            (attempt, materialization_id, row_count, byte_size)
        };

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_output(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                materialization_id,
                row_count,
                byte_size,
            ),
        );

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_completed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                TaskState::Succeeded,
                attempt,
            ),
        );

        Ok(())
    }

    fn record_task_failure(
        run: &mut Run,
        task_id: &TaskId,
        error: crate::task::TaskError,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let attempt = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;
            let attempt = exec.attempt;
            exec.fail(error.clone())?;
            attempt
        };

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_failed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                error,
                attempt,
            ),
        );

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_completed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                TaskState::Failed,
                attempt,
            ),
        );

        Ok(())
    }

    fn record_task_cancelled(
        run: &mut Run,
        task_id: &TaskId,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let attempt = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;
            let attempt = exec.attempt;
            exec.cancel()?;
            attempt
        };

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_completed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                TaskState::Cancelled,
                attempt,
            ),
        );

        Ok(())
    }

    /// Completes the run if all tasks are terminal.
    ///
    /// Returns `Ok(Some(state))` if the run transitioned to a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if the completion transition is invalid.
    pub fn maybe_complete_run(
        &self,
        run: &mut Run,
        outbox: &mut impl EventSink,
    ) -> Result<Option<RunState>> {
        let Some(final_state) = self.check_run_completion(run) else {
            return Ok(None);
        };
        self.complete_run(run, final_state, outbox)?;
        Ok(Some(final_state))
    }
}

fn emit_sequenced(run: &mut Run, outbox: &mut impl EventSink, event: crate::events::EventEnvelope) {
    let seq = run.next_sequence();
    outbox.push(event.with_sequence(seq));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::InMemoryOutbox;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use arco_core::AssetId;

    #[test]
    fn scheduler_creates_run_from_plan() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.task_executions.len(), 1);
        assert_eq!(outbox.events().len(), 1);
        assert_eq!(outbox.events()[0].sequence, Some(1));
    }

    #[test]
    fn scheduler_returns_ready_tasks() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
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
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, task_a);
    }

    #[test]
    fn scheduler_skips_downstream_on_failure() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
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
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        scheduler.start_run(&mut run, &mut outbox).unwrap();

        // Fail task_a and cascade skip.
        scheduler
            .queue_task(&mut run, &task_a, &mut outbox)
            .unwrap();
        scheduler
            .start_task(&mut run, &task_a, "worker-1", &mut outbox)
            .unwrap();

        scheduler
            .record_task_result(
                &mut run,
                &task_a,
                TaskResult::Failed(crate::task::TaskError::new(
                    crate::task::TaskErrorCategory::UserCode,
                    "test failure",
                )),
                &mut outbox,
            )
            .unwrap();

        let task_b_state = run.get_task(&task_b).unwrap().state;
        assert_eq!(task_b_state, TaskState::Skipped);

        let completed = scheduler.maybe_complete_run(&mut run, &mut outbox).unwrap();
        assert_eq!(completed, Some(RunState::Failed));
        assert_eq!(run.state, RunState::Failed);
    }

    #[test]
    fn scheduler_prioritizes_lower_priority_values() {
        let task_low = TaskId::generate();
        let task_high = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_low,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "low"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 10,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_high,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "high"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].task_id, task_high);
        assert_eq!(ready[1].task_id, task_low);
    }

    #[test]
    fn scheduler_respects_max_parallelism() {
        let tasks: Vec<_> = (0..5)
            .map(|_| TaskSpec {
                task_id: TaskId::generate(),
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "task"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .collect();

        let mut builder = PlanBuilder::new("tenant", "workspace");
        for task in tasks {
            builder = builder.add_task(task);
        }
        let plan = builder.build().unwrap();

        let config = SchedulerConfig {
            max_parallelism: 2,
            continue_on_failure: true,
        };
        let scheduler = Scheduler::with_config(plan, config);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn scheduler_run_lifecycle() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        scheduler.start_run(&mut run, &mut outbox).unwrap();
        assert_eq!(run.state, RunState::Running);

        scheduler
            .queue_task(&mut run, &task_id, &mut outbox)
            .unwrap();
        scheduler
            .start_task(&mut run, &task_id, "worker-1", &mut outbox)
            .unwrap();

        scheduler
            .record_task_result(
                &mut run,
                &task_id,
                TaskResult::Succeeded(crate::task::TaskOutput {
                    materialization_id: arco_core::MaterializationId::generate(),
                    files: vec![],
                    row_count: 0,
                    byte_size: 0,
                }),
                &mut outbox,
            )
            .unwrap();

        let final_state = scheduler.maybe_complete_run(&mut run, &mut outbox).unwrap();
        assert_eq!(final_state, Some(RunState::Succeeded));
        assert_eq!(run.state, RunState::Succeeded);
    }

    #[test]
    fn scheduler_cancel_run() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "a"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "b"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        scheduler.start_run(&mut run, &mut outbox).unwrap();
        scheduler.cancel_run(&mut run, &mut outbox).unwrap();

        assert_eq!(run.state, RunState::Cancelled);
        assert_eq!(run.get_task(&task_a).unwrap().state, TaskState::Cancelled);
        assert_eq!(run.get_task(&task_b).unwrap().state, TaskState::Cancelled);
    }
}
