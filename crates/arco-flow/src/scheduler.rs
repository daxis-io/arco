//! Dependency-aware task scheduling.
//!
//! The scheduler executes plans with:
//!
//! - **Parallelism**: Independent tasks run concurrently
//! - **Dependency ordering**: Tasks wait for their dependencies
//! - **Fault tolerance**: Failed tasks skip downstream, continue independent branches

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use arco_core::{RunId, TaskId};

use crate::error::{Error, Result};
use crate::events::EventBuilder;
use crate::outbox::EventSink;
use crate::plan::{Plan, TaskSpec};
use crate::run::{Run, RunState, RunTrigger};
use crate::runner::TaskResult;
use crate::task::{TaskError, TaskErrorCategory, TaskState};

/// Scheduler configuration.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum concurrent tasks.
    pub max_parallelism: usize,
    /// Whether to continue on task failure (skip downstream only).
    pub continue_on_failure: bool,
    /// Retry policy for retryable failures.
    pub retry_policy: RetryPolicy,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_parallelism: 10,
            continue_on_failure: true,
            retry_policy: RetryPolicy::default(),
        }
    }
}

/// Retry policy for task failures.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Whether retries are enabled for retryable errors.
    pub enabled: bool,
    /// Base backoff used for the first retry.
    pub base_backoff: Duration,
    /// Maximum backoff cap.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            base_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(5 * 60),
        }
    }
}

impl RetryPolicy {
    fn backoff_for(&self, run_id: RunId, task_id: TaskId, attempt: u32) -> Duration {
        if !self.enabled {
            return Duration::ZERO;
        }

        let exponent = attempt.saturating_sub(1).min(31);
        let factor = 1u32 << exponent;

        let exponential = self
            .base_backoff
            .checked_mul(factor)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff);

        deterministic_full_jitter(run_id, task_id, attempt, exponential)
    }
}

fn deterministic_full_jitter(
    run_id: RunId,
    task_id: TaskId,
    attempt: u32,
    max_delay: Duration,
) -> Duration {
    let max_ms = u64::try_from(max_delay.as_millis()).unwrap_or(u64::MAX);
    if max_ms == 0 {
        return Duration::ZERO;
    }

    let mut hasher = Sha256::new();
    hasher.update(run_id.to_string());
    hasher.update(task_id.to_string());
    hasher.update(attempt.to_le_bytes());

    let digest = hasher.finalize();
    let value = digest.get(..8).map_or(0, |bytes| {
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(bytes);
        u64::from_le_bytes(prefix)
    });

    let range = max_ms.saturating_add(1);
    let jitter_ms = if range == 0 { value } else { value % range };
    Duration::from_millis(jitter_ms)
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
    #[tracing::instrument(
        skip(self, outbox),
        fields(
            tenant_id = %self.plan.tenant_id,
            workspace_id = %self.plan.workspace_id,
            plan_id = %self.plan.plan_id,
            trigger_type = %trigger.trigger_type
        )
    )]
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
    /// - It is in Pending or Ready state
    /// - All its upstream dependencies have Succeeded
    /// - We haven't exceeded `max_parallelism` (considering queued/running tasks)
    #[must_use]
    #[tracing::instrument(skip(self, run), fields(run_id = %run.id))]
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
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id, task_id = %task_id))]
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
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id, task_id = %task_id))]
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
            if matches!(exec.state, TaskState::Pending | TaskState::Ready) && !is_ready {
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
    #[tracing::instrument(
        skip(self, run, outbox),
        fields(run_id = %run.id, task_id = %task_id, worker_id = %worker_id)
    )]
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
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id))]
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
    #[tracing::instrument(
        skip(self, run, outbox),
        fields(run_id = %run.id, final_state = %final_state)
    )]
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
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id))]
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
    #[tracing::instrument(
        skip(self, run, outbox, result),
        fields(run_id = %run.id, task_id = %task_id)
    )]
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
                self.record_task_failure(run, task_id, error, outbox)?;
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
        &self,
        run: &mut Run,
        task_id: &TaskId,
        error: TaskError,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        let run_id = run.id;
        let (attempt, retry_at, backoff) = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;
            let attempt = exec.attempt;
            exec.fail(error.clone())?;
            if self.config.retry_policy.enabled && error.retryable && exec.can_retry() {
                let now = Utc::now();
                let backoff = self
                    .config
                    .retry_policy
                    .backoff_for(run_id, *task_id, attempt);
                let retry_at =
                    now + chrono::Duration::from_std(backoff).unwrap_or(chrono::Duration::MAX);

                exec.transition_to(TaskState::RetryWait)?;
                exec.retry_at = Some(retry_at);

                (attempt, Some(retry_at), Some(backoff))
            } else {
                (attempt, None, None)
            }
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

        if let (Some(retry_at), Some(backoff)) = (retry_at, backoff) {
            emit_sequenced(
                run,
                outbox,
                EventBuilder::task_retry_scheduled(
                    &run.tenant_id,
                    &run.workspace_id,
                    run.id,
                    *task_id,
                    attempt,
                    retry_at,
                    backoff,
                ),
            );
        }

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
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id))]
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

    /// Advances any tasks whose retry backoff has elapsed.
    ///
    /// Returns the task IDs that transitioned from `RetryWait` to `Ready`.
    ///
    /// # Errors
    ///
    /// Returns an error if a task transition is invalid.
    pub fn process_retries(
        &self,
        run: &mut Run,
        outbox: &mut impl EventSink,
    ) -> Result<Vec<TaskId>> {
        self.process_retries_at(run, outbox, Utc::now())
    }

    /// Advances any tasks whose retry backoff has elapsed at a given time.
    ///
    /// This is useful for tests and deterministic simulations.
    ///
    /// # Errors
    ///
    /// Returns an error if a task transition is invalid.
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id, now = %now))]
    pub fn process_retries_at(
        &self,
        run: &mut Run,
        outbox: &mut impl EventSink,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>> {
        let mut transitioned = Vec::new();

        for spec in &self.plan.tasks {
            let task_id = spec.task_id;
            let due = run
                .get_task(&task_id)
                .is_some_and(|exec| exec.is_retry_due_at(now));

            if !due {
                continue;
            }

            let (previous_attempt, new_attempt) = {
                let exec = run
                    .get_task_mut(&task_id)
                    .ok_or(Error::TaskNotFound { task_id })?;

                let previous_attempt = exec.attempt;
                exec.transition_to(TaskState::Ready)?;
                let new_attempt = exec.attempt;

                (previous_attempt, new_attempt)
            };

            emit_sequenced(
                run,
                outbox,
                EventBuilder::task_retried(
                    &run.tenant_id,
                    &run.workspace_id,
                    run.id,
                    task_id,
                    previous_attempt,
                    new_attempt,
                ),
            );

            transitioned.push(task_id);
        }

        Ok(transitioned)
    }

    /// Records a heartbeat for a running task.
    ///
    /// Heartbeats for non-running tasks are ignored (late/duplicate heartbeats are expected in
    /// distributed systems).
    ///
    /// # Errors
    ///
    /// Returns an error if the task does not exist.
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id, task_id = %task_id))]
    pub fn record_task_heartbeat(
        &self,
        run: &mut Run,
        task_id: &TaskId,
        outbox: &mut impl EventSink,
    ) -> Result<()> {
        self.record_task_heartbeat_at(run, task_id, outbox, Utc::now())
    }

    /// Records a heartbeat for a running task at a given time.
    ///
    /// # Errors
    ///
    /// Returns an error if the task does not exist.
    #[tracing::instrument(
        skip(self, run, outbox),
        fields(run_id = %run.id, task_id = %task_id, now = %now)
    )]
    pub fn record_task_heartbeat_at(
        &self,
        run: &mut Run,
        task_id: &TaskId,
        outbox: &mut impl EventSink,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let attempt = {
            let exec = run
                .get_task_mut(task_id)
                .ok_or(Error::TaskNotFound { task_id: *task_id })?;

            if !matches!(exec.state, TaskState::Running | TaskState::Dispatched) {
                return Ok(());
            }

            exec.record_heartbeat_at(now);
            exec.attempt
        };

        emit_sequenced(
            run,
            outbox,
            EventBuilder::task_heartbeat(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                *task_id,
                attempt,
            ),
        );

        Ok(())
    }

    /// Detects tasks with stale heartbeats and marks them failed (retrying if configured).
    ///
    /// Returns task IDs that were marked stale.
    ///
    /// # Errors
    ///
    /// Returns an error if a task transition is invalid.
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id))]
    pub fn process_stale_heartbeats(
        &self,
        run: &mut Run,
        outbox: &mut impl EventSink,
    ) -> Result<Vec<TaskId>> {
        self.process_stale_heartbeats_at(run, outbox, Utc::now())
    }

    /// Detects stale heartbeats at a given time.
    ///
    /// # Errors
    ///
    /// Returns an error if a task transition is invalid.
    #[tracing::instrument(skip(self, run, outbox), fields(run_id = %run.id, now = %now))]
    pub fn process_stale_heartbeats_at(
        &self,
        run: &mut Run,
        outbox: &mut impl EventSink,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>> {
        let mut stale = Vec::new();

        for spec in &self.plan.tasks {
            let task_id = spec.task_id;
            let is_stale = run
                .get_task(&task_id)
                .is_some_and(|exec| exec.is_heartbeat_stale_at(now));

            if !is_stale {
                continue;
            }

            let error = TaskError::new(TaskErrorCategory::Infrastructure, "heartbeat timeout");
            self.record_task_failure(run, &task_id, error, outbox)?;
            self.process_task_completion(run, &task_id, outbox)?;
            stale.push(task_id);
        }

        Ok(stale)
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
    fn scheduler_creates_run_from_plan() -> Result<()> {
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
            .build()?;

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.task_executions.len(), 1);
        assert_eq!(outbox.events().len(), 1);
        assert_eq!(outbox.events()[0].sequence, Some(1));

        Ok(())
    }

    #[test]
    fn scheduler_returns_ready_tasks() -> Result<()> {
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
            .build()?;

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, task_a);

        Ok(())
    }

    #[test]
    fn scheduler_skips_downstream_on_failure() -> Result<()> {
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
            .build()?;

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        scheduler.start_run(&mut run, &mut outbox)?;

        // Fail task_a and cascade skip.
        scheduler.queue_task(&mut run, &task_a, &mut outbox)?;
        scheduler.start_task(&mut run, &task_a, "worker-1", &mut outbox)?;

        scheduler.record_task_result(
            &mut run,
            &task_a,
            TaskResult::Failed(TaskError::new(TaskErrorCategory::UserCode, "test failure")),
            &mut outbox,
        )?;

        let task_b_state = run
            .get_task(&task_b)
            .ok_or(Error::TaskNotFound { task_id: task_b })?
            .state;
        assert_eq!(task_b_state, TaskState::Skipped);

        let completed = scheduler.maybe_complete_run(&mut run, &mut outbox)?;
        assert_eq!(completed, Some(RunState::Failed));
        assert_eq!(run.state, RunState::Failed);

        Ok(())
    }

    #[test]
    fn scheduler_prioritizes_lower_priority_values() -> Result<()> {
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
            .build()?;

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].task_id, task_high);
        assert_eq!(ready[1].task_id, task_low);

        Ok(())
    }

    #[test]
    fn scheduler_respects_max_parallelism() -> Result<()> {
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
        let plan = builder.build()?;

        let config = SchedulerConfig {
            max_parallelism: 2,
            continue_on_failure: true,
            retry_policy: RetryPolicy::default(),
        };
        let scheduler = Scheduler::with_config(plan, config);
        let mut outbox = InMemoryOutbox::new();
        let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 2);

        Ok(())
    }

    #[test]
    fn scheduler_run_lifecycle() -> Result<()> {
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
            .build()?;

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        scheduler.start_run(&mut run, &mut outbox)?;
        assert_eq!(run.state, RunState::Running);

        scheduler.queue_task(&mut run, &task_id, &mut outbox)?;
        scheduler.start_task(&mut run, &task_id, "worker-1", &mut outbox)?;

        scheduler.record_task_result(
            &mut run,
            &task_id,
            TaskResult::Succeeded(crate::task::TaskOutput {
                materialization_id: arco_core::MaterializationId::generate(),
                files: vec![],
                row_count: 0,
                byte_size: 0,
            }),
            &mut outbox,
        )?;

        let final_state = scheduler.maybe_complete_run(&mut run, &mut outbox)?;
        assert_eq!(final_state, Some(RunState::Succeeded));
        assert_eq!(run.state, RunState::Succeeded);

        Ok(())
    }

    #[test]
    fn scheduler_cancel_run() -> Result<()> {
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
            .build()?;

        let scheduler = Scheduler::new(plan);
        let mut outbox = InMemoryOutbox::new();
        let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

        scheduler.start_run(&mut run, &mut outbox)?;
        scheduler.cancel_run(&mut run, &mut outbox)?;

        assert_eq!(run.state, RunState::Cancelled);
        assert_eq!(
            run.get_task(&task_a)
                .ok_or(Error::TaskNotFound { task_id: task_a })?
                .state,
            TaskState::Cancelled
        );
        assert_eq!(
            run.get_task(&task_b)
                .ok_or(Error::TaskNotFound { task_id: task_b })?
                .state,
            TaskState::Cancelled
        );

        Ok(())
    }
}
