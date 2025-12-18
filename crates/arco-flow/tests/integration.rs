//! Integration tests for arco-flow orchestration.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use arco_core::{AssetId, TaskId};
use arco_flow::error::{Error, Result};
use arco_flow::events::ExecutionEventData;
use arco_flow::outbox::InMemoryOutbox;
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::{RunState, RunTrigger};
use arco_flow::runner::{NoOpRunner, RunContext, Runner};
use arco_flow::scheduler::{RetryPolicy, Scheduler, SchedulerConfig};
use arco_flow::task::{TaskError, TaskErrorCategory, TaskState};
use arco_flow::task_key::TaskOperation;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Test full orchestration lifecycle: plan -> run -> execute -> complete.
#[tokio::test]
async fn full_orchestration_lifecycle() -> Result<()> {
    // Build a simple DAG: raw -> staging -> mart
    let task_raw = TaskId::generate();
    let task_staging = TaskId::generate();
    let task_mart = TaskId::generate();

    let plan = PlanBuilder::new("acme-corp", "production")
        .add_task(TaskSpec {
            task_id: task_raw,
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
            task_id: task_staging,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("staging", "events_cleaned"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![task_raw],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_mart,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "daily_summary"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![task_staging],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()?;

    // Verify plan is topologically sorted
    let pos_raw = plan
        .tasks
        .iter()
        .position(|t| t.task_id == task_raw)
        .ok_or(Error::TaskNotFound { task_id: task_raw })?;
    let pos_staging = plan
        .tasks
        .iter()
        .position(|t| t.task_id == task_staging)
        .ok_or(Error::TaskNotFound {
            task_id: task_staging,
        })?;
    let pos_mart = plan
        .tasks
        .iter()
        .position(|t| t.task_id == task_mart)
        .ok_or(Error::TaskNotFound { task_id: task_mart })?;
    assert!(pos_raw < pos_staging);
    assert!(pos_staging < pos_mart);

    // Create scheduler and run
    let scheduler = Scheduler::new(plan.clone());
    let mut outbox = InMemoryOutbox::new();
    let mut run = scheduler.create_run(RunTrigger::manual("test@example.com"), &mut outbox);

    // Start the run
    scheduler.start_run(&mut run, &mut outbox)?;
    assert_eq!(run.state, RunState::Running);

    // Create runner and context
    let runner = NoOpRunner;
    let context = RunContext {
        tenant_id: run.tenant_id.clone(),
        workspace_id: run.workspace_id.clone(),
        run_id: run.id,
    };

    // Execute tasks in dependency order
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_raw);

    // Execute raw task
    scheduler.queue_task(&mut run, &task_raw, &mut outbox)?;
    scheduler.start_task(&mut run, &task_raw, "worker-1", &mut outbox)?;

    let task_raw_spec = plan
        .get_task(&task_raw)
        .ok_or(Error::TaskNotFound { task_id: task_raw })?;
    let result = runner.run(&context, task_raw_spec).await;
    assert!(result.is_success());

    scheduler.record_task_result(&mut run, &task_raw, result, &mut outbox)?;
    assert_eq!(
        run.get_task(&task_raw)
            .ok_or(Error::TaskNotFound { task_id: task_raw })?
            .state,
        TaskState::Succeeded
    );

    // Phase 2: staging task should now be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_staging);

    // Execute staging task
    scheduler.queue_task(&mut run, &task_staging, &mut outbox)?;
    scheduler.start_task(&mut run, &task_staging, "worker-2", &mut outbox)?;
    let task_staging_spec = plan.get_task(&task_staging).ok_or(Error::TaskNotFound {
        task_id: task_staging,
    })?;
    let result = runner.run(&context, task_staging_spec).await;
    scheduler.record_task_result(&mut run, &task_staging, result, &mut outbox)?;
    assert_eq!(
        run.get_task(&task_staging)
            .ok_or(Error::TaskNotFound {
                task_id: task_staging,
            })?
            .state,
        TaskState::Succeeded
    );

    // Phase 3: mart task should now be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_mart);

    // Execute mart task
    scheduler.queue_task(&mut run, &task_mart, &mut outbox)?;
    scheduler.start_task(&mut run, &task_mart, "worker-3", &mut outbox)?;
    let task_mart_spec = plan
        .get_task(&task_mart)
        .ok_or(Error::TaskNotFound { task_id: task_mart })?;
    let result = runner.run(&context, task_mart_spec).await;
    scheduler.record_task_result(&mut run, &task_mart, result, &mut outbox)?;
    assert_eq!(
        run.get_task(&task_mart)
            .ok_or(Error::TaskNotFound { task_id: task_mart })?
            .state,
        TaskState::Succeeded
    );

    // Complete the run (and emit RunCompleted)
    let final_state = scheduler
        .maybe_complete_run(&mut run, &mut outbox)?
        .ok_or_else(|| Error::TaskExecutionFailed {
            message: "run should complete".into(),
        })?;
    assert_eq!(final_state, RunState::Succeeded);
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.tasks_succeeded(), 3);

    // Verify events are sequenced and scoped to the run stream.
    let stream_id = format!("run:{}", run.id);
    for (idx, event) in outbox.events().iter().enumerate() {
        assert_eq!(event.sequence, Some((idx + 1) as u64));
        assert_eq!(event.stream_id.as_deref(), Some(stream_id.as_str()));
    }

    Ok(())
}

/// Test that downstream tasks are skipped when upstream fails.
#[tokio::test]
async fn skip_downstream_on_failure() -> Result<()> {
    let task_a = TaskId::generate();
    let task_b = TaskId::generate();
    let task_c = TaskId::generate();

    // DAG: a -> b -> c
    let plan = PlanBuilder::new("tenant", "workspace")
        .add_task(TaskSpec {
            task_id: task_a,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "a"),
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
            asset_key: AssetKey::new("staging", "b"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![task_a],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_c,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "c"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![task_b],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()?;

    let scheduler = Scheduler::new(plan);
    let mut outbox = InMemoryOutbox::new();
    let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

    scheduler.start_run(&mut run, &mut outbox)?;

    // Fail task_a
    scheduler.queue_task(&mut run, &task_a, &mut outbox)?;
    scheduler.start_task(&mut run, &task_a, "worker-1", &mut outbox)?;
    scheduler.record_task_result(
        &mut run,
        &task_a,
        arco_flow::runner::TaskResult::Failed(TaskError::new(
            TaskErrorCategory::UserCode,
            "test failure",
        )),
        &mut outbox,
    )?;

    // Verify downstream tasks are skipped
    assert_eq!(
        run.get_task(&task_b)
            .ok_or(Error::TaskNotFound { task_id: task_b })?
            .state,
        TaskState::Skipped
    );
    assert_eq!(
        run.get_task(&task_c)
            .ok_or(Error::TaskNotFound { task_id: task_c })?
            .state,
        TaskState::Skipped
    );

    // Run should complete as failed
    let final_state = scheduler
        .maybe_complete_run(&mut run, &mut outbox)?
        .ok_or_else(|| Error::TaskExecutionFailed {
            message: "run should complete".into(),
        })?;
    assert_eq!(final_state, RunState::Failed);

    let skipped_task_ids: Vec<_> = outbox
        .events()
        .iter()
        .filter_map(|e| match &e.data {
            ExecutionEventData::TaskCompleted { task_id, state, .. }
                if *state == TaskState::Skipped =>
            {
                Some(*task_id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(skipped_task_ids, vec![task_b, task_c]);

    Ok(())
}

/// Test parallel execution of independent tasks.
#[tokio::test]
async fn parallel_independent_tasks() -> Result<()> {
    let task_a = TaskId::generate();
    let task_b = TaskId::generate();
    let task_c = TaskId::generate();

    // DAG: a and b are independent, c depends on both
    let plan = PlanBuilder::new("tenant", "workspace")
        .add_task(TaskSpec {
            task_id: task_a,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "a"),
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
            asset_key: AssetKey::new("raw", "b"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_c,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "c"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![task_a, task_b],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()?;

    let scheduler = Scheduler::new(plan);
    let mut outbox = InMemoryOutbox::new();
    let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

    // Both a and b should be ready initially
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 2);

    let ready_ids: Vec<_> = ready.iter().map(|t| t.task_id).collect();
    assert!(ready_ids.contains(&task_a));
    assert!(ready_ids.contains(&task_b));

    Ok(())
}

/// Test plan fingerprint is stable.
#[test]
fn plan_fingerprint_stability() -> Result<()> {
    let task_id = TaskId::generate();
    let asset_id = AssetId::generate();

    let task = TaskSpec {
        task_id,
        asset_id,
        asset_key: AssetKey::new("raw", "events"),
        operation: TaskOperation::Materialize,
        partition_key: None,
        upstream_task_ids: vec![],
        stage: 0,
        priority: 0,
        resources: ResourceRequirements::default(),
    };

    let plan1 = PlanBuilder::new("tenant", "workspace")
        .add_task(task.clone())
        .build()?;

    let plan2 = PlanBuilder::new("tenant", "workspace")
        .add_task(task)
        .build()?;

    // Same inputs should produce same fingerprint
    assert_eq!(plan1.fingerprint, plan2.fingerprint);
    // But different plan IDs (generated)
    assert_ne!(plan1.plan_id, plan2.plan_id);

    Ok(())
}

/// Test that retryable failures enter backoff and are retried.
#[tokio::test]
async fn retryable_task_failure_retries_with_backoff() -> Result<()> {
    struct FlakyRunner {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Runner for FlakyRunner {
        async fn run(
            &self,
            _context: &RunContext,
            _task: &TaskSpec,
        ) -> arco_flow::runner::TaskResult {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return arco_flow::runner::TaskResult::Failed(TaskError::new(
                    TaskErrorCategory::Infrastructure,
                    "transient infrastructure failure",
                ));
            }

            arco_flow::runner::TaskResult::Succeeded(arco_flow::task::TaskOutput {
                materialization_id: arco_core::MaterializationId::generate(),
                files: vec![],
                row_count: 1,
                byte_size: 1,
            })
        }
    }

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

    let config = SchedulerConfig {
        max_parallelism: 1,
        continue_on_failure: true,
        retry_policy: RetryPolicy {
            enabled: true,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        },
    };

    let scheduler = Scheduler::with_config(plan.clone(), config);
    let mut outbox = InMemoryOutbox::new();
    let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);
    scheduler.start_run(&mut run, &mut outbox)?;

    let runner = FlakyRunner {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let context = RunContext {
        tenant_id: run.tenant_id.clone(),
        workspace_id: run.workspace_id.clone(),
        run_id: run.id,
    };

    // First attempt fails -> RetryWait.
    scheduler.queue_task(&mut run, &task_id, &mut outbox)?;
    scheduler.start_task(&mut run, &task_id, "worker-1", &mut outbox)?;
    let task_spec = plan
        .get_task(&task_id)
        .ok_or(Error::TaskNotFound { task_id })?;
    let result = runner.run(&context, task_spec).await;
    scheduler.record_task_result(&mut run, &task_id, result, &mut outbox)?;

    assert_eq!(
        run.get_task(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?
            .state,
        TaskState::RetryWait
    );
    assert!(matches!(
        &outbox
            .events()
            .last()
            .ok_or_else(|| Error::TaskExecutionFailed {
                message: "expected TaskRetryScheduled event".into(),
            })?
            .data,
        ExecutionEventData::TaskRetryScheduled { .. }
    ));

    // Backoff elapsed -> Ready with incremented attempt.
    scheduler.process_retries(&mut run, &mut outbox)?;
    assert_eq!(
        run.get_task(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?
            .state,
        TaskState::Ready
    );
    assert_eq!(
        run.get_task(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?
            .attempt,
        2
    );

    // Second attempt succeeds.
    scheduler.queue_task(&mut run, &task_id, &mut outbox)?;
    scheduler.start_task(&mut run, &task_id, "worker-2", &mut outbox)?;
    let task_spec = plan
        .get_task(&task_id)
        .ok_or(Error::TaskNotFound { task_id })?;
    let result = runner.run(&context, task_spec).await;
    scheduler.record_task_result(&mut run, &task_id, result, &mut outbox)?;

    let final_state = scheduler
        .maybe_complete_run(&mut run, &mut outbox)?
        .ok_or_else(|| Error::TaskExecutionFailed {
            message: "run should complete".into(),
        })?;
    assert_eq!(final_state, RunState::Succeeded);

    let retried = outbox
        .events()
        .iter()
        .filter(|e| matches!(e.data, ExecutionEventData::TaskRetried { .. }))
        .count();
    assert_eq!(retried, 1);

    Ok(())
}

/// Test that stale heartbeats are detected and converted into retryable failures.
#[tokio::test]
async fn heartbeat_staleness_triggers_retry() -> Result<()> {
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

    let config = SchedulerConfig {
        max_parallelism: 1,
        continue_on_failure: true,
        retry_policy: RetryPolicy {
            enabled: true,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        },
    };

    let scheduler = Scheduler::with_config(plan.clone(), config);
    let mut outbox = InMemoryOutbox::new();
    let mut run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);
    scheduler.start_run(&mut run, &mut outbox)?;

    // Put the task into RUNNING, then force a stale heartbeat.
    scheduler.queue_task(&mut run, &task_id, &mut outbox)?;
    scheduler.start_task(&mut run, &task_id, "worker-1", &mut outbox)?;

    {
        let exec = run
            .get_task_mut(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?;
        exec.heartbeat_timeout = Duration::from_secs(1);
        exec.last_heartbeat = Some(chrono::Utc::now() - chrono::Duration::seconds(10));
    }

    let stale = scheduler.process_stale_heartbeats_at(&mut run, &mut outbox, chrono::Utc::now())?;
    assert_eq!(stale, vec![task_id]);
    assert_eq!(
        run.get_task(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?
            .state,
        TaskState::RetryWait
    );

    // Retry becomes ready immediately under zero-backoff policy.
    scheduler.process_retries(&mut run, &mut outbox)?;
    assert_eq!(
        run.get_task(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?
            .state,
        TaskState::Ready
    );
    assert_eq!(
        run.get_task(&task_id)
            .ok_or(Error::TaskNotFound { task_id })?
            .attempt,
        2
    );

    // Verify we can complete successfully after retry.
    let runner = NoOpRunner;
    let context = RunContext {
        tenant_id: run.tenant_id.clone(),
        workspace_id: run.workspace_id.clone(),
        run_id: run.id,
    };

    scheduler.queue_task(&mut run, &task_id, &mut outbox)?;
    scheduler.start_task(&mut run, &task_id, "worker-2", &mut outbox)?;
    let task_spec = plan
        .get_task(&task_id)
        .ok_or(Error::TaskNotFound { task_id })?;
    let result = runner.run(&context, task_spec).await;
    scheduler.record_task_result(&mut run, &task_id, result, &mut outbox)?;

    let final_state = scheduler
        .maybe_complete_run(&mut run, &mut outbox)?
        .ok_or_else(|| Error::TaskExecutionFailed {
            message: "run should complete".into(),
        })?;
    assert_eq!(final_state, RunState::Succeeded);

    let failed = outbox
        .events()
        .iter()
        .filter(|e| matches!(e.data, ExecutionEventData::TaskFailed { .. }))
        .count();
    assert_eq!(failed, 1);

    Ok(())
}
