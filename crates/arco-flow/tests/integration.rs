//! Integration tests for arco-flow orchestration.

use arco_core::{AssetId, TaskId};
use arco_flow::events::ExecutionEventData;
use arco_flow::outbox::InMemoryOutbox;
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::{RunState, RunTrigger};
use arco_flow::runner::{NoOpRunner, RunContext, Runner};
use arco_flow::scheduler::Scheduler;
use arco_flow::task::{TaskError, TaskErrorCategory, TaskState};

/// Test full orchestration lifecycle: plan -> run -> execute -> complete.
#[tokio::test]
async fn full_orchestration_lifecycle() {
    // Build a simple DAG: raw -> staging -> mart
    let task_raw = TaskId::generate();
    let task_staging = TaskId::generate();
    let task_mart = TaskId::generate();

    let plan = PlanBuilder::new("acme-corp", "production")
        .add_task(TaskSpec {
            task_id: task_raw,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
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
            partition_key: None,
            upstream_task_ids: vec![task_staging],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .expect("plan should be valid");

    // Verify plan is topologically sorted
    let pos_raw = plan
        .tasks
        .iter()
        .position(|t| t.task_id == task_raw)
        .unwrap();
    let pos_staging = plan
        .tasks
        .iter()
        .position(|t| t.task_id == task_staging)
        .unwrap();
    let pos_mart = plan
        .tasks
        .iter()
        .position(|t| t.task_id == task_mart)
        .unwrap();
    assert!(pos_raw < pos_staging);
    assert!(pos_staging < pos_mart);

    // Create scheduler and run
    let scheduler = Scheduler::new(plan.clone());
    let mut outbox = InMemoryOutbox::new();
    let mut run = scheduler.create_run(RunTrigger::manual("test@example.com"), &mut outbox);

    // Start the run
    scheduler.start_run(&mut run, &mut outbox).unwrap();
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
    scheduler
        .queue_task(&mut run, &task_raw, &mut outbox)
        .unwrap();
    scheduler
        .start_task(&mut run, &task_raw, "worker-1", &mut outbox)
        .unwrap();

    let result = runner
        .run(&context, &plan.get_task(&task_raw).unwrap())
        .await;
    assert!(result.is_success());

    scheduler
        .record_task_result(&mut run, &task_raw, result, &mut outbox)
        .unwrap();
    assert_eq!(run.get_task(&task_raw).unwrap().state, TaskState::Succeeded);

    // Phase 2: staging task should now be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_staging);

    // Execute staging task
    scheduler
        .queue_task(&mut run, &task_staging, &mut outbox)
        .unwrap();
    scheduler
        .start_task(&mut run, &task_staging, "worker-2", &mut outbox)
        .unwrap();
    let result = runner
        .run(&context, &plan.get_task(&task_staging).unwrap())
        .await;
    scheduler
        .record_task_result(&mut run, &task_staging, result, &mut outbox)
        .unwrap();
    assert_eq!(
        run.get_task(&task_staging).unwrap().state,
        TaskState::Succeeded
    );

    // Phase 3: mart task should now be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_mart);

    // Execute mart task
    scheduler
        .queue_task(&mut run, &task_mart, &mut outbox)
        .unwrap();
    scheduler
        .start_task(&mut run, &task_mart, "worker-3", &mut outbox)
        .unwrap();
    let result = runner
        .run(&context, &plan.get_task(&task_mart).unwrap())
        .await;
    scheduler
        .record_task_result(&mut run, &task_mart, result, &mut outbox)
        .unwrap();
    assert_eq!(
        run.get_task(&task_mart).unwrap().state,
        TaskState::Succeeded
    );

    // Complete the run (and emit RunCompleted)
    let final_state = scheduler
        .maybe_complete_run(&mut run, &mut outbox)
        .unwrap()
        .expect("run should complete");
    assert_eq!(final_state, RunState::Succeeded);
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.tasks_succeeded(), 3);

    // Verify events are sequenced and scoped to the run stream.
    let stream_id = format!("run:{}", run.id);
    for (idx, event) in outbox.events().iter().enumerate() {
        assert_eq!(event.sequence, Some((idx + 1) as u64));
        assert_eq!(event.stream_id.as_deref(), Some(stream_id.as_str()));
    }
}

/// Test that downstream tasks are skipped when upstream fails.
#[tokio::test]
async fn skip_downstream_on_failure() {
    let task_a = TaskId::generate();
    let task_b = TaskId::generate();
    let task_c = TaskId::generate();

    // DAG: a -> b -> c
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
            asset_key: AssetKey::new("staging", "b"),
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
            partition_key: None,
            upstream_task_ids: vec![task_b],
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

    // Fail task_a
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
            arco_flow::runner::TaskResult::Failed(TaskError::new(
                TaskErrorCategory::UserCode,
                "test failure",
            )),
            &mut outbox,
        )
        .unwrap();

    // Verify downstream tasks are skipped
    assert_eq!(run.get_task(&task_b).unwrap().state, TaskState::Skipped);
    assert_eq!(run.get_task(&task_c).unwrap().state, TaskState::Skipped);

    // Run should complete as failed
    let final_state = scheduler
        .maybe_complete_run(&mut run, &mut outbox)
        .unwrap()
        .expect("run should complete");
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
}

/// Test parallel execution of independent tasks.
#[tokio::test]
async fn parallel_independent_tasks() {
    let task_a = TaskId::generate();
    let task_b = TaskId::generate();
    let task_c = TaskId::generate();

    // DAG: a and b are independent, c depends on both
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
        .add_task(TaskSpec {
            task_id: task_c,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "c"),
            partition_key: None,
            upstream_task_ids: vec![task_a, task_b],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .unwrap();

    let scheduler = Scheduler::new(plan);
    let mut outbox = InMemoryOutbox::new();
    let run = scheduler.create_run(RunTrigger::manual("user"), &mut outbox);

    // Both a and b should be ready initially
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 2);

    let ready_ids: Vec<_> = ready.iter().map(|t| t.task_id).collect();
    assert!(ready_ids.contains(&task_a));
    assert!(ready_ids.contains(&task_b));
}

/// Test plan fingerprint is stable.
#[test]
fn plan_fingerprint_stability() {
    let task_id = TaskId::generate();
    let asset_id = AssetId::generate();

    let task = TaskSpec {
        task_id,
        asset_id,
        asset_key: AssetKey::new("raw", "events"),
        partition_key: None,
        upstream_task_ids: vec![],
        stage: 0,
        priority: 0,
        resources: ResourceRequirements::default(),
    };

    let plan1 = PlanBuilder::new("tenant", "workspace")
        .add_task(task.clone())
        .build()
        .unwrap();

    let plan2 = PlanBuilder::new("tenant", "workspace")
        .add_task(task)
        .build()
        .unwrap();

    // Same inputs should produce same fingerprint
    assert_eq!(plan1.fingerprint, plan2.fingerprint);
    // But different plan IDs (generated)
    assert_ne!(plan1.plan_id, plan2.plan_id);
}
