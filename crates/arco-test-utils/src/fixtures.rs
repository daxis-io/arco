//! Pre-built test fixtures for common test scenarios.
//!
//! Provides factory functions to create test data with sensible defaults.

use std::sync::Arc;

use arco_core::{AssetId, TaskId};
use arco_flow::events::{EventBuilder, EventEnvelope};
use arco_flow::plan::{AssetKey, Plan, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::{Run, RunState};
use arco_flow::task::TaskState;

use crate::storage::TracingMemoryBackend;

/// Test context with pre-configured storage and identifiers.
pub struct TestContext {
    /// Shared storage backend.
    pub storage: Arc<TracingMemoryBackend>,
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
}

impl TestContext {
    /// Creates a new test context with unique tenant/workspace.
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Arc::new(TracingMemoryBackend::new()),
            tenant_id: format!("test-tenant-{}", uuid::Uuid::new_v4().as_simple()),
            workspace_id: "test".to_string(),
        }
    }

    /// Creates context with a specific tenant/workspace.
    #[must_use]
    pub fn with_ids(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self {
            storage: Arc::new(TracingMemoryBackend::new()),
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
        }
    }

    /// Returns the base path for this tenant/workspace.
    #[must_use]
    pub fn base_path(&self) -> String {
        format!(
            "tenant={}/workspace={}",
            self.tenant_id, self.workspace_id
        )
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating test plans.
pub struct PlanFactory;

impl PlanFactory {
    /// Creates a simple linear DAG: a -> b -> c
    #[must_use]
    pub fn linear_dag(tenant_id: &str, workspace_id: &str) -> (Plan, TaskId, TaskId, TaskId) {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();

        let plan = PlanBuilder::new(tenant_id, workspace_id)
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "events_cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "daily_summary"),
                partition_key: None,
                upstream_task_ids: vec![task_b],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .expect("linear DAG should be valid");

        (plan, task_a, task_b, task_c)
    }

    /// Creates a diamond DAG: a -> [b, c] -> d
    #[must_use]
    pub fn diamond_dag(tenant_id: &str, workspace_id: &str) -> Plan {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();
        let task_d = TaskId::generate();

        PlanBuilder::new(tenant_id, workspace_id)
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "source"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "branch_a"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "branch_b"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_d,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "combined"),
                partition_key: None,
                upstream_task_ids: vec![task_b, task_c],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .expect("diamond DAG should be valid")
    }

    /// Creates a plan with N independent tasks (for parallelism testing).
    #[must_use]
    pub fn parallel_tasks(tenant_id: &str, workspace_id: &str, count: usize) -> Plan {
        let mut builder = PlanBuilder::new(tenant_id, workspace_id);

        for i in 0..count {
            builder = builder.add_task(TaskSpec {
                task_id: TaskId::generate(),
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", format!("task_{i}")),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: i32::try_from(i).unwrap_or(0),
                stage: 0,
                resources: ResourceRequirements::default(),
            });
        }

        builder.build().expect("parallel tasks should be valid")
    }
}

/// Factory for creating test events.
pub struct EventFactory;

impl EventFactory {
    /// Creates a sequence of events for a complete successful run.
    #[must_use]
    pub fn complete_run_events(plan: &Plan, run: &Run) -> Vec<EventEnvelope> {
        let mut events = Vec::new();

        // RunCreated
        events.push(EventBuilder::run_created(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            &plan.plan_id,
            "Manual",
            Some("test@example.com".to_string()),
        ));

        // RunStarted
        events.push(EventBuilder::run_started(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            &plan.plan_id,
        ));

        // Task events for each task
        for (idx, task) in plan.tasks.iter().enumerate() {
            events.push(EventBuilder::task_queued(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                task.task_id,
                1,
            ));
            events.push(EventBuilder::task_started(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                task.task_id,
                1,
                format!("worker-{idx}"),
            ));
            events.push(EventBuilder::task_completed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                task.task_id,
                TaskState::Succeeded,
                1,
            ));
        }

        // RunCompleted
        events.push(EventBuilder::run_completed(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            RunState::Succeeded,
            plan.tasks.len(),
            0,
            0,
            0,
        ));

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_dag_has_correct_structure() {
        let ctx = TestContext::new();
        let (plan, task_a, task_b, task_c) =
            PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

        assert_eq!(plan.tasks.len(), 3);

        // Verify dependencies
        let spec_a = plan.get_task(&task_a).expect("task a");
        let spec_b = plan.get_task(&task_b).expect("task b");
        let spec_c = plan.get_task(&task_c).expect("task c");

        assert!(spec_a.upstream_task_ids.is_empty());
        assert_eq!(spec_b.upstream_task_ids, vec![task_a]);
        assert_eq!(spec_c.upstream_task_ids, vec![task_b]);
    }

    #[test]
    fn diamond_dag_has_correct_structure() {
        let ctx = TestContext::new();
        let plan = PlanFactory::diamond_dag(&ctx.tenant_id, &ctx.workspace_id);

        assert_eq!(plan.tasks.len(), 4);

        // Find the final task (has 2 dependencies)
        let final_task = plan.tasks.iter().find(|t| t.upstream_task_ids.len() == 2);
        assert!(final_task.is_some());
    }
}
