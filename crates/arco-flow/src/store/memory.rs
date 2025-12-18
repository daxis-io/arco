//! In-memory store implementation for testing.
//!
//! This module provides [`InMemoryStore`], a simple in-memory implementation of
//! the [`Store`] trait suitable for testing and development.
//!
//! ## Limitations
//!
//! - **NOT suitable for production**: No durability, no cross-process coordination
//! - **Single-process only**: State is not shared across process boundaries
//! - **No persistence**: All state is lost when the process exits

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use arco_core::{RunId, TaskId};

use super::{CasResult, Store};
use crate::error::{Error, Result};
use crate::run::Run;
use crate::task::{TaskExecution, TaskState, TaskTimeoutConfig, TransitionReason};

/// In-memory store for testing.
///
/// Provides a simple, thread-safe implementation of the [`Store`] trait using
/// `RwLock` for synchronization.
///
/// ## Example
///
/// ```rust
/// use arco_flow::store::memory::InMemoryStore;
///
/// let store = InMemoryStore::new();
/// // Use store in tests...
/// ```
#[derive(Debug)]
pub struct InMemoryStore {
    runs: RwLock<HashMap<RunId, Run>>,
    timeout_config: TaskTimeoutConfig,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts a lock poison error to a storage error.
fn poison_err<T>(_: PoisonError<T>) -> Error {
    Error::storage("lock poisoned")
}

impl InMemoryStore {
    /// Creates a new in-memory store with default timeout configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            runs: RwLock::new(HashMap::new()),
            timeout_config: TaskTimeoutConfig::default(),
        }
    }

    /// Creates a store with custom timeout configuration.
    ///
    /// Use this to test zombie detection with specific timeout values.
    #[must_use]
    pub fn with_timeout_config(config: TaskTimeoutConfig) -> Self {
        Self {
            runs: RwLock::new(HashMap::new()),
            timeout_config: config,
        }
    }

    /// Returns the number of runs currently stored.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn run_count(&self) -> Result<usize> {
        let count = {
            let runs = self.runs.read().map_err(poison_err)?;
            runs.len()
        };
        Ok(count)
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn get_run(&self, run_id: &RunId) -> Result<Option<Run>> {
        let result = {
            let runs = self.runs.read().map_err(poison_err)?;
            runs.get(run_id).cloned()
        };
        Ok(result)
    }

    async fn save_run(&self, run: &Run) -> Result<()> {
        {
            let mut runs = self.runs.write().map_err(poison_err)?;
            runs.insert(run.id, run.clone());
        }
        Ok(())
    }

    async fn cas_task_state(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        expected_state: TaskState,
        target_state: TaskState,
        reason: TransitionReason,
    ) -> Result<CasResult> {
        let mut runs = self.runs.write().map_err(poison_err)?;

        let Some(run) = runs.get_mut(run_id) else {
            drop(runs);
            return Ok(CasResult::NotFound);
        };

        let Some(task) = run.get_task_mut(task_id) else {
            drop(runs);
            return Ok(CasResult::NotFound);
        };

        if task.state != expected_state {
            let actual = task.state;
            drop(runs);
            return Ok(CasResult::StateMismatch { actual });
        }

        let transition_result = task.transition_to_with_reason(target_state, reason);
        drop(runs);
        transition_result.map(|()| CasResult::Success)
    }

    async fn get_tasks_by_state(
        &self,
        run_id: &RunId,
        state: TaskState,
    ) -> Result<Vec<TaskExecution>> {
        let result = {
            let runs = self.runs.read().map_err(poison_err)?;
            runs.get(run_id).map_or_else(Vec::new, |run| {
                run.task_executions
                    .iter()
                    .filter(|t| t.state == state)
                    .cloned()
                    .collect()
            })
        };
        Ok(result)
    }

    async fn get_zombie_tasks(&self, run_id: &RunId, now: DateTime<Utc>) -> Result<Vec<TaskId>> {
        let timeout_config = self.timeout_config;
        let result = {
            let runs = self.runs.read().map_err(poison_err)?;
            runs.get(run_id).map_or_else(Vec::new, |run| {
                run.task_executions
                    .iter()
                    .filter(|t| t.is_zombie(now, &timeout_config))
                    .map(|t| t.task_id)
                    .collect()
            })
        };
        Ok(result)
    }

    async fn get_retry_eligible_tasks(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>> {
        let result = {
            let runs = self.runs.read().map_err(poison_err)?;
            runs.get(run_id).map_or_else(Vec::new, |run| {
                run.task_executions
                    .iter()
                    .filter(|t| t.state == TaskState::RetryWait && t.retry_at.is_some_and(|r| r <= now))
                    .map(|t| t.task_id)
                    .collect()
            })
        };
        Ok(result)
    }

    async fn get_ready_tasks_by_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<(String, Vec<TaskExecution>)>> {
        let runs = self.runs.read().map_err(poison_err)?;

        let Some(run) = runs.get(run_id) else {
            drop(runs);
            return Ok(vec![]);
        };

        let mut ready_tasks: Vec<TaskExecution> = run
            .task_executions
            .iter()
            .filter(|t| t.state == TaskState::Ready)
            .cloned()
            .collect();

        let tenant_id = run.tenant_id.clone();
        drop(runs);

        ready_tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| match (&a.task_key, &b.task_key) {
                    (Some(a_key), Some(b_key)) => a_key.cmp(b_key),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                })
                .then_with(|| a.task_id.cmp(&b.task_id))
        });

        if ready_tasks.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![(tenant_id, ready_tasks)])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use crate::run::{Run, RunTrigger};
    use crate::task_key::TaskOperation;
    use arco_core::AssetId;
    use std::time::Duration;

    fn create_test_run() -> Result<Run> {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("test-tenant", "test-workspace")
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

        Ok(Run::from_plan(&plan, RunTrigger::manual("test-user")))
    }

    fn create_priority_run() -> Result<(Run, TaskId, TaskId, TaskId)> {
        let high_priority_task = TaskId::generate();
        let mid_priority_task = TaskId::generate();
        let low_priority_task = TaskId::generate();

        let plan = PlanBuilder::new("test-tenant", "test-workspace")
            .add_task(TaskSpec {
                task_id: low_priority_task,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 5,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: high_priority_task,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "accounts"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 1,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: mid_priority_task,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "users"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 1,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        Ok((
            Run::from_plan(&plan, RunTrigger::manual("test-user")),
            low_priority_task,
            high_priority_task,
            mid_priority_task,
        ))
    }

    #[tokio::test]
    async fn save_and_get_run() -> Result<()> {
        let store = InMemoryStore::new();
        let run = create_test_run()?;
        let run_id = run.id;

        // Initially empty
        assert!(store.get_run(&run_id).await?.is_none());

        // Save
        store.save_run(&run).await?;

        // Retrieve
        let retrieved = store.get_run(&run_id).await?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, run_id);

        Ok(())
    }

    #[tokio::test]
    async fn cas_task_state_success() -> Result<()> {
        let store = InMemoryStore::new();
        let run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        store.save_run(&run).await?;

        // Transition from Pending to Ready
        let result = store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Pending,
                TaskState::Ready,
                TransitionReason::DependenciesSatisfied,
            )
            .await?;

        assert!(result.is_success());

        // Verify state changed
        let updated = store.get_run(&run_id).await?.unwrap();
        let task = updated.get_task(&task_id).unwrap();
        assert_eq!(task.state, TaskState::Ready);
        assert_eq!(
            task.last_transition_reason,
            Some(TransitionReason::DependenciesSatisfied)
        );
        assert!(task.last_transition_at.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn cas_task_state_sets_transition_timestamps() -> Result<()> {
        let store = InMemoryStore::new();
        let run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        store.save_run(&run).await?;

        store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Pending,
                TaskState::Ready,
                TransitionReason::DependenciesSatisfied,
            )
            .await?;
        store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Ready,
                TaskState::Queued,
                TransitionReason::QuotaAcquired,
            )
            .await?;
        store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Queued,
                TaskState::Dispatched,
                TransitionReason::DispatchedToWorker,
            )
            .await?;

        let updated = store.get_run(&run_id).await?.unwrap();
        let task = updated.get_task(&task_id).unwrap();
        assert_eq!(task.state, TaskState::Dispatched);
        assert!(task.dispatched_at.is_some());
        assert!(task.last_transition_at.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn cas_task_state_rejects_invalid_transition() -> Result<()> {
        let store = InMemoryStore::new();
        let run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        store.save_run(&run).await?;

        let result = store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Pending,
                TaskState::Running,
                TransitionReason::ExecutionStarted,
            )
            .await;

        assert!(matches!(
            result,
            Err(Error::InvalidStateTransition { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn cas_task_state_mismatch() -> Result<()> {
        let store = InMemoryStore::new();
        let run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        store.save_run(&run).await?;

        // Try to transition from wrong state
        let result = store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Running, // Wrong! Actual is Pending
                TaskState::Succeeded,
                TransitionReason::ExecutionSucceeded,
            )
            .await?;

        assert_eq!(
            result,
            CasResult::StateMismatch {
                actual: TaskState::Pending
            }
        );

        // Verify state unchanged
        let run = store.get_run(&run_id).await?.unwrap();
        let task = run.get_task(&task_id).unwrap();
        assert_eq!(task.state, TaskState::Pending);

        Ok(())
    }

    #[tokio::test]
    async fn cas_task_state_not_found() -> Result<()> {
        let store = InMemoryStore::new();

        let result = store
            .cas_task_state(
                &RunId::generate(),
                &TaskId::generate(),
                TaskState::Pending,
                TaskState::Ready,
                TransitionReason::DependenciesSatisfied,
            )
            .await?;

        assert!(result.is_not_found());

        Ok(())
    }

    #[tokio::test]
    async fn get_tasks_by_state() -> Result<()> {
        let store = InMemoryStore::new();
        let run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        store.save_run(&run).await?;

        // All tasks start as Pending
        let pending = store
            .get_tasks_by_state(&run_id, TaskState::Pending)
            .await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].task_id, task_id);

        // No Ready tasks initially
        let ready = store.get_tasks_by_state(&run_id, TaskState::Ready).await?;
        assert!(ready.is_empty());

        // Transition to Ready
        store
            .cas_task_state(
                &run_id,
                &task_id,
                TaskState::Pending,
                TaskState::Ready,
                TransitionReason::DependenciesSatisfied,
            )
            .await?;

        // Now we have one Ready task
        let ready = store.get_tasks_by_state(&run_id, TaskState::Ready).await?;
        assert_eq!(ready.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn get_zombie_tasks_dispatched() -> Result<()> {
        let config = TaskTimeoutConfig {
            dispatch_ack_timeout: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(60),
            max_execution_time: Duration::from_secs(3600),
        };
        let store = InMemoryStore::with_timeout_config(config);

        let mut run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        // Manually set task to Dispatched with old timestamp
        let task = run.get_task_mut(&task_id).unwrap();
        task.state = TaskState::Dispatched;
        let dispatch_time = Utc::now() - chrono::Duration::seconds(60);
        task.dispatched_at = Some(dispatch_time);

        store.save_run(&run).await?;

        // Check for zombies at "now" (60s after dispatch, timeout is 30s)
        let zombies = store.get_zombie_tasks(&run_id, Utc::now()).await?;
        assert_eq!(zombies.len(), 1);
        assert_eq!(zombies[0], task_id);

        Ok(())
    }

    #[tokio::test]
    async fn get_zombie_tasks_running() -> Result<()> {
        let config = TaskTimeoutConfig {
            dispatch_ack_timeout: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(60),
            max_execution_time: Duration::from_secs(3600),
        };
        let store = InMemoryStore::with_timeout_config(config);

        let mut run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        // Manually set task to Running with old heartbeat
        let task = run.get_task_mut(&task_id).unwrap();
        task.state = TaskState::Running;
        let old_heartbeat = Utc::now() - chrono::Duration::seconds(120);
        task.last_heartbeat = Some(old_heartbeat);

        store.save_run(&run).await?;

        // Check for zombies (120s since heartbeat, timeout is 60s)
        let zombies = store.get_zombie_tasks(&run_id, Utc::now()).await?;
        assert_eq!(zombies.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn get_retry_eligible_tasks() -> Result<()> {
        let store = InMemoryStore::new();
        let mut run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        // Set task to RetryWait with retry_at in the past
        let task = run.get_task_mut(&task_id).unwrap();
        task.state = TaskState::RetryWait;
        task.retry_at = Some(Utc::now() - chrono::Duration::seconds(10));

        store.save_run(&run).await?;

        // Should be eligible for retry
        let eligible = store.get_retry_eligible_tasks(&run_id, Utc::now()).await?;
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0], task_id);

        Ok(())
    }

    #[tokio::test]
    async fn get_retry_eligible_tasks_not_due() -> Result<()> {
        let store = InMemoryStore::new();
        let mut run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        // Set task to RetryWait with retry_at in the future
        let task = run.get_task_mut(&task_id).unwrap();
        task.state = TaskState::RetryWait;
        task.retry_at = Some(Utc::now() + chrono::Duration::seconds(60));

        store.save_run(&run).await?;

        // Should NOT be eligible yet
        let eligible = store.get_retry_eligible_tasks(&run_id, Utc::now()).await?;
        assert!(eligible.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn get_ready_tasks_by_tenant() -> Result<()> {
        let store = InMemoryStore::new();
        let mut run = create_test_run()?;
        let run_id = run.id;
        let task_id = run.task_executions[0].task_id;

        // Set task to Ready
        let task = run.get_task_mut(&task_id).unwrap();
        task.state = TaskState::Ready;

        store.save_run(&run).await?;

        let by_tenant = store.get_ready_tasks_by_tenant(&run_id).await?;
        assert_eq!(by_tenant.len(), 1);
        assert_eq!(by_tenant[0].0, "test-tenant");
        assert_eq!(by_tenant[0].1.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn get_ready_tasks_by_tenant_orders_by_priority_and_key() -> Result<()> {
        let store = InMemoryStore::new();
        let (mut run, low_priority_task, high_priority_task, mid_priority_task) =
            create_priority_run()?;
        let run_id = run.id;

        for task_id in [low_priority_task, high_priority_task, mid_priority_task] {
            run.get_task_mut(&task_id)
                .unwrap()
                .transition_to(TaskState::Ready)?;
        }

        store.save_run(&run).await?;

        let by_tenant = store.get_ready_tasks_by_tenant(&run_id).await?;
        assert_eq!(by_tenant.len(), 1);
        let ready_ids: Vec<TaskId> = by_tenant[0].1.iter().map(|t| t.task_id).collect();

        assert_eq!(
            ready_ids,
            vec![high_priority_task, mid_priority_task, low_priority_task]
        );

        Ok(())
    }

    #[tokio::test]
    async fn run_count() -> Result<()> {
        let store = InMemoryStore::new();
        assert_eq!(store.run_count()?, 0);

        let run1 = create_test_run()?;
        store.save_run(&run1).await?;
        assert_eq!(store.run_count()?, 1);

        let run2 = create_test_run()?;
        store.save_run(&run2).await?;
        assert_eq!(store.run_count()?, 2);

        Ok(())
    }
}
