//! Task execution runner trait and implementations.
//!
//! The runner is responsible for executing individual tasks,
//! typically by delegating to worker processes or cloud functions.

use async_trait::async_trait;

use arco_core::RunId;

use crate::plan::TaskSpec;
use crate::task::{TaskError, TaskOutput};

/// Context for a task execution.
#[derive(Debug, Clone)]
pub struct RunContext {
    /// Tenant scope.
    pub tenant_id: String,
    /// Workspace scope.
    pub workspace_id: String,
    /// Run this task belongs to.
    pub run_id: RunId,
}

/// Result of a task execution.
#[derive(Debug)]
pub enum TaskResult {
    /// Task completed successfully.
    Succeeded(TaskOutput),
    /// Task failed with an error.
    Failed(TaskError),
    /// Task was cancelled.
    Cancelled,
}

impl TaskResult {
    /// Returns true if the task succeeded.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    /// Returns the output if succeeded.
    #[must_use]
    pub const fn output(&self) -> Option<&TaskOutput> {
        match self {
            Self::Succeeded(output) => Some(output),
            _ => None,
        }
    }

    /// Returns the error if failed.
    #[must_use]
    pub const fn error(&self) -> Option<&TaskError> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }
}

/// Trait for task execution.
///
/// Implementations can execute tasks locally, via cloud functions,
/// or through any other execution mechanism.
#[async_trait]
pub trait Runner: Send + Sync {
    /// Executes a task and returns the result.
    ///
    /// The implementation should:
    /// 1. Set up the execution environment
    /// 2. Execute the asset computation
    /// 3. Capture outputs and metrics
    /// 4. Return success with output or failure with error
    async fn run(&self, context: &RunContext, task: &TaskSpec) -> TaskResult;
}

/// A no-op runner for testing that immediately succeeds.
#[derive(Debug, Default)]
pub struct NoOpRunner;

#[async_trait]
impl Runner for NoOpRunner {
    async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
        TaskResult::Succeeded(TaskOutput {
            materialization_id: arco_core::MaterializationId::generate(),
            files: vec![],
            row_count: 0,
            byte_size: 0,
        })
    }
}

/// A runner that always fails with a configurable error.
#[derive(Debug)]
pub struct FailingRunner {
    error: TaskError,
}

impl FailingRunner {
    /// Creates a new failing runner with the given error.
    #[must_use]
    pub const fn new(error: TaskError) -> Self {
        Self { error }
    }
}

#[async_trait]
impl Runner for FailingRunner {
    async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
        TaskResult::Failed(self.error.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, ResourceRequirements, TaskSpec};
    use crate::task::TaskErrorCategory;
    use crate::task_key::TaskOperation;
    use arco_core::{AssetId, MaterializationId, TaskId};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingRunner {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Runner for CountingRunner {
        async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
            self.count.fetch_add(1, Ordering::SeqCst);
            TaskResult::Succeeded(TaskOutput {
                materialization_id: MaterializationId::generate(),
                files: vec![],
                row_count: 100,
                byte_size: 1024,
            })
        }
    }

    #[tokio::test]
    async fn runner_executes_task() {
        let count = Arc::new(AtomicUsize::new(0));
        let runner = CountingRunner {
            count: count.clone(),
        };

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;

        assert!(matches!(result, TaskResult::Succeeded(_)));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn noop_runner_succeeds() {
        let runner = NoOpRunner;

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn failing_runner_fails() {
        let error = TaskError::new(TaskErrorCategory::UserCode, "expected failure");
        let runner = FailingRunner::new(error);

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;
        assert!(result.error().is_some());
    }
}
