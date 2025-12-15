//! Error types for the orchestration domain.

use arco_core::{RunId, TaskId};

/// The result type used throughout arco-flow.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in orchestration operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A cycle was detected in the dependency graph.
    #[error("cycle detected in dependency graph: {cycle:?}")]
    CycleDetected {
        /// The cycle path (asset keys or task IDs).
        cycle: Vec<String>,
    },

    /// A task was not found in the plan or run.
    #[error("task not found: {task_id}")]
    TaskNotFound {
        /// The task ID that was not found.
        task_id: TaskId,
    },

    /// A run was not found.
    #[error("run not found: {run_id}")]
    RunNotFound {
        /// The run ID that was not found.
        run_id: RunId,
    },

    /// An invalid state transition was attempted.
    #[error("invalid state transition: {from} -> {to} ({reason})")]
    InvalidStateTransition {
        /// The current state.
        from: String,
        /// The attempted target state.
        to: String,
        /// The reason the transition is invalid.
        reason: String,
    },

    /// A dependency was not found.
    #[error("dependency not found: {asset_key}")]
    DependencyNotFound {
        /// The asset key of the missing dependency.
        asset_key: String,
    },

    /// A DAG node was not found (internal graph operation error).
    #[error("DAG node not found: {node}")]
    DagNodeNotFound {
        /// The node identifier (index or value).
        node: String,
    },

    /// Plan generation failed.
    #[error("plan generation failed: {message}")]
    PlanGenerationFailed {
        /// Description of the failure.
        message: String,
    },

    /// Task execution failed.
    #[error("task execution failed: {message}")]
    TaskExecutionFailed {
        /// Description of the failure.
        message: String,
    },

    /// A storage operation failed.
    #[error("storage error: {message}")]
    Storage {
        /// Description of the storage failure.
        message: String,
        /// The underlying cause, if any.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A serialization error occurred.
    #[error("serialization error: {message}")]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// An error from arco-core.
    #[error("core error: {0}")]
    Core(#[from] arco_core::error::Error),
}

impl Error {
    /// Creates a new storage error.
    #[must_use]
    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a new storage error with a source.
    #[must_use]
    pub fn storage_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Storage {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    #[test]
    fn plan_error_display() {
        let err = Error::CycleDetected {
            cycle: vec!["a".into(), "b".into(), "a".into()],
        };
        assert!(err.to_string().contains("cycle detected"));
    }

    #[test]
    fn task_error_display() {
        let err = Error::TaskNotFound {
            task_id: TaskId::generate(),
        };
        assert!(err.to_string().contains("task not found"));
    }

    #[test]
    fn state_transition_error_display() {
        let err = Error::InvalidStateTransition {
            from: "pending".into(),
            to: "completed".into(),
            reason: "must transition through running first".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("pending"));
        assert!(msg.contains("completed"));
        assert!(msg.contains("must transition"));
    }

    #[test]
    fn storage_error_with_source() {
        let source = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = Error::storage_with_source("failed to read file", source);
        assert!(err.to_string().contains("storage error"));
        assert!(StdError::source(&err).is_some());
    }
}
