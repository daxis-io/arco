//! Pluggable storage for orchestration state.
//!
//! The Store trait defines the persistence layer for runs and tasks.
//! Leader election is handled separately by [`crate::leader::LeaderElector`].
//!
//! ## Design Principles
//!
//! - **CAS semantics**: State transitions use compare-and-swap to prevent races
//! - **Separation of concerns**: Storage is independent of leader election
//! - **Testability**: In-memory implementation for testing, Postgres for production

pub mod memory;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use arco_core::{RunId, TaskId};

use crate::error::Result;
use crate::run::Run;
use crate::task::{TaskExecution, TaskState, TransitionReason};

/// Result of a compare-and-swap operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    /// Operation succeeded.
    Success,
    /// Entity not found.
    NotFound,
    /// State didn't match expected value.
    StateMismatch {
        /// The actual state that was found.
        actual: TaskState,
    },
    /// Version conflict (concurrent modification).
    VersionConflict {
        /// The actual version that was found.
        actual: u64,
    },
}

impl CasResult {
    /// Returns true if the operation succeeded.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    /// Returns true if the entity was not found.
    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }
}

/// Storage abstraction for orchestration state.
///
/// Implementations must provide:
/// - Durability appropriate for the deployment (in-memory for tests, Postgres for prod)
/// - CAS semantics for state transitions
/// - Efficient queries for scheduler operation
///
/// ## CAS Semantics
///
/// The `cas_task_state` method is the core primitive for distributed correctness:
/// - Prevents double-dispatch (two schedulers dispatching the same task)
/// - Ensures exactly-once state transitions
/// - Enables optimistic concurrency control
///
/// ## Thread Safety
///
/// All methods are `Send + Sync` to support concurrent access from multiple
/// scheduler tasks.
#[async_trait]
pub trait Store: Send + Sync {
    // --- Run Operations ---

    /// Gets a run by ID.
    ///
    /// Returns `None` if the run does not exist.
    async fn get_run(&self, run_id: &RunId) -> Result<Option<Run>>;

    /// Saves a run (insert or update).
    ///
    /// This is a full replacement of the run state. For concurrent updates,
    /// use `cas_task_state` for individual task transitions.
    async fn save_run(&self, run: &Run) -> Result<()>;

    // --- Task State Operations (CAS) ---

    /// Atomically transitions task state if current state matches expected.
    ///
    /// This is the core primitive for distributed correctness:
    /// - Prevents double-dispatch
    /// - Ensures exactly-once state transitions
    ///
    /// # Returns
    ///
    /// - `CasResult::Success` if the transition was applied
    /// - `CasResult::NotFound` if the run or task doesn't exist
    /// - `CasResult::StateMismatch` if the current state doesn't match expected
    async fn cas_task_state(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        expected_state: TaskState,
        target_state: TaskState,
        reason: TransitionReason,
    ) -> Result<CasResult>;

    // --- Scheduler Query Operations ---

    /// Gets all tasks in a specific state for a run.
    async fn get_tasks_by_state(
        &self,
        run_id: &RunId,
        state: TaskState,
    ) -> Result<Vec<TaskExecution>>;

    /// Gets tasks that are zombies (missed heartbeat/dispatch-ack deadlines).
    ///
    /// Zombie detection uses the two-timeout model:
    /// - Dispatched tasks: `dispatch_ack_timeout` (typically 30s)
    /// - Running tasks: `heartbeat_timeout` (typically 60s)
    async fn get_zombie_tasks(&self, run_id: &RunId, now: DateTime<Utc>) -> Result<Vec<TaskId>>;

    /// Gets tasks whose retry timer has expired.
    ///
    /// These are tasks in `RetryWait` state where `retry_at <= now`.
    async fn get_retry_eligible_tasks(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>>;

    /// Gets READY tasks grouped by tenant with deterministic ordering.
    ///
    /// Returns `(tenant_id, Vec<TaskExecution>)` pairs for fair scheduling.
    /// Tasks within each tenant are sorted by priority (lower = higher priority),
    /// then by task key (ascending) for deterministic dispatch order.
    async fn get_ready_tasks_by_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<(String, Vec<TaskExecution>)>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_result_is_success() {
        assert!(CasResult::Success.is_success());
        assert!(!CasResult::NotFound.is_success());
        assert!(!CasResult::StateMismatch {
            actual: TaskState::Running
        }
        .is_success());
    }

    #[test]
    fn cas_result_is_not_found() {
        assert!(CasResult::NotFound.is_not_found());
        assert!(!CasResult::Success.is_not_found());
    }
}
