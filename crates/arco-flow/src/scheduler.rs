//! Dependency-aware task scheduling.
//!
//! The scheduler executes plans with:
//!
//! - **Parallelism**: Independent tasks run concurrently
//! - **Dependency ordering**: Tasks wait for their dependencies
//! - **Fault tolerance**: Failed tasks are tracked, not fatal

use crate::plan::Plan;
use crate::run::{Run, RunState};
use arco_core::{Result, RunId};

/// Scheduler for executing plans.
#[derive(Debug)]
pub struct Scheduler {
    plan: Plan,
}

impl Scheduler {
    /// Creates a new scheduler for the given plan.
    #[must_use]
    pub fn new(plan: Plan) -> Self {
        Self { plan }
    }

    /// Returns the plan being scheduled.
    #[must_use]
    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Executes the plan and returns the completed run.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduler cannot execute the plan.
    pub async fn execute(&self) -> Result<Run> {
        let id = RunId::generate();
        let mut run = Run::new(id);

        run.started_at = Some(chrono::Utc::now());
        run.state = RunState::Running;

        // TODO: Implement actual task execution
        // For now, immediately mark as succeeded
        run.state = RunState::Succeeded;
        run.completed_at = Some(chrono::Utc::now());

        Ok(run)
    }
}
