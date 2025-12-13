//! # arco-flow
//!
//! Orchestration engine for the Arco serverless lakehouse infrastructure.
//!
//! This crate implements the orchestration domain, providing:
//!
//! - **Deterministic Planning**: Reproducible execution plans from asset definitions
//! - **DAG Scheduling**: Dependency-aware task scheduling with parallelism
//! - **State Machine**: Reliable run state management with recovery
//! - **Execution Tracking**: Full observability of run progress and outcomes
//!
//! ## Core Concepts
//!
//! - **Plan**: A deterministic specification of what will execute, generated
//!   from asset definitions and their dependencies
//! - **Run**: An execution of a plan, capturing inputs, outputs, and timing
//! - **Task**: A unit of work within a run, typically one asset materialization
//!
//! ## Guarantees
//!
//! - **Deterministic**: Same inputs always produce the same plan
//! - **Replayable**: Any historical run can be replayed for debugging
//! - **Explainable**: Every execution decision can be traced to its cause
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_flow::{Planner, Scheduler};
//!
//! // Generate a deterministic plan
//! let plan = Planner::new(assets).plan()?;
//!
//! // Execute with dependency-aware scheduling
//! let run = Scheduler::new(plan).execute().await?;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]

pub mod plan;
pub mod run;
pub mod scheduler;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::plan::Plan;
    pub use crate::run::Run;
    pub use crate::scheduler::Scheduler;
}
