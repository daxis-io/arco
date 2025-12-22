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
//! ```rust,no_run
//! use arco_core::{AssetId, TaskId};
//! use arco_flow::error::Result;
//! use arco_flow::outbox::InMemoryOutbox;
//! use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
//! use arco_flow::run::RunTrigger;
//! use arco_flow::scheduler::Scheduler;
//! use arco_flow::task_key::TaskOperation;
//!
//! # fn main() -> Result<()> {
//! // Generate a deterministic plan with one task.
//! let task = TaskSpec {
//!     task_id: TaskId::generate(),
//!     asset_id: AssetId::generate(),
//!     asset_key: AssetKey::new("raw", "events"),
//!     operation: TaskOperation::Materialize,
//!     partition_key: None,
//!     upstream_task_ids: Vec::new(),
//!     stage: 0,
//!     priority: 0,
//!     resources: ResourceRequirements::default(),
//! };
//!
//! let plan = PlanBuilder::new("tenant", "workspace")
//!     .add_task(task)
//!     .build()?;
//!
//! // Create a run and emit events to an outbox.
//! let scheduler = Scheduler::new(plan);
//! let mut outbox = InMemoryOutbox::new();
//! let _run = scheduler.create_run(RunTrigger::manual("user@example.com"), &mut outbox);
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]

// Internal modules - not exposed in public API.
pub(crate) mod dag;

pub mod dispatch;

/// Event-driven orchestration module (ADR-020).
pub mod orchestration;
pub mod error;
pub mod events;
pub mod leader;
pub mod metrics;
pub mod outbox;
pub mod plan;
pub mod quota;
pub mod run;
pub mod runner;
pub mod scheduler;
pub mod store;
pub mod task;
pub mod task_key;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::dispatch::{EnqueueResult, TaskEnvelope, TaskQueue};
    pub use crate::error::{Error, Result};
    pub use crate::leader::{LeaderElector, LeadershipResult, RenewalResult};
    pub use crate::metrics::FlowMetrics;
    pub use crate::outbox::{EventSink, InMemoryOutbox, LedgerWriter};
    pub use crate::plan::{Plan, PlanBuilder, TaskSpec};
    pub use crate::quota::{QuotaDecision, QuotaManager, TenantQuota};
    pub use crate::run::{Run, RunState};
    pub use crate::runner::{RunContext, Runner, TaskResult};
    pub use crate::scheduler::Scheduler;
    pub use crate::store::{CasResult, Store};
    pub use crate::task::{TaskExecution, TaskState};
    pub use crate::task_key::{TaskKey, TaskOperation};
}
