//! Deterministic execution planning.
//!
//! Plans are generated from asset definitions and represent exactly
//! what will execute. Plans are:
//!
//! - **Deterministic**: Same inputs always produce the same plan
//! - **Serializable**: Can be stored and compared for debugging
//! - **Explainable**: Every task inclusion can be traced to a reason

use arco_core::AssetId;
use serde::{Deserialize, Serialize};

/// A deterministic execution plan.
///
/// Generated from asset definitions, a plan specifies exactly
/// which tasks will execute and in what order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// The tasks to execute, in dependency order.
    pub tasks: Vec<PlannedTask>,
}

impl Plan {
    /// Creates a new empty plan.
    #[must_use]
    pub const fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    /// Returns the number of tasks in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns true if the plan has no tasks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

/// A single task within a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedTask {
    /// The asset this task will materialize.
    pub asset_id: AssetId,

    /// IDs of tasks that must complete before this one.
    pub dependencies: Vec<AssetId>,

    /// Human-readable reason this task is included.
    pub reason: String,
}
