//! Planning snapshot provider interfaces.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::planning::RunIntent;

/// Token naming the planning snapshot used by a compiler.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PlanningSnapshotToken(String);

impl PlanningSnapshotToken {
    /// Creates a planning snapshot token.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the encoded token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PlanningSnapshotToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Snapshot of planning inputs available to a compiler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanningSnapshot {
    /// Token naming this planning snapshot.
    pub token: PlanningSnapshotToken,
}

impl PlanningSnapshot {
    /// Creates a planning snapshot.
    #[must_use]
    pub fn new(token: PlanningSnapshotToken) -> Self {
        Self { token }
    }
}

/// Provides planning snapshots for run intents.
pub trait PlanningSnapshotProvider {
    /// Returns the planning snapshot to use for `intent`.
    fn snapshot_for_intent(&self, intent: &RunIntent) -> PlanningSnapshot;
}

/// Current in-process compatibility snapshot provider.
#[derive(Debug, Clone, Default)]
pub struct CurrentInProcessPlanningSnapshotProvider;

impl CurrentInProcessPlanningSnapshotProvider {
    /// Snapshot token for the current in-process compatibility planner.
    pub const TOKEN: &'static str = "current_in_process:run_bridge_compatibility";
}

impl PlanningSnapshotProvider for CurrentInProcessPlanningSnapshotProvider {
    fn snapshot_for_intent(&self, _intent: &RunIntent) -> PlanningSnapshot {
        PlanningSnapshot::new(PlanningSnapshotToken::new(Self::TOKEN))
    }
}
