//! Execution run tracking.
//!
//! A run represents a single execution of a plan, capturing:
//!
//! - **Inputs**: What data was read
//! - **Outputs**: What data was produced
//! - **Timing**: When each task started and completed
//! - **State**: Current status and any errors

use arco_core::RunId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A pipeline execution run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    /// Unique identifier for this run.
    pub id: RunId,

    /// Current state of the run.
    pub state: RunState,

    /// When the run was created.
    pub created_at: DateTime<Utc>,

    /// When the run started executing (if started).
    pub started_at: Option<DateTime<Utc>>,

    /// When the run completed (if completed).
    pub completed_at: Option<DateTime<Utc>>,
}

impl Run {
    /// Creates a new run with the given ID.
    #[must_use]
    pub fn new(id: RunId) -> Self {
        Self {
            id,
            state: RunState::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
        }
    }

    /// Returns true if the run is in a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            RunState::Succeeded | RunState::Failed | RunState::Cancelled
        )
    }
}

/// The state of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    /// Run is queued but not yet started.
    Pending,

    /// Run is currently executing.
    Running,

    /// Run completed successfully.
    Succeeded,

    /// Run failed with an error.
    Failed,

    /// Run was cancelled by user or system.
    Cancelled,
}
