//! Backfill event types for bulk partition materialization.
//!
//! Backfills divide partition ranges into chunks, each producing a separate run.
//! This module defines the supporting types for backfill events.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Backfill lifecycle state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackfillState {
    /// Backfill created but not yet started.
    Pending,
    /// Backfill is actively processing chunks.
    Running,
    /// Backfill is paused (active chunks will complete).
    Paused,
    /// All chunks completed successfully.
    Succeeded,
    /// At least one chunk failed after retries.
    Failed,
    /// Backfill was cancelled.
    Cancelled,
}

impl BackfillState {
    /// Returns true if this is a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// Validates a state transition.
    #[must_use]
    pub fn is_valid_transition(from: Self, to: Self) -> bool {
        matches!(
            (from, to),
            (Self::Pending | Self::Paused, Self::Running)
                | (
                    Self::Pending | Self::Running | Self::Paused,
                    Self::Cancelled
                )
                | (Self::Running, Self::Paused | Self::Succeeded | Self::Failed)
        )
    }
}

/// Selector for backfill partitions.
///
/// Per P0-6, `BackfillCreated` uses a compact selector rather than
/// embedding the full partition list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionSelector {
    /// Time-based range (e.g., "2025-01-01" to "2025-12-31").
    Range {
        /// Start of partition range (inclusive).
        start: String,
        /// End of partition range (inclusive).
        end: String,
    },
    /// Explicit list of partition keys.
    Explicit {
        /// Partition keys to include.
        partition_keys: Vec<String>,
    },
    /// Filter-based selection (e.g., region = "us-*").
    Filter {
        /// Filter expressions.
        filters: HashMap<String, String>,
    },
}

/// Backfill chunk state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChunkState {
    /// Chunk is pending (not yet planned).
    Pending,
    /// Chunk has been planned (run requested).
    Planned,
    /// Chunk run is in progress.
    Running,
    /// Chunk run succeeded.
    Succeeded,
    /// Chunk run failed.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backfill_state_transitions() {
        // Valid transitions
        assert!(BackfillState::is_valid_transition(
            BackfillState::Pending,
            BackfillState::Running
        ));
        assert!(BackfillState::is_valid_transition(
            BackfillState::Running,
            BackfillState::Paused
        ));
        assert!(BackfillState::is_valid_transition(
            BackfillState::Paused,
            BackfillState::Running
        ));
        assert!(BackfillState::is_valid_transition(
            BackfillState::Running,
            BackfillState::Succeeded
        ));
        assert!(BackfillState::is_valid_transition(
            BackfillState::Running,
            BackfillState::Cancelled
        ));

        // Invalid transitions
        assert!(!BackfillState::is_valid_transition(
            BackfillState::Succeeded,
            BackfillState::Running
        ));
        assert!(!BackfillState::is_valid_transition(
            BackfillState::Failed,
            BackfillState::Running
        ));
        assert!(!BackfillState::is_valid_transition(
            BackfillState::Cancelled,
            BackfillState::Running
        ));
    }

    #[test]
    fn test_backfill_terminal_states() {
        assert!(BackfillState::Succeeded.is_terminal());
        assert!(BackfillState::Failed.is_terminal());
        assert!(BackfillState::Cancelled.is_terminal());
        assert!(!BackfillState::Pending.is_terminal());
        assert!(!BackfillState::Running.is_terminal());
        assert!(!BackfillState::Paused.is_terminal());
    }

    #[test]
    fn test_partition_selector_serialization() {
        let range = PartitionSelector::Range {
            start: "2025-01-01".into(),
            end: "2025-12-31".into(),
        };
        let json = serde_json::to_string(&range).unwrap();
        assert!(json.contains("range"));
        assert!(json.contains("2025-01-01"));

        let explicit = PartitionSelector::Explicit {
            partition_keys: vec!["2025-01-01".into(), "2025-01-02".into()],
        };
        let json = serde_json::to_string(&explicit).unwrap();
        assert!(json.contains("explicit"));
    }
}
