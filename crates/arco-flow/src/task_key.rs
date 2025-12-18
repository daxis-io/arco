//! Semantic task identity for deterministic ordering and deduplication.
//!
//! `TaskKey` provides a stable identity for tasks based on their semantic
//! meaning (asset + partition + operation), independent of generated IDs.

use arco_core::partition::PartitionKey;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::plan::AssetKey;

/// The type of operation a task performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOperation {
    /// Produce asset data (default).
    Materialize,
    /// Run data quality checks.
    Check,
    /// Historical data backfill chunk.
    Backfill,
}

impl Default for TaskOperation {
    fn default() -> Self {
        Self::Materialize
    }
}

impl std::fmt::Display for TaskOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialize => write!(f, "materialize"),
            Self::Check => write!(f, "check"),
            Self::Backfill => write!(f, "backfill"),
        }
    }
}

impl PartialOrd for TaskOperation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskOperation {
    fn cmp(&self, other: &Self) -> Ordering {
        // Stable ordering: Materialize < Check < Backfill
        fn rank(op: TaskOperation) -> u8 {
            match op {
                TaskOperation::Materialize => 0,
                TaskOperation::Check => 1,
                TaskOperation::Backfill => 2,
            }
        }
        rank(*self).cmp(&rank(*other))
    }
}

/// Semantic identity for a task.
///
/// Used for:
/// - Deterministic topological sort tie-breaking
/// - Stable task ordering before fingerprinting
/// - Duplicate detection (same `TaskKey` = same logical task)
///
/// Does NOT include `task_id` (which is generated and non-deterministic).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskKey {
    /// The asset this task operates on.
    pub asset_key: AssetKey,

    /// Typed partition key (if partitioned).
    /// Uses `arco_core::partition::PartitionKey` for type safety.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<PartitionKey>,

    /// The operation type.
    pub operation: TaskOperation,
}

impl TaskKey {
    /// Creates a new `TaskKey` for a non-partitioned asset.
    #[must_use]
    pub fn new(asset_key: AssetKey, operation: TaskOperation) -> Self {
        Self {
            asset_key,
            partition_key: None,
            operation,
        }
    }

    /// Creates a new `TaskKey` with a partition key.
    #[must_use]
    pub fn with_partition(
        asset_key: AssetKey,
        partition_key: PartitionKey,
        operation: TaskOperation,
    ) -> Self {
        Self {
            asset_key,
            partition_key: Some(partition_key),
            operation,
        }
    }

    /// Returns the canonical string representation for sorting/hashing.
    ///
    /// Format: `{namespace}/{name}[{partition}]:{operation}`
    /// - Uses `/` separator per ADR-011 (resolves doc drift)
    /// - Partition uses `PartitionKey::canonical_string()` format
    /// - Includes operation to distinguish Materialize vs Check for same asset
    #[must_use]
    pub fn canonical_string(&self) -> String {
        let base = self.partition_key.as_ref().map_or_else(
            || format!("{}/{}", self.asset_key.namespace, self.asset_key.name),
            |pk| {
                format!(
                    "{}/{}[{}]",
                    self.asset_key.namespace,
                    self.asset_key.name,
                    pk.canonical_string()
                )
            },
        );
        format!("{base}:{}", self.operation)
    }
}

impl PartialOrd for TaskKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by: asset_key, then partition_key, then operation
        self.asset_key
            .namespace
            .cmp(&other.asset_key.namespace)
            .then_with(|| self.asset_key.name.cmp(&other.asset_key.name))
            .then_with(|| self.partition_key.cmp(&other.partition_key))
            .then_with(|| self.operation.cmp(&other.operation))
    }
}

impl std::fmt::Display for TaskKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // canonical_string() already includes operation, so just use it directly
        write!(f, "{}", self.canonical_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::partition::ScalarValue;

    /// Helper to create a partition key with a date dimension.
    fn date_partition(date: &str) -> PartitionKey {
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date(date.into()));
        pk
    }

    #[test]
    fn task_operation_ordering_is_stable() {
        assert!(TaskOperation::Materialize < TaskOperation::Check);
        assert!(TaskOperation::Check < TaskOperation::Backfill);
    }

    #[test]
    fn task_operation_default_is_materialize() {
        assert_eq!(TaskOperation::default(), TaskOperation::Materialize);
    }

    #[test]
    fn task_key_ordering_is_deterministic() {
        let key_a = TaskKey {
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            operation: TaskOperation::Materialize,
        };
        let key_b = TaskKey {
            asset_key: AssetKey::new("staging", "events"),
            partition_key: None,
            operation: TaskOperation::Materialize,
        };

        // raw < staging (lexicographic)
        assert!(key_a < key_b);
    }

    #[test]
    fn task_key_with_partition_orders_correctly() {
        let key_a = TaskKey::with_partition(
            AssetKey::new("raw", "events"),
            date_partition("2025-01-01"),
            TaskOperation::Materialize,
        );
        let key_b = TaskKey::with_partition(
            AssetKey::new("raw", "events"),
            date_partition("2025-01-02"),
            TaskOperation::Materialize,
        );

        // Same asset, different partition: 01 < 02
        assert!(key_a < key_b);
    }

    #[test]
    fn task_key_operation_breaks_ties() {
        let materialize = TaskKey::new(AssetKey::new("raw", "events"), TaskOperation::Materialize);
        let check = TaskKey::new(AssetKey::new("raw", "events"), TaskOperation::Check);

        // Same asset: Materialize < Check
        assert!(materialize < check);
    }

    #[test]
    fn task_key_display_format() {
        let key = TaskKey::with_partition(
            AssetKey::new("raw", "events"),
            date_partition("2025-01-01"),
            TaskOperation::Materialize,
        );

        // Format: namespace/name[partition]:operation (per ADR-011)
        // Partition uses typed PartitionKey format: date=d:2025-01-01
        assert_eq!(key.to_string(), "raw/events[date=d:2025-01-01]:materialize");
    }

    #[test]
    fn task_key_canonical_string_without_partition() {
        let key = TaskKey::new(AssetKey::new("staging", "cleaned"), TaskOperation::Check);

        // Includes operation to distinguish Materialize vs Check
        assert_eq!(key.canonical_string(), "staging/cleaned:check");
    }

    #[test]
    fn task_key_operation_included_in_canonical() {
        let materialize = TaskKey::new(AssetKey::new("raw", "events"), TaskOperation::Materialize);
        let check = TaskKey::new(AssetKey::new("raw", "events"), TaskOperation::Check);

        // Same asset, different operation = different canonical string
        assert_ne!(materialize.canonical_string(), check.canonical_string());
        assert_eq!(materialize.canonical_string(), "raw/events:materialize");
        assert_eq!(check.canonical_string(), "raw/events:check");
    }
}
