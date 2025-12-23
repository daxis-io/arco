//! Commit-related types for the Iceberg REST write path.
//!
//! This module contains types for the commit table endpoint including:
//! - Update requirements for optimistic concurrency
//! - Table updates (schema changes, snapshots, etc.)
//! - Request/response types
//!
//! The `CommitKey` type is in the `ids` module as it's also used elsewhere.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Iceberg table update requirement for optimistic concurrency.
///
/// Requirements are checked before applying updates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UpdateRequirement {
    /// Assert that the table UUID matches.
    AssertTableUuid {
        /// Expected table UUID.
        uuid: Uuid,
    },

    /// Assert that a ref points to a specific snapshot.
    AssertRefSnapshotId {
        /// Reference name (e.g., "main").
        #[serde(rename = "ref")]
        ref_name: String,
        /// Expected snapshot ID (null means ref should not exist).
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },

    /// Assert the last assigned column ID.
    AssertLastAssignedFieldId {
        /// Expected last assigned field ID.
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },

    /// Assert the current schema ID.
    AssertCurrentSchemaId {
        /// Expected current schema ID.
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },

    /// Assert the last assigned partition ID.
    AssertLastAssignedPartitionId {
        /// Expected last assigned partition ID.
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },

    /// Assert the default spec ID.
    AssertDefaultSpecId {
        /// Expected default spec ID.
        #[serde(rename = "default-spec-id")]
        default_spec_id: i32,
    },

    /// Assert the default sort order ID.
    AssertDefaultSortOrderId {
        /// Expected default sort order ID.
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_requirement_deserialization() {
        let json = r#"{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 123}"#;
        let req: UpdateRequirement = serde_json::from_str(json).expect("deserialize");
        assert!(matches!(req, UpdateRequirement::AssertRefSnapshotId { .. }));
    }

    #[test]
    fn test_assert_table_uuid() {
        let json = r#"{"type": "assert-table-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"}"#;
        let req: UpdateRequirement = serde_json::from_str(json).expect("deserialize");
        if let UpdateRequirement::AssertTableUuid { uuid } = req {
            assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("wrong variant");
        }
    }
}
