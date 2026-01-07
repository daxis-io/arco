//! Commit-related types for the Iceberg REST write path.
//!
//! This module contains types for the commit table endpoint including:
//! - Update requirements for optimistic concurrency
//! - Table updates (schema changes, snapshots, etc.)
//! - Request/response types
//!
//! The `CommitKey` type is in the `ids` module as it's also used elsewhere.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use super::table::{PartitionSpec, Schema, Snapshot, SortOrder, TableIdent};

/// Iceberg table update requirement for optimistic concurrency.
///
/// Requirements are checked before applying updates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
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

/// Iceberg table update action.
///
/// Updates modify table metadata atomically.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    /// Assign a new UUID to the table.
    AssignUuid {
        /// New UUID for the table.
        uuid: Uuid,
    },

    /// Upgrade the format version.
    UpgradeFormatVersion {
        /// Target format version.
        #[serde(rename = "format-version")]
        format_version: i32,
    },

    /// Add a new schema.
    AddSchema {
        /// The schema to add.
        schema: Schema,
        /// ID to assign (optional).
        #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
        last_column_id: Option<i32>,
    },

    /// Set the current schema.
    SetCurrentSchema {
        /// Schema ID to make current.
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },

    /// Add a new partition spec.
    AddPartitionSpec {
        /// The partition spec to add.
        spec: PartitionSpec,
    },

    /// Set the default partition spec.
    SetDefaultSpec {
        /// Spec ID to make default.
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },

    /// Add a new sort order.
    AddSortOrder {
        /// The sort order to add.
        #[serde(rename = "sort-order")]
        sort_order: SortOrder,
    },

    /// Set the default sort order.
    SetDefaultSortOrder {
        /// Sort order ID to make default.
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },

    /// Add a new snapshot.
    AddSnapshot {
        /// The snapshot to add.
        snapshot: Snapshot,
    },

    /// Set a snapshot reference (branch or tag).
    SetSnapshotRef {
        /// Reference name.
        #[serde(rename = "ref-name")]
        ref_name: String,
        /// Reference type ("branch" or "tag").
        #[serde(rename = "type")]
        ref_type: SnapshotRefType,
        /// Snapshot ID for the ref.
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        /// Max ref age in ms (branches only).
        #[serde(rename = "max-ref-age-ms", skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
        /// Max snapshot age in ms (branches only).
        #[serde(
            rename = "max-snapshot-age-ms",
            skip_serializing_if = "Option::is_none"
        )]
        max_snapshot_age_ms: Option<i64>,
        /// Min snapshots to keep (branches only).
        #[serde(
            rename = "min-snapshots-to-keep",
            skip_serializing_if = "Option::is_none"
        )]
        min_snapshots_to_keep: Option<i32>,
    },

    /// Remove a snapshot reference.
    RemoveSnapshotRef {
        /// Reference name to remove.
        #[serde(rename = "ref-name")]
        ref_name: String,
    },

    /// Remove snapshots by IDs.
    RemoveSnapshots {
        /// Snapshot IDs to remove.
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },

    /// Set table location.
    ///
    /// **Note:** Arco rejects this update (400 `BadRequest`) - Arco owns storage location.
    SetLocation {
        /// New location.
        location: String,
    },

    /// Set table properties.
    SetProperties {
        /// Properties to set.
        updates: HashMap<String, String>,
    },

    /// Remove table properties.
    RemoveProperties {
        /// Property keys to remove.
        removals: Vec<String>,
    },
}

impl TableUpdate {
    /// Returns true if this update is rejected by Arco governance guardrails.
    ///
    /// Per design doc Section 1.3:
    /// - `SetLocation` is always rejected (Arco owns storage location)
    /// - `SetProperties` with `arco.*` keys is rejected (reserved namespace, case-insensitive)
    /// - `RemoveProperties` with `arco.*` keys is rejected (reserved namespace, case-insensitive)
    #[must_use]
    pub fn is_rejected_by_guardrails(&self) -> Option<String> {
        fn is_reserved_key(key: &str) -> bool {
            key.get(..5)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("arco."))
        }

        match self {
            Self::SetLocation { .. } => Some(
                "SetLocationUpdate is rejected: Arco owns storage location".to_string(),
            ),
            Self::SetProperties { updates } => updates.keys().find(|k| is_reserved_key(k)).map(
                |key| {
                    format!(
                        "SetPropertiesUpdate with reserved key '{key}' is rejected: Arco owns the 'arco.' namespace"
                    )
                },
            ),
            Self::RemoveProperties { removals } => {
                removals.iter().find(|k| is_reserved_key(k)).map(|key| {
                    format!(
                        "RemovePropertiesUpdate with reserved key '{key}' is rejected: Arco owns the 'arco.' namespace"
                    )
                })
            }
            _ => None,
        }
    }
}

/// Snapshot reference type for `set-snapshot-ref`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    /// Mutable branch (e.g., "main").
    Branch,
    /// Immutable tag.
    Tag,
}

/// Request body for `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CommitTableRequest {
    /// Table identifier (optional in body, taken from URL path).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,

    /// Requirements that must be met before applying updates.
    #[serde(default)]
    pub requirements: Vec<UpdateRequirement>,

    /// Updates to apply atomically.
    #[serde(default)]
    pub updates: Vec<TableUpdate>,
}

impl CommitTableRequest {
    /// Checks all updates against governance guardrails.
    ///
    /// Returns the first rejection reason if any update is rejected.
    #[must_use]
    pub fn check_guardrails(&self) -> Option<String> {
        self.updates
            .iter()
            .find_map(TableUpdate::is_rejected_by_guardrails)
    }

    /// Starts building a commit request.
    #[must_use]
    pub fn builder() -> CommitTableRequestBuilder {
        CommitTableRequestBuilder::default()
    }
}

/// Builder for `CommitTableRequest`.
///
/// # Example
///
/// ```
/// use arco_iceberg::types::{CommitTableRequest, TableUpdate};
///
/// let request = CommitTableRequest::builder()
///     .add_update(TableUpdate::RemoveSnapshots { snapshot_ids: vec![1, 2] })
///     .build();
/// assert_eq!(request.updates.len(), 1);
/// ```
#[derive(Debug, Default, Clone)]
pub struct CommitTableRequestBuilder {
    identifier: Option<TableIdent>,
    requirements: Vec<UpdateRequirement>,
    updates: Vec<TableUpdate>,
}

impl CommitTableRequestBuilder {
    /// Creates a new commit request builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the table identifier.
    #[must_use]
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// Adds a single update requirement.
    #[must_use]
    pub fn add_requirement(mut self, requirement: UpdateRequirement) -> Self {
        self.requirements.push(requirement);
        self
    }

    /// Adds multiple update requirements.
    #[must_use]
    pub fn requirements(
        mut self,
        requirements: impl IntoIterator<Item = UpdateRequirement>,
    ) -> Self {
        self.requirements.extend(requirements);
        self
    }

    /// Adds a single table update.
    #[must_use]
    pub fn add_update(mut self, update: TableUpdate) -> Self {
        self.updates.push(update);
        self
    }

    /// Adds multiple table updates.
    #[must_use]
    pub fn updates(mut self, updates: impl IntoIterator<Item = TableUpdate>) -> Self {
        self.updates.extend(updates);
        self
    }

    /// Builds the commit request.
    #[must_use]
    pub fn build(self) -> CommitTableRequest {
        CommitTableRequest {
            identifier: self.identifier,
            requirements: self.requirements,
            updates: self.updates,
        }
    }
}

/// Response from `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CommitTableResponse {
    /// Location of the new metadata file.
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,

    /// Full table metadata (inline JSON).
    pub metadata: serde_json::Value,
}

/// Request body for `POST /v1/{prefix}/transactions/commit`.
///
/// Commits changes to multiple tables atomically.
///
/// # Arco Implementation
///
/// Arco only supports single-table commits through this endpoint. Requests with
/// more than one table change will return 406 Not Acceptable. This endpoint is
/// **not advertised** in `/v1/config` because true multi-table atomicity is not
/// supported.
///
/// Single-table requests are delegated to the same commit logic as the
/// per-table commit endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CommitTransactionRequest {
    /// Table changes to commit atomically.
    ///
    /// Each entry must include an `identifier` field. Arco requires exactly
    /// one entry; multiple entries will be rejected with 406.
    #[serde(rename = "table-changes")]
    pub table_changes: Vec<CommitTableRequest>,
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
        let json =
            r#"{"type": "assert-table-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"}"#;
        let req: UpdateRequirement = serde_json::from_str(json).expect("deserialize");
        if let UpdateRequirement::AssertTableUuid { uuid } = req {
            assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_table_update_add_snapshot() {
        let json = r#"{
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 12345,
                "timestamp-ms": 1234567890000,
                "manifest-list": "s3://bucket/manifests/snap-12345.avro",
                "summary": {"operation": "append"}
            }
        }"#;
        let update: TableUpdate = serde_json::from_str(json).expect("deserialize");
        assert!(matches!(update, TableUpdate::AddSnapshot { .. }));
    }

    #[test]
    fn test_table_update_set_snapshot_ref() {
        let json = r#"{
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "type": "branch",
            "snapshot-id": 12345
        }"#;
        let update: TableUpdate = serde_json::from_str(json).expect("deserialize");
        if let TableUpdate::SetSnapshotRef {
            ref_name,
            ref_type,
            snapshot_id,
            ..
        } = update
        {
            assert_eq!(ref_name, "main");
            assert_eq!(ref_type, SnapshotRefType::Branch);
            assert_eq!(snapshot_id, 12345);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_table_update_set_snapshot_ref_invalid_type() {
        let json = r#"{
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "type": "invalid",
            "snapshot-id": 12345
        }"#;
        let update: Result<TableUpdate, _> = serde_json::from_str(json);
        assert!(update.is_err());
    }

    #[test]
    fn test_guardrail_rejects_set_location() {
        let update = TableUpdate::SetLocation {
            location: "s3://new-bucket/table".to_string(),
        };
        assert!(update.is_rejected_by_guardrails().is_some());
    }

    #[test]
    fn test_guardrail_rejects_arco_properties() {
        let update = TableUpdate::SetProperties {
            updates: [("arco.lineage.source".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        };
        let rejection = update.is_rejected_by_guardrails().expect("rejected");
        assert!(rejection.contains("arco.lineage.source"));
    }

    #[test]
    fn test_guardrail_rejects_mixed_case_arco_properties() {
        let update = TableUpdate::SetProperties {
            updates: [("ArCo.lineage.source".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        };
        let rejection = update.is_rejected_by_guardrails().expect("rejected");
        assert!(rejection.contains("ArCo.lineage.source"));
    }

    #[test]
    fn test_guardrail_allows_normal_properties() {
        let update = TableUpdate::SetProperties {
            updates: [("write.format.default".to_string(), "parquet".to_string())]
                .into_iter()
                .collect(),
        };
        assert!(update.is_rejected_by_guardrails().is_none());
    }

    #[test]
    fn test_guardrail_rejects_remove_arco_properties() {
        let update = TableUpdate::RemoveProperties {
            removals: vec!["ARCO.governance.owner".to_string()],
        };
        let rejection = update.is_rejected_by_guardrails().expect("rejected");
        assert!(rejection.contains("ARCO.governance.owner"));
    }

    #[test]
    fn test_guardrail_allows_add_snapshot() {
        use super::super::table::Snapshot;

        let update = TableUpdate::AddSnapshot {
            snapshot: Snapshot {
                snapshot_id: 1,
                parent_snapshot_id: None,
                sequence_number: 1,
                timestamp_ms: 1234567890000,
                manifest_list: "s3://bucket/manifests/snap.avro".to_string(),
                summary: HashMap::new(),
                schema_id: Some(0),
            },
        };
        assert!(update.is_rejected_by_guardrails().is_none());
    }

    #[test]
    fn test_commit_table_request_deserialization() {
        let json = r#"{
            "identifier": {"namespace": ["sales"], "name": "orders"},
            "requirements": [
                {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 100}
            ],
            "updates": [
                {"action": "add-snapshot", "snapshot": {"snapshot-id": 101, "timestamp-ms": 1234567890000, "manifest-list": "s3://bucket/snap.avro"}}
            ]
        }"#;
        let req: CommitTableRequest = serde_json::from_str(json).expect("deserialize");
        let identifier = req.identifier.expect("identifier present");
        assert_eq!(identifier.name, "orders");
        assert_eq!(req.requirements.len(), 1);
        assert_eq!(req.updates.len(), 1);
    }

    #[test]
    fn test_commit_table_response_serialization() {
        let response = CommitTableResponse {
            metadata_location: "s3://bucket/metadata/00001.json".to_string(),
            metadata: serde_json::json!({"format-version": 2}),
        };
        let json = serde_json::to_string(&response).expect("serialize");
        assert!(json.contains("metadata-location"));
    }

    #[test]
    fn test_commit_table_request_builder() {
        let request = CommitTableRequest::builder()
            .identifier(TableIdent::simple("sales", "orders"))
            .add_requirement(UpdateRequirement::AssertCurrentSchemaId {
                current_schema_id: 0,
            })
            .add_update(TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![1, 2, 3],
            })
            .build();

        let identifier = request.identifier.expect("identifier");
        assert_eq!(identifier.name, "orders");
        assert_eq!(request.requirements.len(), 1);
        assert_eq!(request.updates.len(), 1);
    }
}
