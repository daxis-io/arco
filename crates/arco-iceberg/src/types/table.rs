//! Table-related request and response types.

use super::credentials::StorageCredential;
use super::ids::TableUuid;
use super::namespace::NamespaceIdent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Table identifier with namespace and name.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TableIdent {
    /// The namespace containing the table.
    pub namespace: NamespaceIdent,

    /// The table name.
    pub name: String,
}

impl TableIdent {
    /// Creates a new table identifier.
    #[must_use]
    pub fn new(namespace: NamespaceIdent, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }

    /// Creates a table identifier for a single-level namespace.
    #[must_use]
    pub fn simple(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: vec![namespace.into()],
            name: name.into(),
        }
    }
}

/// Response from `GET /v1/{prefix}/namespaces/{namespace}/tables`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ListTablesResponse {
    /// List of table identifiers.
    pub identifiers: Vec<TableIdent>,

    /// Token for fetching the next page of results.
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Query parameters for listing tables.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
pub struct ListTablesQuery {
    /// Token for pagination.
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,

    /// Maximum number of results to return.
    #[serde(rename = "pageSize")]
    pub page_size: Option<u32>,
}

/// Response from `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
///
/// Contains the full Iceberg table metadata.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LoadTableResponse {
    /// The location of the metadata file.
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,

    /// The table metadata (inline JSON).
    ///
    /// This is the parsed content of the metadata file.
    pub metadata: TableMetadata,

    /// Additional configuration for the table.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,

    /// Storage credentials for accessing table data.
    ///
    /// Only present when `X-Iceberg-Access-Delegation: vended-credentials` is requested.
    #[serde(
        rename = "storage-credentials",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

/// Iceberg table metadata.
///
/// This is a simplified version for Phase A; full metadata parsing
/// will be added as needed.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TableMetadata {
    /// Format version (1 or 2).
    #[serde(rename = "format-version")]
    pub format_version: i32,

    /// Unique table identifier.
    #[serde(rename = "table-uuid")]
    pub table_uuid: TableUuid,

    /// Table location (root path for data and metadata).
    pub location: String,

    /// Last sequence number assigned.
    #[serde(rename = "last-sequence-number")]
    pub last_sequence_number: i64,

    /// Last updated timestamp in milliseconds.
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,

    /// Last assigned column ID.
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,

    /// Current schema ID.
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,

    /// All schemas.
    pub schemas: Vec<Schema>,

    /// Current snapshot ID.
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,

    /// All snapshots.
    pub snapshots: Vec<Snapshot>,

    /// Snapshot log (history of current-snapshot-id changes).
    #[serde(rename = "snapshot-log")]
    pub snapshot_log: Vec<SnapshotLogEntry>,

    /// Metadata log (history of metadata files).
    #[serde(rename = "metadata-log")]
    pub metadata_log: Vec<MetadataLogEntry>,

    /// Table properties.
    pub properties: HashMap<String, String>,

    /// Default partition spec ID.
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,

    /// Partition specs.
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<PartitionSpec>,

    /// Highest assigned partition field ID.
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,

    /// Snapshot refs (branches and tags).
    #[serde(default)]
    pub refs: HashMap<String, SnapshotRefMetadata>,

    /// Default sort order ID.
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,

    /// Sort orders.
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<SortOrder>,
}

/// Iceberg schema.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Schema {
    /// Schema ID.
    #[serde(rename = "schema-id")]
    pub schema_id: i32,

    /// Schema type (always "struct" for table schemas).
    #[serde(rename = "type", default = "default_struct_type")]
    pub schema_type: String,

    /// Schema fields.
    #[serde(default)]
    pub fields: Vec<SchemaField>,
}

fn default_struct_type() -> String {
    "struct".to_string()
}

/// A field in a schema.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SchemaField {
    /// Unique field ID.
    pub id: i32,

    /// Field name.
    pub name: String,

    /// Whether the field is required.
    pub required: bool,

    /// Field data type.
    #[serde(rename = "type")]
    pub field_type: serde_json::Value, // Can be string or nested type
}

/// Iceberg snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Snapshot {
    /// Unique snapshot ID.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    /// Parent snapshot ID.
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,

    /// Sequence number.
    #[serde(rename = "sequence-number", default)]
    pub sequence_number: i64,

    /// Timestamp in milliseconds.
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,

    /// Manifest list location.
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,

    /// Snapshot summary.
    #[serde(default)]
    pub summary: HashMap<String, String>,

    /// Schema ID for this snapshot.
    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}

/// Entry in the snapshot log.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SnapshotLogEntry {
    /// Snapshot ID.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    /// Timestamp in milliseconds.
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

/// Entry in the metadata log.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct MetadataLogEntry {
    /// Metadata file location.
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,

    /// Timestamp in milliseconds.
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

/// Partition specification.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PartitionSpec {
    /// Spec ID.
    #[serde(rename = "spec-id")]
    pub spec_id: i32,

    /// Partition fields.
    #[serde(default)]
    pub fields: Vec<PartitionField>,
}

/// A field in a partition specification.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PartitionField {
    /// Unique field ID.
    #[serde(rename = "field-id")]
    pub field_id: i32,

    /// Source column ID.
    #[serde(rename = "source-id")]
    pub source_id: i32,

    /// Field name.
    pub name: String,

    /// Transform type (identity, bucket, truncate, etc.).
    pub transform: String,
}

/// Snapshot reference metadata (for refs map).
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SnapshotRefMetadata {
    /// Snapshot ID.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    /// Reference type.
    #[serde(rename = "type")]
    pub ref_type: String,
}

/// Sort order specification.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SortOrder {
    /// Order ID.
    #[serde(rename = "order-id")]
    pub order_id: i32,

    /// Sort fields.
    #[serde(default)]
    pub fields: Vec<SortField>,
}

/// A field in a sort order.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SortField {
    /// Source column ID.
    #[serde(rename = "source-id")]
    pub source_id: i32,

    /// Transform type.
    pub transform: String,

    /// Sort direction (asc or desc).
    pub direction: String,

    /// Null ordering (nulls-first or nulls-last).
    #[serde(rename = "null-order")]
    pub null_order: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_ident_simple() {
        let ident = TableIdent::simple("my_db", "my_table");
        assert_eq!(ident.namespace, vec!["my_db"]);
        assert_eq!(ident.name, "my_table");
    }

    #[test]
    fn test_list_tables_response() {
        let response = ListTablesResponse {
            identifiers: vec![
                TableIdent::simple("db", "table1"),
                TableIdent::simple("db", "table2"),
            ],
            next_page_token: None,
        };

        let json = serde_json::to_string(&response).expect("serialization failed");
        assert!(json.contains("\"identifiers\""));
        assert!(json.contains("\"table1\""));
        assert!(json.contains("\"table2\""));
    }

    #[test]
    fn test_load_table_response_minimal() {
        let json = r#"{
            "metadata-location": "gs://bucket/metadata/v1.metadata.json",
            "metadata": {
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "gs://bucket/table",
                "last-sequence-number": 0,
                "last-updated-ms": 1234567890000,
                "last-column-id": 3,
                "current-schema-id": 0,
                "schemas": [],
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "properties": {},
                "default-spec-id": 0,
                "partition-specs": [],
                "last-partition-id": 0,
                "default-sort-order-id": 0,
                "sort-orders": []
            }
        }"#;

        let response: LoadTableResponse =
            serde_json::from_str(json).expect("deserialization failed");
        assert_eq!(
            response.metadata_location,
            "gs://bucket/metadata/v1.metadata.json"
        );
        assert_eq!(response.metadata.format_version, 2);
        assert!(response.storage_credentials.is_none());
    }

    #[test]
    fn test_load_table_response_with_credentials() {
        let response = LoadTableResponse {
            metadata_location: "gs://bucket/metadata.json".to_string(),
            metadata: TableMetadata {
                format_version: 2,
                table_uuid: TableUuid::new(uuid::Uuid::new_v4()),
                location: "gs://bucket/table".to_string(),
                last_sequence_number: 1,
                last_updated_ms: 1234567890000,
                last_column_id: 3,
                current_schema_id: 0,
                schemas: vec![],
                current_snapshot_id: None,
                snapshots: vec![],
                snapshot_log: vec![],
                metadata_log: vec![],
                properties: HashMap::new(),
                default_spec_id: 0,
                partition_specs: vec![],
                last_partition_id: 0,
                refs: HashMap::new(),
                default_sort_order_id: 0,
                sort_orders: vec![],
            },
            config: HashMap::new(),
            storage_credentials: Some(vec![StorageCredential::gcs(
                "gs://bucket/",
                "token",
                "2025-01-15T15:00:00Z",
            )]),
        };

        let json = serde_json::to_string(&response).expect("serialization failed");
        assert!(json.contains("storage-credentials"));
        assert!(json.contains("gcs.oauth2.token"));
    }

    #[test]
    fn test_list_tables_roundtrip() {
        let json = r#"{"identifiers":[{"namespace":["db"],"name":"table1"},{"namespace":["db","schema"],"name":"table2"}],"next-page-token":"token-1"}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: ListTablesResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }

    #[test]
    fn test_list_tables_empty_roundtrip() {
        let json = r#"{"identifiers":[]}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: ListTablesResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }

    #[test]
    fn test_load_table_response_roundtrip() {
        let json = r#"{"metadata-location":"gs://bucket/metadata/v1.metadata.json","metadata":{"format-version":2,"table-uuid":"550e8400-e29b-41d4-a716-446655440000","location":"gs://bucket/table","last-sequence-number":0,"last-updated-ms":1234567890000,"last-column-id":3,"current-schema-id":0,"schemas":[],"current-snapshot-id":null,"snapshots":[],"snapshot-log":[],"metadata-log":[],"properties":{},"default-spec-id":0,"partition-specs":[],"last-partition-id":0,"refs":{},"default-sort-order-id":0,"sort-orders":[]},"config":{"read.default.name-mapping":"true"},"storage-credentials":[{"prefix":"gs://bucket/","config":{"gcs.oauth2.token":"token","gcs.oauth2.token-expires-at":"2025-01-15T15:00:00Z"}}]}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: LoadTableResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }
}
