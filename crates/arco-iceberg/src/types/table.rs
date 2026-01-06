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

/// Query parameters for loading a table.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
pub struct LoadTableQuery {
    /// Controls which snapshots are returned: `all` (default) or `refs`.
    ///
    /// - `all`: Returns all snapshots in the table
    /// - `refs`: Returns only snapshots referenced by branches and tags
    ///
    /// **Note**: When `refs` is used, only `metadata.snapshots` is filtered.
    /// Fields like `current_snapshot_id` and `snapshot_log` are not reconciled.
    #[serde(default)]
    pub snapshots: Option<SnapshotsFilter>,
}

/// Filter for which snapshots to include in `load_table` response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotsFilter {
    /// Return all snapshots (default).
    #[default]
    All,
    /// Return only snapshots referenced by branches and tags.
    Refs,
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

/// Request body for `POST /v1/{prefix}/namespaces/{namespace}/tables`.
///
/// Creates a new Iceberg table with the provided schema and optional configuration.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct CreateTableRequest {
    /// The name of the table (must be unique within the namespace).
    pub name: String,

    /// Storage location for the table. If not provided, the catalog will assign one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    /// The schema for the table.
    pub schema: Schema,

    /// Optional partition specification.
    #[serde(rename = "partition-spec", skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<PartitionSpec>,

    /// Optional write order (sort order for data files).
    #[serde(rename = "write-order", skip_serializing_if = "Option::is_none")]
    pub write_order: Option<SortOrder>,

    /// Whether to stage the creation without committing.
    ///
    /// If true, the table is created but not committed - the caller must
    /// commit it using the commit endpoint.
    #[serde(rename = "stage-create", default)]
    pub stage_create: bool,

    /// Table properties.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Request body for `POST /v1/{prefix}/namespaces/{namespace}/register`.
///
/// Registers an existing Iceberg table with the catalog by providing
/// its metadata file location.
///
/// # Metadata Handling
///
/// During registration, Arco writes a **new** metadata file with an updated
/// `table-uuid` that matches the catalog's assigned table ID. The original
/// metadata file at `metadata-location` is left unchanged (immutable).
///
/// The response's `metadata-location` will point to the newly created file:
/// `{table-location}/metadata/arco-registered-{catalog-uuid}.metadata.json`
///
/// This ensures consistency between the catalog's table ID and the metadata's
/// `table-uuid` field, which is required for commit operations that use
/// `assert-table-uuid` requirements.
///
/// # Metadata-Location Semantics
///
/// The `metadata-location` input is interpreted as follows:
/// - Absolute URIs (e.g., `gs://bucket/path/metadata.json`) have scheme and
///   bucket stripped; only the path portion is used for storage operations.
/// - Scoped paths (e.g., `tenant=X/workspace=Y/path`) have the scope prefix
///   stripped to derive the relative storage path.
/// - Clients should use the `metadata-location` from the response (not their
///   input) for subsequent operations.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct RegisterTableRequest {
    /// The name to register the table under.
    pub name: String,

    /// The location of the table's existing metadata file.
    ///
    /// The server reads this file to extract table configuration but does not
    /// modify it. A new metadata file is created with the catalog-assigned UUID.
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,

    /// Whether to overwrite if a table with this name already exists.
    #[serde(default)]
    pub overwrite: bool,
}

/// Query parameters for `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
pub struct DropTableQuery {
    /// Request to purge all data and metadata files associated with the table.
    ///
    /// **Note**: Data purge is not currently supported. Requests with
    /// `purgeRequested=true` will return 406 Not Acceptable.
    ///
    /// When false or absent, the catalog drops the table from the catalog
    /// but leaves the data files in place (the table can potentially be
    /// re-registered).
    #[serde(rename = "purgeRequested", default)]
    pub purge_requested: bool,
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
    fn test_snapshots_filter_parsing() {
        let all: SnapshotsFilter = serde_json::from_str(r#""all""#).unwrap();
        assert_eq!(all, SnapshotsFilter::All);

        let refs: SnapshotsFilter = serde_json::from_str(r#""refs""#).unwrap();
        assert_eq!(refs, SnapshotsFilter::Refs);
    }

    #[test]
    fn test_snapshots_filter_default() {
        assert_eq!(SnapshotsFilter::default(), SnapshotsFilter::All);
    }

    #[test]
    fn test_load_table_query_default() {
        let query = LoadTableQuery::default();
        assert!(query.snapshots.is_none());
    }

    #[test]
    fn test_create_table_request_parsing() {
        let json = r#"{
            "name": "my_table",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            },
            "properties": {"owner": "data-team"}
        }"#;

        let request: CreateTableRequest =
            serde_json::from_str(json).expect("deserialization failed");
        assert_eq!(request.name, "my_table");
        assert!(request.location.is_none());
        assert!(!request.stage_create);
        assert_eq!(
            request.properties.get("owner"),
            Some(&"data-team".to_string())
        );
    }

    #[test]
    fn test_create_table_request_with_location_and_stage() {
        let json = r#"{
            "name": "staged_table",
            "location": "gs://bucket/tables/staged",
            "schema": {"schema-id": 0, "fields": []},
            "stage-create": true
        }"#;

        let request: CreateTableRequest =
            serde_json::from_str(json).expect("deserialization failed");
        assert_eq!(request.name, "staged_table");
        assert_eq!(
            request.location,
            Some("gs://bucket/tables/staged".to_string())
        );
        assert!(request.stage_create);
    }

    #[test]
    fn test_register_table_request_parsing() {
        let json = r#"{
            "name": "imported_table",
            "metadata-location": "gs://bucket/metadata/v1.metadata.json"
        }"#;

        let request: RegisterTableRequest =
            serde_json::from_str(json).expect("deserialization failed");
        assert_eq!(request.name, "imported_table");
        assert_eq!(
            request.metadata_location,
            "gs://bucket/metadata/v1.metadata.json"
        );
    }

    #[test]
    fn test_drop_table_query_defaults_to_false() {
        let query = DropTableQuery::default();
        assert!(!query.purge_requested);
    }

    #[test]
    fn test_drop_table_query_with_purge() {
        let json = r#"{"purgeRequested": true}"#;
        let query: DropTableQuery = serde_json::from_str(json).expect("deserialization failed");
        assert!(query.purge_requested);
    }
}
