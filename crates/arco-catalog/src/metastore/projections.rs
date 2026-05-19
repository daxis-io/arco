//! Projection registry and safe metastore projection builders.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, Result};

use super::replay::MetastoreState;

/// Current schema version for `metastore_objects.parquet`.
pub const METASTORE_OBJECTS_SCHEMA_VERSION: i32 = 2;

/// Current schema version for `storage_governance.parquet`.
pub const STORAGE_GOVERNANCE_SCHEMA_VERSION: i32 = 1;

/// Current allowlisted metastore object projection file.
pub const METASTORE_OBJECTS_PROJECTION: &str = "metastore_objects.parquet";

/// Current allowlisted storage-governance projection file.
pub const STORAGE_GOVERNANCE_PROJECTION: &str = "storage_governance.parquet";

/// Registered metastore projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSpec {
    /// Projection file name.
    pub file_name: &'static str,
    /// Projection schema version.
    pub schema_version: i32,
    /// Whether this projection may be tenant visible.
    pub tenant_visible: bool,
}

/// Projection registry with explicit allowlisting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRegistry {
    specs: BTreeSet<&'static str>,
}

impl Default for ProjectionRegistry {
    fn default() -> Self {
        Self::new([METASTORE_OBJECTS_PROJECTION, STORAGE_GOVERNANCE_PROJECTION])
    }
}

impl ProjectionRegistry {
    /// Creates a projection registry from an allowlist.
    #[must_use]
    pub fn new(files: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            specs: files.into_iter().collect(),
        }
    }

    /// Returns true when a projection file is allowlisted.
    #[must_use]
    pub fn contains(&self, file_name: &str) -> bool {
        self.specs.contains(file_name)
    }

    /// Returns enabled projection file names in deterministic order.
    #[must_use]
    pub fn enabled_files(&self) -> Vec<&'static str> {
        self.specs.iter().copied().collect()
    }

    /// Returns registered projection specs in deterministic order.
    #[must_use]
    pub fn specs(&self) -> Vec<ProjectionSpec> {
        self.enabled_files()
            .into_iter()
            .map(|file_name| ProjectionSpec {
                file_name,
                schema_version: projection_schema_version(file_name),
                tenant_visible: true,
            })
            .collect()
    }
}

/// Row in `metastore_objects.parquet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreObjectProjectionRecord {
    /// Projection schema version.
    pub schema_version: i32,
    /// Ledger watermark event ID.
    pub ledger_watermark: String,
    /// Stable object ID.
    pub object_id: String,
    /// Object type.
    pub object_type: String,
    /// Optional lookup name.
    pub name: Option<String>,
    /// Optional owner.
    pub owner: Option<String>,
    /// Lifecycle state.
    pub lifecycle_state: String,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at: i64,
    /// Optional governed URL for storage-governance rows.
    pub url: Option<String>,
    /// Optional related storage credential ID.
    pub credential_id: Option<String>,
    /// Optional workspace ID for workspace-scoped storage governance rows.
    pub workspace_id: Option<String>,
    /// Optional bound object ID for binding rows.
    pub bound_object_id: Option<String>,
    /// Optional bound object type for binding rows.
    pub bound_object_type: Option<String>,
    /// JSON-encoded compatibility properties.
    pub properties_json: Option<String>,
}

/// Built projection file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionFile {
    /// Projection file name.
    pub file_name: &'static str,
    /// Projection schema version.
    pub schema_version: i32,
    /// Ledger watermark event ID.
    pub ledger_watermark: String,
    /// Safe rows for the projection.
    pub rows: Vec<MetastoreObjectProjectionRecord>,
}

impl ProjectionFile {
    /// Returns the safe field names exposed by this projection.
    #[must_use]
    pub fn schema_field_names(&self) -> Vec<&'static str> {
        vec![
            "schema_version",
            "ledger_watermark",
            "object_id",
            "object_type",
            "name",
            "owner",
            "lifecycle_state",
            "updated_at",
            "url",
            "credential_id",
            "workspace_id",
            "bound_object_id",
            "bound_object_type",
            "properties_json",
        ]
    }

    /// Serializes the projection to Parquet bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow batch construction or Parquet writing fails.
    pub fn write_parquet(&self) -> Result<Bytes> {
        write_metastore_objects(&self.rows)
    }
}

/// Built projection set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSet {
    /// Projection files.
    pub files: Vec<ProjectionFile>,
}

impl ProjectionSet {
    /// Finds a projection by file name.
    #[must_use]
    pub fn file(&self, file_name: &str) -> Option<&ProjectionFile> {
        self.files.iter().find(|file| file.file_name == file_name)
    }
}

/// Returns the Arrow schema for `metastore_objects.parquet`.
#[must_use]
pub fn metastore_objects_schema() -> Schema {
    Schema::new(vec![
        Field::new("schema_version", DataType::Int32, false),
        Field::new("ledger_watermark", DataType::Utf8, false),
        Field::new("object_id", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("owner", DataType::Utf8, true),
        Field::new("lifecycle_state", DataType::Utf8, false),
        Field::new("updated_at", DataType::Int64, false),
        Field::new("url", DataType::Utf8, true),
        Field::new("credential_id", DataType::Utf8, true),
        Field::new("workspace_id", DataType::Utf8, true),
        Field::new("bound_object_id", DataType::Utf8, true),
        Field::new("bound_object_type", DataType::Utf8, true),
        Field::new("properties_json", DataType::Utf8, true),
    ])
}

/// Builds all allowlisted projection files from replayed state.
///
/// # Errors
///
/// Returns an error if projection construction fails.
pub fn build_projection_set(
    state: &MetastoreState,
    registry: &ProjectionRegistry,
    ledger_watermark: &str,
) -> Result<ProjectionSet> {
    let mut files = Vec::new();
    if registry.contains(METASTORE_OBJECTS_PROJECTION) {
        files.push(ProjectionFile {
            file_name: METASTORE_OBJECTS_PROJECTION,
            schema_version: METASTORE_OBJECTS_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            rows: metastore_object_rows(state, ledger_watermark)?,
        });
    }
    if registry.contains(STORAGE_GOVERNANCE_PROJECTION) {
        files.push(ProjectionFile {
            file_name: STORAGE_GOVERNANCE_PROJECTION,
            schema_version: STORAGE_GOVERNANCE_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            rows: storage_governance_rows(state, ledger_watermark)?,
        });
    }

    Ok(ProjectionSet { files })
}

/// Builds safe object rows from replayed state.
///
/// # Errors
///
/// Returns an error if projection construction fails.
pub fn metastore_object_rows(
    state: &MetastoreState,
    ledger_watermark: &str,
) -> Result<Vec<MetastoreObjectProjectionRecord>> {
    let mut rows = Vec::new();

    for record in state.principals.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: METASTORE_OBJECTS_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.principal_id.clone(),
            object_type: format!("principal:{}", record.principal_kind.as_str()),
            name: Some(record.name.clone()),
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: None,
            credential_id: None,
            workspace_id: None,
            bound_object_id: None,
            bound_object_type: None,
            properties_json: None,
        });
    }

    for record in state.grants.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: METASTORE_OBJECTS_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.grant_id.clone(),
            object_type: format!("grant:{}", record.object_type),
            name: Some(record.privilege.clone()),
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: None,
            credential_id: None,
            workspace_id: None,
            bound_object_id: None,
            bound_object_type: None,
            properties_json: None,
        });
    }

    for record in state.storage_credentials.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: METASTORE_OBJECTS_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.credential_id.clone(),
            object_type: format!("storage_credential:{}", record.cloud),
            name: Some(record.name.clone()),
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: None,
            credential_id: None,
            workspace_id: None,
            bound_object_id: None,
            bound_object_type: None,
            properties_json: None,
        });
    }

    rows.sort_by(|left, right| {
        left.object_type
            .cmp(&right.object_type)
            .then_with(|| left.object_id.cmp(&right.object_id))
    });

    Ok(rows)
}

/// Builds safe storage-governance rows from replayed state.
///
/// # Errors
///
/// Returns an error if projection construction fails.
pub fn storage_governance_rows(
    state: &MetastoreState,
    ledger_watermark: &str,
) -> Result<Vec<MetastoreObjectProjectionRecord>> {
    let mut rows = Vec::new();

    for record in state.storage_credentials.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: STORAGE_GOVERNANCE_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.credential_id.clone(),
            object_type: "storage_credential".to_string(),
            name: Some(record.name.clone()),
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: None,
            credential_id: None,
            workspace_id: None,
            bound_object_id: None,
            bound_object_type: None,
            properties_json: None,
        });
    }

    for record in state.external_locations.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: STORAGE_GOVERNANCE_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.location_id.clone(),
            object_type: "external_location".to_string(),
            name: Some(record.name.clone()),
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: Some(record.url.clone()),
            credential_id: Some(record.credential_id.clone()),
            workspace_id: None,
            bound_object_id: None,
            bound_object_type: None,
            properties_json: None,
        });
    }

    for record in state.managed_roots.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: STORAGE_GOVERNANCE_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.root_id.clone(),
            object_type: "managed_root".to_string(),
            name: Some(record.name.clone()),
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: Some(record.url.clone()),
            credential_id: None,
            workspace_id: Some(record.workspace_id.clone()),
            bound_object_id: None,
            bound_object_type: None,
            properties_json: None,
        });
    }

    for record in state.workspace_bindings.values() {
        rows.push(MetastoreObjectProjectionRecord {
            schema_version: STORAGE_GOVERNANCE_SCHEMA_VERSION,
            ledger_watermark: ledger_watermark.to_string(),
            object_id: record.binding_id.clone(),
            object_type: "workspace_binding".to_string(),
            name: None,
            owner: Some(record.owner.clone()),
            lifecycle_state: record.lifecycle_state.as_str().to_string(),
            updated_at: record.updated_at_ms,
            url: None,
            credential_id: None,
            workspace_id: Some(record.workspace_id.clone()),
            bound_object_id: Some(record.object_id.clone()),
            bound_object_type: Some(record.object_type.clone()),
            properties_json: None,
        });
    }

    rows.sort_by(|left, right| {
        left.object_type
            .cmp(&right.object_type)
            .then_with(|| left.object_id.cmp(&right.object_id))
    });

    Ok(rows)
}

/// Writes `metastore_objects.parquet`.
///
/// # Errors
///
/// Returns an error if the record batch cannot be built or the Parquet write
/// fails.
pub fn write_metastore_objects(rows: &[MetastoreObjectProjectionRecord]) -> Result<Bytes> {
    let schema = Arc::new(metastore_objects_schema());

    let schema_versions = Int32Array::from(
        rows.iter()
            .map(|record| record.schema_version)
            .collect::<Vec<_>>(),
    );
    let ledger_watermarks = StringArray::from(
        rows.iter()
            .map(|record| Some(record.ledger_watermark.as_str()))
            .collect::<Vec<_>>(),
    );
    let object_ids = StringArray::from(
        rows.iter()
            .map(|record| Some(record.object_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let object_types = StringArray::from(
        rows.iter()
            .map(|record| Some(record.object_type.as_str()))
            .collect::<Vec<_>>(),
    );
    let names = StringArray::from(
        rows.iter()
            .map(|record| record.name.as_deref())
            .collect::<Vec<_>>(),
    );
    let owners = StringArray::from(
        rows.iter()
            .map(|record| record.owner.as_deref())
            .collect::<Vec<_>>(),
    );
    let lifecycle_states = StringArray::from(
        rows.iter()
            .map(|record| Some(record.lifecycle_state.as_str()))
            .collect::<Vec<_>>(),
    );
    let updated_at = Int64Array::from(
        rows.iter()
            .map(|record| record.updated_at)
            .collect::<Vec<_>>(),
    );
    let urls = StringArray::from(
        rows.iter()
            .map(|record| record.url.as_deref())
            .collect::<Vec<_>>(),
    );
    let credential_ids = StringArray::from(
        rows.iter()
            .map(|record| record.credential_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let workspace_ids = StringArray::from(
        rows.iter()
            .map(|record| record.workspace_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let bound_object_ids = StringArray::from(
        rows.iter()
            .map(|record| record.bound_object_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let bound_object_types = StringArray::from(
        rows.iter()
            .map(|record| record.bound_object_type.as_deref())
            .collect::<Vec<_>>(),
    );
    let properties_json = StringArray::from(
        rows.iter()
            .map(|record| record.properties_json.as_deref())
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(schema_versions),
            Arc::new(ledger_watermarks),
            Arc::new(object_ids),
            Arc::new(object_types),
            Arc::new(names),
            Arc::new(owners),
            Arc::new(lifecycle_states),
            Arc::new(updated_at),
            Arc::new(urls),
            Arc::new(credential_ids),
            Arc::new(workspace_ids),
            Arc::new(bound_object_ids),
            Arc::new(bound_object_types),
            Arc::new(properties_json),
        ],
    )
    .map_err(|err| CatalogError::Parquet {
        message: format!("metastore object record batch build failed: {err}"),
    })?;

    let mut cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(WriterProperties::default()))
        .map_err(|err| CatalogError::Parquet {
        message: format!("metastore object parquet writer init failed: {err}"),
    })?;
    writer.write(&batch).map_err(|err| CatalogError::Parquet {
        message: format!("metastore object parquet write failed: {err}"),
    })?;
    writer.close().map_err(|err| CatalogError::Parquet {
        message: format!("metastore object parquet close failed: {err}"),
    })?;

    Ok(Bytes::from(cursor.into_inner()))
}

fn projection_schema_version(file_name: &str) -> i32 {
    match file_name {
        STORAGE_GOVERNANCE_PROJECTION => STORAGE_GOVERNANCE_SCHEMA_VERSION,
        _ => METASTORE_OBJECTS_SCHEMA_VERSION,
    }
}
