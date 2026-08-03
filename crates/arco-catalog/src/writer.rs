//! Catalog write operations (`CatalogWriter` facade).
//!
//! The catalog writer handles all mutations to catalog state,
//! implementing the two-tier consistency model:
//!
//! - **Tier 1**: Strongly consistent DDL (namespaces, tables, lineage)
//! - **Tier 2**: Eventually consistent events (via `EventWriter`)
//!
//! ## Domain-Split Architecture
//!
//! `CatalogWriter` uses **separate locks per domain** to avoid contention:
//!
//! - `catalog` domain: namespaces, tables, columns (low-frequency DDL)
//! - `lineage` domain: lineage edges (medium-frequency, per-execution)
//!
//! This ensures lineage writes don't block catalog DDL operations.

// MVP: Allow some pedantic lints that will be cleaned up in refinement
#![allow(clippy::doc_markdown)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::uninlined_format_args)]

use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest as _, Sha256};
use ulid::Ulid;
use uuid::Uuid;

use arco_core::control_plane_transactions::{
    ControlPlaneHandleMutationRef, ControlPlaneHandleRecord, ControlPlaneHandleScope,
    ControlPlaneHandleStatus,
};
use arco_core::storage::StorageBackend;
use arco_core::sync_compact::SyncCompactRequest;
use arco_core::{
    CatalogDomain, CatalogEventPayload, CatalogPaths, ControlPlaneIdempotencyRecord,
    ControlPlaneScope, ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths,
    ControlPlaneTxRecord, ControlPlaneTxStatus, DeltaPaths, EventId, ScopedStorage, TableFormat,
};

use crate::error::{CatalogError, Result};
use crate::event_writer::EventWriter;
use crate::idempotency::{
    CatalogIdempotencyMarker, CatalogOperation, DEFAULT_STALE_TIMEOUT, IdempotencyCheck,
    IdempotencyStore, IdempotencyStoreImpl, ObjectVersion, canonical_request_hash,
    check_idempotency,
};
use crate::lock::DistributedLock;
use crate::manifest::SnapshotInfo;
use crate::parquet_util::{
    CatalogRecord, ColumnRecord, LineageEdgeRecord, NamespaceRecord, TableRecord,
};
use crate::state::CatalogState;
use crate::sync_compactor::SyncCompactor;
use crate::tier1_events::{
    CatalogDdlEvent, CatalogDdlEventV2, CatalogDdlEventV3, CatalogDdlEventV4, LineageDdlEvent,
};
use crate::tier1_state;
use crate::tier1_writer::{
    CatalogTransactionEventInspection, CatalogTransactionEventRecovery,
    CatalogTransactionPublication, PublishedCatalogTransactionEvent, Tier1Writer,
};
use crate::write_options::{CatalogTransactionIdentity, IdempotencyKey, WriteOptions};

/// Native metastore event accepted by future catalog product writer paths.
pub type MetastoreWriteEvent = crate::metastore::events::MetastoreEvent;

/// Default lock TTL for write operations.
const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(30);
/// Default maximum lock acquisition retries.
const DEFAULT_LOCK_MAX_RETRIES: u32 = 10;

fn normalize_new_table_format(raw: Option<&str>) -> Result<String> {
    match raw {
        Some(value) => TableFormat::normalize(value).map_err(CatalogError::from),
        None => Ok(TableFormat::default_for_new_tables().as_str().to_string()),
    }
}

fn normalize_table_format_patch(raw: Option<String>) -> Result<Option<String>> {
    raw.map(|value| TableFormat::normalize(&value).map_err(CatalogError::from))
        .transpose()
}

fn normalize_table_location_for_write(
    format: TableFormat,
    raw: Option<String>,
    tenant: &str,
    workspace: &str,
) -> Result<Option<String>> {
    if format != TableFormat::Delta {
        return Ok(raw);
    }

    let Some(location) = raw else {
        return Ok(None);
    };

    let trimmed = location.trim().to_string();
    DeltaPaths::from_table_location(Uuid::nil(), Some(&trimmed), tenant, workspace)
        .map_err(CatalogError::from)?;
    Ok(Some(trimmed))
}

fn encode_uc_properties(properties: Option<&BTreeMap<String, String>>) -> Result<Option<String>> {
    properties
        .map(|properties| {
            serde_json::to_string(properties).map_err(|err| CatalogError::Serialization {
                message: format!("failed to serialize UC properties: {err}"),
            })
        })
        .transpose()
}

fn decode_uc_properties(
    properties_json: Option<String>,
) -> Result<Option<BTreeMap<String, String>>> {
    properties_json
        .map(|properties_json| {
            serde_json::from_str::<BTreeMap<String, String>>(&properties_json).map_err(|err| {
                CatalogError::Serialization {
                    message: format!("failed to parse UC properties JSON: {err}"),
                }
            })
        })
        .transpose()
}
// ============================================================================
// Domain Types (returned from write operations)
// ============================================================================

/// A schema in the catalog.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Unique schema ID (UUID v7).
    pub id: String,
    /// Parent catalog ID (UUID v7).
    pub catalog_id: Option<String>,
    /// Schema name (unique within its catalog).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Optional UC properties.
    pub properties: Option<BTreeMap<String, String>>,
    /// Optional UC storage root.
    pub storage_root: Option<String>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: i64,
    /// Last update timestamp (milliseconds since epoch).
    pub updated_at: i64,
}

/// Backward-compatible alias for the legacy namespace terminology.
///
/// TODO: remove this alias after downstream consumers complete the
/// `Namespace` -> `Schema` migration.
pub type Namespace = Schema;

/// A catalog in the metastore.
#[derive(Debug, Clone)]
pub struct Catalog {
    /// Unique catalog ID (UUID v7).
    pub id: String,
    /// Catalog name (unique within workspace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Optional UC properties.
    pub properties: Option<BTreeMap<String, String>>,
    /// Optional UC storage root.
    pub storage_root: Option<String>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: i64,
    /// Last update timestamp (milliseconds since epoch).
    pub updated_at: i64,
}

impl TryFrom<CatalogRecord> for Catalog {
    type Error = CatalogError;

    fn try_from(r: CatalogRecord) -> Result<Self> {
        Ok(Self {
            id: r.id,
            name: r.name,
            description: r.description,
            properties: decode_uc_properties(r.properties_json)?,
            storage_root: r.storage_root,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }
}

impl TryFrom<&Catalog> for CatalogRecord {
    type Error = CatalogError;

    fn try_from(catalog: &Catalog) -> Result<Self> {
        Ok(Self {
            id: catalog.id.clone(),
            name: catalog.name.clone(),
            description: catalog.description.clone(),
            created_at: catalog.created_at,
            updated_at: catalog.updated_at,
            properties_json: encode_uc_properties(catalog.properties.as_ref())?,
            storage_root: catalog.storage_root.clone(),
        })
    }
}

impl TryFrom<NamespaceRecord> for Schema {
    type Error = CatalogError;

    fn try_from(r: NamespaceRecord) -> Result<Self> {
        Ok(Self {
            id: r.id,
            catalog_id: r.catalog_id,
            name: r.name,
            description: r.description,
            properties: decode_uc_properties(r.properties_json)?,
            storage_root: r.storage_root,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }
}

impl TryFrom<&Schema> for NamespaceRecord {
    type Error = CatalogError;

    fn try_from(ns: &Schema) -> Result<Self> {
        Ok(Self {
            id: ns.id.clone(),
            catalog_id: ns.catalog_id.clone(),
            name: ns.name.clone(),
            description: ns.description.clone(),
            created_at: ns.created_at,
            updated_at: ns.updated_at,
            properties_json: encode_uc_properties(ns.properties.as_ref())?,
            storage_root: ns.storage_root.clone(),
        })
    }
}

/// A table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    /// Unique table ID (UUID v7).
    pub id: String,
    /// Parent namespace ID.
    pub namespace_id: String,
    /// Table name (unique within namespace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Storage location.
    pub location: Option<String>,
    /// Lakehouse table format (`delta`, `iceberg`, or `parquet`).
    pub format: Option<String>,
    /// Optional UC table type (for example `EXTERNAL`).
    pub table_type: Option<String>,
    /// Optional UC properties.
    pub properties: Option<BTreeMap<String, String>>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: i64,
    /// Last update timestamp (milliseconds since epoch).
    pub updated_at: i64,
}

impl TryFrom<TableRecord> for Table {
    type Error = CatalogError;

    fn try_from(r: TableRecord) -> Result<Self> {
        Ok(Self {
            id: r.id,
            namespace_id: r.namespace_id,
            name: r.name,
            description: r.description,
            location: r.location,
            format: r.format,
            table_type: r.table_type,
            properties: decode_uc_properties(r.properties_json)?,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }
}

impl TryFrom<&Table> for TableRecord {
    type Error = CatalogError;

    fn try_from(t: &Table) -> Result<Self> {
        Ok(Self {
            id: t.id.clone(),
            namespace_id: t.namespace_id.clone(),
            name: t.name.clone(),
            description: t.description.clone(),
            location: t.location.clone(),
            format: t.format.clone(),
            created_at: t.created_at,
            updated_at: t.updated_at,
            table_type: t.table_type.clone(),
            properties_json: encode_uc_properties(t.properties.as_ref())?,
        })
    }
}

/// A column in a table schema.
#[derive(Debug, Clone)]
pub struct Column {
    /// Unique column ID (UUID v7).
    pub id: String,
    /// Parent table ID.
    pub table_id: String,
    /// Column name.
    pub name: String,
    /// Data type (e.g., "STRING", "INT64").
    pub data_type: String,
    /// Whether the column is nullable.
    pub is_nullable: bool,
    /// Column ordinal position (0-indexed).
    pub ordinal: i32,
    /// Optional description.
    pub description: Option<String>,
}

impl From<ColumnRecord> for Column {
    fn from(r: ColumnRecord) -> Self {
        Self {
            id: r.id,
            table_id: r.table_id,
            name: r.name,
            data_type: r.data_type,
            is_nullable: r.is_nullable,
            ordinal: r.ordinal,
            description: r.description,
        }
    }
}

impl From<&Column> for ColumnRecord {
    fn from(c: &Column) -> Self {
        Self {
            id: c.id.clone(),
            table_id: c.table_id.clone(),
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            is_nullable: c.is_nullable,
            ordinal: c.ordinal,
            description: c.description.clone(),
        }
    }
}

/// A lineage edge representing data flow between entities.
#[derive(Debug, Clone)]
pub struct LineageEdge {
    /// Unique edge ID. Mixed identity: content-derived SHA-256 hex for edges
    /// minted by the L0 route, ULID for rows written before it. Opaque to
    /// readers.
    pub id: String,
    /// Source entity ID.
    pub source_id: String,
    /// Target entity ID.
    pub target_id: String,
    /// Edge type (e.g., "derives_from", "depends_on").
    pub edge_type: String,
    /// Optional run ID that created this edge.
    pub run_id: Option<String>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: i64,
}

impl From<LineageEdgeRecord> for LineageEdge {
    fn from(r: LineageEdgeRecord) -> Self {
        Self {
            id: r.id,
            source_id: r.source_id,
            target_id: r.target_id,
            edge_type: r.edge_type,
            run_id: r.run_id,
            created_at: r.created_at,
        }
    }
}

impl From<&LineageEdge> for LineageEdgeRecord {
    fn from(e: &LineageEdge) -> Self {
        Self {
            id: e.id.clone(),
            source_id: e.source_id.clone(),
            target_id: e.target_id.clone(),
            edge_type: e.edge_type.clone(),
            run_id: e.run_id.clone(),
            created_at: e.created_at,
        }
    }
}

/// Request to register a new table.
#[derive(Debug, Clone)]
pub struct RegisterTableRequest {
    /// Namespace name (must exist).
    pub namespace: String,
    /// Table name (must be unique within namespace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Storage location.
    pub location: Option<String>,
    /// Lakehouse table format (`delta`, `iceberg`, or `parquet`).
    ///
    /// When omitted, new table registrations default to Delta Lake.
    pub format: Option<String>,
    /// Column definitions.
    pub columns: Vec<ColumnDefinition>,
}

/// Request to register a new table under a UC-like catalog + schema.
#[derive(Debug, Clone)]
pub struct RegisterTableInSchemaRequest {
    /// Table name (must be unique within schema).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Storage location.
    pub location: Option<String>,
    /// Lakehouse table format (`delta`, `iceberg`, or `parquet`).
    ///
    /// When omitted, new table registrations default to Delta Lake.
    pub format: Option<String>,
    /// Optional UC table type (for example `EXTERNAL`).
    pub table_type: Option<String>,
    /// Optional UC properties.
    pub properties: Option<BTreeMap<String, String>>,
    /// Column definitions.
    pub columns: Vec<ColumnDefinition>,
}

/// Column definition for table registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Data type (e.g., "STRING", "INT64").
    pub data_type: String,
    /// Whether the column is nullable.
    pub is_nullable: bool,
    /// Column ordinal position (0-indexed).
    pub ordinal: i32,
    /// Optional description.
    pub description: Option<String>,
}

/// Patch for updating a table.
#[derive(Debug, Clone, Default)]
pub struct TablePatch {
    /// New description (None = no change).
    pub description: Option<Option<String>>,
    /// New location (None = no change).
    pub location: Option<Option<String>>,
    /// New format (None = no change).
    pub format: Option<Option<String>>,
}

/// Canonical internal request shape for a durable catalog transaction.
///
/// This is an implementation contract shared by the control-plane transaction
/// service and the catalog writer. It does not expose a mutation entry point.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogTransactionRequest {
    /// Creates one named catalog.
    CreateCatalog {
        /// Catalog name.
        catalog: String,
        /// Optional description.
        description: Option<String>,
    },
    /// Creates one schema in a catalog.
    CreateSchema {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Optional description.
        description: Option<String>,
    },
    /// Registers one table.
    RegisterTable {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Table name.
        table: String,
        /// Optional description.
        description: Option<String>,
        /// Optional table location.
        location: Option<String>,
        /// Optional table format.
        format: Option<String>,
        /// Ordered column definitions.
        columns: Vec<ColumnDefinition>,
    },
    /// Updates one table.
    UpdateTable {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Table name.
        table: String,
        /// Optional description patch.
        description: Option<Option<String>>,
        /// Optional location patch.
        location: Option<Option<String>>,
        /// Optional format patch.
        format: Option<Option<String>>,
    },
    /// Drops one table.
    DropTable {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Table name.
        table: String,
    },
    /// Renames one table.
    RenameTable {
        /// Catalog name.
        catalog: String,
        /// Schema name.
        schema: String,
        /// Current table name.
        table: String,
        /// New table name.
        new_table: String,
    },
}

impl CatalogTransactionRequest {
    /// Returns the canonical JSON value hashed for frozen transaction identity.
    #[must_use]
    pub fn request_value(&self) -> serde_json::Value {
        match self {
            Self::CreateCatalog {
                catalog,
                description,
            } => serde_json::json!({
                "type": "create_catalog",
                "catalog": catalog,
                "description": description,
            }),
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => serde_json::json!({
                "type": "create_schema",
                "catalog": catalog,
                "schema": schema,
                "description": description,
            }),
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => serde_json::json!({
                "type": "register_table",
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "description": description,
                "location": location,
                "format": format,
                "columns": columns.iter().map(|column| serde_json::json!({
                    "name": column.name,
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "ordinal": column.ordinal,
                    "description": column.description,
                })).collect::<Vec<_>>(),
            }),
            Self::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => Self::update_request_value(
                catalog,
                schema,
                table,
                description.as_ref(),
                location.as_ref(),
                format.as_ref(),
            ),
            Self::DropTable {
                catalog,
                schema,
                table,
            } => serde_json::json!({
                "type": "drop_table",
                "catalog": catalog,
                "schema": schema,
                "table": table,
            }),
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => serde_json::json!({
                "type": "rename_table",
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "new_table": new_table,
            }),
        }
    }

    /// Returns the prefixed canonical request digest.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical request hashing fails.
    pub fn request_hash(&self) -> Result<String> {
        canonical_request_hash(&self.request_value())
            .map(|hash| format!("sha256:{hash}"))
            .map_err(|error| CatalogError::InvariantViolation {
                message: format!("failed to hash catalog transaction request: {error}"),
            })
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn validate_event_realization(
        &self,
        event_type: &str,
        event_version: u32,
        payload: &serde_json::Value,
        base: &CatalogState,
        tenant: &str,
        workspace: &str,
    ) -> Result<serde_json::Value> {
        match self {
            Self::CreateCatalog {
                catalog,
                description,
            } => validate_create_catalog_realization(
                event_type,
                event_version,
                payload,
                base,
                catalog,
                description.as_deref(),
            ),
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => validate_create_schema_realization(
                event_type,
                event_version,
                payload,
                base,
                catalog,
                schema,
                description.as_deref(),
            ),
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => validate_register_table_realization(
                event_type,
                event_version,
                payload,
                base,
                CatalogTableTarget {
                    catalog,
                    schema,
                    table,
                },
                NewTableFields {
                    description,
                    location,
                    format,
                    columns,
                },
                tenant,
                workspace,
            ),
            Self::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => validate_update_table_realization(
                event_type,
                event_version,
                payload,
                base,
                CatalogTableTarget {
                    catalog,
                    schema,
                    table,
                },
                TablePatchFields {
                    description,
                    location,
                    format,
                },
                tenant,
                workspace,
            ),
            Self::DropTable {
                catalog,
                schema,
                table,
            } => validate_drop_table_realization(
                event_type,
                event_version,
                payload,
                base,
                CatalogTableTarget {
                    catalog,
                    schema,
                    table,
                },
            ),
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => validate_rename_table_realization(
                event_type,
                event_version,
                payload,
                base,
                CatalogTableTarget {
                    catalog,
                    schema,
                    table,
                },
                new_table,
            ),
        }
    }

    #[allow(clippy::option_option)]
    fn update_request_value(
        catalog: &str,
        schema: &str,
        table: &str,
        description: Option<&Option<String>>,
        location: Option<&Option<String>>,
        format: Option<&Option<String>>,
    ) -> serde_json::Value {
        let mut value = serde_json::Map::from_iter([
            (
                "type".to_string(),
                serde_json::Value::String("update_table".to_string()),
            ),
            (
                "catalog".to_string(),
                serde_json::Value::String(catalog.to_string()),
            ),
            (
                "schema".to_string(),
                serde_json::Value::String(schema.to_string()),
            ),
            (
                "table".to_string(),
                serde_json::Value::String(table.to_string()),
            ),
        ]);
        for (field, patch) in [
            ("description", description),
            ("location", location),
            ("format", format),
        ] {
            if let Some(patch) = patch {
                value.insert(
                    field.to_string(),
                    patch
                        .clone()
                        .map_or(serde_json::Value::Null, serde_json::Value::String),
                );
            }
        }
        serde_json::Value::Object(value)
    }
}

#[derive(Clone, Copy)]
struct CatalogTableTarget<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
}

#[derive(Clone, Copy)]
struct NewTableFields<'a> {
    description: &'a Option<String>,
    location: &'a Option<String>,
    format: &'a Option<String>,
    columns: &'a [ColumnDefinition],
}

#[derive(Clone, Copy)]
#[allow(clippy::option_option)]
struct TablePatchFields<'a> {
    description: &'a Option<Option<String>>,
    location: &'a Option<Option<String>>,
    format: &'a Option<Option<String>>,
}

fn catalog_event_realization_error() -> CatalogError {
    CatalogError::InvariantViolation {
        message: "catalog transaction event does not realize its reviewed staged operation"
            .to_string(),
    }
}

fn decode_exact_catalog_event<T>(payload: &serde_json::Value) -> Result<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let decoded = serde_json::from_value::<T>(payload.clone())
        .map_err(|_| catalog_event_realization_error())?;
    if serde_json::to_value(&decoded).map_err(|_| catalog_event_realization_error())? != *payload {
        return Err(catalog_event_realization_error());
    }
    Ok(decoded)
}

fn validate_catalog_event_version(
    event_type: &str,
    event_version: u32,
    expected_version: u32,
) -> Result<()> {
    if event_type != "catalog.ddl" || event_version != expected_version {
        return Err(catalog_event_realization_error());
    }
    Ok(())
}

fn runtime_uuid_v7_is_valid(value: &str) -> bool {
    Uuid::parse_str(value)
        .is_ok_and(|uuid| uuid.get_version_num() == 7 && uuid.to_string() == value)
}

fn runtime_timestamp_is_valid(value: i64) -> bool {
    value > 0
}

fn replace_semantic_field(
    semantics: &mut serde_json::Value,
    pointer: &str,
    replacement: &str,
) -> Result<()> {
    let field = semantics
        .pointer_mut(pointer)
        .ok_or_else(catalog_event_realization_error)?;
    *field = serde_json::Value::String(replacement.to_string());
    Ok(())
}

fn catalog_record_for_name<'a>(
    state: &'a CatalogState,
    catalog: &str,
) -> Result<&'a CatalogRecord> {
    state
        .catalogs
        .iter()
        .find(|candidate| candidate.name == catalog)
        .ok_or_else(catalog_event_realization_error)
}

fn namespace_record_for_target<'a>(
    state: &'a CatalogState,
    catalog: &str,
    schema: &str,
) -> Result<&'a NamespaceRecord> {
    let catalog_record = catalog_record_for_name(state, catalog)?;
    let default_catalog_id = state
        .catalogs
        .iter()
        .find(|candidate| candidate.name == "default")
        .map(|candidate| candidate.id.as_str());
    state
        .namespaces
        .iter()
        .find(|candidate| {
            candidate.name == schema
                && candidate.catalog_id.as_deref().or(default_catalog_id)
                    == Some(catalog_record.id.as_str())
        })
        .ok_or_else(catalog_event_realization_error)
}

fn table_record_for_target<'a>(
    state: &'a CatalogState,
    target: CatalogTableTarget<'_>,
) -> Result<&'a TableRecord> {
    let namespace = namespace_record_for_target(state, target.catalog, target.schema)?;
    state
        .tables
        .iter()
        .find(|candidate| candidate.namespace_id == namespace.id && candidate.name == target.table)
        .ok_or_else(catalog_event_realization_error)
}

fn validate_create_catalog_realization(
    event_type: &str,
    event_version: u32,
    payload: &serde_json::Value,
    base: &CatalogState,
    name: &str,
    description: Option<&str>,
) -> Result<serde_json::Value> {
    validate_catalog_event_version(event_type, event_version, 2)?;
    let CatalogDdlEventV2::CatalogCreated { catalog } =
        decode_exact_catalog_event::<CatalogDdlEventV2>(payload)?;
    if base.catalogs.iter().any(|candidate| candidate.name == name)
        || catalog.name != name
        || catalog.description.as_deref() != description
        || catalog.properties_json.is_some()
        || catalog.storage_root.is_some()
        || catalog.created_at != catalog.updated_at
        || !runtime_timestamp_is_valid(catalog.created_at)
        || !runtime_uuid_v7_is_valid(&catalog.id)
    {
        return Err(catalog_event_realization_error());
    }
    let mut semantics = payload.clone();
    replace_semantic_field(&mut semantics, "/catalog/id", "runtime_uuid_v7")?;
    replace_semantic_field(
        &mut semantics,
        "/catalog/created_at",
        "runtime_timestamp_ms",
    )?;
    replace_semantic_field(
        &mut semantics,
        "/catalog/updated_at",
        "runtime_timestamp_ms",
    )?;
    Ok(semantics)
}

fn validate_create_schema_realization(
    event_type: &str,
    event_version: u32,
    payload: &serde_json::Value,
    base: &CatalogState,
    catalog_name: &str,
    schema: &str,
    description: Option<&str>,
) -> Result<serde_json::Value> {
    validate_catalog_event_version(event_type, event_version, 1)?;
    let CatalogDdlEvent::NamespaceCreated { namespace } =
        decode_exact_catalog_event::<CatalogDdlEvent>(payload)?
    else {
        return Err(catalog_event_realization_error());
    };
    let catalog = catalog_record_for_name(base, catalog_name)?;
    let default_catalog_id = base
        .catalogs
        .iter()
        .find(|candidate| candidate.name == "default")
        .map(|candidate| candidate.id.as_str());
    if base.namespaces.iter().any(|candidate| {
        candidate.name == schema
            && candidate.catalog_id.as_deref().or(default_catalog_id) == Some(catalog.id.as_str())
    }) || namespace.catalog_id.as_deref() != Some(catalog.id.as_str())
        || namespace.name != schema
        || namespace.description.as_deref() != description
        || namespace.properties_json.is_some()
        || namespace.storage_root.is_some()
        || namespace.created_at != namespace.updated_at
        || !runtime_timestamp_is_valid(namespace.created_at)
        || !runtime_uuid_v7_is_valid(&namespace.id)
    {
        return Err(catalog_event_realization_error());
    }
    let mut semantics = payload.clone();
    replace_semantic_field(&mut semantics, "/namespace/id", "runtime_uuid_v7")?;
    replace_semantic_field(
        &mut semantics,
        "/namespace/created_at",
        "runtime_timestamp_ms",
    )?;
    replace_semantic_field(
        &mut semantics,
        "/namespace/updated_at",
        "runtime_timestamp_ms",
    )?;
    Ok(semantics)
}

#[allow(clippy::too_many_arguments)]
fn validate_register_table_realization(
    event_type: &str,
    event_version: u32,
    payload: &serde_json::Value,
    base: &CatalogState,
    target: CatalogTableTarget<'_>,
    fields: NewTableFields<'_>,
    tenant: &str,
    workspace: &str,
) -> Result<serde_json::Value> {
    validate_catalog_event_version(event_type, event_version, 1)?;
    let CatalogDdlEvent::TableRegistered { table, columns } =
        decode_exact_catalog_event::<CatalogDdlEvent>(payload)?
    else {
        return Err(catalog_event_realization_error());
    };
    let namespace = namespace_record_for_target(base, target.catalog, target.schema)?;
    let expected_format = normalize_new_table_format(fields.format.as_deref())?;
    let format = TableFormat::parse(&expected_format).map_err(CatalogError::from)?;
    let expected_location =
        normalize_table_location_for_write(format, fields.location.clone(), tenant, workspace)?;
    let table_matches_reviewed_fields = table.namespace_id == namespace.id
        && table.name == target.table
        && table.description == *fields.description
        && table.location == expected_location
        && table.format.as_deref() == Some(expected_format.as_str())
        && table.table_type.is_none()
        && table.properties_json.is_none()
        && table.created_at == table.updated_at
        && runtime_timestamp_is_valid(table.created_at)
        && runtime_uuid_v7_is_valid(&table.id);
    if base
        .tables
        .iter()
        .any(|candidate| candidate.namespace_id == namespace.id && candidate.name == target.table)
        || !table_matches_reviewed_fields
        || columns.len() != fields.columns.len()
    {
        return Err(catalog_event_realization_error());
    }
    let mut column_ids = std::collections::HashSet::new();
    for (actual, expected) in columns.iter().zip(fields.columns) {
        let belongs_to_registered_table = actual.table_id == table.id;
        let column_matches_reviewed_fields = actual.name == expected.name
            && actual.data_type == expected.data_type
            && actual.is_nullable == expected.is_nullable
            && actual.ordinal == expected.ordinal
            && actual.description == expected.description
            && runtime_uuid_v7_is_valid(&actual.id);
        if !belongs_to_registered_table
            || !column_matches_reviewed_fields
            || !column_ids.insert(actual.id.as_str())
        {
            return Err(catalog_event_realization_error());
        }
    }
    let mut semantics = payload.clone();
    replace_semantic_field(&mut semantics, "/table/id", "runtime_uuid_v7")?;
    replace_semantic_field(&mut semantics, "/table/created_at", "runtime_timestamp_ms")?;
    replace_semantic_field(&mut semantics, "/table/updated_at", "runtime_timestamp_ms")?;
    for index in 0..columns.len() {
        replace_semantic_field(
            &mut semantics,
            &format!("/columns/{index}/id"),
            "runtime_uuid_v7",
        )?;
        replace_semantic_field(
            &mut semantics,
            &format!("/columns/{index}/table_id"),
            "runtime_table_uuid_v7",
        )?;
    }
    Ok(semantics)
}

#[allow(clippy::too_many_arguments)]
fn validate_update_table_realization(
    event_type: &str,
    event_version: u32,
    payload: &serde_json::Value,
    base: &CatalogState,
    target: CatalogTableTarget<'_>,
    patch: TablePatchFields<'_>,
    tenant: &str,
    workspace: &str,
) -> Result<serde_json::Value> {
    validate_catalog_event_version(event_type, event_version, 1)?;
    let CatalogDdlEvent::TableUpdated { table } =
        decode_exact_catalog_event::<CatalogDdlEvent>(payload)?
    else {
        return Err(catalog_event_realization_error());
    };
    let current = table_record_for_target(base, target)?;
    let mut expected = current.clone();
    if let Some(description) = patch.description {
        expected.description.clone_from(description);
    }
    if let Some(location) = patch.location {
        expected.location.clone_from(location);
    }
    if let Some(format) = patch.format {
        expected.format = normalize_table_format_patch(format.clone())?;
    }
    let effective_format = expected
        .format
        .as_deref()
        .map(TableFormat::parse)
        .transpose()
        .map_err(CatalogError::from)?;
    if effective_format == Some(TableFormat::Delta) {
        expected.location = normalize_table_location_for_write(
            TableFormat::Delta,
            expected.location,
            tenant,
            workspace,
        )?;
    }
    if !runtime_timestamp_is_valid(table.updated_at)
        || table.updated_at < current.updated_at
        || table.updated_at < current.created_at
    {
        return Err(catalog_event_realization_error());
    }
    expected.updated_at = table.updated_at;
    if table != expected {
        return Err(catalog_event_realization_error());
    }
    let mut semantics = payload.clone();
    replace_semantic_field(&mut semantics, "/table/updated_at", "runtime_timestamp_ms")?;
    Ok(semantics)
}

fn validate_drop_table_realization(
    event_type: &str,
    event_version: u32,
    payload: &serde_json::Value,
    base: &CatalogState,
    target: CatalogTableTarget<'_>,
) -> Result<serde_json::Value> {
    validate_catalog_event_version(event_type, event_version, 1)?;
    let CatalogDdlEvent::TableDropped {
        table_id,
        namespace_id,
        table_name,
    } = decode_exact_catalog_event::<CatalogDdlEvent>(payload)?
    else {
        return Err(catalog_event_realization_error());
    };
    let table = table_record_for_target(base, target)?;
    if table_id != table.id || namespace_id != table.namespace_id || table_name != target.table {
        return Err(catalog_event_realization_error());
    }
    Ok(payload.clone())
}

fn validate_rename_table_realization(
    event_type: &str,
    event_version: u32,
    payload: &serde_json::Value,
    base: &CatalogState,
    target: CatalogTableTarget<'_>,
    new_table: &str,
) -> Result<serde_json::Value> {
    validate_catalog_event_version(event_type, event_version, 1)?;
    let CatalogDdlEvent::TableRenamed {
        table_id,
        namespace_id,
        old_name,
        new_name,
        updated_at,
    } = decode_exact_catalog_event::<CatalogDdlEvent>(payload)?
    else {
        return Err(catalog_event_realization_error());
    };
    let table = table_record_for_target(base, target)?;
    if table_id != table.id
        || namespace_id != table.namespace_id
        || old_name != target.table
        || new_name != new_table
        || !runtime_timestamp_is_valid(updated_at)
        || updated_at < table.updated_at
        || base.tables.iter().any(|candidate| {
            target.table != new_table
                && candidate.namespace_id == table.namespace_id
                && candidate.name == new_table
        })
    {
        return Err(catalog_event_realization_error());
    }
    let mut semantics = payload.clone();
    replace_semantic_field(&mut semantics, "/updated_at", "runtime_timestamp_ms")?;
    Ok(semantics)
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum StagedCatalogRequestAuthority {
    CreateCatalog {
        catalog: String,
        description: Option<String>,
    },
    CreateSchema {
        catalog: String,
        schema: String,
        description: Option<String>,
    },
    RegisterTable {
        catalog: String,
        schema: String,
        table: String,
        description: Option<String>,
        location: Option<String>,
        format: Option<String>,
        columns: Vec<StagedCatalogColumnAuthority>,
    },
    UpdateTable {
        catalog: String,
        schema: String,
        table: String,
        description: StagedCatalogTextPatchAuthority,
        location: StagedCatalogTextPatchAuthority,
        format: StagedCatalogTextPatchAuthority,
    },
    DropTable {
        catalog: String,
        schema: String,
        table: String,
    },
    RenameTable {
        catalog: String,
        schema: String,
        table: String,
        new_table: String,
    },
}

#[derive(Debug, serde::Deserialize)]
struct StagedCatalogColumnAuthority {
    name: String,
    data_type: String,
    is_nullable: bool,
    ordinal: i32,
    description: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "action", content = "value", rename_all = "snake_case")]
enum StagedCatalogTextPatchAuthority {
    Unchanged,
    Clear,
    Set(String),
}

impl StagedCatalogTextPatchAuthority {
    #[allow(clippy::option_option)]
    fn into_nested(self) -> Option<Option<String>> {
        match self {
            Self::Unchanged => None,
            Self::Clear => Some(None),
            Self::Set(value) => Some(Some(value)),
        }
    }
}

impl From<StagedCatalogRequestAuthority> for CatalogTransactionRequest {
    fn from(authority: StagedCatalogRequestAuthority) -> Self {
        match authority {
            StagedCatalogRequestAuthority::CreateCatalog {
                catalog,
                description,
            } => Self::CreateCatalog {
                catalog,
                description,
            },
            StagedCatalogRequestAuthority::CreateSchema {
                catalog,
                schema,
                description,
            } => Self::CreateSchema {
                catalog,
                schema,
                description,
            },
            StagedCatalogRequestAuthority::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns: columns
                    .into_iter()
                    .map(|column| ColumnDefinition {
                        name: column.name,
                        data_type: column.data_type,
                        is_nullable: column.is_nullable,
                        ordinal: column.ordinal,
                        description: column.description,
                    })
                    .collect(),
            },
            StagedCatalogRequestAuthority::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => Self::UpdateTable {
                catalog,
                schema,
                table,
                description: description.into_nested(),
                location: location.into_nested(),
                format: format.into_nested(),
            },
            StagedCatalogRequestAuthority::DropTable {
                catalog,
                schema,
                table,
            } => Self::DropTable {
                catalog,
                schema,
                table,
            },
            StagedCatalogRequestAuthority::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            },
        }
    }
}

/// Patch for updating catalog UC-facing metadata.
#[derive(Debug, Clone, Default)]
pub struct CatalogPatch {
    /// New description (None = no change).
    pub description: Option<Option<String>>,
    /// New catalog name.
    pub new_name: Option<String>,
    /// New UC properties (None = no change, Some(None) = clear).
    pub properties: Option<Option<BTreeMap<String, String>>>,
    /// New UC storage root (None = no change, Some(None) = clear).
    pub storage_root: Option<Option<String>>,
}

/// Patch for updating schema UC-facing metadata.
#[derive(Debug, Clone, Default)]
pub struct SchemaPatch {
    /// New description (None = no change).
    pub description: Option<Option<String>>,
    /// New schema name.
    pub new_name: Option<String>,
    /// New UC properties (None = no change, Some(None) = clear).
    pub properties: Option<Option<BTreeMap<String, String>>>,
    /// New UC storage root (None = no change, Some(None) = clear).
    pub storage_root: Option<Option<String>>,
}

/// Event source identifier for Tier-2 event writing.
#[derive(Debug, Clone)]
pub struct EventSource {
    /// Service/component name (e.g., "api-server", "scheduler").
    pub service: String,
    /// Optional instance identifier.
    pub instance: Option<String>,
}

/// Visible commit metadata for one catalog DDL transaction.
#[derive(Debug, Clone)]
pub struct CatalogTransactionCommit {
    /// Ledger event identifier published by the DDL writer.
    pub event_id: String,
    /// Visible commit identifier from the immutable catalog manifest.
    pub commit_id: String,
    /// Visible immutable manifest identifier.
    pub manifest_id: String,
    /// Visible snapshot version.
    pub snapshot_version: u64,
    /// Pointer object version returned by the visible CAS publish.
    pub pointer_version: String,
    /// Canonical catalog lock path used for this commit.
    pub lock_path: String,
    /// Fencing token held while publishing this visible head.
    pub fencing_token: u64,
    /// Whether repairable post-commit side effects are still outstanding.
    pub repair_pending: bool,
    /// Table identity removed by this transaction, when the transaction dropped a table.
    pub dropped_table: Option<DroppedTableIdentity>,
}

/// Identity of a table removed by a catalog DDL transaction.
#[derive(Debug, Clone)]
pub struct DroppedTableIdentity {
    /// UUID string of the table removed under the catalog lock.
    pub table_id: String,
    /// Lakehouse table format of the removed table.
    pub format: Option<String>,
}

#[derive(Debug, Clone)]
struct DefaultCatalogOutcome {
    catalog: CatalogRecord,
    repair_pending: bool,
}

#[derive(Debug)]
struct FrozenCatalogHandleBinding {
    handle_id: String,
    ordinal: u64,
    direct_identity: String,
    root_child: bool,
}

#[derive(Debug)]
struct FrozenCatalogHandleAuthority {
    handle: ControlPlaneHandleRecord,
    reference: ControlPlaneHandleMutationRef,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
struct FrozenCatalogClaimIdentity {
    domain: ControlPlaneTxDomain,
    kind: ControlPlaneTxKind,
    idempotency_key: String,
}

#[derive(Debug, serde::Deserialize)]
struct FrozenCatalogHandleIntent {
    mutation_ref: ControlPlaneHandleMutationRef,
    claim_identities: Vec<FrozenCatalogClaimIdentity>,
}

#[derive(Debug, serde::Deserialize)]
struct FrozenCatalogIdentityAuthority {
    record_type: String,
    version: u16,
    handle_id: String,
    scope: ControlPlaneHandleScope,
    ordinal: u64,
    legacy_reservations: Vec<FrozenCatalogClaimIdentity>,
    handle_intent: Option<FrozenCatalogHandleIntent>,
}

impl EventSource {
    /// Creates a new event source.
    #[must_use]
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            instance: None,
        }
    }

    /// Sets the instance identifier.
    #[must_use]
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    fn to_source_string(&self) -> String {
        match &self.instance {
            Some(inst) => format!("{}:{}", self.service, inst),
            None => self.service.clone(),
        }
    }
}

// ============================================================================
// CatalogWriter
// ============================================================================

/// Writer for catalog mutations.
///
/// Handles both Tier 1 (strongly consistent) and Tier 2 (eventually consistent)
/// write operations to the catalog.
///
/// ## Domain-Split Architecture
///
/// Uses separate locks per domain to minimize contention:
/// - Catalog domain (namespaces, tables, columns) has its own lock
/// - Lineage domain (edges) has a separate lock
///
/// This ensures medium-frequency lineage writes don't block low-frequency DDL.
pub struct CatalogWriter {
    storage: ScopedStorage,
    scope: ControlPlaneScope,
    /// Tier-1 writer (handles catalog domain lock + CAS)
    tier1: Tier1Writer,
    /// Separate lock for lineage domain
    lineage_lock: DistributedLock<dyn StorageBackend>,
    /// Lock TTL
    lock_ttl: Duration,
    /// Max lock retries
    lock_max_retries: u32,
    /// Sync compaction client for Tier-1 DDL operations.
    sync_compactor: Option<Arc<dyn SyncCompactor>>,
}

impl std::fmt::Debug for CatalogWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogWriter")
            .field("storage", &"ScopedStorage { ... }")
            .finish()
    }
}

impl CatalogWriter {
    /// Creates a new catalog writer for the given storage scope.
    ///
    /// # Panics
    ///
    /// Panics if the already-validated scoped storage IDs cannot form a
    /// workspace alias scope.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arco_catalog::CatalogWriter;
    /// use arco_core::ScopedStorage;
    ///
    /// let storage = ScopedStorage::new(backend, "acme", "production")?;
    /// let compactor = std::sync::Arc::new(arco_catalog::Tier1Compactor::new(storage.clone()));
    /// let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
    /// ```
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(storage: ScopedStorage) -> Self {
        let scope = ControlPlaneScope::workspace_alias(storage.tenant_id(), storage.workspace_id())
            .expect("ScopedStorage tenant/workspace IDs are already validated");
        Self::new_with_scope(storage, scope)
    }

    /// Creates a new catalog writer with an explicit control-plane scope.
    ///
    /// The supplied storage remains rooted at its current workspace prefix. This
    /// keeps Task 3 as an API-threading change only; moving durable catalog paths
    /// to metastore prefixes is handled by the later path migration tasks.
    ///
    /// # Panics
    ///
    /// Panics if the explicit scope does not match the scoped storage tenant and
    /// workspace.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new_with_scope(storage: ScopedStorage, scope: ControlPlaneScope) -> Self {
        Self::try_new_with_scope(storage, scope)
            .expect("explicit control-plane scope must match scoped storage")
    }

    /// Tries to create a catalog writer with an explicit control-plane scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage tenant/workspace does not match the
    /// execution tenant/workspace carried by the explicit scope.
    pub fn try_new_with_scope(storage: ScopedStorage, scope: ControlPlaneScope) -> Result<Self> {
        validate_storage_scope(&storage, &scope)?;
        let backend = storage.backend().clone();
        let lineage_lock_path = storage.lock(CatalogDomain::Lineage);
        let lineage_lock = DistributedLock::new(backend, lineage_lock_path);

        Ok(Self {
            tier1: Tier1Writer::new(storage.clone()),
            lineage_lock,
            storage,
            scope,
            lock_ttl: DEFAULT_LOCK_TTL,
            lock_max_retries: DEFAULT_LOCK_MAX_RETRIES,
            sync_compactor: None,
        })
    }

    /// Configures the sync compaction client for Tier-1 DDL operations.
    #[must_use]
    pub fn with_sync_compactor(mut self, compactor: Arc<dyn SyncCompactor>) -> Self {
        self.sync_compactor = Some(compactor);
        self
    }

    /// Sets the lock acquisition policy for this writer.
    #[must_use]
    pub const fn with_lock_policy(mut self, ttl: Duration, max_retries: u32) -> Self {
        self.lock_ttl = ttl;
        self.lock_max_retries = max_retries;
        self
    }

    /// Returns a reference to the underlying storage.
    #[must_use]
    pub fn storage(&self) -> &ScopedStorage {
        &self.storage
    }

    /// Returns the explicit control-plane scope for this writer.
    #[must_use]
    pub fn scope(&self) -> &ControlPlaneScope {
        &self.scope
    }

    fn sync_compactor(&self) -> Result<&Arc<dyn SyncCompactor>> {
        self.sync_compactor
            .as_ref()
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "sync compactor is not configured (Tier-1 DDL is disabled)".to_string(),
            })
    }

    fn idempotency_store(&self) -> IdempotencyStoreImpl<ScopedStorage> {
        IdempotencyStoreImpl::new(Arc::new(self.storage.clone()))
    }

    fn idempotency_request_hash(value: &serde_json::Value) -> Result<String> {
        canonical_request_hash(value).map_err(|err| CatalogError::InvariantViolation {
            message: format!("failed to canonicalize idempotent request: {err}"),
        })
    }

    fn idempotency_request_failed(http_status: u16, message: impl Into<String>) -> CatalogError {
        CatalogError::RequestFailed {
            http_status,
            message: message.into(),
        }
    }

    fn idempotency_conflict_error() -> CatalogError {
        Self::idempotency_request_failed(
            409,
            "Idempotency-Key already used with different request body",
        )
    }

    fn idempotency_in_progress_error() -> CatalogError {
        Self::idempotency_request_failed(409, "request with Idempotency-Key is still in progress")
    }

    fn should_cache_idempotency_failure(err: &CatalogError) -> bool {
        match err {
            CatalogError::CasFailed { .. } | CatalogError::PreconditionFailed { .. } => false,
            _ => err.http_status_code().is_some(),
        }
    }

    async fn finalize_idempotency_success(
        &self,
        marker: CatalogIdempotencyMarker,
        version: ObjectVersion,
        entity_id: &str,
        entity_name: &str,
    ) -> Result<()> {
        let finalized = marker.finalize_committed(entity_id.to_string(), entity_name.to_string());
        match self
            .idempotency_store()
            .finalize(&finalized, &version)
            .await?
        {
            crate::idempotency::FinalizeResult::Success { .. } => Ok(()),
            crate::idempotency::FinalizeResult::Conflict { current_version } => {
                Err(CatalogError::InvariantViolation {
                    message: format!(
                        "idempotency marker finalize conflict after successful operation: {}",
                        current_version.as_str()
                    ),
                })
            }
        }
    }

    async fn finalize_idempotency_failure(
        &self,
        marker: CatalogIdempotencyMarker,
        version: ObjectVersion,
        err: &CatalogError,
    ) -> Result<()> {
        if !Self::should_cache_idempotency_failure(err) {
            return Ok(());
        }

        let Some(http_status) = err.http_status_code() else {
            return Ok(());
        };

        let finalized = marker.finalize_failed(http_status, err.to_string());
        match self
            .idempotency_store()
            .finalize(&finalized, &version)
            .await?
        {
            crate::idempotency::FinalizeResult::Success { .. } => Ok(()),
            crate::idempotency::FinalizeResult::Conflict { current_version } => {
                Err(CatalogError::InvariantViolation {
                    message: format!(
                        "idempotency marker finalize conflict after failed operation: {}",
                        current_version.as_str()
                    ),
                })
            }
        }
    }

    fn reserved_idempotency_entity(marker: &CatalogIdempotencyMarker) -> Result<(&str, &str)> {
        match (&marker.entity_id, &marker.entity_name) {
            (Some(entity_id), Some(entity_name)) => Ok((entity_id.as_str(), entity_name.as_str())),
            _ => Err(CatalogError::InvariantViolation {
                message: "stale idempotency marker missing reserved entity proof".to_string(),
            }),
        }
    }

    async fn reserve_idempotency_entity(
        &self,
        marker: CatalogIdempotencyMarker,
        version: ObjectVersion,
        entity_id: &str,
        entity_name: &str,
    ) -> Result<(CatalogIdempotencyMarker, ObjectVersion)> {
        match (&marker.entity_id, &marker.entity_name) {
            (Some(existing_id), Some(existing_name))
                if existing_id == entity_id && existing_name == entity_name =>
            {
                Ok((marker, version))
            }
            (None, None) => {
                let reserved =
                    marker.reserve_entity(entity_id.to_string(), entity_name.to_string());
                match self
                    .idempotency_store()
                    .finalize(&reserved, &version)
                    .await?
                {
                    crate::idempotency::FinalizeResult::Success { version } => {
                        Ok((reserved, version))
                    }
                    crate::idempotency::FinalizeResult::Conflict { current_version } => {
                        Err(CatalogError::InvariantViolation {
                            message: format!(
                                "idempotency marker reservation conflict: {}",
                                current_version.as_str()
                            ),
                        })
                    }
                }
            }
            _ => Err(CatalogError::InvariantViolation {
                message: "idempotency marker reserved entity proof mismatch".to_string(),
            }),
        }
    }

    async fn refresh_stale_reserved_idempotency(
        &self,
        marker: CatalogIdempotencyMarker,
        version: ObjectVersion,
    ) -> Result<(CatalogIdempotencyMarker, ObjectVersion)> {
        Self::reserved_idempotency_entity(&marker)?;
        match self.idempotency_store().takeover(&marker, &version).await? {
            crate::idempotency::TakeoverResult::Success { marker, version } => {
                Ok((marker, version))
            }
            crate::idempotency::TakeoverResult::RaceDetected { current_marker, .. } => {
                match current_marker.status {
                    crate::idempotency::IdempotencyStatus::InProgress => {
                        Err(Self::idempotency_in_progress_error())
                    }
                    crate::idempotency::IdempotencyStatus::Committed => {
                        Err(CatalogError::InvariantViolation {
                            message:
                                "idempotency marker finalized during stale reservation recovery"
                                    .to_string(),
                        })
                    }
                    crate::idempotency::IdempotencyStatus::Failed => {
                        Err(Self::idempotency_request_failed(
                            current_marker.error_http_status.unwrap_or(409),
                            current_marker.error_message.clone().unwrap_or_else(|| {
                                "previous idempotent request failed".to_string()
                            }),
                        ))
                    }
                }
            }
        }
    }

    async fn recover_reserved_catalog(
        &self,
        marker: &CatalogIdempotencyMarker,
        version: &ObjectVersion,
    ) -> Result<Option<Catalog>> {
        let (reserved_id, reserved_name) = Self::reserved_idempotency_entity(marker)?;
        let Some(catalog) =
            crate::reader::CatalogReader::new_with_scope(self.storage.clone(), self.scope.clone())
                .get_catalog(reserved_name)
                .await?
        else {
            return Ok(None);
        };
        if catalog.id != reserved_id {
            return Err(CatalogError::AlreadyExists {
                entity: "catalog".into(),
                name: reserved_name.to_string(),
            });
        }
        self.finalize_idempotency_success(
            marker.clone(),
            version.clone(),
            &catalog.id,
            &catalog.name,
        )
        .await?;
        Ok(Some(catalog))
    }

    async fn recover_reserved_schema(
        &self,
        catalog: &str,
        marker: &CatalogIdempotencyMarker,
        version: &ObjectVersion,
    ) -> Result<Option<Schema>> {
        let (reserved_id, reserved_name) = Self::reserved_idempotency_entity(marker)?;
        let schema =
            crate::reader::CatalogReader::new_with_scope(self.storage.clone(), self.scope.clone())
                .list_schemas(catalog)
                .await?
                .into_iter()
                .find(|candidate| candidate.name == reserved_name);
        let Some(schema) = schema else {
            return Ok(None);
        };
        if schema.id != reserved_id {
            return Err(CatalogError::AlreadyExists {
                entity: "namespace".into(),
                name: reserved_name.to_string(),
            });
        }
        self.finalize_idempotency_success(
            marker.clone(),
            version.clone(),
            &schema.id,
            &schema.name,
        )
        .await?;
        Ok(Some(schema))
    }

    async fn recover_reserved_table(
        &self,
        catalog: &str,
        schema: &str,
        marker: &CatalogIdempotencyMarker,
        version: &ObjectVersion,
    ) -> Result<Option<Table>> {
        let (reserved_id, reserved_name) = Self::reserved_idempotency_entity(marker)?;
        let Some(table) =
            crate::reader::CatalogReader::new_with_scope(self.storage.clone(), self.scope.clone())
                .get_table_in_schema(catalog, schema, reserved_name)
                .await?
        else {
            return Ok(None);
        };
        if table.id != reserved_id {
            return Err(CatalogError::AlreadyExists {
                entity: "table".into(),
                name: format!("{catalog}.{schema}.{reserved_name}"),
            });
        }
        self.finalize_idempotency_success(marker.clone(), version.clone(), &table.id, &table.name)
            .await?;
        Ok(Some(table))
    }

    async fn replay_catalog_by_name(&self, name: &str) -> Result<Catalog> {
        crate::reader::CatalogReader::new_with_scope(self.storage.clone(), self.scope.clone())
            .get_catalog(name)
            .await?
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: format!("idempotency replay target missing: catalog {name}"),
            })
    }

    async fn replay_schema_by_name(&self, catalog: &str, schema: &str) -> Result<Schema> {
        crate::reader::CatalogReader::new_with_scope(self.storage.clone(), self.scope.clone())
            .list_schemas(catalog)
            .await?
            .into_iter()
            .find(|candidate| candidate.name == schema)
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: format!("idempotency replay target missing: schema {catalog}.{schema}"),
            })
    }

    async fn replay_table_by_name(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Table> {
        crate::reader::CatalogReader::new_with_scope(self.storage.clone(), self.scope.clone())
            .get_table_in_schema(catalog, schema, table)
            .await?
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: format!(
                    "idempotency replay target missing: table {catalog}.{schema}.{table}"
                ),
            })
    }

    fn single_event_sync_compact_request(
        &self,
        domain: CatalogDomain,
        event_id: &impl Display,
        fencing_token: u64,
        request_id: Option<String>,
    ) -> SyncCompactRequest {
        SyncCompactRequest {
            domain: domain.as_str().to_string(),
            event_paths: vec![CatalogPaths::ledger_event(domain, &event_id.to_string())],
            fencing_token,
            lock_path: Some(self.storage.lock(domain)),
            request_id,
        }
    }

    fn multi_event_sync_compact_request(
        &self,
        domain: CatalogDomain,
        event_ids: &[String],
        fencing_token: u64,
        request_id: Option<String>,
    ) -> SyncCompactRequest {
        SyncCompactRequest {
            domain: domain.as_str().to_string(),
            event_paths: event_ids
                .iter()
                .map(|event_id| CatalogPaths::ledger_event(domain, event_id))
                .collect(),
            fencing_token,
            lock_path: Some(self.storage.lock(domain)),
            request_id,
        }
    }

    async fn finish_catalog_transaction(
        &self,
        guard: crate::lock::LockGuard<dyn StorageBackend>,
        event_id: String,
        result: Result<arco_core::sync_compact::SyncCompactResponse>,
    ) -> Result<CatalogTransactionCommit> {
        let fencing_token = guard.fencing_token().sequence();
        let event_id_for_log = event_id.clone();
        let outcome = result.map(|response| CatalogTransactionCommit {
            event_id,
            commit_id: response.commit_ulid,
            manifest_id: response.manifest_id,
            snapshot_version: response.snapshot_version,
            pointer_version: response.manifest_version,
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token,
            repair_pending: response.repair_pending,
            dropped_table: None,
        });

        if let Err(error) = guard.release().await {
            tracing::warn!(
                error = ?error,
                event_id = event_id_for_log,
                fencing_token,
                "catalog transaction committed visibly but lock release failed"
            );
        }
        outcome
    }

    fn parse_frozen_catalog_handle_binding(
        request_id: &str,
        idempotency_key: &str,
    ) -> Result<FrozenCatalogHandleBinding> {
        fn parse_direct(value: &str) -> Result<(String, u64)> {
            let value =
                value
                    .strip_prefix("handle:")
                    .ok_or_else(|| CatalogError::InvariantViolation {
                        message: "frozen catalog identity is not owned by a durable handle"
                            .to_string(),
                    })?;
            let (handle_id, encoded_ordinal) =
                value
                    .split_once(":mutation:")
                    .ok_or_else(|| CatalogError::InvariantViolation {
                        message: "frozen catalog handle identity is non-canonical".to_string(),
                    })?;
            if handle_id.contains(':') {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog handle identity is non-canonical".to_string(),
                });
            }
            ControlPlaneTxPaths::handle_record(handle_id).map_err(|_| {
                CatalogError::InvariantViolation {
                    message: "frozen catalog handle identity is non-canonical".to_string(),
                }
            })?;
            let ordinal =
                encoded_ordinal
                    .parse::<u64>()
                    .map_err(|_| CatalogError::InvariantViolation {
                        message: "frozen catalog handle ordinal is non-canonical".to_string(),
                    })?;
            if ordinal == 0 || format!("{ordinal:020}") != encoded_ordinal {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog handle ordinal is non-canonical".to_string(),
                });
            }
            Ok((handle_id.to_string(), ordinal))
        }

        let (direct_identity, root_child) =
            if let Some(value) = idempotency_key.strip_prefix("root:") {
                let direct = value.strip_suffix(":catalog").ok_or_else(|| {
                    CatalogError::InvariantViolation {
                        message: "frozen catalog root-child identity is non-canonical".to_string(),
                    }
                })?;
                if request_id != direct {
                    return Err(CatalogError::InvariantViolation {
                        message: "frozen catalog root-child request identity diverges".to_string(),
                    });
                }
                (direct, true)
            } else {
                if request_id != idempotency_key {
                    return Err(CatalogError::InvariantViolation {
                        message: "frozen catalog request and idempotency identities diverge"
                            .to_string(),
                    });
                }
                (idempotency_key, false)
            };
        let (handle_id, ordinal) = parse_direct(direct_identity)?;
        Ok(FrozenCatalogHandleBinding {
            handle_id,
            ordinal,
            direct_identity: direct_identity.to_string(),
            root_child,
        })
    }

    async fn load_stable_exact_bytes(&self, path: &str, label: &str) -> Result<Bytes> {
        let before =
            self.storage
                .head_raw(path)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: format!("{label} is missing"),
                })?;
        let bytes = self.storage.get_raw(path).await?;
        let after =
            self.storage
                .head_raw(path)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: format!("{label} disappeared during exact read"),
                })?;
        if before.version != after.version {
            return Err(CatalogError::PreconditionFailed {
                message: format!("{label} changed during exact read"),
            });
        }
        Ok(bytes)
    }

    fn parse_canonical_authority(bytes: &[u8], label: &str) -> Result<serde_json::Value> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| CatalogError::InvariantViolation {
                message: format!("{label} is corrupt"),
            })?;
        let canonical = arco_core::canonical_json::to_canonical_bytes(&value).map_err(|_| {
            CatalogError::InvariantViolation {
                message: format!("{label} cannot be canonicalized"),
            }
        })?;
        if canonical.as_slice() != bytes {
            return Err(CatalogError::InvariantViolation {
                message: format!("{label} is not canonical JSON"),
            });
        }
        Ok(value)
    }

    fn staged_catalog_transaction_request(
        staged: &serde_json::Value,
        root_child: bool,
    ) -> Result<CatalogTransactionRequest> {
        let mutation = staged
            .get("mutation")
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "frozen staged mutation body is missing".to_string(),
            })?;
        let operation = if root_child {
            if mutation
                .get("mutation_type")
                .and_then(serde_json::Value::as_str)
                != Some("root")
            {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog child is not owned by a staged root mutation"
                        .to_string(),
                });
            }
            let children = mutation
                .get("mutations")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "frozen root mutation child set is corrupt".to_string(),
                })?;
            let mut catalog_children = children.iter().filter(|child| {
                child.get("domain").and_then(serde_json::Value::as_str) == Some("catalog")
            });
            let child =
                catalog_children
                    .next()
                    .ok_or_else(|| CatalogError::InvariantViolation {
                        message: "frozen root mutation has no catalog child".to_string(),
                    })?;
            if catalog_children.next().is_some() {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen root mutation has duplicate catalog children".to_string(),
                });
            }
            child
                .get("operation")
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "frozen root catalog child is missing its operation".to_string(),
                })?
        } else {
            if mutation
                .get("mutation_type")
                .and_then(serde_json::Value::as_str)
                != Some("catalog")
            {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog identity names a non-catalog staged mutation"
                        .to_string(),
                });
            }
            mutation
                .get("operation")
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "frozen catalog mutation is missing its operation".to_string(),
                })?
        };
        serde_json::from_value::<StagedCatalogRequestAuthority>(operation.clone())
            .map(CatalogTransactionRequest::from)
            .map_err(|_| CatalogError::InvariantViolation {
                message: "frozen staged catalog operation is corrupt".to_string(),
            })
    }

    const fn frozen_catalog_claim_kind(domain: ControlPlaneTxDomain) -> ControlPlaneTxKind {
        match domain {
            ControlPlaneTxDomain::Catalog => ControlPlaneTxKind::CatalogDdl,
            ControlPlaneTxDomain::Orchestration => ControlPlaneTxKind::OrchestrationBatch,
            ControlPlaneTxDomain::Root => ControlPlaneTxKind::RootCommit,
        }
    }

    const fn frozen_catalog_claim_kind_rank(kind: ControlPlaneTxKind) -> u8 {
        match kind {
            ControlPlaneTxKind::CatalogDdl => 0,
            ControlPlaneTxKind::OrchestrationBatch => 1,
            ControlPlaneTxKind::RootCommit => 2,
        }
    }

    fn sort_frozen_catalog_claims(claims: &mut [FrozenCatalogClaimIdentity]) {
        claims.sort_by(|left, right| {
            left.domain
                .cmp(&right.domain)
                .then_with(|| {
                    Self::frozen_catalog_claim_kind_rank(left.kind)
                        .cmp(&Self::frozen_catalog_claim_kind_rank(right.kind))
                })
                .then_with(|| left.idempotency_key.cmp(&right.idempotency_key))
        });
    }

    fn validate_frozen_catalog_claims(
        claims: &[FrozenCatalogClaimIdentity],
        binding: &FrozenCatalogHandleBinding,
        label: &str,
    ) -> Result<()> {
        let mut canonical = claims.to_vec();
        Self::sort_frozen_catalog_claims(&mut canonical);
        let duplicate = canonical.windows(2).any(|pair| pair[0] == pair[1]);
        if canonical != claims || duplicate {
            return Err(CatalogError::InvariantViolation {
                message: format!("frozen {label} must be sorted and unique"),
            });
        }
        let child_prefix = format!("root:{}:", binding.direct_identity);
        for claim in claims {
            let canonical_identity = claim.idempotency_key == binding.direct_identity
                || claim
                    .idempotency_key
                    .strip_prefix(&child_prefix)
                    .is_some_and(|domain| domain == claim.domain.as_str());
            if claim.kind != Self::frozen_catalog_claim_kind(claim.domain) || !canonical_identity {
                return Err(CatalogError::InvariantViolation {
                    message: format!(
                        "frozen {label} contains a noncanonical domain claim identity"
                    ),
                });
            }
        }
        Ok(())
    }

    fn expected_frozen_catalog_claims(
        staged: &serde_json::Value,
        binding: &FrozenCatalogHandleBinding,
    ) -> Result<Vec<FrozenCatalogClaimIdentity>> {
        let mutation = staged
            .get("mutation")
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "frozen staged mutation body is missing".to_string(),
            })?;
        let mutation_type = mutation
            .get("mutation_type")
            .and_then(serde_json::Value::as_str);
        let mut claims = if binding.root_child {
            if mutation_type != Some("root") {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog child is not owned by a staged root mutation"
                        .to_string(),
                });
            }
            let children = mutation
                .get("mutations")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "frozen root mutation child set is corrupt".to_string(),
                })?;
            let mut claims = vec![FrozenCatalogClaimIdentity {
                domain: ControlPlaneTxDomain::Root,
                kind: ControlPlaneTxKind::RootCommit,
                idempotency_key: binding.direct_identity.clone(),
            }];
            for child in children {
                let domain = match child.get("domain").and_then(serde_json::Value::as_str) {
                    Some("catalog") => ControlPlaneTxDomain::Catalog,
                    Some("orchestration") => ControlPlaneTxDomain::Orchestration,
                    _ => {
                        return Err(CatalogError::InvariantViolation {
                            message: "frozen root mutation contains an unsupported child domain"
                                .to_string(),
                        });
                    }
                };
                claims.push(FrozenCatalogClaimIdentity {
                    domain,
                    kind: Self::frozen_catalog_claim_kind(domain),
                    idempotency_key: format!(
                        "root:{}:{}",
                        binding.direct_identity,
                        domain.as_str()
                    ),
                });
            }
            claims
        } else {
            if mutation_type != Some("catalog") {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog identity names a non-catalog staged mutation"
                        .to_string(),
                });
            }
            vec![FrozenCatalogClaimIdentity {
                domain: ControlPlaneTxDomain::Catalog,
                kind: ControlPlaneTxKind::CatalogDdl,
                idempotency_key: binding.direct_identity.clone(),
            }]
        };
        Self::sort_frozen_catalog_claims(&mut claims);
        if claims.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CatalogError::InvariantViolation {
                message: "frozen staged mutation contains duplicate claim identities".to_string(),
            });
        }
        Ok(claims)
    }

    fn validate_identity_authority(
        authority: serde_json::Value,
        handle: &ControlPlaneHandleRecord,
        handle_reference: &ControlPlaneHandleMutationRef,
        binding: &FrozenCatalogHandleBinding,
        expected_claims: &[FrozenCatalogClaimIdentity],
        idempotency_key: &str,
    ) -> Result<()> {
        let authority: FrozenCatalogIdentityAuthority =
            serde_json::from_value(authority).map_err(|_| CatalogError::InvariantViolation {
                message: "frozen handle identity authority is corrupt".to_string(),
            })?;
        if authority.record_type != "control_plane_transaction_handle_identity_authority"
            || authority.version != 1
            || authority.handle_id != binding.handle_id
            || authority.ordinal != binding.ordinal
            || authority.scope != handle.scope
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen handle identity authority diverges from its exact path"
                    .to_string(),
            });
        }
        Self::validate_frozen_catalog_claims(
            &authority.legacy_reservations,
            binding,
            "legacy identity reservations",
        )?;
        let intent = authority
            .handle_intent
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "frozen handle identity authority has no handle intent".to_string(),
            })?;
        Self::validate_frozen_catalog_claims(
            &intent.claim_identities,
            binding,
            "handle intent claim identities",
        )?;
        if intent.mutation_ref != *handle_reference
            || intent.claim_identities != expected_claims
            || !expected_claims.iter().any(|claim| {
                claim.domain == ControlPlaneTxDomain::Catalog
                    && claim.kind == ControlPlaneTxKind::CatalogDdl
                    && claim.idempotency_key == idempotency_key
            })
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen handle intent diverges from its exact staged authority"
                    .to_string(),
            });
        }
        if authority
            .legacy_reservations
            .iter()
            .any(|reservation| intent.claim_identities.contains(reservation))
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog claims overlap legacy identity authority".to_string(),
            });
        }
        Ok(())
    }

    async fn load_frozen_catalog_handle_authority(
        &self,
        binding: &FrozenCatalogHandleBinding,
        tx_id: &str,
        request_hash: &str,
        request_id: &str,
        idempotency_key: &str,
    ) -> Result<FrozenCatalogHandleAuthority> {
        let handle_path =
            ControlPlaneTxPaths::handle_record(&binding.handle_id).map_err(CatalogError::from)?;
        let handle_bytes = self
            .load_stable_exact_bytes(&handle_path, "frozen transaction handle")
            .await?;
        let handle =
            ControlPlaneHandleRecord::from_json_slice(handle_bytes.as_ref()).map_err(|_| {
                CatalogError::InvariantViolation {
                    message: "frozen transaction handle is corrupt".to_string(),
                }
            })?;
        if handle.handle_id != binding.handle_id
            || handle.scope.tenant_id != self.storage.tenant_id()
            || handle.scope.workspace_id != self.storage.workspace_id()
            || !matches!(
                handle.status,
                ControlPlaneHandleStatus::Committing
                    | ControlPlaneHandleStatus::RepairRequired
                    | ControlPlaneHandleStatus::Visible
            )
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction is not authorized by an executable frozen handle"
                    .to_string(),
            });
        }
        let index =
            usize::try_from(binding.ordinal - 1).map_err(|_| CatalogError::InvariantViolation {
                message: "frozen handle ordinal exceeds addressable memory".to_string(),
            })?;
        let reference =
            handle
                .mutation_refs
                .get(index)
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "frozen handle is missing its catalog mutation reference".to_string(),
                })?;
        let participant =
            handle
                .participants
                .get(index)
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "frozen handle is missing its catalog participant".to_string(),
                })?;
        let participant_matches = if binding.root_child {
            participant.kind == ControlPlaneTxKind::RootCommit
                && participant.domain == ControlPlaneTxDomain::Root
                && participant.request_id == binding.direct_identity
                && participant.idempotency_key == binding.direct_identity
        } else {
            participant.kind == ControlPlaneTxKind::CatalogDdl
                && participant.domain == ControlPlaneTxDomain::Catalog
                && participant.request_id == request_id
                && participant.idempotency_key == idempotency_key
                && participant.request_hash == request_hash
                && participant
                    .tx_id
                    .as_ref()
                    .is_none_or(|participant_tx_id| participant_tx_id == tx_id)
        };
        if reference.ordinal != binding.ordinal
            || reference.kind != participant.kind
            || participant.ordinal != binding.ordinal
            || !participant_matches
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction diverges from its frozen handle participant"
                    .to_string(),
            });
        }
        let reference = reference.clone();
        Ok(FrozenCatalogHandleAuthority { handle, reference })
    }

    async fn validate_frozen_catalog_staged_authority(
        &self,
        binding: &FrozenCatalogHandleBinding,
        handle_authority: &FrozenCatalogHandleAuthority,
        request_hash: &str,
        idempotency_key: &str,
    ) -> Result<(String, CatalogTransactionRequest)> {
        let authority_path =
            ControlPlaneTxPaths::handle_identity_authority(&binding.handle_id, binding.ordinal)
                .map_err(CatalogError::from)?;
        let authority_bytes = self
            .load_stable_exact_bytes(
                &authority_path,
                "frozen transaction handle identity authority",
            )
            .await?;
        let identity_authority = Self::parse_canonical_authority(
            authority_bytes.as_ref(),
            "frozen transaction handle identity authority",
        )?;

        let staged_bytes = self
            .load_stable_exact_bytes(
                &handle_authority.reference.path,
                "frozen staged catalog mutation",
            )
            .await?;
        let staged_sha256 = format!("sha256:{}", hex::encode(Sha256::digest(&staged_bytes)));
        if staged_sha256 != handle_authority.reference.sha256 {
            return Err(CatalogError::InvariantViolation {
                message: "frozen staged catalog mutation digest diverges".to_string(),
            });
        }
        let staged = Self::parse_canonical_authority(
            staged_bytes.as_ref(),
            "frozen staged catalog mutation",
        )?;
        let staged_scope: ControlPlaneHandleScope =
            serde_json::from_value(staged.get("scope").cloned().ok_or_else(|| {
                CatalogError::InvariantViolation {
                    message: "frozen staged catalog mutation is missing its scope".to_string(),
                }
            })?)
            .map_err(|_| CatalogError::InvariantViolation {
                message: "frozen staged catalog mutation has corrupt scope".to_string(),
            })?;
        let expected_kind = if binding.root_child {
            ControlPlaneTxKind::RootCommit
        } else {
            ControlPlaneTxKind::CatalogDdl
        };
        let staged_kind: ControlPlaneTxKind =
            serde_json::from_value(staged.get("kind").cloned().ok_or_else(|| {
                CatalogError::InvariantViolation {
                    message: "frozen staged catalog mutation is missing its kind".to_string(),
                }
            })?)
            .map_err(|_| CatalogError::InvariantViolation {
                message: "frozen staged catalog mutation has corrupt kind".to_string(),
            })?;
        if staged
            .get("record_type")
            .and_then(serde_json::Value::as_str)
            != Some("control_plane_transaction_handle_mutation")
            || staged.get("version").and_then(serde_json::Value::as_u64) != Some(1)
            || staged.get("handle_id").and_then(serde_json::Value::as_str)
                != Some(binding.handle_id.as_str())
            || staged.get("ordinal").and_then(serde_json::Value::as_u64) != Some(binding.ordinal)
            || staged_scope != handle_authority.handle.scope
            || staged_kind != expected_kind
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen staged catalog mutation diverges from its exact authority"
                    .to_string(),
            });
        }
        let expected_claims = Self::expected_frozen_catalog_claims(&staged, binding)?;
        Self::validate_identity_authority(
            identity_authority,
            &handle_authority.handle,
            &handle_authority.reference,
            binding,
            &expected_claims,
            idempotency_key,
        )?;
        let staged_request = Self::staged_catalog_transaction_request(&staged, binding.root_child)?;
        if staged_request.request_hash()? != request_hash {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog request hash diverges from its staged operation"
                    .to_string(),
            });
        }
        Ok((staged_sha256, staged_request))
    }

    fn frozen_catalog_record_matches_claim(
        record: &ControlPlaneTxRecord<serde_json::Value>,
        tx_id: &str,
        request_hash: &str,
        request_id: &str,
        idempotency_key: &str,
    ) -> bool {
        record.tx_id == tx_id
            && record.kind == ControlPlaneTxKind::CatalogDdl
            && record.request_id == request_id
            && record.idempotency_key == idempotency_key
            && record.request_hash == request_hash
    }

    fn validate_clean_frozen_catalog_prepared(
        record: &ControlPlaneTxRecord<serde_json::Value>,
    ) -> Result<()> {
        if record.status != ControlPlaneTxStatus::Prepared
            || record.repair_pending
            || record.visible_at.is_some()
            || record.result.is_some()
            || record.durable_append.is_some()
            || record.lock_path != CatalogPaths::domain_lock(CatalogDomain::Catalog)
            || record.fencing_token != 0
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog transaction is not an exact clean prepared predecessor"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn validate_frozen_catalog_visible(
        record: &ControlPlaneTxRecord<serde_json::Value>,
    ) -> Result<()> {
        if record.status != ControlPlaneTxStatus::Visible
            || record.visible_at.is_none()
            || record
                .visible_at
                .is_some_and(|visible_at| visible_at < record.prepared_at)
            || record.result.is_none()
            || record.durable_append.is_some()
            || record.lock_path != CatalogPaths::domain_lock(CatalogDomain::Catalog)
            || record.fencing_token == 0
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog visible transaction authority is malformed".to_string(),
            });
        }
        Ok(())
    }

    fn validate_frozen_catalog_cached_shadow(
        record: &ControlPlaneTxRecord<serde_json::Value>,
        cached: &ControlPlaneTxRecord<serde_json::Value>,
    ) -> Result<()> {
        if !matches!(
            record.status,
            ControlPlaneTxStatus::Prepared | ControlPlaneTxStatus::Aborted
        ) || record.repair_pending
            || record.visible_at.is_some()
            || record.result.is_some()
            || record.durable_append.is_some()
            || record.lock_path != CatalogPaths::domain_lock(CatalogDomain::Catalog)
            || record.fencing_token != 0
            || record.prepared_at != cached.prepared_at
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog exact record conflicts with cached visible authority"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn frozen_catalog_visible_records_match(
        left: &ControlPlaneTxRecord<serde_json::Value>,
        right: &ControlPlaneTxRecord<serde_json::Value>,
    ) -> bool {
        let mut left = left.clone();
        let mut right = right.clone();
        left.repair_pending = false;
        right.repair_pending = false;
        left == right
    }

    fn validate_frozen_catalog_marker(
        marker: ControlPlaneIdempotencyRecord,
        tx_id: &str,
        request_hash: &str,
        request_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<ControlPlaneTxRecord<serde_json::Value>>> {
        if marker.tx_id != tx_id
            || marker.kind != ControlPlaneTxKind::CatalogDdl
            || marker.request_id != request_id
            || marker.idempotency_key != idempotency_key
            || marker.request_hash != request_hash
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog idempotency claim diverges from its handle".to_string(),
            });
        }
        let marker_visible_at = marker.visible_at;
        let cached: Option<ControlPlaneTxRecord<serde_json::Value>> = marker
            .tx_record
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "cached frozen catalog transaction record is corrupt".to_string(),
            })?;
        match (&cached, marker_visible_at) {
            (Some(cached), Some(marker_visible_at)) => {
                if !Self::frozen_catalog_record_matches_claim(
                    cached,
                    tx_id,
                    request_hash,
                    request_id,
                    idempotency_key,
                ) || cached.visible_at != Some(marker_visible_at)
                {
                    return Err(CatalogError::InvariantViolation {
                        message: "cached frozen catalog transaction diverges from its exact claim"
                            .to_string(),
                    });
                }
                Self::validate_frozen_catalog_visible(cached)?;
            }
            (None, None) => {}
            _ => {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog idempotency marker has incomplete visible authority"
                        .to_string(),
                });
            }
        }
        Ok(cached)
    }

    async fn validate_frozen_catalog_low_level_claim(
        &self,
        tx_id: &str,
        request_hash: &str,
        request_id: &str,
        idempotency_key: &str,
    ) -> Result<bool> {
        let marker_path =
            ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, idempotency_key);
        let marker_bytes = self
            .load_stable_exact_bytes(&marker_path, "frozen catalog idempotency claim")
            .await?;
        let marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(marker_bytes.as_ref())
            .map_err(|_| {
            CatalogError::InvariantViolation {
                message: "frozen catalog idempotency claim is corrupt".to_string(),
            }
        })?;
        let cached = Self::validate_frozen_catalog_marker(
            marker,
            tx_id,
            request_hash,
            request_id,
            idempotency_key,
        )?;

        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, tx_id);
        let exact = if self.storage.head_raw(&record_path).await?.is_some() {
            let record_bytes = self
                .load_stable_exact_bytes(&record_path, "frozen catalog transaction record")
                .await?;
            let record: ControlPlaneTxRecord<serde_json::Value> =
                serde_json::from_slice(record_bytes.as_ref()).map_err(|_| {
                    CatalogError::InvariantViolation {
                        message: "frozen catalog transaction record is corrupt".to_string(),
                    }
                })?;
            if !Self::frozen_catalog_record_matches_claim(
                &record,
                tx_id,
                request_hash,
                request_id,
                idempotency_key,
            ) {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog claim diverges from its exact low-level transaction"
                        .to_string(),
                });
            }
            Some(record)
        } else {
            None
        };
        let mutation_authorized = match (exact.as_ref(), cached.as_ref()) {
            (Some(exact), None) if exact.status == ControlPlaneTxStatus::Prepared => {
                Self::validate_clean_frozen_catalog_prepared(exact)?;
                true
            }
            (Some(exact), None) if exact.status == ControlPlaneTxStatus::Visible => {
                Self::validate_frozen_catalog_visible(exact)?;
                false
            }
            (Some(exact), Some(cached)) if exact.status == ControlPlaneTxStatus::Visible => {
                Self::validate_frozen_catalog_visible(exact)?;
                if !Self::frozen_catalog_visible_records_match(exact, cached) {
                    return Err(CatalogError::InvariantViolation {
                        message: "exact and cached frozen catalog visible authority diverge"
                            .to_string(),
                    });
                }
                false
            }
            (Some(exact), Some(cached)) => {
                Self::validate_frozen_catalog_cached_shadow(exact, cached)?;
                false
            }
            (None, Some(_)) => false,
            (None, None) => {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog transaction has no exact or cached record".to_string(),
                });
            }
            (Some(_), None) => {
                return Err(CatalogError::InvariantViolation {
                    message:
                        "frozen catalog exact transaction is neither clean prepared nor visible"
                            .to_string(),
                });
            }
        };
        Ok(mutation_authorized)
    }

    /// Exact-reads frozen handle authority and returns an opaque catalog capability.
    ///
    /// This read-only seam is internal to durable handle execution. Handle-shaped
    /// caller syntax, without matching handle, staged, identity, idempotency, and
    /// low-level transaction records, cannot produce a capability.
    ///
    /// # Errors
    ///
    /// Returns an error when any durable authority is missing, changing,
    /// noncanonical, out of scope, or divergent from the exact catalog request.
    #[doc(hidden)]
    pub async fn authorize_frozen_catalog_transaction(
        &self,
        tx_id: &str,
        request_hash: &str,
        request_id: &str,
        idempotency_key: &str,
    ) -> Result<CatalogTransactionIdentity> {
        let parsed_tx_id =
            Ulid::from_string(tx_id).map_err(|_| CatalogError::InvariantViolation {
                message: "frozen catalog transaction ID is non-canonical".to_string(),
            })?;
        if parsed_tx_id.to_string() != tx_id {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog transaction ID is non-canonical".to_string(),
            });
        }
        let binding = Self::parse_frozen_catalog_handle_binding(request_id, idempotency_key)?;
        let authority = self
            .load_frozen_catalog_handle_authority(
                &binding,
                tx_id,
                request_hash,
                request_id,
                idempotency_key,
            )
            .await?;
        let (staged_sha256, reviewed_request) = self
            .validate_frozen_catalog_staged_authority(
                &binding,
                &authority,
                request_hash,
                idempotency_key,
            )
            .await?;
        let mutation_authorized = self
            .validate_frozen_catalog_low_level_claim(
                tx_id,
                request_hash,
                request_id,
                idempotency_key,
            )
            .await?;

        Ok(CatalogTransactionIdentity {
            tx_id: tx_id.to_string(),
            request_hash: request_hash.to_string(),
            tenant_id: authority.handle.scope.tenant_id,
            workspace_id: authority.handle.scope.workspace_id,
            request_id: request_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
            handle_id: binding.handle_id,
            ordinal: binding.ordinal,
            staged_sha256,
            reviewed_request,
            mutation_authorized,
        })
    }

    fn validate_catalog_transaction_capability(
        &self,
        identity: &CatalogTransactionIdentity,
    ) -> Result<()> {
        let binding = Self::parse_frozen_catalog_handle_binding(
            &identity.request_id,
            &identity.idempotency_key,
        )?;
        let digest_is_canonical = identity.staged_sha256.starts_with("sha256:")
            && identity.staged_sha256.len() == "sha256:".len() + 64
            && identity.staged_sha256["sha256:".len()..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
        let reviewed_request_matches = identity
            .reviewed_request
            .request_hash()
            .is_ok_and(|request_hash| request_hash == identity.request_hash);
        if identity.tenant_id != self.storage.tenant_id()
            || identity.workspace_id != self.storage.workspace_id()
            || identity.handle_id != binding.handle_id
            || identity.ordinal != binding.ordinal
            || !digest_is_canonical
            || !reviewed_request_matches
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog capability is corrupt or out of writer scope".to_string(),
            });
        }
        Ok(())
    }

    fn bind_catalog_transaction_request(
        &self,
        mut opts: WriteOptions,
        request: &CatalogTransactionRequest,
    ) -> Result<WriteOptions> {
        if let Some(identity) = &opts.transaction_identity {
            self.validate_catalog_transaction_capability(identity)?;
            if !identity.mutation_authorized
                || opts.request_id.as_deref() != Some(identity.request_id.as_str())
                || opts.idempotency_key.as_ref().map(IdempotencyKey::as_str)
                    != Some(identity.idempotency_key.as_str())
            {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog capability is out of writer scope".to_string(),
                });
            }
            let actual_request_hash = request.request_hash()?;
            if actual_request_hash != identity.request_hash {
                return Err(CatalogError::InvariantViolation {
                    message:
                        "catalog transaction payload diverges from its frozen staged operation"
                            .to_string(),
                });
            }
            opts.validated_transaction_request_hash = Some(actual_request_hash);
        }
        Ok(opts)
    }

    async fn append_catalog_transaction_event<T: CatalogEventPayload + serde::Serialize + Sync>(
        &self,
        guard: &crate::lock::LockGuard<dyn StorageBackend>,
        payload: &T,
        opts: &WriteOptions,
    ) -> Result<EventId> {
        let source = opts.actor.as_deref().unwrap_or("api");
        if let Some(identity) = &opts.transaction_identity {
            if opts.validated_transaction_request_hash.as_deref()
                != Some(identity.request_hash.as_str())
            {
                return Err(CatalogError::InvariantViolation {
                    message: "frozen catalog event publication lacks exact request validation"
                        .to_string(),
                });
            }
            self.tier1
                .append_ledger_event_for_transaction(
                    guard,
                    CatalogDomain::Catalog,
                    payload,
                    source,
                    identity,
                )
                .await
        } else {
            self.tier1
                .append_ledger_event(guard, CatalogDomain::Catalog, payload, source)
                .await
        }
    }

    /// Validates a frozen catalog commit against its event intent and immutable
    /// manifest chain without mutating catalog state.
    ///
    /// Returns the immutable manifest publication timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when the intent, receipt, or immutable manifest
    /// authority is missing, corrupt, or divergent.
    pub async fn validate_catalog_transaction_commit(
        &self,
        identity: &CatalogTransactionIdentity,
        commit: &CatalogTransactionCommit,
    ) -> Result<chrono::DateTime<Utc>> {
        self.validate_catalog_transaction_capability(identity)?;
        self.tier1
            .validate_catalog_transaction_publication(
                identity,
                &CatalogTransactionPublication {
                    event_id: &commit.event_id,
                    commit_id: &commit.commit_id,
                    manifest_id: &commit.manifest_id,
                    snapshot_version: commit.snapshot_version,
                    authority_version: &commit.pointer_version,
                    fencing_token: commit.fencing_token,
                },
            )
            .await
    }

    fn recovered_catalog_transaction_commit(
        published: PublishedCatalogTransactionEvent,
    ) -> Result<CatalogTransactionCommit> {
        let fencing_token = published
            .manifest
            .fencing_token
            .unwrap_or(published.manifest.epoch);
        let commit_id = published
            .manifest
            .last_commit_id
            .clone()
            .or_else(|| published.manifest.commit_ulid.clone())
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "recovered catalog manifest is missing its commit ID".to_string(),
            })?;
        if fencing_token == 0 {
            return Err(CatalogError::InvariantViolation {
                message: "recovered catalog manifest has no fencing authority".to_string(),
            });
        }
        Ok(CatalogTransactionCommit {
            event_id: published.event_id,
            commit_id,
            manifest_id: published.manifest.manifest_id,
            snapshot_version: published.manifest.snapshot_version,
            pointer_version: published.authority_version,
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token,
            repair_pending: false,
            dropped_table: None,
        })
    }

    fn validate_catalog_transaction_recovery_request(
        &self,
        identity: &CatalogTransactionIdentity,
        request_id: Option<&str>,
    ) -> Result<()> {
        self.validate_catalog_transaction_capability(identity)?;
        if !identity.mutation_authorized || request_id != Some(identity.request_id.as_str()) {
            return Err(CatalogError::InvariantViolation {
                message: "catalog recovery request identity diverges from its frozen capability"
                    .to_string(),
            });
        }
        Ok(())
    }

    async fn finish_recovered_catalog_transaction(
        identity: &CatalogTransactionIdentity,
        guard: crate::lock::LockGuard<dyn StorageBackend>,
        published: PublishedCatalogTransactionEvent,
    ) -> Result<CatalogTransactionCommit> {
        let outcome = Self::recovered_catalog_transaction_commit(published);
        let release = guard.release().await.map_err(CatalogError::from);
        match (outcome, release) {
            (Ok(commit), Ok(())) => Ok(commit),
            (Ok(_), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => {
                tracing::warn!(
                    error = ?release_error,
                    tx_id = identity.tx_id,
                    "catalog transaction recovery validation failed and lock release also failed"
                );
                Err(error)
            }
        }
    }

    /// Recovers an interrupted exact-addressed catalog transaction event.
    ///
    /// Returns `None` when the writer never published an event intent, which
    /// proves it is safe for the caller to begin the reviewed mutation.
    ///
    /// # Errors
    ///
    /// Returns an error when recovery cannot acquire or release the catalog
    /// lock, or when the frozen intent and immutable publication diverge.
    pub async fn recover_catalog_transaction(
        &self,
        identity: &CatalogTransactionIdentity,
        request_id: Option<String>,
    ) -> Result<Option<CatalogTransactionCommit>> {
        self.validate_catalog_transaction_recovery_request(identity, request_id.as_deref())?;
        if !self.tier1.has_catalog_transaction_intent(identity).await? {
            return Ok(None);
        }
        let compactor = self.sync_compactor()?;
        for _ in 0..8 {
            let inspection = match self
                .tier1
                .inspect_catalog_transaction_event(identity)
                .await?
            {
                CatalogTransactionEventInspection::Published(published) => {
                    return Ok(Some(Self::recovered_catalog_transaction_commit(
                        *published,
                    )?));
                }
                CatalogTransactionEventInspection::Unpublished(inspection) => inspection,
            };
            let guard = self
                .tier1
                .acquire_lock(self.lock_ttl, self.lock_max_retries)
                .await?;
            let recovery = match self
                .tier1
                .recover_catalog_transaction_event_after_inspection(identity, &inspection)
                .await
            {
                Ok(recovery) => recovery,
                Err(error) => {
                    if let Err(release_error) = guard.release().await {
                        tracing::warn!(
                            error = ?release_error,
                            tx_id = identity.tx_id,
                            "catalog transaction recovery failed and lock release also failed"
                        );
                    }
                    return Err(error);
                }
            };
            match recovery {
                CatalogTransactionEventRecovery::Published(published) => {
                    return Ok(Some(
                        Self::finish_recovered_catalog_transaction(identity, guard, *published)
                            .await?,
                    ));
                }
                CatalogTransactionEventRecovery::Ready(event_id) => {
                    let request = self.single_event_sync_compact_request(
                        CatalogDomain::Catalog,
                        &event_id,
                        guard.fencing_token().sequence(),
                        request_id.clone(),
                    );
                    let event_id_string = event_id.to_string();
                    return Ok(Some(
                        self.finish_catalog_transaction(
                            guard,
                            event_id_string,
                            compactor.sync_compact(request).await,
                        )
                        .await?,
                    ));
                }
                CatalogTransactionEventRecovery::RetryUnlocked => {
                    guard.release().await.map_err(CatalogError::from)?;
                }
            }
        }
        Err(CatalogError::PreconditionFailed {
            message: "catalog transaction recovery inspection did not converge".to_string(),
        })
    }

    fn default_catalog_id(state: &CatalogState) -> Option<&str> {
        state
            .catalogs
            .iter()
            .find(|catalog| catalog.name == "default")
            .map(|catalog| catalog.id.as_str())
    }

    fn find_default_namespace<'a>(
        state: &'a CatalogState,
        schema: &str,
    ) -> Option<&'a NamespaceRecord> {
        let default_catalog_id = Self::default_catalog_id(state);
        state.namespaces.iter().find(|namespace| {
            namespace.name == schema
                && match namespace.catalog_id.as_deref() {
                    Some(catalog_id) => Some(catalog_id) == default_catalog_id,
                    None => true,
                }
        })
    }

    async fn ensure_default_catalog_locked(
        &self,
        guard: &crate::lock::LockGuard<dyn StorageBackend>,
        state: &CatalogState,
        compactor: &Arc<dyn SyncCompactor>,
        opts: &WriteOptions,
    ) -> Result<CatalogRecord> {
        Ok(self
            .ensure_default_catalog_locked_with_result(guard, state, compactor, opts)
            .await?
            .catalog)
    }

    async fn ensure_default_catalog_locked_with_result(
        &self,
        guard: &crate::lock::LockGuard<dyn StorageBackend>,
        state: &CatalogState,
        compactor: &Arc<dyn SyncCompactor>,
        opts: &WriteOptions,
    ) -> Result<DefaultCatalogOutcome> {
        if let Some(existing) = state.catalogs.iter().find(|c| c.name == "default") {
            return Ok(DefaultCatalogOutcome {
                catalog: existing.clone(),
                repair_pending: false,
            });
        }

        let now = Utc::now().timestamp_millis();
        let default = Catalog {
            id: Uuid::now_v7().to_string(),
            name: "default".to_string(),
            description: None,
            properties: None,
            storage_root: None,
            created_at: now,
            updated_at: now,
        };

        let event = CatalogDdlEventV2::CatalogCreated {
            catalog: CatalogRecord::try_from(&default)?,
        };

        let event_id = self
            .tier1
            .append_ledger_event(
                guard,
                CatalogDomain::Catalog,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
            )
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let response = compactor.sync_compact(request).await?;

        Ok(DefaultCatalogOutcome {
            catalog: CatalogRecord::try_from(&default)?,
            repair_pending: response.repair_pending,
        })
    }

    // ========================================================================
    // Initialization
    // ========================================================================

    /// Initializes the catalog manifest scaffolding.
    ///
    /// Creates:
    /// - All manifest files (root, catalog, lineage, executions, search)
    ///
    /// Parquet snapshots are written by the compactor on first sync compaction.
    ///
    /// Idempotent: safe to call multiple times.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail.
    pub async fn initialize(&self) -> Result<()> {
        // Only create manifest scaffolding. Parquet snapshots are written by the compactor.
        self.tier1.initialize().await
    }

    // ========================================================================
    // Catalogs (Tier 1 - catalog domain)
    // ========================================================================

    /// Creates a new catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A catalog with this name already exists
    /// - Lock acquisition fails
    /// - Storage operations fail
    pub async fn create_catalog(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        self.create_catalog_with_metadata(name, description, None, None, opts)
            .await
    }

    /// Creates a new catalog with authoritative UC metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog already exists or storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn create_catalog_with_metadata(
        &self,
        name: &str,
        description: Option<&str>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        let request_hash = Self::idempotency_request_hash(&serde_json::json!({
            "name": name,
            "description": description,
            "properties": properties,
            "storage_root": storage_root,
            "if_match": opts.if_match.map(|version| version.as_u64()),
        }))?;
        let idempotency_store = self.idempotency_store();
        let mut idempotency = match check_idempotency(
            &idempotency_store,
            opts.idempotency_key.as_ref().map(IdempotencyKey::as_str),
            CatalogOperation::CreateCatalog,
            &request_hash,
            DEFAULT_STALE_TIMEOUT,
        )
        .await?
        {
            IdempotencyCheck::NoKey => None,
            IdempotencyCheck::Proceed { marker, version } => Some((marker, version)),
            IdempotencyCheck::StaleReserved { marker, version } => {
                if let Some(catalog) = self.recover_reserved_catalog(&marker, &version).await? {
                    return Ok(catalog);
                }
                Some(
                    self.refresh_stale_reserved_idempotency(*marker, version)
                        .await?,
                )
            }
            IdempotencyCheck::Replay { entity_name, .. } => {
                return self.replay_catalog_by_name(&entity_name).await;
            }
            IdempotencyCheck::Conflict => return Err(Self::idempotency_conflict_error()),
            IdempotencyCheck::PreviousFailed {
                http_status,
                message,
            } => return Err(Self::idempotency_request_failed(http_status, message)),
            IdempotencyCheck::InProgress { .. } => {
                return Err(Self::idempotency_in_progress_error());
            }
        };
        let catalog_id = idempotency
            .as_ref()
            .and_then(|(marker, _)| marker.entity_id.clone())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        if let Some((marker, version)) = idempotency.take() {
            idempotency = Some(
                self.reserve_idempotency_entity(marker, version, &catalog_id, name)
                    .await?,
            );
        }
        let result = async {
            // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
            if let Some(expected) = &opts.if_match {
                let manifest = self.tier1.read_manifest().await?;
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }

            let now = Utc::now().timestamp_millis();
            let catalog = Catalog {
                id: catalog_id.clone(),
                name: name.to_string(),
                description: description.map(String::from),
                properties,
                storage_root: storage_root.map(String::from),
                created_at: now,
                updated_at: now,
            };

            let compactor = self.sync_compactor()?;

            // Acquire lock and append ledger event.
            let guard = self
                .tier1
                .acquire_lock(self.lock_ttl, self.lock_max_retries)
                .await?;

            let manifest = self.tier1.read_manifest().await?;
            if let Some(expected) = &opts.if_match {
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    guard.release().await?;
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }
            let state =
                tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path)
                    .await?;

            // Check for duplicate
            if state.catalogs.iter().any(|c| c.name == name) {
                guard.release().await?;
                return Err(CatalogError::AlreadyExists {
                    entity: "catalog".into(),
                    name: name.to_string(),
                });
            }

            let event = CatalogDdlEventV2::CatalogCreated {
                catalog: CatalogRecord::try_from(&catalog)?,
            };

            let event_id = self
                .tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?;

            let request = self.single_event_sync_compact_request(
                CatalogDomain::Catalog,
                &event_id,
                guard.fencing_token().sequence(),
                opts.request_id.clone(),
            );

            let result = compactor.sync_compact(request).await;
            if let Err(error) = guard.release().await {
                if result.is_ok() {
                    tracing::warn!(
                        error = %error,
                        catalog = %catalog.name,
                        "catalog create published visibly but lock release failed"
                    );
                } else {
                    return Err(error.into());
                }
            }
            result?;

            Ok(catalog)
        }
        .await;

        match (idempotency, result) {
            (Some((marker, version)), Ok(catalog)) => {
                self.finalize_idempotency_success(marker, version, &catalog.id, &catalog.name)
                    .await?;
                Ok(catalog)
            }
            (Some((marker, version)), Err(err)) => {
                self.finalize_idempotency_failure(marker, version, &err)
                    .await?;
                Err(err)
            }
            (None, result) => result,
        }
    }

    /// Creates a new catalog and returns visible commit metadata for transaction APIs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::create_catalog`].
    pub async fn create_catalog_transaction(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::CreateCatalog {
                catalog: name.to_string(),
                description: description.map(str::to_string),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let now = Utc::now().timestamp_millis();
        let catalog = Catalog {
            id: Uuid::now_v7().to_string(),
            name: name.to_string(),
            description: description.map(String::from),
            properties: None,
            storage_root: None,
            created_at: now,
            updated_at: now,
        };

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        if state.catalogs.iter().any(|existing| existing.name == name) {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "catalog".into(),
                name: name.to_string(),
            });
        }

        let event = CatalogDdlEventV2::CatalogCreated {
            catalog: CatalogRecord::try_from(&catalog)?,
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
    }

    /// Updates catalog metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog doesn't exist or storage operations fail.
    pub async fn update_catalog(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        self.patch_catalog(
            name,
            CatalogPatch {
                description: Some(description.map(str::to_string)),
                ..CatalogPatch::default()
            },
            opts,
        )
        .await
    }

    /// Applies an authoritative UC metadata patch to a catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog doesn't exist, a rename conflicts, or storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn patch_catalog(
        &self,
        name: &str,
        patch: CatalogPatch,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let existing = state
            .catalogs
            .iter()
            .find(|catalog| catalog.name == name)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "catalog".into(),
                name: name.to_string(),
            })?;

        if name == "default" && patch.new_name.is_some() {
            guard.release().await?;
            return Err(CatalogError::Validation {
                message: "default catalog cannot be renamed".to_string(),
            });
        }

        if patch.new_name.as_deref() == Some("default") && name != "default" {
            guard.release().await?;
            return Err(CatalogError::Validation {
                message: "catalogs cannot be renamed to reserved name 'default'".to_string(),
            });
        }

        let existing_catalog = Catalog::try_from(existing.clone())?;
        let next_name = patch
            .new_name
            .clone()
            .unwrap_or_else(|| existing_catalog.name.clone());

        if next_name != existing_catalog.name
            && state
                .catalogs
                .iter()
                .any(|catalog| catalog.name == next_name)
        {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "catalog".into(),
                name: next_name,
            });
        }

        let now = Utc::now().timestamp_millis();
        let catalog = Catalog {
            id: existing_catalog.id.clone(),
            name: next_name,
            description: patch
                .description
                .unwrap_or_else(|| existing_catalog.description.clone()),
            properties: patch
                .properties
                .unwrap_or_else(|| existing_catalog.properties.clone()),
            storage_root: patch
                .storage_root
                .unwrap_or_else(|| existing_catalog.storage_root.clone()),
            created_at: existing_catalog.created_at,
            updated_at: now,
        };

        if catalog.name == existing_catalog.name
            && catalog.description == existing_catalog.description
            && catalog.properties == existing_catalog.properties
            && catalog.storage_root == existing_catalog.storage_root
        {
            guard.release().await?;
            return Ok(existing_catalog);
        }

        let event_id = if catalog.name == existing_catalog.name {
            let event = CatalogDdlEventV3::CatalogUpdated {
                catalog: CatalogRecord::try_from(&catalog)?,
            };
            self.tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?
        } else {
            let event = CatalogDdlEventV4::CatalogRenamed {
                catalog: CatalogRecord::try_from(&catalog)?,
                old_name: existing_catalog.name,
            };
            self.tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?
        };

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| catalog)
    }

    /// Deletes a catalog, optionally cascading through schemas and tables.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog doesn't exist or storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn delete_catalog(&self, name: &str, force: bool, opts: WriteOptions) -> Result<()> {
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let target_catalog = state
            .catalogs
            .iter()
            .find(|catalog| catalog.name == name)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "catalog".into(),
                name: name.to_string(),
            })?
            .clone();

        let default_catalog_id = state
            .catalogs
            .iter()
            .find(|catalog| catalog.name == "default")
            .map(|catalog| catalog.id.as_str());
        let target_catalog_id = target_catalog.id.as_str();

        let mut namespaces: Vec<_> = state
            .namespaces
            .iter()
            .filter(|namespace| {
                namespace.catalog_id.as_deref().or(default_catalog_id) == Some(target_catalog_id)
            })
            .cloned()
            .collect();
        namespaces.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

        let namespace_ids = namespaces
            .iter()
            .map(|namespace| namespace.id.clone())
            .collect::<std::collections::HashSet<_>>();
        let mut tables: Vec<_> = state
            .tables
            .iter()
            .filter(|table| namespace_ids.contains(&table.namespace_id))
            .cloned()
            .collect();
        tables.sort_by(|left, right| {
            left.namespace_id
                .cmp(&right.namespace_id)
                .then(left.name.cmp(&right.name))
                .then(left.id.cmp(&right.id))
        });

        if (!tables.is_empty() || !namespaces.is_empty()) && !force {
            guard.release().await?;
            return Err(CatalogError::Validation {
                message: format!("catalog '{name}' contains schemas, cannot delete"),
            });
        }

        let mut event_ids = Vec::with_capacity(tables.len() + namespaces.len() + 1);
        let mut previous_event_id = None;
        for table in tables {
            let event = CatalogDdlEvent::TableDropped {
                table_id: table.id,
                namespace_id: table.namespace_id,
                table_name: table.name,
            };
            let event_id = self
                .tier1
                .append_ledger_event_after(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                    previous_event_id,
                )
                .await?;
            previous_event_id = Some(event_id);
            event_ids.push(event_id.to_string());
        }

        for namespace in namespaces {
            let event = CatalogDdlEvent::NamespaceDeleted {
                namespace_id: namespace.id,
                namespace_name: namespace.name,
            };
            let event_id = self
                .tier1
                .append_ledger_event_after(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                    previous_event_id,
                )
                .await?;
            previous_event_id = Some(event_id);
            event_ids.push(event_id.to_string());
        }

        let event = CatalogDdlEventV3::CatalogDeleted {
            catalog_id: target_catalog.id,
            catalog_name: target_catalog.name,
        };
        let event_id = self
            .tier1
            .append_ledger_event_after(
                &guard,
                CatalogDomain::Catalog,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
                previous_event_id,
            )
            .await?;
        event_ids.push(event_id.to_string());

        let request = self.multi_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_ids,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| ())
    }

    // ========================================================================
    // Namespaces (Tier 1 - catalog domain)
    // ========================================================================

    /// Creates a new schema within a catalog.
    ///
    /// In the UC-like model, "schemas" are the same underlying entity as legacy
    /// namespaces, but scoped to a catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The catalog doesn't exist
    /// - A schema with this name already exists
    /// - Lock acquisition fails
    /// - Storage operations fail
    pub async fn create_schema(
        &self,
        catalog: &str,
        schema: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        self.create_schema_with_metadata(catalog, schema, description, None, None, opts)
            .await
    }

    /// Creates a new schema within a catalog with authoritative UC metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog or schema is invalid or storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn create_schema_with_metadata(
        &self,
        catalog: &str,
        schema: &str,
        description: Option<&str>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        let request_hash = Self::idempotency_request_hash(&serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "description": description,
            "properties": properties,
            "storage_root": storage_root,
            "if_match": opts.if_match.map(|version| version.as_u64()),
        }))?;
        let idempotency_store = self.idempotency_store();
        let mut idempotency = match check_idempotency(
            &idempotency_store,
            opts.idempotency_key.as_ref().map(IdempotencyKey::as_str),
            CatalogOperation::CreateSchema,
            &request_hash,
            DEFAULT_STALE_TIMEOUT,
        )
        .await?
        {
            IdempotencyCheck::NoKey => None,
            IdempotencyCheck::Proceed { marker, version } => Some((marker, version)),
            IdempotencyCheck::StaleReserved { marker, version } => {
                if let Some(schema) = self
                    .recover_reserved_schema(catalog, &marker, &version)
                    .await?
                {
                    return Ok(schema);
                }
                Some(
                    self.refresh_stale_reserved_idempotency(*marker, version)
                        .await?,
                )
            }
            IdempotencyCheck::Replay { entity_name, .. } => {
                return self.replay_schema_by_name(catalog, &entity_name).await;
            }
            IdempotencyCheck::Conflict => return Err(Self::idempotency_conflict_error()),
            IdempotencyCheck::PreviousFailed {
                http_status,
                message,
            } => return Err(Self::idempotency_request_failed(http_status, message)),
            IdempotencyCheck::InProgress { .. } => {
                return Err(Self::idempotency_in_progress_error());
            }
        };
        let schema_id = idempotency
            .as_ref()
            .and_then(|(marker, _)| marker.entity_id.clone())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        if let Some((marker, version)) = idempotency.take() {
            idempotency = Some(
                self.reserve_idempotency_entity(marker, version, &schema_id, schema)
                    .await?,
            );
        }
        let result = async {
            // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
            if let Some(expected) = &opts.if_match {
                let manifest = self.tier1.read_manifest().await?;
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }

            let compactor = self.sync_compactor()?;

            let guard = self
                .tier1
                .acquire_lock(self.lock_ttl, self.lock_max_retries)
                .await?;

            let manifest = self.tier1.read_manifest().await?;
            if let Some(expected) = &opts.if_match {
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    guard.release().await?;
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }
            let state =
                tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path)
                    .await?;

            let Some(catalog_record) = state.catalogs.iter().find(|c| c.name == catalog) else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };

            // Check for duplicate within the catalog (UC semantics).
            let default_catalog_id = state
                .catalogs
                .iter()
                .find(|c| c.name == "default")
                .map(|c| c.id.as_str());
            let catalog_id = catalog_record.id.as_str();
            if state.namespaces.iter().any(|ns| {
                ns.name == schema
                    && ns.catalog_id.as_deref().or(default_catalog_id) == Some(catalog_id)
            }) {
                guard.release().await?;
                return Err(CatalogError::AlreadyExists {
                    entity: "namespace".into(),
                    name: schema.to_string(),
                });
            }

            let now = Utc::now().timestamp_millis();
            let namespace = Namespace {
                id: schema_id.clone(),
                catalog_id: Some(catalog_record.id.clone()),
                name: schema.to_string(),
                description: description.map(String::from),
                properties,
                storage_root: storage_root.map(String::from),
                created_at: now,
                updated_at: now,
            };

            let event = CatalogDdlEvent::NamespaceCreated {
                namespace: NamespaceRecord::try_from(&namespace)?,
            };

            let event_id = self
                .tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?;

            let request = self.single_event_sync_compact_request(
                CatalogDomain::Catalog,
                &event_id,
                guard.fencing_token().sequence(),
                opts.request_id.clone(),
            );

            let result = compactor.sync_compact(request).await;
            guard.release().await?;
            result.map(|_| namespace)
        }
        .await;

        match (idempotency, result) {
            (Some((marker, version)), Ok(schema)) => {
                self.finalize_idempotency_success(marker, version, &schema.id, &schema.name)
                    .await?;
                Ok(schema)
            }
            (Some((marker, version)), Err(err)) => {
                self.finalize_idempotency_failure(marker, version, &err)
                    .await?;
                Err(err)
            }
            (None, result) => result,
        }
    }

    /// Creates a schema and returns visible commit metadata for transaction APIs.
    ///
    /// The `default` catalog uses the same metadata-preserving path as named catalogs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::create_schema`].
    #[allow(clippy::too_many_lines)]
    pub async fn create_schema_transaction(
        &self,
        catalog: &str,
        schema: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::CreateSchema {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                description: description.map(str::to_string),
            },
        )?;
        if catalog == "default" {
            return self
                .create_namespace_transaction(schema, description, opts)
                .await;
        }

        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
        else {
            guard.release().await?;
            return Err(CatalogError::NotFound {
                entity: "catalog".into(),
                name: catalog.to_string(),
            });
        };

        let default_catalog_id = state
            .catalogs
            .iter()
            .find(|record| record.name == "default")
            .map(|record| record.id.as_str());
        let catalog_id = catalog_record.id.as_str();
        if state.namespaces.iter().any(|namespace| {
            namespace.name == schema
                && namespace.catalog_id.as_deref().or(default_catalog_id) == Some(catalog_id)
        }) {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "namespace".into(),
                name: schema.to_string(),
            });
        }

        let now = Utc::now().timestamp_millis();
        let namespace = Namespace {
            id: Uuid::now_v7().to_string(),
            catalog_id: Some(catalog_record.id.clone()),
            name: schema.to_string(),
            description: description.map(String::from),
            properties: None,
            storage_root: None,
            created_at: now,
            updated_at: now,
        };
        let event = CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord::try_from(&namespace)?,
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
    }

    /// Creates a new namespace.
    ///
    /// # Arguments
    ///
    /// * `name` - Namespace name (must be unique within workspace)
    /// * `description` - Optional description
    /// * `opts` - Write options (idempotency, optimistic locking)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A namespace with this name already exists
    /// - Lock acquisition fails
    /// - Storage operations fail
    pub async fn create_namespace(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        // Acquire lock and append ledger event
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        if Self::find_default_namespace(&state, name).is_some() {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "namespace".into(),
                name: name.to_string(),
            });
        }

        let default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let now = Utc::now().timestamp_millis();
        let namespace = Namespace {
            id: Uuid::now_v7().to_string(),
            catalog_id: Some(default_catalog.id),
            name: name.to_string(),
            description: description.map(String::from),
            properties: None,
            storage_root: None,
            created_at: now,
            updated_at: now,
        };

        let event = CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord::try_from(&namespace)?,
        };

        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| namespace)
    }

    /// Creates a namespace and returns visible commit metadata for transaction APIs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::create_namespace`].
    pub async fn create_namespace_transaction(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::CreateSchema {
                catalog: "default".to_string(),
                schema: name.to_string(),
                description: description.map(str::to_string),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        if Self::find_default_namespace(&state, name).is_some() {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "namespace".into(),
                name: name.to_string(),
            });
        }

        let default_catalog = match self
            .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_repair_pending = default_catalog.repair_pending;
        let default_catalog = default_catalog.catalog;

        let now = Utc::now().timestamp_millis();
        let namespace = Namespace {
            id: Uuid::now_v7().to_string(),
            catalog_id: Some(default_catalog.id),
            name: name.to_string(),
            description: description.map(String::from),
            properties: None,
            storage_root: None,
            created_at: now,
            updated_at: now,
        };
        let event = CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord::try_from(&namespace)?,
        };

        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit
        })
    }

    /// Updates a schema's description within a catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog or schema doesn't exist.
    pub async fn update_schema_in_catalog(
        &self,
        catalog: &str,
        schema: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        self.patch_schema_in_catalog(
            catalog,
            schema,
            SchemaPatch {
                description: Some(description.map(str::to_string)),
                ..SchemaPatch::default()
            },
            opts,
        )
        .await
    }

    /// Applies an authoritative UC metadata patch to a schema within a catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema doesn't exist, a rename conflicts, or storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn patch_schema_in_catalog(
        &self,
        catalog: &str,
        schema: &str,
        patch: SchemaPatch,
        opts: WriteOptions,
    ) -> Result<Schema> {
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let target_catalog = if catalog == "default" {
            match self
                .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
                .await
            {
                Ok(catalog) => catalog,
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            }
        } else {
            let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
            else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };
            catalog_record.clone()
        };

        let default_catalog_id = if catalog == "default" {
            Some(target_catalog.id.clone())
        } else {
            state
                .catalogs
                .iter()
                .find(|record| record.name == "default")
                .map(|record| record.id.clone())
        };
        let target_catalog_id = target_catalog.id.as_str();
        let default_catalog_id_ref = default_catalog_id.as_deref();
        let existing = state
            .namespaces
            .iter()
            .find(|candidate| {
                candidate.name == schema
                    && candidate.catalog_id.as_deref().or(default_catalog_id_ref)
                        == Some(target_catalog_id)
            })
            .ok_or_else(|| CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            })?;

        let existing_schema = Schema::try_from(existing.clone())?;
        let next_name = patch
            .new_name
            .clone()
            .unwrap_or_else(|| existing_schema.name.clone());

        if next_name != existing_schema.name
            && state.namespaces.iter().any(|candidate| {
                candidate.name == next_name
                    && candidate.catalog_id.as_deref().or(default_catalog_id_ref)
                        == Some(target_catalog_id)
            })
        {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "schema".into(),
                name: format!("{catalog}.{next_name}"),
            });
        }

        let now = Utc::now().timestamp_millis();
        let namespace = Namespace {
            id: existing_schema.id.clone(),
            catalog_id: Some(target_catalog.id.clone()),
            name: next_name,
            description: patch
                .description
                .unwrap_or_else(|| existing_schema.description.clone()),
            properties: patch
                .properties
                .unwrap_or_else(|| existing_schema.properties.clone()),
            storage_root: patch
                .storage_root
                .unwrap_or_else(|| existing_schema.storage_root.clone()),
            created_at: existing_schema.created_at,
            updated_at: now,
        };

        if namespace.name == existing_schema.name
            && namespace.description == existing_schema.description
            && namespace.properties == existing_schema.properties
            && namespace.storage_root == existing_schema.storage_root
        {
            guard.release().await?;
            return Ok(existing_schema);
        }

        let event_id = if namespace.name == existing_schema.name {
            let event = CatalogDdlEvent::NamespaceUpdated {
                namespace: NamespaceRecord::try_from(&namespace)?,
            };
            self.tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?
        } else {
            let event = CatalogDdlEventV4::NamespaceRenamed {
                namespace: NamespaceRecord::try_from(&namespace)?,
                old_name: existing_schema.name,
            };
            self.tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?
        };

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| namespace)
    }

    /// Deletes a schema within a catalog, optionally cascading through tables.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog or schema doesn't exist.
    #[allow(clippy::too_many_lines)]
    pub async fn delete_schema_in_catalog(
        &self,
        catalog: &str,
        schema: &str,
        force: bool,
        opts: WriteOptions,
    ) -> Result<()> {
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let (catalog_id, default_catalog_id) = if catalog == "default" {
            let default_catalog = match self
                .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
                .await
            {
                Ok(catalog_record) => catalog_record,
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            };
            let default_catalog_id = default_catalog.id.clone();
            (default_catalog.id, Some(default_catalog_id))
        } else {
            let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
            else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };

            (
                catalog_record.id.clone(),
                state
                    .catalogs
                    .iter()
                    .find(|record| record.name == "default")
                    .map(|record| record.id.clone()),
            )
        };

        let default_catalog_id_ref = default_catalog_id.as_deref();
        let namespace = state
            .namespaces
            .iter()
            .find(|candidate| {
                candidate.name == schema
                    && candidate.catalog_id.as_deref().or(default_catalog_id_ref)
                        == Some(catalog_id.as_str())
            })
            .ok_or_else(|| CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            })?
            .clone();

        let mut tables: Vec<_> = state
            .tables
            .iter()
            .filter(|table| table.namespace_id == namespace.id)
            .cloned()
            .collect();
        tables.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

        if !tables.is_empty() && !force {
            guard.release().await?;
            return Err(CatalogError::Validation {
                message: format!("schema '{catalog}.{schema}' contains tables, cannot delete"),
            });
        }

        let mut event_ids = Vec::with_capacity(tables.len() + 1);
        let mut previous_event_id = None;
        for table in tables {
            let event = CatalogDdlEvent::TableDropped {
                table_id: table.id,
                namespace_id: table.namespace_id,
                table_name: table.name,
            };
            let event_id = self
                .tier1
                .append_ledger_event_after(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                    previous_event_id,
                )
                .await?;
            previous_event_id = Some(event_id);
            event_ids.push(event_id.to_string());
        }

        let event = CatalogDdlEvent::NamespaceDeleted {
            namespace_id: namespace.id,
            namespace_name: namespace.name,
        };
        let event_id = self
            .tier1
            .append_ledger_event_after(
                &guard,
                CatalogDomain::Catalog,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
                previous_event_id,
            )
            .await?;
        event_ids.push(event_id.to_string());

        let request = self.multi_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_ids,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| ())
    }

    /// Updates a namespace's description.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist or storage operations fail.
    pub async fn update_namespace(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_id = default_catalog.id.as_str();

        let existing = state
            .namespaces
            .iter()
            .find(|ns| {
                ns.name == name
                    && ns.catalog_id.as_deref().unwrap_or(default_catalog_id) == default_catalog_id
            })
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: name.to_string(),
            })?;

        let now = Utc::now().timestamp_millis();
        let namespace = Namespace {
            id: existing.id.clone(),
            catalog_id: Some(default_catalog.id),
            name: existing.name.clone(),
            description: description.map(String::from),
            created_at: existing.created_at,
            updated_at: now,
            properties: decode_uc_properties(existing.properties_json.clone())?,
            storage_root: existing.storage_root.clone(),
        };

        let event = CatalogDdlEvent::NamespaceUpdated {
            namespace: NamespaceRecord::try_from(&namespace)?,
        };

        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| namespace)
    }

    /// Deletes a namespace.
    ///
    /// # Arguments
    ///
    /// * `name` - Namespace name
    /// * `opts` - Write options
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Namespace doesn't exist
    /// - Namespace contains tables (must be empty)
    /// - Lock acquisition or storage operations fail
    pub async fn delete_namespace(&self, name: &str, opts: WriteOptions) -> Result<()> {
        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_id = default_catalog.id.as_str();

        // Find namespace in the default catalog (legacy route semantics).
        let ns_idx = state
            .namespaces
            .iter()
            .position(|ns| {
                ns.name == name
                    && ns.catalog_id.as_deref().unwrap_or(default_catalog_id) == default_catalog_id
            })
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: name.to_string(),
            })?;

        let ns_id = &state.namespaces[ns_idx].id;

        // Check if namespace has tables
        if state.tables.iter().any(|t| &t.namespace_id == ns_id) {
            guard.release().await?;
            return Err(CatalogError::Validation {
                message: format!("namespace '{}' contains tables, cannot delete", name),
            });
        }

        let event = CatalogDdlEvent::NamespaceDeleted {
            namespace_id: ns_id.clone(),
            namespace_name: name.to_string(),
        };

        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| ())
    }

    // ========================================================================
    // Tables (Tier 1 - catalog domain)
    // ========================================================================

    /// Registers a new table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Namespace doesn't exist
    /// - Table name already exists in namespace
    /// - Lock acquisition or storage operations fail
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn register_table(
        &self,
        req: RegisterTableRequest,
        opts: WriteOptions,
    ) -> Result<Table> {
        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let ns = match Self::find_default_namespace(&state, &req.namespace) {
            Some(namespace) => namespace,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: req.namespace.clone(),
                });
            }
        };
        let namespace_id = ns.id.clone();

        if state
            .tables
            .iter()
            .any(|t| t.namespace_id == namespace_id && t.name == req.name)
        {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "table".into(),
                name: format!("{}.{}", req.namespace, req.name),
            });
        }

        let _default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let now = Utc::now().timestamp_millis();
        let table_id = Uuid::now_v7().to_string();
        let table_format = normalize_new_table_format(req.format.as_deref())?;
        let table_format_kind = TableFormat::parse(&table_format).map_err(CatalogError::from)?;
        let table_location = normalize_table_location_for_write(
            table_format_kind,
            req.location.clone(),
            self.storage.tenant_id(),
            self.storage.workspace_id(),
        )?;

        let table = Table {
            id: table_id.clone(),
            namespace_id: namespace_id.clone(),
            name: req.name.clone(),
            description: req.description.clone(),
            location: table_location,
            format: Some(table_format),
            table_type: None,
            properties: None,
            created_at: now,
            updated_at: now,
        };

        let columns: Vec<ColumnRecord> = req
            .columns
            .iter()
            .map(|col_def| ColumnRecord {
                id: Uuid::now_v7().to_string(),
                table_id: table_id.clone(),
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                is_nullable: col_def.is_nullable,
                ordinal: col_def.ordinal,
                description: col_def.description.clone(),
            })
            .collect();

        let event = CatalogDdlEvent::TableRegistered {
            table: TableRecord::try_from(&table)?,
            columns,
        };

        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| table)
    }

    /// Registers a table and returns visible commit metadata for transaction APIs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::register_table`].
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn register_table_transaction(
        &self,
        req: RegisterTableRequest,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::RegisterTable {
                catalog: "default".to_string(),
                schema: req.namespace.clone(),
                table: req.name.clone(),
                description: req.description.clone(),
                location: req.location.clone(),
                format: req.format.clone(),
                columns: req.columns.clone(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let ns = match Self::find_default_namespace(&state, &req.namespace) {
            Some(namespace) => namespace,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: req.namespace.clone(),
                });
            }
        };
        let namespace_id = ns.id.clone();

        if state
            .tables
            .iter()
            .any(|t| t.namespace_id == namespace_id && t.name == req.name)
        {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "table".into(),
                name: format!("{}.{}", req.namespace, req.name),
            });
        }

        let default_catalog = match self
            .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_repair_pending = default_catalog.repair_pending;

        let now = Utc::now().timestamp_millis();
        let table_id = Uuid::now_v7().to_string();
        let table_format = normalize_new_table_format(req.format.as_deref())?;
        let table_format_kind = TableFormat::parse(&table_format).map_err(CatalogError::from)?;
        let table_location = normalize_table_location_for_write(
            table_format_kind,
            req.location.clone(),
            self.storage.tenant_id(),
            self.storage.workspace_id(),
        )?;

        let table = Table {
            id: table_id.clone(),
            namespace_id: namespace_id.clone(),
            name: req.name.clone(),
            description: req.description.clone(),
            location: table_location,
            format: Some(table_format),
            table_type: None,
            properties: None,
            created_at: now,
            updated_at: now,
        };

        let columns: Vec<ColumnRecord> = req
            .columns
            .iter()
            .map(|col_def| ColumnRecord {
                id: Uuid::now_v7().to_string(),
                table_id: table_id.clone(),
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                is_nullable: col_def.is_nullable,
                ordinal: col_def.ordinal,
                description: col_def.description.clone(),
            })
            .collect();

        let event = CatalogDdlEvent::TableRegistered {
            table: TableRecord::try_from(&table)?,
            columns,
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit
        })
    }

    /// Registers a new table under a UC-like catalog + schema.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Catalog doesn't exist (unless `catalog == "default"`, which is auto-created)
    /// - Schema doesn't exist within the catalog
    /// - Table name already exists in schema
    /// - Lock acquisition or storage operations fail
    #[allow(clippy::too_many_lines)]
    pub async fn register_table_in_schema(
        &self,
        catalog: &str,
        schema: &str,
        req: RegisterTableInSchemaRequest,
        opts: WriteOptions,
    ) -> Result<Table> {
        let request_hash = Self::idempotency_request_hash(&serde_json::json!({
            "catalog": catalog,
            "schema": schema,
            "name": req.name.clone(),
            "description": req.description.clone(),
            "location": req.location.clone(),
            "format": req.format.clone(),
            "table_type": req.table_type.clone(),
            "properties": req.properties.clone(),
            "columns": req.columns.iter().map(|column| serde_json::json!({
                "name": column.name.clone(),
                "data_type": column.data_type.clone(),
                "is_nullable": column.is_nullable,
                "ordinal": column.ordinal,
                "description": column.description.clone(),
            })).collect::<Vec<_>>(),
            "if_match": opts.if_match.map(|version| version.as_u64()),
        }))?;
        let idempotency_store = self.idempotency_store();
        let mut idempotency = match check_idempotency(
            &idempotency_store,
            opts.idempotency_key.as_ref().map(IdempotencyKey::as_str),
            CatalogOperation::RegisterTableInSchema,
            &request_hash,
            DEFAULT_STALE_TIMEOUT,
        )
        .await?
        {
            IdempotencyCheck::NoKey => None,
            IdempotencyCheck::Proceed { marker, version } => Some((marker, version)),
            IdempotencyCheck::StaleReserved { marker, version } => {
                if let Some(table) = self
                    .recover_reserved_table(catalog, schema, &marker, &version)
                    .await?
                {
                    return Ok(table);
                }
                Some(
                    self.refresh_stale_reserved_idempotency(*marker, version)
                        .await?,
                )
            }
            IdempotencyCheck::Replay { entity_name, .. } => {
                return self
                    .replay_table_by_name(catalog, schema, &entity_name)
                    .await;
            }
            IdempotencyCheck::Conflict => return Err(Self::idempotency_conflict_error()),
            IdempotencyCheck::PreviousFailed {
                http_status,
                message,
            } => return Err(Self::idempotency_request_failed(http_status, message)),
            IdempotencyCheck::InProgress { .. } => {
                return Err(Self::idempotency_in_progress_error());
            }
        };
        let table_id = idempotency
            .as_ref()
            .and_then(|(marker, _)| marker.entity_id.clone())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        if let Some((marker, version)) = idempotency.take() {
            idempotency = Some(
                self.reserve_idempotency_entity(marker, version, &table_id, &req.name)
                    .await?,
            );
        }
        let result = async {
            // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
            if let Some(expected) = &opts.if_match {
                let manifest = self.tier1.read_manifest().await?;
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }

            let compactor = self.sync_compactor()?;

            let guard = self
                .tier1
                .acquire_lock(self.lock_ttl, self.lock_max_retries)
                .await?;

            let manifest = self.tier1.read_manifest().await?;
            if let Some(expected) = &opts.if_match {
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    guard.release().await?;
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }
            let state =
                tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path)
                    .await?;

            let target_catalog = if catalog == "default" {
                match self
                    .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
                    .await
                {
                    Ok(catalog) => catalog,
                    Err(err) => {
                        guard.release().await?;
                        return Err(err);
                    }
                }
            } else {
                let Some(catalog_record) = state.catalogs.iter().find(|c| c.name == catalog) else {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "catalog".into(),
                        name: catalog.to_string(),
                    });
                };

                catalog_record.clone()
            };

            let target_catalog_id = target_catalog.id.as_str();
            let default_catalog_id = state
                .catalogs
                .iter()
                .find(|c| c.name == "default")
                .map(|c| c.id.as_str());

            // Find schema within the requested catalog, treating legacy `NULL` catalog_id as `default`.
            let ns = state
                .namespaces
                .iter()
                .find(|ns| {
                    ns.name == schema
                        && ns.catalog_id.as_deref().or(default_catalog_id)
                            == Some(target_catalog_id)
                })
                .ok_or_else(|| CatalogError::NotFound {
                    entity: "schema".into(),
                    name: format!("{catalog}.{schema}"),
                })?;
            let namespace_id = ns.id.clone();

            // Check for duplicate table
            if state
                .tables
                .iter()
                .any(|t| t.namespace_id == namespace_id && t.name == req.name)
            {
                guard.release().await?;
                return Err(CatalogError::AlreadyExists {
                    entity: "table".into(),
                    name: format!("{catalog}.{schema}.{}", req.name),
                });
            }

            let now = Utc::now().timestamp_millis();
            let table_format = normalize_new_table_format(req.format.as_deref())?;
            let table_format_kind =
                TableFormat::parse(&table_format).map_err(CatalogError::from)?;
            let table_location = normalize_table_location_for_write(
                table_format_kind,
                req.location.clone(),
                self.storage.tenant_id(),
                self.storage.workspace_id(),
            )?;

            let table = Table {
                id: table_id.clone(),
                namespace_id: namespace_id.clone(),
                name: req.name.clone(),
                description: req.description.clone(),
                location: table_location,
                format: Some(table_format),
                table_type: req.table_type.clone(),
                properties: req.properties.clone(),
                created_at: now,
                updated_at: now,
            };

            let columns: Vec<ColumnRecord> = req
                .columns
                .iter()
                .map(|col_def| ColumnRecord {
                    id: Uuid::now_v7().to_string(),
                    table_id: table_id.clone(),
                    name: col_def.name.clone(),
                    data_type: col_def.data_type.clone(),
                    is_nullable: col_def.is_nullable,
                    ordinal: col_def.ordinal,
                    description: col_def.description.clone(),
                })
                .collect();

            let event = CatalogDdlEvent::TableRegistered {
                table: TableRecord::try_from(&table)?,
                columns,
            };

            let event_id = self
                .tier1
                .append_ledger_event(
                    &guard,
                    CatalogDomain::Catalog,
                    &event,
                    opts.actor.as_deref().unwrap_or("api"),
                )
                .await?;

            let request = self.single_event_sync_compact_request(
                CatalogDomain::Catalog,
                &event_id,
                guard.fencing_token().sequence(),
                opts.request_id.clone(),
            );

            let result = compactor.sync_compact(request).await;
            guard.release().await?;
            result.map(|_| table)
        }
        .await;

        match (idempotency, result) {
            (Some((marker, version)), Ok(table)) => {
                self.finalize_idempotency_success(marker, version, &table.id, &table.name)
                    .await?;
                Ok(table)
            }
            (Some((marker, version)), Err(err)) => {
                self.finalize_idempotency_failure(marker, version, &err)
                    .await?;
                Err(err)
            }
            (None, result) => result,
        }
    }

    /// Registers a table in a catalog/schema and returns visible commit metadata.
    ///
    /// The `default` catalog uses the same metadata-preserving path as named catalogs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::register_table_in_schema`].
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn register_table_in_schema_transaction(
        &self,
        catalog: &str,
        schema: &str,
        req: RegisterTableInSchemaRequest,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        if opts.transaction_identity.is_some()
            && (req.table_type.is_some() || req.properties.is_some())
        {
            return Err(CatalogError::InvariantViolation {
                message: "frozen catalog registration contains unreviewed metadata".to_string(),
            });
        }
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::RegisterTable {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: req.name.clone(),
                description: req.description.clone(),
                location: req.location.clone(),
                format: req.format.clone(),
                columns: req.columns.clone(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let (namespace_id, default_catalog_repair_pending) = if catalog == "default" {
            let namespace = match Self::find_default_namespace(&state, schema) {
                Some(namespace) => namespace,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "schema".into(),
                        name: format!("{catalog}.{schema}"),
                    });
                }
            };
            let namespace_id = namespace.id.clone();

            if state
                .tables
                .iter()
                .any(|table| table.namespace_id == namespace_id && table.name == req.name)
            {
                guard.release().await?;
                return Err(CatalogError::AlreadyExists {
                    entity: "table".into(),
                    name: format!("{catalog}.{schema}.{}", req.name),
                });
            }

            match self
                .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
                .await
            {
                Ok(outcome) => (namespace_id, outcome.repair_pending),
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            }
        } else {
            let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
            else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };

            let default_catalog_id = Self::default_catalog_id(&state);
            let catalog_id = catalog_record.id.as_str();
            let namespace = match state.namespaces.iter().find(|candidate| {
                candidate.name == schema
                    && candidate.catalog_id.as_deref().or(default_catalog_id) == Some(catalog_id)
            }) {
                Some(namespace) => namespace,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "schema".into(),
                        name: format!("{catalog}.{schema}"),
                    });
                }
            };
            let namespace_id = namespace.id.clone();

            if state
                .tables
                .iter()
                .any(|table| table.namespace_id == namespace_id && table.name == req.name)
            {
                guard.release().await?;
                return Err(CatalogError::AlreadyExists {
                    entity: "table".into(),
                    name: format!("{catalog}.{schema}.{}", req.name),
                });
            }

            (namespace_id, false)
        };

        let now = Utc::now().timestamp_millis();
        let table_id = Uuid::now_v7().to_string();
        let table_format = normalize_new_table_format(req.format.as_deref())?;
        let table_format_kind = TableFormat::parse(&table_format).map_err(CatalogError::from)?;
        let table_location = normalize_table_location_for_write(
            table_format_kind,
            req.location.clone(),
            self.storage.tenant_id(),
            self.storage.workspace_id(),
        )?;

        let table = Table {
            id: table_id.clone(),
            namespace_id: namespace_id.clone(),
            name: req.name.clone(),
            description: req.description.clone(),
            location: table_location,
            format: Some(table_format),
            table_type: req.table_type.clone(),
            properties: req.properties.clone(),
            created_at: now,
            updated_at: now,
        };
        let columns: Vec<ColumnRecord> = req
            .columns
            .iter()
            .map(|col_def| ColumnRecord {
                id: Uuid::now_v7().to_string(),
                table_id: table_id.clone(),
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                is_nullable: col_def.is_nullable,
                ordinal: col_def.ordinal,
                description: col_def.description.clone(),
            })
            .collect();

        let event = CatalogDdlEvent::TableRegistered {
            table: TableRecord::try_from(&table)?,
            columns,
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit
        })
    }

    /// Updates a table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table doesn't exist
    /// - `opts.if_match` doesn't match current version
    /// - Lock acquisition or storage operations fail
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn update_table(
        &self,
        namespace: &str,
        name: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<Table> {
        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let mut state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let ns = match Self::find_default_namespace(&state, namespace) {
            Some(namespace) => namespace,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: namespace.to_string(),
                });
            }
        };
        let namespace_id = ns.id.clone();

        if !state
            .tables
            .iter()
            .any(|table| table.namespace_id == namespace_id && table.name == name)
        {
            guard.release().await?;
            return Err(CatalogError::NotFound {
                entity: "table".into(),
                name: format!("{}.{}", namespace, name),
            });
        }

        let _default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let table_rec = match state
            .tables
            .iter_mut()
            .find(|table| table.namespace_id == namespace_id && table.name == name)
        {
            Some(table) => table,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{}.{}", namespace, name),
                });
            }
        };

        let now = Utc::now().timestamp_millis();
        table_rec.updated_at = now;

        if let Some(desc) = patch.description {
            table_rec.description = desc;
        }
        if let Some(loc) = patch.location {
            table_rec.location = loc;
        }
        if let Some(fmt) = patch.format {
            table_rec.format = normalize_table_format_patch(fmt)?;
        }

        let effective_format = table_rec
            .format
            .as_deref()
            .map(TableFormat::parse)
            .transpose()
            .map_err(CatalogError::from)?;
        if effective_format == Some(TableFormat::Delta) {
            table_rec.location = normalize_table_location_for_write(
                TableFormat::Delta,
                table_rec.location.clone(),
                self.storage.tenant_id(),
                self.storage.workspace_id(),
            )?;
        }

        let table_record = table_rec.clone();
        let updated_table = Table::try_from(table_record.clone())?;

        let event = CatalogDdlEvent::TableUpdated {
            table: table_record,
        };

        let event_id = self
            .tier1
            .append_ledger_event(
                &guard,
                CatalogDomain::Catalog,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
            )
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| updated_table)
    }

    /// Updates a table and returns visible commit metadata for transaction APIs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::update_table`].
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn update_table_transaction(
        &self,
        namespace: &str,
        name: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::UpdateTable {
                catalog: "default".to_string(),
                schema: namespace.to_string(),
                table: name.to_string(),
                description: patch.description.clone(),
                location: patch.location.clone(),
                format: patch.format.clone(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let mut state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let ns = match Self::find_default_namespace(&state, namespace) {
            Some(namespace) => namespace,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: namespace.to_string(),
                });
            }
        };
        let namespace_id = ns.id.clone();

        if !state
            .tables
            .iter()
            .any(|table| table.namespace_id == namespace_id && table.name == name)
        {
            guard.release().await?;
            return Err(CatalogError::NotFound {
                entity: "table".into(),
                name: format!("{}.{}", namespace, name),
            });
        }

        let default_catalog = match self
            .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_repair_pending = default_catalog.repair_pending;

        let table_rec = match state
            .tables
            .iter_mut()
            .find(|table| table.namespace_id == namespace_id && table.name == name)
        {
            Some(table) => table,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{}.{}", namespace, name),
                });
            }
        };

        let now = Utc::now().timestamp_millis();
        table_rec.updated_at = now;

        if let Some(desc) = patch.description {
            table_rec.description = desc;
        }
        if let Some(loc) = patch.location {
            table_rec.location = loc;
        }
        if let Some(fmt) = patch.format {
            table_rec.format = normalize_table_format_patch(fmt)?;
        }

        let effective_format = table_rec
            .format
            .as_deref()
            .map(TableFormat::parse)
            .transpose()
            .map_err(CatalogError::from)?;
        if effective_format == Some(TableFormat::Delta) {
            table_rec.location = normalize_table_location_for_write(
                TableFormat::Delta,
                table_rec.location.clone(),
                self.storage.tenant_id(),
                self.storage.workspace_id(),
            )?;
        }

        let event = CatalogDdlEvent::TableUpdated {
            table: table_rec.clone(),
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit
        })
    }

    /// Updates a table in a catalog/schema and returns visible commit metadata.
    ///
    /// The `default` catalog uses the same metadata-preserving path as named catalogs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::update_table`].
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn update_table_in_schema_transaction(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::UpdateTable {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: name.to_string(),
                description: patch.description.clone(),
                location: patch.location.clone(),
                format: patch.format.clone(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let mut state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let (namespace_id, default_catalog_repair_pending) = if catalog == "default" {
            let namespace = match Self::find_default_namespace(&state, schema) {
                Some(namespace) => namespace,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "schema".into(),
                        name: format!("{catalog}.{schema}"),
                    });
                }
            };
            let namespace_id = namespace.id.clone();

            if !state
                .tables
                .iter()
                .any(|table| table.namespace_id == namespace_id && table.name == name)
            {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{catalog}.{schema}.{name}"),
                });
            }

            match self
                .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
                .await
            {
                Ok(outcome) => (namespace_id, outcome.repair_pending),
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            }
        } else {
            let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
            else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };

            let default_catalog_id = Self::default_catalog_id(&state);
            let catalog_id = catalog_record.id.as_str();
            let namespace = match state.namespaces.iter().find(|candidate| {
                candidate.name == schema
                    && candidate.catalog_id.as_deref().or(default_catalog_id) == Some(catalog_id)
            }) {
                Some(namespace) => namespace,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "schema".into(),
                        name: format!("{catalog}.{schema}"),
                    });
                }
            };
            let namespace_id = namespace.id.clone();

            if !state
                .tables
                .iter()
                .any(|table| table.namespace_id == namespace_id && table.name == name)
            {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{catalog}.{schema}.{name}"),
                });
            }

            (namespace_id, false)
        };

        let table_rec = match state
            .tables
            .iter_mut()
            .find(|table| table.namespace_id == namespace_id && table.name == name)
        {
            Some(table) => table,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{catalog}.{schema}.{name}"),
                });
            }
        };

        let now = Utc::now().timestamp_millis();
        table_rec.updated_at = now;
        if let Some(description) = patch.description {
            table_rec.description = description;
        }
        if let Some(location) = patch.location {
            table_rec.location = location;
        }
        if let Some(format) = patch.format {
            table_rec.format = normalize_table_format_patch(format)?;
        }

        let effective_format = table_rec
            .format
            .as_deref()
            .map(TableFormat::parse)
            .transpose()
            .map_err(CatalogError::from)?;
        if effective_format == Some(TableFormat::Delta) {
            table_rec.location = normalize_table_location_for_write(
                TableFormat::Delta,
                table_rec.location.clone(),
                self.storage.tenant_id(),
                self.storage.workspace_id(),
            )?;
        }

        let event = CatalogDdlEvent::TableUpdated {
            table: table_rec.clone(),
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit
        })
    }

    /// Drops a table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table doesn't exist
    /// - Lock acquisition or storage operations fail
    #[allow(clippy::manual_let_else, clippy::single_match_else)]
    pub async fn drop_table(&self, namespace: &str, name: &str, opts: WriteOptions) -> Result<()> {
        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let ns = match Self::find_default_namespace(&state, namespace) {
            Some(namespace) => namespace,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: namespace.to_string(),
                });
            }
        };
        let namespace_id = ns.id.clone();

        let table_id = match state
            .tables
            .iter()
            .find(|table| table.namespace_id == namespace_id && table.name == name)
        {
            Some(table) => table.id.clone(),
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{}.{}", namespace, name),
                });
            }
        };

        let _default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let event = CatalogDdlEvent::TableDropped {
            table_id: table_id.clone(),
            namespace_id: namespace_id.clone(),
            table_name: name.to_string(),
        };

        let event_id = self
            .tier1
            .append_ledger_event(
                &guard,
                CatalogDomain::Catalog,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
            )
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| ())
    }

    /// Drops a table and returns visible commit metadata for transaction APIs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::drop_table`].
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn drop_table_transaction(
        &self,
        namespace: &str,
        name: &str,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::DropTable {
                catalog: "default".to_string(),
                schema: namespace.to_string(),
                table: name.to_string(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let ns = match Self::find_default_namespace(&state, namespace) {
            Some(namespace) => namespace,
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: namespace.to_string(),
                });
            }
        };
        let namespace_id = ns.id.clone();

        let table_id = match state
            .tables
            .iter()
            .find(|table| table.namespace_id == namespace_id && table.name == name)
        {
            Some(table) => table.id.clone(),
            None => {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{}.{}", namespace, name),
                });
            }
        };

        let default_catalog = match self
            .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_repair_pending = default_catalog.repair_pending;

        let event = CatalogDdlEvent::TableDropped {
            table_id,
            namespace_id,
            table_name: name.to_string(),
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit
        })
    }

    /// Drops a table in a catalog/schema and returns visible commit metadata.
    ///
    /// The `default` catalog uses the same metadata-preserving path as named catalogs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::drop_table`].
    #[allow(
        clippy::manual_let_else,
        clippy::single_match_else,
        clippy::too_many_lines
    )]
    pub async fn drop_table_in_schema_transaction(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::DropTable {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: name.to_string(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let (namespace_id, dropped_table, default_catalog_repair_pending) = if catalog == "default"
        {
            let namespace = match Self::find_default_namespace(&state, schema) {
                Some(namespace) => namespace,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "schema".into(),
                        name: format!("{catalog}.{schema}"),
                    });
                }
            };
            let namespace_id = namespace.id.clone();
            let table = match state
                .tables
                .iter()
                .find(|table| table.namespace_id == namespace_id && table.name == name)
            {
                Some(table) => table,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "table".into(),
                        name: format!("{catalog}.{schema}.{name}"),
                    });
                }
            };
            let dropped_table = DroppedTableIdentity {
                table_id: table.id.clone(),
                format: table.format.clone(),
            };

            match self
                .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
                .await
            {
                Ok(outcome) => (namespace_id, dropped_table, outcome.repair_pending),
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            }
        } else {
            let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
            else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };

            let default_catalog_id = Self::default_catalog_id(&state);
            let catalog_id = catalog_record.id.as_str();
            let namespace = match state.namespaces.iter().find(|candidate| {
                candidate.name == schema
                    && candidate.catalog_id.as_deref().or(default_catalog_id) == Some(catalog_id)
            }) {
                Some(namespace) => namespace,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "schema".into(),
                        name: format!("{catalog}.{schema}"),
                    });
                }
            };
            let namespace_id = namespace.id.clone();

            let table = match state
                .tables
                .iter()
                .find(|table| table.namespace_id == namespace_id && table.name == name)
            {
                Some(table) => table,
                None => {
                    guard.release().await?;
                    return Err(CatalogError::NotFound {
                        entity: "table".into(),
                        name: format!("{catalog}.{schema}.{name}"),
                    });
                }
            };
            let dropped_table = DroppedTableIdentity {
                table_id: table.id.clone(),
                format: table.format.clone(),
            };

            (namespace_id, dropped_table, false)
        };

        let event = CatalogDdlEvent::TableDropped {
            table_id: dropped_table.table_id.clone(),
            namespace_id,
            table_name: name.to_string(),
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= default_catalog_repair_pending;
            commit.dropped_table = Some(dropped_table);
            commit
        })
    }

    /// Renames a table within the same namespace.
    ///
    /// # Arguments
    ///
    /// * `source_namespace` - Namespace containing the table
    /// * `source_name` - Current table name
    /// * `dest_namespace` - Destination namespace (must match source)
    /// * `dest_name` - New table name
    /// * `opts` - Write options
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Source table doesn't exist
    /// - Source and destination namespaces differ (cross-namespace rename not supported)
    /// - A table with the destination name already exists
    /// - Lock acquisition or storage operations fail
    #[allow(clippy::too_many_lines)]
    pub async fn rename_table(
        &self,
        source_namespace: &str,
        source_name: &str,
        dest_namespace: &str,
        dest_name: &str,
        opts: WriteOptions,
    ) -> Result<Table> {
        // Cross-namespace rename is not supported
        if source_namespace != dest_namespace {
            return Err(CatalogError::UnsupportedOperation {
                message: format!(
                    "cross-namespace rename not supported: '{}' -> '{}'",
                    source_namespace, dest_namespace
                ),
            });
        }

        // No-op if names are the same.
        if source_name == dest_name {
            let compactor = self.sync_compactor()?;

            let guard = self
                .tier1
                .acquire_lock(self.lock_ttl, self.lock_max_retries)
                .await?;

            let manifest = self.tier1.read_manifest().await?;
            if let Some(expected) = &opts.if_match {
                if manifest.catalog.snapshot_version != expected.as_u64() {
                    guard.release().await?;
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "version mismatch: expected {}, got {}",
                            expected.as_u64(),
                            manifest.catalog.snapshot_version
                        ),
                    });
                }
            }

            let state =
                tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path)
                    .await?;

            let default_catalog = match self
                .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
                .await
            {
                Ok(catalog) => catalog,
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            };

            let default_catalog_id = default_catalog.id.as_str();

            let ns = state
                .namespaces
                .iter()
                .find(|ns| {
                    ns.name == source_namespace
                        && ns.catalog_id.as_deref().unwrap_or(default_catalog_id)
                            == default_catalog_id
                })
                .ok_or_else(|| CatalogError::NotFound {
                    entity: "namespace".into(),
                    name: source_namespace.to_string(),
                })?;

            let table = state
                .tables
                .iter()
                .find(|t| t.namespace_id == ns.id && t.name == source_name)
                .ok_or_else(|| CatalogError::NotFound {
                    entity: "table".into(),
                    name: format!("{}.{}", source_namespace, source_name),
                })?;

            let out = Table::try_from(table.clone())?;
            guard.release().await?;
            return Ok(out);
        }

        // Fast optimistic locking check. Revalidated under the Tier-1 lock before writing.
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let default_catalog = match self
            .ensure_default_catalog_locked(&guard, &state, compactor, &opts)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                guard.release().await?;
                return Err(err);
            }
        };

        let default_catalog_id = default_catalog.id.as_str();

        // Find namespace in the default catalog (legacy route semantics).
        let ns = state
            .namespaces
            .iter()
            .find(|ns| {
                ns.name == source_namespace
                    && ns.catalog_id.as_deref().unwrap_or(default_catalog_id) == default_catalog_id
            })
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: source_namespace.to_string(),
            })?;
        let namespace_id = ns.id.clone();

        // Find source table
        let Some(table) = state
            .tables
            .iter()
            .find(|t| t.namespace_id == namespace_id && t.name == source_name)
        else {
            guard.release().await?;
            return Err(CatalogError::NotFound {
                entity: "table".into(),
                name: format!("{}.{}", source_namespace, source_name),
            });
        };

        let table_id = table.id.clone();

        // Check destination name doesn't conflict
        if state
            .tables
            .iter()
            .any(|t| t.namespace_id == namespace_id && t.name == dest_name)
        {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "table".into(),
                name: format!("{}.{}", dest_namespace, dest_name),
            });
        }

        let now = Utc::now().timestamp_millis();

        let event = CatalogDdlEvent::TableRenamed {
            table_id: table_id.clone(),
            namespace_id: namespace_id.clone(),
            old_name: source_name.to_string(),
            new_name: dest_name.to_string(),
            updated_at: now,
        };

        let event_id = self
            .tier1
            .append_ledger_event(
                &guard,
                CatalogDomain::Catalog,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
            )
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;

        // Construct the renamed table for return.
        result.and_then(|_| {
            let existing_table = Table::try_from(table.clone())?;
            Ok(Table {
                id: table_id,
                namespace_id,
                name: dest_name.to_string(),
                description: existing_table.description,
                location: existing_table.location,
                format: existing_table.format,
                table_type: existing_table.table_type,
                properties: existing_table.properties,
                created_at: existing_table.created_at,
                updated_at: now,
            })
        })
    }

    /// Renames a table in a catalog/schema and returns visible commit metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The target catalog or schema doesn't exist
    /// - The source table doesn't exist
    /// - A different table with the destination name already exists
    /// - Lock acquisition or storage operations fail
    #[allow(clippy::too_many_lines)]
    pub async fn rename_table_in_schema_transaction(
        &self,
        catalog: &str,
        schema: &str,
        old_name: &str,
        new_name: &str,
        opts: WriteOptions,
    ) -> Result<CatalogTransactionCommit> {
        let opts = self.bind_catalog_transaction_request(
            opts,
            &CatalogTransactionRequest::RenameTable {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: old_name.to_string(),
                new_table: new_name.to_string(),
            },
        )?;
        if let Some(expected) = &opts.if_match {
            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version != expected.as_u64() {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }

        let compactor = self.sync_compactor()?;
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        if let Some(expected) = &opts.if_match {
            if manifest.catalog.snapshot_version != expected.as_u64() {
                guard.release().await?;
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "version mismatch: expected {}, got {}",
                        expected.as_u64(),
                        manifest.catalog.snapshot_version
                    ),
                });
            }
        }
        let state =
            tier1_state::load_catalog_state(&self.storage, &manifest.catalog.snapshot_path).await?;

        let (catalog_id, repair_pending) = if catalog == "default" {
            match self
                .ensure_default_catalog_locked_with_result(&guard, &state, compactor, &opts)
                .await
            {
                Ok(outcome) => (outcome.catalog.id, outcome.repair_pending),
                Err(err) => {
                    guard.release().await?;
                    return Err(err);
                }
            }
        } else {
            let Some(catalog_record) = state.catalogs.iter().find(|record| record.name == catalog)
            else {
                guard.release().await?;
                return Err(CatalogError::NotFound {
                    entity: "catalog".into(),
                    name: catalog.to_string(),
                });
            };
            (catalog_record.id.clone(), false)
        };

        // Bridge legacy `NULL` catalog_id namespaces onto whichever default catalog
        // ID is authoritative for this transaction, even if the default catalog was
        // bootstrapped after we loaded the pre-transaction snapshot.
        let default_catalog_id = if catalog == "default" {
            Some(catalog_id.as_str())
        } else {
            state
                .catalogs
                .iter()
                .find(|record| record.name == "default")
                .map(|record| record.id.as_str())
        };
        let namespace = state
            .namespaces
            .iter()
            .find(|candidate| {
                candidate.name == schema
                    && candidate.catalog_id.as_deref().or(default_catalog_id)
                        == Some(catalog_id.as_str())
            })
            .ok_or_else(|| CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            })?;
        let namespace_id = namespace.id.clone();

        let Some(table) = state
            .tables
            .iter()
            .find(|candidate| candidate.namespace_id == namespace_id && candidate.name == old_name)
        else {
            guard.release().await?;
            return Err(CatalogError::NotFound {
                entity: "table".into(),
                name: format!("{catalog}.{schema}.{old_name}"),
            });
        };
        let table_id = table.id.clone();

        if old_name != new_name
            && state.tables.iter().any(|candidate| {
                candidate.namespace_id == namespace_id && candidate.name == new_name
            })
        {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "table".into(),
                name: format!("{catalog}.{schema}.{new_name}"),
            });
        }

        let now = Utc::now().timestamp_millis();
        let event = CatalogDdlEvent::TableRenamed {
            table_id,
            namespace_id,
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
            updated_at: now,
        };
        let event_id = self
            .append_catalog_transaction_event(&guard, &event, &opts)
            .await?;
        let event_id_string = event_id.to_string();
        let request = self.single_event_sync_compact_request(
            CatalogDomain::Catalog,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        self.finish_catalog_transaction(
            guard,
            event_id_string,
            compactor.sync_compact(request).await,
        )
        .await
        .map(|mut commit| {
            commit.repair_pending |= repair_pending;
            commit
        })
    }

    // ========================================================================
    // Lineage (Tier 1 - lineage domain, separate lock)
    // ========================================================================

    /// Adds a lineage edge.
    ///
    /// Uses the lineage domain lock (separate from catalog lock).
    ///
    /// # Errors
    ///
    /// Returns an error if lock acquisition or storage operations fail.
    pub async fn add_lineage_edge(
        &self,
        edge: LineageEdge,
        opts: WriteOptions,
    ) -> Result<LineageEdge> {
        let compactor = self.sync_compactor()?;

        let guard = self
            .lineage_lock
            .acquire(self.lock_ttl, self.lock_max_retries)
            .await?;

        let event = LineageDdlEvent::EdgesAdded {
            edges: vec![LineageEdgeRecord::from(&edge)],
        };

        let event_id = self
            .tier1
            .append_ledger_event(
                &guard,
                CatalogDomain::Lineage,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
            )
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Lineage,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| edge)
    }

    /// Adds multiple lineage edges in a single transaction.
    ///
    /// Uses the lineage domain lock (separate from catalog lock).
    ///
    /// # Errors
    ///
    /// Returns an error if lock acquisition or storage operations fail.
    pub async fn add_lineage_edges(
        &self,
        edges: Vec<LineageEdge>,
        opts: WriteOptions,
    ) -> Result<Vec<LineageEdge>> {
        if edges.is_empty() {
            return Ok(Vec::new());
        }

        let compactor = self.sync_compactor()?;

        let guard = self
            .lineage_lock
            .acquire(self.lock_ttl, self.lock_max_retries)
            .await?;

        let event = LineageDdlEvent::EdgesAdded {
            edges: edges.iter().map(LineageEdgeRecord::from).collect(),
        };

        let event_id = self
            .tier1
            .append_ledger_event(
                &guard,
                CatalogDomain::Lineage,
                &event,
                opts.actor.as_deref().unwrap_or("api"),
            )
            .await?;

        let request = self.single_event_sync_compact_request(
            CatalogDomain::Lineage,
            &event_id,
            guard.fencing_token().sequence(),
            opts.request_id.clone(),
        );

        let result = compactor.sync_compact(request).await;
        guard.release().await?;
        result.map(|_| edges)
    }

    // ========================================================================
    // Tier 2 (EventWriter factory)
    // ========================================================================

    /// Creates an [`EventWriter`] for Tier-2 event ingestion.
    ///
    /// Returns a new writer (not a reference) to maintain tier separation.
    /// The returned writer is for append-only event ledger writes.
    #[must_use]
    pub fn event_writer(&self, source: &EventSource) -> EventWriter {
        EventWriter::new(self.storage.clone()).with_source(source.to_source_string())
    }

    /// Gets current snapshot info for a domain.
    ///
    /// # Errors
    ///
    /// Returns an error if manifest cannot be read.
    pub async fn get_snapshot_info(&self, domain: CatalogDomain) -> Result<Option<SnapshotInfo>> {
        let manifest = self.tier1.read_manifest().await?;
        match domain {
            CatalogDomain::Catalog => Ok(manifest.catalog.snapshot),
            CatalogDomain::Lineage => Ok(manifest.lineage.snapshot),
            CatalogDomain::Search => Ok(manifest.search.snapshot),
            CatalogDomain::Executions => Ok(None),
        }
    }
}

fn validate_storage_scope(storage: &ScopedStorage, scope: &ControlPlaneScope) -> Result<()> {
    if storage.tenant_id() != scope.tenant_id() {
        return Err(CatalogError::Validation {
            message: format!(
                "storage tenant '{}' does not match control-plane tenant '{}'",
                storage.tenant_id(),
                scope.tenant_id()
            ),
        });
    }
    if storage.workspace_id() != scope.workspace_id() {
        return Err(CatalogError::Validation {
            message: format!(
                "storage workspace '{}' does not match control-plane workspace '{}'",
                storage.workspace_id(),
                scope.workspace_id()
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::CatalogReader;
    use crate::tier1_compactor::Tier1Compactor;
    use arco_core::storage::MemoryBackend;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use ulid::Ulid;

    fn setup() -> CatalogWriter {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").expect("valid storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        CatalogWriter::new(storage).with_sync_compactor(compactor)
    }

    fn test_catalog_identity(
        tx_id: String,
        request: &CatalogTransactionRequest,
    ) -> CatalogTransactionIdentity {
        let handle_id = "hdl_00000000000000000000000000";
        let identity = format!("handle:{handle_id}:mutation:{:020}", 1);
        CatalogTransactionIdentity {
            tx_id,
            request_hash: request.request_hash().expect("test request hash"),
            tenant_id: "acme".to_string(),
            workspace_id: "production".to_string(),
            request_id: identity.clone(),
            idempotency_key: identity,
            handle_id: handle_id.to_string(),
            ordinal: 1,
            staged_sha256: format!("sha256:{}", "d".repeat(64)),
            reviewed_request: request.clone(),
            mutation_authorized: true,
        }
    }

    fn test_catalog_options(identity: &CatalogTransactionIdentity) -> WriteOptions {
        WriteOptions::default()
            .with_request_id(&identity.request_id)
            .with_idempotency_key(&identity.idempotency_key)
            .with_transaction_identity(identity.clone())
    }

    #[test]
    fn frozen_catalog_capability_is_bound_to_one_exact_request() {
        let writer = setup();
        let reviewed = CatalogTransactionRequest::CreateCatalog {
            catalog: "reviewed".to_string(),
            description: Some("exact request".to_string()),
        };
        let identity = test_catalog_identity(Ulid::new().to_string(), &reviewed);
        let bound = writer
            .bind_catalog_transaction_request(test_catalog_options(&identity), &reviewed)
            .expect("bind exact reviewed request");
        assert_eq!(
            bound.validated_transaction_request_hash.as_deref(),
            Some(identity.request_hash.as_str())
        );

        let wrong_requests = [
            CatalogTransactionRequest::CreateCatalog {
                catalog: "different".to_string(),
                description: Some("exact request".to_string()),
            },
            CatalogTransactionRequest::CreateSchema {
                catalog: "default".to_string(),
                schema: "reviewed".to_string(),
                description: None,
            },
            CatalogTransactionRequest::RegisterTable {
                catalog: "default".to_string(),
                schema: "default".to_string(),
                table: "reviewed".to_string(),
                description: None,
                location: None,
                format: Some("delta".to_string()),
                columns: vec![ColumnDefinition {
                    name: "id".to_string(),
                    data_type: "INT64".to_string(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                }],
            },
            CatalogTransactionRequest::UpdateTable {
                catalog: "default".to_string(),
                schema: "default".to_string(),
                table: "reviewed".to_string(),
                description: Some(None),
                location: None,
                format: None,
            },
            CatalogTransactionRequest::DropTable {
                catalog: "default".to_string(),
                schema: "default".to_string(),
                table: "reviewed".to_string(),
            },
            CatalogTransactionRequest::RenameTable {
                catalog: "default".to_string(),
                schema: "default".to_string(),
                table: "reviewed".to_string(),
                new_table: "different".to_string(),
            },
        ];
        for wrong in wrong_requests {
            writer
                .bind_catalog_transaction_request(test_catalog_options(&identity), &wrong)
                .expect_err("a capability must reject every different operation payload");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn reviewed_catalog_request_validates_all_six_event_realizations() {
        let mut base = CatalogState::empty();
        base.catalogs.push(CatalogRecord {
            id: "catalog-existing".to_string(),
            name: "existing".to_string(),
            description: None,
            created_at: 1_000,
            updated_at: 1_000,
            properties_json: None,
            storage_root: None,
        });
        base.namespaces.push(NamespaceRecord {
            id: "namespace-existing".to_string(),
            catalog_id: Some("catalog-existing".to_string()),
            name: "schema".to_string(),
            description: None,
            created_at: 1_000,
            updated_at: 1_000,
            properties_json: None,
            storage_root: None,
        });
        let existing_table = TableRecord {
            id: "table-existing".to_string(),
            namespace_id: "namespace-existing".to_string(),
            name: "table".to_string(),
            description: Some("before".to_string()),
            location: None,
            format: Some("parquet".to_string()),
            created_at: 1_000,
            updated_at: 1_000,
            table_type: None,
            properties_json: None,
        };
        base.tables.push(existing_table.clone());

        let create_catalog = CatalogTransactionRequest::CreateCatalog {
            catalog: "created".to_string(),
            description: Some("catalog".to_string()),
        };
        let create_catalog_event = serde_json::to_value(CatalogDdlEventV2::CatalogCreated {
            catalog: CatalogRecord {
                id: Uuid::now_v7().to_string(),
                name: "created".to_string(),
                description: Some("catalog".to_string()),
                created_at: 2_000,
                updated_at: 2_000,
                properties_json: None,
                storage_root: None,
            },
        })
        .expect("create catalog event");
        create_catalog
            .validate_event_realization(
                "catalog.ddl",
                2,
                &create_catalog_event,
                &base,
                "acme",
                "production",
            )
            .expect("reviewed catalog create realization");
        let mut advanced_base = base.clone();
        advanced_base.catalogs.push(CatalogRecord {
            id: "catalog-created-by-another-writer".to_string(),
            name: "created".to_string(),
            description: Some("catalog".to_string()),
            created_at: 1_500,
            updated_at: 1_500,
            properties_json: None,
            storage_root: None,
        });
        create_catalog
            .validate_event_realization(
                "catalog.ddl",
                2,
                &create_catalog_event,
                &advanced_base,
                "acme",
                "production",
            )
            .expect_err("catalog creation cannot execute against a stale current base");

        let create_schema = CatalogTransactionRequest::CreateSchema {
            catalog: "existing".to_string(),
            schema: "created_schema".to_string(),
            description: Some("schema".to_string()),
        };
        let create_schema_event = serde_json::to_value(CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord {
                id: Uuid::now_v7().to_string(),
                catalog_id: Some("catalog-existing".to_string()),
                name: "created_schema".to_string(),
                description: Some("schema".to_string()),
                created_at: 2_000,
                updated_at: 2_000,
                properties_json: None,
                storage_root: None,
            },
        })
        .expect("create schema event");
        create_schema
            .validate_event_realization(
                "catalog.ddl",
                1,
                &create_schema_event,
                &base,
                "acme",
                "production",
            )
            .expect("reviewed schema create realization");
        let mut advanced_base = base.clone();
        advanced_base.catalogs[0].id = "catalog-replaced".to_string();
        create_schema
            .validate_event_realization(
                "catalog.ddl",
                1,
                &create_schema_event,
                &advanced_base,
                "acme",
                "production",
            )
            .expect_err("schema creation cannot retain a stale resolved catalog ID");

        let register = CatalogTransactionRequest::RegisterTable {
            catalog: "existing".to_string(),
            schema: "schema".to_string(),
            table: "registered".to_string(),
            description: Some("table".to_string()),
            location: None,
            format: Some("parquet".to_string()),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: "INT64".to_string(),
                is_nullable: false,
                ordinal: 0,
                description: None,
            }],
        };
        let registered_table_id = Uuid::now_v7().to_string();
        let register_event = serde_json::to_value(CatalogDdlEvent::TableRegistered {
            table: TableRecord {
                id: registered_table_id.clone(),
                namespace_id: "namespace-existing".to_string(),
                name: "registered".to_string(),
                description: Some("table".to_string()),
                location: None,
                format: Some("parquet".to_string()),
                created_at: 2_000,
                updated_at: 2_000,
                table_type: None,
                properties_json: None,
            },
            columns: vec![ColumnRecord {
                id: Uuid::now_v7().to_string(),
                table_id: registered_table_id,
                name: "id".to_string(),
                data_type: "INT64".to_string(),
                is_nullable: false,
                ordinal: 0,
                description: None,
            }],
        })
        .expect("register table event");
        register
            .validate_event_realization(
                "catalog.ddl",
                1,
                &register_event,
                &base,
                "acme",
                "production",
            )
            .expect("reviewed table registration realization");
        let mut advanced_base = base.clone();
        advanced_base.namespaces[0].id = "namespace-replaced".to_string();
        register
            .validate_event_realization(
                "catalog.ddl",
                1,
                &register_event,
                &advanced_base,
                "acme",
                "production",
            )
            .expect_err("table registration cannot retain a stale resolved schema ID");

        let update = CatalogTransactionRequest::UpdateTable {
            catalog: "existing".to_string(),
            schema: "schema".to_string(),
            table: "table".to_string(),
            description: Some(Some("after".to_string())),
            location: None,
            format: None,
        };
        let mut updated_table = existing_table;
        updated_table.description = Some("after".to_string());
        updated_table.updated_at = 2_000;
        let update_event = CatalogDdlEvent::TableUpdated {
            table: updated_table.clone(),
        };
        let update_event_value = serde_json::to_value(&update_event).expect("update table event");
        update
            .validate_event_realization(
                "catalog.ddl",
                1,
                &update_event_value,
                &base,
                "acme",
                "production",
            )
            .expect("reviewed table update realization");
        let mut advanced_base = base.clone();
        advanced_base.tables[0].location = Some("s3://catalog/intervening".to_string());
        advanced_base.tables[0].updated_at = 1_500;
        update
            .validate_event_realization(
                "catalog.ddl",
                1,
                &update_event_value,
                &advanced_base,
                "acme",
                "production",
            )
            .expect_err("table update cannot overwrite stale inherited state");
        updated_table.location = Some("unreviewed://location".to_string());
        update
            .validate_event_realization(
                "catalog.ddl",
                1,
                &serde_json::to_value(CatalogDdlEvent::TableUpdated {
                    table: updated_table,
                })
                .expect("divergent update event"),
                &base,
                "acme",
                "production",
            )
            .expect_err("an inherited field cannot diverge from the bound base");

        let drop_request = CatalogTransactionRequest::DropTable {
            catalog: "existing".to_string(),
            schema: "schema".to_string(),
            table: "table".to_string(),
        };
        let drop_event = serde_json::to_value(CatalogDdlEvent::TableDropped {
            table_id: "table-existing".to_string(),
            namespace_id: "namespace-existing".to_string(),
            table_name: "table".to_string(),
        })
        .expect("drop table event");
        drop_request
            .validate_event_realization("catalog.ddl", 1, &drop_event, &base, "acme", "production")
            .expect("reviewed table drop realization");
        let mut advanced_base = base.clone();
        advanced_base.tables[0].id = "table-replaced".to_string();
        drop_request
            .validate_event_realization(
                "catalog.ddl",
                1,
                &drop_event,
                &advanced_base,
                "acme",
                "production",
            )
            .expect_err("table drop cannot retain a stale resolved table ID");

        let rename = CatalogTransactionRequest::RenameTable {
            catalog: "existing".to_string(),
            schema: "schema".to_string(),
            table: "table".to_string(),
            new_table: "renamed".to_string(),
        };
        let rename_event = serde_json::to_value(CatalogDdlEvent::TableRenamed {
            table_id: "table-existing".to_string(),
            namespace_id: "namespace-existing".to_string(),
            old_name: "table".to_string(),
            new_name: "renamed".to_string(),
            updated_at: 2_000,
        })
        .expect("rename table event");
        rename
            .validate_event_realization(
                "catalog.ddl",
                1,
                &rename_event,
                &base,
                "acme",
                "production",
            )
            .expect("reviewed table rename realization");
        let mut advanced_base = base;
        advanced_base.tables[0].id = "table-replaced".to_string();
        rename
            .validate_event_realization(
                "catalog.ddl",
                1,
                &rename_event,
                &advanced_base,
                "acme",
                "production",
            )
            .expect_err("table rename cannot retain a stale resolved table ID");
    }

    #[tokio::test]
    async fn frozen_catalog_transaction_intent_is_opt_in_for_legacy_writes() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog_transaction(
                "legacy",
                Some("legacy transaction path"),
                WriteOptions::default(),
            )
            .await
            .expect("legacy catalog transaction");
        assert!(
            writer
                .storage
                .list("transactions/catalog/")
                .await
                .expect("list test-only transaction artifacts")
                .is_empty(),
            "legacy writers must not publish durable-handle event intents"
        );

        let tx_id = Ulid::new().to_string();
        let handle_id = "hdl_00000000000000000000000000";
        let handle_identity = format!("handle:{handle_id}:mutation:{:020}", 1);
        writer
            .authorize_frozen_catalog_transaction(
                &tx_id,
                &format!("sha256:{}", "b".repeat(64)),
                &handle_identity,
                &handle_identity,
            )
            .await
            .expect_err("handle-shaped syntax alone must not create a capability");
        assert!(
            writer
                .storage
                .list("transactions/catalog/")
                .await
                .expect("list rejected transaction artifacts")
                .is_empty(),
            "rejected syntax must not publish a transaction intent"
        );
    }

    #[tokio::test]
    async fn failed_frozen_catalog_recovery_releases_its_lock() {
        let writer = setup();
        writer.initialize().await.expect("initialize");
        let request = CatalogTransactionRequest::CreateCatalog {
            catalog: "frozen".to_string(),
            description: Some("frozen transaction path".to_string()),
        };
        let identity = test_catalog_identity(Ulid::new().to_string(), &request);
        let commit = writer
            .create_catalog_transaction(
                "frozen",
                Some("frozen transaction path"),
                test_catalog_options(&identity),
            )
            .await
            .expect("frozen catalog transaction");

        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &commit.manifest_id);
        let mut manifest: crate::manifest::CatalogDomainManifest = serde_json::from_slice(
            writer
                .storage
                .get_raw(&manifest_path)
                .await
                .expect("read immutable manifest")
                .as_ref(),
        )
        .expect("decode immutable manifest");
        manifest.last_commit_id = None;
        manifest.commit_ulid = None;
        writer
            .storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).expect("manifest JSON")),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("corrupt immutable manifest");

        writer
            .recover_catalog_transaction(&identity, Some(identity.request_id.clone()))
            .await
            .expect_err("corrupt immutable authority must fail recovery");

        let lock: crate::lock::LockInfo = serde_json::from_slice(
            writer
                .storage
                .get_raw(&CatalogPaths::domain_lock(CatalogDomain::Catalog))
                .await
                .expect("read catalog lock")
                .as_ref(),
        )
        .expect("decode catalog lock");
        assert!(
            lock.is_expired(),
            "a failed recovery must synchronously release its catalog lock"
        );
    }

    #[tokio::test]
    async fn test_create_catalog() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        let catalog = writer
            .create_catalog("default", Some("Default catalog"), WriteOptions::default())
            .await
            .expect("create catalog");

        assert_eq!(catalog.name, "default");
        assert_eq!(catalog.description, Some("Default catalog".to_string()));

        let manifest = writer.tier1.read_manifest().await.expect("manifest");
        let state =
            tier1_state::load_catalog_state(&writer.storage, &manifest.catalog.snapshot_path)
                .await
                .expect("load catalog state");

        assert_eq!(state.catalogs.len(), 1);
        assert_eq!(state.catalogs[0].name, "default");
    }

    #[tokio::test]
    async fn test_patch_catalog_persists_authoritative_uc_metadata_and_rename() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog_with_metadata(
                "analytics",
                Some("before"),
                Some(BTreeMap::from([(
                    "classification".to_string(),
                    "internal".to_string(),
                )])),
                Some("s3://bucket/catalogs/analytics"),
                WriteOptions::default(),
            )
            .await
            .expect("create catalog with UC metadata");

        writer
            .patch_catalog(
                "analytics",
                CatalogPatch {
                    description: Some(Some("after".to_string())),
                    new_name: Some("analytics_curated".to_string()),
                    properties: Some(Some(BTreeMap::from([
                        ("classification".to_string(), "restricted".to_string()),
                        ("owner".to_string(), "governance".to_string()),
                    ]))),
                    storage_root: Some(Some("s3://bucket/catalogs/analytics_curated".to_string())),
                },
                WriteOptions::default(),
            )
            .await
            .expect("patch catalog with UC metadata");

        let reader = CatalogReader::new(writer.storage.clone());
        let catalog = reader
            .get_catalog("analytics_curated")
            .await
            .expect("get renamed catalog")
            .expect("renamed catalog should exist");
        assert_eq!(catalog.description.as_deref(), Some("after"));
        assert_eq!(
            catalog
                .properties
                .as_ref()
                .and_then(|properties| properties.get("classification"))
                .map(String::as_str),
            Some("restricted")
        );
        assert_eq!(
            catalog
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner"))
                .map(String::as_str),
            Some("governance")
        );
        assert_eq!(
            catalog.storage_root.as_deref(),
            Some("s3://bucket/catalogs/analytics_curated")
        );
        assert!(
            reader
                .get_catalog("analytics")
                .await
                .expect("get old catalog name")
                .is_none(),
            "old catalog name must disappear after authoritative rename"
        );
    }

    #[tokio::test]
    async fn test_create_duplicate_catalog_fails() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog("default", None, WriteOptions::default())
            .await
            .expect("first create");

        let result = writer
            .create_catalog("default", None, WriteOptions::default())
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn test_create_schema_sets_catalog_id() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        let catalog = writer
            .create_catalog("default", None, WriteOptions::default())
            .await
            .expect("create catalog");

        let ns = writer
            .create_schema(
                "default",
                "sales",
                Some("Sales schema"),
                WriteOptions::default(),
            )
            .await
            .expect("create schema");

        assert_eq!(ns.name, "sales");
        assert_eq!(ns.catalog_id, Some(catalog.id.clone()));

        let manifest = writer.tier1.read_manifest().await.expect("manifest");
        let state =
            tier1_state::load_catalog_state(&writer.storage, &manifest.catalog.snapshot_path)
                .await
                .expect("load catalog state");

        let stored = state
            .namespaces
            .iter()
            .find(|n| n.id == ns.id)
            .expect("namespace stored");

        assert_eq!(stored.catalog_id, Some(catalog.id));
    }

    #[tokio::test]
    async fn test_create_schema_allows_same_name_in_different_catalogs() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        let default = writer
            .create_catalog("default", None, WriteOptions::default())
            .await
            .expect("create default catalog");

        let analytics = writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");

        let ns_default = writer
            .create_schema("default", "sales", None, WriteOptions::default())
            .await
            .expect("create schema in default");

        let ns_analytics = writer
            .create_schema("analytics", "sales", None, WriteOptions::default())
            .await
            .expect("create schema in analytics");

        assert_eq!(ns_default.name, "sales");
        assert_eq!(ns_default.catalog_id, Some(default.id.clone()));
        assert_eq!(ns_analytics.name, "sales");
        assert_eq!(ns_analytics.catalog_id, Some(analytics.id.clone()));

        let manifest = writer.tier1.read_manifest().await.expect("manifest");
        let state =
            tier1_state::load_catalog_state(&writer.storage, &manifest.catalog.snapshot_path)
                .await
                .expect("load catalog state");

        assert_eq!(state.namespaces.len(), 2);
    }

    #[tokio::test]
    async fn test_initialize_creates_manifests() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        // Should be idempotent
        writer.initialize().await.expect("initialize again");
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        let ns = writer
            .create_namespace(
                "default",
                Some("Default namespace"),
                WriteOptions::default(),
            )
            .await
            .expect("create");

        assert_eq!(ns.name, "default");
        assert_eq!(ns.description, Some("Default namespace".to_string()));
        assert!(!ns.id.is_empty());
    }

    #[test]
    fn schema_roundtrips_through_namespace_storage_record() {
        let schema = Schema {
            id: "schema-01".to_string(),
            catalog_id: Some("catalog-01".to_string()),
            name: "sales".to_string(),
            description: Some("sales schema".to_string()),
            properties: Some(BTreeMap::from([(
                "domain".to_string(),
                "finance".to_string(),
            )])),
            storage_root: Some("s3://bucket/schemas/sales".to_string()),
            created_at: 1,
            updated_at: 2,
        };

        let record = NamespaceRecord::try_from(&schema).expect("encode schema record");
        let roundtrip = Schema::try_from(record).expect("decode schema record");

        assert_eq!(roundtrip.id, "schema-01");
        assert_eq!(roundtrip.catalog_id.as_deref(), Some("catalog-01"));
        assert_eq!(roundtrip.name, "sales");
        assert_eq!(roundtrip.description.as_deref(), Some("sales schema"));
        assert_eq!(
            roundtrip
                .properties
                .as_ref()
                .and_then(|properties| properties.get("domain"))
                .map(String::as_str),
            Some("finance")
        );
        assert_eq!(
            roundtrip.storage_root.as_deref(),
            Some("s3://bucket/schemas/sales")
        );
    }

    #[test]
    fn schema_roundtrips_without_optional_description() {
        let schema = Schema {
            id: "schema-02".to_string(),
            catalog_id: Some("catalog-01".to_string()),
            name: "finance".to_string(),
            description: None,
            properties: None,
            storage_root: None,
            created_at: 3,
            updated_at: 4,
        };

        let record = NamespaceRecord::try_from(&schema).expect("encode schema record");
        let roundtrip = Schema::try_from(record).expect("decode schema record");

        assert_eq!(roundtrip.id, "schema-02");
        assert_eq!(roundtrip.catalog_id.as_deref(), Some("catalog-01"));
        assert_eq!(roundtrip.name, "finance");
        assert_eq!(roundtrip.description, None);
    }

    #[tokio::test]
    async fn test_patch_schema_in_catalog_persists_authoritative_uc_metadata_and_rename() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");
        writer
            .create_schema_with_metadata(
                "analytics",
                "staging",
                Some("before"),
                Some(BTreeMap::from([(
                    "domain".to_string(),
                    "finance".to_string(),
                )])),
                Some("s3://bucket/schemas/staging"),
                WriteOptions::default(),
            )
            .await
            .expect("create schema with UC metadata");

        writer
            .patch_schema_in_catalog(
                "analytics",
                "staging",
                SchemaPatch {
                    description: Some(Some("after".to_string())),
                    new_name: Some("gold".to_string()),
                    properties: Some(Some(BTreeMap::from([
                        ("domain".to_string(), "governance".to_string()),
                        ("retention".to_string(), "90d".to_string()),
                    ]))),
                    storage_root: Some(Some("s3://bucket/schemas/gold".to_string())),
                },
                WriteOptions::default(),
            )
            .await
            .expect("patch schema with UC metadata");

        let reader = CatalogReader::new(writer.storage.clone());
        let schema = reader
            .list_schemas("analytics")
            .await
            .expect("list schemas in analytics")
            .into_iter()
            .find(|candidate| candidate.name == "gold")
            .expect("renamed schema should exist");
        assert_eq!(schema.description.as_deref(), Some("after"));
        assert_eq!(
            schema
                .properties
                .as_ref()
                .and_then(|properties| properties.get("domain"))
                .map(String::as_str),
            Some("governance")
        );
        assert_eq!(
            schema
                .properties
                .as_ref()
                .and_then(|properties| properties.get("retention"))
                .map(String::as_str),
            Some("90d")
        );
        assert_eq!(
            schema.storage_root.as_deref(),
            Some("s3://bucket/schemas/gold")
        );
    }

    #[tokio::test]
    async fn test_create_namespace_creates_default_catalog_once() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        let ns_one = writer
            .create_namespace("sales", None, WriteOptions::default())
            .await
            .expect("create namespace one");

        let ns_two = writer
            .create_namespace("analytics", None, WriteOptions::default())
            .await
            .expect("create namespace two");

        let manifest = writer.tier1.read_manifest().await.expect("manifest");
        let state =
            tier1_state::load_catalog_state(&writer.storage, &manifest.catalog.snapshot_path)
                .await
                .expect("load catalog state");

        assert_eq!(state.catalogs.len(), 1);
        assert_eq!(state.catalogs[0].name, "default");
        let default_id = state.catalogs[0].id.clone();

        assert_eq!(ns_one.catalog_id, Some(default_id.clone()));
        assert_eq!(ns_two.catalog_id, Some(default_id.clone()));

        let stored_one = state
            .namespaces
            .iter()
            .find(|n| n.id == ns_one.id)
            .expect("namespace one stored");
        let stored_two = state
            .namespaces
            .iter()
            .find(|n| n.id == ns_two.id)
            .expect("namespace two stored");

        assert_eq!(stored_one.catalog_id, Some(default_id.clone()));
        assert_eq!(stored_two.catalog_id, Some(default_id));
    }

    #[tokio::test]
    async fn test_create_duplicate_namespace_fails() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("first create");

        let result = writer
            .create_namespace("default", None, WriteOptions::default())
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn test_delete_namespace() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("to_delete", None, WriteOptions::default())
            .await
            .expect("create");

        writer
            .delete_namespace("to_delete", WriteOptions::default())
            .await
            .expect("delete");

        // Deleting again should fail
        let result = writer
            .delete_namespace("to_delete", WriteOptions::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_table() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let table = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "users".to_string(),
                    description: Some("User table".to_string()),
                    location: Some("s3://bucket/users".to_string()),
                    format: Some("parquet".to_string()),
                    columns: vec![
                        ColumnDefinition {
                            name: "id".to_string(),
                            data_type: "STRING".to_string(),
                            is_nullable: false,
                            ordinal: 0,
                            description: Some("Primary key".to_string()),
                        },
                        ColumnDefinition {
                            name: "email".to_string(),
                            data_type: "STRING".to_string(),
                            is_nullable: true,
                            ordinal: 1,
                            description: None,
                        },
                    ],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        assert_eq!(table.name, "users");
        assert!(!table.id.is_empty());
    }

    #[tokio::test]
    async fn test_register_table_defaults_to_delta_format() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let table = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "events".to_string(),
                    description: None,
                    location: Some("warehouse/default/events".to_string()),
                    format: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        assert_eq!(table.format.as_deref(), Some("delta"));
    }

    #[tokio::test]
    async fn test_register_table_validates_and_canonicalizes_format() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let table = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "delta_table".to_string(),
                    description: None,
                    location: Some("warehouse/default/delta_table".to_string()),
                    format: Some("DeLtA".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        assert_eq!(table.format.as_deref(), Some("delta"));

        let err = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "unknown_format".to_string(),
                    description: None,
                    location: Some("warehouse/default/unknown_format".to_string()),
                    format: Some("avro".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("unknown format must fail");
        assert!(matches!(err, CatalogError::Validation { .. }));
    }

    #[tokio::test]
    async fn test_register_table_rejects_invalid_delta_location() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let empty_location = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "events_empty_location".to_string(),
                    description: None,
                    location: Some("   ".to_string()),
                    format: Some("delta".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("empty location must fail");
        assert!(matches!(empty_location, CatalogError::Validation { .. }));

        let traversal_location = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "events_traversal_location".to_string(),
                    description: None,
                    location: Some("../escape".to_string()),
                    format: Some("delta".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("path traversal location must fail");
        assert!(matches!(
            traversal_location,
            CatalogError::Validation { .. }
        ));
    }

    #[tokio::test]
    async fn test_register_table_resolves_namespace_in_default_catalog() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        // Create a non-default catalog/schema pair with the same schema name.
        writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");
        writer
            .create_schema("analytics", "sales", None, WriteOptions::default())
            .await
            .expect("create sales schema in analytics");

        // Create the default schema with the same name via legacy API.
        let default_sales = writer
            .create_namespace("sales", None, WriteOptions::default())
            .await
            .expect("create sales namespace in default");

        let table = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "sales".to_string(),
                    name: "orders".to_string(),
                    description: None,
                    location: None,
                    format: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        assert_eq!(table.namespace_id, default_sales.id);
    }

    #[tokio::test]
    async fn test_register_table_in_schema_uses_catalog_and_schema() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");
        let sales = writer
            .create_schema("analytics", "sales", None, WriteOptions::default())
            .await
            .expect("create sales schema");

        let table = writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders".to_string(),
                    description: None,
                    location: Some("gs://bucket/warehouse/sales/orders".to_string()),
                    format: Some("delta".to_string()),
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        assert_eq!(table.namespace_id, sales.id);
        assert_eq!(table.name, "orders");
    }

    #[tokio::test]
    async fn test_register_table_in_schema_defaults_to_delta_and_validates() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");
        writer
            .create_schema("analytics", "sales", None, WriteOptions::default())
            .await
            .expect("create schema");

        let created = writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders".to_string(),
                    description: None,
                    location: Some("warehouse/sales/orders".to_string()),
                    format: None,
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");
        assert_eq!(created.format.as_deref(), Some("delta"));

        let created_iceberg = writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders_iceberg".to_string(),
                    description: None,
                    location: Some("warehouse/sales/orders_iceberg".to_string()),
                    format: Some("Iceberg".to_string()),
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");
        assert_eq!(created_iceberg.format.as_deref(), Some("iceberg"));

        let err = writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders_invalid".to_string(),
                    description: None,
                    location: Some("warehouse/sales/orders_invalid".to_string()),
                    format: Some("orc".to_string()),
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("invalid format");
        assert!(matches!(err, CatalogError::Validation { .. }));
    }

    #[tokio::test]
    async fn test_register_table_in_schema_rejects_invalid_delta_location() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");
        writer
            .create_schema("analytics", "sales", None, WriteOptions::default())
            .await
            .expect("create schema");

        let empty_location = writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders_empty_location".to_string(),
                    description: None,
                    location: Some("   ".to_string()),
                    format: Some("delta".to_string()),
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("empty location must fail");
        assert!(matches!(empty_location, CatalogError::Validation { .. }));

        let traversal_location = writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders_traversal_location".to_string(),
                    description: None,
                    location: Some("../escape".to_string()),
                    format: Some("delta".to_string()),
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("path traversal location must fail");
        assert!(matches!(
            traversal_location,
            CatalogError::Validation { .. }
        ));
    }

    #[tokio::test]
    async fn test_register_table_in_schema_persists_authoritative_uc_table_metadata() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_catalog("analytics", None, WriteOptions::default())
            .await
            .expect("create analytics catalog");
        writer
            .create_schema("analytics", "sales", None, WriteOptions::default())
            .await
            .expect("create schema");

        writer
            .register_table_in_schema(
                "analytics",
                "sales",
                RegisterTableInSchemaRequest {
                    name: "orders".to_string(),
                    description: Some("orders table".to_string()),
                    location: Some("s3://bucket/analytics/sales/orders".to_string()),
                    format: Some("delta".to_string()),
                    table_type: Some("EXTERNAL".to_string()),
                    properties: Some(BTreeMap::from([
                        ("quality".to_string(), "silver".to_string()),
                        ("retention".to_string(), "30d".to_string()),
                    ])),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table with UC metadata");

        let reader = CatalogReader::new(writer.storage.clone());
        let table = reader
            .get_table_in_schema("analytics", "sales", "orders")
            .await
            .expect("get registered table")
            .expect("registered table should exist");
        assert_eq!(table.table_type.as_deref(), Some("EXTERNAL"));
        assert_eq!(
            table
                .properties
                .as_ref()
                .and_then(|properties| properties.get("quality"))
                .map(String::as_str),
            Some("silver")
        );
        assert_eq!(
            table
                .properties
                .as_ref()
                .and_then(|properties| properties.get("retention"))
                .map(String::as_str),
            Some("30d")
        );
    }

    #[tokio::test]
    async fn test_update_table_rejects_invalid_location_when_switching_to_delta() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "legacy_parquet".to_string(),
                    description: None,
                    location: Some("../legacy/parquet/path".to_string()),
                    format: Some("parquet".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register parquet table");

        let err = writer
            .update_table(
                "default",
                "legacy_parquet",
                TablePatch {
                    format: Some(Some("delta".to_string())),
                    ..TablePatch::default()
                },
                WriteOptions::default(),
            )
            .await
            .expect_err("switching to delta with invalid location must fail");
        assert!(matches!(err, CatalogError::Validation { .. }));
    }
    #[tokio::test]
    async fn test_event_writer_returns_owned() {
        let writer = setup();
        let source = EventSource::new("test-service");
        let event_writer = writer.event_writer(&source);

        // Should compile - event_writer is owned, not borrowed
        drop(writer);
        let _ = event_writer;
    }

    #[tokio::test]
    async fn test_domain_split_separate_locks() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        // Create namespace (catalog domain)
        writer
            .create_namespace("test", None, WriteOptions::default())
            .await
            .expect("create namespace");

        // Add lineage edge (lineage domain - separate lock)
        let edge = LineageEdge {
            id: Ulid::new().to_string(),
            source_id: "table_a".to_string(),
            target_id: "table_b".to_string(),
            edge_type: "derives_from".to_string(),
            run_id: Some("run_001".to_string()),
            created_at: Utc::now().timestamp_millis(),
        };

        writer
            .add_lineage_edge(edge, WriteOptions::default())
            .await
            .expect("add lineage edge");
    }

    #[tokio::test]
    async fn test_lineage_writes_publish_and_increment_manifest() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        let reader = CatalogReader::new(storage);

        writer.initialize().await.expect("initialize");

        // After initialize, lineage is at v0. First edge publish creates v1.
        let edge1 = LineageEdge {
            id: Ulid::new().to_string(),
            source_id: "table_a".to_string(),
            target_id: "table_b".to_string(),
            edge_type: "derives_from".to_string(),
            run_id: Some("run_001".to_string()),
            created_at: Utc::now().timestamp_millis(),
        };
        writer
            .add_lineage_edge(edge1.clone(), WriteOptions::default())
            .await
            .expect("add edge1");

        let info1 = writer
            .get_snapshot_info(CatalogDomain::Lineage)
            .await
            .expect("snapshot info");
        assert_eq!(info1.unwrap().version, 1);

        let graph1 = reader.get_lineage("table_a").await.expect("lineage");
        assert_eq!(graph1.downstream.len(), 1);

        // Second write should publish v2 and include both edges.
        let edge2 = LineageEdge {
            id: Ulid::new().to_string(),
            source_id: "table_a".to_string(),
            target_id: "table_c".to_string(),
            edge_type: "derives_from".to_string(),
            run_id: Some("run_002".to_string()),
            created_at: Utc::now().timestamp_millis(),
        };
        writer
            .add_lineage_edge(edge2, WriteOptions::default())
            .await
            .expect("add edge2");

        let info2 = writer
            .get_snapshot_info(CatalogDomain::Lineage)
            .await
            .expect("snapshot info 2");
        assert_eq!(info2.unwrap().version, 2);

        let graph2 = reader.get_lineage("table_a").await.expect("lineage 2");
        assert_eq!(graph2.downstream.len(), 2);
    }

    #[tokio::test]
    async fn test_rename_table() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "old_name".to_string(),
                    description: Some("Test table".to_string()),
                    location: Some("s3://bucket/old_name".to_string()),
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        let renamed = writer
            .rename_table(
                "default",
                "old_name",
                "default",
                "new_name",
                WriteOptions::default(),
            )
            .await
            .expect("rename table");

        assert_eq!(renamed.name, "new_name");
        assert_eq!(renamed.description, Some("Test table".to_string()));
    }

    #[tokio::test]
    async fn test_drop_table_transaction_reports_locked_table_identity() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let table = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "events".to_string(),
                    description: None,
                    location: Some("s3://bucket/events".to_string()),
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        let commit = writer
            .drop_table_in_schema_transaction(
                "default",
                "default",
                "events",
                WriteOptions::default(),
            )
            .await
            .expect("drop table transaction");

        let dropped = commit.dropped_table.expect("dropped table identity");
        assert_eq!(dropped.table_id, table.id);
        assert_eq!(dropped.format.as_deref(), Some("iceberg"));
    }

    #[tokio::test]
    async fn test_rename_table_cross_namespace_fails() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("ns1", None, WriteOptions::default())
            .await
            .expect("create ns1");

        writer
            .create_namespace("ns2", None, WriteOptions::default())
            .await
            .expect("create ns2");

        writer
            .register_table(
                RegisterTableRequest {
                    namespace: "ns1".to_string(),
                    name: "my_table".to_string(),
                    description: None,
                    location: None,
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        let result = writer
            .rename_table(
                "ns1",
                "my_table",
                "ns2",
                "my_table",
                WriteOptions::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CatalogError::UnsupportedOperation { .. }
        ));
    }

    #[tokio::test]
    async fn test_rename_table_conflict_fails() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "table_a".to_string(),
                    description: None,
                    location: None,
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table_a");

        writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "table_b".to_string(),
                    description: None,
                    location: None,
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table_b");

        let result = writer
            .rename_table(
                "default",
                "table_a",
                "default",
                "table_b",
                WriteOptions::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CatalogError::AlreadyExists { .. }
        ));
    }

    #[tokio::test]
    async fn test_rename_table_not_found_fails() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let result = writer
            .rename_table(
                "default",
                "nonexistent",
                "default",
                "new_name",
                WriteOptions::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CatalogError::NotFound { .. }));
    }

    #[tokio::test]
    async fn test_rename_table_same_name_noop() {
        let writer = setup();
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let table = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "my_table".to_string(),
                    description: Some("Original".to_string()),
                    location: None,
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        let renamed = writer
            .rename_table(
                "default",
                "my_table",
                "default",
                "my_table",
                WriteOptions::default(),
            )
            .await
            .expect("same name rename should succeed");

        assert_eq!(renamed.id, table.id);
        assert_eq!(renamed.name, "my_table");
    }
}
