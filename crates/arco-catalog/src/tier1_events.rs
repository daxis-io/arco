//! Tier-1 DDL event payloads for ledger-driven compaction.
//!
//! These payloads represent strongly-consistent catalog mutations that are
//! appended to the ledger while holding the Tier-1 lock. The compactor applies
//! them to produce new Parquet snapshots.

use serde::{Deserialize, Serialize};

use arco_core::CatalogEventPayload;

use crate::parquet_util::{
    CatalogRecord, ColumnRecord, LineageEdgeRecord, NamespaceRecord, TableRecord,
};

/// Catalog domain DDL events (namespaces, tables, columns).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogDdlEvent {
    /// Create a namespace (records include IDs/timestamps chosen by API).
    NamespaceCreated {
        /// The namespace record to create.
        namespace: NamespaceRecord,
    },
    /// Update namespace metadata.
    NamespaceUpdated {
        /// The updated namespace record.
        namespace: NamespaceRecord,
    },
    /// Delete a namespace by ID.
    NamespaceDeleted {
        /// ID of the namespace to delete.
        namespace_id: String,
        /// Name of the namespace (for verification).
        namespace_name: String,
    },
    /// Register a new table (with initial column set).
    TableRegistered {
        /// The table record to register.
        table: TableRecord,
        /// Initial columns for the table.
        columns: Vec<ColumnRecord>,
    },
    /// Update table metadata (description/location/format).
    TableUpdated {
        /// The updated table record.
        table: TableRecord,
    },
    /// Drop a table and its columns.
    TableDropped {
        /// ID of the table to drop.
        table_id: String,
        /// Namespace ID containing the table.
        namespace_id: String,
        /// Name of the table (for verification).
        table_name: String,
    },
    /// Rename a table within the same namespace.
    TableRenamed {
        /// ID of the table to rename.
        table_id: String,
        /// Namespace ID (must match existing table).
        namespace_id: String,
        /// Current name of the table (for verification).
        old_name: String,
        /// New name for the table.
        new_name: String,
        /// Updated timestamp in milliseconds.
        updated_at: i64,
    },
}

impl CatalogEventPayload for CatalogDdlEvent {
    const EVENT_TYPE: &'static str = "catalog.ddl";
    const EVENT_VERSION: u32 = 1;
}

/// Catalog domain DDL events (schema v2).
///
/// New variants MUST be introduced as new versions to preserve rolling upgrades:
/// deploy the `Tier1Compactor` that can read v2 before writers emit v2 events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogDdlEventV2 {
    /// Create a catalog (records include IDs/timestamps chosen by API).
    CatalogCreated {
        /// The catalog record to create.
        catalog: CatalogRecord,
    },
}

impl CatalogEventPayload for CatalogDdlEventV2 {
    const EVENT_TYPE: &'static str = "catalog.ddl";
    const EVENT_VERSION: u32 = 2;
}

/// Lineage domain DDL events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LineageDdlEvent {
    /// Append one or more lineage edges.
    EdgesAdded {
        /// The lineage edges to add.
        edges: Vec<LineageEdgeRecord>,
    },
}

impl CatalogEventPayload for LineageDdlEvent {
    const EVENT_TYPE: &'static str = "lineage.ddl";
    const EVENT_VERSION: u32 = 1;
}
