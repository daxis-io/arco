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

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use arco_core::storage::StorageBackend;
use arco_core::storage::{WritePrecondition, WriteResult};
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
use bytes::Bytes;

use crate::error::{CatalogError, Result};
use crate::event_writer::EventWriter;
use crate::lock::{DistributedLock, LockGuard};
use crate::manifest::{LineageManifest, RootManifest, SnapshotInfo};
use crate::parquet_util::{ColumnRecord, LineageEdgeRecord, NamespaceRecord, TableRecord};
use crate::state::{CatalogState, LineageState};
use crate::tier1_writer::Tier1Writer;
use crate::write_options::WriteOptions;

/// Default lock TTL for write operations.
const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(30);
/// Default maximum lock acquisition retries.
const DEFAULT_LOCK_MAX_RETRIES: u32 = 10;
/// Default maximum CAS retries for lineage manifest updates.
const DEFAULT_LINEAGE_CAS_MAX_RETRIES: u32 = 10;

// ============================================================================
// Domain Types (returned from write operations)
// ============================================================================

/// A namespace in the catalog.
#[derive(Debug, Clone)]
pub struct Namespace {
    /// Unique namespace ID (UUID v7).
    pub id: String,
    /// Namespace name (unique within workspace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: i64,
    /// Last update timestamp (milliseconds since epoch).
    pub updated_at: i64,
}

impl From<NamespaceRecord> for Namespace {
    fn from(r: NamespaceRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            description: r.description,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

impl From<&Namespace> for NamespaceRecord {
    fn from(ns: &Namespace) -> Self {
        Self {
            id: ns.id.clone(),
            name: ns.name.clone(),
            description: ns.description.clone(),
            created_at: ns.created_at,
            updated_at: ns.updated_at,
        }
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
    /// File format (e.g., "parquet", "iceberg").
    pub format: Option<String>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: i64,
    /// Last update timestamp (milliseconds since epoch).
    pub updated_at: i64,
}

impl From<TableRecord> for Table {
    fn from(r: TableRecord) -> Self {
        Self {
            id: r.id,
            namespace_id: r.namespace_id,
            name: r.name,
            description: r.description,
            location: r.location,
            format: r.format,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

impl From<&Table> for TableRecord {
    fn from(t: &Table) -> Self {
        Self {
            id: t.id.clone(),
            namespace_id: t.namespace_id.clone(),
            name: t.name.clone(),
            description: t.description.clone(),
            location: t.location.clone(),
            format: t.format.clone(),
            created_at: t.created_at,
            updated_at: t.updated_at,
        }
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
    /// Unique edge ID (ULID).
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
    /// File format (e.g., "parquet", "iceberg").
    pub format: Option<String>,
    /// Column definitions.
    pub columns: Vec<ColumnDefinition>,
}

/// Column definition for table registration.
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Data type (e.g., "STRING", "INT64").
    pub data_type: String,
    /// Whether the column is nullable.
    pub is_nullable: bool,
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

/// Event source identifier for Tier-2 event writing.
#[derive(Debug, Clone)]
pub struct EventSource {
    /// Service/component name (e.g., "api-server", "scheduler").
    pub service: String,
    /// Optional instance identifier.
    pub instance: Option<String>,
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
    /// Tier-1 writer (handles catalog domain lock + CAS)
    tier1: Tier1Writer,
    /// Separate lock for lineage domain
    lineage_lock: DistributedLock<dyn StorageBackend>,
    /// Lock TTL
    lock_ttl: Duration,
    /// Max lock retries
    lock_max_retries: u32,
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
    /// # Example
    ///
    /// ```rust,ignore
    /// use arco_catalog::CatalogWriter;
    /// use arco_core::ScopedStorage;
    ///
    /// let storage = ScopedStorage::new(backend, "acme", "production")?;
    /// let writer = CatalogWriter::new(storage);
    /// ```
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        let backend = storage.backend().clone();
        let lineage_lock_path = storage.lock(CatalogDomain::Lineage);
        let lineage_lock = DistributedLock::new(backend, lineage_lock_path);

        Self {
            tier1: Tier1Writer::new(storage.clone()),
            lineage_lock,
            storage,
            lock_ttl: DEFAULT_LOCK_TTL,
            lock_max_retries: DEFAULT_LOCK_MAX_RETRIES,
        }
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

    // ========================================================================
    // Initialization
    // ========================================================================

    /// Initializes the catalog with empty Parquet state tables.
    ///
    /// Creates:
    /// - All manifest files (root, catalog, lineage, executions, search)
    /// - Empty Parquet snapshots for catalog domain (namespaces, tables, columns)
    /// - Empty Parquet snapshot for lineage domain (lineage_edges)
    ///
    /// Idempotent: safe to call multiple times.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail.
    pub async fn initialize(&self) -> Result<()> {
        // Initialize manifests (idempotent)
        self.tier1.initialize().await?;

        // Initialize catalog domain snapshot (idempotent)
        {
            let guard = self
                .tier1
                .acquire_lock(self.lock_ttl, self.lock_max_retries)
                .await?;

            let manifest = self.tier1.read_manifest().await?;
            if manifest.catalog.snapshot_version == 0 {
                let empty_catalog = CatalogState::empty();
                let next_version = manifest.catalog.snapshot_version + 1;
                let catalog_snapshot = self
                    .tier1
                    .write_catalog_snapshot(&guard, next_version, &empty_catalog)
                    .await?;

                // Publish under the same lock to prevent intermediate contention.
                self.tier1
                    .update_locked(&guard, |m| {
                        m.snapshot_version = catalog_snapshot.version;
                        m.snapshot_path.clone_from(&catalog_snapshot.path);
                        m.snapshot = Some(catalog_snapshot.clone());
                        Ok(())
                    })
                    .await?;
            }

            guard.release().await?;
        }

        // Initialize lineage domain snapshot (idempotent)
        {
            let guard = self
                .lineage_lock
                .acquire(self.lock_ttl, self.lock_max_retries)
                .await?;

            let manifest = self.tier1.read_manifest().await?;
            if manifest.lineage.snapshot_version == 0 {
                let empty_lineage = LineageState::empty();
                let next_version = manifest.lineage.snapshot_version + 1;
                let lineage_snapshot = self
                    .tier1
                    .write_lineage_snapshot(&guard, next_version, &empty_lineage)
                    .await?;

                self.publish_lineage_snapshot(
                    &guard,
                    &lineage_snapshot,
                    manifest.lineage.snapshot_version,
                )
                .await?;
            }

            guard.release().await?;
        }

        Ok(())
    }

    // ========================================================================
    // Namespaces (Tier 1 - catalog domain)
    // ========================================================================

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
    ) -> Result<Namespace> {
        // Check optimistic locking
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
        let namespace = Namespace {
            id: Uuid::now_v7().to_string(),
            name: name.to_string(),
            description: description.map(String::from),
            created_at: now,
            updated_at: now,
        };

        // Acquire lock and write snapshot
        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        // Load current state and add namespace
        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_catalog_state(&manifest.catalog.snapshot_path)
            .await?;

        // Check for duplicate
        if state.namespaces.iter().any(|ns| ns.name == name) {
            guard.release().await?;
            return Err(CatalogError::AlreadyExists {
                entity: "namespace".into(),
                name: name.to_string(),
            });
        }

        state.namespaces.push(NamespaceRecord::from(&namespace));

        // Write new snapshot
        let next_version = manifest.catalog.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_catalog_snapshot(&guard, next_version, &state)
            .await?;

        // Publish under the same lock to prevent intermediate contention.
        self.tier1
            .update_locked(&guard, |m| {
                m.snapshot_version = snapshot.version;
                m.snapshot_path.clone_from(&snapshot.path);
                m.snapshot = Some(snapshot.clone());
                Ok(())
            })
            .await?;

        guard.release().await?;

        Ok(namespace)
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
        // Check optimistic locking
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

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_catalog_state(&manifest.catalog.snapshot_path)
            .await?;

        // Find namespace
        let ns_idx = state
            .namespaces
            .iter()
            .position(|ns| ns.name == name)
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

        state.namespaces.remove(ns_idx);

        // Write new snapshot
        let next_version = manifest.catalog.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_catalog_snapshot(&guard, next_version, &state)
            .await?;

        // Publish under the same lock to prevent intermediate contention.
        self.tier1
            .update_locked(&guard, |m| {
                m.snapshot_version = snapshot.version;
                m.snapshot_path.clone_from(&snapshot.path);
                m.snapshot = Some(snapshot.clone());
                Ok(())
            })
            .await?;

        guard.release().await?;

        Ok(())
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
    pub async fn register_table(
        &self,
        req: RegisterTableRequest,
        opts: WriteOptions,
    ) -> Result<Table> {
        // Check optimistic locking
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

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_catalog_state(&manifest.catalog.snapshot_path)
            .await?;

        // Find namespace
        let ns = state
            .namespaces
            .iter()
            .find(|ns| ns.name == req.namespace)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: req.namespace.clone(),
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
                name: format!("{}.{}", req.namespace, req.name),
            });
        }

        let now = Utc::now().timestamp_millis();
        let table_id = Uuid::now_v7().to_string();

        let table = Table {
            id: table_id.clone(),
            namespace_id: namespace_id.clone(),
            name: req.name.clone(),
            description: req.description.clone(),
            location: req.location.clone(),
            format: req.format.clone(),
            created_at: now,
            updated_at: now,
        };

        state.tables.push(TableRecord::from(&table));

        // Add columns
        for (ordinal, col_def) in req.columns.iter().enumerate() {
            state.columns.push(ColumnRecord {
                id: Uuid::now_v7().to_string(),
                table_id: table_id.clone(),
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                is_nullable: col_def.is_nullable,
                ordinal: ordinal as i32,
                description: col_def.description.clone(),
            });
        }

        // Write new snapshot
        let next_version = manifest.catalog.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_catalog_snapshot(&guard, next_version, &state)
            .await?;

        // Publish under the same lock to prevent intermediate contention.
        self.tier1
            .update_locked(&guard, |m| {
                m.snapshot_version = snapshot.version;
                m.snapshot_path.clone_from(&snapshot.path);
                m.snapshot = Some(snapshot.clone());
                Ok(())
            })
            .await?;

        guard.release().await?;

        Ok(table)
    }

    /// Updates a table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table doesn't exist
    /// - `opts.if_match` doesn't match current version
    /// - Lock acquisition or storage operations fail
    pub async fn update_table(
        &self,
        namespace: &str,
        name: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<Table> {
        // Check optimistic locking
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

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_catalog_state(&manifest.catalog.snapshot_path)
            .await?;

        // Find namespace
        let ns = state
            .namespaces
            .iter()
            .find(|ns| ns.name == namespace)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            })?;
        let namespace_id = &ns.id;

        // Find and update table
        let table_rec = state
            .tables
            .iter_mut()
            .find(|t| &t.namespace_id == namespace_id && t.name == name)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "table".into(),
                name: format!("{}.{}", namespace, name),
            })?;

        let now = Utc::now().timestamp_millis();
        table_rec.updated_at = now;

        if let Some(desc) = patch.description {
            table_rec.description = desc;
        }
        if let Some(loc) = patch.location {
            table_rec.location = loc;
        }
        if let Some(fmt) = patch.format {
            table_rec.format = fmt;
        }

        let updated_table = Table::from(table_rec.clone());

        // Write new snapshot
        let next_version = manifest.catalog.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_catalog_snapshot(&guard, next_version, &state)
            .await?;

        // Publish under the same lock to prevent intermediate contention.
        self.tier1
            .update_locked(&guard, |m| {
                m.snapshot_version = snapshot.version;
                m.snapshot_path.clone_from(&snapshot.path);
                m.snapshot = Some(snapshot.clone());
                Ok(())
            })
            .await?;

        guard.release().await?;

        Ok(updated_table)
    }

    /// Drops a table.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table doesn't exist
    /// - Lock acquisition or storage operations fail
    pub async fn drop_table(&self, namespace: &str, name: &str, opts: WriteOptions) -> Result<()> {
        // Check optimistic locking
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

        let guard = self
            .tier1
            .acquire_lock(self.lock_ttl, self.lock_max_retries)
            .await?;

        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_catalog_state(&manifest.catalog.snapshot_path)
            .await?;

        // Find namespace
        let ns = state
            .namespaces
            .iter()
            .find(|ns| ns.name == namespace)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            })?;
        let namespace_id = ns.id.clone();

        // Find table
        let table_idx = state
            .tables
            .iter()
            .position(|t| t.namespace_id == namespace_id && t.name == name)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "table".into(),
                name: format!("{}.{}", namespace, name),
            })?;

        let table_id = state.tables[table_idx].id.clone();

        // Remove columns for this table
        state.columns.retain(|c| c.table_id != table_id);

        // Remove table
        state.tables.remove(table_idx);

        // Write new snapshot
        let next_version = manifest.catalog.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_catalog_snapshot(&guard, next_version, &state)
            .await?;

        // Publish under the same lock to prevent intermediate contention.
        self.tier1
            .update_locked(&guard, |m| {
                m.snapshot_version = snapshot.version;
                m.snapshot_path.clone_from(&snapshot.path);
                m.snapshot = Some(snapshot.clone());
                Ok(())
            })
            .await?;

        guard.release().await?;

        Ok(())
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
        _opts: WriteOptions,
    ) -> Result<LineageEdge> {
        let guard = self
            .lineage_lock
            .acquire(self.lock_ttl, self.lock_max_retries)
            .await?;

        // Load current lineage state
        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_lineage_state(&manifest.lineage.edges_path)
            .await?;

        state.edges.push(LineageEdgeRecord::from(&edge));

        // Write new snapshot
        // Note: For MVP, we're using a simple version increment
        let next_version = manifest.lineage.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_lineage_snapshot(&guard, next_version, &state)
            .await?;

        self.publish_lineage_snapshot(&guard, &snapshot, manifest.lineage.snapshot_version)
            .await?;

        guard.release().await?;

        Ok(edge)
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
        _opts: WriteOptions,
    ) -> Result<Vec<LineageEdge>> {
        if edges.is_empty() {
            return Ok(Vec::new());
        }

        let guard = self
            .lineage_lock
            .acquire(self.lock_ttl, self.lock_max_retries)
            .await?;

        // Load current lineage state
        let manifest = self.tier1.read_manifest().await?;
        let mut state = self
            .load_lineage_state(&manifest.lineage.edges_path)
            .await?;

        for edge in &edges {
            state.edges.push(LineageEdgeRecord::from(edge));
        }

        // Write new snapshot
        let next_version = manifest.lineage.snapshot_version + 1;
        let snapshot = self
            .tier1
            .write_lineage_snapshot(&guard, next_version, &state)
            .await?;

        self.publish_lineage_snapshot(&guard, &snapshot, manifest.lineage.snapshot_version)
            .await?;

        guard.release().await?;

        Ok(edges)
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

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Loads catalog state from the current snapshot.
    ///
    /// Returns empty state if snapshot doesn't exist yet.
    async fn load_catalog_state(&self, snapshot_path: &str) -> Result<CatalogState> {
        // snapshot_path looks like "snapshots/catalog/v1/"
        // If version is 0 or path is empty, return empty state
        if snapshot_path.is_empty() || snapshot_path.contains("/v0/") {
            return Ok(CatalogState::empty());
        }

        // Parse version from path (e.g., "snapshots/catalog/v1/" -> 1)
        let version = snapshot_path
            .split("/v")
            .last()
            .and_then(|s| s.trim_end_matches('/').parse::<u64>().ok())
            .unwrap_or(0);

        if version == 0 {
            return Ok(CatalogState::empty());
        }

        // Read individual Parquet files
        let ns_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "namespaces.parquet");
        let tables_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "tables.parquet");
        let columns_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "columns.parquet");

        let namespaces = match self.storage.get_raw(&ns_path).await {
            Ok(bytes) => crate::parquet_util::read_namespaces(&bytes)?,
            Err(_) => Vec::new(),
        };

        let tables = match self.storage.get_raw(&tables_path).await {
            Ok(bytes) => crate::parquet_util::read_tables(&bytes)?,
            Err(_) => Vec::new(),
        };

        let columns = match self.storage.get_raw(&columns_path).await {
            Ok(bytes) => crate::parquet_util::read_columns(&bytes)?,
            Err(_) => Vec::new(),
        };

        Ok(CatalogState {
            namespaces,
            tables,
            columns,
        })
    }

    /// Loads lineage state from the current snapshot.
    ///
    /// Returns empty state if snapshot doesn't exist yet.
    async fn load_lineage_state(&self, edges_path: &str) -> Result<LineageState> {
        // edges_path looks like "snapshots/lineage/v1/"
        if edges_path.is_empty() || edges_path.contains("/v0/") {
            return Ok(LineageState::empty());
        }

        // Parse version from path
        let version = edges_path
            .split("/v")
            .last()
            .and_then(|s| s.trim_end_matches('/').parse::<u64>().ok())
            .unwrap_or(0);

        if version == 0 {
            return Ok(LineageState::empty());
        }

        let path =
            CatalogPaths::snapshot_file(CatalogDomain::Lineage, version, "lineage_edges.parquet");
        let edges = match self.storage.get_raw(&path).await {
            Ok(bytes) => crate::parquet_util::read_lineage_edges(&bytes)?,
            Err(_) => Vec::new(),
        };

        Ok(LineageState { edges })
    }

    async fn publish_lineage_snapshot(
        &self,
        _guard: &LockGuard<dyn StorageBackend>,
        snapshot: &SnapshotInfo,
        expected_prev_version: u64,
    ) -> Result<()> {
        for attempt in 1..=DEFAULT_LINEAGE_CAS_MAX_RETRIES {
            let root = self.read_root_manifest().await?;
            let (mut lineage, lineage_version) =
                self.read_lineage_manifest_with_version(&root).await?;

            if lineage.snapshot_version >= snapshot.version {
                return Ok(());
            }

            if lineage.snapshot_version != expected_prev_version {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "lineage manifest version mismatch: expected {}, got {}",
                        expected_prev_version, lineage.snapshot_version
                    ),
                });
            }

            lineage.snapshot_version = snapshot.version;
            lineage.edges_path.clone_from(&snapshot.path);
            lineage.snapshot = Some(snapshot.clone());
            lineage.updated_at = Utc::now();

            let bytes = serde_json::to_vec(&lineage).map_err(|e| CatalogError::Serialization {
                message: format!("serialize lineage manifest: {e}"),
            })?;

            let result = self
                .storage
                .put_raw(
                    &root.lineage_manifest_path,
                    Bytes::from(bytes),
                    WritePrecondition::MatchesVersion(lineage_version),
                )
                .await?;

            match result {
                WriteResult::Success { .. } => return Ok(()),
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == DEFAULT_LINEAGE_CAS_MAX_RETRIES {
                        break;
                    }
                    crate::metrics::record_cas_retry("lineage_manifest");
                    continue;
                }
            }
        }

        Err(CatalogError::CasFailed {
            message: format!(
                "lineage manifest update lost CAS race after {DEFAULT_LINEAGE_CAS_MAX_RETRIES} retries"
            ),
        })
    }

    async fn read_root_manifest(&self) -> Result<RootManifest> {
        let bytes = self.storage.get_raw(CatalogPaths::ROOT_MANIFEST).await?;
        let mut root: RootManifest =
            serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
                message: format!("parse root manifest: {e}"),
            })?;
        root.normalize_paths();
        Ok(root)
    }

    async fn read_lineage_manifest_with_version(
        &self,
        root: &RootManifest,
    ) -> Result<(LineageManifest, String)> {
        let meta = self
            .storage
            .head_raw(&root.lineage_manifest_path)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                entity: "manifest".into(),
                name: root.lineage_manifest_path.clone(),
            })?;

        let bytes = self.storage.get_raw(&root.lineage_manifest_path).await?;
        let lineage: LineageManifest =
            serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
                message: format!("parse lineage manifest: {e}"),
            })?;

        Ok((lineage, meta.version))
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
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use std::sync::Arc;
    use ulid::Ulid;

    fn setup() -> CatalogWriter {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").expect("valid storage");
        CatalogWriter::new(storage)
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
                            description: Some("Primary key".to_string()),
                        },
                        ColumnDefinition {
                            name: "email".to_string(),
                            data_type: "STRING".to_string(),
                            is_nullable: true,
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
        let writer = CatalogWriter::new(storage.clone());
        let reader = crate::reader::CatalogReader::new(storage);

        writer.initialize().await.expect("initialize");

        // After initialize, lineage is at v1 (empty). First edge publish creates v2.
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
        assert_eq!(info1.unwrap().version, 2);

        let graph1 = reader.get_lineage("table_a").await.expect("lineage");
        assert_eq!(graph1.downstream.len(), 1);

        // Second write should publish v3 and include both edges.
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
        assert_eq!(info2.unwrap().version, 3);

        let graph2 = reader.get_lineage("table_a").await.expect("lineage 2");
        assert_eq!(graph2.downstream.len(), 2);
    }
}
