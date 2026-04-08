//! Catalog read operations (`CatalogReader` facade).
//!
//! The catalog reader provides query-native access to catalog metadata.
//! It reads directly from Parquet snapshots, enabling fast, consistent reads
//! without an always-on read infrastructure.
//!
//! ## Key Invariants
//!
//! - **Invariant 5: Readers never need ledger** - All reads come from Parquet snapshots
//! - **Invariant 6: No bucket listing for correctness** - Uses manifest-driven paths
//!
//! ## Browser Read Path
//!
//! The reader supports browser-direct reads via signed URLs:
//!
//! ```text
//! 1. Browser calls get_mintable_paths() → list of allowed snapshot files
//! 2. Browser calls mint_signed_urls(paths) → signed URLs for those files
//! 3. Browser loads Parquet directly via DuckDB-WASM
//! ```

// MVP: Allow some pedantic lints that will be cleaned up in refinement
#![allow(clippy::doc_markdown)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::uninlined_format_args)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};

use arco_core::control_plane_transactions::{
    ControlPlaneTxDomain, ControlPlaneTxPaths, ControlPlaneTxStatus, RootTxManifest, RootTxRecord,
};
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

use crate::error::{CatalogError, Result};
use crate::manifest::{
    CatalogDomainManifest, CatalogManifest, DomainManifestPointer, ExecutionsManifest,
    LineageManifest, RootManifest, SearchManifest, SnapshotInfo, TailRange,
};
use crate::parquet_util;
use crate::write_options::SnapshotVersion;
use crate::writer::{Catalog, Column, LineageEdge, Namespace, Table};

// ============================================================================
// Freshness Metadata
// ============================================================================

/// Freshness metadata for snapshot reads.
///
/// Provides information about when the snapshot was published
/// and its version, enabling clients to make freshness decisions.
#[derive(Debug, Clone)]
pub struct SnapshotFreshness {
    /// Snapshot version ID.
    pub version: SnapshotVersion,
    /// When this snapshot was published.
    pub published_at: DateTime<Utc>,
    /// Optional: tail commit range for incremental refresh.
    pub tail: Option<TailRange>,
}

// ============================================================================
// Lineage Graph
// ============================================================================

/// Lineage graph for a table.
///
/// Contains edges where the table is either the source or target,
/// enabling upstream/downstream traversal.
#[derive(Debug, Clone, Default)]
pub struct LineageGraph {
    /// Edges where the queried table is the target (upstream dependencies).
    pub upstream: Vec<LineageEdge>,
    /// Edges where the queried table is the source (downstream dependents).
    pub downstream: Vec<LineageEdge>,
}

// ============================================================================
// Signed URL Types
// ============================================================================

/// A signed URL for browser-direct access to a Parquet file.
#[derive(Debug, Clone)]
pub struct SignedUrl {
    /// Original path that was requested.
    pub path: String,
    /// Signed URL with time-limited access.
    pub url: String,
    /// When the signed URL expires.
    pub expires_at: DateTime<Utc>,
}

// ============================================================================
// Catalog Reader
// ============================================================================

/// Reader for catalog metadata.
///
/// Provides fast, consistent reads of catalog state by reading
/// directly from Parquet snapshots. Never accesses the ledger
/// (Invariant 5: Readers never need ledger).
///
/// # Example
///
/// ```rust,ignore
/// use arco_catalog::CatalogReader;
/// use arco_core::ScopedStorage;
///
/// let storage = ScopedStorage::new(backend, "acme-corp", "production")?;
/// let reader = CatalogReader::new(storage);
///
/// // List namespaces
/// let namespaces = reader.list_namespaces().await?;
///
/// // Get freshness info
/// let freshness = reader.get_freshness(CatalogDomain::Catalog).await?;
/// println!("Snapshot published at: {}", freshness.published_at);
///
/// // Browser read path: get mintable paths, then mint URLs
/// let paths = reader.get_mintable_paths(CatalogDomain::Catalog).await?;
/// let urls = reader.mint_signed_urls(paths, Duration::from_secs(900)).await?;
/// ```
pub struct CatalogReader {
    storage: ScopedStorage,
    table_lookup_cache: RwLock<Option<TableLookupCache>>,
}

/// Explicit opt-in catalog read view pinned to one root transaction token.
///
/// This view resolves `root:{tx_id}` once, then reads the immutable catalog
/// manifest referenced by that root super-manifest for all catalog-domain
/// operations exposed here.
#[cfg(test)]
#[derive(Debug, Clone)]
struct PinnedCatalogReader<'a> {
    reader: &'a CatalogReader,
    catalog_manifest: CatalogDomainManifest,
}

#[derive(Debug, Clone)]
struct TableLookupCache {
    snapshot_version: u64,
    by_id: Arc<HashMap<String, Table>>,
}

fn join_snapshot_path(dir: &str, file: &str) -> String {
    if dir.ends_with('/') {
        format!("{dir}{file}")
    } else {
        format!("{dir}/{file}")
    }
}

impl std::fmt::Debug for CatalogReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogReader")
            .field("storage", &"ScopedStorage")
            .finish()
    }
}

impl CatalogReader {
    /// Creates a new catalog reader for the given workspace.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            table_lookup_cache: RwLock::new(None),
        }
    }

    // ========================================================================
    // Manifest Reading (internal helpers)
    // ========================================================================

    /// Reads the combined catalog manifest from storage.
    async fn read_manifest(&self) -> Result<CatalogManifest> {
        // Read root manifest
        let root_path = CatalogPaths::ROOT_MANIFEST;
        let root_bytes = self.storage.get_raw(root_path).await?;
        let root: RootManifest =
            serde_json::from_slice(&root_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse root manifest: {}", e),
            })?;
        let mut root = root;
        root.normalize_paths();

        // Read domain manifests in parallel from pointer-resolved immutable snapshots.
        let (catalog_bytes, lineage_bytes, executions_bytes, search_bytes) = tokio::join!(
            self.read_domain_manifest_bytes(CatalogDomain::Catalog, None),
            self.read_domain_manifest_bytes(CatalogDomain::Lineage, None),
            self.read_domain_manifest_bytes(
                CatalogDomain::Executions,
                Some(&root.executions_manifest_path)
            ),
            self.read_domain_manifest_bytes(CatalogDomain::Search, None),
        );

        let catalog: CatalogDomainManifest =
            serde_json::from_slice(&catalog_bytes?).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse catalog manifest: {}", e),
            })?;

        let lineage: LineageManifest =
            serde_json::from_slice(&lineage_bytes?).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse lineage manifest: {}", e),
            })?;

        let executions: ExecutionsManifest =
            serde_json::from_slice(&executions_bytes?).map_err(|e| {
                CatalogError::Serialization {
                    message: format!("failed to parse executions manifest: {}", e),
                }
            })?;

        let search: SearchManifest =
            serde_json::from_slice(&search_bytes?).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse search manifest: {}", e),
            })?;

        Ok(CatalogManifest {
            version: root.version,
            catalog,
            lineage,
            executions,
            search,
            created_at: root.updated_at,
            updated_at: root.updated_at,
        })
    }

    async fn table_lookup_by_id(
        &self,
        snapshot_version: u64,
    ) -> Result<Arc<HashMap<String, Table>>> {
        let cached_lookup = {
            let cache =
                self.table_lookup_cache
                    .read()
                    .map_err(|_| CatalogError::InvariantViolation {
                        message: "table lookup cache lock poisoned".to_string(),
                    })?;
            cache.as_ref().and_then(|entry| {
                (entry.snapshot_version == snapshot_version).then(|| Arc::clone(&entry.by_id))
            })
        };
        if let Some(cached_lookup) = cached_lookup {
            return Ok(cached_lookup);
        }

        let tables_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, snapshot_version, "tables.parquet");
        let bytes = self.storage.get_raw(&tables_path).await?;
        let records = parquet_util::read_tables(&bytes)?;
        let by_id = Arc::new(
            records
                .into_iter()
                .map(Table::from)
                .map(|table| (table.id.clone(), table))
                .collect::<HashMap<_, _>>(),
        );

        let existing_lookup = {
            let mut cache =
                self.table_lookup_cache
                    .write()
                    .map_err(|_| CatalogError::InvariantViolation {
                        message: "table lookup cache lock poisoned".to_string(),
                    })?;
            if let Some(entry) = cache.as_ref() {
                if entry.snapshot_version == snapshot_version {
                    Some(Arc::clone(&entry.by_id))
                } else {
                    *cache = Some(TableLookupCache {
                        snapshot_version,
                        by_id: Arc::clone(&by_id),
                    });
                    None
                }
            } else {
                *cache = Some(TableLookupCache {
                    snapshot_version,
                    by_id: Arc::clone(&by_id),
                });
                None
            }
        };
        if let Some(existing_lookup) = existing_lookup {
            return Ok(existing_lookup);
        }

        Ok(by_id)
    }

    async fn read_domain_manifest_bytes(
        &self,
        domain: CatalogDomain,
        executions_path: Option<&str>,
    ) -> Result<bytes::Bytes> {
        if domain == CatalogDomain::Executions {
            let path = executions_path.ok_or_else(|| CatalogError::InvariantViolation {
                message: "executions manifest path is required".to_string(),
            })?;
            return self.storage.get_raw(path).await.map_err(Into::into);
        }

        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse manifest pointer at {pointer_path}: {e}"),
            })?;
        self.storage
            .get_raw(&pointer.manifest_path)
            .await
            .map_err(Into::into)
    }

    async fn read_catalog_manifest_for_root_token(
        &self,
        read_token: &str,
    ) -> Result<CatalogDomainManifest> {
        let tx_id = parse_root_read_token(read_token)?;
        let tx_record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id);
        let tx_record_bytes = self.storage.get_raw(&tx_record_path).await?;
        let tx_record: RootTxRecord =
            serde_json::from_slice(&tx_record_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse root transaction record: {e}"),
            })?;

        if tx_record.status != ControlPlaneTxStatus::Visible {
            return Err(CatalogError::PreconditionFailed {
                message: format!("root transaction '{tx_id}' is not visible"),
            });
        }

        let super_manifest_path = tx_record
            .result
            .as_ref()
            .map(|result| result.super_manifest_path.as_str())
            .filter(|path| !path.is_empty())
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: format!(
                    "visible root transaction '{tx_id}' is missing super_manifest_path"
                ),
            })?;

        let super_manifest_bytes = self.storage.get_raw(super_manifest_path).await?;
        let super_manifest: RootTxManifest = serde_json::from_slice(&super_manifest_bytes)
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse root super-manifest: {e}"),
            })?;

        let catalog = super_manifest
            .domains
            .get(&ControlPlaneTxDomain::Catalog)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "root transaction domain".to_string(),
                name: "catalog".to_string(),
            })?;

        let catalog_manifest_bytes = self.storage.get_raw(&catalog.manifest_path).await?;
        serde_json::from_slice(&catalog_manifest_bytes).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse pinned catalog manifest: {e}"),
        })
    }

    /// Resolves an explicit root read token into a pinned catalog read view.
    ///
    /// This is the opt-in path for root-token reads. It does not change the
    /// default pointer-first behavior of [`CatalogReader`].
    #[cfg(test)]
    async fn pin_root_token(&self, read_token: &str) -> Result<PinnedCatalogReader<'_>> {
        Ok(PinnedCatalogReader {
            reader: self,
            catalog_manifest: self
                .read_catalog_manifest_for_root_token(read_token)
                .await?,
        })
    }

    async fn list_catalogs_from_catalog_manifest(
        &self,
        catalog_manifest: &CatalogDomainManifest,
    ) -> Result<Vec<Catalog>> {
        if catalog_manifest.snapshot_version == 0 {
            return Ok(Vec::new());
        }

        let catalogs_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            catalog_manifest.snapshot_version,
            "catalogs.parquet",
        );

        let records = match self.storage.get_raw(&catalogs_path).await {
            Ok(bytes) => parquet_util::read_catalogs(&bytes)?,
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Vec::new()
            }
            Err(e) => return Err(e.into()),
        };

        Ok(records.into_iter().map(Catalog::from).collect())
    }

    async fn list_namespaces_from_catalog_manifest(
        &self,
        manifest: &CatalogDomainManifest,
    ) -> Result<Vec<Namespace>> {
        if manifest.snapshot_version == 0 {
            return Ok(Vec::new());
        }

        let ns_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.snapshot_version,
            "namespaces.parquet",
        );

        let bytes = self.storage.get_raw(&ns_path).await?;
        let records = parquet_util::read_namespaces(&bytes)?;

        Ok(records.into_iter().map(Namespace::from).collect())
    }

    async fn list_schemas_from_catalog_manifest(
        &self,
        catalog_manifest: &CatalogDomainManifest,
        catalog: &str,
    ) -> Result<Vec<Namespace>> {
        if catalog_manifest.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "catalog".into(),
                name: catalog.to_string(),
            });
        }

        let catalogs = self
            .list_catalogs_from_catalog_manifest(catalog_manifest)
            .await?;

        let default_catalog_id = catalogs
            .iter()
            .find(|c| c.name == "default")
            .map(|c| c.id.as_str());
        let requested_catalog_id = catalogs
            .iter()
            .find(|c| c.name == catalog)
            .map(|c| c.id.as_str());

        let effective_requested_catalog_id = if let Some(id) = requested_catalog_id {
            Some(id)
        } else if catalog == "default" {
            default_catalog_id
        } else {
            return Err(CatalogError::NotFound {
                entity: "catalog".into(),
                name: catalog.to_string(),
            });
        };

        let namespaces = self
            .list_namespaces_from_catalog_manifest(catalog_manifest)
            .await?;
        Ok(namespaces
            .into_iter()
            .filter(|ns| match effective_requested_catalog_id {
                Some(requested) => {
                    ns.catalog_id.as_deref().or(default_catalog_id) == Some(requested)
                }
                None => ns.catalog_id.is_none(),
            })
            .collect())
    }

    async fn list_tables_for_namespace_id(
        &self,
        catalog_manifest: &CatalogDomainManifest,
        namespace_id: &str,
    ) -> Result<Vec<Table>> {
        let tables_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            catalog_manifest.snapshot_version,
            "tables.parquet",
        );
        let bytes = self.storage.get_raw(&tables_path).await?;
        let records = parquet_util::read_tables(&bytes)?;

        Ok(records
            .into_iter()
            .filter(|table| table.namespace_id == namespace_id)
            .map(Table::from)
            .collect())
    }

    async fn list_tables_from_catalog_manifest(
        &self,
        catalog_manifest: &CatalogDomainManifest,
        namespace: &str,
    ) -> Result<Vec<Table>> {
        if catalog_manifest.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            });
        }

        let namespace_id = self
            .list_namespaces_from_catalog_manifest(catalog_manifest)
            .await?
            .into_iter()
            .find(|ns| ns.name == namespace)
            .map(|ns| ns.id)
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            })?;

        self.list_tables_for_namespace_id(catalog_manifest, namespace_id.as_str())
            .await
    }

    async fn get_table_by_id_from_catalog_manifest(
        &self,
        catalog_manifest: &CatalogDomainManifest,
        table_id: &str,
    ) -> Result<Option<Table>> {
        if catalog_manifest.snapshot_version == 0 {
            return Ok(None);
        }

        let by_id = self
            .table_lookup_by_id(catalog_manifest.snapshot_version)
            .await?;

        Ok(by_id.get(table_id).cloned())
    }

    async fn get_columns_from_catalog_manifest(
        &self,
        catalog_manifest: &CatalogDomainManifest,
        table_id: &str,
    ) -> Result<Vec<Column>> {
        if catalog_manifest.snapshot_version == 0 {
            return Ok(Vec::new());
        }

        let columns_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            catalog_manifest.snapshot_version,
            "columns.parquet",
        );
        let bytes = self.storage.get_raw(&columns_path).await?;
        let records = parquet_util::read_columns(&bytes)?;

        Ok(records
            .into_iter()
            .filter(|column| column.table_id == table_id)
            .map(Column::from)
            .collect())
    }
    // ========================================================================
    // Catalog Operations (UC-like model)
    // ========================================================================

    /// Lists catalogs in the metastore.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be read or Parquet decoding fails.
    pub async fn list_catalogs(&self) -> Result<Vec<Catalog>> {
        let manifest = self.read_manifest().await?;
        self.list_catalogs_from_catalog_manifest(&manifest.catalog)
            .await
    }

    /// Gets a catalog by name.
    ///
    /// # Returns
    ///
    /// Returns `None` if the catalog doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be read or Parquet decoding fails.
    pub async fn get_catalog(&self, name: &str) -> Result<Option<Catalog>> {
        let catalogs = self.list_catalogs().await?;
        Ok(catalogs.into_iter().find(|c| c.name == name))
    }

    /// Lists schemas (namespaces) within a catalog.
    ///
    /// Legacy namespaces with `catalog_id = NULL` are treated as belonging to the
    /// `default` catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog doesn't exist or snapshot reads fail.
    pub async fn list_schemas(&self, catalog: &str) -> Result<Vec<Namespace>> {
        let manifest = self.read_manifest().await?;
        self.list_schemas_from_catalog_manifest(&manifest.catalog, catalog)
            .await
    }

    /// Lists tables within a schema (namespace) inside the given catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog or schema doesn't exist, or snapshot reads fail.
    pub async fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Result<Vec<Table>> {
        let manifest = self.read_manifest().await?;
        let namespace_id = self
            .list_schemas_from_catalog_manifest(&manifest.catalog, catalog)
            .await?
            .iter()
            .find(|ns| ns.name == schema)
            .map(|ns| ns.id.clone())
            .ok_or_else(|| CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            })?;
        self.list_tables_for_namespace_id(&manifest.catalog, namespace_id.as_str())
            .await
    }

    /// Gets a table by full UC-like name (catalog.schema.table).
    ///
    /// # Returns
    ///
    /// Returns `None` if the table doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog or schema doesn't exist, or snapshot reads fail.
    pub async fn get_table_in_schema(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<Table>> {
        let tables = self.list_tables_in_schema(catalog, schema).await?;
        Ok(tables.into_iter().find(|t| t.name == table))
    }

    // ========================================================================
    // Namespace Operations (Tier 1 - snapshot reads)
    // ========================================================================

    /// Lists all namespaces in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read.
    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>> {
        let manifest = self.read_manifest().await?;
        self.list_namespaces_from_catalog_manifest(&manifest.catalog)
            .await
    }

    /// Lists namespaces from a pinned root read token.
    ///
    /// This opt-in path resolves `root:{tx_id}` through the visible root
    /// transaction record and immutable root super-manifest instead of the
    /// moving catalog pointer.
    ///
    /// # Errors
    ///
    /// Returns an error if the root token is invalid, not visible, omits the
    /// catalog domain, or the pinned snapshot cannot be read.
    pub async fn list_namespaces_for_root_token(&self, read_token: &str) -> Result<Vec<Namespace>> {
        let manifest = self
            .read_catalog_manifest_for_root_token(read_token)
            .await?;
        self.list_namespaces_from_catalog_manifest(&manifest).await
    }

    /// Gets a namespace by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name
    ///
    /// # Returns
    ///
    /// Returns `None` if the namespace doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read.
    pub async fn get_namespace(&self, name: &str) -> Result<Option<Namespace>> {
        let manifest = self.read_manifest().await?;
        let namespaces = self
            .list_namespaces_from_catalog_manifest(&manifest.catalog)
            .await?;
        Ok(namespaces.into_iter().find(|ns| ns.name == name))
    }

    // ========================================================================
    // Table Operations (Tier 1 - snapshot reads)
    // ========================================================================

    /// Lists all tables in a namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace name
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist or the snapshot cannot be read.
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<Table>> {
        let manifest = self.read_manifest().await?;
        self.list_tables_from_catalog_manifest(&manifest.catalog, namespace)
            .await
    }

    /// Gets a table by namespace and name.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace name
    /// * `name` - The table name
    ///
    /// # Returns
    ///
    /// Returns `None` if the table doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist or the snapshot cannot be read.
    pub async fn get_table(&self, namespace: &str, name: &str) -> Result<Option<Table>> {
        let manifest = self.read_manifest().await?;
        let tables = self
            .list_tables_from_catalog_manifest(&manifest.catalog, namespace)
            .await?;
        Ok(tables.into_iter().find(|t| t.name == name))
    }

    /// Gets a table by table ID.
    ///
    /// # Returns
    ///
    /// Returns `None` if the table doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot reads fail.
    pub async fn get_table_by_id(&self, table_id: &str) -> Result<Option<Table>> {
        let manifest = self.read_manifest().await?;
        self.get_table_by_id_from_catalog_manifest(&manifest.catalog, table_id)
            .await
    }

    /// Lists columns for a table by table ID.
    ///
    /// Columns are returned in ordinal order.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read.
    pub async fn get_columns(&self, table_id: &str) -> Result<Vec<Column>> {
        let manifest = self.read_manifest().await?;
        self.get_columns_from_catalog_manifest(&manifest.catalog, table_id)
            .await
    }

    // ========================================================================
    // Lineage Operations (Tier 1 - snapshot reads)
    // ========================================================================

    /// Gets the lineage graph for a table.
    ///
    /// Returns edges where the table is either the source (downstream dependents)
    /// or target (upstream dependencies).
    ///
    /// # Arguments
    ///
    /// * `table_id` - The table ID to get lineage for
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read.
    pub async fn get_lineage(&self, table_id: &str) -> Result<LineageGraph> {
        let manifest = self.read_manifest().await?;

        if manifest.lineage.snapshot_version == 0 {
            return Ok(LineageGraph::default());
        }

        let edges_path = CatalogPaths::snapshot_file(
            CatalogDomain::Lineage,
            manifest.lineage.snapshot_version,
            "lineage_edges.parquet",
        );

        let Ok(bytes) = self.storage.get_raw(&edges_path).await else {
            return Ok(LineageGraph::default());
        };

        let records = parquet_util::read_lineage_edges(&bytes)?;

        let upstream: Vec<LineageEdge> = records
            .iter()
            .filter(|e| e.target_id == table_id)
            .cloned()
            .map(LineageEdge::from)
            .collect();

        let downstream: Vec<LineageEdge> = records
            .iter()
            .filter(|e| e.source_id == table_id)
            .cloned()
            .map(LineageEdge::from)
            .collect();

        Ok(LineageGraph {
            upstream,
            downstream,
        })
    }

    // ========================================================================
    // Freshness Metadata
    // ========================================================================

    /// Gets freshness metadata for a domain snapshot.
    ///
    /// Provides information about when the snapshot was published
    /// and its version, enabling clients to make freshness decisions.
    ///
    /// # Arguments
    ///
    /// * `domain` - The catalog domain to check
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be read.
    pub async fn get_freshness(&self, domain: CatalogDomain) -> Result<SnapshotFreshness> {
        let manifest = self.read_manifest().await?;

        match domain {
            CatalogDomain::Catalog => Ok(SnapshotFreshness {
                version: SnapshotVersion::new(manifest.catalog.snapshot_version),
                published_at: manifest
                    .catalog
                    .snapshot
                    .as_ref()
                    .map_or(manifest.catalog.updated_at, |s| s.published_at),
                tail: None,
            }),
            CatalogDomain::Lineage => Ok(SnapshotFreshness {
                version: SnapshotVersion::new(manifest.lineage.snapshot_version),
                published_at: manifest
                    .lineage
                    .snapshot
                    .as_ref()
                    .map_or(manifest.lineage.updated_at, |s| s.published_at),
                tail: None,
            }),
            CatalogDomain::Executions => {
                let published_at = manifest
                    .executions
                    .last_compaction_at
                    .unwrap_or(manifest.executions.updated_at);
                Ok(SnapshotFreshness {
                    version: SnapshotVersion::new(manifest.executions.snapshot_version),
                    published_at,
                    tail: None,
                })
            }
            CatalogDomain::Search => Ok(SnapshotFreshness {
                version: SnapshotVersion::new(manifest.search.snapshot_version),
                published_at: manifest
                    .search
                    .snapshot
                    .as_ref()
                    .map_or(manifest.search.updated_at, |s| s.published_at),
                tail: None,
            }),
        }
    }

    // ========================================================================
    // Signed URL Minting (manifest-driven allowlist)
    // ========================================================================

    /// Gets all mintable paths for a domain from the current manifest.
    ///
    /// Browser clients call this to discover available snapshot files,
    /// then request signed URLs for specific files.
    ///
    /// # Arguments
    ///
    /// * `domain` - The catalog domain to get paths for
    ///
    /// # Returns
    ///
    /// List of file paths that can be minted as signed URLs.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be read.
    pub async fn get_mintable_paths(&self, domain: CatalogDomain) -> Result<Vec<String>> {
        let manifest = self.read_manifest().await?;

        let paths = match domain {
            CatalogDomain::Catalog => {
                if let Some(snapshot) = &manifest.catalog.snapshot {
                    snapshot
                        .files
                        .iter()
                        .map(|f| join_snapshot_path(&snapshot.path, &f.path))
                        .collect()
                } else if manifest.catalog.snapshot_version == 0 {
                    Vec::new()
                } else {
                    // Legacy fallback: derive known paths from version.
                    let version = manifest.catalog.snapshot_version;
                    vec![
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "catalogs.parquet",
                        ),
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "namespaces.parquet",
                        ),
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "tables.parquet",
                        ),
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "columns.parquet",
                        ),
                    ]
                }
            }
            CatalogDomain::Lineage => {
                if let Some(snapshot) = &manifest.lineage.snapshot {
                    snapshot
                        .files
                        .iter()
                        .map(|f| join_snapshot_path(&snapshot.path, &f.path))
                        .collect()
                } else if manifest.lineage.snapshot_version == 0 {
                    Vec::new()
                } else {
                    // Legacy fallback: derive known path from version.
                    let version = manifest.lineage.snapshot_version;
                    vec![CatalogPaths::snapshot_file(
                        CatalogDomain::Lineage,
                        version,
                        "lineage_edges.parquet",
                    )]
                }
            }
            CatalogDomain::Executions => {
                // Tier-2: paths come from manifest snapshot_path
                manifest
                    .executions
                    .snapshot_path
                    .map(|p| vec![p])
                    .unwrap_or_default()
            }
            CatalogDomain::Search => {
                if let Some(snapshot) = &manifest.search.snapshot {
                    snapshot
                        .files
                        .iter()
                        .map(|f| join_snapshot_path(&snapshot.path, &f.path))
                        .collect()
                } else if manifest.search.snapshot_version == 0 {
                    Vec::new()
                } else {
                    let version = manifest.search.snapshot_version;
                    vec![CatalogPaths::snapshot_file(
                        CatalogDomain::Search,
                        version,
                        "token_postings.parquet",
                    )]
                }
            }
        };

        Ok(paths)
    }

    /// Mints signed URLs for browser-direct reads.
    ///
    /// **Security**: Only allows paths enumerated in the manifest's snapshot files.
    /// This prevents:
    /// - Minting URLs for ledger files (Tier-2 internal)
    /// - Minting URLs for manifest files (metadata leak)
    /// - Minting URLs for arbitrary paths (path traversal)
    ///
    /// # Arguments
    ///
    /// * `paths` - Paths to mint URLs for (must be in manifest allowlist)
    /// * `ttl` - Time-to-live for the signed URLs
    ///
    /// # Errors
    ///
    /// Returns `CatalogError::Validation` if any path is not in the allowlist.
    /// Returns an error if URL signing fails.
    pub async fn mint_signed_urls(
        &self,
        paths: Vec<String>,
        ttl: Duration,
    ) -> Result<Vec<SignedUrl>> {
        // Cap TTL at 1 hour for security
        let capped_ttl = ttl.min(Duration::from_secs(3600));

        // Get all mintable paths across all domains
        let mut allowed: HashSet<String> = HashSet::new();
        for domain in [
            CatalogDomain::Catalog,
            CatalogDomain::Lineage,
            CatalogDomain::Executions,
            CatalogDomain::Search,
        ] {
            for path in self.get_mintable_paths(domain).await? {
                allowed.insert(path);
            }
        }

        self.mint_signed_urls_with_allowlist(paths, &allowed, capped_ttl)
            .await
    }

    /// Mints signed URLs using a caller-provided allowlist.
    ///
    /// This is useful when the caller already fetched domain paths and wants to
    /// avoid recomputing manifest-derived allowlists.
    ///
    /// # Arguments
    ///
    /// * `paths` - Paths to mint URLs for (must be in the provided allowlist)
    /// * `allowlist` - Manifest-derived allowed paths
    /// * `ttl` - Time-to-live for the signed URLs
    ///
    /// # Errors
    ///
    /// Returns `CatalogError::Validation` if any path is not in the allowlist.
    /// Returns an error if URL signing fails.
    pub async fn mint_signed_urls_with_allowlist(
        &self,
        paths: Vec<String>,
        allowlist: &HashSet<String>,
        ttl: Duration,
    ) -> Result<Vec<SignedUrl>> {
        // Cap TTL at 1 hour for security.
        let capped_ttl = ttl.min(Duration::from_secs(3600));

        // Validate all requested paths are in allowlist
        for path in &paths {
            if !allowlist.contains(path) {
                return Err(CatalogError::Validation {
                    message: format!("path not in manifest allowlist: {}", path),
                });
            }
        }

        // Mint signed URLs
        let expires_at = Utc::now() + chrono::Duration::from_std(capped_ttl).unwrap_or_default();
        let mut urls = Vec::with_capacity(paths.len());

        for path in paths {
            let signed = self.storage.signed_url_raw(&path, capped_ttl).await?;
            urls.push(SignedUrl {
                path,
                url: signed,
                expires_at,
            });
        }

        Ok(urls)
    }

    // ========================================================================
    // Snapshot Info (for advanced use cases)
    // ========================================================================

    /// Gets detailed snapshot info for a domain.
    ///
    /// This is an advanced API for clients that need full snapshot metadata
    /// including per-file checksums and row counts.
    ///
    /// # Arguments
    ///
    /// * `domain` - The catalog domain to get info for
    ///
    /// # Returns
    ///
    /// Returns `None` if no snapshot exists for the domain.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be read.
    pub async fn get_snapshot_info(&self, domain: CatalogDomain) -> Result<Option<SnapshotInfo>> {
        let manifest = self.read_manifest().await?;

        match domain {
            CatalogDomain::Catalog => Ok(manifest.catalog.snapshot),
            CatalogDomain::Lineage => Ok(manifest.lineage.snapshot),
            CatalogDomain::Executions => {
                // Tier-2 snapshots do not yet persist SnapshotInfo metadata.
                Ok(None)
            }
            CatalogDomain::Search => Ok(manifest.search.snapshot),
        }
    }
}

#[cfg(test)]
impl PinnedCatalogReader<'_> {
    /// Lists namespaces from the pinned catalog manifest.
    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>> {
        self.reader
            .list_namespaces_from_catalog_manifest(&self.catalog_manifest)
            .await
    }

    /// Gets a namespace by name from the pinned catalog manifest.
    pub async fn get_namespace(&self, name: &str) -> Result<Option<Namespace>> {
        let namespaces = self.list_namespaces().await?;
        Ok(namespaces
            .into_iter()
            .find(|namespace| namespace.name == name))
    }

    /// Lists tables in a namespace from the pinned catalog manifest.
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<Table>> {
        self.reader
            .list_tables_from_catalog_manifest(&self.catalog_manifest, namespace)
            .await
    }

    /// Gets a table by namespace and name from the pinned catalog manifest.
    pub async fn get_table(&self, namespace: &str, name: &str) -> Result<Option<Table>> {
        let tables = self.list_tables(namespace).await?;
        Ok(tables.into_iter().find(|table| table.name == name))
    }

    /// Gets a table by table ID from the pinned catalog manifest.
    pub async fn get_table_by_id(&self, table_id: &str) -> Result<Option<Table>> {
        self.reader
            .get_table_by_id_from_catalog_manifest(&self.catalog_manifest, table_id)
            .await
    }
}

fn parse_root_read_token(read_token: &str) -> Result<&str> {
    read_token
        .strip_prefix("root:")
        .filter(|tx_id| !tx_id.is_empty())
        .ok_or_else(|| CatalogError::Validation {
            message: format!("invalid root read token '{read_token}'"),
        })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{DomainManifestPointer, SnapshotFile};
    use crate::tier1_compactor::Tier1Compactor;
    use crate::tier1_writer::Tier1Writer;
    use crate::write_options::WriteOptions;
    use crate::writer::{CatalogWriter, ColumnDefinition, RegisterTableRequest};
    use arco_core::control_plane_transactions::{
        ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxStatus,
        DomainCommit, RootTxManifest, RootTxManifestDomain, RootTxReceipt, RootTxRecord,
    };
    use arco_core::storage::{
        MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn setup() -> CatalogReader {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend, "test-tenant", "test-workspace").expect("scoped storage");
        CatalogReader::new(storage)
    }

    #[derive(Debug)]
    struct CountingTablesBackend {
        inner: MemoryBackend,
        table_reads: AtomicUsize,
    }

    impl CountingTablesBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                table_reads: AtomicUsize::new(0),
            }
        }

        fn table_reads(&self) -> usize {
            self.table_reads.load(Ordering::Relaxed)
        }

        fn reset_table_reads(&self) {
            self.table_reads.store(0, Ordering::Relaxed);
        }

        fn maybe_record_table_read(&self, path: &str) {
            if path.ends_with("/tables.parquet") {
                self.table_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[async_trait]
    impl StorageBackend for CountingTablesBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.maybe_record_table_read(path);
            self.inner.get(path).await
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.maybe_record_table_read(path);
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    fn column(name: &str) -> ColumnDefinition {
        ColumnDefinition {
            name: name.to_string(),
            data_type: "STRING".to_string(),
            is_nullable: true,
            description: None,
        }
    }

    #[tokio::test]
    async fn test_reader_new() {
        let reader = setup();
        assert!(format!("{:?}", reader).contains("CatalogReader"));
    }

    #[tokio::test]
    async fn test_list_namespaces_no_manifest() {
        let reader = setup();
        // No manifest exists, should error
        let result = reader.list_namespaces().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_freshness_requires_manifest() {
        let reader = setup();
        let result = reader.get_freshness(CatalogDomain::Catalog).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_mintable_paths_requires_manifest() {
        let reader = setup();
        let result = reader.get_mintable_paths(CatalogDomain::Catalog).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_search_mintable_paths_from_snapshot() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend, "test-tenant", "test-workspace").expect("storage");
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("init");

        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Search);
        let pointer_bytes = storage
            .get_raw(&pointer_path)
            .await
            .expect("search pointer");
        let current_pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).expect("pointer parse");

        let snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Search, 1);
        let mut snapshot = SnapshotInfo::new(1, snapshot_path.clone());
        snapshot.add_file(SnapshotFile {
            path: "token_postings.parquet".to_string(),
            checksum_sha256: "00".repeat(32),
            byte_size: 1,
            row_count: 0,
            position_range: None,
        });

        let search_bytes = storage
            .get_raw(&current_pointer.manifest_path)
            .await
            .expect("search manifest");
        let mut search: SearchManifest = serde_json::from_slice(&search_bytes).expect("parse");
        let manifest_id = crate::manifest::format_manifest_id(1);
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Search, &manifest_id);
        search.manifest_id = manifest_id.clone();
        search.epoch = 1;
        search.previous_manifest_path = Some(current_pointer.manifest_path);
        search.snapshot_version = 1;
        search.base_path = snapshot_path;
        search.snapshot = Some(snapshot);
        search.updated_at = Utc::now();

        let search_payload = serde_json::to_vec(&search).expect("serialize");
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(search_payload),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write search manifest");
        storage
            .put_raw(
                &pointer_path,
                Bytes::from(
                    serde_json::to_vec(&DomainManifestPointer {
                        manifest_id,
                        manifest_path,
                        epoch: 1,
                        parent_pointer_hash: None,
                        updated_at: Utc::now(),
                    })
                    .expect("serialize pointer"),
                ),
                WritePrecondition::None,
            )
            .await
            .expect("write pointer");

        let reader = CatalogReader::new(storage);
        let paths = reader
            .get_mintable_paths(CatalogDomain::Search)
            .await
            .expect("paths");
        assert_eq!(
            paths,
            vec![CatalogPaths::snapshot_file(
                CatalogDomain::Search,
                1,
                "token_postings.parquet"
            )]
        );
    }

    #[tokio::test]
    async fn test_catalog_mintable_paths_include_catalogs_parquet() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend, "test-tenant", "test-workspace").expect("storage");
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("init");

        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_bytes = storage
            .get_raw(&pointer_path)
            .await
            .expect("catalog pointer");
        let current_pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).expect("pointer parse");

        let catalog_bytes = storage
            .get_raw(&current_pointer.manifest_path)
            .await
            .expect("catalog manifest");
        let mut catalog: CatalogDomainManifest =
            serde_json::from_slice(&catalog_bytes).expect("catalog parse");
        let manifest_id = crate::manifest::format_manifest_id(1);
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &manifest_id);
        catalog.manifest_id = manifest_id.clone();
        catalog.epoch = 1;
        catalog.previous_manifest_path = Some(current_pointer.manifest_path);
        catalog.snapshot_version = 1;
        catalog.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        catalog.snapshot = None;
        catalog.updated_at = Utc::now();

        let catalog_payload = serde_json::to_vec(&catalog).expect("serialize");
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(catalog_payload),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write catalog manifest");
        storage
            .put_raw(
                &pointer_path,
                Bytes::from(
                    serde_json::to_vec(&DomainManifestPointer {
                        manifest_id,
                        manifest_path,
                        epoch: 1,
                        parent_pointer_hash: None,
                        updated_at: Utc::now(),
                    })
                    .expect("serialize pointer"),
                ),
                WritePrecondition::None,
            )
            .await
            .expect("write pointer");

        let reader = CatalogReader::new(storage);
        let paths = reader
            .get_mintable_paths(CatalogDomain::Catalog)
            .await
            .expect("paths");
        assert_eq!(
            paths,
            vec![
                CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "catalogs.parquet"),
                CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "namespaces.parquet"),
                CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "tables.parquet"),
                CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "columns.parquet"),
            ]
        );
    }

    #[tokio::test]
    async fn test_mint_signed_urls_validates_paths() {
        let reader = setup();
        // Try to mint URL for path not in allowlist
        let result = reader
            .mint_signed_urls(
                vec!["ledger/executions/event.json".to_string()],
                Duration::from_secs(300),
            )
            .await;
        // Should error because no manifest exists
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mint_signed_urls_with_allowlist_avoids_manifest_recompute() {
        let reader = setup();
        let path = "state/catalog/snapshots/v1/tables.parquet".to_string();
        let mut allowlist = HashSet::new();
        allowlist.insert(path.clone());

        let urls = reader
            .mint_signed_urls_with_allowlist(
                vec![path.clone()],
                &allowlist,
                Duration::from_secs(300),
            )
            .await
            .expect("mint with precomputed allowlist");

        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].path, path);
        assert!(urls[0].url.contains("signature=mock"));
    }

    #[tokio::test]
    async fn test_lineage_graph_default() {
        let graph = LineageGraph::default();
        assert!(graph.upstream.is_empty());
        assert!(graph.downstream.is_empty());
    }

    #[tokio::test]
    async fn test_get_table_by_id_reads_tables_snapshot_once_per_version() {
        let backend = Arc::new(CountingTablesBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "test-tenant", "test-workspace").expect("storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
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
                    location: None,
                    format: None,
                    columns: vec![column("event_id")],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");

        let reader = CatalogReader::new(storage.clone());
        backend.reset_table_reads();

        let first = reader
            .get_table_by_id(&table.id)
            .await
            .expect("first lookup")
            .expect("table exists");
        let second = reader
            .get_table_by_id(&table.id)
            .await
            .expect("second lookup")
            .expect("table exists");

        assert_eq!(first.id, table.id);
        assert_eq!(second.id, table.id);
        assert_eq!(
            backend.table_reads(),
            1,
            "repeated lookup should not reload tables.parquet for the same snapshot"
        );
    }

    #[tokio::test]
    async fn test_get_table_by_id_cache_refreshes_on_new_snapshot() {
        let backend = Arc::new(CountingTablesBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "test-tenant", "test-workspace").expect("storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("default", None, WriteOptions::default())
            .await
            .expect("create namespace");
        let first = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "events".to_string(),
                    description: None,
                    location: None,
                    format: None,
                    columns: vec![column("event_id")],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register first table");

        let reader = CatalogReader::new(storage.clone());
        let resolved_first = reader
            .get_table_by_id(&first.id)
            .await
            .expect("first lookup")
            .expect("first exists");
        assert_eq!(resolved_first.id, first.id);

        let second = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "default".to_string(),
                    name: "sessions".to_string(),
                    description: None,
                    location: None,
                    format: None,
                    columns: vec![column("session_id")],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register second table");

        let resolved_second = reader
            .get_table_by_id(&second.id)
            .await
            .expect("second lookup")
            .expect("second exists");
        assert_eq!(resolved_second.id, second.id);
    }

    #[tokio::test]
    async fn test_list_namespaces_for_root_token_uses_pinned_super_manifest_catalog_head() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend, "test-tenant", "test-workspace").expect("storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("bronze", None, WriteOptions::default())
            .await
            .expect("create bronze namespace");

        let first_pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await
            .expect("first pointer");
        let first_pointer: DomainManifestPointer =
            serde_json::from_slice(&first_pointer_bytes).expect("parse first pointer");
        let first_manifest_bytes = storage
            .get_raw(&first_pointer.manifest_path)
            .await
            .expect("first immutable catalog manifest");
        let first_manifest: CatalogDomainManifest =
            serde_json::from_slice(&first_manifest_bytes).expect("parse first manifest");

        writer
            .create_namespace("silver", None, WriteOptions::default())
            .await
            .expect("create silver namespace");

        let reader = CatalogReader::new(storage.clone());
        let mut current_namespaces = reader
            .list_namespaces()
            .await
            .expect("list current namespaces");
        current_namespaces.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(
            current_namespaces
                .iter()
                .map(|namespace| namespace.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bronze", "silver"]
        );

        let tx_id = "01JROOTREADTOKEN000000000001";
        let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(tx_id);
        let commit_id = first_manifest
            .last_commit_id
            .clone()
            .expect("catalog manifest commit id");
        let root_manifest = RootTxManifest {
            tx_id: tx_id.to_string(),
            fencing_token: 1,
            published_at: Utc::now(),
            domains: BTreeMap::from([(
                ControlPlaneTxDomain::Catalog,
                RootTxManifestDomain {
                    manifest_id: first_pointer.manifest_id.clone(),
                    manifest_path: first_pointer.manifest_path.clone(),
                    commit_id: commit_id.clone(),
                },
            )]),
        };
        storage
            .put_raw(
                &super_manifest_path,
                Bytes::from(serde_json::to_vec(&root_manifest).expect("serialize root manifest")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write root super-manifest");

        let visible_at = Utc::now();
        let root_record = RootTxRecord {
            tx_id: tx_id.to_string(),
            kind: ControlPlaneTxKind::RootCommit,
            status: ControlPlaneTxStatus::Visible,
            repair_pending: false,
            request_id: "req-root-reader-01".to_string(),
            idempotency_key: "idem-root-reader-01".to_string(),
            request_hash: "sha256:root-reader".to_string(),
            lock_path: ControlPlaneTxPaths::root_lock(),
            fencing_token: 1,
            prepared_at: visible_at,
            visible_at: Some(visible_at),
            result: Some(RootTxReceipt {
                tx_id: tx_id.to_string(),
                root_commit_id: "01JROOTREADCOMMIT0000000001".to_string(),
                super_manifest_path: super_manifest_path.clone(),
                domain_commits: vec![DomainCommit {
                    domain: ControlPlaneTxDomain::Catalog,
                    tx_id: "01JCATROOTPARTICIPANT0000001".to_string(),
                    commit_id,
                    manifest_id: first_pointer.manifest_id.clone(),
                    manifest_path: first_pointer.manifest_path.clone(),
                    read_token: format!("catalog:{}", first_pointer.manifest_id),
                }],
                read_token: format!("root:{tx_id}"),
                visible_at,
            }),
        };
        storage
            .put_raw(
                &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id),
                Bytes::from(serde_json::to_vec(&root_record).expect("serialize root tx record")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write root tx record");

        let pinned = reader
            .list_namespaces_for_root_token(&format!("root:{tx_id}"))
            .await
            .expect("list pinned namespaces");
        assert_eq!(
            pinned
                .iter()
                .map(|namespace| namespace.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bronze"]
        );
    }

    #[tokio::test]
    async fn test_pin_root_token_returns_pinned_catalog_view_without_following_current_pointer() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend, "test-tenant", "test-workspace").expect("storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("initialize");

        writer
            .create_namespace("bronze", None, WriteOptions::default())
            .await
            .expect("create bronze namespace");
        let bronze_events = writer
            .register_table(
                RegisterTableRequest {
                    namespace: "bronze".to_string(),
                    name: "events".to_string(),
                    description: None,
                    location: None,
                    format: None,
                    columns: vec![column("event_id")],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register bronze.events");

        let first_pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await
            .expect("first pointer");
        let first_pointer: DomainManifestPointer =
            serde_json::from_slice(&first_pointer_bytes).expect("parse first pointer");
        let first_manifest_bytes = storage
            .get_raw(&first_pointer.manifest_path)
            .await
            .expect("first immutable catalog manifest");
        let first_manifest: CatalogDomainManifest =
            serde_json::from_slice(&first_manifest_bytes).expect("parse first manifest");

        writer
            .create_namespace("silver", None, WriteOptions::default())
            .await
            .expect("create silver namespace");
        writer
            .register_table(
                RegisterTableRequest {
                    namespace: "bronze".to_string(),
                    name: "sessions".to_string(),
                    description: None,
                    location: None,
                    format: None,
                    columns: vec![column("session_id")],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register bronze.sessions");

        let reader = CatalogReader::new(storage.clone());
        let mut current_namespaces = reader
            .list_namespaces()
            .await
            .expect("list current namespaces");
        current_namespaces.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(
            current_namespaces
                .iter()
                .map(|namespace| namespace.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bronze", "silver"]
        );
        let mut current_tables = reader
            .list_tables("bronze")
            .await
            .expect("list current bronze tables");
        current_tables.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(
            current_tables
                .iter()
                .map(|table| table.name.as_str())
                .collect::<Vec<_>>(),
            vec!["events", "sessions"]
        );

        let tx_id = "01JROOTPINNEDVIEW0000000001";
        let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(tx_id);
        let commit_id = first_manifest
            .last_commit_id
            .clone()
            .expect("catalog manifest commit id");
        let root_manifest = RootTxManifest {
            tx_id: tx_id.to_string(),
            fencing_token: 1,
            published_at: Utc::now(),
            domains: BTreeMap::from([(
                ControlPlaneTxDomain::Catalog,
                RootTxManifestDomain {
                    manifest_id: first_pointer.manifest_id.clone(),
                    manifest_path: first_pointer.manifest_path.clone(),
                    commit_id: commit_id.clone(),
                },
            )]),
        };
        storage
            .put_raw(
                &super_manifest_path,
                Bytes::from(serde_json::to_vec(&root_manifest).expect("serialize root manifest")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write root super-manifest");

        let visible_at = Utc::now();
        let root_record = RootTxRecord {
            tx_id: tx_id.to_string(),
            kind: ControlPlaneTxKind::RootCommit,
            status: ControlPlaneTxStatus::Visible,
            repair_pending: false,
            request_id: "req-root-reader-02".to_string(),
            idempotency_key: "idem-root-reader-02".to_string(),
            request_hash: "sha256:root-reader-pinned-view".to_string(),
            lock_path: ControlPlaneTxPaths::root_lock(),
            fencing_token: 1,
            prepared_at: visible_at,
            visible_at: Some(visible_at),
            result: Some(RootTxReceipt {
                tx_id: tx_id.to_string(),
                root_commit_id: "01JROOTPINNEDVIEWCOMMIT0001".to_string(),
                super_manifest_path: super_manifest_path.clone(),
                domain_commits: vec![DomainCommit {
                    domain: ControlPlaneTxDomain::Catalog,
                    tx_id: "01JCATROOTPARTICIPANT0000002".to_string(),
                    commit_id,
                    manifest_id: first_pointer.manifest_id.clone(),
                    manifest_path: first_pointer.manifest_path.clone(),
                    read_token: format!("catalog:{}", first_pointer.manifest_id),
                }],
                read_token: format!("root:{tx_id}"),
                visible_at,
            }),
        };
        storage
            .put_raw(
                &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id),
                Bytes::from(serde_json::to_vec(&root_record).expect("serialize root tx record")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write root tx record");

        let pinned = reader
            .pin_root_token(&format!("root:{tx_id}"))
            .await
            .expect("pin root token");
        let mut pinned_namespaces = pinned
            .list_namespaces()
            .await
            .expect("list pinned namespaces");
        pinned_namespaces.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(
            pinned_namespaces
                .iter()
                .map(|namespace| namespace.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bronze"]
        );
        assert!(
            pinned
                .get_namespace("silver")
                .await
                .expect("lookup pinned silver namespace")
                .is_none()
        );
        let bronze_namespace = pinned
            .get_namespace("bronze")
            .await
            .expect("lookup pinned bronze namespace")
            .expect("bronze namespace exists");
        assert_eq!(bronze_namespace.name, "bronze");

        let mut pinned_tables = pinned
            .list_tables("bronze")
            .await
            .expect("list pinned bronze tables");
        pinned_tables.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(
            pinned_tables
                .iter()
                .map(|table| table.name.as_str())
                .collect::<Vec<_>>(),
            vec!["events"]
        );
        assert!(
            pinned
                .get_table("bronze", "sessions")
                .await
                .expect("lookup pinned bronze.sessions")
                .is_none()
        );
        let pinned_events = pinned
            .get_table("bronze", "events")
            .await
            .expect("lookup pinned bronze.events")
            .expect("bronze.events exists");
        assert_eq!(pinned_events.id, bronze_events.id);
        let pinned_by_id = pinned
            .get_table_by_id(&bronze_events.id)
            .await
            .expect("lookup pinned table by id")
            .expect("bronze.events exists by id");
        assert_eq!(pinned_by_id.name, "events");
    }
}
