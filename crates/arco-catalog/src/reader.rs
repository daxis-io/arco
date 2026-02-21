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

        // Read domain manifests in parallel, preferring pointer-resolved immutable snapshots.
        let (catalog_bytes, lineage_bytes, executions_bytes, search_bytes) = tokio::join!(
            self.read_domain_manifest_bytes(CatalogDomain::Catalog, &root.catalog_manifest_path),
            self.read_domain_manifest_bytes(CatalogDomain::Lineage, &root.lineage_manifest_path),
            self.read_domain_manifest_bytes(
                CatalogDomain::Executions,
                &root.executions_manifest_path
            ),
            self.read_domain_manifest_bytes(CatalogDomain::Search, &root.search_manifest_path),
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
        legacy_path: &str,
    ) -> Result<bytes::Bytes> {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        match self.storage.get_raw(&pointer_path).await {
            Ok(pointer_bytes) => {
                let pointer: DomainManifestPointer = serde_json::from_slice(&pointer_bytes)
                    .map_err(|e| CatalogError::Serialization {
                        message: format!("failed to parse manifest pointer at {pointer_path}: {e}"),
                    })?;

                match self.storage.get_raw(&pointer.manifest_path).await {
                    Ok(bytes) => Ok(bytes),
                    Err(
                        arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. },
                    ) => {
                        // Migration fallback: pointer exists but immutable snapshot is unavailable.
                        self.storage.get_raw(legacy_path).await.map_err(Into::into)
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                self.storage.get_raw(legacy_path).await.map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
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

        if manifest.catalog.snapshot_version == 0 {
            return Ok(Vec::new());
        }

        let catalogs_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
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

        if manifest.catalog.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "catalog".into(),
                name: catalog.to_string(),
            });
        }

        let catalogs_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "catalogs.parquet",
        );
        let catalogs = match self.storage.get_raw(&catalogs_path).await {
            Ok(bytes) => parquet_util::read_catalogs(&bytes)?,
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Vec::new()
            }
            Err(e) => return Err(e.into()),
        };

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

        let ns_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "namespaces.parquet",
        );
        let bytes = self.storage.get_raw(&ns_path).await?;
        let records = parquet_util::read_namespaces(&bytes)?;

        Ok(records
            .into_iter()
            .filter(|ns| match effective_requested_catalog_id {
                Some(requested) => {
                    ns.catalog_id.as_deref().or(default_catalog_id) == Some(requested)
                }
                None => ns.catalog_id.is_none(),
            })
            .map(Namespace::from)
            .collect())
    }

    /// Lists tables within a schema (namespace) inside the given catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog or schema doesn't exist, or snapshot reads fail.
    pub async fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Result<Vec<Table>> {
        let manifest = self.read_manifest().await?;

        if manifest.catalog.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            });
        }

        let schemas = self.list_schemas(catalog).await?;
        let namespace_id = schemas
            .iter()
            .find(|ns| ns.name == schema)
            .map(|ns| ns.id.clone())
            .ok_or_else(|| CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            })?;

        let tables_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "tables.parquet",
        );
        let bytes = self.storage.get_raw(&tables_path).await?;
        let records = parquet_util::read_tables(&bytes)?;

        Ok(records
            .into_iter()
            .filter(|t| t.namespace_id == namespace_id)
            .map(Table::from)
            .collect())
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

        if manifest.catalog.snapshot_version == 0 {
            return Ok(Vec::new());
        }

        let ns_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "namespaces.parquet",
        );

        let bytes = self.storage.get_raw(&ns_path).await?;
        let records = parquet_util::read_namespaces(&bytes)?;

        Ok(records.into_iter().map(Namespace::from).collect())
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
        let namespaces = self.list_namespaces().await?;
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

        if manifest.catalog.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            });
        }

        // First, find the namespace to get its ID
        let ns_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "namespaces.parquet",
        );
        let ns_bytes = self.storage.get_raw(&ns_path).await?;
        let ns_records = parquet_util::read_namespaces(&ns_bytes)?;

        let namespace_id = ns_records
            .iter()
            .find(|ns| ns.name == namespace)
            .map(|ns| ns.id.clone())
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            })?;

        // Read tables and filter by namespace_id
        let tables_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "tables.parquet",
        );
        let tables_bytes = self.storage.get_raw(&tables_path).await?;
        let table_records = parquet_util::read_tables(&tables_bytes)?;

        Ok(table_records
            .into_iter()
            .filter(|t| t.namespace_id == namespace_id)
            .map(Table::from)
            .collect())
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
        let tables = self.list_tables(namespace).await?;
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

        if manifest.catalog.snapshot_version == 0 {
            return Ok(None);
        }

        let by_id = self
            .table_lookup_by_id(manifest.catalog.snapshot_version)
            .await?;

        Ok(by_id.get(table_id).cloned())
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

        if manifest.catalog.snapshot_version == 0 {
            return Ok(Vec::new());
        }

        let columns_path = CatalogPaths::snapshot_file(
            CatalogDomain::Catalog,
            manifest.catalog.snapshot_version,
            "columns.parquet",
        );
        let bytes = self.storage.get_raw(&columns_path).await?;
        let records = parquet_util::read_columns(&bytes)?;

        Ok(records
            .into_iter()
            .filter(|c| c.table_id == table_id)
            .map(Column::from)
            .collect())
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

        // Validate all requested paths are in allowlist
        for path in &paths {
            if !allowed.contains(path) {
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::SnapshotFile;
    use crate::tier1_compactor::Tier1Compactor;
    use crate::tier1_writer::Tier1Writer;
    use crate::write_options::WriteOptions;
    use crate::writer::{CatalogWriter, ColumnDefinition, RegisterTableRequest};
    use arco_core::storage::{
        MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
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

        let root_bytes = storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .expect("root manifest");
        let mut root: RootManifest = serde_json::from_slice(&root_bytes).expect("root parse");
        root.normalize_paths();

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
            .get_raw(&root.search_manifest_path)
            .await
            .expect("search manifest");
        let mut search: SearchManifest = serde_json::from_slice(&search_bytes).expect("parse");
        search.snapshot_version = 1;
        search.base_path = snapshot_path;
        search.snapshot = Some(snapshot);
        search.updated_at = Utc::now();

        let search_payload = serde_json::to_vec(&search).expect("serialize");
        storage
            .put_raw(
                &root.search_manifest_path,
                Bytes::from(search_payload),
                WritePrecondition::None,
            )
            .await
            .expect("write search manifest");

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

        let root_bytes = storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .expect("root manifest");
        let mut root: RootManifest = serde_json::from_slice(&root_bytes).expect("root parse");
        root.normalize_paths();

        let catalog_bytes = storage
            .get_raw(&root.catalog_manifest_path)
            .await
            .expect("catalog manifest");
        let mut catalog: CatalogDomainManifest =
            serde_json::from_slice(&catalog_bytes).expect("catalog parse");
        catalog.snapshot_version = 1;
        catalog.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        catalog.snapshot = None;
        catalog.updated_at = Utc::now();

        let catalog_payload = serde_json::to_vec(&catalog).expect("serialize");
        storage
            .put_raw(
                &root.catalog_manifest_path,
                Bytes::from(catalog_payload),
                WritePrecondition::None,
            )
            .await
            .expect("write catalog manifest");

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
}
