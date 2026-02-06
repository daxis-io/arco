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

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};

use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

use crate::error::{CatalogError, Result};
use crate::manifest::{
    CatalogDomainManifest, CatalogManifest, ExecutionsManifest, LineageManifest, RootManifest,
    SearchManifest, SnapshotInfo, TailRange,
};
use crate::parquet_util;
use crate::write_options::SnapshotVersion;
use crate::writer::{Column, LineageEdge, Namespace, Table};

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
        Self { storage }
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

        // Read domain manifests in parallel
        let (catalog_bytes, lineage_bytes, executions_bytes, search_bytes) = tokio::join!(
            self.storage.get_raw(&root.catalog_manifest_path),
            self.storage.get_raw(&root.lineage_manifest_path),
            self.storage.get_raw(&root.executions_manifest_path),
            self.storage.get_raw(&root.search_manifest_path),
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
    use crate::tier1_writer::Tier1Writer;
    use arco_core::storage::{MemoryBackend, WritePrecondition};
    use bytes::Bytes;
    use std::sync::Arc;

    fn setup() -> CatalogReader {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend, "test-tenant", "test-workspace").expect("scoped storage");
        CatalogReader::new(storage)
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
}
