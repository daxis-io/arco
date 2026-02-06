//! Helpers for loading Tier-1 snapshot state.

use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

use crate::error::Result;
use crate::parquet_util;
use crate::state::{CatalogState, LineageState};

/// Loads catalog state from the current snapshot path.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails.
pub async fn load_catalog_state(
    storage: &ScopedStorage,
    snapshot_path: &str,
) -> Result<CatalogState> {
    if snapshot_path.is_empty() || snapshot_path.contains("/v0/") {
        return Ok(CatalogState::empty());
    }

    let version = snapshot_path
        .split("/v")
        .last()
        .and_then(|s| s.trim_end_matches('/').parse::<u64>().ok())
        .unwrap_or(0);

    if version == 0 {
        return Ok(CatalogState::empty());
    }

    let ns_path =
        CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "namespaces.parquet");
    let tables_path =
        CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "tables.parquet");
    let columns_path =
        CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "columns.parquet");
    let catalogs_path =
        CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "catalogs.parquet");

    let catalogs = match storage.get_raw(&catalogs_path).await {
        Ok(bytes) => parquet_util::read_catalogs(&bytes)?,
        Err(_) => Vec::new(),
    };

    let namespaces = match storage.get_raw(&ns_path).await {
        Ok(bytes) => parquet_util::read_namespaces(&bytes)?,
        Err(_) => Vec::new(),
    };

    let tables = match storage.get_raw(&tables_path).await {
        Ok(bytes) => parquet_util::read_tables(&bytes)?,
        Err(_) => Vec::new(),
    };

    let columns = match storage.get_raw(&columns_path).await {
        Ok(bytes) => parquet_util::read_columns(&bytes)?,
        Err(_) => Vec::new(),
    };

    Ok(CatalogState {
        catalogs,
        namespaces,
        tables,
        columns,
    })
}

/// Loads lineage state from the current snapshot path.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails.
pub async fn load_lineage_state(storage: &ScopedStorage, edges_path: &str) -> Result<LineageState> {
    if edges_path.is_empty() || edges_path.contains("/v0/") {
        return Ok(LineageState::empty());
    }

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
    let edges = match storage.get_raw(&path).await {
        Ok(bytes) => parquet_util::read_lineage_edges(&bytes)?,
        Err(_) => Vec::new(),
    };

    Ok(LineageState { edges })
}
