//! Helpers for loading Tier-1 snapshot state.

use arco_core::ScopedStorage;

use crate::error::Result;
use crate::parquet_util;
use crate::state::{CatalogState, LineageState};

/// Native metastore state loaded by future pointer-published projections.
pub type MetastoreState = crate::metastore::replay::MetastoreState;

/// Loads catalog state from the current snapshot path.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails.
pub async fn load_catalog_state(
    storage: &ScopedStorage,
    snapshot_path: &str,
) -> Result<CatalogState> {
    if is_empty_snapshot(snapshot_path) {
        return Ok(CatalogState::empty());
    }

    let ns_path = snapshot_file_path(snapshot_path, "namespaces.parquet");
    let tables_path = snapshot_file_path(snapshot_path, "tables.parquet");
    let columns_path = snapshot_file_path(snapshot_path, "columns.parquet");
    let catalogs_path = snapshot_file_path(snapshot_path, "catalogs.parquet");
    let commits_path = snapshot_file_path(snapshot_path, "commits.parquet");

    let catalogs = match storage.get_raw(&catalogs_path).await {
        Ok(bytes) => parquet_util::read_catalogs(&bytes)?,
        Err(_) => Vec::new(),
    };

    let namespaces = match storage.get_raw(&ns_path).await {
        Ok(bytes) => parquet_util::read_namespaces(&bytes)?,
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Vec::new()
        }
        Err(e) => return Err(e.into()),
    };

    let tables = match storage.get_raw(&tables_path).await {
        Ok(bytes) => parquet_util::read_tables(&bytes)?,
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Vec::new()
        }
        Err(e) => return Err(e.into()),
    };

    let columns = match storage.get_raw(&columns_path).await {
        Ok(bytes) => parquet_util::read_columns(&bytes)?,
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Vec::new()
        }
        Err(e) => return Err(e.into()),
    };

    let commits = match storage.get_raw(&commits_path).await {
        Ok(bytes) => parquet_util::read_commits(&bytes)?,
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Vec::new()
        }
        Err(e) => return Err(e.into()),
    };

    Ok(CatalogState {
        catalogs,
        namespaces,
        tables,
        columns,
        commits,
    })
}

/// Loads lineage state from the current snapshot path.
///
/// # Errors
///
/// Returns an error if Parquet decoding fails.
pub async fn load_lineage_state(storage: &ScopedStorage, edges_path: &str) -> Result<LineageState> {
    if is_empty_snapshot(edges_path) {
        return Ok(LineageState::empty());
    }

    let path = snapshot_file_path(edges_path, "lineage_edges.parquet");
    let edges = match storage.get_raw(&path).await {
        Ok(bytes) => parquet_util::read_lineage_edges(&bytes)?,
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Vec::new()
        }
        Err(e) => return Err(e.into()),
    };

    Ok(LineageState { edges })
}

fn is_empty_snapshot(snapshot_path: &str) -> bool {
    snapshot_path.is_empty() || snapshot_path.split('/').any(|segment| segment == "v0")
}

fn snapshot_file_path(snapshot_path: &str, filename: &str) -> String {
    format!("{}/{filename}", snapshot_path.trim_end_matches('/'))
}
