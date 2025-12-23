//! Tier-1 snapshot writers for catalog and lineage domains.

use bytes::Bytes;
use sha2::{Digest, Sha256};

use arco_core::storage::{WriteResult};
use arco_core::storage_keys::StateKey;
use arco_core::storage_traits::StatePutStore;
use arco_core::CatalogDomain;

use crate::error::{CatalogError, Result};
use crate::manifest::{SnapshotFile, SnapshotInfo};
use crate::parquet_util;
use crate::state::{CatalogState, LineageState};

/// Writes a catalog snapshot (namespaces/tables/columns) to storage.
///
/// # Errors
///
/// Returns an error if Parquet serialization or storage writes fail.
pub async fn write_catalog_snapshot<S: StatePutStore + ?Sized>(
    storage: &S,
    version: u64,
    state: &CatalogState,
) -> Result<SnapshotInfo> {
    let snapshot_dir = StateKey::snapshot_dir(CatalogDomain::Catalog, version);

    let namespaces_bytes = parquet_util::write_namespaces(&state.namespaces)?;
    let tables_bytes = parquet_util::write_tables(&state.tables)?;
    let columns_bytes = parquet_util::write_columns(&state.columns)?;

    let ns_key = StateKey::snapshot_file(CatalogDomain::Catalog, version, "namespaces.parquet");
    let tables_key = StateKey::snapshot_file(CatalogDomain::Catalog, version, "tables.parquet");
    let cols_key = StateKey::snapshot_file(CatalogDomain::Catalog, version, "columns.parquet");

    put_state_if_absent(storage, &ns_key, namespaces_bytes.clone()).await?;
    put_state_if_absent(storage, &tables_key, tables_bytes.clone()).await?;
    put_state_if_absent(storage, &cols_key, columns_bytes.clone()).await?;

    let mut info = SnapshotInfo::new(version, snapshot_dir.as_ref().to_string());
    info.add_file(SnapshotFile {
        path: "namespaces.parquet".to_string(),
        checksum_sha256: sha256_hex(&namespaces_bytes),
        byte_size: namespaces_bytes.len() as u64,
        row_count: state.namespaces.len() as u64,
        position_range: None,
    });
    info.add_file(SnapshotFile {
        path: "tables.parquet".to_string(),
        checksum_sha256: sha256_hex(&tables_bytes),
        byte_size: tables_bytes.len() as u64,
        row_count: state.tables.len() as u64,
        position_range: None,
    });
    info.add_file(SnapshotFile {
        path: "columns.parquet".to_string(),
        checksum_sha256: sha256_hex(&columns_bytes),
        byte_size: columns_bytes.len() as u64,
        row_count: state.columns.len() as u64,
        position_range: None,
    });

    Ok(info)
}

/// Writes a lineage snapshot (edges) to storage.
///
/// # Errors
///
/// Returns an error if Parquet serialization or storage writes fail.
pub async fn write_lineage_snapshot<S: StatePutStore + ?Sized>(
    storage: &S,
    version: u64,
    state: &LineageState,
) -> Result<SnapshotInfo> {
    let snapshot_dir = StateKey::snapshot_dir(CatalogDomain::Lineage, version);
    let bytes = parquet_util::write_lineage_edges(&state.edges)?;
    let key = StateKey::snapshot_file(CatalogDomain::Lineage, version, "lineage_edges.parquet");

    put_state_if_absent(storage, &key, bytes.clone()).await?;

    let mut info = SnapshotInfo::new(version, snapshot_dir.as_ref().to_string());
    info.add_file(SnapshotFile {
        path: "lineage_edges.parquet".to_string(),
        checksum_sha256: sha256_hex(&bytes),
        byte_size: bytes.len() as u64,
        row_count: state.edges.len() as u64,
        position_range: None,
    });

    Ok(info)
}

async fn put_state_if_absent<S: StatePutStore + ?Sized>(
    storage: &S,
    key: &StateKey,
    data: Bytes,
) -> Result<()> {
    match storage.put_state(key, data).await.map_err(CatalogError::from)? {
        WriteResult::Success { .. } | WriteResult::PreconditionFailed { .. } => Ok(()),
    }
}

fn sha256_hex(bytes: &Bytes) -> String {
    let hash = Sha256::digest(bytes);
    hex::encode(hash)
}
