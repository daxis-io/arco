//! Registration helpers for tenant-visible logical `system.*` tables.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::{CatalogProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arco_catalog::CatalogReader;
use arco_core::{CatalogDomain, ScopedStorage};

use crate::error::ApiError;

/// Explicit mapping from a manifest-selected artifact to a logical `system.*` table.
pub(crate) struct SystemTableSpec {
    /// System schema name beneath the logical `system` catalog.
    pub schema: &'static str,
    /// Table name within the logical `system.{schema}` namespace.
    pub table: &'static str,
    /// Snapshot artifact file name to allowlist.
    pub path: &'static str,
}

const CATALOG_SYSTEM_TABLES: &[SystemTableSpec] = &[
    SystemTableSpec {
        schema: "catalog",
        table: "catalogs",
        path: "catalogs.parquet",
    },
    SystemTableSpec {
        schema: "catalog",
        table: "namespaces",
        path: "namespaces.parquet",
    },
    SystemTableSpec {
        schema: "catalog",
        table: "tables",
        path: "tables.parquet",
    },
    SystemTableSpec {
        schema: "catalog",
        table: "columns",
        path: "columns.parquet",
    },
];

const LINEAGE_SYSTEM_TABLES: &[SystemTableSpec] = &[SystemTableSpec {
    schema: "lineage",
    table: "edges",
    path: "lineage_edges.parquet",
}];

/// Registers allowlisted catalog and lineage projections under logical `system.*` names.
///
/// This keeps the system-table surface explicit and manifest-driven without
/// auto-exposing every file in a snapshot.
pub(crate) async fn register_catalog_and_lineage_system_tables(
    session: &SessionContext,
    reader: &CatalogReader,
    storage: &ScopedStorage,
) -> Result<usize, ApiError> {
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    session.register_catalog("system", catalog_provider.clone());

    let schema_providers = register_system_schemas(
        &catalog_provider,
        CATALOG_SYSTEM_TABLES
            .iter()
            .chain(LINEAGE_SYSTEM_TABLES.iter())
            .map(|spec| spec.schema),
    )?;

    let mut registered = 0;
    registered += register_domain_specs(
        reader,
        storage,
        CatalogDomain::Catalog,
        CATALOG_SYSTEM_TABLES,
        &schema_providers,
    )
    .await?;
    registered += register_domain_specs(
        reader,
        storage,
        CatalogDomain::Lineage,
        LINEAGE_SYSTEM_TABLES,
        &schema_providers,
    )
    .await?;
    Ok(registered)
}

fn register_system_schemas<'a>(
    catalog_provider: &Arc<MemoryCatalogProvider>,
    schemas: impl Iterator<Item = &'a str>,
) -> Result<HashMap<&'a str, Arc<MemorySchemaProvider>>, ApiError> {
    let mut providers = HashMap::new();

    for schema in schemas {
        if providers.contains_key(schema) {
            continue;
        }

        let provider = Arc::new(MemorySchemaProvider::new());
        catalog_provider
            .register_schema(schema, provider.clone())
            .map_err(|err| ApiError::internal(format!("failed to register system schema: {err}")))?;
        providers.insert(schema, provider);
    }

    Ok(providers)
}

async fn register_domain_specs(
    reader: &CatalogReader,
    storage: &ScopedStorage,
    domain: CatalogDomain,
    specs: &[SystemTableSpec],
    schema_providers: &HashMap<&str, Arc<MemorySchemaProvider>>,
) -> Result<usize, ApiError> {
    let paths = reader.get_mintable_paths(domain).await.map_err(ApiError::from)?;
    if paths.is_empty() {
        return Ok(0);
    }

    let mut registered = 0;
    for path in paths {
        let Some(file_name) = path.rsplit('/').next() else {
            continue;
        };
        let Some(spec) = specs.iter().find(|spec| spec.path == file_name) else {
            continue;
        };
        let Some(schema_provider) = schema_providers.get(spec.schema) else {
            return Err(ApiError::internal(format!(
                "missing system schema provider for '{}'",
                spec.schema
            )));
        };

        let bytes = storage.get_raw(&path).await.map_err(ApiError::from)?;
        let table = parquet_bytes_to_mem_table(bytes)?;
        schema_provider
            .register_table(spec.table.to_string(), table)
            .map_err(|err| ApiError::internal(format!("failed to register system table: {err}")))?;
        registered += 1;
    }

    Ok(registered)
}

fn parquet_bytes_to_mem_table(bytes: Bytes) -> Result<Arc<MemTable>, ApiError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| ApiError::internal(format!("failed to read parquet bytes: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| ApiError::internal(format!("failed to build parquet reader: {e}")))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch
            .map_err(|e| ApiError::internal(format!("failed to decode parquet batch: {e}")))?;
        batches.push(batch);
    }

    let table = MemTable::try_new(schema, vec![batches])
        .map_err(|e| ApiError::internal(format!("failed to register table: {e}")))?;
    Ok(Arc::new(table))
}
