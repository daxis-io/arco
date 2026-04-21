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

use arco_catalog::{CatalogError, CatalogReader};
use arco_core::{CatalogDomain, ScopedStorage};
use arco_flow::orchestration::compactor::{MicroCompactor, TableArtifact};

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

const ORCHESTRATION_SCHEMA: &str = "orchestration";

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
            .map(|spec| spec.schema)
            .chain(std::iter::once(ORCHESTRATION_SCHEMA)),
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
    registered += register_orchestration_tables(storage, &schema_providers).await?;
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
            .map_err(|err| {
                ApiError::internal(format!("failed to register system schema: {err}"))
            })?;
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
    let paths = match reader.get_mintable_paths(domain).await {
        Ok(paths) => paths,
        Err(CatalogError::NotFound { .. }) => return Ok(0),
        Err(err) => return Err(ApiError::from(err)),
    };
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

async fn register_orchestration_tables(
    storage: &ScopedStorage,
    schema_providers: &HashMap<&str, Arc<MemorySchemaProvider>>,
) -> Result<usize, ApiError> {
    let Some(schema_provider) = schema_providers.get(ORCHESTRATION_SCHEMA) else {
        return Err(ApiError::internal(
            "missing system schema provider for 'orchestration'",
        ));
    };

    let table_paths = MicroCompactor::new(storage.clone())
        .current_base_table_paths()
        .await
        .map_err(|err| {
            ApiError::internal(format!(
                "failed to load orchestration base snapshot paths: {err}"
            ))
        })?;

    let mut registered = 0;
    registered +=
        register_table_artifact(schema_provider, storage, "runs", table_paths.runs.as_ref())
            .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "tasks",
        table_paths.tasks.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "dep_satisfaction",
        table_paths.dep_satisfaction.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "timers",
        table_paths.timers.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "dispatch_outbox",
        table_paths.dispatch_outbox.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "sensor_state",
        table_paths.sensor_state.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "sensor_evals",
        table_paths.sensor_evals.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "partition_status",
        table_paths.partition_status.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "schedule_definitions",
        table_paths.schedule_definitions.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "schedule_state",
        table_paths.schedule_state.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "schedule_ticks",
        table_paths.schedule_ticks.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "backfills",
        table_paths.backfills.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "backfill_chunks",
        table_paths.backfill_chunks.as_ref(),
    )
    .await?;
    registered += register_table_artifact(
        schema_provider,
        storage,
        "run_key_conflicts",
        table_paths.run_key_conflicts.as_ref(),
    )
    .await?;

    Ok(registered)
}

async fn register_table_artifact(
    schema_provider: &Arc<MemorySchemaProvider>,
    storage: &ScopedStorage,
    table_name: &str,
    artifact: Option<&TableArtifact>,
) -> Result<usize, ApiError> {
    let Some(artifact) = artifact else {
        return Ok(0);
    };

    let bytes = storage
        .get_raw(artifact.path())
        .await
        .map_err(ApiError::from)?;
    let table = parquet_bytes_to_mem_table(bytes)?;
    schema_provider
        .register_table(table_name.to_string(), table)
        .map_err(|err| ApiError::internal(format!("failed to register system table: {err}")))?;
    Ok(1)
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
