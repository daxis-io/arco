//! Registration helpers for tenant-visible logical `system.*` tables.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::Schema;
use bytes::Bytes;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::{CatalogProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use arco_catalog::{CatalogError, CatalogReader};
use arco_core::{CatalogDomain, ScopedStorage};
use arco_flow::error::Result as FlowResult;
use arco_flow::orchestration::compactor::{
    MicroCompactor, backfill_chunks_parquet_schema, backfills_parquet_schema,
    dep_satisfaction_parquet_schema, dispatch_outbox_parquet_schema,
    partition_status_parquet_schema, run_key_conflicts_parquet_schema, run_schema,
    schedule_definitions_parquet_schema, schedule_state_parquet_schema,
    schedule_ticks_parquet_schema, sensor_evals_parquet_schema, sensor_state_parquet_schema,
    task_schema, timer_schema, write_backfill_chunks, write_backfills, write_dep_satisfaction,
    write_dispatch_outbox, write_partition_status, write_run_key_conflicts, write_runs,
    write_schedule_definitions, write_schedule_state, write_schedule_ticks, write_sensor_evals,
    write_sensor_state, write_tasks, write_timers,
};

use crate::error::ApiError;
use crate::parquet_table::parquet_bytes_to_mem_table;

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

/// Registers allowlisted projections under logical `system.*` names.
///
/// This keeps the system-table surface explicit and manifest-driven without
/// auto-exposing every file in a snapshot.
pub(crate) async fn register_system_tables(
    session: &SessionContext,
    reader: &CatalogReader,
    storage: &ScopedStorage,
    requested_tables: &HashMap<String, HashSet<String>>,
) -> Result<usize, ApiError> {
    if requested_tables.is_empty() {
        return Ok(0);
    }

    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    session.register_catalog("system", catalog_provider.clone());

    let schema_providers = register_system_schemas(
        &catalog_provider,
        requested_tables.keys().map(String::as_str),
    )?;

    let mut registered = 0;
    if let Some(catalog_tables) = requested_tables.get("catalog") {
        registered += register_domain_specs(
            reader,
            storage,
            CatalogDomain::Catalog,
            CATALOG_SYSTEM_TABLES,
            catalog_tables,
            &schema_providers,
        )
        .await?;
    }
    if let Some(lineage_tables) = requested_tables.get("lineage") {
        registered += register_domain_specs(
            reader,
            storage,
            CatalogDomain::Lineage,
            LINEAGE_SYSTEM_TABLES,
            lineage_tables,
            &schema_providers,
        )
        .await?;
    }
    if let Some(orchestration_tables) = requested_tables.get(ORCHESTRATION_SCHEMA) {
        registered +=
            register_orchestration_tables(storage, orchestration_tables, &schema_providers).await?;
    }
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
    requested_tables: &HashSet<String>,
    schema_providers: &HashMap<&str, Arc<MemorySchemaProvider>>,
) -> Result<usize, ApiError> {
    if requested_tables.is_empty() {
        return Ok(0);
    }

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
        let Some(spec) = specs
            .iter()
            .find(|spec| spec.path == file_name && requested_tables.contains(spec.table))
        else {
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
    requested_tables: &HashSet<String>,
    schema_providers: &HashMap<&str, Arc<MemorySchemaProvider>>,
) -> Result<usize, ApiError> {
    if requested_tables.is_empty() {
        return Ok(0);
    }

    let Some(schema_provider) = schema_providers.get(ORCHESTRATION_SCHEMA) else {
        return Err(ApiError::internal(
            "missing system schema provider for 'orchestration'",
        ));
    };

    let (_, state) = MicroCompactor::new(storage.clone())
        .load_state()
        .await
        .map_err(|err| {
            ApiError::internal(format!(
                "failed to load visible orchestration state for system tables: {err}"
            ))
        })?;

    let mut registered = 0;
    if requested_tables.contains("runs") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "runs",
            state.runs.values().cloned().collect(),
            run_schema,
            write_runs,
        )?;
    }
    if requested_tables.contains("tasks") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "tasks",
            state.tasks.values().cloned().collect(),
            task_schema,
            write_tasks,
        )?;
    }
    if requested_tables.contains("dep_satisfaction") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "dep_satisfaction",
            state.dep_satisfaction.values().cloned().collect(),
            dep_satisfaction_parquet_schema,
            write_dep_satisfaction,
        )?;
    }
    if requested_tables.contains("timers") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "timers",
            state.timers.values().cloned().collect(),
            timer_schema,
            write_timers,
        )?;
    }
    if requested_tables.contains("dispatch_outbox") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "dispatch_outbox",
            state.dispatch_outbox.values().cloned().collect(),
            dispatch_outbox_parquet_schema,
            write_dispatch_outbox,
        )?;
    }
    if requested_tables.contains("sensor_state") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "sensor_state",
            state.sensor_state.values().cloned().collect(),
            sensor_state_parquet_schema,
            write_sensor_state,
        )?;
    }
    if requested_tables.contains("sensor_evals") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "sensor_evals",
            state.sensor_evals.values().cloned().collect(),
            sensor_evals_parquet_schema,
            write_sensor_evals,
        )?;
    }
    if requested_tables.contains("partition_status") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "partition_status",
            state.partition_status.values().cloned().collect(),
            partition_status_parquet_schema,
            write_partition_status,
        )?;
    }
    if requested_tables.contains("schedule_definitions") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "schedule_definitions",
            state.schedule_definitions.values().cloned().collect(),
            schedule_definitions_parquet_schema,
            write_schedule_definitions,
        )?;
    }
    if requested_tables.contains("schedule_state") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "schedule_state",
            state.schedule_state.values().cloned().collect(),
            schedule_state_parquet_schema,
            write_schedule_state,
        )?;
    }
    if requested_tables.contains("schedule_ticks") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "schedule_ticks",
            state.schedule_ticks.values().cloned().collect(),
            schedule_ticks_parquet_schema,
            write_schedule_ticks,
        )?;
    }
    if requested_tables.contains("backfills") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "backfills",
            state.backfills.values().cloned().collect(),
            backfills_parquet_schema,
            write_backfills,
        )?;
    }
    if requested_tables.contains("backfill_chunks") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "backfill_chunks",
            state.backfill_chunks.values().cloned().collect(),
            backfill_chunks_parquet_schema,
            write_backfill_chunks,
        )?;
    }
    if requested_tables.contains("run_key_conflicts") {
        registered += register_orchestration_rows_table(
            schema_provider,
            "run_key_conflicts",
            state.run_key_conflicts.values().cloned().collect(),
            run_key_conflicts_parquet_schema,
            write_run_key_conflicts,
        )?;
    }

    Ok(registered)
}

fn register_orchestration_rows_table<R>(
    schema_provider: &Arc<MemorySchemaProvider>,
    table_name: &str,
    rows: Vec<R>,
    schema_fn: fn() -> Schema,
    encode: fn(&[R]) -> FlowResult<Bytes>,
) -> Result<usize, ApiError>
where
    R: Clone,
{
    let table = if rows.is_empty() {
        empty_mem_table(schema_fn())?
    } else {
        let bytes = encode(&rows).map_err(|err| {
            ApiError::internal(format!(
                "failed to encode orchestration system table '{table_name}': {err}"
            ))
        })?;
        parquet_bytes_to_mem_table(bytes)?
    };
    register_mem_table(schema_provider, table_name, table)?;
    Ok(1)
}

fn register_mem_table(
    schema_provider: &Arc<MemorySchemaProvider>,
    table_name: &str,
    table: Arc<MemTable>,
) -> Result<(), ApiError> {
    schema_provider
        .register_table(table_name.to_string(), table)
        .map_err(|err| ApiError::internal(format!("failed to register system table: {err}")))?;
    Ok(())
}

fn empty_mem_table(schema: Schema) -> Result<Arc<MemTable>, ApiError> {
    let table = MemTable::try_new(Arc::new(schema), vec![Vec::new()]).map_err(|err| {
        ApiError::internal(format!("failed to register empty system table: {err}"))
    })?;
    Ok(Arc::new(table))
}
