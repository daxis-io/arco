//! Registration helpers for tenant-visible logical `system.*` tables.

#![allow(clippy::redundant_pub_crate, clippy::too_many_lines)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::{CatalogProvider, SchemaProvider};
use datafusion::prelude::SessionContext;

use arco_catalog::{CatalogError, CatalogReader};
use arco_core::{CatalogDomain, ScopedStorage};
use arco_flow::orchestration::compactor::{
    MicroCompactor, write_backfill_chunks, write_backfills, write_catalog_run_index,
    write_dep_satisfaction, write_dispatch_outbox, write_partition_status, write_run_key_conflicts,
    write_runs, write_schedule_definitions, write_schedule_state, write_schedule_ticks,
    write_sensor_evals, write_sensor_state, write_tasks, write_timers,
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
    SystemTableSpec {
        schema: "catalog",
        table: "commits",
        path: "commits.parquet",
    },
];

const LINEAGE_SYSTEM_TABLES: &[SystemTableSpec] = &[SystemTableSpec {
    schema: "lineage",
    table: "edges",
    path: "lineage_edges.parquet",
}];

const ORCHESTRATION_SCHEMA: &str = "orchestration";
const ORCHESTRATION_SYSTEM_TABLES: &[&str] = &[
    "runs",
    "tasks",
    "catalog_run_index",
    "dep_satisfaction",
    "timers",
    "dispatch_outbox",
    "sensor_state",
    "sensor_evals",
    "partition_status",
    "schedule_definitions",
    "schedule_state",
    "schedule_ticks",
    "backfills",
    "backfill_chunks",
    "run_key_conflicts",
];

/// Returns true when `system.{schema}.{table}` is part of the current
/// tenant-visible system catalog surface.
pub(crate) fn is_allowlisted_system_table(schema: &str, table: &str) -> bool {
    match schema {
        "catalog" => spec_table_is_allowlisted(CATALOG_SYSTEM_TABLES, table),
        "lineage" => spec_table_is_allowlisted(LINEAGE_SYSTEM_TABLES, table),
        ORCHESTRATION_SCHEMA => ORCHESTRATION_SYSTEM_TABLES.contains(&table),
        _ => false,
    }
}

fn spec_table_is_allowlisted(specs: &[SystemTableSpec], table: &str) -> bool {
    specs.iter().any(|spec| spec.table == table)
}

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

#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
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

    let (_manifest, state) = MicroCompactor::new(storage.clone())
        .load_state()
        .await
        .map_err(|err| {
            ApiError::internal(format!(
                "failed to load orchestration system table state: {err}"
            ))
        })?;

    let mut registered = 0;
    macro_rules! register_state_table {
        ($table_name:literal, $rows:expr, $writer:expr) => {
            if requested_tables.contains($table_name) {
                let rows: Vec<_> = $rows;
                let bytes = $writer(&rows).map_err(|err| {
                    ApiError::internal(format!(
                        "failed to encode system.orchestration.{}: {err}",
                        $table_name
                    ))
                })?;
                registered += register_table_bytes(schema_provider, $table_name, bytes)?;
            }
        };
    }

    if requested_tables.contains("runs") {
        register_state_table!("runs", state.runs.values().cloned().collect(), write_runs);
    }
    if requested_tables.contains("tasks") {
        register_state_table!(
            "tasks",
            state.tasks.values().cloned().collect(),
            write_tasks
        );
    }
    if requested_tables.contains("catalog_run_index") {
        register_state_table!(
            "catalog_run_index",
            state
                .catalog_run_index
                .values()
                .filter(|row| {
                    row.org_id.as_str() == storage.tenant_id()
                        && row.workspace_id.as_str() == storage.workspace_id()
                })
                .cloned()
                .collect(),
            write_catalog_run_index
        );
    }
    if requested_tables.contains("dep_satisfaction") {
        register_state_table!(
            "dep_satisfaction",
            state.dep_satisfaction.values().cloned().collect(),
            write_dep_satisfaction
        );
    }
    if requested_tables.contains("timers") {
        register_state_table!(
            "timers",
            state.timers.values().cloned().collect(),
            write_timers
        );
    }
    if requested_tables.contains("dispatch_outbox") {
        register_state_table!(
            "dispatch_outbox",
            state.dispatch_outbox.values().cloned().collect(),
            write_dispatch_outbox
        );
    }
    if requested_tables.contains("sensor_state") {
        register_state_table!(
            "sensor_state",
            state.sensor_state.values().cloned().collect(),
            write_sensor_state
        );
    }
    if requested_tables.contains("sensor_evals") {
        register_state_table!(
            "sensor_evals",
            state.sensor_evals.values().cloned().collect(),
            write_sensor_evals
        );
    }
    if requested_tables.contains("partition_status") {
        register_state_table!(
            "partition_status",
            state.partition_status.values().cloned().collect(),
            write_partition_status
        );
    }
    if requested_tables.contains("schedule_definitions") {
        register_state_table!(
            "schedule_definitions",
            state.schedule_definitions.values().cloned().collect(),
            write_schedule_definitions
        );
    }
    if requested_tables.contains("schedule_state") {
        register_state_table!(
            "schedule_state",
            state.schedule_state.values().cloned().collect(),
            write_schedule_state
        );
    }
    if requested_tables.contains("schedule_ticks") {
        register_state_table!(
            "schedule_ticks",
            state.schedule_ticks.values().cloned().collect(),
            write_schedule_ticks
        );
    }
    if requested_tables.contains("backfills") {
        register_state_table!(
            "backfills",
            state.backfills.values().cloned().collect(),
            write_backfills
        );
    }
    if requested_tables.contains("backfill_chunks") {
        register_state_table!(
            "backfill_chunks",
            state.backfill_chunks.values().cloned().collect(),
            write_backfill_chunks
        );
    }
    if requested_tables.contains("run_key_conflicts") {
        register_state_table!(
            "run_key_conflicts",
            state.run_key_conflicts.values().cloned().collect(),
            write_run_key_conflicts
        );
    }

    Ok(registered)
}

fn register_table_bytes(
    schema_provider: &Arc<MemorySchemaProvider>,
    table_name: &str,
    bytes: Bytes,
) -> Result<usize, ApiError> {
    let table = parquet_bytes_to_mem_table(bytes)?;
    schema_provider
        .register_table(table_name.to_string(), table)
        .map_err(|err| ApiError::internal(format!("failed to register system table: {err}")))?;
    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::is_allowlisted_system_table;

    #[test]
    fn system_table_allowlist_includes_current_catalog_lineage_and_orchestration_surface() {
        for (schema, table) in [
            ("catalog", "catalogs"),
            ("catalog", "namespaces"),
            ("catalog", "tables"),
            ("catalog", "columns"),
            ("catalog", "commits"),
            ("lineage", "edges"),
            ("orchestration", "runs"),
            ("orchestration", "tasks"),
            ("orchestration", "catalog_run_index"),
            ("orchestration", "dep_satisfaction"),
            ("orchestration", "timers"),
            ("orchestration", "dispatch_outbox"),
            ("orchestration", "sensor_state"),
            ("orchestration", "sensor_evals"),
            ("orchestration", "partition_status"),
            ("orchestration", "schedule_definitions"),
            ("orchestration", "schedule_state"),
            ("orchestration", "schedule_ticks"),
            ("orchestration", "backfills"),
            ("orchestration", "backfill_chunks"),
            ("orchestration", "run_key_conflicts"),
        ] {
            assert!(
                is_allowlisted_system_table(schema, table),
                "system.{schema}.{table} should be in the current allowlist"
            );
        }
    }

    #[test]
    fn system_table_allowlist_defers_unbacked_catalog_product_tables() {
        for (schema, table) in [
            ("access", "grants"),
            ("access", "compiled_permissions"),
            ("access", "audit"),
            ("access", "auth_denies"),
            ("access", "credential_mints"),
            ("storage", "credentials"),
            ("storage", "external_locations"),
            ("storage", "managed_roots"),
            ("storage", "workspace_bindings"),
            ("catalog", "volumes"),
            ("catalog", "functions"),
            ("catalog", "registered_models"),
            ("catalog", "model_versions"),
            ("governance", "attachments"),
        ] {
            assert!(
                !is_allowlisted_system_table(schema, table),
                "system.{schema}.{table} must stay deferred until authoritative projections exist"
            );
        }
    }
}
