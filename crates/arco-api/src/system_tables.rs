//! Registration helpers for tenant-visible logical `system.*` tables.

#![allow(clippy::redundant_pub_crate, clippy::too_many_lines)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::{CatalogProvider, SchemaProvider};
use datafusion::prelude::SessionContext;

use arco_catalog::{CatalogError, CatalogReader};
use arco_core::{CatalogDomain, ScopedStorage};
use arco_flow::orchestration::compactor::{MicroCompactor, TableArtifact};

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

#[allow(clippy::cognitive_complexity)]
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

    let table_paths = MicroCompactor::new(storage.clone())
        .current_base_table_paths()
        .await
        .map_err(|err| {
            ApiError::internal(format!(
                "failed to load orchestration base snapshot paths: {err}"
            ))
        })?;

    let mut registered = 0;
    if requested_tables.contains("runs") {
        registered +=
            register_table_artifact(schema_provider, storage, "runs", table_paths.runs.as_ref())
                .await?;
    }
    if requested_tables.contains("tasks") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "tasks",
            table_paths.tasks.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("dep_satisfaction") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "dep_satisfaction",
            table_paths.dep_satisfaction.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("timers") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "timers",
            table_paths.timers.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("dispatch_outbox") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "dispatch_outbox",
            table_paths.dispatch_outbox.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("sensor_state") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "sensor_state",
            table_paths.sensor_state.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("sensor_evals") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "sensor_evals",
            table_paths.sensor_evals.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("partition_status") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "partition_status",
            table_paths.partition_status.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("schedule_definitions") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "schedule_definitions",
            table_paths.schedule_definitions.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("schedule_state") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "schedule_state",
            table_paths.schedule_state.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("schedule_ticks") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "schedule_ticks",
            table_paths.schedule_ticks.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("backfills") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "backfills",
            table_paths.backfills.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("backfill_chunks") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "backfill_chunks",
            table_paths.backfill_chunks.as_ref(),
        )
        .await?;
    }
    if requested_tables.contains("run_key_conflicts") {
        registered += register_table_artifact(
            schema_provider,
            storage,
            "run_key_conflicts",
            table_paths.run_key_conflicts.as_ref(),
        )
        .await?;
    }

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
