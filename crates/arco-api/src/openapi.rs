//! `OpenAPI` (3.1) specification generation for `arco-api`.
//!
//! The checked-in spec is used to generate external clients (TS / Rust) and to
//! detect breaking API changes in CI.

#![allow(clippy::needless_for_each)]

use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

/// `OpenAPI` documentation for the Arco REST API (`/api/v1/*`).
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Arco API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Arco catalog REST API (M4/M5)"
    ),
    paths(
        crate::routes::catalogs::create_catalog,
        crate::routes::catalogs::list_catalogs,
        crate::routes::catalogs::get_catalog,
        crate::routes::catalogs::create_schema,
        crate::routes::catalogs::list_schemas,
        crate::routes::catalogs::register_table_in_schema,
        crate::routes::catalogs::list_tables_in_schema,
        crate::routes::catalogs::get_table_in_schema,
        crate::routes::namespaces::create_namespace,
        crate::routes::namespaces::list_namespaces,
        crate::routes::namespaces::get_namespace,
        crate::routes::namespaces::delete_namespace,
        crate::routes::tables::register_table,
        crate::routes::tables::list_tables,
        crate::routes::tables::get_table,
        crate::routes::tables::update_table,
        crate::routes::tables::drop_table,
        crate::routes::catalogs::create_catalog,
        crate::routes::catalogs::list_catalogs,
        crate::routes::catalogs::get_catalog,
        crate::routes::catalogs::create_schema,
        crate::routes::catalogs::list_schemas,
        crate::routes::catalogs::register_table_in_schema,
        crate::routes::catalogs::list_tables_in_schema,
        crate::routes::catalogs::get_table_in_schema,
        crate::routes::delta::stage_commit_payload,
        crate::routes::delta::commit_staged,
        crate::routes::lineage::add_edges,
        crate::routes::lineage::get_lineage,
        crate::routes::browser::mint_urls,
        crate::routes::query::query,
        crate::routes::query_data::query_data,
        crate::routes::orchestration::trigger_run,
        crate::routes::orchestration::list_runs,
        crate::routes::orchestration::get_run,
        crate::routes::orchestration::rerun_run,
        crate::routes::orchestration::cancel_run,
        crate::routes::orchestration::backfill_run_key,
        crate::routes::orchestration::manual_evaluate_sensor,
        crate::routes::orchestration::upload_run_logs,
        crate::routes::orchestration::get_run_logs,
        crate::routes::orchestration::list_schedules,
        crate::routes::orchestration::get_schedule,
        crate::routes::orchestration::list_schedule_ticks,
        crate::routes::orchestration::list_sensors,
        crate::routes::orchestration::get_sensor,
        crate::routes::orchestration::list_sensor_evals,
        crate::routes::orchestration::create_backfill,
        crate::routes::orchestration::list_backfills,
        crate::routes::orchestration::get_backfill,
        crate::routes::orchestration::list_backfill_chunks,
        crate::routes::orchestration::list_partitions,
        crate::routes::orchestration::get_asset_partition_summary,
        crate::routes::manifests::deploy_manifest,
        crate::routes::manifests::list_manifests,
        crate::routes::manifests::get_manifest,
        crate::routes::tasks::task_started,
        crate::routes::tasks::task_heartbeat,
        crate::routes::tasks::task_completed,
    ),
    components(
        schemas(
            crate::error::ApiErrorBody,
            crate::routes::catalogs::CreateCatalogRequest,
            crate::routes::catalogs::CatalogResponse,
            crate::routes::catalogs::ListCatalogsResponse,
            crate::routes::catalogs::CreateSchemaRequest,
            crate::routes::catalogs::SchemaResponse,
            crate::routes::catalogs::ListSchemasResponse,
            crate::routes::catalogs::RegisterSchemaTableRequest,
            crate::routes::catalogs::SchemaTableResponse,
            crate::routes::catalogs::ListSchemaTablesResponse,
            crate::routes::namespaces::CreateNamespaceRequest,
            crate::routes::namespaces::NamespaceResponse,
            crate::routes::namespaces::ListNamespacesResponse,
            crate::routes::tables::RegisterTableRequest,
            crate::routes::tables::ColumnDefinition,
            crate::routes::tables::UpdateTableRequest,
            crate::routes::tables::TableResponse,
            crate::routes::tables::ColumnResponse,
            crate::routes::tables::ListTablesResponse,
            crate::routes::catalogs::CreateCatalogRequest,
            crate::routes::catalogs::CatalogResponse,
            crate::routes::catalogs::ListCatalogsResponse,
            crate::routes::catalogs::CreateSchemaRequest,
            crate::routes::catalogs::SchemaResponse,
            crate::routes::catalogs::ListSchemasResponse,
            crate::routes::catalogs::RegisterSchemaTableRequest,
            crate::routes::catalogs::SchemaTableResponse,
            crate::routes::catalogs::ListSchemaTablesResponse,
            crate::routes::delta::StageCommitRequest,
            crate::routes::delta::StageCommitResponse,
            crate::routes::delta::CommitRequest,
            crate::routes::delta::CommitResponse,
            crate::routes::lineage::AddEdgesRequest,
            crate::routes::lineage::EdgeDefinition,
            crate::routes::lineage::EdgeResponse,
            crate::routes::lineage::AddEdgesResponse,
            crate::routes::lineage::LineageResponse,
            crate::routes::browser::MintUrlsRequest,
            crate::routes::browser::MintUrlsResponse,
            crate::routes::browser::SignedUrl,
            crate::routes::query::QueryRequest,
            crate::routes::delta::StageCommitRequest,
            crate::routes::delta::StageCommitResponse,
            crate::routes::delta::CommitRequest,
            crate::routes::delta::CommitResponse,
            crate::routes::orchestration::TriggerRunRequestOpenApi,
            crate::routes::orchestration::TriggerRunResponse,
            crate::routes::orchestration::RerunMode,
            crate::routes::orchestration::RerunKindResponse,
            crate::routes::orchestration::RerunRunRequest,
            crate::routes::orchestration::RerunRunResponse,
            crate::routes::orchestration::RunKeyBackfillRequest,
            crate::routes::orchestration::RunKeyBackfillResponse,
            crate::routes::orchestration::PartitionValue,
            crate::routes::orchestration::RunStateResponse,
            crate::routes::orchestration::RunResponse,
            crate::routes::orchestration::TaskSummary,
            crate::routes::orchestration::TaskStateResponse,
            crate::routes::orchestration::TaskCounts,
            crate::routes::orchestration::ListRunsResponse,
            crate::routes::orchestration::RunListItem,
            crate::routes::orchestration::CancelRunRequest,
            crate::routes::orchestration::CancelRunResponse,
            crate::routes::orchestration::RunLogsRequest,
            crate::routes::orchestration::RunLogsResponse,
            crate::routes::orchestration::RunLogsQuery,
            crate::routes::orchestration::ManualSensorEvaluateRequest,
            crate::routes::orchestration::ManualSensorEvaluateResponse,
            crate::routes::orchestration::SensorEvalStatusResponse,
            crate::routes::orchestration::RunRequestResponse,
            crate::routes::orchestration::ScheduleResponse,
            crate::routes::orchestration::ListSchedulesResponse,
            crate::routes::orchestration::ScheduleTickResponse,
            crate::routes::orchestration::TickStatusResponse,
            crate::routes::orchestration::ListScheduleTicksResponse,
            crate::routes::orchestration::SensorResponse,
            crate::routes::orchestration::SensorStatusResponse,
            crate::routes::orchestration::ListSensorsResponse,
            crate::routes::orchestration::SensorEvalResponse,
            crate::routes::orchestration::ListSensorEvalsResponse,
            crate::routes::orchestration::CreateBackfillRequest,
            crate::routes::orchestration::CreateBackfillResponse,
            crate::routes::orchestration::PartitionSelectorRequest,
            crate::routes::orchestration::UnprocessableEntityResponse,
            crate::routes::orchestration::BackfillResponse,
            crate::routes::orchestration::PartitionSelectorResponse,
            crate::routes::orchestration::BackfillStateResponse,
            crate::routes::orchestration::ChunkCounts,
            crate::routes::orchestration::ListBackfillsResponse,
            crate::routes::orchestration::BackfillChunkResponse,
            crate::routes::orchestration::ChunkStateResponse,
            crate::routes::orchestration::ListBackfillChunksResponse,
            crate::routes::orchestration::PartitionStatusApiResponse,
            crate::routes::orchestration::ListPartitionsResponse,
            crate::routes::orchestration::AssetPartitionSummaryResponse,
            crate::routes::orchestration::ListQuery,
            crate::routes::orchestration::ListTicksQuery,
            crate::routes::orchestration::ListBackfillsQuery,
            crate::routes::orchestration::ListChunksQuery,
            crate::routes::orchestration::ListPartitionsQuery,
            crate::routes::manifests::DeployManifestRequest,
            crate::routes::manifests::DeployManifestResponse,
            crate::routes::manifests::StoredManifest,
            crate::routes::manifests::ListManifestsResponse,
            crate::routes::manifests::ManifestListItem,
            crate::routes::manifests::AssetEntry,
            crate::routes::manifests::AssetKey,
            crate::routes::manifests::AssetDependency,
            crate::routes::manifests::ScheduleEntry,
            crate::routes::manifests::GitContext,
            crate::routes::tasks::TaskStartedRequest,
            crate::routes::tasks::TaskStartedResponse,
            crate::routes::tasks::HeartbeatRequest,
            crate::routes::tasks::HeartbeatResponse,
            crate::routes::tasks::TaskCompletedRequest,
            crate::routes::tasks::TaskCompletedResponse,
            crate::routes::tasks::WorkerOutcome,
            crate::routes::tasks::TaskOutput,
            crate::routes::tasks::TaskError,
            crate::routes::tasks::ErrorCategory,
            crate::routes::tasks::TaskMetrics,
            crate::routes::tasks::CallbackErrorResponse,
        )
    ),
    tags(
        (name = "catalogs", description = "Catalog operations"),
        (name = "schemas", description = "Schema operations"),
        (name = "namespaces", description = "Namespace operations"),
        (name = "tables", description = "Table operations"),
        (name = "catalogs", description = "Catalog operations"),
        (name = "schemas", description = "Schema operations"),
        (name = "delta", description = "Delta commit coordination"),
        (name = "lineage", description = "Lineage operations"),
        (name = "browser", description = "Browser signed URL minting"),
        (name = "query", description = "SQL query execution"),
        (name = "delta", description = "Delta commit coordination (Mode B)"),
        (name = "Orchestration", description = "Run orchestration operations"),
        (name = "Manifests", description = "Manifest deployment operations"),
        (name = "Worker Callbacks", description = "Worker task lifecycle callbacks"),
    ),
    modifiers(&SecurityAddon),
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
        components.add_security_scheme(
            "taskAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

/// Returns the generated `OpenAPI` spec.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

/// Returns the generated `OpenAPI` spec serialized as pretty JSON.
///
/// # Errors
///
/// Returns an error if JSON serialization fails (should not happen).
pub fn openapi_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&openapi())
}
