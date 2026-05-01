//! Query API routes.
//!
//! Provides a minimal SQL query endpoint backed by `DataFusion`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use tokio::time::timeout;
use utoipa::ToSchema;

use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow_json::ArrayWriter;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{ObjectName, Statement as SqlStatement, visit_relations};

use arco_catalog::{CatalogError, CatalogReader};
use arco_core::CatalogDomain;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::parquet_table::parquet_bytes_to_mem_table;
use crate::server::AppState;
use crate::system_tables;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const JSON_CONTENT_TYPE: &str = "application/json";
const MAX_QUERY_LENGTH: usize = 10_000;
const MAX_QUERY_ROWS: usize = 10_000;
const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Query request payload.
#[derive(Debug, Deserialize, ToSchema)]
pub struct QueryRequest {
    /// SQL query to execute (read-only).
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryParams {
    /// Optional response format override: "json" or "arrow".
    pub format: Option<String>,
}

#[derive(Debug, Default)]
struct QueryRegistrationTargets {
    snapshot_tables: HashMap<String, HashSet<String>>,
    system_tables: HashMap<String, HashSet<String>>,
    known_relation_count: usize,
}

impl QueryRegistrationTargets {
    fn record_snapshot(&mut self, schema: &str, table: &str) {
        if self
            .snapshot_tables
            .entry(schema.to_string())
            .or_default()
            .insert(table.to_string())
        {
            self.known_relation_count += 1;
        }
    }

    fn record_system(&mut self, schema: &str, table: &str) {
        if self
            .system_tables
            .entry(schema.to_string())
            .or_default()
            .insert(table.to_string())
        {
            self.known_relation_count += 1;
        }
    }
}

/// Creates query routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/query", post(query))
}

/// Execute a SQL query against catalog snapshots.
///
/// POST /api/v1/query
#[utoipa::path(
    post,
    path = "/api/v1/query",
    tag = "query",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query results (Arrow IPC stream or JSON)"),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn query(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    Json(req): Json<QueryRequest>,
) -> Result<Response, ApiError> {
    let sql = req.sql.trim();
    if sql.is_empty() {
        return Err(ApiError::bad_request("sql cannot be empty"));
    }
    if sql.len() > MAX_QUERY_LENGTH {
        return Err(ApiError::bad_request(format!(
            "sql exceeds max length ({MAX_QUERY_LENGTH} bytes)",
        )));
    }
    let registration_targets = analyze_query(sql)?;

    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        query_len = sql.len(),
        "Executing SQL query"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage.clone());

    let session = SessionContext::new();
    let registered = register_snapshot_tables(
        &session,
        &reader,
        &storage,
        &registration_targets.snapshot_tables,
    )
    .await?
        + system_tables::register_system_tables(
            &session,
            &reader,
            &storage,
            &registration_targets.system_tables,
        )
        .await?;
    if registration_targets.known_relation_count > 0 && registered == 0 {
        return Err(ApiError::not_found(
            "No snapshot tables available for query",
        ));
    }

    let df = session
        .sql(sql)
        .await
        .map_err(|err| map_datafusion_error(&err))?
        .limit(0, Some(MAX_QUERY_ROWS))
        .map_err(|err| map_datafusion_error(&err))?;
    let schema = df.schema().clone();
    let batches = timeout(QUERY_TIMEOUT, df.collect())
        .await
        .map_err(|_| ApiError::bad_request("query timed out"))?
        .map_err(|err| map_datafusion_error(&err))?;

    if wants_json(&params, &headers)? {
        let payload = batches_to_json(&batches)?;
        ensure_response_size(payload.len())?;
        let mut response = Response::new(Body::from(payload));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(JSON_CONTENT_TYPE),
        );
        return Ok(response);
    }

    let payload = batches_to_arrow(&schema, &batches)?;
    ensure_response_size(payload.len())?;
    let mut response = Response::new(Body::from(payload));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(ARROW_STREAM_CONTENT_TYPE),
    );
    Ok(response)
}

fn analyze_query(sql: &str) -> Result<QueryRegistrationTargets, ApiError> {
    let statements = DFParser::parse_sql(sql)
        .map_err(|err| ApiError::bad_request(format!("failed to parse SQL: {err}")))?;
    let mut iter = statements.iter();
    let Some(statement) = iter.next() else {
        return Err(ApiError::bad_request("sql must contain a statement"));
    };
    if iter.next().is_some() {
        return Err(ApiError::bad_request(
            "only single-statement queries are supported",
        ));
    }
    let DFStatement::Statement(statement) = statement else {
        return Err(ApiError::bad_request(
            "Only SELECT/CTE queries are supported",
        ));
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return Err(ApiError::bad_request(
            "Only SELECT/CTE queries are supported",
        ));
    };

    let cte_names: HashSet<String> = query
        .with
        .as_ref()
        .map(|with| {
            with.cte_tables
                .iter()
                .map(|cte| cte.alias.name.value.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default();

    let mut targets = QueryRegistrationTargets::default();

    let _ = visit_relations(query, |relation: &ObjectName| {
        let parts: Vec<String> = relation
            .0
            .iter()
            .map(|ident| ident.value.to_ascii_lowercase())
            .collect();

        if parts.len() == 1 && cte_names.contains(&parts[0]) {
            return std::ops::ControlFlow::<()>::Continue(());
        }

        match parts.as_slice() {
            [schema, table] if is_known_snapshot_table(schema, table) => {
                targets.record_snapshot(schema, table);
            }
            [catalog, schema, table]
                if catalog == "system" && is_known_system_table(schema, table) =>
            {
                targets.record_system(schema, table);
            }
            _ => {}
        }

        std::ops::ControlFlow::<()>::Continue(())
    });

    Ok(targets)
}

fn is_known_snapshot_table(schema: &str, table: &str) -> bool {
    matches!(
        (schema, table),
        ("catalog", "catalogs" | "namespaces" | "tables" | "columns")
            | ("lineage", "lineage_edges")
            | ("search", "token_postings")
    )
}

fn is_known_system_table(schema: &str, table: &str) -> bool {
    matches!(
        (schema, table),
        (
            "catalog",
            "catalogs" | "namespaces" | "tables" | "columns" | "commits"
        ) | ("lineage", "edges")
            | (
                "orchestration",
                "runs"
                    | "tasks"
                    | "dep_satisfaction"
                    | "timers"
                    | "dispatch_outbox"
                    | "sensor_state"
                    | "sensor_evals"
                    | "partition_status"
                    | "schedule_definitions"
                    | "schedule_state"
                    | "schedule_ticks"
                    | "backfills"
                    | "backfill_chunks"
                    | "run_key_conflicts"
            )
    )
}

fn wants_json(params: &QueryParams, headers: &HeaderMap) -> Result<bool, ApiError> {
    if let Some(format) = params.format.as_deref() {
        let normalized = format.trim().to_ascii_lowercase();
        return match normalized.as_str() {
            "json" => Ok(true),
            "arrow" => Ok(false),
            _ => Err(ApiError::bad_request("format must be one of: json, arrow")),
        };
    }

    let Some(value) = headers.get(header::ACCEPT) else {
        return Ok(false);
    };
    let Ok(value) = value.to_str() else {
        return Ok(false);
    };
    Ok(value.contains(JSON_CONTENT_TYPE))
}

async fn register_snapshot_tables(
    session: &SessionContext,
    reader: &CatalogReader,
    storage: &arco_core::ScopedStorage,
    requested_tables: &HashMap<String, HashSet<String>>,
) -> Result<usize, ApiError> {
    let mut registered = 0;
    if let Some(catalog_tables) = requested_tables.get("catalog") {
        registered += register_domain_tables(
            session,
            reader,
            storage,
            CatalogDomain::Catalog,
            &[
                ("catalogs.parquet", "catalogs"),
                ("namespaces.parquet", "namespaces"),
                ("tables.parquet", "tables"),
                ("columns.parquet", "columns"),
            ],
            catalog_tables,
        )
        .await?;
    }
    if let Some(lineage_tables) = requested_tables.get("lineage") {
        registered += register_domain_tables(
            session,
            reader,
            storage,
            CatalogDomain::Lineage,
            &[("lineage_edges.parquet", "lineage_edges")],
            lineage_tables,
        )
        .await?;
    }
    if let Some(search_tables) = requested_tables.get("search") {
        registered += register_domain_tables(
            session,
            reader,
            storage,
            CatalogDomain::Search,
            &[("token_postings.parquet", "token_postings")],
            search_tables,
        )
        .await?;
    }
    Ok(registered)
}

async fn register_domain_tables(
    session: &SessionContext,
    reader: &CatalogReader,
    storage: &arco_core::ScopedStorage,
    domain: CatalogDomain,
    mappings: &[(&str, &str)],
    requested_tables: &HashSet<String>,
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

    ensure_schema(session, domain.as_str()).await?;

    let mut registered = 0;
    for path in paths {
        let Some(file_name) = path.rsplit('/').next() else {
            continue;
        };
        let Some((_, table_name)) = mappings.iter().find(|(name, table_name)| {
            *name == file_name && requested_tables.contains(*table_name)
        }) else {
            continue;
        };

        let bytes = storage.get_raw(&path).await.map_err(ApiError::from)?;
        let table = parquet_bytes_to_mem_table(bytes)?;
        session
            .register_table(TableReference::partial(domain.as_str(), *table_name), table)
            .map_err(|err| map_datafusion_error(&err))?;
        registered += 1;
    }

    Ok(registered)
}

async fn ensure_schema(session: &SessionContext, schema: &str) -> Result<(), ApiError> {
    let statement = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
    let df = session
        .sql(&statement)
        .await
        .map_err(|err| map_datafusion_error(&err))?;
    df.collect()
        .await
        .map_err(|err| map_datafusion_error(&err))?;
    Ok(())
}

fn batches_to_json(batches: &[arrow::record_batch::RecordBatch]) -> Result<Vec<u8>, ApiError> {
    let mut writer = ArrayWriter::new(Vec::new());
    let refs: Vec<&arrow::record_batch::RecordBatch> = batches.iter().collect();
    writer
        .write_batches(&refs)
        .map_err(|e| ApiError::internal(format!("failed to write JSON: {e}")))?;
    writer
        .finish()
        .map_err(|e| ApiError::internal(format!("failed to finalize JSON: {e}")))?;
    Ok(writer.into_inner())
}

fn batches_to_arrow(
    schema: &DFSchema,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ApiError> {
    let schema = Schema::from(schema);
    let schema = Arc::new(schema);
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| ApiError::internal(format!("failed to start arrow stream: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| ApiError::internal(format!("failed to write arrow batch: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| ApiError::internal(format!("failed to finish arrow stream: {e}")))?;
    }
    Ok(buffer)
}

fn ensure_response_size(len: usize) -> Result<(), ApiError> {
    if len > MAX_RESPONSE_BYTES {
        return Err(ApiError::bad_request(format!(
            "query result exceeds max size ({MAX_RESPONSE_BYTES} bytes)",
        )));
    }
    Ok(())
}

fn map_datafusion_error(err: &DataFusionError) -> ApiError {
    if is_client_query_error(err) {
        ApiError::bad_request(err.to_string())
    } else {
        ApiError::internal(format!("query failed: {err}"))
    }
}

fn is_client_query_error(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Execution(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::NotImplemented(_) => true,
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            is_client_query_error(inner)
        }
        DataFusionError::Shared(inner) => is_client_query_error(inner),
        DataFusionError::Collection(errors) => errors.iter().all(is_client_query_error),
        _ => false,
    }
}
