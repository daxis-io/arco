//! Query-data API routes.
//!
//! Provides a minimal SQL query endpoint backed by `DataFusion` over user tables
//! registered in the catalog.
//!
//! Notes:
//! - Only single-statement `SELECT` / `CTE` queries are supported.
//! - Table references must be fully qualified as `<catalog>.<schema>.<table>` (CTE names excluded).
//! - Currently only Parquet single-file locations are supported.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use tokio::time::timeout;

use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatchReader;
use arrow_json::ArrayWriter;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog_common::{CatalogProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use datafusion::sql::sqlparser::ast::{ObjectName, visit_relations};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arco_catalog::{CatalogReader, Table};

use super::query::QueryRequest;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const JSON_CONTENT_TYPE: &str = "application/json";
const MAX_QUERY_LENGTH: usize = 10_000;
const MAX_QUERY_ROWS: usize = 10_000;
const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REFERENCED_TABLES: usize = 25;
const MAX_PARQUET_BYTES_PER_TABLE: u64 = 64 * 1024 * 1024;
const MAX_PARQUET_BYTES_TOTAL: u64 = 256 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub(crate) struct QueryParams {
    /// Optional response format override: "json" or "arrow".
    pub format: Option<String>,
}

/// Creates query-data routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/query-data", post(query_data))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QualifiedTableRef {
    catalog: String,
    schema: String,
    table: String,
}

impl QualifiedTableRef {
    fn display(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Execute a SQL query against registered user tables.
///
/// POST /api/v1/query-data
#[utoipa::path(
    post,
    path = "/api/v1/query-data",
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
#[allow(clippy::too_many_lines)]
pub(crate) async fn query_data(
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
    let referenced_tables = extract_referenced_tables(sql)?;
    if referenced_tables.len() > MAX_REFERENCED_TABLES {
        return Err(ApiError::bad_request(format!(
            "query references too many tables (max {MAX_REFERENCED_TABLES})"
        )));
    }

    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        query_len = sql.len(),
        referenced_tables = referenced_tables.len(),
        "Executing SQL query (query-data)"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage.clone());

    let session = SessionContext::new();
    register_referenced_tables(&session, &ctx, &reader, &storage, &referenced_tables).await?;

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

fn extract_referenced_tables(sql: &str) -> Result<Vec<QualifiedTableRef>, ApiError> {
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
                .map(|cte| cte.alias.name.value.clone())
                .collect()
        })
        .unwrap_or_default();

    let mut referenced = Vec::new();
    let mut seen = HashSet::<QualifiedTableRef>::new();
    let mut err: Option<ApiError> = None;

    let _ = visit_relations(query, |relation: &ObjectName| {
        if err.is_some() {
            return std::ops::ControlFlow::Break(());
        }

        let parts: Vec<String> = relation.0.iter().map(|ident| ident.value.clone()).collect();

        match parts.as_slice() {
            [cte] if cte_names.contains(cte) => {}
            [catalog, schema, table] => {
                let table_ref = QualifiedTableRef {
                    catalog: catalog.clone(),
                    schema: schema.clone(),
                    table: table.clone(),
                };
                if seen.insert(table_ref.clone()) {
                    referenced.push(table_ref);
                }
            }
            _ => {
                err = Some(ApiError::bad_request(format!(
                    "table references must be fully qualified as <catalog>.<schema>.<table>; got {relation}"
                )));
                return std::ops::ControlFlow::Break(());
            }
        }

        std::ops::ControlFlow::Continue(())
    });

    if let Some(err) = err {
        return Err(err);
    }

    Ok(referenced)
}

async fn register_referenced_tables(
    session: &SessionContext,
    ctx: &RequestContext,
    reader: &CatalogReader,
    storage: &arco_core::ScopedStorage,
    referenced_tables: &[QualifiedTableRef],
) -> Result<usize, ApiError> {
    if referenced_tables.is_empty() {
        return Ok(0);
    }

    let mut registered = 0;
    let mut total_bytes: u64 = 0;
    let mut catalog_providers: HashMap<String, Arc<MemoryCatalogProvider>> = HashMap::new();
    let mut schema_providers: HashMap<(String, String), Arc<MemorySchemaProvider>> = HashMap::new();
    let mut schema_tables: HashMap<(String, String), Vec<Table>> = HashMap::new();

    for table_ref in referenced_tables {
        let catalog_provider = catalog_providers
            .get(&table_ref.catalog)
            .cloned()
            .map_or_else(
                || {
                    let provider = Arc::new(MemoryCatalogProvider::new());
                    session.register_catalog(&table_ref.catalog, provider.clone());
                    catalog_providers.insert(table_ref.catalog.clone(), provider.clone());
                    provider
                },
                |provider| provider,
            );

        let schema_key = (table_ref.catalog.clone(), table_ref.schema.clone());
        let schema_provider = if let Some(provider) = schema_providers.get(&schema_key) {
            provider.clone()
        } else {
            let provider = Arc::new(MemorySchemaProvider::new());
            catalog_provider
                .register_schema(&table_ref.schema, provider.clone())
                .map_err(|err| map_datafusion_error(&err))?;
            schema_providers.insert(schema_key.clone(), provider.clone());
            provider
        };

        if !schema_tables.contains_key(&schema_key) {
            let tables = reader
                .list_tables_in_schema(&table_ref.catalog, &table_ref.schema)
                .await
                .map_err(ApiError::from)?;
            schema_tables.insert(schema_key.clone(), tables);
        }

        let Some(table) = schema_tables
            .get(&schema_key)
            .and_then(|tables| tables.iter().find(|t| t.name == table_ref.table))
        else {
            return Err(ApiError::not_found(format!(
                "table not found: {}",
                table_ref.display()
            )));
        };

        if table.format.as_deref() != Some("parquet") {
            return Err(ApiError::bad_request(format!(
                "query-data currently supports only parquet tables; got {} ({:?})",
                table_ref.display(),
                table.format.as_deref().unwrap_or("unknown"),
            )));
        }

        let Some(location) = table.location.as_deref() else {
            return Err(ApiError::bad_request(format!(
                "table has no location: {}",
                table_ref.display()
            )));
        };

        let relative = normalize_scoped_location(location, &ctx.tenant, &ctx.workspace)?;
        if !relative.ends_with(".parquet") {
            return Err(ApiError::bad_request(format!(
                "query-data currently supports only .parquet locations; got {} ({location})",
                table_ref.display()
            )));
        }

        let meta = storage.head_raw(&relative).await.map_err(ApiError::from)?;
        let Some(meta) = meta else {
            return Err(ApiError::not_found(format!(
                "table location not found: {} ({location})",
                table_ref.display()
            )));
        };

        if meta.size > MAX_PARQUET_BYTES_PER_TABLE {
            return Err(ApiError::bad_request(format!(
                "table parquet file is too large for query-data (max {MAX_PARQUET_BYTES_PER_TABLE} bytes): {} ({location})",
                table_ref.display()
            )));
        }

        total_bytes = total_bytes.saturating_add(meta.size);
        if total_bytes > MAX_PARQUET_BYTES_TOTAL {
            return Err(ApiError::bad_request(format!(
                "combined parquet bytes exceeds query-data limit (max {MAX_PARQUET_BYTES_TOTAL} bytes)"
            )));
        }

        let bytes = storage.get_raw(&relative).await.map_err(ApiError::from)?;
        let provider: Arc<MemTable> = parquet_bytes_to_mem_table(bytes)?;

        schema_provider
            .register_table(table_ref.table.clone(), provider)
            .map_err(|err| map_datafusion_error(&err))?;

        registered += 1;
    }

    Ok(registered)
}

fn normalize_scoped_location(
    location: &str,
    tenant: &str,
    workspace: &str,
) -> Result<String, ApiError> {
    if location.starts_with("gs://")
        || location.starts_with("s3://")
        || location.starts_with("abfss://")
    {
        return Err(ApiError::bad_request(
            "query-data currently supports only scoped paths (not URI locations)",
        ));
    }

    let prefix = format!("tenant={tenant}/workspace={workspace}/");
    if let Some(stripped) = location.strip_prefix(&prefix) {
        return Ok(stripped.to_string());
    }

    if location.starts_with("tenant=") {
        return Err(ApiError::bad_request(
            "scoped location must match the request tenant/workspace",
        ));
    }

    Ok(location.to_string())
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
    schema: &datafusion::common::DFSchema,
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
    match err {
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(_, _) => ApiError::bad_request(err.to_string()),
        DataFusionError::ObjectStore(_) | DataFusionError::External(_) => {
            ApiError::not_found(err.to_string())
        }
        _ => ApiError::internal(err.to_string()),
    }
}
