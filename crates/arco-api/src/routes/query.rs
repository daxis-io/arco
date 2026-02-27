//! Query API routes.
//!
//! Provides a minimal SQL query endpoint backed by `DataFusion`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use tokio::time::timeout;
use utoipa::ToSchema;

use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatchReader;
use arrow_json::ArrayWriter;
use datafusion::common::DFSchema;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tower::limit::ConcurrencyLimitLayer;

use arco_catalog::CatalogReader;
use arco_core::CatalogDomain;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const JSON_CONTENT_TYPE: &str = "application/json";
const MAX_QUERY_LENGTH: usize = 10_000;
const MAX_QUERY_ROWS: usize = 10_000;
const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const MAX_QUERY_REQUEST_BYTES: usize = 64 * 1024;
const MAX_QUERY_CONCURRENCY: usize = 8;
const MAX_SNAPSHOT_FILE_BYTES: u64 = 64 * 1024 * 1024;
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

/// Creates query routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/query", post(query))
        .layer(DefaultBodyLimit::max(MAX_QUERY_REQUEST_BYTES))
        .layer(ConcurrencyLimitLayer::new(MAX_QUERY_CONCURRENCY))
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
    validate_query(sql)?;

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
    let registered = register_snapshot_tables(&session, &reader, &storage).await?;
    if registered == 0 {
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

fn validate_query(sql: &str) -> Result<(), ApiError> {
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
    match statement {
        DFStatement::Statement(statement) => match statement.as_ref() {
            SqlStatement::Query(_) => Ok(()),
            _ => Err(ApiError::bad_request(
                "Only SELECT/CTE queries are supported",
            )),
        },
        _ => Err(ApiError::bad_request(
            "Only SELECT/CTE queries are supported",
        )),
    }
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
) -> Result<usize, ApiError> {
    let mut registered = 0;
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
    )
    .await?;
    registered += register_domain_tables(
        session,
        reader,
        storage,
        CatalogDomain::Lineage,
        &[("lineage_edges.parquet", "lineage_edges")],
    )
    .await?;
    registered += register_domain_tables(
        session,
        reader,
        storage,
        CatalogDomain::Search,
        &[("token_postings.parquet", "token_postings")],
    )
    .await?;
    Ok(registered)
}

async fn register_domain_tables(
    session: &SessionContext,
    reader: &CatalogReader,
    storage: &arco_core::ScopedStorage,
    domain: CatalogDomain,
    mappings: &[(&str, &str)],
) -> Result<usize, ApiError> {
    let paths = reader
        .get_mintable_paths(domain)
        .await
        .map_err(ApiError::from)?;
    if paths.is_empty() {
        return Ok(0);
    }

    ensure_schema(session, domain.as_str()).await?;

    let mut registered = 0;
    for path in paths {
        let Some(file_name) = path.rsplit('/').next() else {
            continue;
        };
        let Some((_, table_name)) = mappings.iter().find(|(name, _)| *name == file_name) else {
            continue;
        };

        let object_meta = storage
            .head_raw(&path)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::not_found(format!("snapshot file not found: {path}")))?;
        ensure_snapshot_file_size(&path, object_meta.size)?;

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

fn ensure_snapshot_file_size(path: &str, size: u64) -> Result<(), ApiError> {
    if size > MAX_SNAPSHOT_FILE_BYTES {
        return Err(ApiError::bad_request(format!(
            "snapshot file exceeds max size ({MAX_SNAPSHOT_FILE_BYTES} bytes): {path}",
        )));
    }
    Ok(())
}

fn map_datafusion_error(err: &DataFusionError) -> ApiError {
    match err {
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(_, _) => ApiError::bad_request(err.to_string()),
        _ => ApiError::internal(format!("query failed: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tower::util::ServiceExt;

    use crate::config::Config;
    use crate::server::AppState;

    #[tokio::test]
    async fn query_route_rejects_oversized_payload() {
        let mut config = Config::default();
        config.debug = true;
        let state = Arc::new(AppState::with_memory_storage(config));
        let app = routes().with_state(state);

        let sql = "x".repeat(70_000);
        let body = serde_json::json!({ "sql": sql }).to_string();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-tenant-id", "acme")
                    .header("x-workspace-id", "analytics")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn snapshot_file_size_limit_rejects_oversized_file() {
        let err = ensure_snapshot_file_size(
            "catalog/tables.parquet",
            MAX_SNAPSHOT_FILE_BYTES.saturating_add(1),
        )
        .expect_err("oversized file should be rejected");
        assert!(err.message().contains("snapshot file exceeds max size"));
    }

    #[test]
    fn snapshot_file_size_limit_allows_boundary_size() {
        ensure_snapshot_file_size("catalog/tables.parquet", MAX_SNAPSHOT_FILE_BYTES)
            .expect("boundary size should be allowed");
    }
}
