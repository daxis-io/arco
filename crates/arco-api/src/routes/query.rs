//! Query API routes.
//!
//! Provides a minimal SQL query endpoint backed by DataFusion.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arco_catalog::CatalogReader;
use arco_core::CatalogDomain;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const JSON_CONTENT_TYPE: &str = "application/json";

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
        return Err(ApiError::not_found("No snapshot tables available for query"));
    }

    let df = session.sql(sql).await.map_err(map_datafusion_error)?;
    let schema = df.schema().clone();
    let batches = df.collect().await.map_err(map_datafusion_error)?;

    if wants_json(&params, &headers)? {
        let payload = batches_to_json(&batches)?;
        let mut response = Response::new(Body::from(payload));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
        return Ok(response);
    }

    let payload = batches_to_arrow(&schema, &batches)?;
    let mut response = Response::new(Body::from(payload));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(ARROW_STREAM_CONTENT_TYPE),
    );
    Ok(response)
}

fn validate_query(sql: &str) -> Result<(), ApiError> {
    let normalized = sql.trim_start().to_ascii_lowercase();
    if normalized.starts_with("select") || normalized.starts_with("with") {
        return Ok(());
    }
    Err(ApiError::bad_request(
        "Only SELECT/CTE queries are supported",
    ))
}

fn wants_json(params: &QueryParams, headers: &HeaderMap) -> Result<bool, ApiError> {
    if let Some(format) = params.format.as_deref() {
        let normalized = format.trim().to_ascii_lowercase();
        return match normalized.as_str() {
            "json" => Ok(true),
            "arrow" => Ok(false),
            _ => Err(ApiError::bad_request(
                "format must be one of: json, arrow",
            )),
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

        let bytes = storage.get_raw(&path).await.map_err(ApiError::from)?;
        let table = parquet_bytes_to_mem_table(bytes)?;
        session
            .register_table(
                TableReference::partial(domain.as_str(), *table_name),
                table,
            )
            .map_err(map_datafusion_error)?;
        registered += 1;
    }

    Ok(registered)
}

async fn ensure_schema(session: &SessionContext, schema: &str) -> Result<(), ApiError> {
    let statement = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
    let df = session.sql(&statement).await.map_err(map_datafusion_error)?;
    df.collect().await.map_err(map_datafusion_error)?;
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

fn map_datafusion_error(err: DataFusionError) -> ApiError {
    match err {
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(_, _) => ApiError::bad_request(err.to_string()),
        _ => ApiError::internal(format!("query failed: {err}")),
    }
}
