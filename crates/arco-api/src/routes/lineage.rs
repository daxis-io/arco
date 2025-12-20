//! Lineage API routes.
//!
//! Provides operations for tracking data lineage (edges between tables).
//!
//! ## Routes
//!
//! - `POST /lineage/edges` - Add lineage edge(s)
//! - `GET  /lineage/{table_id}` - Get lineage for a table

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::error::ApiErrorBody;
use crate::server::AppState;
use arco_catalog::Tier1Compactor;

/// Request to add lineage edge(s).
#[derive(Debug, Deserialize, ToSchema)]
pub struct AddEdgesRequest {
    /// List of edges to add.
    pub edges: Vec<EdgeDefinition>,
}

/// Edge definition for lineage.
#[derive(Debug, Deserialize, ToSchema)]
pub struct EdgeDefinition {
    /// Source table ID (upstream).
    pub source_id: String,
    /// Target table ID (downstream).
    pub target_id: String,
    /// Edge type (e.g., `derives_from`, `copies`, `transforms`).
    #[serde(default = "default_edge_type")]
    pub edge_type: String,
    /// Optional run ID that created this edge.
    pub run_id: Option<String>,
}

fn default_edge_type() -> String {
    "derives_from".to_string()
}

/// Lineage edge response.
#[derive(Debug, Serialize, ToSchema)]
pub struct EdgeResponse {
    /// Edge ID.
    pub id: String,
    /// Source table ID.
    pub source_id: String,
    /// Target table ID.
    pub target_id: String,
    /// Edge type.
    pub edge_type: String,
    /// Optional run ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
}

/// Add edges response.
#[derive(Debug, Serialize, ToSchema)]
pub struct AddEdgesResponse {
    /// Number of edges added.
    pub added: usize,
}

/// Get lineage response.
#[derive(Debug, Serialize, ToSchema)]
pub struct LineageResponse {
    /// Table ID queried.
    pub table_id: String,
    /// Upstream edges (sources that feed into this table).
    pub upstream: Vec<EdgeResponse>,
    /// Downstream edges (tables that depend on this table).
    pub downstream: Vec<EdgeResponse>,
}

/// Creates lineage routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/lineage/edges", post(add_edges))
        .route("/lineage/:table_id", get(get_lineage))
}

/// Add lineage edge(s).
///
/// POST /api/v1/lineage/edges
#[utoipa::path(
    post,
    path = "/api/v1/lineage/edges",
    tag = "lineage",
    request_body = AddEdgesRequest,
    responses(
        (status = 201, description = "Edges added", body = AddEdgesResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn add_edges(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddEdgesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        edge_count = req.edges.len(),
        "Adding lineage edges"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage).with_sync_compactor(compactor);

    // Ensure initialized
    writer
        .initialize()
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    // Convert edges (generate IDs and timestamps)
    let now = chrono::Utc::now().timestamp_millis();
    let edges: Vec<arco_catalog::LineageEdge> = req
        .edges
        .into_iter()
        .map(|e| arco_catalog::LineageEdge {
            id: Ulid::new().to_string(),
            source_id: e.source_id,
            target_id: e.target_id,
            edge_type: e.edge_type,
            run_id: e.run_id,
            created_at: now,
        })
        .collect();

    let count = edges.len();

    writer
        .add_lineage_edges(edges, options)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok((StatusCode::CREATED, Json(AddEdgesResponse { added: count })))
}

/// Get lineage for a table.
///
/// `GET /api/v1/lineage/{table_id}`
#[utoipa::path(
    get,
    path = "/api/v1/lineage/{table_id}",
    tag = "lineage",
    params(
        ("table_id" = String, Path, description = "Table ID")
    ),
    responses(
        (status = 200, description = "Lineage graph", body = LineageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_lineage(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(table_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        table_id = %table_id,
        "Getting lineage"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    // Get lineage graph for the table
    let graph = reader
        .get_lineage(&table_id)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Convert upstream edges
    let upstream = graph
        .upstream
        .into_iter()
        .map(|e| EdgeResponse {
            id: e.id,
            source_id: e.source_id,
            target_id: e.target_id,
            edge_type: e.edge_type,
            run_id: e.run_id,
            created_at: format_timestamp(e.created_at),
        })
        .collect();

    // Convert downstream edges
    let downstream = graph
        .downstream
        .into_iter()
        .map(|e| EdgeResponse {
            id: e.id,
            source_id: e.source_id,
            target_id: e.target_id,
            edge_type: e.edge_type,
            run_id: e.run_id,
            created_at: format_timestamp(e.created_at),
        })
        .collect();

    Ok(Json(LineageResponse {
        table_id,
        upstream,
        downstream,
    }))
}

/// Format a millisecond timestamp as ISO 8601.
fn format_timestamp(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .map_or_else(|| millis.to_string(), |dt| dt.to_rfc3339())
}
