//! Namespace API routes.
//!
//! Provides CRUD operations for namespaces within a tenant/workspace.
//!
//! ## Routes
//!
//! - `POST   /namespaces` - Create a namespace
//! - `GET    /namespaces` - List all namespaces
//! - `GET    /namespaces/{name}` - Get namespace by name
//! - `DELETE /namespaces/{name}` - Delete a namespace

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::error::ApiErrorBody;
use crate::server::AppState;
use arco_catalog::Tier1Compactor;

/// Request to create a namespace.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateNamespaceRequest {
    /// Namespace name (must be unique within workspace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

/// Namespace response.
#[derive(Debug, Serialize, ToSchema)]
pub struct NamespaceResponse {
    /// Namespace ID.
    pub id: String,
    /// Namespace name.
    pub name: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
    /// Last update timestamp (ISO 8601).
    pub updated_at: String,
}

/// List namespaces response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListNamespacesResponse {
    /// List of namespaces.
    pub namespaces: Vec<NamespaceResponse>,
}

/// Creates namespace routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/namespaces", post(create_namespace).get(list_namespaces))
        .route(
            "/namespaces/:name",
            get(get_namespace).delete(delete_namespace),
        )
}

/// Create a namespace.
///
/// POST /api/v1/namespaces
#[utoipa::path(
    post,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    request_body = CreateNamespaceRequest,
    responses(
        (status = 201, description = "Namespace created", body = NamespaceResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 409, description = "Conflict", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn create_namespace(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %req.name,
        "Creating namespace"
    );

    // Get storage backend from state
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    // Create catalog writer and ensure initialized
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    // Initialize if not already (idempotent)
    writer.initialize().await.map_err(ApiError::from)?;

    // Create the namespace
    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    writer
        .create_namespace(&req.name, req.description.as_deref(), options)
        .await
        .map_err(ApiError::from)?;

    // Read back the namespace to return it
    let reader = arco_catalog::CatalogReader::new(storage);
    let ns = reader
        .get_namespace(&req.name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::internal("Namespace created but not found"))?;

    let response = NamespaceResponse {
        id: ns.id,
        name: ns.name,
        description: ns.description,
        created_at: format_timestamp(ns.created_at),
        updated_at: format_timestamp(ns.updated_at),
    };

    Ok((StatusCode::CREATED, Json(response)))
}

/// List all namespaces.
///
/// GET /api/v1/namespaces
#[utoipa::path(
    get,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    responses(
        (status = 200, description = "Namespaces listed", body = ListNamespacesResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_namespaces(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        "Listing namespaces"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    let namespaces = reader
        .list_namespaces()
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|ns| NamespaceResponse {
            id: ns.id,
            name: ns.name,
            description: ns.description,
            created_at: format_timestamp(ns.created_at),
            updated_at: format_timestamp(ns.updated_at),
        })
        .collect();

    Ok(Json(ListNamespacesResponse { namespaces }))
}

/// Get a namespace by name.
///
/// GET /api/v1/namespaces/{name}
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{name}",
    tag = "namespaces",
    params(
        ("name" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 200, description = "Namespace found", body = NamespaceResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_namespace(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %name,
        "Getting namespace"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    let ns = reader
        .get_namespace(&name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found(format!("Namespace not found: {name}")))?;

    let response = NamespaceResponse {
        id: ns.id,
        name: ns.name,
        description: ns.description,
        created_at: format_timestamp(ns.created_at),
        updated_at: format_timestamp(ns.updated_at),
    };

    Ok(Json(response))
}

/// Delete a namespace.
///
/// DELETE /api/v1/namespaces/{name}
#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{name}",
    tag = "namespaces",
    params(
        ("name" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 204, description = "Namespace deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn delete_namespace(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %name,
        "Deleting namespace"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage).with_sync_compactor(compactor);

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    writer
        .delete_namespace(&name, options)
        .await
        .map_err(ApiError::from)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Format a millisecond timestamp as ISO 8601.
fn format_timestamp(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .map_or_else(|| millis.to_string(), |dt| dt.to_rfc3339())
}
