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
use arco_catalog::idempotency::{
    CatalogOperation, IdempotencyCheck, IdempotencyStore, IdempotencyStoreImpl,
    calculate_retry_after, canonical_request_hash, check_idempotency,
};

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
#[allow(clippy::too_many_lines)]
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

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let request_json = serde_json::json!({
        "name": req.name,
        "description": req.description
    });
    let request_hash = canonical_request_hash(&request_json)
        .map_err(|e| ApiError::internal(format!("Failed to compute request hash: {e}")))?;

    let storage_arc = Arc::new(storage.clone());
    let idempotency_store = IdempotencyStoreImpl::new(storage_arc);
    let idempotency_check = check_idempotency(
        &idempotency_store,
        ctx.idempotency_key.as_deref(),
        CatalogOperation::CreateNamespace,
        &request_hash,
        state.config.idempotency_stale_timeout(),
    )
    .await
    .map_err(ApiError::from)?;

    let (marker, marker_version) = match idempotency_check {
        IdempotencyCheck::NoKey => (None, None),
        IdempotencyCheck::Proceed { marker, version } => (Some(marker), Some(version)),
        IdempotencyCheck::Replay {
            entity_id,
            entity_name,
        } => {
            let reader = arco_catalog::CatalogReader::new(storage);
            let ns = reader
                .get_namespace(&entity_name)
                .await
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError::internal("Cached namespace not found"))?;
            let response = NamespaceResponse {
                id: entity_id,
                name: ns.name,
                description: ns.description,
                created_at: format_timestamp(ns.created_at),
                updated_at: format_timestamp(ns.updated_at),
            };
            return Ok((StatusCode::CREATED, Json(response)));
        }
        IdempotencyCheck::Conflict => {
            return Err(ApiError::conflict(
                "Idempotency-Key already used with different request body",
            ));
        }
        IdempotencyCheck::PreviousFailed {
            http_status,
            message,
        } => {
            return Err(ApiError::from_status_and_message(http_status, message));
        }
        IdempotencyCheck::InProgress { started_at } => {
            let retry_after =
                calculate_retry_after(started_at, state.config.idempotency_stale_timeout());
            return Err(ApiError::conflict_in_progress(retry_after));
        }
    };

    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    writer.initialize().await.map_err(ApiError::from)?;

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    let create_result = writer
        .create_namespace(&req.name, req.description.as_deref(), options)
        .await;

    if let (Some(marker), Some(version)) = (&marker, &marker_version) {
        match &create_result {
            Ok(ns) => {
                let finalized = marker
                    .clone()
                    .finalize_committed(ns.id.clone(), ns.name.clone());
                if let Err(e) = idempotency_store.finalize(&finalized, version).await {
                    tracing::warn!(
                        idempotency_key = %marker.idempotency_key,
                        operation = ?marker.operation,
                        error = %e,
                        "Failed to finalize idempotency marker as committed"
                    );
                }
            }
            Err(e) => {
                if let Some(status) = e.http_status_code() {
                    if (400..500).contains(&status) {
                        let finalized = marker.clone().finalize_failed(status, e.to_string());
                        if let Err(fin_err) = idempotency_store.finalize(&finalized, version).await
                        {
                            tracing::warn!(
                                idempotency_key = %marker.idempotency_key,
                                operation = ?marker.operation,
                                error = %fin_err,
                                "Failed to finalize idempotency marker as failed"
                            );
                        }
                    }
                }
            }
        }
    }

    create_result.map_err(ApiError::from)?;

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
