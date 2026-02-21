//! Namespace endpoint handlers for Iceberg REST Catalog.
//!
//! Read endpoints (always available):
//! - `GET /v1/{prefix}/namespaces` - List namespaces
//! - `HEAD /v1/{prefix}/namespaces/{namespace}` - Check namespace exists
//! - `GET /v1/{prefix}/namespaces/{namespace}` - Get namespace
//!
//! Write endpoints (require `allow_namespace_crud` config):
//! - `POST /v1/{prefix}/namespaces` - Create namespace
//! - `DELETE /v1/{prefix}/namespaces/{namespace}` - Delete namespace
//! - `POST /v1/{prefix}/namespaces/{namespace}/properties` - Update properties

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tracing::instrument;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogReader, CatalogWriter};

use crate::context::IcebergRequestContext;
use crate::error::{IcebergError, IcebergResult};
use crate::routes::utils::{ensure_prefix, join_namespace, paginate, parse_namespace};
use crate::state::{IcebergConfig, IcebergState};
use crate::types::{
    CreateNamespaceRequest, CreateNamespaceResponse, GetNamespaceResponse, ListNamespacesQuery,
    ListNamespacesResponse, NamespaceIdent, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
};

/// Creates namespace routes.
pub fn routes() -> Router<IcebergState> {
    Router::new()
        .route("/namespaces", get(list_namespaces).post(create_namespace))
        .route(
            "/namespaces/:namespace",
            get(get_namespace)
                .head(head_namespace)
                .delete(delete_namespace),
        )
        .route(
            "/namespaces/:namespace/properties",
            axum::routing::post(update_namespace_properties),
        )
}

fn ensure_namespace_crud_enabled(config: &IcebergConfig) -> IcebergResult<()> {
    if !config.allow_namespace_crud {
        return Err(IcebergError::unsupported_operation(
            "Namespace mutations are not enabled",
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub(crate) struct PrefixPath {
    pub(crate) prefix: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct NamespacePath {
    pub(crate) prefix: String,
    pub(crate) namespace: String,
}

/// List namespaces.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ListNamespacesQuery
    ),
    responses(
        (status = 200, description = "Namespaces listed", body = ListNamespacesResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Namespaces"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix))]
async fn list_namespaces(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<PrefixPath>,
    Query(query): Query<ListNamespacesQuery>,
) -> IcebergResult<Json<ListNamespacesResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage);

    let all_namespaces: Vec<NamespaceIdent> = reader
        .list_namespaces()
        .await
        .map_err(IcebergError::from)?
        .into_iter()
        .map(|ns| parse_namespace(&ns.name, &separator))
        .collect::<IcebergResult<Vec<_>>>()?;

    let parent_filter = query
        .parent
        .as_deref()
        .filter(|p| !p.is_empty())
        .map(|p| parse_namespace(p, &separator))
        .transpose()?;

    let mut namespaces: Vec<NamespaceIdent> = if let Some(ref parent_ident) = parent_filter {
        let parent_name = join_namespace(parent_ident, &separator)?;
        let parent_exists = reader
            .get_namespace(&parent_name)
            .await
            .map_err(IcebergError::from)?
            .is_some();

        if !parent_exists {
            return Err(IcebergError::namespace_not_found(&parent_name));
        }

        all_namespaces
            .into_iter()
            .filter(|ident| {
                ident.len() == parent_ident.len() + 1 && ident.starts_with(parent_ident)
            })
            .collect()
    } else {
        all_namespaces
            .into_iter()
            .filter(|ident| ident.len() == 1)
            .collect()
    };

    namespaces.sort();
    let (page, next) = paginate(namespaces, query.page_token, query.page_size)?;

    Ok(Json(ListNamespacesResponse {
        namespaces: page,
        next_page_token: next,
    }))
}

/// Create namespace.
#[utoipa::path(
    post,
    path = "/v1/{prefix}/namespaces",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    request_body = CreateNamespaceRequest,
    responses(
        (status = 200, description = "Namespace created", body = CreateNamespaceResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Namespace already exists", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Namespaces"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix))]
pub(crate) async fn create_namespace(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<PrefixPath>,
    Json(req): Json<CreateNamespaceRequest>,
) -> IcebergResult<Json<CreateNamespaceResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_namespace_crud_enabled(&state.config)?;

    let separator = state.config.namespace_separator_decoded();
    let namespace_name = join_namespace(&req.namespace, &separator)?;
    let description = req.properties.get("comment").cloned();

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let compactor = state.create_compactor(&storage)?;
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    writer.initialize().await.map_err(IcebergError::from)?;

    let mut options = WriteOptions::default()
        .with_actor(format!("iceberg-api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(ref key) = ctx.idempotency_key {
        options = options.with_idempotency_key(key);
    }

    writer
        .create_namespace(&namespace_name, description.as_deref(), options)
        .await
        .map_err(IcebergError::from)?;

    let reader = CatalogReader::new(storage);
    let ns = reader
        .get_namespace(&namespace_name)
        .await
        .map_err(IcebergError::from)?
        .ok_or_else(|| IcebergError::Internal {
            message: "Namespace created but not found".to_string(),
        })?;

    let mut properties = HashMap::from([("arco.id".to_string(), ns.id)]);
    if let Some(desc) = ns.description {
        properties.insert("comment".to_string(), desc);
    }

    Ok(Json(CreateNamespaceResponse {
        namespace: req.namespace,
        properties,
    }))
}

/// Get namespace.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces/{namespace}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 200, description = "Namespace found", body = GetNamespaceResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Namespaces"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
async fn get_namespace(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespacePath>,
) -> IcebergResult<Json<GetNamespaceResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage);

    let ns = reader
        .get_namespace(&namespace_name)
        .await
        .map_err(IcebergError::from)?
        .ok_or_else(|| IcebergError::namespace_not_found(&namespace_name))?;

    let mut properties = HashMap::from([("arco.id".to_string(), ns.id.clone())]);
    if let Some(description) = ns.description {
        properties.insert("comment".to_string(), description);
    }

    Ok(Json(GetNamespaceResponse::new(namespace_ident, properties)))
}

/// Head namespace.
#[utoipa::path(
    head,
    path = "/v1/{prefix}/namespaces/{namespace}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 204, description = "Namespace exists"),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Namespaces"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
async fn head_namespace(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespacePath>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage);

    let exists = reader
        .get_namespace(&namespace_name)
        .await
        .map_err(IcebergError::from)?
        .is_some();

    let status = if exists {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    };

    Response::builder()
        .status(status)
        .body(axum::body::Body::empty())
        .map_err(|e| IcebergError::Internal {
            message: format!("Failed to build response: {e}"),
        })
}

/// Delete namespace.
#[utoipa::path(
    delete,
    path = "/v1/{prefix}/namespaces/{namespace}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    responses(
        (status = 204, description = "Namespace deleted"),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Namespace not empty", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Namespaces"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
pub(crate) async fn delete_namespace(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespacePath>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_namespace_crud_enabled(&state.config)?;

    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let compactor = state.create_compactor(&storage)?;
    let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);

    let mut options = WriteOptions::default()
        .with_actor(format!("iceberg-api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(ref key) = ctx.idempotency_key {
        options = options.with_idempotency_key(key);
    }

    writer
        .delete_namespace(&namespace_name, options)
        .await
        .map_err(IcebergError::from)?;

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(axum::body::Body::empty())
        .map_err(|e| IcebergError::Internal {
            message: format!("Failed to build response: {e}"),
        })
}

#[utoipa::path(
    post,
    path = "/v1/{prefix}/namespaces/{namespace}/properties",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    request_body = UpdateNamespacePropertiesRequest,
    responses(
        (status = 200, description = "Properties updated", body = UpdateNamespacePropertiesResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation", body = crate::error::IcebergErrorResponse),
        (status = 422, description = "Unprocessable entity", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Namespaces"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
pub(crate) async fn update_namespace_properties(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespacePath>,
    Json(req): Json<UpdateNamespacePropertiesRequest>,
) -> IcebergResult<Json<UpdateNamespacePropertiesResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_namespace_crud_enabled(&state.config)?;

    let overlap: Vec<_> = req
        .removals
        .iter()
        .filter(|key| req.updates.contains_key(*key))
        .cloned()
        .collect();
    if !overlap.is_empty() {
        return Err(IcebergError::property_overlap(&overlap));
    }

    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage.clone());

    let ns = reader
        .get_namespace(&namespace_name)
        .await
        .map_err(IcebergError::from)?
        .ok_or_else(|| IcebergError::namespace_not_found(&namespace_name))?;

    let current_comment = ns.description.as_deref();

    let mut updated = Vec::new();
    let mut removed = Vec::new();
    let mut missing = Vec::new();

    let remove_comment = req.removals.contains(&"comment".to_string());
    let new_comment = req.updates.get("comment");

    for key in &req.removals {
        if key == "comment" {
            if current_comment.is_some() {
                removed.push(key.clone());
            } else {
                missing.push(key.clone());
            }
        } else {
            missing.push(key.clone());
        }
    }

    if let Some(comment) = new_comment {
        if current_comment != Some(comment.as_str()) {
            updated.push("comment".to_string());
        }
    }

    for key in req.updates.keys() {
        if key != "comment" {
            missing.push(key.clone());
        }
    }

    let needs_persist = !removed.is_empty() || updated.contains(&"comment".to_string());

    if needs_persist {
        let new_description = if remove_comment {
            None
        } else if let Some(comment) = new_comment {
            Some(comment.as_str())
        } else {
            current_comment
        };

        let compactor = state.create_compactor(&storage)?;
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        let mut opts = WriteOptions::default()
            .with_actor(format!("iceberg-api:{}", ctx.tenant))
            .with_request_id(&ctx.request_id);
        if let Some(ref key) = ctx.idempotency_key {
            opts = opts.with_idempotency_key(key);
        }
        writer
            .update_namespace(&namespace_name, new_description, opts)
            .await
            .map_err(IcebergError::from)?;
    }

    Ok(Json(UpdateNamespacePropertiesResponse {
        updated,
        removed,
        missing: if missing.is_empty() {
            None
        } else {
            Some(missing)
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_catalog::CatalogWriter;
    use arco_catalog::Tier1Compactor;
    use arco_catalog::write_options::WriteOptions;
    use arco_core::ScopedStorage;
    use arco_core::storage::MemoryBackend;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::state::{IcebergConfig, Tier1CompactorFactory};

    fn build_state() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        IcebergState::new(storage)
    }

    fn build_state_with_crud_enabled() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            allow_namespace_crud: true,
            ..Default::default()
        };
        IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(Tier1CompactorFactory))
    }

    async fn seed_namespace(state: &IcebergState, namespace: &str) {
        seed_namespace_with_description(state, namespace, None).await;
    }

    async fn seed_namespace_with_description(
        state: &IcebergState,
        namespace: &str,
        description: Option<&str>,
    ) {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace(namespace, description, WriteOptions::default())
            .await
            .expect("create namespace");
    }

    async fn initialize_empty_catalog(state: &IcebergState) {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
    }

    fn app(state: IcebergState) -> Router {
        Router::new()
            .nest("/v1/:prefix", routes())
            .layer(axum::middleware::from_fn(
                crate::context::context_middleware,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_namespaces_from_catalog() {
        let state = build_state();
        seed_namespace(&state, "sales").await;
        seed_namespace(&state, "finance").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("deserialize list response");
        assert_eq!(
            payload.namespaces,
            vec![vec!["finance".to_string()], vec!["sales".to_string()],]
        );
        assert!(payload.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_namespaces_pagination_with_page_token_and_size() {
        let state = build_state();
        seed_namespace(&state, "a").await;
        seed_namespace(&state, "b").await;
        seed_namespace(&state, "c").await;

        let router = app(state.clone());
        let first = router
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageSize=2")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(first.status(), StatusCode::OK);
        let first_body = axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .expect("body");
        let first_payload: ListNamespacesResponse =
            serde_json::from_slice(&first_body).expect("deserialize first page");
        assert_eq!(
            first_payload.namespaces,
            vec![vec!["a".to_string()], vec!["b".to_string()]]
        );
        assert_eq!(first_payload.next_page_token, Some("2".to_string()));

        let router = app(state);
        let second = router
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageToken=2&pageSize=2")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(second.status(), StatusCode::OK);
        let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .expect("body");
        let second_payload: ListNamespacesResponse =
            serde_json::from_slice(&second_body).expect("deserialize second page");
        assert_eq!(second_payload.namespaces, vec![vec!["c".to_string()]]);
        assert!(second_payload.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_namespaces_parent_filtering() {
        let state = build_state();
        seed_namespace(&state, "accounting").await;
        seed_namespace(&state, "accounting\u{1F}tax").await;
        seed_namespace(&state, "accounting\u{1F}payroll").await;
        seed_namespace(&state, "accounting\u{1F}tax\u{1F}paid").await;
        seed_namespace(&state, "sales").await;

        let router = app(state.clone());
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?parent=accounting")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("deserialize list response");
        assert_eq!(
            payload.namespaces,
            vec![
                vec!["accounting".to_string(), "payroll".to_string()],
                vec!["accounting".to_string(), "tax".to_string()],
            ]
        );
        assert!(payload.next_page_token.is_none());

        let router = app(state);
        let nested = router
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?parent=accounting%1Ftax")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(nested.status(), StatusCode::OK);
        let nested_body = axum::body::to_bytes(nested.into_body(), usize::MAX)
            .await
            .expect("body");
        let nested_payload: ListNamespacesResponse =
            serde_json::from_slice(&nested_body).expect("deserialize list response");
        assert_eq!(
            nested_payload.namespaces,
            vec![vec![
                "accounting".to_string(),
                "tax".to_string(),
                "paid".to_string(),
            ]]
        );
        assert!(nested_payload.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_namespaces_parent_not_found_returns_404() {
        let state = build_state();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?parent=missing")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("deserialize error");
        assert_eq!(payload["error"]["type"], "NoSuchNamespaceException");
    }

    #[tokio::test]
    async fn test_list_namespaces_invalid_page_token_returns_400() {
        let state = build_state();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageToken=not-a-number&pageSize=1")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("deserialize error");
        assert_eq!(payload["error"]["type"], "BadRequestException");
        assert_eq!(payload["error"]["message"], "Invalid pageToken");
    }

    #[tokio::test]
    async fn test_list_namespaces_page_token_out_of_range_returns_400() {
        let state = build_state();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageToken=10&pageSize=1")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("deserialize error");
        assert_eq!(payload["error"]["type"], "BadRequestException");
        assert_eq!(payload["error"]["message"], "pageToken out of range");
    }

    #[tokio::test]
    async fn test_list_namespaces_page_size_zero_returns_400() {
        let state = build_state();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageSize=0")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("deserialize error");
        assert_eq!(payload["error"]["type"], "BadRequestException");
        assert_eq!(
            payload["error"]["message"],
            "pageSize must be greater than zero"
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_empty_catalog_returns_empty_list() {
        let state = build_state();
        initialize_empty_catalog(&state).await;
        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("deserialize list response");
        assert!(payload.namespaces.is_empty());
        assert!(payload.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_get_namespace_existing_returns_properties() {
        let state = build_state();
        seed_namespace_with_description(&state, "sales", Some("Sales data")).await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: GetNamespaceResponse =
            serde_json::from_slice(&body).expect("deserialize get namespace response");
        assert_eq!(payload.namespace, vec!["sales".to_string()]);
        assert_eq!(
            payload.properties.get("comment"),
            Some(&"Sales data".to_string())
        );
        assert!(
            payload
                .properties
                .get("arco.id")
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[tokio::test]
    async fn test_get_namespace_non_existent_returns_404() {
        let app = app(build_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/missing")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_namespace_identifier_is_url_decoded() {
        let state = build_state();
        seed_namespace(&state, "sales ops").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales%20ops")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: GetNamespaceResponse =
            serde_json::from_slice(&body).expect("deserialize get namespace response");
        assert_eq!(payload.namespace, vec!["sales ops".to_string()]);
    }

    #[tokio::test]
    async fn test_get_namespace_nested_identifier_decodes_separator() {
        let state = build_state();
        seed_namespace(&state, "accounting\u{1F}tax").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/accounting%1Ftax")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let payload: GetNamespaceResponse =
            serde_json::from_slice(&body).expect("deserialize get namespace response");
        assert_eq!(
            payload.namespace,
            vec!["accounting".to_string(), "tax".to_string()]
        );
    }

    #[tokio::test]
    async fn test_head_namespace_not_found() {
        let app = app(build_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/v1/arco/namespaces/missing")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_head_namespace_exists_returns_no_content_without_body() {
        let state = build_state();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/v1/arco/namespaces/sales")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let state = build_state_with_crud_enabled();
        let app = app(state);

        let req_body = serde_json::json!({
            "namespace": ["sales"],
            "properties": {"comment": "Sales data"}
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("body");
        let resp: CreateNamespaceResponse = serde_json::from_slice(&body).expect("deserialize");
        assert_eq!(resp.namespace, vec!["sales".to_string()]);
        assert!(resp.properties.contains_key("arco.id"));
        assert_eq!(
            resp.properties.get("comment"),
            Some(&"Sales data".to_string())
        );
    }

    #[tokio::test]
    async fn test_create_namespace_disabled() {
        let state = build_state();
        let app = app(state);

        let req_body = serde_json::json!({"namespace": ["sales"]});

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn test_create_namespace_already_exists() {
        let state = build_state_with_crud_enabled();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let req_body = serde_json::json!({"namespace": ["sales"]});

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_delete_namespace() {
        let state = build_state_with_crud_enabled();
        seed_namespace(&state, "to_delete").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/to_delete")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_delete_namespace_not_found() {
        let state = build_state_with_crud_enabled();
        let app = app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/missing")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_namespace_disabled() {
        let state = build_state();
        let app = app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/sales")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn test_update_properties_overlap_returns_422() {
        let state = build_state_with_crud_enabled();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let req_body = serde_json::json!({
            "removals": ["comment"],
            "updates": {"comment": "new value"}
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/properties")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["type"], "UnprocessableEntityException");
    }

    #[tokio::test]
    async fn test_update_properties_persists_comment() {
        let state = build_state_with_crud_enabled();
        seed_namespace_with_description(&state, "sales", Some("Original comment")).await;

        let router = app(state.clone());
        let req_body = serde_json::json!({
            "removals": [],
            "updates": {"comment": "Updated comment"}
        });

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/properties")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["updated"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("comment"))
        );

        let router2 = app(state);
        let get_response = router2
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(get_response.status(), StatusCode::OK);

        let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let get_json: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
        assert_eq!(get_json["properties"]["comment"], "Updated comment");
    }

    #[tokio::test]
    async fn test_update_properties_unknown_keys_in_missing() {
        let state = build_state_with_crud_enabled();
        seed_namespace(&state, "sales").await;

        let app = app(state);
        let req_body = serde_json::json!({
            "removals": ["unknown_removal"],
            "updates": {"unknown_update": "value", "another_unknown": "value2"}
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/properties")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let updated = json["updated"].as_array().unwrap();
        assert!(updated.is_empty(), "unknown keys should not be in updated");

        let missing = json["missing"].as_array().unwrap();
        assert!(missing.contains(&serde_json::json!("unknown_removal")));
        assert!(missing.contains(&serde_json::json!("unknown_update")));
        assert!(missing.contains(&serde_json::json!("another_unknown")));
    }
}
