//! Handlers for namespace endpoints.
//!
//! Implements:
//! - `GET /v1/{prefix}/namespaces` - List namespaces
//! - `HEAD /v1/{prefix}/namespaces/{namespace}` - Check namespace exists
//! - `GET /v1/{prefix}/namespaces/{namespace}` - Get namespace

use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tracing::instrument;

use arco_catalog::CatalogReader;

use crate::context::IcebergRequestContext;
use crate::error::{IcebergError, IcebergResult};
use crate::routes::utils::{ensure_prefix, join_namespace, paginate, parse_namespace};
use crate::state::IcebergState;
use crate::types::{
    GetNamespaceResponse, ListNamespacesQuery, ListNamespacesResponse, NamespaceIdent,
};
use std::collections::HashMap;

/// Creates namespace routes.
pub fn routes() -> Router<IcebergState> {
    Router::new()
        .route("/namespaces", get(list_namespaces))
        .route(
            "/namespaces/:namespace",
            get(get_namespace).head(head_namespace),
        )
}

#[derive(Debug, Deserialize)]
struct PrefixPath {
    prefix: String,
}

#[derive(Debug, Deserialize)]
struct NamespacePath {
    prefix: String,
    namespace: String,
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

    let mut namespaces: Vec<NamespaceIdent> = reader
        .list_namespaces()
        .await
        .map_err(IcebergError::from)?
        .into_iter()
        .map(|ns| parse_namespace(&ns.name, &separator))
        .collect::<IcebergResult<Vec<_>>>()?;

    if let Some(parent) = query.parent.as_deref() {
        let parent_ident = parse_namespace(parent, &separator)?;
        namespaces.retain(|ident| {
            ident.len() == parent_ident.len() + 1 && ident.starts_with(&parent_ident)
        });
    }

    namespaces.sort();
    let (page, next) = paginate(namespaces, query.page_token, query.page_size)?;

    Ok(Json(ListNamespacesResponse {
        namespaces: page,
        next_page_token: next,
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

    fn build_state() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        IcebergState::new(storage)
    }

    async fn seed_namespace(state: &IcebergState, namespace: &str) {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace(namespace, None, WriteOptions::default())
            .await
            .expect("create namespace");
    }

    async fn seed_namespace_with_description(
        state: &IcebergState,
        namespace: &str,
        description: &str,
    ) {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace(namespace, Some(description), WriteOptions::default())
            .await
            .expect("create namespace");
    }

    async fn init_catalog(state: &IcebergState) {
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
    async fn test_list_namespaces() {
        let state = build_state();
        seed_namespace(&state, "sales").await;

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
    }

    #[tokio::test]
    async fn test_list_namespaces_empty_returns_empty_list() {
        let state = build_state();
        init_catalog(&state).await;
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
            .expect("body read failed");
        let list: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("json parse failed");

        assert!(list.namespaces.is_empty());
        assert!(list.next_page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_namespaces_pagination() {
        let state = build_state();
        seed_namespace(&state, "beta").await;
        seed_namespace(&state, "alpha").await;
        seed_namespace(&state, "gamma").await;

        let app = app(state);
        let response = app.clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageSize=1")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        let list: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("json parse failed");
        assert_eq!(list.namespaces, vec![vec!["alpha".to_string()]]);
        assert_eq!(list.next_page_token.as_deref(), Some("1"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?pageSize=1&pageToken=1")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        let list: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("json parse failed");
        assert_eq!(list.namespaces, vec![vec!["beta".to_string()]]);
        assert_eq!(list.next_page_token.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn test_list_namespaces_parent_filter() {
        let state = build_state();
        seed_namespace(&state, "sales").await;
        seed_namespace(&state, "sales\u{1F}north").await;
        seed_namespace(&state, "sales\u{1F}south").await;
        seed_namespace(&state, "sales\u{1F}north\u{1F}east").await;
        seed_namespace(&state, "marketing").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces?parent=sales")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        let list: ListNamespacesResponse =
            serde_json::from_slice(&body).expect("json parse failed");

        assert_eq!(
            list.namespaces,
            vec![
                vec!["sales".to_string(), "north".to_string()],
                vec!["sales".to_string(), "south".to_string()],
            ]
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
    }

    #[tokio::test]
    async fn test_get_namespace_returns_properties() {
        let state = build_state();
        seed_namespace_with_description(&state, "sales", "Sales data").await;

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
            .expect("body read failed");
        let response: GetNamespaceResponse =
            serde_json::from_slice(&body).expect("json parse failed");

        assert!(response.properties.contains_key("arco.id"));
        assert_eq!(
            response.properties.get("comment"),
            Some(&"Sales data".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_namespace_missing_returns_404() {
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
    async fn test_get_namespace_url_decoding() {
        let state = build_state();
        seed_namespace(&state, "sales team").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales%20team")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_namespace_separator_decoding() {
        let state = build_state();
        seed_namespace(&state, "sales\u{1F}north").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales%1Fnorth")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_head_namespace_exists_returns_no_content() {
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
    }

    #[tokio::test]
    async fn test_head_namespace_returns_empty_body() {
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

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        assert!(body.is_empty());
    }
}
