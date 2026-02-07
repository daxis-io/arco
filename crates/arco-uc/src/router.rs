//! Unity Catalog facade router setup.

use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::http::StatusCode;
use axum::middleware;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::context::context_middleware;
use crate::contract::is_known_operation;
use crate::error::{UnityCatalogError, UnityCatalogErrorDetail, UnityCatalogErrorResponse};
use crate::routes;
use crate::state::UnityCatalogState;

/// Creates the Unity Catalog facade router.
pub fn unity_catalog_router(state: UnityCatalogState) -> Router {
    let request_timeout = state.config.request_timeout;
    let concurrency_limit = state.config.concurrency_limit;

    let router = Router::new()
        .merge(routes::catalogs::routes())
        .merge(routes::schemas::routes())
        .merge(routes::tables::routes())
        .merge(routes::permissions::routes())
        .merge(routes::credentials::routes())
        .merge(routes::delta_commits::routes())
        .route(
            "/openapi.json",
            axum::routing::get(routes::openapi::get_openapi_json),
        )
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(middleware::from_fn(context_middleware))
        .layer(TraceLayer::new_for_http());

    let router = match concurrency_limit {
        Some(limit) => router.layer(ConcurrencyLimitLayer::new(limit)),
        None => router,
    };

    let router = match request_timeout {
        Some(timeout) => router.layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_timeout_error))
                .layer(TimeoutLayer::new(timeout)),
        ),
        None => router,
    };

    router.with_state(state)
}

async fn not_found(method: Method, uri: OriginalUri) -> UnityCatalogError {
    let path = uri.0.path();
    if is_known_operation(&method, path) {
        UnityCatalogError::NotImplemented {
            message: format!(
                "{method} {path} is defined by Unity Catalog OpenAPI but not supported in this deployment"
            ),
        }
    } else {
        UnityCatalogError::NotFound {
            message: format!("not found: {path}"),
        }
    }
}

async fn method_not_allowed(method: Method, uri: OriginalUri) -> UnityCatalogError {
    let path = uri.0.path();
    if is_known_operation(&method, path) {
        UnityCatalogError::NotImplemented {
            message: format!(
                "{method} {path} is defined by Unity Catalog OpenAPI but not supported in this deployment"
            ),
        }
    } else {
        UnityCatalogError::NotFound {
            message: format!("not found: {path}"),
        }
    }
}

async fn handle_timeout_error(
    _err: tower::BoxError,
) -> (StatusCode, axum::Json<UnityCatalogErrorResponse>) {
    let response = UnityCatalogErrorResponse {
        error: UnityCatalogErrorDetail {
            error_code: "SERVICE_UNAVAILABLE".to_string(),
            message: "Request timed out".to_string(),
        },
    };
    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::UnityCatalogConfig;
    use arco_core::storage::MemoryBackend;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    #[test]
    fn test_router_creation() {
        let storage = Arc::new(MemoryBackend::new());
        let state = UnityCatalogState::new(storage);
        let _router = unity_catalog_router(state);
    }

    #[test]
    fn test_router_with_timeout_and_concurrency() {
        let storage = Arc::new(MemoryBackend::new());
        let config = UnityCatalogConfig {
            request_timeout: Some(Duration::from_secs(30)),
            concurrency_limit: Some(100),
        };
        let state = UnityCatalogState::with_config(storage, config);
        let _router = unity_catalog_router(state);
    }

    #[tokio::test]
    async fn test_fallback_known_operation_returns_not_implemented() {
        let storage = Arc::new(MemoryBackend::new());
        let state = UnityCatalogState::new(storage);
        let app = unity_catalog_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/temporary-volume-credentials")
                    .header("X-Tenant-Id", "tenant1")
                    .header("X-Workspace-Id", "workspace1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn test_fallback_unknown_operation_returns_not_found() {
        let storage = Arc::new(MemoryBackend::new());
        let state = UnityCatalogState::new(storage);
        let app = unity_catalog_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/this/path/does/not/exist")
                    .header("X-Tenant-Id", "tenant1")
                    .header("X-Workspace-Id", "workspace1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
