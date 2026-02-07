//! Unity Catalog facade router setup.

use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::extract::OriginalUri;
use axum::http::StatusCode;
use axum::middleware;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::context::context_middleware;
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

async fn not_found(uri: OriginalUri) -> UnityCatalogError {
    UnityCatalogError::NotFound {
        message: format!("not found: {}", uri.0.path()),
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
    use axum::http::{Request, StatusCode};
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
    async fn known_scope_path_returns_not_implemented() {
        let storage = Arc::new(MemoryBackend::new());
        let state = UnityCatalogState::new(storage);
        let app = unity_catalog_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/catalogs")
            .header("X-Tenant-Id", "acme")
            .header("X-Workspace-Id", "analytics")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn unknown_path_returns_not_found() {
        let storage = Arc::new(MemoryBackend::new());
        let state = UnityCatalogState::new(storage);
        let app = unity_catalog_router(state);

        let request = Request::builder()
            .method("GET")
            .uri("/not-a-real-uc-route")
            .header("X-Tenant-Id", "acme")
            .header("X-Workspace-Id", "analytics")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
