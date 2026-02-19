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
        .route(
            "/openapi.json",
            axum::routing::get(routes::openapi::get_openapi_json),
        )
        .route(
            "/catalogs",
            axum::routing::get(routes::catalogs::get_catalogs)
                .post(routes::catalogs::post_catalogs),
        )
        .route(
            "/schemas",
            axum::routing::get(routes::schemas::get_schemas).post(routes::schemas::post_schemas),
        )
        .route(
            "/tables",
            axum::routing::get(routes::tables::get_tables).post(routes::tables::post_tables),
        )
        .route(
            "/delta/preview/commits",
            axum::routing::get(routes::delta_commits::get_delta_preview_commits)
                .post(routes::delta_commits::post_delta_preview_commits),
        )
        .route(
            "/temporary-table-credentials",
            axum::routing::post(routes::credentials::post_temporary_table_credentials),
        )
        .route(
            "/temporary-path-credentials",
            axum::routing::post(routes::credentials::post_temporary_path_credentials),
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
    use std::sync::Arc;
    use std::time::Duration;

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
}
