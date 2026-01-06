//! Iceberg REST Catalog router setup.
//!
//! Provides the router builder for mounting Iceberg endpoints.

use axum::Router;
use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::middleware;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::context::context_middleware;
use crate::error::{IcebergErrorDetail, IcebergErrorResponse};
use crate::metrics::metrics_middleware;
use crate::routes;
use crate::state::IcebergState;

/// Creates the Iceberg REST Catalog router.
pub fn iceberg_router(state: IcebergState) -> Router {
    let request_timeout = state.config.request_timeout;
    let concurrency_limit = state.config.concurrency_limit;

    let router = Router::new()
        .route(
            "/openapi.json",
            axum::routing::get(routes::openapi::get_openapi_json),
        )
        .route("/v1/config", axum::routing::get(routes::config::get_config))
        .nest(
            "/v1/:prefix",
            routes::namespaces::routes().merge(routes::tables::routes()),
        )
        .layer(middleware::from_fn(context_middleware))
        .layer(middleware::from_fn(metrics_middleware))
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

async fn handle_timeout_error(
    _err: tower::BoxError,
) -> (StatusCode, axum::Json<IcebergErrorResponse>) {
    let response = IcebergErrorResponse {
        error: IcebergErrorDetail {
            message: "Request timed out".to_string(),
            error_type: "ServiceUnavailableException".to_string(),
            code: 503,
        },
    };
    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::IcebergConfig;
    use arco_core::storage::MemoryBackend;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_router_creation() {
        let storage = Arc::new(MemoryBackend::new());
        let state = IcebergState::new(storage);
        let _router = iceberg_router(state);
    }

    #[test]
    fn test_router_with_timeout_and_concurrency() {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            request_timeout: Some(Duration::from_secs(30)),
            concurrency_limit: Some(100),
            ..Default::default()
        };
        let state = IcebergState::with_config(storage, config);
        let _router = iceberg_router(state);
    }
}
