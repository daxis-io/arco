//! Iceberg REST Catalog router setup.
//!
//! Provides the router builder for mounting Iceberg endpoints.

use axum::Router;
use axum::middleware;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::context::context_middleware;
use crate::metrics::metrics_middleware;
use crate::routes;
use crate::state::IcebergState;

/// Creates the Iceberg REST Catalog router.
///
/// The router is designed to be nested under `/iceberg` in the main API:
///
/// ```rust,ignore
/// use arco_iceberg::router::iceberg_router;
///
/// let app = axum::Router::new()
///     .nest("/iceberg", iceberg_router(state));
/// ```
///
/// # Endpoints
///
/// Phase A (read-only):
/// - `GET /v1/config` - Catalog configuration
/// - `GET /v1/{prefix}/namespaces` - List namespaces
/// - `HEAD /v1/{prefix}/namespaces/{namespace}` - Check namespace exists
/// - `GET /v1/{prefix}/namespaces/{namespace}` - Get namespace
/// - `GET /v1/{prefix}/namespaces/{namespace}/tables` - List tables
/// - `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Check table exists
/// - `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Load table
/// - `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` - Get credentials
pub fn iceberg_router(state: IcebergState) -> Router {
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

    // Apply optional concurrency limit
    // Note: request_timeout is applied at the outer API layer, not here
    let router = match state.config.concurrency_limit {
        Some(limit) => router.layer(ConcurrencyLimitLayer::new(limit)),
        None => router,
    };

    router.with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use std::sync::Arc;

    #[test]
    fn test_router_creation() {
        let storage = Arc::new(MemoryBackend::new());
        let state = IcebergState::new(storage);
        let _router = iceberg_router(state);
        // Router should be created without panicking
    }
}
