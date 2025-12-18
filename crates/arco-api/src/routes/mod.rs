//! HTTP route handlers.

pub mod browser;
pub mod lineage;
pub mod namespaces;
pub mod tables;

use std::sync::Arc;

use axum::Router;

use crate::server::AppState;

/// `/api/v1` routes (authenticated).
pub fn api_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .merge(namespaces::routes())
        .merge(tables::routes())
        .merge(lineage::routes())
        .merge(browser::routes())
}
