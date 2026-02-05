//! HTTP route handlers.

pub mod browser;
pub mod catalogs;
pub mod delta;
pub mod lineage;
pub mod manifests;
pub mod namespaces;
pub mod orchestration;
pub mod query;
pub mod tables;
pub mod tasks;

use std::sync::Arc;

use axum::Router;

use crate::server::AppState;

/// `/api/v1` routes (authenticated).
pub fn api_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .merge(catalogs::routes())
        .merge(namespaces::routes())
        .merge(tables::routes())
        .merge(lineage::routes())
        .merge(browser::routes())
        .merge(query::routes())
        .merge(delta::routes())
        .merge(orchestration::routes())
        .merge(manifests::routes())
}

/// `/api/v1` task callback routes (task-authenticated).
pub fn api_task_routes() -> Router<Arc<AppState>> {
    tasks::routes()
}
