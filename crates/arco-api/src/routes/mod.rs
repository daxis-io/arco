//! HTTP route handlers.

pub mod browser;
pub mod catalogs;
pub mod control_store;
pub mod delta;
pub mod lineage;
pub mod manifests;
pub mod namespaces;
pub mod orchestration;
pub(crate) mod pagination;
pub mod query;
pub mod query_data;
pub mod tables;
pub mod tasks;
pub mod transactions;

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
        .merge(query_data::routes())
        .merge(delta::routes())
        .merge(orchestration::routes())
        .merge(transactions::routes())
        .merge(manifests::routes())
}

/// Operator-only `/internal` routes (authenticated, default off).
///
/// Mounted by the server only when
/// [`crate::config::Config::control_store_operator_endpoints`] is enabled, so
/// the routes do not exist — and requests 404 — in the default posture.
pub fn internal_operator_routes() -> Router<Arc<AppState>> {
    control_store::routes()
}

/// `/api/v1` task callback routes (task-authenticated).
pub fn api_task_routes() -> Router<Arc<AppState>> {
    tasks::routes()
}
