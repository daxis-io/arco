//! Table endpoints for the UC facade.

use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::routing::{get, post};

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::state::UnityCatalogState;

/// Table route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/tables", post(create_table).get(list_tables))
        .route("/tables/:full_name", get(get_table).delete(delete_table))
}

/// `POST /tables` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/tables",
    tag = "Tables",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn create_table(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `GET /tables` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/tables",
    tag = "Tables",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn list_tables(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `GET /tables/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn get_table(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `DELETE /tables/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    delete,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn delete_table(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}
