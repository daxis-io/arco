//! Schema endpoints for the UC facade.

use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::routing::{get, post};

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::state::UnityCatalogState;

/// Schema route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/schemas", post(create_schema).get(list_schemas))
        .route(
            "/schemas/:full_name",
            get(get_schema).patch(update_schema).delete(delete_schema),
        )
}

/// `POST /schemas` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/schemas",
    tag = "Schemas",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn create_schema(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `GET /schemas` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/schemas",
    tag = "Schemas",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn list_schemas(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `GET /schemas/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/schemas/{full_name}",
    tag = "Schemas",
    params(
        ("full_name" = String, Path, description = "Schema full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn get_schema(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `PATCH /schemas/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    patch,
    path = "/schemas/{full_name}",
    tag = "Schemas",
    params(
        ("full_name" = String, Path, description = "Schema full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn update_schema(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `DELETE /schemas/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    delete,
    path = "/schemas/{full_name}",
    tag = "Schemas",
    params(
        ("full_name" = String, Path, description = "Schema full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn delete_schema(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}
