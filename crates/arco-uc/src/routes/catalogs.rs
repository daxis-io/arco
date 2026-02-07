//! Catalog endpoints for the UC facade.

use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::routing::{get, post};

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::state::UnityCatalogState;

/// Catalog route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/catalogs", post(create_catalog).get(list_catalogs))
        .route(
            "/catalogs/:name",
            get(get_catalog)
                .patch(update_catalog)
                .delete(delete_catalog),
        )
}

/// `POST /catalogs` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/catalogs",
    tag = "Catalogs",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn create_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `GET /catalogs` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/catalogs",
    tag = "Catalogs",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn list_catalogs(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `GET /catalogs/{name}` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn get_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `PATCH /catalogs/{name}` (known UC operation; currently unsupported).
#[utoipa::path(
    patch,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn update_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `DELETE /catalogs/{name}` (known UC operation; currently unsupported).
#[utoipa::path(
    delete,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn delete_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}
