//! Catalog endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::{get, post};

use serde_json::json;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::error::UnityCatalogResult;
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
    super::common::known_but_unsupported(&method, &uri)
}

/// `GET /catalogs` (Scope A).
///
/// # Errors
///
/// Returns an error if tenant/workspace scoping is invalid or the catalog
/// snapshot cannot be read.
#[utoipa::path(
    get,
    path = "/catalogs",
    tag = "Catalogs",
    responses(
        (status = 200, description = "List catalogs"),
    )
)]
pub async fn list_catalogs(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let reader = super::common::catalog_reader(&state, &ctx)?;
    let catalogs = reader
        .list_catalogs()
        .await
        .map_err(super::common::map_catalog_error)?;

    let payload = json!({
        "catalogs": catalogs.into_iter().map(|catalog| {
            json!({
                "name": catalog.name,
                "comment": catalog.description,
                "created_at": catalog.created_at,
                "updated_at": catalog.updated_at
            })
        }).collect::<Vec<_>>()
    });

    Ok((StatusCode::OK, Json(payload)))
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
    super::common::known_but_unsupported(&method, &uri)
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
    super::common::known_but_unsupported(&method, &uri)
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
    super::common::known_but_unsupported(&method, &uri)
}
