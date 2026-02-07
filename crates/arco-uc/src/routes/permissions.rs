//! Permission endpoints for the UC facade.

use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::routing::get;

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::state::UnityCatalogState;

/// Permission route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route(
        "/permissions/:securable_type/:full_name",
        get(get_permissions).patch(update_permissions),
    )
}

/// `GET /permissions/{securable_type}/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/permissions/{securable_type}/{full_name}",
    tag = "Permissions",
    params(
        ("securable_type" = String, Path, description = "Securable type"),
        ("full_name" = String, Path, description = "Securable full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn get_permissions(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `PATCH /permissions/{securable_type}/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    patch,
    path = "/permissions/{securable_type}/{full_name}",
    tag = "Permissions",
    params(
        ("securable_type" = String, Path, description = "Securable type"),
        ("full_name" = String, Path, description = "Securable full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn update_permissions(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}
