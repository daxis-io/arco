//! Temporary credential endpoints for the UC facade.

use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::routing::post;

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::state::UnityCatalogState;

/// Temporary credential route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route(
            "/temporary-model-version-credentials",
            post(temporary_model_version_credentials),
        )
        .route(
            "/temporary-table-credentials",
            post(temporary_table_credentials),
        )
        .route(
            "/temporary-volume-credentials",
            post(temporary_volume_credentials),
        )
        .route(
            "/temporary-path-credentials",
            post(temporary_path_credentials),
        )
}

/// `POST /temporary-model-version-credentials` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/temporary-model-version-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn temporary_model_version_credentials(
    method: Method,
    uri: OriginalUri,
) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `POST /temporary-table-credentials` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/temporary-table-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn temporary_table_credentials(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `POST /temporary-volume-credentials` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/temporary-volume-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn temporary_volume_credentials(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `POST /temporary-path-credentials` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/temporary-path-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn temporary_path_credentials(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}
