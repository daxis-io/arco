//! Temporary credential endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::post;

use serde::Deserialize;
use serde_json::json;
use utoipa::ToSchema;

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::error::UnityCatalogResult;
use crate::state::UnityCatalogState;

#[derive(Debug, Deserialize, ToSchema)]
/// Request payload for `POST /temporary-path-credentials`.
pub struct TemporaryPathCredentialsRequest {
    /// Cloud storage URL (e.g. `gs://bucket/path`).
    pub url: String,
    /// Requested operation (UC enum-like string).
    pub operation: String,
}

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
    super::common::known_but_unsupported(&method, &uri)
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
    super::common::known_but_unsupported(&method, &uri)
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
    super::common::known_but_unsupported(&method, &uri)
}

/// `POST /temporary-path-credentials` (Scope A).
///
/// # Errors
///
/// Returns [`UnityCatalogError::BadRequest`] if the requested `operation` is not
/// recognized.
#[utoipa::path(
    post,
    path = "/temporary-path-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 200, description = "Get temporary path credentials"),
        (status = 400, description = "Bad request", body = UnityCatalogErrorResponse),
    ),
)]
pub async fn temporary_path_credentials(
    Json(request): Json<TemporaryPathCredentialsRequest>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let allowed = ["PATH_READ", "PATH_WRITE", "READ", "WRITE", "LIST", "MANAGE"];

    if !allowed.contains(&request.operation.as_str()) {
        return Err(UnityCatalogError::BadRequest {
            message: format!("unknown path operation: {}", request.operation),
        });
    }

    // Placeholder response until we plumb cloud-specific credential vending.
    let payload = json!({
        "url": request.url,
        "operation": request.operation,
        "credentials": []
    });

    Ok((StatusCode::OK, Json(payload)))
}
