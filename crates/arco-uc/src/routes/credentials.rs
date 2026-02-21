//! Temporary credential routes for the Unity Catalog facade.

use axum::Json;
use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::post;
use serde::Deserialize;
use serde_json::json;

use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::state::UnityCatalogState;

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "GenerateTemporaryTableCredentialRequestBody")]
#[serde(default)]
pub(crate) struct GenerateTemporaryTableCredentialRequestBody {
    table_id: Option<String>,
    operation: Option<String>,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[schema(title = "GenerateTemporaryPathCredentialRequestBody")]
pub(crate) struct GenerateTemporaryPathCredentialRequestBody {
    /// Cloud storage URL (e.g. `gs://bucket/path`).
    url: String,
    /// Requested operation.
    operation: String,
}

/// Temporary credential route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route(
            "/temporary-model-version-credentials",
            post(post_temporary_model_version_credentials),
        )
        .route(
            "/temporary-table-credentials",
            post(post_temporary_table_credentials),
        )
        .route(
            "/temporary-volume-credentials",
            post(post_temporary_volume_credentials),
        )
        .route(
            "/temporary-path-credentials",
            post(post_temporary_path_credentials),
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
pub async fn post_temporary_model_version_credentials(
    method: Method,
    uri: OriginalUri,
) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// Generates temporary table credentials (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/temporary-table-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryTableCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_temporary_table_credentials(
    method: Method,
    uri: OriginalUri,
    payload: Json<GenerateTemporaryTableCredentialRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let Json(payload) = payload;
    let _ = (payload.table_id, payload.operation);
    Err(super::common::known_but_unsupported(&method, &uri))
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
pub async fn post_temporary_volume_credentials(
    method: Method,
    uri: OriginalUri,
) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// Generates temporary path credentials.
///
/// # Errors
///
/// Returns [`UnityCatalogError::BadRequest`] if `operation` is not recognized.
#[utoipa::path(
    post,
    path = "/temporary-path-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryPathCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_temporary_path_credentials(
    Json(request): Json<GenerateTemporaryPathCredentialRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let allowed = ["PATH_READ", "PATH_WRITE", "READ", "WRITE", "LIST", "MANAGE"];

    if !allowed.contains(&request.operation.as_str()) {
        return Err(UnityCatalogError::BadRequest {
            message: format!("unknown path operation: {}", request.operation),
        });
    }

    // Placeholder response until cloud-specific credential vending is plumbed through.
    let payload = json!({
        "url": request.url,
        "operation": request.operation,
        "credentials": []
    });

    Ok((StatusCode::OK, Json(payload)))
}
