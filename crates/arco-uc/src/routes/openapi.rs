//! Handler for the UC facade `OpenAPI` specification.

use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::openapi::openapi_json;

/// Returns the UC facade `OpenAPI` spec as JSON.
#[utoipa::path(
    get,
    path = "/openapi.json",
    tag = "OpenAPI",
    responses(
        (
            status = 200,
            description = "OpenAPI specification for the UC facade",
            body = String,
            content_type = "application/json"
        ),
        (
            status = 500,
            description = "Internal error",
            body = UnityCatalogErrorResponse
        ),
    )
)]
pub async fn get_openapi_json() -> Response {
    match openapi_json() {
        Ok(spec) => (StatusCode::OK, [(CONTENT_TYPE, "application/json")], spec).into_response(),
        Err(err) => UnityCatalogError::Internal {
            message: format!("failed to serialize OpenAPI spec: {err}"),
        }
        .into_response(),
    }
}
