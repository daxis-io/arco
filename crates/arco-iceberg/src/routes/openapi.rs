//! Handler for Iceberg `OpenAPI` specification.

use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::IcebergError;
use crate::openapi::openapi_json;

/// Returns the Iceberg REST Catalog `OpenAPI` spec as JSON.
pub async fn get_openapi_json() -> Response {
    match openapi_json() {
        Ok(spec) => (StatusCode::OK, [(CONTENT_TYPE, "application/json")], spec).into_response(),
        Err(err) => IcebergError::Internal {
            message: format!("failed to serialize OpenAPI spec: {err}"),
        }
        .into_response(),
    }
}
