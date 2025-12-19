//! API error types and HTTP response mapping.

use axum::Json;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header::HeaderName;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

use arco_catalog::CatalogError;
use arco_core::Error as CoreError;

/// API result type.
pub type ApiResult<T> = Result<T, ApiError>;

/// Standard JSON error response body.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiErrorBody {
    /// Stable machine-readable error code.
    pub code: String,
    /// Human-readable message (safe for clients).
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional request ID for correlation.
    pub request_id: Option<String>,
}

/// HTTP API error with stable machine-readable code.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    request_id: Option<String>,
}

impl ApiError {
    /// Returns an error response for invalid input.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "BAD_REQUEST", message)
    }

    /// Returns an error response for authentication failures.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", message)
    }

    /// Returns an error response when the Authorization header is missing.
    #[must_use]
    pub fn missing_auth() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "MISSING_AUTH",
            "Authorization header required",
        )
    }

    /// Returns an error response when the bearer token is invalid.
    #[must_use]
    pub fn invalid_token() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "INVALID_TOKEN",
            "Invalid bearer token",
        )
    }

    /// Returns an error response for authorization failures.
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "FORBIDDEN", message)
    }

    /// Returns an error response for missing resources.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message)
    }

    /// Returns an error response for conflict (already exists / CAS).
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "CONFLICT", message)
    }

    /// Returns an error response for failed preconditions (`If-Match`).
    pub fn precondition_failed(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "PRECONDITION_FAILED",
            message,
        )
    }

    /// Returns an internal error response.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", message)
    }

    /// Attaches a request ID for correlation.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            request_id: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = self.request_id;
        let mut response = (
            self.status,
            Json(ApiErrorBody {
                code: self.code.to_string(),
                message: self.message,
                request_id: request_id.clone(),
            }),
        )
            .into_response();

        if let Some(request_id) = request_id {
            if let Ok(value) = HeaderValue::from_str(&request_id) {
                response
                    .headers_mut()
                    .insert(HeaderName::from_static("x-request-id"), value);
            }
        }

        response
    }
}

impl From<CatalogError> for ApiError {
    fn from(value: CatalogError) -> Self {
        match value {
            CatalogError::Validation { message } => Self::bad_request(message),
            CatalogError::AlreadyExists { entity, name } => {
                Self::conflict(format!("{entity} already exists: {name}"))
            }
            CatalogError::NotFound { entity, name } => {
                Self::not_found(format!("{entity} not found: {name}"))
            }
            CatalogError::PreconditionFailed { message } => Self::precondition_failed(message),
            CatalogError::CasFailed { message } => Self::conflict(message),
            CatalogError::Storage { message }
            | CatalogError::Serialization { message }
            | CatalogError::Parquet { message }
            | CatalogError::InvariantViolation { message } => Self::internal(message),
        }
    }
}

impl From<CoreError> for ApiError {
    fn from(value: CoreError) -> Self {
        match value {
            CoreError::InvalidId { message }
            | CoreError::InvalidInput(message)
            | CoreError::Validation { message } => Self::bad_request(message),
            CoreError::TenantIsolation { message } => Self::forbidden(message),
            CoreError::NotFound(message) => Self::not_found(message),
            CoreError::ResourceNotFound { resource_type, id } => {
                Self::not_found(format!("{resource_type} not found: {id}"))
            }
            CoreError::PreconditionFailed { message } => Self::precondition_failed(message),
            CoreError::Storage { message, .. }
            | CoreError::Serialization { message }
            | CoreError::Internal { message } => Self::internal(message),
        }
    }
}
