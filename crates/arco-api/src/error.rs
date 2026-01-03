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
    /// Optional error category (e.g., `unprocessable_entity`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
    error: Option<&'static str>,
    request_id: Option<String>,
    retry_after_secs: Option<u64>,
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

    /// Returns an error response when a request times out.
    pub fn request_timeout(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::REQUEST_TIMEOUT,
            "REQUEST_TIMEOUT",
            message,
        )
    }

    /// Returns an internal error response.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", message)
    }

    /// Returns an error response for unsupported operations (406).
    pub fn not_acceptable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_ACCEPTABLE, "NOT_ACCEPTABLE", message)
    }

    /// Returns a not implemented error response.
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, "NOT_IMPLEMENTED", message)
    }

    /// Returns an unprocessable entity error response.
    pub fn unprocessable_entity(code: &'static str, message: impl Into<String>) -> Self {
        Self::new_with_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            code,
            message,
            Some("unprocessable_entity"),
        )
    }

    /// Attaches a request ID for correlation.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Returns the HTTP status code for this error.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns the human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the request ID, if one was attached.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Returns the stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self::new_with_error(status, code, message, None)
    }

    fn new_with_error(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        error: Option<&'static str>,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            error,
            request_id: None,
            retry_after_secs: None,
        }
    }

    /// Attaches a Retry-After header value in seconds.
    #[must_use]
    pub fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after_secs = Some(seconds);
        self
    }

    /// Returns a 409 Conflict for in-progress idempotent requests with Retry-After header.
    #[must_use]
    pub fn conflict_in_progress(retry_after_secs: u64) -> Self {
        Self::conflict("Request already in progress").with_retry_after(retry_after_secs)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = self.request_id;
        let retry_after_secs = self.retry_after_secs;
        let mut response = (
            self.status,
            Json(ApiErrorBody {
                code: self.code.to_string(),
                message: self.message,
                error: self.error.map(str::to_string),
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

        if let Some(secs) = retry_after_secs {
            if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
                response
                    .headers_mut()
                    .insert(HeaderName::from_static("retry-after"), value);
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
            CatalogError::UnsupportedOperation { message } => Self::not_acceptable(message),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict_in_progress_has_retry_after() {
        let error = ApiError::conflict_in_progress(5);
        assert_eq!(error.status(), StatusCode::CONFLICT);
        assert_eq!(error.code(), "CONFLICT");
        assert!(error.message().contains("in progress"));

        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let retry_after = response
            .headers()
            .get("retry-after")
            .expect("Retry-After header should be present");
        assert_eq!(retry_after.to_str().unwrap(), "5");
    }

    #[test]
    fn test_with_retry_after_sets_header() {
        let error = ApiError::conflict("test").with_retry_after(10);
        let response = error.into_response();

        let retry_after = response
            .headers()
            .get("retry-after")
            .expect("Retry-After header should be present");
        assert_eq!(retry_after.to_str().unwrap(), "10");
    }

    #[test]
    fn test_regular_conflict_has_no_retry_after() {
        let error = ApiError::conflict("test");
        let response = error.into_response();

        assert!(response.headers().get("retry-after").is_none());
    }
}
