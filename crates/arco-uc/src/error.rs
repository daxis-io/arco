//! UC facade error types and error response payloads.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

/// A UC-shaped error response payload (placeholder until pinned spec is vendored).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UnityCatalogErrorDetail {
    /// Stable error code identifier.
    pub error_code: String,
    /// Human readable message.
    pub message: String,
}

/// Error response wrapper (placeholder until pinned spec is vendored).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UnityCatalogErrorResponse {
    /// Error detail.
    pub error: UnityCatalogErrorDetail,
}

/// UC facade error type.
#[derive(Debug, thiserror::Error)]
pub enum UnityCatalogError {
    /// Request was invalid.
    #[error("{message}")]
    BadRequest {
        /// Human readable message.
        message: String,
    },
    /// Authentication required or invalid.
    #[error("{message}")]
    Unauthorized {
        /// Human readable message.
        message: String,
    },
    /// Authenticated but not permitted.
    #[error("{message}")]
    Forbidden {
        /// Human readable message.
        message: String,
    },
    /// Resource not found.
    #[error("{message}")]
    NotFound {
        /// Human readable message.
        message: String,
    },
    /// Conflict / precondition failure.
    #[error("{message}")]
    Conflict {
        /// Human readable message.
        message: String,
    },
    /// Feature is not supported by this deployment.
    #[error("{message}")]
    NotImplemented {
        /// Human readable message.
        message: String,
    },
    /// Service unavailable (retryable).
    #[error("{message}")]
    ServiceUnavailable {
        /// Human readable message.
        message: String,
    },
    /// Too many requests / throttled.
    #[error("{message}")]
    TooManyRequests {
        /// Human readable message.
        message: String,
    },
    /// Internal error.
    #[error("{message}")]
    Internal {
        /// Human readable message.
        message: String,
    },
}

impl UnityCatalogError {
    fn to_status_and_payload(&self) -> (StatusCode, UnityCatalogErrorResponse) {
        match self {
            Self::BadRequest { message } => (
                StatusCode::BAD_REQUEST,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "BAD_REQUEST".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::Unauthorized { message } => (
                StatusCode::UNAUTHORIZED,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "UNAUTHORIZED".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::Forbidden { message } => (
                StatusCode::FORBIDDEN,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "FORBIDDEN".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::NotFound { message } => (
                StatusCode::NOT_FOUND,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "NOT_FOUND".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::Conflict { message } => (
                StatusCode::CONFLICT,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "CONFLICT".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::NotImplemented { message } => (
                StatusCode::NOT_IMPLEMENTED,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "NOT_SUPPORTED".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::ServiceUnavailable { message } => (
                StatusCode::SERVICE_UNAVAILABLE,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "SERVICE_UNAVAILABLE".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::TooManyRequests { message } => (
                StatusCode::TOO_MANY_REQUESTS,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "TOO_MANY_REQUESTS".to_string(),
                        message: message.clone(),
                    },
                },
            ),
            Self::Internal { message } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                UnityCatalogErrorResponse {
                    error: UnityCatalogErrorDetail {
                        error_code: "INTERNAL".to_string(),
                        message: message.clone(),
                    },
                },
            ),
        }
    }
}

impl IntoResponse for UnityCatalogError {
    fn into_response(self) -> Response {
        let (status, payload) = self.to_status_and_payload();
        (status, axum::Json(payload)).into_response()
    }
}

/// Result type for UC facade handlers.
pub type UnityCatalogResult<T> = Result<T, UnityCatalogError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_many_requests_maps_to_429_with_stable_error_code() {
        let response = UnityCatalogError::TooManyRequests {
            message: "rate limit exceeded".to_string(),
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
