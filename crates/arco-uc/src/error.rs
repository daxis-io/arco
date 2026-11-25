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
#[non_exhaustive]
pub enum UnityCatalogError {
    /// Request was invalid.
    #[error("{message}")]
    BadRequest {
        /// Human readable message.
        message: String,
    },
    /// Request or selected object exceeds a bounded payload limit.
    #[error("{message}")]
    PayloadTooLarge {
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
    /// Fail-closed catalog authority error with a stable code and retry delay.
    #[error("{message}")]
    CatalogAuthorityUnavailable {
        /// Stable machine-readable catalog authority code.
        error_code: &'static str,
        /// Human readable redacted message.
        message: String,
        /// Retry delay in seconds.
        retry_after_seconds: u64,
    },
    /// Request rate was limited.
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
        let (status, error_code, message) = match self {
            Self::BadRequest { message } => (StatusCode::BAD_REQUEST, "BAD_REQUEST", message),
            Self::PayloadTooLarge { message } => {
                (StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE", message)
            }
            Self::Unauthorized { message } => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", message),
            Self::Forbidden { message } => (StatusCode::FORBIDDEN, "FORBIDDEN", message),
            Self::NotFound { message } => (StatusCode::NOT_FOUND, "NOT_FOUND", message),
            Self::Conflict { message } => (StatusCode::CONFLICT, "CONFLICT", message),
            Self::NotImplemented { message } => {
                (StatusCode::NOT_IMPLEMENTED, "NOT_SUPPORTED", message)
            }
            Self::ServiceUnavailable { message } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "SERVICE_UNAVAILABLE",
                message,
            ),
            Self::CatalogAuthorityUnavailable {
                error_code,
                message,
                ..
            } => (StatusCode::SERVICE_UNAVAILABLE, *error_code, message),
            Self::TooManyRequests { message } => {
                (StatusCode::TOO_MANY_REQUESTS, "TOO_MANY_REQUESTS", message)
            }
            Self::Internal { message } => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", message),
        };
        (
            status,
            UnityCatalogErrorResponse {
                error: UnityCatalogErrorDetail {
                    error_code: error_code.to_string(),
                    message: message.clone(),
                },
            },
        )
    }
}

impl IntoResponse for UnityCatalogError {
    fn into_response(self) -> Response {
        let (status, payload) = self.to_status_and_payload();
        let mut response = (status, axum::Json(payload)).into_response();
        if let Self::CatalogAuthorityUnavailable {
            retry_after_seconds,
            ..
        } = self
        {
            if let Ok(value) = retry_after_seconds.to_string().parse() {
                response.headers_mut().insert("Retry-After", value);
            }
        }
        response
    }
}

/// Result type for UC facade handlers.
pub type UnityCatalogResult<T> = Result<T, UnityCatalogError>;
