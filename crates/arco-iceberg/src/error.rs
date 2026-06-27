//! Iceberg-specific error types and HTTP status mapping.
//!
//! This module provides error types that align with the Iceberg REST Catalog
//! specification's error response format.

use arco_catalog::error::CatalogError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Result type alias for Iceberg operations.
pub type IcebergResult<T> = Result<T, IcebergError>;

/// Iceberg REST Catalog error types.
///
/// Maps to HTTP status codes and Iceberg exception types as defined
/// in the REST Catalog specification.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IcebergError {
    /// Bad request (400) - Invalid input.
    #[error("Bad request: {message}")]
    BadRequest {
        /// Human-readable error message.
        message: String,
        /// Iceberg exception type (e.g., `BadRequestException`).
        error_type: &'static str,
    },

    /// Not acceptable (406) - Unsupported operation.
    #[error("Unsupported: {message}")]
    UnsupportedOperation {
        /// Human-readable error message.
        message: String,
    },

    /// Unauthorized (401) - Authentication required or invalid.
    #[error("Unauthorized: {message}")]
    Unauthorized {
        /// Human-readable error message.
        message: String,
    },

    /// Forbidden (403) - Insufficient permissions.
    #[error("Forbidden: {message}")]
    Forbidden {
        /// Human-readable error message.
        message: String,
    },

    /// Not found (404) - Resource does not exist.
    #[error("Not found: {message}")]
    NotFound {
        /// Human-readable error message.
        message: String,
        /// Iceberg exception type (e.g., `NoSuchTableException`).
        error_type: &'static str,
    },

    /// Conflict (409) - Concurrent modification or idempotency key conflict.
    #[error("Conflict: {message}")]
    Conflict {
        /// Human-readable error message.
        message: String,
        /// Iceberg exception type (e.g., `CommitFailedException`).
        error_type: &'static str,
    },

    /// Unprocessable entity (422) - Semantic validation failure.
    #[error("Unprocessable: {message}")]
    UnprocessableEntity {
        /// Human-readable error message.
        message: String,
    },

    /// Service unavailable (503) - Retry after delay.
    #[error("Service unavailable: {message}")]
    ServiceUnavailable {
        /// Human-readable error message.
        message: String,
        /// Suggested retry delay in seconds.
        retry_after_seconds: Option<u32>,
    },

    /// Too many requests (429) - rate limit exceeded.
    #[error("Too many requests: {message}")]
    TooManyRequests {
        /// Human-readable error message.
        message: String,
        /// Suggested retry delay in seconds.
        retry_after_seconds: Option<u64>,
    },

    /// Internal server error (500) - Unexpected failure.
    #[error("Internal error: {message}")]
    Internal {
        /// Human-readable error message.
        message: String,
    },
}

impl IcebergError {
    /// Create a bad request error for invalid idempotency key.
    #[must_use]
    pub fn invalid_idempotency_key(details: impl Into<String>) -> Self {
        Self::BadRequest {
            message: format!("Invalid Idempotency-Key: {}", details.into()),
            error_type: "BadRequestException",
        }
    }

    /// Create a not found error for a missing table.
    #[must_use]
    pub fn table_not_found(namespace: &str, table: &str) -> Self {
        Self::NotFound {
            message: format!("Table does not exist: {namespace}.{table}"),
            error_type: "NoSuchTableException",
        }
    }

    /// Creates a namespace not found error.
    #[must_use]
    pub fn namespace_not_found(namespace: &str) -> Self {
        Self::NotFound {
            message: format!("Namespace does not exist: {namespace}"),
            error_type: "NoSuchNamespaceException",
        }
    }

    /// Creates a namespace not empty error (409).
    #[must_use]
    pub fn namespace_not_empty(namespace: &str) -> Self {
        Self::Conflict {
            message: format!("Namespace is not empty: {namespace}"),
            error_type: "NamespaceNotEmptyException",
        }
    }

    /// Creates an unsupported operation error (406).
    #[must_use]
    pub fn unsupported_operation(message: impl Into<String>) -> Self {
        Self::UnsupportedOperation {
            message: message.into(),
        }
    }

    /// Creates a 422 error for property keys appearing in both updates and removals.
    #[must_use]
    pub fn property_overlap(keys: &[String]) -> Self {
        Self::UnprocessableEntity {
            message: format!(
                "Keys present in both updates and removals: {}",
                keys.join(", ")
            ),
        }
    }

    /// Create a conflict error for CAS failure.
    #[must_use]
    pub fn commit_conflict(details: impl Into<String>) -> Self {
        Self::Conflict {
            message: details.into(),
            error_type: "CommitFailedException",
        }
    }

    /// Create a conflict error for idempotency key reuse.
    #[must_use]
    pub fn idempotency_key_conflict() -> Self {
        Self::Conflict {
            message: "Idempotency-Key reused with different request payload".to_string(),
            error_type: "CommitFailedException",
        }
    }

    /// Returns the HTTP status code for this error.
    #[must_use]
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest { .. } => StatusCode::BAD_REQUEST,
            Self::UnsupportedOperation { .. } => StatusCode::NOT_ACCEPTABLE,
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Conflict { .. } => StatusCode::CONFLICT,
            Self::UnprocessableEntity { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::TooManyRequests { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Returns the Iceberg error type string.
    #[must_use]
    pub const fn error_type(&self) -> &'static str {
        match self {
            Self::BadRequest { error_type, .. }
            | Self::NotFound { error_type, .. }
            | Self::Conflict { error_type, .. } => error_type,
            Self::UnsupportedOperation { .. } => "UnsupportedOperationException",
            Self::Unauthorized { .. } => "UnauthorizedException",
            Self::Forbidden { .. } => "ForbiddenException",
            Self::UnprocessableEntity { .. } => "UnprocessableEntityException",
            Self::ServiceUnavailable { .. } => "ServiceUnavailableException",
            Self::TooManyRequests { .. } => "TooManyRequestsException",
            Self::Internal { .. } => "InternalServerException",
        }
    }

    /// Returns the human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest { message, .. }
            | Self::UnsupportedOperation { message }
            | Self::Unauthorized { message }
            | Self::Forbidden { message }
            | Self::NotFound { message, .. }
            | Self::Conflict { message, .. }
            | Self::UnprocessableEntity { message }
            | Self::ServiceUnavailable { message, .. }
            | Self::TooManyRequests { message, .. }
            | Self::Internal { message } => message,
        }
    }
}

const PUBLIC_STORAGE_UNAVAILABLE_MESSAGE: &str = "Service temporarily unavailable";
const PUBLIC_INTERNAL_ERROR_MESSAGE: &str = "Internal server error";

/// Iceberg REST Catalog error response format.
///
/// Matches the `OpenAPI` specification for error responses.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IcebergErrorResponse {
    /// The error type (exception class name).
    #[serde(rename = "error")]
    pub error: IcebergErrorDetail,
}

/// Detailed error information.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IcebergErrorDetail {
    /// Human-readable error message.
    pub message: String,
    /// Exception type (e.g., `NoSuchTableException`).
    #[serde(rename = "type")]
    pub error_type: String,
    /// HTTP status code.
    pub code: u16,
}

impl From<&IcebergError> for IcebergErrorResponse {
    fn from(err: &IcebergError) -> Self {
        Self {
            error: IcebergErrorDetail {
                message: err.message().to_string(),
                error_type: err.error_type().to_string(),
                code: err.status_code().as_u16(),
            },
        }
    }
}

impl From<CatalogError> for IcebergError {
    fn from(err: CatalogError) -> Self {
        match err {
            CatalogError::NotFound { entity, name } => map_catalog_not_found(&entity, &name),
            CatalogError::AlreadyExists { entity, name } => {
                map_catalog_already_exists(&entity, &name)
            }
            CatalogError::Validation { message } => map_catalog_validation(message),
            CatalogError::PreconditionFailed { message } | CatalogError::CasFailed { message } => {
                map_catalog_commit_conflict(message)
            }
            CatalogError::RequestFailed {
                http_status,
                message,
            } => map_catalog_request_failed(http_status, message),
            CatalogError::Storage { message } => map_catalog_storage(&message),
            CatalogError::Serialization { message }
            | CatalogError::Parquet { message }
            | CatalogError::InvariantViolation { message } => map_catalog_internal(&message),
            CatalogError::UnsupportedOperation { message } => {
                Self::UnsupportedOperation { message }
            }
            error => map_unknown_catalog_error(&error),
        }
    }
}

fn map_catalog_not_found(entity: &str, name: &str) -> IcebergError {
    if entity == "namespace" {
        IcebergError::namespace_not_found(name)
    } else if entity == "table" {
        IcebergError::NotFound {
            message: format!("Table does not exist: {name}"),
            error_type: "NoSuchTableException",
        }
    } else {
        IcebergError::NotFound {
            message: format!("{entity} not found: {name}"),
            error_type: "NotFoundException",
        }
    }
}

fn map_catalog_already_exists(entity: &str, name: &str) -> IcebergError {
    IcebergError::Conflict {
        message: format!("{entity} already exists: {name}"),
        error_type: "AlreadyExistsException",
    }
}

fn map_catalog_validation(message: String) -> IcebergError {
    if message.contains("contains tables, cannot delete") {
        let ns_name = message
            .strip_prefix("namespace '")
            .and_then(|s| s.split('\'').next())
            .unwrap_or("unknown");
        IcebergError::namespace_not_empty(ns_name)
    } else {
        IcebergError::BadRequest {
            message,
            error_type: "BadRequestException",
        }
    }
}

fn map_catalog_commit_conflict(message: String) -> IcebergError {
    IcebergError::Conflict {
        message,
        error_type: "CommitFailedException",
    }
}

fn map_catalog_request_failed(http_status: u16, message: String) -> IcebergError {
    match http_status {
        400 => IcebergError::BadRequest {
            message,
            error_type: "BadRequestException",
        },
        404 => IcebergError::NotFound {
            message,
            error_type: "NotFoundException",
        },
        409 | 412 => IcebergError::Conflict {
            message,
            error_type: "CommitFailedException",
        },
        406 | 501 => IcebergError::UnsupportedOperation { message },
        _ => IcebergError::Internal { message },
    }
}

fn map_catalog_storage(message: &str) -> IcebergError {
    tracing::warn!(internal_error = %message, "redacted Iceberg storage error");
    IcebergError::ServiceUnavailable {
        message: PUBLIC_STORAGE_UNAVAILABLE_MESSAGE.to_string(),
        retry_after_seconds: None,
    }
}

fn map_catalog_internal(message: &str) -> IcebergError {
    tracing::warn!(internal_error = %message, "redacted Iceberg internal error");
    IcebergError::Internal {
        message: PUBLIC_INTERNAL_ERROR_MESSAGE.to_string(),
    }
}

fn map_unknown_catalog_error(error: &CatalogError) -> IcebergError {
    tracing::warn!(internal_error = %error, "redacted unknown Iceberg catalog error");
    IcebergError::Internal {
        message: PUBLIC_INTERNAL_ERROR_MESSAGE.to_string(),
    }
}

impl IntoResponse for IcebergError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = IcebergErrorResponse::from(&self);

        let mut response = (status, axum::Json(body)).into_response();

        // Add Retry-After header for 503 responses
        if let Self::ServiceUnavailable {
            retry_after_seconds: Some(seconds),
            ..
        } = &self
        {
            if let Ok(value) = seconds.to_string().parse() {
                response.headers_mut().insert("Retry-After", value);
            }
        }
        if let Self::TooManyRequests {
            retry_after_seconds: Some(seconds),
            ..
        } = &self
        {
            if let Ok(value) = seconds.to_string().parse() {
                response.headers_mut().insert("Retry-After", value);
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_status_codes() {
        assert_eq!(
            IcebergError::BadRequest {
                message: "test".into(),
                error_type: "BadRequestException"
            }
            .status_code(),
            StatusCode::BAD_REQUEST
        );

        assert_eq!(
            IcebergError::table_not_found("ns", "tbl").status_code(),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            IcebergError::commit_conflict("test").status_code(),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn test_error_response_serialization() {
        let err = IcebergError::table_not_found("my_namespace", "my_table");
        let response = IcebergErrorResponse::from(&err);

        let json = serde_json::to_string(&response).expect("serialization failed");
        assert!(json.contains("NoSuchTableException"));
        assert!(json.contains("404"));
    }

    #[test]
    fn storage_errors_do_not_expose_internal_details() {
        let err = IcebergError::from(CatalogError::Storage {
            message: "gcs bucket prod-secret path tenant=acme/workspace=analytics/token"
                .to_string(),
        });

        let body = IcebergErrorResponse::from(&err);

        assert_eq!(err.status_code(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.error.message, "Service temporarily unavailable");
        assert!(!body.error.message.contains("prod-secret"));
        assert!(!body.error.message.contains("tenant=acme"));
    }

    #[test]
    fn test_error_type_mapping() {
        assert_eq!(
            IcebergError::namespace_not_found("ns").error_type(),
            "NoSuchNamespaceException"
        );

        assert_eq!(
            IcebergError::idempotency_key_conflict().error_type(),
            "CommitFailedException"
        );
    }
}
