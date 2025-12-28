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
pub enum IcebergError {
    /// Bad request (400) - Invalid input or unsupported operation.
    #[error("Bad request: {message}")]
    BadRequest {
        /// Human-readable error message.
        message: String,
        /// Iceberg exception type (e.g., `BadRequestException`).
        error_type: &'static str,
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

    /// Service unavailable (503) - Retry after delay.
    #[error("Service unavailable: {message}")]
    ServiceUnavailable {
        /// Human-readable error message.
        message: String,
        /// Suggested retry delay in seconds.
        retry_after_seconds: Option<u32>,
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

    /// Create a not found error for a missing namespace.
    #[must_use]
    pub fn namespace_not_found(namespace: &str) -> Self {
        Self::NotFound {
            message: format!("Namespace does not exist: {namespace}"),
            error_type: "NoSuchNamespaceException",
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
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Conflict { .. } => StatusCode::CONFLICT,
            Self::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
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
            Self::Unauthorized { .. } => "UnauthorizedException",
            Self::Forbidden { .. } => "ForbiddenException",
            Self::ServiceUnavailable { .. } => "ServiceUnavailableException",
            Self::Internal { .. } => "InternalServerException",
        }
    }

    /// Returns the human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest { message, .. }
            | Self::Unauthorized { message }
            | Self::Forbidden { message }
            | Self::NotFound { message, .. }
            | Self::Conflict { message, .. }
            | Self::ServiceUnavailable { message, .. }
            | Self::Internal { message } => message,
        }
    }
}

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
            CatalogError::NotFound { entity, name } => {
                if entity == "namespace" {
                    Self::namespace_not_found(&name)
                } else if entity == "table" {
                    Self::NotFound {
                        message: format!("Table does not exist: {name}"),
                        error_type: "NoSuchTableException",
                    }
                } else {
                    Self::NotFound {
                        message: format!("{entity} not found: {name}"),
                        error_type: "NotFoundException",
                    }
                }
            }
            CatalogError::AlreadyExists { entity, name } => Self::Conflict {
                message: format!("{entity} already exists: {name}"),
                error_type: "AlreadyExistsException",
            },
            CatalogError::Validation { message } => Self::BadRequest {
                message,
                error_type: "BadRequestException",
            },
            CatalogError::PreconditionFailed { message } | CatalogError::CasFailed { message } => {
                Self::Conflict {
                    message,
                    error_type: "CommitFailedException",
                }
            }
            CatalogError::Storage { message } => Self::ServiceUnavailable {
                message,
                retry_after_seconds: None,
            },
            CatalogError::Serialization { message }
            | CatalogError::Parquet { message }
            | CatalogError::InvariantViolation { message } => Self::Internal { message },
        }
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

    #[test]
    fn test_catalog_error_not_found_namespace_mapping() {
        let err = CatalogError::NotFound {
            entity: "namespace".to_string(),
            name: "analytics".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::NotFound { .. }));
        assert_eq!(mapped.error_type(), "NoSuchNamespaceException");
        assert_eq!(mapped.status_code(), StatusCode::NOT_FOUND);
        assert!(mapped.message().contains("analytics"));
    }

    #[test]
    fn test_catalog_error_not_found_table_mapping() {
        let err = CatalogError::NotFound {
            entity: "table".to_string(),
            name: "analytics.events".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::NotFound { .. }));
        assert_eq!(mapped.error_type(), "NoSuchTableException");
        assert_eq!(mapped.status_code(), StatusCode::NOT_FOUND);
        assert!(mapped.message().contains("analytics.events"));
    }

    #[test]
    fn test_catalog_error_already_exists_mapping() {
        let err = CatalogError::AlreadyExists {
            entity: "table".to_string(),
            name: "analytics.events".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::Conflict { .. }));
        assert_eq!(mapped.error_type(), "AlreadyExistsException");
        assert_eq!(mapped.status_code(), StatusCode::CONFLICT);
        assert!(mapped.message().contains("already exists"));
    }

    #[test]
    fn test_catalog_error_validation_mapping() {
        let err = CatalogError::Validation {
            message: "invalid table name".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::BadRequest { .. }));
        assert_eq!(mapped.error_type(), "BadRequestException");
        assert_eq!(mapped.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(mapped.message(), "invalid table name");
    }

    #[test]
    fn test_catalog_error_precondition_failed_mapping() {
        let err = CatalogError::PreconditionFailed {
            message: "etag mismatch".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::Conflict { .. }));
        assert_eq!(mapped.error_type(), "CommitFailedException");
        assert_eq!(mapped.status_code(), StatusCode::CONFLICT);
        assert_eq!(mapped.message(), "etag mismatch");
    }

    #[test]
    fn test_catalog_error_cas_failed_mapping() {
        let err = CatalogError::CasFailed {
            message: "pointer version conflict".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::Conflict { .. }));
        assert_eq!(mapped.error_type(), "CommitFailedException");
        assert_eq!(mapped.status_code(), StatusCode::CONFLICT);
        assert_eq!(mapped.message(), "pointer version conflict");
    }

    #[test]
    fn test_catalog_error_storage_mapping() {
        let err = CatalogError::Storage {
            message: "backend unavailable".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(
            &mapped,
            IcebergError::ServiceUnavailable {
                retry_after_seconds: None,
                ..
            }
        ));
        assert_eq!(mapped.error_type(), "ServiceUnavailableException");
        assert_eq!(mapped.status_code(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.message(), "backend unavailable");
    }

    #[test]
    fn test_catalog_error_serialization_mapping() {
        let err = CatalogError::Serialization {
            message: "serde failure".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::Internal { .. }));
        assert_eq!(mapped.error_type(), "InternalServerException");
        assert_eq!(mapped.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(mapped.message(), "serde failure");
    }

    #[test]
    fn test_catalog_error_parquet_mapping() {
        let err = CatalogError::Parquet {
            message: "parquet decode error".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::Internal { .. }));
        assert_eq!(mapped.error_type(), "InternalServerException");
        assert_eq!(mapped.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(mapped.message(), "parquet decode error");
    }

    #[test]
    fn test_catalog_error_invariant_violation_mapping() {
        let err = CatalogError::InvariantViolation {
            message: "unexpected state".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::Internal { .. }));
        assert_eq!(mapped.error_type(), "InternalServerException");
        assert_eq!(mapped.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(mapped.message(), "unexpected state");
    }

    #[test]
    fn test_catalog_error_not_found_other_mapping() {
        let err = CatalogError::NotFound {
            entity: "view".to_string(),
            name: "analytics.my_view".to_string(),
        };
        let mapped = IcebergError::from(err);

        assert!(matches!(&mapped, IcebergError::NotFound { .. }));
        assert_eq!(mapped.error_type(), "NotFoundException");
        assert_eq!(mapped.status_code(), StatusCode::NOT_FOUND);
        assert!(mapped.message().contains("view"));
        assert!(mapped.message().contains("analytics.my_view"));
    }

    #[test]
    fn test_unauthorized_error_roundtrip() {
        let err = IcebergError::Unauthorized {
            message: "Invalid credentials".to_string(),
        };
        let response = IcebergErrorResponse::from(&err);
        let json = serde_json::to_value(&response).expect("serialization failed");

        assert_eq!(json["error"]["code"], 401);
        assert_eq!(json["error"]["type"], "UnauthorizedException");
        assert_eq!(json["error"]["message"], "Invalid credentials");

        // Roundtrip
        let parsed: IcebergErrorResponse =
            serde_json::from_value(json.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("re-serialization failed");
        assert_eq!(roundtrip, json);
    }

    #[test]
    fn test_forbidden_error_roundtrip() {
        let err = IcebergError::Forbidden {
            message: "Insufficient permissions for namespace".to_string(),
        };
        let response = IcebergErrorResponse::from(&err);
        let json = serde_json::to_value(&response).expect("serialization failed");

        assert_eq!(json["error"]["code"], 403);
        assert_eq!(json["error"]["type"], "ForbiddenException");
        assert_eq!(json["error"]["message"], "Insufficient permissions for namespace");

        // Roundtrip
        let parsed: IcebergErrorResponse =
            serde_json::from_value(json.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("re-serialization failed");
        assert_eq!(roundtrip, json);
    }

    #[test]
    fn test_internal_error_roundtrip() {
        let err = IcebergError::Internal {
            message: "Unexpected storage failure".to_string(),
        };
        let response = IcebergErrorResponse::from(&err);
        let json = serde_json::to_value(&response).expect("serialization failed");

        assert_eq!(json["error"]["code"], 500);
        assert_eq!(json["error"]["type"], "InternalServerException");
        assert_eq!(json["error"]["message"], "Unexpected storage failure");

        // Roundtrip
        let parsed: IcebergErrorResponse =
            serde_json::from_value(json.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("re-serialization failed");
        assert_eq!(roundtrip, json);
    }

    #[test]
    fn test_service_unavailable_error_roundtrip() {
        let err = IcebergError::ServiceUnavailable {
            message: "Storage backend temporarily unavailable".to_string(),
            retry_after_seconds: Some(30),
        };
        let response = IcebergErrorResponse::from(&err);
        let json = serde_json::to_value(&response).expect("serialization failed");

        assert_eq!(json["error"]["code"], 503);
        assert_eq!(json["error"]["type"], "ServiceUnavailableException");
        assert_eq!(json["error"]["message"], "Storage backend temporarily unavailable");

        // Roundtrip
        let parsed: IcebergErrorResponse =
            serde_json::from_value(json.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("re-serialization failed");
        assert_eq!(roundtrip, json);
    }
}
