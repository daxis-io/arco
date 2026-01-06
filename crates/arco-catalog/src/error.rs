//! Error types for arco-catalog operations.

use std::fmt;

use thiserror::Error;

/// Result type alias for catalog operations.
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Errors that can occur during catalog operations.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Storage operation failed.
    #[error("storage error: {message}")]
    Storage {
        /// Description of the storage failure.
        message: String,
    },

    /// Serialization/deserialization failed.
    #[error("serialization error: {message}")]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// Parquet/Arrow encoding or decoding failed.
    #[error("parquet error: {message}")]
    Parquet {
        /// Description of the parquet failure.
        message: String,
    },

    /// Validation failed for user input or request semantics.
    #[error("validation error: {message}")]
    Validation {
        /// Description of the validation failure.
        message: String,
    },

    /// Resource already exists.
    #[error("already exists: {entity} {name}")]
    AlreadyExists {
        /// The entity type (e.g., "namespace", "table").
        entity: String,
        /// The entity name or identifier.
        name: String,
    },

    /// Resource not found.
    #[error("not found: {entity} {name}")]
    NotFound {
        /// The entity type (e.g., "namespace", "table").
        entity: String,
        /// The entity name or identifier.
        name: String,
    },

    /// A precondition for the operation was not met (optimistic locking, CAS, etc).
    #[error("precondition failed: {message}")]
    PreconditionFailed {
        /// Description of the failed precondition.
        message: String,
    },

    /// CAS (Compare-And-Swap) operation failed due to concurrent modification.
    #[error("CAS failed: {message}")]
    CasFailed {
        /// Description of the CAS failure.
        message: String,
    },

    /// An internal invariant was violated (bug or corrupted state).
    #[error("invariant violation: {message}")]
    InvariantViolation {
        /// Description of the invariant violation.
        message: String,
    },
}

impl CatalogError {
    /// Creates a storage error from a displayable message.
    #[must_use]
    pub fn storage(message: impl fmt::Display) -> Self {
        Self::Storage {
            message: message.to_string(),
        }
    }

    /// Returns the HTTP status code for this error, or None for 5xx errors.
    #[must_use]
    pub const fn http_status_code(&self) -> Option<u16> {
        match self {
            Self::Validation { .. } => Some(400),
            Self::AlreadyExists { .. } | Self::CasFailed { .. } => Some(409),
            Self::NotFound { .. } => Some(404),
            Self::PreconditionFailed { .. } => Some(412),
            Self::Storage { .. }
            | Self::Serialization { .. }
            | Self::Parquet { .. }
            | Self::InvariantViolation { .. } => None,
        }
    }
}

impl From<arco_core::Error> for CatalogError {
    fn from(value: arco_core::Error) -> Self {
        match value {
            arco_core::Error::InvalidId { message }
            | arco_core::Error::InvalidInput(message)
            | arco_core::Error::TenantIsolation { message }
            | arco_core::Error::Validation { message } => Self::Validation { message },
            arco_core::Error::Storage { message, source } => {
                if let Some(source) = source {
                    Self::Storage {
                        message: format!("{message}: {source}"),
                    }
                } else {
                    Self::Storage { message }
                }
            }
            arco_core::Error::Serialization { message } => Self::Serialization { message },
            arco_core::Error::ResourceNotFound { resource_type, id } => Self::NotFound {
                entity: resource_type.to_string(),
                name: id,
            },
            arco_core::Error::NotFound(message) => Self::NotFound {
                entity: "object".to_string(),
                name: message,
            },
            arco_core::Error::PreconditionFailed { message } => {
                Self::PreconditionFailed { message }
            }
            arco_core::Error::Internal { message } => Self::InvariantViolation { message },
        }
    }
}
