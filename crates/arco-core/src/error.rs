//! Error types and result aliases for Arco.
//!
//! This module defines the shared error types used across all Arco components.
//! Errors are structured for programmatic handling and include context for debugging.

use std::fmt;

/// The result type used throughout Arco.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in Arco operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An invalid identifier was provided.
    #[error("invalid identifier: {message}")]
    InvalidId {
        /// Description of what made the ID invalid.
        message: String,
    },

    /// A tenant isolation boundary was violated.
    #[error("tenant isolation violation: {message}")]
    TenantIsolation {
        /// Description of the violation.
        message: String,
    },

    /// A storage operation failed.
    #[error("storage error: {message}")]
    Storage {
        /// Description of the storage failure.
        message: String,
        /// The underlying cause, if any.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A serialization or deserialization error occurred.
    #[error("serialization error: {message}")]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// The requested resource was not found.
    #[error("not found: {resource_type} with id {id}")]
    ResourceNotFound {
        /// The type of resource that was not found.
        resource_type: &'static str,
        /// The identifier that was looked up.
        id: String,
    },

    /// A path or object was not found (simple variant for storage).
    #[error("not found: {0}")]
    NotFound(String),

    /// Invalid input was provided.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// A precondition for the operation was not met.
    #[error("precondition failed: {message}")]
    PreconditionFailed {
        /// Description of the failed precondition.
        message: String,
    },

    /// An internal error occurred that should not happen in normal operation.
    #[error("internal error: {message}")]
    Internal {
        /// Description of the internal error.
        message: String,
    },
}

impl Error {
    /// Creates a new storage error with the given message.
    #[must_use]
    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a new storage error with a source cause.
    #[must_use]
    pub fn storage_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Storage {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Creates a new resource not found error.
    #[must_use]
    pub fn resource_not_found(resource_type: &'static str, id: impl fmt::Display) -> Self {
        Self::ResourceNotFound {
            resource_type,
            id: id.to_string(),
        }
    }
}
