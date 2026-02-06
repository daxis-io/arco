//! Error types for `arco-delta`.

/// Result type for Delta operations.
pub type Result<T> = std::result::Result<T, DeltaError>;

/// Delta-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    /// Invalid input from a caller.
    #[error("bad request: {message}")]
    BadRequest {
        /// Human-readable error details.
        message: String,
    },

    /// Optimistic concurrency conflict (stale read version, concurrent writer, etc.).
    #[error("conflict: {message}")]
    Conflict {
        /// Human-readable conflict details.
        message: String,
    },

    /// The requested resource was not found.
    #[error("not found: {message}")]
    NotFound {
        /// Human-readable not-found details.
        message: String,
    },

    /// A storage operation failed.
    #[error(transparent)]
    Storage(#[from] arco_core::Error),

    /// Failed to serialize/deserialize JSON state.
    #[error("serialization error: {message}")]
    Serialization {
        /// Human-readable serialization details.
        message: String,
    },
}

impl DeltaError {
    /// Creates a bad request error.
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest {
            message: message.into(),
        }
    }

    /// Creates a conflict error.
    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    /// Creates a not found error.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
        }
    }

    pub(crate) fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization {
            message: message.into(),
        }
    }
}
