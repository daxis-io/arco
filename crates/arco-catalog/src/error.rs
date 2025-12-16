//! Error types for arco-catalog operations.

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

    /// CAS (Compare-And-Swap) operation failed due to concurrent modification.
    #[error("CAS failed: {message}")]
    CasFailed {
        /// Description of the CAS failure.
        message: String,
    },

    /// Resource not found.
    #[error("not found: {message}")]
    NotFound {
        /// Description of what was not found.
        message: String,
    },
}
