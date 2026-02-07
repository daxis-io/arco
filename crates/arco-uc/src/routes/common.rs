//! Shared UC route helpers.

use axum::extract::OriginalUri;
use axum::http::Method;

use crate::error::UnityCatalogError;

/// Returns a standardized UC `501` for known-but-unsupported operations.
pub(crate) async fn known_but_unsupported(method: Method, uri: OriginalUri) -> UnityCatalogError {
    UnityCatalogError::NotImplemented {
        message: format!("operation not supported: {method} {}", uri.0.path()),
    }
}
