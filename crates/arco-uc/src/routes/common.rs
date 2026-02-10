//! Shared UC route helpers.

use axum::extract::OriginalUri;
use axum::http::Method;

use arco_catalog::{CatalogError, CatalogReader};

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::error::UnityCatalogResult;
use crate::state::UnityCatalogState;

/// Returns a standardized UC `501` for known-but-unsupported operations.
pub(crate) fn known_but_unsupported(method: &Method, uri: &OriginalUri) -> UnityCatalogError {
    UnityCatalogError::NotImplemented {
        message: format!("operation not supported: {method} {}", uri.0.path()),
    }
}

pub(crate) fn catalog_reader(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> UnityCatalogResult<CatalogReader> {
    let scoped = ctx.scoped_storage(state.storage.clone())?;
    Ok(CatalogReader::new(scoped))
}

pub(crate) fn map_catalog_error(err: CatalogError) -> UnityCatalogError {
    match err {
        CatalogError::Validation { message } => UnityCatalogError::BadRequest { message },
        CatalogError::AlreadyExists { entity, name } => UnityCatalogError::Conflict {
            message: format!("already exists: {entity} {name}"),
        },
        CatalogError::NotFound { entity, name } => UnityCatalogError::NotFound {
            message: format!("not found: {entity} {name}"),
        },
        CatalogError::PreconditionFailed { message } | CatalogError::CasFailed { message } => {
            UnityCatalogError::Conflict { message }
        }
        CatalogError::UnsupportedOperation { message } => UnityCatalogError::NotImplemented {
            message: format!("unsupported operation: {message}"),
        },
        CatalogError::Storage { message }
        | CatalogError::Serialization { message }
        | CatalogError::Parquet { message }
        | CatalogError::InvariantViolation { message } => UnityCatalogError::Internal { message },
    }
}
