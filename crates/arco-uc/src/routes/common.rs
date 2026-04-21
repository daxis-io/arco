//! Shared UC route helpers.

use std::sync::Arc;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogError, CatalogWriter, Tier1Compactor};

use crate::context::UnityCatalogRequestContext;
use axum::extract::OriginalUri;
use axum::http::Method;

use crate::error::UnityCatalogError;
use crate::state::UnityCatalogState;

/// Returns a standardized UC `501` for known-but-unsupported operations.
pub(crate) fn known_but_unsupported(method: &Method, uri: &OriginalUri) -> UnityCatalogError {
    UnityCatalogError::NotImplemented {
        message: format!("operation not supported: {method} {}", uri.0.path()),
    }
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

pub(crate) fn writer_options(ctx: &UnityCatalogRequestContext) -> WriteOptions {
    let options = WriteOptions::default()
        .with_actor(format!("uc:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    }
}

pub(crate) async fn initialized_catalog_writer(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<CatalogWriter, UnityCatalogError> {
    let storage = ctx.scoped_storage(state.storage.clone())?;
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
    writer.initialize().await.map_err(map_catalog_error)?;
    Ok(writer)
}
