//! Shared UC route helpers.

use std::sync::Arc;

use arco_catalog::authz::decision::{AuthzDecision, AuthzRequest, DecisionOutcome};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogError, CatalogReader, CatalogWriter, Tier1Compactor};
use arco_core::{CatalogPaths, ControlPlaneScope, ScopedStorage};
use serde::{Deserialize, Deserializer};

use crate::context::UnityCatalogRequestContext;
use axum::extract::OriginalUri;
use axum::http::Method;

use crate::error::UnityCatalogError;
use crate::state::UnityCatalogState;

/// Returns a standardized UC `501` for known-but-unsupported operations.
pub(crate) fn known_but_unsupported(method: &Method, uri: &OriginalUri) -> UnityCatalogError {
    if let Some(message) = crate::support::unsupported_message(method, uri.0.path()) {
        return UnityCatalogError::NotImplemented { message };
    }
    UnityCatalogError::NotImplemented {
        message: format!("operation not supported: {method} {}", uri.0.path()),
    }
}

fn unity_catalog_error_for_status(http_status: u16, message: String) -> UnityCatalogError {
    match http_status {
        400 => UnityCatalogError::BadRequest { message },
        401 => UnityCatalogError::Unauthorized { message },
        403 => UnityCatalogError::Forbidden { message },
        404 => UnityCatalogError::NotFound { message },
        409 | 412 => UnityCatalogError::Conflict { message },
        429 => UnityCatalogError::TooManyRequests { message },
        501 => UnityCatalogError::NotImplemented { message },
        503 => UnityCatalogError::ServiceUnavailable { message },
        _ => UnityCatalogError::Internal { message },
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
        CatalogError::RequestFailed {
            http_status,
            message,
        } => unity_catalog_error_for_status(http_status, message),
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

#[allow(clippy::option_option)]
pub(crate) fn deserialize_nullable_patch_field<'de, D, T>(
    deserializer: D,
) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

pub(crate) fn scoped_storage(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<ScopedStorage, UnityCatalogError> {
    ctx.scoped_storage(state.storage.clone())
}

pub(crate) fn control_plane_scope(
    ctx: &UnityCatalogRequestContext,
) -> Result<ControlPlaneScope, UnityCatalogError> {
    ControlPlaneScope::workspace_alias(ctx.tenant.as_str(), ctx.workspace.as_str()).map_err(|err| {
        UnityCatalogError::BadRequest {
            message: err.to_string(),
        }
    })
}

pub(crate) fn authz_denial_reason(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    object_id: &str,
    object_type: &str,
    privilege: Privilege,
) -> Option<String> {
    authz_denial_reason_for_watermark(state, ctx, object_id, object_type, privilege, None)
}

pub(crate) fn authz_context_denial_reason_for_watermark(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    expected_ledger_watermark: Option<&str>,
) -> Option<String> {
    let Some(principal_id) = ctx.user_id.as_ref() else {
        return Some("unauthenticated_principal".to_string());
    };
    let Some(compiled_permissions) = state.compiled_permissions.as_ref() else {
        return Some("permissions_unavailable".to_string());
    };
    let Ok(compiled_permissions) = compiled_permissions.read() else {
        return Some("permissions_unavailable".to_string());
    };
    if !compiled_permissions.fresh {
        return Some("authz_stale_projection".to_string());
    }
    if let Some(expected_ledger_watermark) = expected_ledger_watermark {
        if compiled_permissions.ledger_watermark != expected_ledger_watermark {
            return Some("authz_stale_projection".to_string());
        }
    }
    if principal_id.is_empty() {
        return Some("unauthenticated_principal".to_string());
    }

    None
}

pub(crate) fn authz_denial_reason_for_watermark(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    object_id: &str,
    object_type: &str,
    privilege: Privilege,
    expected_ledger_watermark: Option<&str>,
) -> Option<String> {
    if let Some(reason_code) =
        authz_context_denial_reason_for_watermark(state, ctx, expected_ledger_watermark)
    {
        return Some(reason_code);
    }
    let Some(compiled_permissions) = state.compiled_permissions.as_ref() else {
        return Some("permissions_unavailable".to_string());
    };
    let Ok(compiled_permissions) = compiled_permissions.read() else {
        return Some("permissions_unavailable".to_string());
    };
    let Some(principal_id) = ctx.user_id.as_ref() else {
        return Some("unauthenticated_principal".to_string());
    };
    let request = AuthzRequest::new(
        principal_id.clone(),
        object_id.to_string(),
        object_type.to_string(),
        privilege,
    )
    .with_request_id(&ctx.request_id);
    let decision = AuthzDecision::evaluate(&request, &compiled_permissions);
    if decision.outcome == DecisionOutcome::Allow {
        None
    } else {
        Some(format!("authz_{}", decision.reason_code))
    }
}

pub(crate) fn require_authz(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    object_id: &str,
    object_type: &str,
    privilege: Privilege,
    message_prefix: &str,
) -> Result<(), UnityCatalogError> {
    if let Some(reason_code) = authz_denial_reason(state, ctx, object_id, object_type, privilege) {
        return Err(UnityCatalogError::Forbidden {
            message: format!("{message_prefix}:{reason_code}"),
        });
    }
    Ok(())
}

pub(crate) async fn authoritative_catalog_reader(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<Option<CatalogReader>, UnityCatalogError> {
    let storage = scoped_storage(state, ctx)?;
    let initialized = storage
        .head_raw(CatalogPaths::ROOT_MANIFEST)
        .await
        .map_err(|err| map_catalog_error(CatalogError::from(err)))?
        .is_some();

    Ok(initialized.then(|| CatalogReader::new(storage)))
}

pub(crate) async fn initialized_catalog_writer(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<CatalogWriter, UnityCatalogError> {
    let storage = scoped_storage(state, ctx)?;
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
    writer.initialize().await.map_err(map_catalog_error)?;
    Ok(writer)
}
