//! Shared UC route helpers.

use std::sync::Arc;

use arco_catalog::authz::compiler::CompiledPermissionSet;
use arco_catalog::authz::decision::{AuthzDecision, AuthzRequest, DecisionOutcome};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogError, CatalogReader, CatalogWriter, Tier1Compactor};
use arco_core::{CatalogPaths, ControlPlaneScope, ScopedStorage};
use serde::{Deserialize, Deserializer};

use crate::context::UnityCatalogRequestContext;
use axum::extract::OriginalUri;
use axum::http::{Method, Uri};

use crate::error::UnityCatalogError;
use crate::state::UnityCatalogState;

const PUBLIC_STORAGE_UNAVAILABLE_MESSAGE: &str = "Service temporarily unavailable";
const PUBLIC_INTERNAL_ERROR_MESSAGE: &str = "Internal server error";

/// Returns a standardized UC `501` for known-but-unsupported operations.
pub(crate) fn known_but_unsupported(
    method: &Method,
    uri: &Uri,
    original_uri: &OriginalUri,
) -> UnityCatalogError {
    let display_path = original_uri.0.path();
    if let Some(message) =
        crate::support::unsupported_message_for_display(method, uri.path(), display_path)
    {
        return UnityCatalogError::NotImplemented { message };
    }
    UnityCatalogError::NotImplemented {
        message: format!("operation not supported: {method} {display_path}"),
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
        CatalogError::Storage { message } => {
            tracing::warn!(internal_error = %message, "redacted UC storage error");
            UnityCatalogError::ServiceUnavailable {
                message: PUBLIC_STORAGE_UNAVAILABLE_MESSAGE.to_string(),
            }
        }
        CatalogError::Serialization { message }
        | CatalogError::Parquet { message }
        | CatalogError::InvariantViolation { message } => {
            tracing::warn!(internal_error = %message, "redacted UC internal error");
            UnityCatalogError::Internal {
                message: PUBLIC_INTERNAL_ERROR_MESSAGE.to_string(),
            }
        }
        error => {
            tracing::warn!(internal_error = %error, "redacted unknown UC catalog error");
            UnityCatalogError::Internal {
                message: PUBLIC_INTERNAL_ERROR_MESSAGE.to_string(),
            }
        }
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

/// Publishes the metastore projection at the current ledger watermark.
///
/// This is the production storage-governance projection publisher (#362):
/// every successful metastore ledger commit made by a UC governance route must
/// be followed by this call so that credential vending serves from a fresh
/// published projection instead of denying closed forever. The call is
/// idempotent and monotonic, so route handlers also invoke it *before*
/// validating a new mutation: any earlier commit whose publication failed is
/// healed by the next authorized governance request.
pub(crate) async fn publish_storage_governance_projection(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<(), UnityCatalogError> {
    let storage = scoped_storage(state, ctx)?;
    arco_catalog::metastore::publish::publish_current_metastore_projection(
        &storage,
        &arco_catalog::metastore::projections::ProjectionRegistry::default(),
    )
    .await
    .map(|_| ())
    .map_err(|error| {
        tracing::warn!(internal_error = %error, "storage governance projection publication failed");
        UnityCatalogError::ServiceUnavailable {
            message: "storage_governance_projection_publication_failed: committed metastore \
                      events remain durable; credential vending stays deny-closed until the \
                      projection is republished by a retried governance request or an admin \
                      POST /storage-governance/projection/republish"
                .to_string(),
        }
    })
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

/// Resolves the compiled permission view for this request scope.
///
/// Production wiring supplies a [`crate::permissions::CompiledPermissionSource`],
/// which compiles a scope-correct view from the scope's authoritative metastore
/// ledger; harnesses may instead pin a static view. Resolution failures return
/// `None` and every caller treats that as a denial, so an unreachable or
/// unreadable permission source is fail-closed, never fail-open.
pub(crate) async fn resolve_compiled_permissions(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Option<Arc<CompiledPermissionSet>> {
    if let Some(source) = state.permission_source.as_ref() {
        return match source
            .compiled_permissions(ctx.tenant.as_str(), ctx.workspace.as_str())
            .await
        {
            Ok(permissions) => Some(permissions),
            Err(error) => {
                tracing::warn!(
                    internal_error = %error,
                    request_id = %ctx.request_id,
                    "compiled permission view unavailable; denying closed"
                );
                None
            }
        };
    }
    let compiled = state.compiled_permissions.as_ref()?;
    let guard = compiled.read().ok()?;
    Some(Arc::clone(&guard))
}

pub(crate) fn authz_context_denial_reason_for_watermark(
    permissions: Option<&CompiledPermissionSet>,
    ctx: &UnityCatalogRequestContext,
    expected_ledger_watermark: Option<&str>,
) -> Option<String> {
    let Some(principal_id) = ctx.user_id.as_ref() else {
        return Some("unauthenticated_principal".to_string());
    };
    let Some(compiled_permissions) = permissions else {
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
    permissions: Option<&CompiledPermissionSet>,
    ctx: &UnityCatalogRequestContext,
    object_id: &str,
    object_type: &str,
    privilege: Privilege,
    expected_ledger_watermark: Option<&str>,
) -> Option<String> {
    if let Some(reason_code) =
        authz_context_denial_reason_for_watermark(permissions, ctx, expected_ledger_watermark)
    {
        return Some(reason_code);
    }
    let Some(compiled_permissions) = permissions else {
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
    let decision = AuthzDecision::evaluate(&request, compiled_permissions);
    if decision.outcome == DecisionOutcome::Allow {
        None
    } else {
        Some(format!("authz_{}", decision.reason_code))
    }
}

/// Authorizes a privilege on a securable, resolving the request scope's
/// compiled permission view first.
pub(crate) async fn require_authz(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    object_id: &str,
    object_type: &str,
    privilege: Privilege,
    message_prefix: &str,
) -> Result<(), UnityCatalogError> {
    let permissions = resolve_compiled_permissions(state, ctx).await;
    if let Some(reason_code) = authz_denial_reason_for_watermark(
        permissions.as_deref(),
        ctx,
        object_id,
        object_type,
        privilege,
        None,
    ) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_errors_do_not_expose_internal_details() {
        let err = map_catalog_error(CatalogError::Storage {
            message: "gcs bucket prod-secret path tenant=acme/workspace=analytics/token"
                .to_string(),
        });

        match err {
            UnityCatalogError::ServiceUnavailable { message }
            | UnityCatalogError::Internal { message } => {
                assert_eq!(message, "Service temporarily unavailable");
                assert!(!message.contains("prod-secret"));
                assert!(!message.contains("tenant=acme"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
