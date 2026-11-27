//! Audit helpers for UC facade authz and credential decisions.

use crate::context::UnityCatalogRequestContext;
use crate::state::UnityCatalogState;
use arco_catalog::credential_vending::CredentialVendingDecision;
use arco_core::audit::{AuditAction, AuditEvent};
use sha2::{Digest, Sha256};

/// Authz allow reason.
pub const REASON_PERMISSIONS_ALLOW: &str = "permissions_allow";
/// Authz deny reason.
pub const REASON_PERMISSIONS_DENY: &str = "permissions_deny";
/// Credential vending allow reason.
pub const REASON_CREDENTIALS_VENDED: &str = "credentials_vended";
/// Credential vending deny reason.
pub const REASON_CREDENTIALS_DENIED: &str = "credentials_denied";

fn actor_from_ctx(ctx: &UnityCatalogRequestContext) -> String {
    ctx.user_id.as_deref().map_or_else(
        || "anonymous".to_string(),
        |id| format!("user:{}", hex::encode(Sha256::digest(id))),
    )
}

fn emit_event(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    action: AuditAction,
    resource: String,
    reason: &str,
) {
    let Some(emitter) = state.audit_emitter.as_ref() else {
        return;
    };

    if let Ok(event) = AuditEvent::builder()
        .action(action)
        .actor(actor_from_ctx(ctx))
        .tenant_id(&ctx.tenant)
        .workspace_id(&ctx.workspace)
        .resource(resource)
        .decision_reason(reason)
        .request_id(&ctx.request_id)
        .try_build()
    {
        emitter.emit(event);
    }
}

/// Emits an authorization allow event.
pub fn emit_permissions_allow(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    securable_type: &str,
    full_name: &str,
) {
    emit_event(
        state,
        ctx,
        AuditAction::AuthAllow,
        format!("uc_permissions:{securable_type}:{full_name}"),
        REASON_PERMISSIONS_ALLOW,
    );
}

/// Emits an authorization deny event.
pub fn emit_permissions_deny(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    securable_type: &str,
    full_name: &str,
) {
    emit_event(
        state,
        ctx,
        AuditAction::AuthDeny,
        format!("uc_permissions:{securable_type}:{full_name}"),
        REASON_PERMISSIONS_DENY,
    );
}

/// Emits a credential vending allow event.
pub fn emit_credentials_allow(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    resource: &str,
    reason: &str,
) {
    emit_event(
        state,
        ctx,
        AuditAction::CredVendAllow,
        resource.to_string(),
        reason,
    );
}

/// Emits a credential vending deny event.
pub fn emit_credentials_deny(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    resource: &str,
    reason: &str,
) {
    let resource = if resource.contains("://") {
        format!("path:{}", hex::encode(Sha256::digest(resource)))
    } else {
        resource.to_string()
    };
    emit_event(state, ctx, AuditAction::CredVendDeny, resource, reason);
}

pub(crate) fn emit_table_read_allow(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    table_id: &str,
    snapshot_id: &str,
    permission_watermark: &str,
    decision: &CredentialVendingDecision,
) {
    let Some(emitter) = state.audit_emitter.as_ref() else {
        return;
    };
    if let Ok(mut event) = AuditEvent::builder()
        .action(AuditAction::AuthAllow)
        .actor(actor_from_ctx(ctx))
        .tenant_id(&ctx.tenant)
        .workspace_id(&ctx.workspace)
        .resource(format!("uc_table_read:{table_id}:{snapshot_id}"))
        .decision_reason(&decision.reason_code)
        .policy_version(permission_watermark)
        .request_id(&ctx.request_id)
        .try_build()
    {
        event.event_id.clone_from(&decision.audit_event_id);
        emitter.emit(event);
    }
}
