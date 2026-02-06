//! Audit event helpers for API layer.
//!
//! Provides convenience functions for emitting audit events with context
//! from API request handlers.

use arco_core::audit::{AuditAction, AuditEmitter, AuditEvent};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::config::AuditConfig;
use crate::context::RequestContext;
use crate::server::AppState;

pub const REASON_JWT_VALID: &str = "jwt_valid";
pub const REASON_MISSING_TOKEN: &str = "missing_token";
pub const REASON_INVALID_TOKEN: &str = "invalid_token";
pub const REASON_PATH_TRAVERSAL: &str = "path_traversal";
pub const REASON_NOT_IN_ALLOWLIST: &str = "not_in_allowlist";
pub const REASON_UNKNOWN_DOMAIN: &str = "unknown_domain";

type HmacSha256 = Hmac<Sha256>;

fn hash_actor(user_id: &str, config: &AuditConfig) -> String {
    let digest = config
        .actor_hmac_key_bytes()
        .and_then(|key| HmacSha256::new_from_slice(key).ok())
        .map_or_else(
            || Sha256::digest(user_id.as_bytes()),
            |mut mac| {
                mac.update(user_id.as_bytes());
                mac.finalize().into_bytes()
            },
        );
    let prefix = digest.get(..16).unwrap_or(&digest);
    format!("user:{}", hex::encode(prefix))
}

fn actor_from_ctx(ctx: &RequestContext, config: &AuditConfig) -> String {
    ctx.user_id
        .as_ref()
        .map_or_else(|| "anonymous".to_string(), |u| hash_actor(u, config))
}

pub fn emit_auth_allow(state: &AppState, ctx: &RequestContext, resource: &str) {
    let actor = actor_from_ctx(ctx, &state.config.audit);

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::AuthAllow)
        .actor(actor)
        .tenant_id(&ctx.tenant)
        .workspace_id(&ctx.workspace)
        .resource(resource)
        .decision_reason(REASON_JWT_VALID)
        .request_id(&ctx.request_id)
        .try_build()
    {
        state.audit().emit(event);
    }
}

pub fn emit_auth_deny(state: &AppState, request_id: &str, resource: &str, reason: &str) {
    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::AuthDeny)
        .actor("anonymous")
        .resource(resource)
        .decision_reason(reason)
        .request_id(request_id)
        .try_build()
    {
        state.audit().emit(event);
    }
}

pub fn emit_url_mint_allow(
    emitter: &AuditEmitter,
    ctx: &RequestContext,
    domain: &str,
    path_count: usize,
    config: &AuditConfig,
) {
    let actor = actor_from_ctx(ctx, config);
    let resource = format!("browser/urls:{}:{}", domain.to_lowercase(), path_count);

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::UrlMintAllow)
        .actor(actor)
        .tenant_id(&ctx.tenant)
        .workspace_id(&ctx.workspace)
        .resource(resource)
        .decision_reason("allowlist_match")
        .request_id(&ctx.request_id)
        .try_build()
    {
        emitter.emit(event);
    }
}

pub fn emit_url_mint_deny(
    emitter: &AuditEmitter,
    ctx: &RequestContext,
    domain: &str,
    reason: &str,
    config: &AuditConfig,
) {
    let actor = actor_from_ctx(ctx, config);
    let resource = format!("browser/urls:{}", domain.to_lowercase());

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::UrlMintDeny)
        .actor(actor)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::audit::TestAuditSink;
    use std::sync::Arc;

    fn make_test_ctx() -> RequestContext {
        RequestContext {
            tenant: "test-tenant".to_string(),
            workspace: "test-workspace".to_string(),
            user_id: Some("alice@example.com".to_string()),
            request_id: "req-123".to_string(),
            idempotency_key: None,
        }
    }

    fn default_config() -> AuditConfig {
        AuditConfig::default()
    }

    #[test]
    fn test_hash_actor_is_stable() {
        let config = default_config();
        let hash1 = hash_actor("alice@example.com", &config);
        let hash2 = hash_actor("alice@example.com", &config);
        assert_eq!(hash1, hash2);
        assert!(hash1.starts_with("user:"));
        assert!(!hash1.contains("alice"));
        assert!(!hash1.contains("@"));
    }

    #[test]
    fn test_hash_actor_different_inputs() {
        let config = default_config();
        let hash1 = hash_actor("alice@example.com", &config);
        let hash2 = hash_actor("bob@example.com", &config);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_actor_with_hmac_key() {
        use base64::Engine;
        let key =
            base64::engine::general_purpose::STANDARD.encode(b"test-secret-key-32-bytes-long!!");
        let mut config = AuditConfig::default();
        config.actor_hmac_key = Some(key);
        config.prepare();
        let hash = hash_actor("alice@example.com", &config);
        assert!(hash.starts_with("user:"));
        assert_eq!(hash.len(), 5 + 32);
    }

    #[test]
    fn test_hash_actor_hmac_differs_from_plain() {
        use base64::Engine;
        let plain_config = AuditConfig::default();
        let key =
            base64::engine::general_purpose::STANDARD.encode(b"test-secret-key-32-bytes-long!!");
        let mut hmac_config = AuditConfig::default();
        hmac_config.actor_hmac_key = Some(key);
        hmac_config.prepare();

        let plain_hash = hash_actor("alice@example.com", &plain_config);
        let hmac_hash = hash_actor("alice@example.com", &hmac_config);
        assert_ne!(plain_hash, hmac_hash);
    }

    #[test]
    fn test_emit_url_mint_allow() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());
        let ctx = make_test_ctx();
        let config = default_config();

        emit_url_mint_allow(&emitter, &ctx, "catalog", 5, &config);

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::UrlMintAllow);
        assert_eq!(events[0].resource, "browser/urls:catalog:5");
        assert!(events[0].actor.starts_with("user:"));
        assert!(!events[0].actor.contains("alice"));
    }

    #[test]
    fn test_emit_url_mint_deny() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());
        let ctx = make_test_ctx();
        let config = default_config();

        emit_url_mint_deny(&emitter, &ctx, "catalog", REASON_PATH_TRAVERSAL, &config);

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::UrlMintDeny);
        assert_eq!(events[0].decision_reason, "path_traversal");
    }

    #[test]
    fn test_anonymous_actor() {
        let ctx = RequestContext {
            tenant: "test".to_string(),
            workspace: "test".to_string(),
            user_id: None,
            request_id: "req".to_string(),
            idempotency_key: None,
        };
        let config = default_config();
        assert_eq!(actor_from_ctx(&ctx, &config), "anonymous");
    }
}
