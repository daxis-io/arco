//! JWT verification and claim extraction.
//!
//! In production (`Config.debug = false`), the API requires an `Authorization: Bearer <jwt>`
//! header. This module verifies the JWT signature and validates standard claims, then
//! extracts tenant/workspace identifiers from configured claim names.

use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::config::JwtConfig;
use crate::context::{AuthError, TenantId, WorkspaceId};

#[derive(Debug, Clone)]
struct CachedJwks {
    set: Arc<JwkSet>,
    fetched_at: Instant,
}

impl CachedJwks {
    fn is_fresh(&self, ttl: Duration) -> bool {
        self.fetched_at.elapsed() < ttl
    }
}

/// JWT verification service used by request context extraction.
#[derive(Debug)]
pub struct JwtVerifier {
    config: JwtConfig,
    jwks_cache: RwLock<Option<CachedJwks>>,
    http: reqwest::Client,
}

impl JwtVerifier {
    pub fn new(config: JwtConfig) -> Self {
        let http = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Failed to configure reqwest client; using defaults"
                );
                reqwest::Client::new()
            }
        };

        Self {
            config,
            jwks_cache: RwLock::new(None),
            http,
        }
    }

    pub async fn verify(&self, token: &str) -> Result<(TenantId, WorkspaceId), AuthError> {
        let claims = if let Some(secret) = self.config.hs256_secret.as_ref() {
            self.decode_hs256(token, secret)?
        } else if self.config.jwks_url.is_some() {
            self.decode_with_jwks(token).await?
        } else {
            tracing::error!("JWT auth is enabled but no signing key source is configured");
            return Err(AuthError::invalid_token());
        };

        let (tenant, workspace) = self.extract_scope(&claims)?;
        Ok((tenant, workspace))
    }

    fn decode_hs256(&self, token: &str, secret: &str) -> Result<Value, AuthError> {
        let mut validation = self.validation_for(Algorithm::HS256);
        validation.algorithms = vec![Algorithm::HS256];

        decode::<Value>(
            token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )
        .map(|t| t.claims)
        .map_err(|_| AuthError::invalid_token())
    }

    async fn decode_with_jwks(&self, token: &str) -> Result<Value, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::invalid_token())?;
        let kid = header.kid.ok_or_else(AuthError::invalid_token)?;

        let jwk = self
            .get_jwk(&kid)
            .await?
            .ok_or_else(AuthError::invalid_token)?;

        let decoding_key = DecodingKey::from_jwk(&jwk).map_err(|_| AuthError::invalid_token())?;

        // Restrict algorithms to commonly used JWKS algorithms.
        // This prevents `alg` confusion across token types.
        let mut validation = self.validation_for(Algorithm::RS256);
        validation.algorithms = vec![Algorithm::RS256, Algorithm::ES256];

        decode::<Value>(token, &decoding_key, &validation)
            .map(|t| t.claims)
            .map_err(|_| AuthError::invalid_token())
    }

    fn validation_for(&self, algorithm: Algorithm) -> Validation {
        let mut validation = Validation::new(algorithm);
        validation.leeway = self.config.leeway_seconds;

        if let Some(aud) = self.config.audience.as_deref() {
            validation.set_audience(&[aud]);
            validation.set_required_spec_claims(&["exp", "aud"]);
        }

        if let Some(iss) = self.config.issuer.as_deref() {
            validation.set_issuer(&[iss]);

            // Ensure `iss` is required even if `aud` isn't configured.
            if self.config.audience.is_none() {
                validation.set_required_spec_claims(&["exp", "iss"]);
            } else {
                validation.required_spec_claims.insert("iss".to_string());
            }
        }

        validation
    }

    fn extract_scope(&self, claims: &Value) -> Result<(TenantId, WorkspaceId), AuthError> {
        let Some(obj) = claims.as_object() else {
            return Err(AuthError::invalid_token());
        };

        let tenant = obj
            .get(&self.config.tenant_claim)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(TenantId::new)
            .ok_or_else(AuthError::missing_claims)?;

        let workspace = obj
            .get(&self.config.workspace_claim)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(WorkspaceId::new)
            .ok_or_else(AuthError::missing_claims)?;

        Ok((tenant, workspace))
    }

    async fn get_jwk(&self, kid: &str) -> Result<Option<jsonwebtoken::jwk::Jwk>, AuthError> {
        let ttl = Duration::from_secs(self.config.jwks_cache_ttl_seconds);

        // Fast path: read cached set.
        if let Some(jwk) = self.cached_jwk(kid, ttl).await {
            return Ok(Some(jwk));
        }

        // Slow path: refresh cache and retry.
        self.refresh_jwks().await?;
        Ok(self.cached_jwk(kid, ttl).await)
    }

    async fn cached_jwk(&self, kid: &str, ttl: Duration) -> Option<jsonwebtoken::jwk::Jwk> {
        let cache = self.jwks_cache.read().await;
        let set = match cache.as_ref() {
            Some(cached) if cached.is_fresh(ttl) => Arc::clone(&cached.set),
            _ => return None,
        };
        drop(cache);

        set.keys
            .iter()
            .find(|k| k.common.key_id.as_deref() == Some(kid))
            .cloned()
    }

    async fn refresh_jwks(&self) -> Result<(), AuthError> {
        let Some(url) = self.config.jwks_url.as_deref() else {
            return Ok(());
        };

        let set = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|_| AuthError::invalid_token())?
            .error_for_status()
            .map_err(|_| AuthError::invalid_token())?
            .json::<JwkSet>()
            .await
            .map_err(|_| AuthError::invalid_token())?;

        *self.jwks_cache.write().await = Some(CachedJwks {
            set: Arc::new(set),
            fetched_at: Instant::now(),
        });
        Ok(())
    }
}
