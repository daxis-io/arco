//! Internal OIDC verification helpers for service-to-service mutation routes.
//!
//! This module validates Google-style OIDC ID tokens for internal HTTP callbacks.
//! It enforces:
//! - Signature verification (JWKS or optional HS256 test secret)
//! - Required issuer and audience checks
//! - Principal allowlisting via `sub` and/or `email`

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::HeaderMap;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::error::Error;

const DEFAULT_GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const DEFAULT_JWKS_TTL_SECS: u64 = 300;

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

/// Internal OIDC verifier configuration.
#[derive(Debug, Clone)]
pub struct InternalOidcConfig {
    /// Required `iss` claim.
    pub issuer: String,
    /// Required `aud` claim.
    pub audience: String,
    /// Allowed token subjects (`sub`).
    pub allowed_subs: BTreeSet<String>,
    /// Allowed token emails (`email`).
    pub allowed_emails: BTreeSet<String>,
    /// Whether auth failures should hard-deny requests.
    pub enforce: bool,
    /// JWKS endpoint URL.
    pub jwks_url: String,
    /// Optional HS256 secret (primarily for tests/dev).
    pub hs256_secret: Option<String>,
    /// JWKS cache TTL.
    pub jwks_cache_ttl: Duration,
}

impl InternalOidcConfig {
    /// Builds internal OIDC configuration from environment variables.
    ///
    /// Returns `Ok(None)` when internal auth is disabled (no relevant env vars set).
    ///
    /// Supported env vars:
    /// - `ARCO_INTERNAL_AUTH_ISSUER`
    /// - `ARCO_INTERNAL_AUTH_AUDIENCE`
    /// - `ARCO_INTERNAL_AUTH_ALLOWED_SUBS` (comma-separated)
    /// - `ARCO_INTERNAL_AUTH_ALLOWED_EMAILS` (comma-separated)
    /// - `ARCO_INTERNAL_AUTH_ENFORCE` (`true`/`false`)
    /// - `ARCO_INTERNAL_AUTH_JWKS_URL` (optional)
    /// - `ARCO_INTERNAL_AUTH_HS256_SECRET` (optional; tests/dev)
    /// - `ARCO_INTERNAL_AUTH_JWKS_CACHE_TTL_SECS` (optional)
    ///
    /// # Errors
    ///
    /// Returns an error when required variables are missing or malformed.
    pub fn from_env() -> Result<Option<Self>, Error> {
        let enforce = parse_env_bool("ARCO_INTERNAL_AUTH_ENFORCE", false)?;
        let issuer = env_string("ARCO_INTERNAL_AUTH_ISSUER");
        let audience = env_string("ARCO_INTERNAL_AUTH_AUDIENCE");
        let allowed_subs = parse_csv_set(env_string("ARCO_INTERNAL_AUTH_ALLOWED_SUBS"));
        let allowed_emails = parse_csv_set(env_string("ARCO_INTERNAL_AUTH_ALLOWED_EMAILS"));

        let enabled = enforce
            || issuer.is_some()
            || audience.is_some()
            || !allowed_subs.is_empty()
            || !allowed_emails.is_empty();
        if !enabled {
            return Ok(None);
        }

        let issuer = issuer.ok_or_else(|| {
            Error::InvalidInput(
                "ARCO_INTERNAL_AUTH_ISSUER is required when internal auth is enabled".to_string(),
            )
        })?;
        let audience = audience.ok_or_else(|| {
            Error::InvalidInput(
                "ARCO_INTERNAL_AUTH_AUDIENCE is required when internal auth is enabled".to_string(),
            )
        })?;
        if allowed_subs.is_empty() && allowed_emails.is_empty() {
            return Err(Error::InvalidInput(
                "ARCO_INTERNAL_AUTH_ALLOWED_SUBS or ARCO_INTERNAL_AUTH_ALLOWED_EMAILS is required when internal auth is enabled".to_string(),
            ));
        }

        let jwks_url = env_string("ARCO_INTERNAL_AUTH_JWKS_URL")
            .unwrap_or_else(|| DEFAULT_GOOGLE_JWKS_URL.to_string());
        let hs256_secret = env_string("ARCO_INTERNAL_AUTH_HS256_SECRET");
        let jwks_cache_ttl = Duration::from_secs(parse_env_u64(
            "ARCO_INTERNAL_AUTH_JWKS_CACHE_TTL_SECS",
            DEFAULT_JWKS_TTL_SECS,
        )?);

        Ok(Some(Self {
            issuer,
            audience,
            allowed_subs,
            allowed_emails,
            enforce,
            jwks_url,
            hs256_secret,
            jwks_cache_ttl,
        }))
    }

    /// Creates a test-friendly HS256 config.
    #[must_use]
    pub fn hs256_for_tests(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        secret: impl Into<String>,
        allowed_subs: BTreeSet<String>,
        allowed_emails: BTreeSet<String>,
        enforce: bool,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            allowed_subs,
            allowed_emails,
            enforce,
            jwks_url: DEFAULT_GOOGLE_JWKS_URL.to_string(),
            hs256_secret: Some(secret.into()),
            jwks_cache_ttl: Duration::from_secs(DEFAULT_JWKS_TTL_SECS),
        }
    }
}

/// Claims extracted from a validated internal OIDC token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    /// Subject claim (`sub`), if present.
    pub subject: Option<String>,
    /// Email claim (`email`), if present.
    pub email: Option<String>,
    /// Authorized party (`azp`), if present.
    pub azp: Option<String>,
}

/// Internal auth verification error.
#[derive(Debug, thiserror::Error)]
pub enum InternalOidcError {
    /// Authorization header is missing or malformed.
    #[error("missing bearer token")]
    MissingBearerToken,
    /// Token is invalid.
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// Token principal is not allowlisted.
    #[error("principal not allowlisted")]
    PrincipalNotAllowlisted,
    /// Failed to fetch or parse JWKS.
    #[error("jwks refresh failed: {0}")]
    JwksRefresh(String),
}

/// OIDC verifier for internal service endpoints.
#[derive(Debug)]
pub struct InternalOidcVerifier {
    config: InternalOidcConfig,
    jwks_cache: RwLock<Option<CachedJwks>>,
    http: reqwest::Client,
}

impl InternalOidcVerifier {
    /// Creates a new verifier from config.
    ///
    /// # Errors
    ///
    /// Returns an error when required config fields are invalid or the HTTP client cannot be
    /// constructed.
    pub fn new(config: InternalOidcConfig) -> Result<Self, Error> {
        if config.issuer.trim().is_empty() {
            return Err(Error::InvalidInput(
                "internal oidc issuer must not be empty".to_string(),
            ));
        }
        if config.audience.trim().is_empty() {
            return Err(Error::InvalidInput(
                "internal oidc audience must not be empty".to_string(),
            ));
        }
        if config.allowed_subs.is_empty() && config.allowed_emails.is_empty() {
            return Err(Error::InvalidInput(
                "at least one internal oidc allowlist is required".to_string(),
            ));
        }

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::Internal {
                message: format!("failed to build internal oidc http client: {e}"),
            })?;

        Ok(Self {
            config,
            jwks_cache: RwLock::new(None),
            http,
        })
    }

    /// Returns whether failures should be enforced as hard denies.
    #[must_use]
    pub const fn enforce(&self) -> bool {
        self.config.enforce
    }

    /// Validates the bearer token in request headers.
    ///
    /// # Errors
    ///
    /// Returns an error when the header is missing, malformed, or token verification fails.
    pub async fn verify_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<VerifiedPrincipal, InternalOidcError> {
        let token = extract_bearer_token(headers).ok_or(InternalOidcError::MissingBearerToken)?;
        self.verify_token(&token).await
    }

    /// Validates a raw JWT token.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails or the resulting principal is not allowlisted.
    pub async fn verify_token(&self, token: &str) -> Result<VerifiedPrincipal, InternalOidcError> {
        let claims = if let Some(secret) = self.config.hs256_secret.as_deref() {
            self.decode_hs256(token, secret)?
        } else {
            self.decode_with_jwks(token).await?
        };

        self.extract_and_validate_principal(&claims)
    }

    fn decode_hs256(&self, token: &str, secret: &str) -> Result<Value, InternalOidcError> {
        let mut validation = self.validation_for(Algorithm::HS256);
        validation.algorithms = vec![Algorithm::HS256];

        decode::<Value>(
            token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )
        .map(|data| data.claims)
        .map_err(|e| InternalOidcError::InvalidToken(e.to_string()))
    }

    async fn decode_with_jwks(&self, token: &str) -> Result<Value, InternalOidcError> {
        let header =
            decode_header(token).map_err(|e| InternalOidcError::InvalidToken(e.to_string()))?;
        let kid = header
            .kid
            .ok_or_else(|| InternalOidcError::InvalidToken("missing kid".to_string()))?;
        let algorithm = match header.alg {
            Algorithm::RS256 => Algorithm::RS256,
            Algorithm::ES256 => Algorithm::ES256,
            other => {
                return Err(InternalOidcError::InvalidToken(format!(
                    "unsupported algorithm: {other:?}"
                )));
            }
        };

        let jwk = self
            .get_jwk(&kid)
            .await?
            .ok_or_else(|| InternalOidcError::InvalidToken("unknown kid".to_string()))?;
        let decoding_key = DecodingKey::from_jwk(&jwk)
            .map_err(|e| InternalOidcError::InvalidToken(format!("invalid jwk: {e}")))?;

        let mut validation = self.validation_for(algorithm);
        validation.algorithms = vec![algorithm];

        decode::<Value>(token, &decoding_key, &validation)
            .map(|data| data.claims)
            .map_err(|e| InternalOidcError::InvalidToken(e.to_string()))
    }

    fn validation_for(&self, algorithm: Algorithm) -> Validation {
        let mut validation = Validation::new(algorithm);
        validation.validate_nbf = true;
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation
    }

    fn extract_and_validate_principal(
        &self,
        claims: &Value,
    ) -> Result<VerifiedPrincipal, InternalOidcError> {
        let Some(object) = claims.as_object() else {
            return Err(InternalOidcError::InvalidToken(
                "claims payload must be a json object".to_string(),
            ));
        };

        let subject = object
            .get("sub")
            .and_then(Value::as_str)
            .map(str::to_string);
        let email = object
            .get("email")
            .and_then(Value::as_str)
            .map(str::to_string);
        let azp = object
            .get("azp")
            .and_then(Value::as_str)
            .map(str::to_string);

        let allowed = subject
            .as_deref()
            .is_some_and(|sub| self.config.allowed_subs.contains(sub))
            || email
                .as_deref()
                .is_some_and(|mail| self.config.allowed_emails.contains(mail));
        if !allowed {
            return Err(InternalOidcError::PrincipalNotAllowlisted);
        }

        Ok(VerifiedPrincipal {
            subject,
            email,
            azp,
        })
    }

    async fn get_jwk(
        &self,
        kid: &str,
    ) -> Result<Option<jsonwebtoken::jwk::Jwk>, InternalOidcError> {
        if let Some(jwk) = self.cached_jwk(kid).await {
            return Ok(Some(jwk));
        }

        self.refresh_jwks().await?;
        Ok(self.cached_jwk(kid).await)
    }

    async fn cached_jwk(&self, kid: &str) -> Option<jsonwebtoken::jwk::Jwk> {
        let cache = self.jwks_cache.read().await;
        let set = match cache.as_ref() {
            Some(cached) if cached.is_fresh(self.config.jwks_cache_ttl) => Arc::clone(&cached.set),
            _ => return None,
        };
        drop(cache);

        set.keys
            .iter()
            .find(|jwk| jwk.common.key_id.as_deref() == Some(kid))
            .cloned()
    }

    async fn refresh_jwks(&self) -> Result<(), InternalOidcError> {
        let set = self
            .http
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|e| InternalOidcError::JwksRefresh(e.to_string()))?
            .error_for_status()
            .map_err(|e| InternalOidcError::JwksRefresh(e.to_string()))?
            .json::<JwkSet>()
            .await
            .map_err(|e| InternalOidcError::JwksRefresh(e.to_string()))?;

        *self.jwks_cache.write().await = Some(CachedJwks {
            set: Arc::new(set),
            fetched_at: Instant::now(),
        });
        Ok(())
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .and_then(|value| if value.is_empty() { None } else { Some(value) })
}

fn parse_csv_set(input: Option<String>) -> BTreeSet<String> {
    let Some(input) = input else {
        return BTreeSet::new();
    };

    input
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_env_bool(key: &str, default: bool) -> Result<bool, Error> {
    let Some(value) = std::env::var(key).ok() else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" => Ok(true),
        "0" | "false" | "no" | "n" => Ok(false),
        _ => Err(Error::InvalidInput(format!(
            "{key} must be a boolean (true/false/1/0)"
        ))),
    }
}

fn parse_env_u64(key: &str, default: u64) -> Result<u64, Error> {
    let Some(value) = std::env::var(key).ok() else {
        return Ok(default);
    };
    value
        .parse::<u64>()
        .map_err(|_| Error::InvalidInput(format!("{key} must be an unsigned integer")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header};

    fn signed_token(secret: &str, claims: Value) -> Result<String, Box<dyn std::error::Error>> {
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )?;
        Ok(token)
    }

    fn test_verifier() -> InternalOidcVerifier {
        let allowed_subs = [String::from("svc-allowed")]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let config = InternalOidcConfig::hs256_for_tests(
            "https://accounts.google.com",
            "https://internal.run.app",
            "test-secret",
            allowed_subs,
            BTreeSet::new(),
            true,
        );
        InternalOidcVerifier::new(config).expect("test verifier")
    }

    #[tokio::test]
    async fn allowlisted_principal_passes() {
        let verifier = test_verifier();
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp();
        let claims = serde_json::json!({
            "sub": "svc-allowed",
            "iss": "https://accounts.google.com",
            "aud": "https://internal.run.app",
            "exp": exp
        });
        let token = signed_token("test-secret", claims).expect("token");
        let principal = verifier.verify_token(&token).await.expect("principal");
        assert_eq!(principal.subject.as_deref(), Some("svc-allowed"));
    }

    #[tokio::test]
    async fn wrong_issuer_is_rejected() {
        let verifier = test_verifier();
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp();
        let claims = serde_json::json!({
            "sub": "svc-allowed",
            "iss": "https://wrong-issuer.example",
            "aud": "https://internal.run.app",
            "exp": exp
        });
        let token = signed_token("test-secret", claims).expect("token");
        let result = verifier.verify_token(&token).await;
        assert!(matches!(result, Err(InternalOidcError::InvalidToken(_))));
    }

    #[tokio::test]
    async fn wrong_audience_is_rejected() {
        let verifier = test_verifier();
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp();
        let claims = serde_json::json!({
            "sub": "svc-allowed",
            "iss": "https://accounts.google.com",
            "aud": "https://wrong-aud.example",
            "exp": exp
        });
        let token = signed_token("test-secret", claims).expect("token");
        let result = verifier.verify_token(&token).await;
        assert!(matches!(result, Err(InternalOidcError::InvalidToken(_))));
    }

    #[tokio::test]
    async fn wrong_signature_is_rejected() {
        let verifier = test_verifier();
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp();
        let claims = serde_json::json!({
            "sub": "svc-allowed",
            "iss": "https://accounts.google.com",
            "aud": "https://internal.run.app",
            "exp": exp
        });
        let token = signed_token("different-secret", claims).expect("token");
        let result = verifier.verify_token(&token).await;
        assert!(matches!(result, Err(InternalOidcError::InvalidToken(_))));
    }

    #[tokio::test]
    async fn non_allowlisted_principal_is_rejected() {
        let verifier = test_verifier();
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp();
        let claims = serde_json::json!({
            "sub": "svc-other",
            "iss": "https://accounts.google.com",
            "aud": "https://internal.run.app",
            "exp": exp
        });
        let token = signed_token("test-secret", claims).expect("token");
        let result = verifier.verify_token(&token).await;
        assert!(matches!(
            result,
            Err(InternalOidcError::PrincipalNotAllowlisted)
        ));
    }
}
