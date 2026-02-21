//! Shared task-token minting and validation helpers.

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Default task-token TTL in seconds.
pub const DEFAULT_TASK_TOKEN_TTL_SECONDS: u64 = 3_600;

/// Default dispatch execution timeout budget in seconds (Cloud Tasks default).
pub const DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS: u64 = 1_800;

/// Extra callback grace budget in seconds added on top of task timeout.
pub const TASK_TOKEN_CALLBACK_GRACE_SECONDS: u64 = 300;

/// Maximum supported task-token TTL in seconds.
pub const MAX_TASK_TOKEN_TTL_SECONDS: u64 = 86_400;

/// Configuration for task-scoped callback authentication tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskTokenConfig {
    /// HS256 secret for signing and validating tokens.
    #[serde(default)]
    pub hs256_secret: String,
    /// Optional issuer (`iss`) to enforce.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Optional audience (`aud`) to enforce.
    #[serde(default)]
    pub audience: Option<String>,
    /// Token time-to-live in seconds.
    #[serde(default = "default_task_token_ttl_seconds")]
    pub ttl_seconds: u64,
}

impl Default for TaskTokenConfig {
    fn default() -> Self {
        Self {
            hs256_secret: String::new(),
            issuer: None,
            audience: None,
            ttl_seconds: default_task_token_ttl_seconds(),
        }
    }
}

const fn default_task_token_ttl_seconds() -> u64 {
    DEFAULT_TASK_TOKEN_TTL_SECONDS
}

impl TaskTokenConfig {
    /// Returns the configured token TTL as a duration.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        Duration::seconds(i64::try_from(self.ttl_seconds).unwrap_or(i64::MAX))
    }

    /// Returns true when task tokens are configured.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.hs256_secret.trim().is_empty()
    }

    /// Validates configuration sanity.
    ///
    /// # Errors
    ///
    /// Returns an error when required fields are missing or out of range.
    pub fn validate(&self) -> Result<()> {
        if self.hs256_secret.trim().is_empty() {
            return Err(Error::InvalidInput(
                "task_token.hs256_secret is required".to_string(),
            ));
        }
        if self.ttl_seconds == 0 {
            return Err(Error::InvalidInput(
                "task_token.ttl_seconds must be greater than zero".to_string(),
            ));
        }
        if self.ttl_seconds > MAX_TASK_TOKEN_TTL_SECONDS {
            return Err(Error::InvalidInput(format!(
                "task_token.ttl_seconds must be at most {MAX_TASK_TOKEN_TTL_SECONDS}"
            )));
        }
        Ok(())
    }

    /// Validates config for dispatcher/sweeper callback token minting.
    ///
    /// This enforces:
    /// - base token sanity (`validate`)
    /// - optional issuer/audience requirement for strict deployments
    /// - token lifetime budget large enough to cover task timeout + callback grace
    ///
    /// # Errors
    ///
    /// Returns an error when dispatch callback token settings are unsafe.
    pub fn validate_for_dispatch(
        &self,
        task_timeout_seconds: u64,
        require_issuer_audience: bool,
    ) -> Result<()> {
        self.validate()?;

        if task_timeout_seconds == 0 {
            return Err(Error::InvalidInput(
                "dispatch task timeout must be greater than zero".to_string(),
            ));
        }

        if require_issuer_audience {
            if self
                .issuer
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(Error::InvalidInput(
                    "task_token.issuer is required for dispatch token validation".to_string(),
                ));
            }
            if self
                .audience
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(Error::InvalidInput(
                    "task_token.audience is required for dispatch token validation".to_string(),
                ));
            }
        }

        let required_ttl = task_timeout_seconds.saturating_add(TASK_TOKEN_CALLBACK_GRACE_SECONDS);
        if self.ttl_seconds < required_ttl {
            return Err(Error::InvalidInput(format!(
                "task_token.ttl_seconds ({}) must be at least task timeout + callback grace ({required_ttl})",
                self.ttl_seconds
            )));
        }

        Ok(())
    }
}

/// Canonical task-token claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskTokenClaims {
    /// Task identifier used by callback endpoints (`/tasks/{task_id}/...`).
    #[serde(alias = "task_id")]
    pub task_id: String,
    /// Tenant identifier.
    #[serde(alias = "tenant_id", alias = "tenant")]
    pub tenant_id: String,
    /// Workspace identifier.
    #[serde(alias = "workspace_id", alias = "workspace")]
    pub workspace_id: String,
    /// Expiry (unix timestamp seconds).
    pub exp: usize,
    /// Not-before (unix timestamp seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<usize>,
    /// Issued-at (unix timestamp seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<usize>,
    /// Optional issuer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    /// Optional audience.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
}

/// Result of minting a task token.
#[derive(Debug, Clone)]
pub struct MintedTaskToken {
    /// Signed JWT.
    pub token: String,
    /// Token expiry timestamp.
    pub expires_at: DateTime<Utc>,
}

fn timestamp_to_usize(value: i64, field: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| Error::InvalidInput(format!("{field} timestamp out of range")))
}

/// Mints a task-scoped callback token.
///
/// # Errors
///
/// Returns an error when configuration is invalid or signing fails.
pub fn mint_task_token(
    config: &TaskTokenConfig,
    task_id: impl Into<String>,
    tenant_id: impl Into<String>,
    workspace_id: impl Into<String>,
    now: DateTime<Utc>,
) -> Result<MintedTaskToken> {
    config.validate()?;

    let expires_at = now + config.ttl();
    let claims = TaskTokenClaims {
        task_id: task_id.into(),
        tenant_id: tenant_id.into(),
        workspace_id: workspace_id.into(),
        exp: timestamp_to_usize(expires_at.timestamp(), "exp")?,
        nbf: Some(timestamp_to_usize(now.timestamp(), "nbf")?),
        iat: Some(timestamp_to_usize(now.timestamp(), "iat")?),
        iss: config.issuer.clone(),
        aud: config.audience.clone(),
    };

    let token = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(config.hs256_secret.as_bytes()),
    )
    .map_err(|e| Error::InvalidInput(format!("task token minting failed: {e}")))?;

    Ok(MintedTaskToken { token, expires_at })
}

/// Decodes and validates a task-scoped callback token.
///
/// # Errors
///
/// Returns an error when configuration is invalid or token validation fails.
pub fn decode_task_token(config: &TaskTokenConfig, token: &str) -> Result<TaskTokenClaims> {
    config.validate()?;

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_nbf = true;

    if let Some(iss) = config.issuer.as_deref() {
        validation.set_issuer(&[iss]);
    }
    if let Some(aud) = config.audience.as_deref() {
        validation.set_audience(&[aud]);
    }

    let data = jsonwebtoken::decode::<TaskTokenClaims>(
        token,
        &DecodingKey::from_secret(config.hs256_secret.as_bytes()),
        &validation,
    )
    .map_err(|e| Error::InvalidInput(format!("invalid task token: {e}")))?;

    Ok(data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_and_decode_task_token_round_trip() {
        let config = TaskTokenConfig {
            hs256_secret: "secret".to_string(),
            issuer: Some("https://issuer.task".to_string()),
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 60,
        };

        let now = Utc::now();
        let minted =
            mint_task_token(&config, "task-123", "tenant-1", "workspace-1", now).expect("mint");

        let claims = decode_task_token(&config, &minted.token).expect("decode");

        assert_eq!(claims.task_id, "task-123");
        assert_eq!(claims.tenant_id, "tenant-1");
        assert_eq!(claims.workspace_id, "workspace-1");
        assert!(minted.expires_at > now);
    }

    #[test]
    fn validate_rejects_empty_secret() {
        let config = TaskTokenConfig::default();
        let err = config.validate().expect_err("must fail");
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn validate_for_dispatch_rejects_short_ttl_for_timeout_budget() {
        let config = TaskTokenConfig {
            hs256_secret: "secret".to_string(),
            issuer: Some("https://issuer.task".to_string()),
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 900,
        };

        let err = config
            .validate_for_dispatch(1_800, true)
            .expect_err("must fail for short ttl");
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn validate_for_dispatch_requires_issuer_and_audience_when_strict() {
        let config = TaskTokenConfig {
            hs256_secret: "secret".to_string(),
            issuer: None,
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 3_600,
        };

        let err = config
            .validate_for_dispatch(1_800, true)
            .expect_err("must fail when issuer missing");
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn validate_for_dispatch_accepts_valid_budget_and_claim_contract() {
        let config = TaskTokenConfig {
            hs256_secret: "secret".to_string(),
            issuer: Some("https://issuer.task".to_string()),
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 3_600,
        };

        config
            .validate_for_dispatch(1_800, true)
            .expect("valid dispatch config");
    }
}
