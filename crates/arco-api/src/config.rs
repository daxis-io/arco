//! Server configuration.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{Error, Result};

use crate::rate_limit::RateLimitConfig;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// Deployment posture for runtime guardrails.
pub enum Posture {
    /// Local development posture (debug allowed).
    Dev,
    /// Internal deployment posture (debug forbidden).
    Private,
    /// Public deployment posture (debug forbidden).
    Public,
}

impl Posture {
    /// Returns true when posture is dev.
    #[must_use]
    pub fn is_dev(self) -> bool {
        matches!(self, Self::Dev)
    }

    /// Returns true when posture is public.
    #[must_use]
    pub fn is_public(self) -> bool {
        matches!(self, Self::Public)
    }
}

impl Default for Posture {
    fn default() -> Self {
        Self::Dev
    }
}

/// Configuration for the Arco API server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP server port.
    pub http_port: u16,

    /// gRPC server port.
    pub grpc_port: u16,

    /// Enable debug mode.
    ///
    /// When enabled:
    /// - `RequestContext` is extracted from `X-Tenant-Id` / `X-Workspace-Id` headers (dev/tests)
    ///
    /// When disabled:
    /// - `Authorization` is required (JWT claim extraction + signature verification)
    pub debug: bool,

    /// Deployment posture (dev/private/public) for runtime guardrails.
    #[serde(default)]
    pub posture: Posture,

    /// CORS configuration.
    #[serde(default)]
    pub cors: CorsConfig,

    /// JWT authentication configuration (used when `debug` is false).
    #[serde(default)]
    pub jwt: JwtConfig,

    /// Rate limiting configuration (denial-of-wallet protection).
    #[serde(default)]
    pub rate_limit: RateLimitConfig,

    /// Storage configuration (bucket/backend selection).
    #[serde(default)]
    pub storage: StorageConfig,

    /// Compactor base URL for sync compaction (e.g., `<http://compactor:8081>`).
    #[serde(default)]
    pub compactor_url: Option<String>,

    /// Orchestration compactor base URL for sync compaction (e.g., `<http://arco-flow-compactor:8080>`).
    #[serde(default)]
    pub orchestration_compactor_url: Option<String>,

    /// Cutoff timestamp for strict `run_key` fingerprint validation (RFC3339).
    ///
    /// Reservations created before this cutoff allow missing fingerprints for backward compatibility.
    #[serde(default)]
    pub run_key_fingerprint_cutoff: Option<DateTime<Utc>>,

    /// Code version to stamp on new runs (e.g., deployment version or git SHA).
    #[serde(default)]
    pub code_version: Option<String>,

    /// Iceberg REST Catalog configuration.
    #[serde(default)]
    pub iceberg: IcebergApiConfig,

    /// Audit configuration for security event logging.
    #[serde(default)]
    pub audit: AuditConfig,
}

/// Audit configuration for security event logging.
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct AuditConfig {
    /// HMAC key for pseudonymizing actor identifiers (base64-encoded).
    ///
    /// When set, actor IDs are hashed using HMAC-SHA256 with this key,
    /// providing stronger pseudonymization than plain SHA-256.
    /// Recommended: 32+ bytes of cryptographically random data.
    ///
    /// If unset, plain SHA-256 is used (less secure, vulnerable to
    /// dictionary attacks if logs leak).
    pub actor_hmac_key: Option<String>,

    #[serde(skip)]
    actor_hmac_key_bytes: Option<Arc<[u8]>>,

    #[serde(skip)]
    actor_hmac_key_invalid: bool,
}

impl std::fmt::Debug for AuditConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditConfig")
            .field(
                "actor_hmac_key",
                &self.actor_hmac_key.as_ref().map(|_| "[REDACTED]"),
            )
            .finish_non_exhaustive()
    }
}

impl AuditConfig {
    pub(crate) fn prepare(&mut self) {
        self.actor_hmac_key_bytes = None;
        self.actor_hmac_key_invalid = false;

        let Some(key_b64) = self.actor_hmac_key.as_deref() else {
            return;
        };

        match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64) {
            Ok(key) => {
                self.actor_hmac_key_bytes = Some(Arc::from(key));
            }
            Err(_) => {
                self.actor_hmac_key_invalid = true;
            }
        }
    }

    pub(crate) fn actor_hmac_key_bytes(&self) -> Option<&[u8]> {
        self.actor_hmac_key_bytes.as_deref()
    }

    pub(crate) const fn actor_hmac_key_invalid(&self) -> bool {
        self.actor_hmac_key_invalid
    }
}

/// CORS configuration for browser-based access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfig {
    /// Allowed origins. Use `["*"]` to allow all origins (development only).
    /// Empty list disables CORS entirely.
    pub allowed_origins: Vec<String>,

    /// Max age for preflight cache (seconds).
    pub max_age_seconds: u64,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            // Default: disabled (secure-by-default).
            // Set to `["*"]` for local development, or explicit origins for production.
            allowed_origins: Vec::new(),
            max_age_seconds: 3600, // 1 hour
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http_port: 8080,
            grpc_port: 9090,
            debug: false,
            posture: Posture::Dev,
            cors: CorsConfig::default(),
            jwt: JwtConfig::default(),
            rate_limit: RateLimitConfig::default(),
            storage: StorageConfig::default(),
            compactor_url: None,
            orchestration_compactor_url: None,
            run_key_fingerprint_cutoff: None,
            code_version: None,
            iceberg: IcebergApiConfig::default(),
            audit: AuditConfig::default(),
        }
    }
}

/// Configuration for the Iceberg REST Catalog API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergApiConfig {
    /// Enable Iceberg REST endpoints at `/iceberg/v1/*`.
    #[serde(default)]
    pub enabled: bool,
    /// Catalog prefix advertised in `/v1/config`.
    #[serde(default = "default_iceberg_prefix")]
    pub prefix: String,
    /// Namespace separator advertised in `/v1/config` (URL-encoded).
    #[serde(default = "default_namespace_separator")]
    pub namespace_separator: String,
    /// Enable write endpoints (Phase B+).
    #[serde(default)]
    pub allow_write: bool,
    /// Enable namespace create/delete endpoints.
    #[serde(default)]
    pub allow_namespace_crud: bool,
    /// Optional concurrency limit for Iceberg handlers.
    #[serde(default)]
    pub concurrency_limit: Option<usize>,
}

fn default_iceberg_prefix() -> String {
    "arco".to_string()
}

fn default_namespace_separator() -> String {
    "%1F".to_string()
}

impl Default for IcebergApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prefix: default_iceberg_prefix(),
            namespace_separator: default_namespace_separator(),
            allow_write: false,
            allow_namespace_crud: false,
            concurrency_limit: None,
        }
    }
}

impl IcebergApiConfig {
    /// Converts to the arco-iceberg crate's config type.
    #[must_use]
    pub fn to_iceberg_config(&self) -> arco_iceberg::IcebergConfig {
        arco_iceberg::IcebergConfig {
            prefix: self.prefix.clone(),
            namespace_separator: self.namespace_separator.clone(),
            idempotency_key_lifetime: Some("PT1H".to_string()),
            allow_write: self.allow_write,
            allow_namespace_crud: self.allow_namespace_crud,
            request_timeout: None,
            concurrency_limit: self.concurrency_limit,
        }
    }
}

/// Storage configuration for the API server.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Object storage bucket name (e.g., `my-bucket`, `gs://my-bucket`, `s3://my-bucket`).
    ///
    /// In GCS deployments this is injected via Terraform as `ARCO_STORAGE_BUCKET`.
    #[serde(default)]
    pub bucket: Option<String>,
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// This is the canonical runtime configuration path for Cloud Run deployments.
    ///
    /// Supported env vars:
    /// - `ARCO_HTTP_PORT`
    /// - `ARCO_GRPC_PORT`
    /// - `ARCO_DEBUG`
    /// - `ARCO_ENVIRONMENT`
    /// - `ARCO_API_PUBLIC`
    /// - `ARCO_CORS_ALLOWED_ORIGINS` (comma-separated, or `*`)
    /// - `ARCO_CORS_MAX_AGE_SECONDS`
    /// - `ARCO_JWT_SECRET`
    /// - `ARCO_JWT_PUBLIC_KEY_PEM`
    /// - `ARCO_JWT_PUBLIC_KEY_PATH`
    /// - `ARCO_JWT_ISSUER`
    /// - `ARCO_JWT_AUDIENCE`
    /// - `ARCO_JWT_TENANT_CLAIM`
    /// - `ARCO_JWT_WORKSPACE_CLAIM`
    /// - `ARCO_JWT_USER_CLAIM`
    /// - `ARCO_STORAGE_BUCKET`
    /// - `ARCO_COMPACTOR_URL`
    /// - `ARCO_ORCH_COMPACTOR_URL`
    /// - `ARCO_RUN_KEY_FINGERPRINT_CUTOFF` (RFC3339, e.g. "2025-01-01T00:00:00Z")
    /// - `ARCO_CODE_VERSION`
    /// - `ARCO_ICEBERG_ENABLED`
    /// - `ARCO_ICEBERG_PREFIX`
    /// - `ARCO_ICEBERG_NAMESPACE_SEPARATOR`
    /// - `ARCO_ICEBERG_ALLOW_WRITE`
    /// - `ARCO_ICEBERG_ALLOW_NAMESPACE_CRUD`
    /// - `ARCO_ICEBERG_CONCURRENCY_LIMIT`
    /// - `ARCO_AUDIT_ACTOR_HMAC_KEY`
    ///
    /// JWTs must include tenant/workspace/user claims. The user claim defaults
    /// to `sub` unless overridden via `ARCO_JWT_USER_CLAIM`.
    ///
    /// # Errors
    ///
    /// Returns an error if any environment variable is present but cannot be parsed.
    #[allow(clippy::cognitive_complexity)]
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        if let Some(port) = env_u16("ARCO_HTTP_PORT")? {
            config.http_port = port;
        }
        if let Some(port) = env_u16("ARCO_GRPC_PORT")? {
            config.grpc_port = port;
        }
        if let Some(debug) = env_bool("ARCO_DEBUG")? {
            config.debug = debug;
        }

        config.posture = posture_from_env()?;

        if let Some(origins) = env_string("ARCO_CORS_ALLOWED_ORIGINS") {
            config.cors.allowed_origins = parse_cors_allowed_origins(&origins);
        }
        if let Some(max_age) = env_u64("ARCO_CORS_MAX_AGE_SECONDS")? {
            config.cors.max_age_seconds = max_age;
        }

        if let Some(secret) = env_string("ARCO_JWT_SECRET") {
            config.jwt.hs256_secret = Some(secret);
        }
        if let Some(pem) = env_string("ARCO_JWT_PUBLIC_KEY_PEM") {
            config.jwt.rs256_public_key_pem = Some(normalize_pem(&pem));
        }
        if let Some(path) = env_string("ARCO_JWT_PUBLIC_KEY_PATH") {
            if config.jwt.rs256_public_key_pem.is_some() {
                return Err(Error::InvalidInput(
                    "ARCO_JWT_PUBLIC_KEY_PATH cannot be set with ARCO_JWT_PUBLIC_KEY_PEM"
                        .to_string(),
                ));
            }
            let pem = std::fs::read_to_string(&path).map_err(|e| {
                Error::InvalidInput(format!(
                    "ARCO_JWT_PUBLIC_KEY_PATH failed to read {path}: {e}"
                ))
            })?;
            config.jwt.rs256_public_key_pem = Some(normalize_pem(&pem));
        }
        if let Some(issuer) = env_string("ARCO_JWT_ISSUER") {
            config.jwt.issuer = Some(issuer);
        }
        if let Some(audience) = env_string("ARCO_JWT_AUDIENCE") {
            config.jwt.audience = Some(audience);
        }
        if let Some(claim) = env_string("ARCO_JWT_TENANT_CLAIM") {
            config.jwt.tenant_claim = claim;
        }
        if let Some(claim) = env_string("ARCO_JWT_WORKSPACE_CLAIM") {
            config.jwt.workspace_claim = claim;
        }
        if let Some(claim) = env_string("ARCO_JWT_USER_CLAIM") {
            config.jwt.user_claim = claim;
        }

        if let Some(bucket) = env_string("ARCO_STORAGE_BUCKET") {
            config.storage.bucket = Some(bucket);
        }
        if let Some(url) = env_string("ARCO_COMPACTOR_URL") {
            config.compactor_url = Some(url);
        }
        if let Some(url) = env_string("ARCO_ORCH_COMPACTOR_URL") {
            config.orchestration_compactor_url = Some(url);
        }
        if let Some(cutoff) = env_datetime("ARCO_RUN_KEY_FINGERPRINT_CUTOFF")? {
            config.run_key_fingerprint_cutoff = Some(cutoff);
        }
        if let Some(code_version) = env_string("ARCO_CODE_VERSION") {
            config.code_version = Some(code_version);
        }

        // Iceberg configuration
        if let Some(enabled) = env_bool("ARCO_ICEBERG_ENABLED")? {
            config.iceberg.enabled = enabled;
        }
        if let Some(prefix) = env_string("ARCO_ICEBERG_PREFIX") {
            config.iceberg.prefix = prefix;
        }
        if let Some(separator) = env_string("ARCO_ICEBERG_NAMESPACE_SEPARATOR") {
            config.iceberg.namespace_separator = separator;
        }
        if let Some(allow_write) = env_bool("ARCO_ICEBERG_ALLOW_WRITE")? {
            config.iceberg.allow_write = allow_write;
        }
        if let Some(allow_crud) = env_bool("ARCO_ICEBERG_ALLOW_NAMESPACE_CRUD")? {
            config.iceberg.allow_namespace_crud = allow_crud;
        }
        if let Some(limit) = env_usize("ARCO_ICEBERG_CONCURRENCY_LIMIT")? {
            config.iceberg.concurrency_limit = Some(limit);
        }

        if let Some(key) = env_string("ARCO_AUDIT_ACTOR_HMAC_KEY") {
            config.audit.actor_hmac_key = Some(key);
        }

        Ok(config)
    }
}

fn posture_from_env() -> Result<Posture> {
    let environment = env_string("ARCO_ENVIRONMENT");
    let api_public = env_string("ARCO_API_PUBLIC");

    posture_from_env_values(environment.as_deref(), api_public.as_deref())
}

fn posture_from_env_values(environment: Option<&str>, api_public: Option<&str>) -> Result<Posture> {
    let environment = environment.ok_or_else(|| {
        Error::InvalidInput("ARCO_ENVIRONMENT is required (dev|staging|prod)".to_string())
    })?;
    let api_public = api_public.ok_or_else(|| {
        Error::InvalidInput("ARCO_API_PUBLIC is required (true/false)".to_string())
    })?;
    let api_public = parse_bool("ARCO_API_PUBLIC", api_public)?;

    posture_from_values(environment, api_public)
}

fn posture_from_values(environment: &str, api_public: bool) -> Result<Posture> {
    let environment = environment.to_ascii_lowercase();
    let posture = match (environment.as_str(), api_public) {
        ("dev", false) => Posture::Dev,
        ("staging" | "prod", false) => Posture::Private,
        ("dev" | "staging" | "prod", true) => Posture::Public,
        _ => {
            return Err(Error::InvalidInput(format!(
                "ARCO_ENVIRONMENT must be one of dev, staging, prod (got {environment})"
            )));
        }
    };

    Ok(posture)
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn env_u16(name: &str) -> Result<Option<u16>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    v.parse::<u16>()
        .map(Some)
        .map_err(|e| Error::InvalidInput(format!("{name} must be a u16: {e}")))
}

fn env_u64(name: &str) -> Result<Option<u64>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    v.parse::<u64>()
        .map(Some)
        .map_err(|e| Error::InvalidInput(format!("{name} must be a u64: {e}")))
}

fn env_usize(name: &str) -> Result<Option<usize>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    v.parse::<usize>()
        .map(Some)
        .map_err(|e| Error::InvalidInput(format!("{name} must be a usize: {e}")))
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" => Ok(false),
        _ => Err(Error::InvalidInput(format!(
            "{name} must be a boolean (true/false/1/0)"
        ))),
    }
}

fn env_bool(name: &str) -> Result<Option<bool>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    parse_bool(name, &v).map(Some)
}

fn env_datetime(name: &str) -> Result<Option<DateTime<Utc>>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    let parsed = DateTime::parse_from_rfc3339(&v).map_err(|e| {
        Error::InvalidInput(format!(
            "{name} must be RFC3339 (e.g. 2025-01-01T00:00:00Z): {e}"
        ))
    })?;
    Ok(Some(parsed.with_timezone(&Utc)))
}

fn parse_cors_allowed_origins(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if trimmed == "*" {
        return vec!["*".to_string()];
    }

    trimmed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// JWT configuration for production authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    /// HS256 secret used to verify bearer tokens.
    ///
    /// In production this should be delivered via secret manager / env var,
    /// not checked into config files.
    #[serde(default)]
    pub hs256_secret: Option<String>,
    /// RS256 public key in PEM format for verifying bearer tokens.
    ///
    /// Prefer `ARCO_JWT_PUBLIC_KEY_PATH` to avoid multiline env vars.
    #[serde(default)]
    pub rs256_public_key_pem: Option<String>,

    /// Optional issuer (`iss`) to enforce.
    #[serde(default)]
    pub issuer: Option<String>,

    /// Optional audience (`aud`) to enforce.
    #[serde(default)]
    pub audience: Option<String>,

    /// Claim name that contains the tenant identifier.
    #[serde(default = "default_tenant_claim")]
    pub tenant_claim: String,

    /// Claim name that contains the workspace identifier.
    #[serde(default = "default_workspace_claim")]
    pub workspace_claim: String,

    /// Claim name that contains the user identifier.
    #[serde(default = "default_user_claim")]
    pub user_claim: String,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            hs256_secret: None,
            rs256_public_key_pem: None,
            issuer: None,
            audience: None,
            tenant_claim: default_tenant_claim(),
            workspace_claim: default_workspace_claim(),
            user_claim: default_user_claim(),
        }
    }
}

fn default_tenant_claim() -> String {
    "tenant".to_string()
}

fn default_workspace_claim() -> String {
    "workspace".to_string()
}

fn default_user_claim() -> String {
    "sub".to_string()
}

fn normalize_pem(pem: &str) -> String {
    let trimmed = pem.trim();
    if trimmed.contains("\\n") && !trimmed.contains('\n') {
        trimmed.replace("\\n", "\n")
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posture_rejects_invalid_environment() {
        let err = posture_from_values("qa", false).unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn posture_maps_environment_and_public_flag() -> Result<()> {
        assert_eq!(posture_from_values("dev", false)?, Posture::Dev);
        assert_eq!(posture_from_values("DEV", false)?, Posture::Dev);
        assert_eq!(posture_from_values("dev", true)?, Posture::Public);
        assert_eq!(posture_from_values("staging", false)?, Posture::Private);
        assert_eq!(posture_from_values("staging", true)?, Posture::Public);
        assert_eq!(posture_from_values("prod", false)?, Posture::Private);
        assert_eq!(posture_from_values("prod", true)?, Posture::Public);
        Ok(())
    }

    #[test]
    fn posture_env_values_require_environment() {
        let err = posture_from_env_values(None, Some("false")).unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn posture_env_values_require_api_public() {
        let err = posture_from_env_values(Some("dev"), None).unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn posture_env_values_reject_invalid_api_public() {
        let err = posture_from_env_values(Some("dev"), Some("maybe")).unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
    }
}
