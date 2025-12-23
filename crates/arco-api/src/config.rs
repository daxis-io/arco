//! Server configuration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{Error, Result};

use crate::rate_limit::RateLimitConfig;

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

    /// Orchestration compactor base URL for sync compaction (e.g., `<http://servo-compactor:8080>`).
    #[serde(default)]
    pub orchestration_compactor_url: Option<String>,

    /// Cutoff timestamp for strict `run_key` fingerprint validation (RFC3339).
    ///
    /// Reservations created before this cutoff allow missing fingerprints for backward compatibility.
    #[serde(default)]
    pub run_key_fingerprint_cutoff: Option<DateTime<Utc>>,
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
            cors: CorsConfig::default(),
            jwt: JwtConfig::default(),
            rate_limit: RateLimitConfig::default(),
            storage: StorageConfig::default(),
            compactor_url: None,
            orchestration_compactor_url: None,
            run_key_fingerprint_cutoff: None,
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
    ///
    /// JWTs must include tenant/workspace/user claims. The user claim defaults
    /// to `sub` unless overridden via `ARCO_JWT_USER_CLAIM`.
    ///
    /// # Errors
    ///
    /// Returns an error if any environment variable is present but cannot be parsed.
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

        Ok(config)
    }
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

fn env_bool(name: &str) -> Result<Option<bool>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    let v = v.to_ascii_lowercase();
    match v.as_str() {
        "true" | "1" | "yes" | "y" => Ok(Some(true)),
        "false" | "0" | "no" | "n" => Ok(Some(false)),
        _ => Err(Error::InvalidInput(format!(
            "{name} must be a boolean (true/false/1/0)"
        ))),
    }
}

fn env_datetime(name: &str) -> Result<Option<DateTime<Utc>>> {
    let Some(v) = env_string(name) else {
        return Ok(None);
    };
    let parsed = DateTime::parse_from_rfc3339(&v).map_err(|e| {
        Error::InvalidInput(format!("{name} must be RFC3339 (e.g. 2025-01-01T00:00:00Z): {e}"))
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
