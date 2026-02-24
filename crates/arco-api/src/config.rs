//! Server configuration.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use arco_core::TaskTokenConfig;
use arco_core::{Error, MAX_TASK_TOKEN_TTL_SECONDS, Result};

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

/// Compactor auth mode for sync compaction calls.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompactorAuthMode {
    /// Do not attach authorization headers.
    None,
    /// Attach a static bearer token configured via environment.
    StaticBearer,
    /// Fetch a GCP identity token from metadata server.
    GcpIdToken,
}

impl Default for CompactorAuthMode {
    fn default() -> Self {
        Self::None
    }
}

/// Compactor auth configuration.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CompactorAuthConfig {
    /// Auth mode.
    #[serde(default)]
    pub mode: CompactorAuthMode,
    /// Static bearer token for `static_bearer` mode.
    #[serde(default)]
    pub static_bearer_token: Option<String>,
    /// Audience override for `gcp_id_token` mode.
    #[serde(default)]
    pub audience: Option<String>,
    /// Metadata URL override for `gcp_id_token` mode (primarily tests).
    #[serde(default)]
    pub metadata_url: Option<String>,
}

impl std::fmt::Debug for CompactorAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactorAuthConfig")
            .field("mode", &self.mode)
            .field(
                "static_bearer_token",
                &self.static_bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("audience", &self.audience)
            .field("metadata_url", &self.metadata_url)
            .finish()
    }
}

impl CompactorAuthConfig {
    /// No auth mode.
    #[must_use]
    pub fn none() -> Self {
        Self {
            mode: CompactorAuthMode::None,
            static_bearer_token: None,
            audience: None,
            metadata_url: None,
        }
    }

    /// Static bearer auth helper.
    #[must_use]
    pub fn static_bearer(token: impl Into<String>) -> Self {
        Self {
            mode: CompactorAuthMode::StaticBearer,
            static_bearer_token: Some(token.into()),
            audience: None,
            metadata_url: None,
        }
    }

    /// GCP id token auth helper.
    #[must_use]
    pub fn gcp_id_token(audience: Option<&str>) -> Self {
        Self {
            mode: CompactorAuthMode::GcpIdToken,
            static_bearer_token: None,
            audience: audience.map(str::to_string),
            metadata_url: None,
        }
    }

    /// Test helper to override metadata URL.
    #[must_use]
    pub fn with_metadata_url(mut self, metadata_url: impl Into<String>) -> Self {
        self.metadata_url = Some(metadata_url.into());
        self
    }

    /// Validates auth settings.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields for the active mode are missing.
    pub fn validate(&self, debug: bool) -> Result<()> {
        if self.mode == CompactorAuthMode::StaticBearer
            && self
                .static_bearer_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            return Err(Error::InvalidInput(
                "ARCO_COMPACTOR_AUTH_STATIC_BEARER_TOKEN is required when ARCO_COMPACTOR_AUTH_MODE=static_bearer"
                    .to_string(),
            ));
        }
        if !debug && self.metadata_url.is_some() {
            return Err(Error::InvalidInput(
                "ARCO_COMPACTOR_GCP_METADATA_URL is only allowed when ARCO_DEBUG=true".to_string(),
            ));
        }
        Ok(())
    }
}

/// Configuration for the Arco API server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP server port.
    pub http_port: u16,

    /// gRPC server port.
    pub grpc_port: u16,

    /// Optional shared secret required to access `/metrics`.
    ///
    /// When set to a non-empty value, callers must provide either:
    /// - `X-Metrics-Secret: <secret>`, or
    /// - `Authorization: Bearer <secret>`
    ///
    /// Empty/whitespace values are treated as unset.
    #[serde(default)]
    pub metrics_secret: Option<String>,

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

    /// Task callback token authentication configuration.
    #[serde(default)]
    pub task_token: TaskTokenConfig,

    /// Rate limiting configuration (denial-of-wallet protection).
    #[serde(default)]
    pub rate_limit: RateLimitConfig,

    /// Storage configuration (bucket/backend selection).
    #[serde(default)]
    pub storage: StorageConfig,

    /// Compactor base URL for sync compaction (e.g., `<http://compactor:8081>`).
    #[serde(default)]
    pub compactor_url: Option<String>,

    /// Compactor auth mode/token/audience for sync compaction calls.
    #[serde(default)]
    pub compactor_auth: CompactorAuthConfig,

    /// Orchestration compactor base URL for sync compaction (e.g., `<http://arco-flow-compactor:8080>`).
    #[serde(default)]
    pub orchestration_compactor_url: Option<String>,

    /// Cutoff timestamp for strict `run_key` fingerprint validation (RFC3339).
    ///
    /// Reservations created before this cutoff allow missing fingerprints for backward compatibility.
    #[serde(default)]
    pub run_key_fingerprint_cutoff: Option<DateTime<Utc>>,

    /// Cutoff timestamp for strict time partition string validation (RFC3339).
    ///
    /// After this cutoff, time partitions must use `d:`/`t:` tagged values.
    #[serde(default)]
    pub partition_time_string_cutoff: Option<DateTime<Utc>>,

    /// Code version to stamp on new runs (e.g., deployment version or git SHA).
    #[serde(default)]
    pub code_version: Option<String>,

    /// Iceberg REST Catalog configuration.
    #[serde(default)]
    pub iceberg: IcebergApiConfig,

    /// Unity Catalog OSS parity facade configuration.
    #[serde(default)]
    pub unity_catalog: UnityCatalogApiConfig,

    /// Audit configuration for security event logging.
    #[serde(default)]
    pub audit: AuditConfig,

    /// Stale timeout for idempotency markers in seconds.
    ///
    /// In-progress idempotency markers older than this timeout can be taken over
    /// by a new request. This prevents stuck requests from blocking retries.
    ///
    /// Default: 300 (5 minutes).
    #[serde(default = "default_idempotency_stale_timeout_secs")]
    pub idempotency_stale_timeout_secs: u64,
}

const MIN_IDEMPOTENCY_STALE_TIMEOUT_SECS: u64 = 10;
const MAX_IDEMPOTENCY_STALE_TIMEOUT_SECS: u64 = 3600; // 1 hour max

fn default_idempotency_stale_timeout_secs() -> u64 {
    300 // 5 minutes, matching arco_catalog::idempotency::DEFAULT_STALE_TIMEOUT
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
            metrics_secret: None,
            debug: false,
            posture: Posture::Dev,
            cors: CorsConfig::default(),
            jwt: JwtConfig::default(),
            task_token: TaskTokenConfig::default(),
            rate_limit: RateLimitConfig::default(),
            storage: StorageConfig::default(),
            compactor_url: None,
            compactor_auth: CompactorAuthConfig::default(),
            orchestration_compactor_url: None,
            run_key_fingerprint_cutoff: None,
            partition_time_string_cutoff: None,
            code_version: None,
            iceberg: IcebergApiConfig::default(),
            unity_catalog: UnityCatalogApiConfig::default(),
            audit: AuditConfig::default(),
            idempotency_stale_timeout_secs: default_idempotency_stale_timeout_secs(),
        }
    }
}

/// Configuration for the Iceberg REST Catalog API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
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
    /// Enable table create/drop/register endpoints.
    #[serde(default)]
    pub allow_table_crud: bool,
    /// Enable multi-table atomic transactions (ICE-7).
    #[serde(default)]
    pub allow_multi_table_transactions: bool,
    /// Optional concurrency limit for Iceberg handlers.
    #[serde(default)]
    pub concurrency_limit: Option<usize>,
    /// Enable credential vending for GCS storage access.
    #[serde(default)]
    pub enable_credential_vending: bool,
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
            allow_table_crud: false,
            allow_multi_table_transactions: false,
            concurrency_limit: None,
            enable_credential_vending: false,
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
            allow_table_crud: self.allow_table_crud,
            allow_multi_table_transactions: self.allow_multi_table_transactions,
            request_timeout: None,
            concurrency_limit: self.concurrency_limit,
        }
    }
}

/// Configuration for the Unity Catalog OSS parity facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnityCatalogApiConfig {
    /// Enable Unity Catalog facade endpoints.
    #[serde(default)]
    pub enabled: bool,

    /// Mount prefix for UC endpoints (must start with `/`).
    #[serde(default = "default_unity_catalog_mount_prefix")]
    pub mount_prefix: String,
}

fn default_unity_catalog_mount_prefix() -> String {
    // Matches the Unity Catalog OSS OpenAPI `servers.url` base path.
    "/api/2.1/unity-catalog".to_string()
}

impl Default for UnityCatalogApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mount_prefix: default_unity_catalog_mount_prefix(),
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
    /// - `ARCO_METRICS_SECRET`
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
    /// - `ARCO_JWT_GROUPS_CLAIM`
    /// - `ARCO_TASK_TOKEN_SECRET`
    /// - `ARCO_TASK_TOKEN_ISSUER`
    /// - `ARCO_TASK_TOKEN_AUDIENCE`
    /// - `ARCO_TASK_TOKEN_TTL_SECS`
    /// - `ARCO_STORAGE_BUCKET`
    /// - `ARCO_COMPACTOR_URL`
    /// - `ARCO_COMPACTOR_AUTH_MODE` (`none` | `static_bearer` | `gcp_id_token`)
    /// - `ARCO_COMPACTOR_AUTH_STATIC_BEARER_TOKEN`
    /// - `ARCO_COMPACTOR_AUTH_AUDIENCE`
    /// - `ARCO_COMPACTOR_GCP_METADATA_URL`
    /// - `ARCO_ORCH_COMPACTOR_URL`
    /// - `ARCO_RUN_KEY_FINGERPRINT_CUTOFF` (RFC3339, e.g. "2025-01-01T00:00:00Z")
    /// - `ARCO_PARTITION_TIME_STRING_CUTOFF` (RFC3339, e.g. "2025-01-01T00:00:00Z")
    /// - `ARCO_CODE_VERSION`
    /// - `ARCO_ICEBERG_ENABLED`
    /// - `ARCO_ICEBERG_PREFIX`
    /// - `ARCO_ICEBERG_NAMESPACE_SEPARATOR`
    /// - `ARCO_ICEBERG_ALLOW_WRITE`
    /// - `ARCO_ICEBERG_ALLOW_NAMESPACE_CRUD`
    /// - `ARCO_ICEBERG_ALLOW_TABLE_CRUD`
    /// - `ARCO_ICEBERG_ALLOW_MULTI_TABLE_TRANSACTIONS`
    /// - `ARCO_ICEBERG_CONCURRENCY_LIMIT`
    /// - `ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING`
    /// - `ARCO_UNITY_CATALOG_ENABLED`
    /// - `ARCO_UNITY_CATALOG_MOUNT_PREFIX`
    /// - `ARCO_AUDIT_ACTOR_HMAC_KEY`
    /// - `ARCO_IDEMPOTENCY_STALE_TIMEOUT_SECS` (10-3600, default: 300)
    ///
    /// JWTs must include tenant/workspace/user claims. The user claim defaults
    /// to `sub` unless overridden via `ARCO_JWT_USER_CLAIM`.
    ///
    /// # Errors
    ///
    /// Returns an error if any environment variable is present but cannot be parsed.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        if let Some(port) = env_u16("ARCO_HTTP_PORT")? {
            config.http_port = port;
        }
        if let Some(port) = env_u16("ARCO_GRPC_PORT")? {
            config.grpc_port = port;
        }
        config.metrics_secret = env_string("ARCO_METRICS_SECRET");
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
        if let Some(claim) = env_string("ARCO_JWT_GROUPS_CLAIM") {
            config.jwt.groups_claim = claim;
        }
        if let Some(secret) = env_string("ARCO_TASK_TOKEN_SECRET") {
            config.task_token.hs256_secret = secret;
        }
        if let Some(issuer) = env_string("ARCO_TASK_TOKEN_ISSUER") {
            config.task_token.issuer = Some(issuer);
        }
        if let Some(audience) = env_string("ARCO_TASK_TOKEN_AUDIENCE") {
            config.task_token.audience = Some(audience);
        }
        if let Some(ttl) = env_u64("ARCO_TASK_TOKEN_TTL_SECS")? {
            if ttl == 0 {
                return Err(Error::InvalidInput(
                    "ARCO_TASK_TOKEN_TTL_SECS must be greater than 0".to_string(),
                ));
            }
            if ttl > MAX_TASK_TOKEN_TTL_SECONDS {
                return Err(Error::InvalidInput(format!(
                    "ARCO_TASK_TOKEN_TTL_SECS must be at most {MAX_TASK_TOKEN_TTL_SECONDS}"
                )));
            }
            config.task_token.ttl_seconds = ttl;
        }

        if let Some(bucket) = env_string("ARCO_STORAGE_BUCKET") {
            config.storage.bucket = Some(bucket);
        }
        if let Some(url) = env_string("ARCO_COMPACTOR_URL") {
            config.compactor_url = Some(url);
        }
        if let Some(mode) = env_string("ARCO_COMPACTOR_AUTH_MODE") {
            config.compactor_auth.mode =
                parse_compactor_auth_mode("ARCO_COMPACTOR_AUTH_MODE", &mode)?;
        }
        if let Some(token) = env_string("ARCO_COMPACTOR_AUTH_STATIC_BEARER_TOKEN") {
            config.compactor_auth.static_bearer_token = Some(token);
        }
        if let Some(audience) = env_string("ARCO_COMPACTOR_AUTH_AUDIENCE") {
            config.compactor_auth.audience = Some(audience);
        }
        if let Some(metadata_url) = env_string("ARCO_COMPACTOR_GCP_METADATA_URL") {
            config.compactor_auth.metadata_url = Some(metadata_url);
        }
        if let Some(url) = env_string("ARCO_ORCH_COMPACTOR_URL") {
            config.orchestration_compactor_url = Some(url);
        }
        if let Some(cutoff) = env_datetime("ARCO_RUN_KEY_FINGERPRINT_CUTOFF")? {
            config.run_key_fingerprint_cutoff = Some(cutoff);
        }
        if let Some(cutoff) = env_datetime("ARCO_PARTITION_TIME_STRING_CUTOFF")? {
            config.partition_time_string_cutoff = Some(cutoff);
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
        if let Some(allow_table_crud) = env_bool("ARCO_ICEBERG_ALLOW_TABLE_CRUD")? {
            config.iceberg.allow_table_crud = allow_table_crud;
        }
        if let Some(allow_multi_table_tx) = env_bool("ARCO_ICEBERG_ALLOW_MULTI_TABLE_TRANSACTIONS")?
        {
            config.iceberg.allow_multi_table_transactions = allow_multi_table_tx;
        }
        if let Some(limit) = env_usize("ARCO_ICEBERG_CONCURRENCY_LIMIT")? {
            config.iceberg.concurrency_limit = Some(limit);
        }
        if let Some(enable) = env_bool("ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING")? {
            config.iceberg.enable_credential_vending = enable;
        }

        // Unity Catalog facade configuration
        if let Some(enabled) = env_bool("ARCO_UNITY_CATALOG_ENABLED")? {
            config.unity_catalog.enabled = enabled;
        }
        if let Some(prefix) = env_string("ARCO_UNITY_CATALOG_MOUNT_PREFIX") {
            config.unity_catalog.mount_prefix = prefix;
        }

        if let Some(key) = env_string("ARCO_AUDIT_ACTOR_HMAC_KEY") {
            config.audit.actor_hmac_key = Some(key);
        }

        if let Some(secs) = env_u64("ARCO_IDEMPOTENCY_STALE_TIMEOUT_SECS")? {
            if secs < MIN_IDEMPOTENCY_STALE_TIMEOUT_SECS {
                return Err(Error::InvalidInput(format!(
                    "ARCO_IDEMPOTENCY_STALE_TIMEOUT_SECS must be at least {MIN_IDEMPOTENCY_STALE_TIMEOUT_SECS} seconds"
                )));
            }
            if secs > MAX_IDEMPOTENCY_STALE_TIMEOUT_SECS {
                return Err(Error::InvalidInput(format!(
                    "ARCO_IDEMPOTENCY_STALE_TIMEOUT_SECS must be at most {MAX_IDEMPOTENCY_STALE_TIMEOUT_SECS} seconds"
                )));
            }
            config.idempotency_stale_timeout_secs = secs;
        }

        if !config.unity_catalog.mount_prefix.starts_with('/') {
            return Err(Error::InvalidInput(
                "ARCO_UNITY_CATALOG_MOUNT_PREFIX must start with '/'".to_string(),
            ));
        }
        if config.unity_catalog.mount_prefix == "/" {
            return Err(Error::InvalidInput(
                "ARCO_UNITY_CATALOG_MOUNT_PREFIX cannot be '/' (would shadow /health, /ready, and other core endpoints)"
                    .to_string(),
            ));
        }
        if config.unity_catalog.mount_prefix.ends_with('/') {
            config.unity_catalog.mount_prefix = config
                .unity_catalog
                .mount_prefix
                .trim_end_matches('/')
                .to_string();
        }

        Ok(config)
    }

    /// Returns the idempotency stale timeout as a `chrono::Duration`.
    #[must_use]
    pub fn idempotency_stale_timeout(&self) -> chrono::Duration {
        let secs = self
            .idempotency_stale_timeout_secs
            .min(MAX_IDEMPOTENCY_STALE_TIMEOUT_SECS);
        chrono::Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX))
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

fn parse_compactor_auth_mode(name: &str, value: &str) -> Result<CompactorAuthMode> {
    let mode = value.trim().to_ascii_lowercase();
    match mode.as_str() {
        "none" => Ok(CompactorAuthMode::None),
        "static_bearer" => Ok(CompactorAuthMode::StaticBearer),
        "gcp_id_token" => Ok(CompactorAuthMode::GcpIdToken),
        _ => Err(Error::InvalidInput(format!(
            "{name} must be one of: none, static_bearer, gcp_id_token (got {value})"
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

    /// Claim name that contains group memberships.
    ///
    /// When absent, the request principal has no groups.
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
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
            groups_claim: default_groups_claim(),
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

fn default_groups_claim() -> String {
    "groups".to_string()
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

    #[test]
    fn iceberg_credential_vending_default_is_false() {
        let config = IcebergApiConfig::default();
        assert!(!config.enable_credential_vending);
    }

    #[test]
    fn iceberg_multi_table_transactions_default_is_false() {
        let config = IcebergApiConfig::default();
        assert!(!config.allow_multi_table_transactions);
    }

    #[test]
    fn parse_bool_accepts_true_values() {
        assert!(parse_bool("TEST", "true").unwrap());
        assert!(parse_bool("TEST", "1").unwrap());
        assert!(parse_bool("TEST", "yes").unwrap());
        assert!(parse_bool("TEST", "TRUE").unwrap());
    }

    #[test]
    fn parse_bool_accepts_false_values() {
        assert!(!parse_bool("TEST", "false").unwrap());
        assert!(!parse_bool("TEST", "0").unwrap());
        assert!(!parse_bool("TEST", "no").unwrap());
        assert!(!parse_bool("TEST", "FALSE").unwrap());
    }

    #[test]
    fn parse_bool_rejects_invalid_values() {
        assert!(parse_bool("TEST", "maybe").is_err());
        assert!(parse_bool("TEST", "").is_err());
    }

    #[test]
    fn parse_compactor_auth_mode_accepts_all_modes() -> Result<()> {
        assert_eq!(
            parse_compactor_auth_mode("TEST", "none")?,
            CompactorAuthMode::None
        );
        assert_eq!(
            parse_compactor_auth_mode("TEST", "static_bearer")?,
            CompactorAuthMode::StaticBearer
        );
        assert_eq!(
            parse_compactor_auth_mode("TEST", "gcp_id_token")?,
            CompactorAuthMode::GcpIdToken
        );
        Ok(())
    }

    #[test]
    fn parse_compactor_auth_mode_rejects_invalid_value() {
        let err = parse_compactor_auth_mode("TEST", "legacy").unwrap_err();
        let Error::InvalidInput(message) = err else {
            panic!("unexpected error: {err:?}");
        };
        assert!(message.contains("TEST"));
        assert!(message.contains("legacy"));
    }

    #[test]
    fn compactor_auth_debug_redacts_static_token() {
        let auth = CompactorAuthConfig::static_bearer("super-secret");
        let dbg = format!("{auth:?}");
        assert!(dbg.contains("REDACTED"));
        assert!(!dbg.contains("super-secret"));
    }

    #[test]
    fn compactor_auth_metadata_override_rejected_outside_debug() {
        let auth = CompactorAuthConfig::gcp_id_token(None)
            .with_metadata_url("http://127.0.0.1:8181/token");
        let err = auth
            .validate(false)
            .expect_err("metadata override should be blocked outside debug");
        let Error::InvalidInput(message) = err else {
            panic!("unexpected error: {err:?}");
        };
        assert!(message.contains("ARCO_COMPACTOR_GCP_METADATA_URL"));
    }

    #[test]
    fn compactor_auth_metadata_override_allowed_in_debug() -> Result<()> {
        let auth = CompactorAuthConfig::gcp_id_token(None)
            .with_metadata_url("http://127.0.0.1:8181/token");
        auth.validate(true)?;
        Ok(())
    }
}
