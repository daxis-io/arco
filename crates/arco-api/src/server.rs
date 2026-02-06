//! API server implementation.
//!
//! Provides health, ready, and API endpoints for the Arco catalog.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{FromRequestParts, State};
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;

use arco_flow::orchestration::controllers::{NoopSensorEvaluator, SensorEvaluator};
#[cfg(feature = "gcp")]
use arco_iceberg::GcsCredentialProvider;
use arco_iceberg::context::IcebergRequestContext;
use arco_iceberg::{
    CredentialProvider, IcebergError, IcebergState, SharedCompactorFactory, Tier1CompactorFactory,
    iceberg_router,
};

use crate::compactor_client::CompactorClient;
use crate::config::{Config, CorsConfig};
use crate::context::RequestContext;
use crate::error::ApiError;
use crate::rate_limit::RateLimitState;
use arco_catalog::SyncCompactor;
use arco_core::Result;
use arco_core::audit::AuditEmitter;

// ============================================================================
// Health and Ready Responses
// ============================================================================

/// Health check response.
#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub struct HealthResponse {
    /// Service status.
    pub status: String,
}

/// Readiness check response.
#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub struct ReadyResponse {
    /// Service readiness status.
    pub ready: bool,
    /// Optional message about readiness state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ============================================================================
// Application State
// ============================================================================

/// Shared application state for all request handlers.
#[derive(Clone)]
pub struct AppState {
    /// Server configuration.
    pub config: Config,
    /// Storage backend for tenant data.
    storage: Arc<dyn arco_core::storage::StorageBackend>,
    /// Rate limiting state (shared across tenants).
    rate_limit: Arc<RateLimitState>,
    /// Sync compaction client (Tier-1 DDL).
    sync_compactor: Option<Arc<dyn SyncCompactor>>,
    /// Sensor evaluator used by manual sensor evaluate.
    sensor_evaluator: Arc<dyn SensorEvaluator>,
    /// Audit event emitter for security decision logging.
    audit: Arc<AuditEmitter>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("config", &self.config)
            .field("storage", &"<StorageBackend>")
            .field("rate_limit", &"<RateLimitState>")
            .field("sync_compactor", &self.sync_compactor.is_some())
            .field("sensor_evaluator", &"<SensorEvaluator>")
            .field("audit", &self.audit)
            .finish()
    }
}

impl AppState {
    /// Creates new application state with the given storage backend.
    #[must_use]
    pub fn new(config: Config, storage: Arc<dyn arco_core::storage::StorageBackend>) -> Self {
        Self::new_with_evaluator(config, storage, Arc::new(NoopSensorEvaluator))
    }

    /// Creates new application state with an explicit sensor evaluator.
    #[must_use]
    pub fn new_with_evaluator(
        config: Config,
        storage: Arc<dyn arco_core::storage::StorageBackend>,
        sensor_evaluator: Arc<dyn SensorEvaluator>,
    ) -> Self {
        Self::new_with_audit(config, storage, sensor_evaluator, None)
    }

    /// Creates new application state with an explicit sensor evaluator and optional audit emitter.
    ///
    /// If `audit_emitter` is `None`, a default tracing-based emitter is used.
    #[must_use]
    pub fn new_with_audit(
        config: Config,
        storage: Arc<dyn arco_core::storage::StorageBackend>,
        sensor_evaluator: Arc<dyn SensorEvaluator>,
        audit_emitter: Option<Arc<AuditEmitter>>,
    ) -> Self {
        let mut config = config;
        config.audit.prepare();
        if config.audit.actor_hmac_key_invalid() {
            tracing::warn!(
                "ARCO_AUDIT_ACTOR_HMAC_KEY is set but is not valid base64; falling back to SHA-256 pseudonymization"
            );
        }

        let rate_limit = Arc::new(RateLimitState::new(config.rate_limit.clone()));
        let sync_compactor = config.compactor_url.as_ref().map(|url| {
            let client: Arc<dyn SyncCompactor> = Arc::new(CompactorClient::new(url.clone()));
            client
        });
        let audit = audit_emitter.unwrap_or_else(|| Arc::new(AuditEmitter::with_tracing()));
        Self {
            config,
            storage,
            rate_limit,
            sync_compactor,
            sensor_evaluator,
            audit,
        }
    }

    /// Creates new application state with in-memory storage (for testing).
    #[must_use]
    pub fn with_memory_storage(config: Config) -> Self {
        Self::with_memory_storage_and_evaluator(config, Arc::new(NoopSensorEvaluator))
    }

    /// Creates new application state with in-memory storage and a custom evaluator.
    #[must_use]
    pub fn with_memory_storage_and_evaluator(
        config: Config,
        sensor_evaluator: Arc<dyn SensorEvaluator>,
    ) -> Self {
        let mut config = config;
        config.audit.prepare();
        if config.audit.actor_hmac_key_invalid() {
            tracing::warn!(
                "ARCO_AUDIT_ACTOR_HMAC_KEY is set but is not valid base64; falling back to SHA-256 pseudonymization"
            );
        }

        let sync_compactor = config.compactor_url.as_ref().map(|url| {
            let client: Arc<dyn SyncCompactor> = Arc::new(CompactorClient::new(url.clone()));
            client
        });
        Self {
            rate_limit: Arc::new(RateLimitState::new(config.rate_limit.clone())),
            config,
            storage: Arc::new(arco_core::storage::MemoryBackend::new()),
            sync_compactor,
            sensor_evaluator,
            audit: Arc::new(AuditEmitter::with_tracing()),
        }
    }

    /// Returns the storage backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend is not configured (should never happen).
    pub fn storage_backend(&self) -> Result<Arc<dyn arco_core::storage::StorageBackend>> {
        Ok(Arc::clone(&self.storage))
    }

    /// Returns the sync compactor if configured.
    #[must_use]
    pub fn sync_compactor(&self) -> Option<Arc<dyn SyncCompactor>> {
        self.sync_compactor.clone()
    }

    /// Returns the configured sensor evaluator.
    #[must_use]
    pub fn sensor_evaluator(&self) -> Arc<dyn SensorEvaluator> {
        Arc::clone(&self.sensor_evaluator)
    }

    /// Returns the audit event emitter.
    #[must_use]
    pub fn audit(&self) -> &AuditEmitter {
        &self.audit
    }
}

// ============================================================================
// Route Handlers
// ============================================================================

/// Health check endpoint handler.
///
/// Returns 200 OK if the service is alive. This is a shallow check
/// that doesn't verify dependencies.
async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

/// Readiness check endpoint handler.
///
/// Returns 200 OK if the service is ready to accept requests.
/// Checks dependencies like storage connectivity.
async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let backend = match state.storage_backend() {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadyResponse {
                    ready: false,
                    message: Some(format!("storage backend unavailable: {e}")),
                }),
            );
        }
    };

    // Shallow connectivity check. We deliberately avoid bucket listing for readiness.
    // A `HEAD` on a missing key is sufficient to validate credentials and network path.
    let check_key = "__arco/ready-check";
    match backend.head(check_key).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ReadyResponse {
                ready: true,
                message: None,
            }),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                ready: false,
                message: Some(format!("storage check failed: {e}")),
            }),
        ),
    }
}

/// Auth middleware for Iceberg REST requests.
///
/// Allows `/v1/config` without auth; all other endpoints require valid auth and
/// inject an [`IcebergRequestContext`] based on the existing `RequestContext`.
async fn iceberg_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if iceberg_public_path(req.uri().path()) {
        return next.run(req).await;
    }

    let (mut parts, body) = req.into_parts();
    let resource = parts.uri.path().to_string();
    let request_id = crate::context::request_id_from_headers(&parts.headers)
        .unwrap_or_else(|| ulid::Ulid::new().to_string());

    let ctx = match RequestContext::from_request_parts(&mut parts, &state).await {
        Ok(ctx) => ctx,
        Err(err) => {
            let reason = if err.code() == "MISSING_AUTH" {
                crate::audit::REASON_MISSING_TOKEN
            } else {
                crate::audit::REASON_INVALID_TOKEN
            };
            let audit_request_id = err.request_id().unwrap_or(&request_id);
            crate::audit::emit_auth_deny(&state, audit_request_id, &resource, reason);
            return api_error_to_iceberg_response(&err);
        }
    };

    crate::audit::emit_auth_allow(&state, &ctx, &resource);

    let iceberg_ctx = IcebergRequestContext {
        tenant: ctx.tenant.clone(),
        workspace: ctx.workspace.clone(),
        request_id: ctx.request_id.clone(),
        idempotency_key: ctx.idempotency_key.clone(),
    };
    parts.extensions.insert(iceberg_ctx);

    next.run(Request::from_parts(parts, body)).await
}

fn iceberg_public_path(path: &str) -> bool {
    path.ends_with("/v1/config") || path.ends_with("/openapi.json")
}

fn api_error_to_iceberg_response(err: &ApiError) -> Response {
    let status = err.status();
    let message = err.message().to_string();
    let request_id = err.request_id().map(str::to_string);

    let iceberg_error = match status {
        StatusCode::BAD_REQUEST => IcebergError::BadRequest {
            message,
            error_type: "BadRequestException",
        },
        StatusCode::UNAUTHORIZED => IcebergError::Unauthorized { message },
        StatusCode::FORBIDDEN => IcebergError::Forbidden { message },
        StatusCode::NOT_FOUND => IcebergError::NotFound {
            message,
            error_type: "NotFoundException",
        },
        StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED => IcebergError::Conflict {
            message,
            error_type: "CommitFailedException",
        },
        StatusCode::SERVICE_UNAVAILABLE => IcebergError::ServiceUnavailable {
            message,
            retry_after_seconds: None,
        },
        _ => IcebergError::Internal { message },
    };

    let mut response = iceberg_error.into_response();
    if let Some(request_id) = request_id {
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response
                .headers_mut()
                .insert(header::HeaderName::from_static("x-request-id"), value);
        }
    }
    response
}

// ============================================================================
// Server
// ============================================================================

/// The Arco API server.
///
/// Serves both HTTP and gRPC endpoints for catalog and orchestration.
pub struct Server {
    config: Config,
    storage: Arc<dyn arco_core::storage::StorageBackend>,
    sensor_evaluator: Arc<dyn SensorEvaluator>,
    audit_emitter: Option<Arc<AuditEmitter>>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("config", &self.config)
            .field("storage", &"<StorageBackend>")
            .field("sensor_evaluator", &"<SensorEvaluator>")
            .field("audit_emitter", &self.audit_emitter.is_some())
            .finish()
    }
}

impl Server {
    /// Creates a new server with the given configuration.
    ///
    /// Defaults to in-memory storage; use `with_storage_backend` for production.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            storage: Arc::new(arco_core::storage::MemoryBackend::new()),
            sensor_evaluator: Arc::new(NoopSensorEvaluator),
            audit_emitter: None,
        }
    }

    /// Creates a new server with an explicit storage backend.
    #[must_use]
    pub fn with_storage_backend(
        config: Config,
        storage: Arc<dyn arco_core::storage::StorageBackend>,
    ) -> Self {
        Self {
            config,
            storage,
            sensor_evaluator: Arc::new(NoopSensorEvaluator),
            audit_emitter: None,
        }
    }

    /// Creates a new `ServerBuilder`.
    #[must_use]
    pub fn builder() -> ServerBuilder {
        ServerBuilder::new()
    }

    /// Returns the server configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Creates the router with all routes and middleware.
    #[allow(clippy::cognitive_complexity)]
    fn create_router(&self, credential_provider: Option<Arc<dyn CredentialProvider>>) -> Router {
        let state = Arc::new(AppState::new_with_audit(
            self.config.clone(),
            Arc::clone(&self.storage),
            Arc::clone(&self.sensor_evaluator),
            self.audit_emitter.clone(),
        ));

        // Build CORS layer from config
        let cors = self.build_cors_layer();

        let auth_layer =
            middleware::from_fn_with_state(Arc::clone(&state), crate::context::auth_middleware);
        let task_auth_layer = middleware::from_fn_with_state(
            Arc::clone(&state),
            crate::routes::tasks::task_auth_middleware,
        );
        let rate_limit_layer = middleware::from_fn_with_state(
            Arc::clone(&state.rate_limit),
            crate::rate_limit::rate_limit_middleware,
        );
        let task_rate_limit_layer = middleware::from_fn_with_state(
            Arc::clone(&state.rate_limit),
            crate::rate_limit::rate_limit_middleware,
        );
        let metrics_layer = middleware::from_fn(crate::metrics::metrics_middleware);
        let metrics_handler = if state.config.posture.is_public() {
            get(|| async { StatusCode::NOT_FOUND })
        } else {
            get(crate::metrics::serve_metrics)
        };

        let mut router = Router::new()
            // Health, ready, and metrics endpoints (no auth required)
            .route("/health", get(health))
            .route("/ready", get(ready))
            .route("/metrics", metrics_handler)
            // API routes (auth via RequestContext extractor)
            .nest(
                "/api/v1",
                crate::routes::api_v1_routes()
                    .route_layer(rate_limit_layer)
                    .layer(auth_layer),
            )
            .nest(
                "/api/v1",
                crate::routes::api_task_routes()
                    .route_layer(task_rate_limit_layer)
                    .layer(task_auth_layer),
            );

        // Mount Iceberg REST Catalog if enabled
        // Uses nest_service since Iceberg router has its own state type
        if state.config.iceberg.enabled {
            let mut iceberg_state = IcebergState::with_config(
                Arc::clone(&state.storage),
                state.config.iceberg.to_iceberg_config(),
            );

            if state.config.iceberg.allow_namespace_crud || state.config.iceberg.allow_table_crud {
                if let Some(compactor) = state.sync_compactor() {
                    iceberg_state = iceberg_state
                        .with_compactor_factory(Arc::new(SharedCompactorFactory::new(compactor)));
                    tracing::info!("Iceberg CRUD enabled with remote sync compaction");
                } else if state.config.debug {
                    iceberg_state =
                        iceberg_state.with_compactor_factory(Arc::new(Tier1CompactorFactory));
                    tracing::warn!(
                        "Iceberg CRUD enabled without compactor_url; using local Tier1 compaction (debug mode only)"
                    );
                } else {
                    tracing::error!(
                        "Iceberg CRUD enabled without compactor_url; CRUD endpoints will fail"
                    );
                }
            }

            if let Some(provider) = credential_provider {
                iceberg_state = iceberg_state.with_credentials(provider);
            }

            // Wire audit emitter for security event logging
            iceberg_state = iceberg_state.with_audit_emitter(state.audit().clone());

            let iceberg_service = ServiceBuilder::new()
                .layer(middleware::from_fn_with_state(
                    Arc::clone(&state),
                    iceberg_auth_middleware,
                ))
                .service(iceberg_router(iceberg_state).into_service::<Body>());

            tracing::info!("Iceberg REST Catalog enabled at /iceberg/v1/*");
            router = router.nest_service("/iceberg", iceberg_service);
        }

        router
            // Middleware (order matters): Metrics outermost for timing, then trace, then CORS.
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            .layer(metrics_layer)
            // Shared state
            .with_state(state)
    }

    /// Builds the CORS layer from configuration.
    fn build_cors_layer(&self) -> CorsLayer {
        let cors_config = &self.config.cors;
        let cors = Self::build_cors_base(cors_config);
        Self::apply_cors_allowed_origins(cors, cors_config)
    }

    fn build_cors_base(cors_config: &CorsConfig) -> CorsLayer {
        CorsLayer::new()
            // Allow common methods for REST API + preflight
            .allow_methods([
                Method::GET,
                Method::HEAD,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ])
            // Allow common headers including auth
            .allow_headers([
                header::AUTHORIZATION,
                header::CONTENT_TYPE,
                header::ACCEPT,
                header::IF_NONE_MATCH,
                // Custom headers for tenant scoping and idempotency
                header::HeaderName::from_static("x-tenant-id"),
                header::HeaderName::from_static("x-workspace-id"),
                header::HeaderName::from_static("idempotency-key"),
                header::HeaderName::from_static("x-request-id"),
                // Iceberg-specific headers
                header::HeaderName::from_static("x-iceberg-access-delegation"),
            ])
            // Expose headers the browser needs to read
            .expose_headers([
                header::CONTENT_TYPE,
                header::CONTENT_LENGTH,
                header::ETAG,
                header::HeaderName::from_static("x-request-id"),
                header::HeaderName::from_static("retry-after"),
            ])
            // Set max age for preflight caching
            .max_age(Duration::from_secs(cors_config.max_age_seconds))
    }

    fn cors_allows_any_origin(cors_config: &CorsConfig) -> bool {
        cors_config.allowed_origins.len() == 1
            && cors_config
                .allowed_origins
                .first()
                .is_some_and(|origin| origin == "*")
    }

    fn parse_cors_origins(cors_config: &CorsConfig) -> Vec<HeaderValue> {
        let mut allowed = Vec::new();
        for origin in &cors_config.allowed_origins {
            match HeaderValue::from_str(origin) {
                Ok(value) => allowed.push(value),
                Err(_) => {
                    tracing::error!(
                        origin = %origin,
                        "Invalid CORS origin; expected a valid HeaderValue"
                    );
                }
            }
        }
        allowed
    }

    fn apply_cors_allowed_origins(cors: CorsLayer, cors_config: &CorsConfig) -> CorsLayer {
        if cors_config.allowed_origins.is_empty() {
            return cors;
        }

        if Self::cors_allows_any_origin(cors_config) {
            return cors.allow_origin(Any);
        }

        if cors_config
            .allowed_origins
            .iter()
            .any(|origin| origin == "*")
        {
            tracing::error!(
                origins = ?cors_config.allowed_origins,
                "Invalid CORS config: '*' must be the only allowed origin"
            );
            return cors;
        }

        let allowed = Self::parse_cors_origins(cors_config);

        if allowed.is_empty() {
            tracing::warn!("All configured CORS origins were invalid; disabling CORS");
            cors
        } else {
            tracing::info!(origins = ?cors_config.allowed_origins, "CORS configured");
            cors.allow_origin(AllowOrigin::list(allowed))
        }
    }

    /// Starts the server and blocks until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot start or bind to the port.
    pub async fn serve(&self) -> Result<()> {
        self.validate_config()?;

        // Initialize metrics before starting the server
        crate::metrics::init_metrics();
        arco_catalog::metrics::register_metrics();
        arco_iceberg::metrics::register_metrics();

        #[cfg(feature = "gcp")]
        let credential_provider: Option<Arc<dyn CredentialProvider>> = if self
            .config
            .iceberg
            .enabled
            && self.config.iceberg.enable_credential_vending
        {
            match GcsCredentialProvider::from_environment().await {
                Ok(provider) => {
                    tracing::info!("Iceberg credential vending enabled for GCS");
                    Some(Arc::new(provider))
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to initialize GCS credential provider; credential vending disabled");
                    None
                }
            }
        } else {
            None
        };

        #[cfg(not(feature = "gcp"))]
        let credential_provider: Option<Arc<dyn CredentialProvider>> = {
            if self.config.iceberg.enabled && self.config.iceberg.enable_credential_vending {
                tracing::warn!(
                    "Credential vending requested but `gcp` feature is not enabled; skipping"
                );
            }
            None
        };

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.http_port));
        let router = self.create_router(credential_provider);

        tracing::info!(
            http_port = self.config.http_port,
            grpc_port = self.config.grpc_port,
            "Starting Arco API server"
        );

        let listener =
            tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| arco_core::Error::Internal {
                    message: format!("failed to bind to {addr}: {e}"),
                })?;

        axum::serve(listener, router)
            .await
            .map_err(|e| arco_core::Error::Internal {
                message: format!("server error: {e}"),
            })?;

        Ok(())
    }

    /// Creates a test router for the server.
    ///
    /// This is useful for integration tests where you want to test
    /// the routes without actually binding to a port.
    ///
    /// # Note
    ///
    /// This method is intended for testing only. It creates a router
    /// using this server's configured storage backend (default: in-memory).
    /// Credential vending is disabled in test mode.
    #[doc(hidden)]
    pub fn test_router(&self) -> Router {
        self.create_router(None)
    }

    fn validate_config(&self) -> Result<()> {
        if !self.config.posture.is_dev() && self.config.debug {
            return Err(arco_core::Error::InvalidInput(
                "debug mode requires posture=dev".to_string(),
            ));
        }

        // Enforce "no wildcard in production" for CORS.
        if !self.config.debug
            && self
                .config
                .cors
                .allowed_origins
                .iter()
                .any(|origin| origin == "*")
        {
            return Err(arco_core::Error::InvalidInput(
                "cors.allowed_origins cannot include '*' when debug=false".to_string(),
            ));
        }

        if !self.config.debug && self.config.storage.bucket.is_none() {
            return Err(arco_core::Error::InvalidInput(
                "storage.bucket is required when debug=false".to_string(),
            ));
        }

        if !self.config.debug && self.config.compactor_url.is_none() {
            return Err(arco_core::Error::InvalidInput(
                "ARCO_COMPACTOR_URL is required when ARCO_DEBUG=false".to_string(),
            ));
        }

        if self.config.iceberg.enabled
            && (self.config.iceberg.allow_namespace_crud || self.config.iceberg.allow_table_crud)
            && !self.config.debug
            && self.config.compactor_url.is_none()
        {
            return Err(arco_core::Error::InvalidInput(
                "Iceberg CRUD requires ARCO_COMPACTOR_URL when ARCO_DEBUG=false".to_string(),
            ));
        }

        // Require JWT configuration in production mode.
        if !self.config.debug {
            let has_hs256_secret = self.config.jwt.hs256_secret.is_some();
            let has_rs256_public_key = self.config.jwt.rs256_public_key_pem.is_some();

            if !has_hs256_secret && !has_rs256_public_key {
                return Err(arco_core::Error::InvalidInput(
                    "jwt.hs256_secret or jwt.rs256_public_key_pem is required when debug=false"
                        .to_string(),
                ));
            }
            if has_hs256_secret && has_rs256_public_key {
                return Err(arco_core::Error::InvalidInput(
                    "jwt.hs256_secret and jwt.rs256_public_key_pem are mutually exclusive"
                        .to_string(),
                ));
            }

            if !self.config.posture.is_dev() {
                if self
                    .config
                    .jwt
                    .issuer
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return Err(arco_core::Error::InvalidInput(
                        "jwt.issuer is required when debug=false and posture!=dev".to_string(),
                    ));
                }
                if self
                    .config
                    .jwt
                    .audience
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return Err(arco_core::Error::InvalidInput(
                        "jwt.audience is required when debug=false and posture!=dev".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Builder for constructing a server.
pub struct ServerBuilder {
    config: Config,
    storage: Arc<dyn arco_core::storage::StorageBackend>,
    sensor_evaluator: Arc<dyn SensorEvaluator>,
    audit_emitter: Option<Arc<AuditEmitter>>,
}

impl std::fmt::Debug for ServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("config", &self.config)
            .field("storage", &"<StorageBackend>")
            .field("sensor_evaluator", &"<SensorEvaluator>")
            .field("audit_emitter", &self.audit_emitter.is_some())
            .finish()
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            config: Config::default(),
            storage: Arc::new(arco_core::storage::MemoryBackend::new()),
            sensor_evaluator: Arc::new(NoopSensorEvaluator),
            audit_emitter: None,
        }
    }
}

impl ServerBuilder {
    /// Creates a new server builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the HTTP port.
    #[must_use]
    pub fn http_port(mut self, port: u16) -> Self {
        self.config.http_port = port;
        self
    }

    /// Sets the gRPC port.
    #[must_use]
    pub fn grpc_port(mut self, port: u16) -> Self {
        self.config.grpc_port = port;
        self
    }

    /// Enables debug mode.
    ///
    /// See `Config::debug` for behavior changes (header-based auth vs Authorization).
    #[must_use]
    pub fn debug(mut self, enabled: bool) -> Self {
        self.config.debug = enabled;
        self
    }

    /// Sets the JWT HS256 secret used for bearer token verification.
    ///
    /// Required when `debug` is false.
    #[must_use]
    pub fn jwt_hs256_secret(mut self, secret: impl Into<String>) -> Self {
        self.config.jwt.hs256_secret = Some(secret.into());
        self
    }

    /// Sets the full configuration (for advanced use cases or testing).
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Sets the storage backend used by request handlers.
    ///
    /// By default, the server uses an in-memory backend intended only for tests/dev.
    #[must_use]
    pub fn storage_backend(mut self, storage: Arc<dyn arco_core::storage::StorageBackend>) -> Self {
        self.storage = storage;
        self
    }

    /// Sets the sensor evaluator used for manual sensor evaluate.
    #[must_use]
    pub fn sensor_evaluator(mut self, evaluator: Arc<dyn SensorEvaluator>) -> Self {
        self.sensor_evaluator = evaluator;
        self
    }

    /// Enables Iceberg REST Catalog endpoints.
    #[must_use]
    pub fn iceberg_enabled(mut self, enabled: bool) -> Self {
        self.config.iceberg.enabled = enabled;
        self
    }

    /// Sets a custom audit emitter (primarily for testing).
    #[must_use]
    pub fn audit_emitter(mut self, emitter: Arc<AuditEmitter>) -> Self {
        self.audit_emitter = Some(emitter);
        self
    }

    /// Builds the server.
    #[must_use]
    pub fn build(self) -> Server {
        Server {
            config: self.config,
            storage: self.storage,
            sensor_evaluator: self.sensor_evaluator,
            audit_emitter: self.audit_emitter,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::config::Posture;

    #[test]
    fn test_posture_allows_debug_in_dev() -> Result<()> {
        let mut builder = ServerBuilder::new();
        builder.config.posture = Posture::Dev;
        builder.config.debug = true;

        let server = builder.build();
        server.validate_config()?;
        Ok(())
    }

    #[test]
    fn test_posture_blocks_debug_outside_dev() {
        for posture in [Posture::Private, Posture::Public] {
            let mut builder = ServerBuilder::new();
            builder.config.posture = posture;
            builder.config.debug = true;

            let server = builder.build();
            let err = server.validate_config().unwrap_err();
            assert!(matches!(err, arco_core::Error::InvalidInput(_)));
        }
    }

    fn configure_non_dev_jwt(builder: &mut ServerBuilder) {
        builder.config.debug = false;
        builder.config.posture = Posture::Private;
        builder.config.storage.bucket = Some("test-bucket".to_string());
        builder.config.compactor_url = Some("http://compactor:8081".to_string());
        builder.config.jwt.hs256_secret = Some("test-secret".to_string());
    }

    #[test]
    fn test_compactor_url_required_when_debug_false() {
        let mut builder = ServerBuilder::new();
        builder.config.debug = false;
        builder.config.posture = Posture::Private;
        builder.config.storage.bucket = Some("test-bucket".to_string());
        builder.config.jwt.hs256_secret = Some("test-secret".to_string());
        builder.config.compactor_url = None;

        let server = builder.build();
        let err = server.validate_config().unwrap_err();
        let arco_core::Error::InvalidInput(message) = err else {
            panic!("unexpected error: {err:?}");
        };
        assert_eq!(
            message,
            "ARCO_COMPACTOR_URL is required when ARCO_DEBUG=false"
        );
    }

    #[test]
    fn test_posture_requires_jwt_issuer_outside_dev() {
        let mut builder = ServerBuilder::new();
        configure_non_dev_jwt(&mut builder);
        builder.config.jwt.audience = Some("test-audience".to_string());

        let server = builder.build();
        let err = server.validate_config().unwrap_err();
        let arco_core::Error::InvalidInput(message) = err else {
            panic!("unexpected error: {err:?}");
        };
        assert!(message.contains("jwt.issuer"));
    }

    #[test]
    fn test_posture_requires_jwt_audience_outside_dev() {
        let mut builder = ServerBuilder::new();
        configure_non_dev_jwt(&mut builder);
        builder.config.jwt.issuer = Some("test-issuer".to_string());

        let server = builder.build();
        let err = server.validate_config().unwrap_err();
        let arco_core::Error::InvalidInput(message) = err else {
            panic!("unexpected error: {err:?}");
        };
        assert!(message.contains("jwt.audience"));
    }

    #[test]
    fn test_posture_rejects_empty_jwt_claims_outside_dev() {
        let mut builder = ServerBuilder::new();
        configure_non_dev_jwt(&mut builder);
        builder.config.jwt.issuer = Some(" ".to_string());
        builder.config.jwt.audience = Some("".to_string());

        let server = builder.build();
        let err = server.validate_config().unwrap_err();
        let arco_core::Error::InvalidInput(message) = err else {
            panic!("unexpected error: {err:?}");
        };
        assert!(message.contains("jwt.issuer"));
    }

    #[test]
    fn test_posture_accepts_jwt_claims_outside_dev() -> Result<()> {
        let mut builder = ServerBuilder::new();
        configure_non_dev_jwt(&mut builder);
        builder.config.jwt.issuer = Some("test-issuer".to_string());
        builder.config.jwt.audience = Some("test-audience".to_string());

        let server = builder.build();
        server.validate_config()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_health_endpoint() -> Result<()> {
        let server = ServerBuilder::new().build();
        let router = server.test_router();

        let request = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .context("read response body")?;
        let health: HealthResponse = serde_json::from_slice(&body).context("parse JSON body")?;
        assert_eq!(health.status, "ok");
        Ok(())
    }

    #[tokio::test]
    async fn test_ready_endpoint() -> Result<()> {
        let server = ServerBuilder::new().build();
        let router = server.test_router();

        let request = Request::builder()
            .uri("/ready")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .context("read response body")?;
        let ready: ReadyResponse = serde_json::from_slice(&body).context("parse JSON body")?;
        assert!(ready.ready);
        Ok(())
    }

    #[tokio::test]
    async fn test_iceberg_config_endpoint() -> Result<()> {
        let server = ServerBuilder::new().iceberg_enabled(true).build();
        let router = server.test_router();

        let request = Request::builder()
            .uri("/iceberg/v1/config")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 2048)
            .await
            .context("read response body")?;
        let config: serde_json::Value = serde_json::from_slice(&body).context("parse JSON body")?;

        // Verify the response has expected structure
        assert!(config.get("overrides").is_some());
        assert!(config.get("defaults").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_iceberg_openapi_endpoint() -> Result<()> {
        let server = ServerBuilder::new().iceberg_enabled(true).build();
        let router = server.test_router();

        let request = Request::builder()
            .uri("/iceberg/openapi.json")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        assert!(content_type.map_or(false, |value| value.starts_with("application/json")));

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .context("read response body")?;
        let text = String::from_utf8(body.to_vec()).context("decode response body")?;
        assert!(text.contains("Iceberg REST Catalog API"));
        Ok(())
    }

    #[tokio::test]
    async fn test_iceberg_auth_uses_iceberg_error_schema() -> Result<()> {
        let server = ServerBuilder::new()
            .debug(true)
            .iceberg_enabled(true)
            .build();
        let router = server.test_router();

        let request = Request::builder()
            .uri("/iceberg/v1/arco/namespaces")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = axum::body::to_bytes(response.into_body(), 2048)
            .await
            .context("read response body")?;
        let payload: serde_json::Value =
            serde_json::from_slice(&body).context("parse JSON body")?;

        let error = payload.get("error").context("missing error field")?;
        assert_eq!(
            error.get("type").and_then(|value| value.as_str()),
            Some("UnauthorizedException")
        );
        assert_eq!(
            error.get("code").and_then(|value| value.as_u64()),
            Some(401)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_iceberg_disabled_by_default() -> Result<()> {
        let server = ServerBuilder::new().build();
        let router = server.test_router();

        let request = Request::builder()
            .uri("/iceberg/v1/config")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        // When Iceberg is disabled, the route should not exist (404)
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }
}
