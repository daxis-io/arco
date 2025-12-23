//! API server implementation.
//!
//! Provides health, ready, and API endpoints for the Arco catalog.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::compactor_client::CompactorClient;
use crate::config::{Config, CorsConfig};
use crate::rate_limit::RateLimitState;
use arco_core::Result;
use arco_catalog::SyncCompactor;

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
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("config", &self.config)
            .field("storage", &"<StorageBackend>")
            .field("rate_limit", &"<RateLimitState>")
            .field("sync_compactor", &self.sync_compactor.is_some())
            .finish()
    }
}

impl AppState {
    /// Creates new application state with the given storage backend.
    #[must_use]
    pub fn new(config: Config, storage: Arc<dyn arco_core::storage::StorageBackend>) -> Self {
        let rate_limit = Arc::new(RateLimitState::new(config.rate_limit.clone()));
        let sync_compactor = config.compactor_url.as_ref().map(|url| {
            let client: Arc<dyn SyncCompactor> =
                Arc::new(CompactorClient::new(url.clone()));
            client
        });
        Self {
            config,
            storage,
            rate_limit,
            sync_compactor,
        }
    }

    /// Creates new application state with in-memory storage (for testing).
    #[must_use]
    pub fn with_memory_storage(config: Config) -> Self {
        let sync_compactor = config.compactor_url.as_ref().map(|url| {
            let client: Arc<dyn SyncCompactor> =
                Arc::new(CompactorClient::new(url.clone()));
            client
        });
        Self {
            rate_limit: Arc::new(RateLimitState::new(config.rate_limit.clone())),
            config,
            storage: Arc::new(arco_core::storage::MemoryBackend::new()),
            sync_compactor,
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

// ============================================================================
// Server
// ============================================================================

/// The Arco API server.
///
/// Serves both HTTP and gRPC endpoints for catalog and orchestration.
pub struct Server {
    config: Config,
    storage: Arc<dyn arco_core::storage::StorageBackend>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("config", &self.config)
            .field("storage", &"<StorageBackend>")
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
        }
    }

    /// Creates a new server with an explicit storage backend.
    #[must_use]
    pub fn with_storage_backend(
        config: Config,
        storage: Arc<dyn arco_core::storage::StorageBackend>,
    ) -> Self {
        Self { config, storage }
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
    fn create_router(&self) -> Router {
        let state = Arc::new(AppState::new(
            self.config.clone(),
            Arc::clone(&self.storage),
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

        Router::new()
            // Health, ready, and metrics endpoints (no auth required)
            .route("/health", get(health))
            .route("/ready", get(ready))
            .route("/metrics", get(crate::metrics::serve_metrics))
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
            )
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
                // Custom headers for tenant scoping and idempotency
                header::HeaderName::from_static("x-tenant-id"),
                header::HeaderName::from_static("x-workspace-id"),
                header::HeaderName::from_static("idempotency-key"),
                header::HeaderName::from_static("x-request-id"),
            ])
            // Expose headers the browser needs to read
            .expose_headers([
                header::CONTENT_TYPE,
                header::CONTENT_LENGTH,
                header::HeaderName::from_static("x-request-id"),
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

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.http_port));
        let router = self.create_router();

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
    #[doc(hidden)]
    pub fn test_router(&self) -> Router {
        self.create_router()
    }

    fn validate_config(&self) -> Result<()> {
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
        }

        Ok(())
    }
}

/// Builder for constructing a server.
pub struct ServerBuilder {
    config: Config,
    storage: Arc<dyn arco_core::storage::StorageBackend>,
}

impl std::fmt::Debug for ServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("config", &self.config)
            .field("storage", &"<StorageBackend>")
            .finish()
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            config: Config::default(),
            storage: Arc::new(arco_core::storage::MemoryBackend::new()),
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

    /// Sets the storage backend used by request handlers.
    ///
    /// By default, the server uses an in-memory backend intended only for tests/dev.
    #[must_use]
    pub fn storage_backend(mut self, storage: Arc<dyn arco_core::storage::StorageBackend>) -> Self {
        self.storage = storage;
        self
    }

    /// Builds the server.
    #[must_use]
    pub fn build(self) -> Server {
        Server::with_storage_backend(self.config, self.storage)
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
}
