//! Arco Flow orchestration micro-compactor service.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use arco_core::ScopedStorage;
use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::compactor::{CompactionResult, MicroCompactor};

#[derive(Clone)]
struct AppState {
    compactor: MicroCompactor,
}

#[derive(Debug, Deserialize)]
struct CompactRequest {
    event_paths: Vec<String>,
    #[serde(default)]
    epoch: Option<u64>,
}

#[derive(Debug, Serialize)]
struct CompactResponse {
    events_processed: u32,
    delta_id: Option<String>,
    manifest_revision: String,
    visibility_status: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    message: String,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn compact_handler(
    State(state): State<AppState>,
    Json(request): Json<CompactRequest>,
) -> std::result::Result<Json<CompactResponse>, ApiError> {
    let CompactionResult {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status,
    } = state
        .compactor
        .compact_events_with_epoch(request.event_paths, request.epoch)
        .await?;

    Ok(Json(CompactResponse {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status: visibility_status.as_str().to_string(),
    }))
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid PORT"));
    }

    if let Ok(port) = std::env::var("ARCO_FLOW_PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid ARCO_FLOW_PORT"));
    }

    Ok(8080)
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

fn parse_bool_env(key: &str) -> bool {
    std::env::var(key).ok().is_some_and(|value| {
        value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
    })
}

fn load_tenant_secret() -> Result<Vec<u8>> {
    let require_secret = parse_bool_env("ARCO_REQUIRE_TENANT_SECRET");
    let raw = std::env::var("ARCO_TENANT_SECRET_B64").ok();
    let secret = raw.map_or_else(Vec::new, |value| match STANDARD.decode(value.as_bytes()) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "invalid ARCO_TENANT_SECRET_B64; using empty secret"
            );
            Vec::new()
        }
    });

    if secret.is_empty() {
        if require_secret {
            return Err(Error::configuration(
                "ARCO_TENANT_SECRET_B64 required when ARCO_REQUIRE_TENANT_SECRET=true",
            ));
        }
        if !cfg!(test) {
            tracing::warn!("ARCO_TENANT_SECRET_B64 not set; run_id HMAC uses empty secret");
        }
    }

    Ok(secret)
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let port = resolve_port()?;
    let tenant_secret = load_tenant_secret()?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id, workspace_id)?;

    let state = AppState {
        compactor: MicroCompactor::with_tenant_secret(storage, tenant_secret),
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/compact", post(compact_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}
