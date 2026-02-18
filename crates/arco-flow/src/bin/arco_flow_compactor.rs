//! Arco Flow orchestration micro-compactor service.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::compactor::{CompactionResult, MicroCompactor};

#[derive(Clone)]
struct AppState {
    compactor: MicroCompactor,
}

#[derive(Clone)]
struct InternalAuthState {
    verifier: Arc<InternalOidcVerifier>,
    enforce: bool,
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
    status: StatusCode,
    message: String,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        let status = match &error {
            Error::StaleFencingToken { .. } | Error::FencingLockUnavailable { .. } => {
                StatusCode::CONFLICT
            }
            Error::Core(arco_core::Error::PreconditionFailed { .. }) => StatusCode::CONFLICT,
            Error::Core(
                arco_core::Error::InvalidInput(_)
                | arco_core::Error::InvalidId { .. }
                | arco_core::Error::Validation { .. },
            ) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
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

fn build_internal_auth() -> Result<Option<Arc<InternalAuthState>>> {
    let config = InternalOidcConfig::from_env().map_err(|e| Error::configuration(e.to_string()))?;
    let Some(config) = config else {
        return Ok(None);
    };

    let enforce = config.enforce;
    let verifier =
        InternalOidcVerifier::new(config).map_err(|e| Error::configuration(e.to_string()))?;
    Ok(Some(Arc::new(InternalAuthState {
        verifier: Arc::new(verifier),
        enforce,
    })))
}

async fn internal_auth_middleware(
    State(state): State<Arc<InternalAuthState>>,
    request: axum::http::Request<Body>,
    next: Next,
) -> Response {
    match state.verifier.verify_headers(request.headers()).await {
        Ok(_) => next.run(request).await,
        Err(err) => {
            if state.enforce {
                let message = match err {
                    InternalOidcError::MissingBearerToken => "missing bearer token".to_string(),
                    InternalOidcError::InvalidToken(reason) => format!("invalid token: {reason}"),
                    InternalOidcError::PrincipalNotAllowlisted => {
                        "principal not allowlisted".to_string()
                    }
                    InternalOidcError::JwksRefresh(reason) => {
                        format!("jwks refresh failed: {reason}")
                    }
                };
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse { error: message }),
                )
                    .into_response();
            }

            tracing::warn!(error = %err, "internal auth check failed in report-only mode");
            next.run(request).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::Error as CoreError;

    #[test]
    fn compact_request_allows_missing_epoch() {
        let raw = r#"{"event_paths":["ledger/orchestration/2025-01-15/evt_01.json"]}"#;
        let parsed: CompactRequest = serde_json::from_str(raw).expect("valid request");
        assert_eq!(parsed.epoch, None);
    }

    #[test]
    fn compact_request_accepts_epoch_payload() {
        let raw = r#"{
            "event_paths":["ledger/orchestration/2025-01-15/evt_01.json"],
            "epoch":7
        }"#;
        let parsed: CompactRequest = serde_json::from_str(raw).expect("valid epoch request");
        assert_eq!(parsed.epoch, Some(7));
    }

    #[test]
    fn api_error_maps_core_precondition_to_conflict() {
        let error = Error::Core(CoreError::PreconditionFailed {
            message: "manifest publish failed - concurrent write".to_string(),
        });
        let api_error = ApiError::from(error);
        assert_eq!(api_error.status, StatusCode::CONFLICT);
    }

    #[test]
    fn api_error_maps_core_invalid_input_to_bad_request() {
        let error = Error::Core(CoreError::InvalidInput("invalid lock_path".to_string()));
        let api_error = ApiError::from(error);
        assert_eq!(api_error.status, StatusCode::BAD_REQUEST);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let port = resolve_port()?;
    let tenant_secret = load_tenant_secret()?;
    let internal_auth = build_internal_auth()?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id, workspace_id)?;

    let state = AppState {
        compactor: MicroCompactor::with_tenant_secret(storage, tenant_secret),
    };

    let compact_route = if let Some(auth) = internal_auth {
        post(compact_handler).route_layer(middleware::from_fn_with_state(
            auth,
            internal_auth_middleware,
        ))
    } else {
        post(compact_handler)
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/compact", compact_route)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}
