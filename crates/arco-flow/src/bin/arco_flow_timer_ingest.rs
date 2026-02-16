//! Arco Flow timer callback ingestion service.
//!
//! This service receives Cloud Tasks timer callbacks and appends `TimerFired`
//! facts to the orchestration ledger.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::fold::TimerType as FoldTimerType;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TimerType};

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    ledger: LedgerWriter,
}

#[derive(Clone)]
struct InternalAuthState {
    verifier: Arc<InternalOidcVerifier>,
    enforce: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimerFiredRequest {
    timer_id: String,
    #[serde(default)]
    timer_type: Option<FoldTimerType>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    task_key: Option<String>,
    #[serde(default)]
    attempt: Option<u32>,
    #[serde(default)]
    fire_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimerFiredResponse {
    acknowledged: bool,
    effective_once: bool,
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

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
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

async fn timer_fired_handler(
    State(state): State<AppState>,
    Json(request): Json<TimerFiredRequest>,
) -> std::result::Result<Json<TimerFiredResponse>, ApiError> {
    if request.timer_id.trim().is_empty() {
        return Err(ApiError::bad_request("timer_id must not be empty"));
    }
    if request.attempt == Some(0) {
        return Err(ApiError::bad_request("attempt must be >= 1 when present"));
    }

    let timer_type = resolve_timer_type(request.timer_type, &request.timer_id)
        .ok_or_else(|| ApiError::bad_request("unable to resolve timer_type from payload"))?;

    let mut event = OrchestrationEvent::new(
        state.tenant_id.clone(),
        state.workspace_id.clone(),
        OrchestrationEventData::TimerFired {
            timer_id: request.timer_id,
            timer_type,
            run_id: request.run_id,
            task_key: request.task_key,
            attempt: request.attempt,
        },
    );
    if let Some(fire_at) = request.fire_at {
        event.timestamp = fire_at;
    }

    state.ledger.append(event).await?;

    Ok(Json(TimerFiredResponse {
        acknowledged: true,
        effective_once: true,
    }))
}

fn resolve_timer_type(timer_type: Option<FoldTimerType>, timer_id: &str) -> Option<TimerType> {
    if let Some(timer_type) = timer_type {
        return Some(map_timer_type(timer_type));
    }

    let mut parts = timer_id.split(':');
    let prefix = parts.next()?;
    let type_part = parts.next()?;
    if prefix != "timer" {
        return None;
    }

    match type_part {
        "retry" => Some(TimerType::Retry),
        "heartbeat" => Some(TimerType::HeartbeatCheck),
        "cron" => Some(TimerType::Cron),
        "sla" => Some(TimerType::SlaCheck),
        _ => None,
    }
}

const fn map_timer_type(timer_type: FoldTimerType) -> TimerType {
    match timer_type {
        FoldTimerType::Retry => TimerType::Retry,
        FoldTimerType::HeartbeatCheck => TimerType::HeartbeatCheck,
        FoldTimerType::Cron => TimerType::Cron,
        FoldTimerType::SlaCheck => TimerType::SlaCheck,
    }
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

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let port = resolve_port()?;
    let internal_auth = build_internal_auth()?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id.clone(), workspace_id.clone())?;

    let state = AppState {
        tenant_id,
        workspace_id,
        ledger: LedgerWriter::new(storage),
    };

    let timer_route = internal_auth.map_or_else(
        || post(timer_fired_handler),
        |auth| {
            post(timer_fired_handler).route_layer(middleware::from_fn_with_state(
                auth,
                internal_auth_middleware,
            ))
        },
    );

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/internal/timers/fired", timer_route)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::MemoryBackend;

    fn test_state() -> AppState {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        AppState {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            ledger: LedgerWriter::new(storage),
        }
    }

    #[tokio::test]
    async fn rejects_empty_timer_id() {
        let state = test_state();
        let request = TimerFiredRequest {
            timer_id: " ".to_string(),
            timer_type: Some(FoldTimerType::Retry),
            run_id: None,
            task_key: None,
            attempt: None,
            fire_at: None,
        };

        let result = timer_fired_handler(State(state), Json(request)).await;
        assert!(matches!(
            result,
            Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn duplicate_delivery_returns_success() {
        let state = test_state();
        let request = TimerFiredRequest {
            timer_id: "timer:retry:run1:task1:1:1700000000".to_string(),
            timer_type: Some(FoldTimerType::Retry),
            run_id: Some("run1".to_string()),
            task_key: Some("task1".to_string()),
            attempt: Some(1),
            fire_at: Some(Utc::now()),
        };

        let first = timer_fired_handler(State(state.clone()), Json(request)).await;
        assert!(first.is_ok());

        let duplicate = timer_fired_handler(
            State(state),
            Json(TimerFiredRequest {
                timer_id: "timer:retry:run1:task1:1:1700000000".to_string(),
                timer_type: Some(FoldTimerType::Retry),
                run_id: Some("run1".to_string()),
                task_key: Some("task1".to_string()),
                attempt: Some(1),
                fire_at: Some(Utc::now()),
            }),
        )
        .await;
        assert!(duplicate.is_ok());
    }
}
