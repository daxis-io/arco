//! Arco Flow orchestration anti-entropy sweeper service.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::dispatch::cloud_tasks::{CloudTasksConfig, CloudTasksDispatcher};
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::DispatchOutboxRow;
use arco_flow::orchestration::controllers::{AntiEntropySweeper, DispatchPayload, Repair};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::ids::{cloud_task_id, deterministic_attempt_id};

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    cloud_tasks: Arc<CloudTasksDispatcher>,
    dispatch_target_url: String,
    task_token_signer: Option<TaskTokenSigner>,
}

#[derive(Debug, Serialize)]
struct RunError {
    kind: String,
    id: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    repairs_created: usize,
    redispatch_attempted: usize,
    redispatch_enqueued: usize,
    redispatch_deduplicated: usize,
    redispatch_failed: usize,
    skipped_due_to_lag: usize,
    errors: Vec<RunError>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    message: String,
    summary: Option<RunSummary>,
}

#[derive(Clone)]
struct InternalAuthState {
    verifier: Arc<InternalOidcVerifier>,
    enforce: bool,
}

#[derive(Clone)]
struct TaskTokenSigner {
    encoding_key: Arc<EncodingKey>,
    issuer: String,
    audience: String,
    ttl: Duration,
    subject: Option<String>,
    email: Option<String>,
    azp: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CallbackTaskTokenClaims {
    task_id: String,
    tenant_id: String,
    workspace_id: String,
    run_id: String,
    attempt: u32,
    iss: String,
    aud: String,
    exp: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    azp: Option<String>,
}

impl TaskTokenSigner {
    fn from_env() -> Result<Option<Self>> {
        let secret =
            optional_env("ARCO_FLOW_TASK_TOKEN_SECRET").or_else(|| optional_env("ARCO_JWT_SECRET"));
        let Some(secret) = secret else {
            return Ok(None);
        };

        let issuer = optional_env("ARCO_FLOW_TASK_TOKEN_ISSUER")
            .or_else(|| optional_env("ARCO_JWT_ISSUER"))
            .ok_or_else(|| {
                Error::configuration(
                    "ARCO_FLOW_TASK_TOKEN_ISSUER or ARCO_JWT_ISSUER is required when callback token signing is enabled",
                )
            })?;
        let audience = optional_env("ARCO_FLOW_TASK_TOKEN_AUDIENCE")
            .or_else(|| optional_env("ARCO_JWT_AUDIENCE"))
            .ok_or_else(|| {
                Error::configuration(
                    "ARCO_FLOW_TASK_TOKEN_AUDIENCE or ARCO_JWT_AUDIENCE is required when callback token signing is enabled",
                )
            })?;
        let ttl_secs =
            std::env::var("ARCO_FLOW_TASK_TOKEN_TTL_SECS")
                .ok()
                .map_or(Ok(300_u64), |value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| Error::configuration("invalid ARCO_FLOW_TASK_TOKEN_TTL_SECS"))
                })?;

        Ok(Some(Self {
            encoding_key: Arc::new(EncodingKey::from_secret(secret.as_bytes())),
            issuer,
            audience,
            ttl: Duration::seconds(i64::try_from(ttl_secs).unwrap_or(300)),
            subject: optional_env("ARCO_FLOW_TASK_TOKEN_SUB"),
            email: optional_env("ARCO_FLOW_TASK_TOKEN_EMAIL"),
            azp: optional_env("ARCO_FLOW_TASK_TOKEN_AZP"),
        }))
    }

    fn mint(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        run_id: &str,
        task_id: &str,
        attempt: u32,
    ) -> Result<(String, DateTime<Utc>)> {
        let expires_at = Utc::now() + self.ttl;
        let claims = CallbackTaskTokenClaims {
            task_id: task_id.to_string(),
            tenant_id: tenant_id.to_string(),
            workspace_id: workspace_id.to_string(),
            run_id: run_id.to_string(),
            attempt,
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            exp: usize::try_from(expires_at.timestamp())
                .map_err(|_| Error::configuration("failed to convert token expiry timestamp"))?,
            sub: self.subject.clone(),
            email: self.email.clone(),
            azp: self.azp.clone(),
        };

        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            self.encoding_key.as_ref(),
        )
        .map_err(|e| Error::configuration(format!("failed to mint callback task token: {e}")))?;

        Ok((token, expires_at))
    }
}

impl ApiError {
    fn from_summary(summary: RunSummary) -> Self {
        Self {
            message: "sweeper run completed with errors".to_string(),
            summary: Some(summary),
        }
    }
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            message: error.to_string(),
            summary: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        if let Some(summary) = self.summary {
            return (status, Json(summary)).into_response();
        }

        (
            status,
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

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;
    let sweeper = AntiEntropySweeper::with_defaults();

    let tasks: Vec<_> = fold_state.tasks.values().cloned().collect();
    let outbox: Vec<_> = fold_state.dispatch_outbox.values().cloned().collect();
    let outbox_by_id: HashMap<String, DispatchOutboxRow> = outbox
        .iter()
        .cloned()
        .map(|row| (row.dispatch_id.clone(), row))
        .collect();

    let repairs = sweeper.scan(&manifest.watermarks, &tasks, &outbox, Utc::now());

    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut repairs_created = 0;
    let mut redispatch_attempted = 0;
    let mut redispatch_enqueued = 0;
    let mut redispatch_deduplicated = 0;
    let mut redispatch_failed = 0;
    let mut skipped_due_to_lag = 0;

    for repair in repairs {
        match repair {
            Repair::CreateDispatchOutbox {
                run_id,
                task_key,
                attempt,
                ..
            } => {
                let dispatch_id = DispatchOutboxRow::dispatch_id(&run_id, &task_key, attempt);
                let attempt_id = deterministic_attempt_id(&dispatch_id);

                events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchRequested {
                        run_id,
                        task_key,
                        attempt,
                        attempt_id,
                        worker_queue: "default-queue".to_string(),
                        dispatch_id,
                    },
                ));

                repairs_created += 1;
            }
            Repair::RedispatchStuckTask {
                run_id,
                task_key,
                attempt,
                original_dispatch_id,
                ..
            } => {
                redispatch_attempted += 1;

                let attempt_id = outbox_by_id
                    .get(&original_dispatch_id)
                    .map(|row| row.attempt_id.clone())
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| deterministic_attempt_id(&original_dispatch_id));

                let mut payload =
                    DispatchPayload::new(run_id.clone(), task_key.clone(), attempt, attempt_id);
                if let Some(signer) = state.task_token_signer.as_ref() {
                    let (task_token, expires_at) = signer.mint(
                        &state.tenant_id,
                        &state.workspace_id,
                        &run_id,
                        &task_key,
                        attempt,
                    )?;
                    payload = payload.with_task_token(task_token, expires_at);
                }

                let body = payload
                    .to_json()
                    .map_err(|e| Error::serialization(format!("dispatch payload error: {e}")))?;

                let cloud_id = cloud_task_id("d", &original_dispatch_id);
                let options = EnqueueOptions::new();

                let result = state
                    .cloud_tasks
                    .enqueue_http(
                        &cloud_id,
                        &state.dispatch_target_url,
                        body.as_bytes(),
                        options,
                        Some(state.dispatch_target_url.as_str()),
                        None,
                    )
                    .await;

                match result {
                    Ok(EnqueueResult::Enqueued { .. }) => {
                        redispatch_enqueued += 1;
                        events.push(OrchestrationEvent::new(
                            state.tenant_id.clone(),
                            state.workspace_id.clone(),
                            OrchestrationEventData::DispatchEnqueued {
                                dispatch_id: original_dispatch_id.clone(),
                                run_id: Some(run_id),
                                task_key: Some(task_key),
                                attempt: Some(attempt),
                                cloud_task_id: cloud_id,
                            },
                        ));
                    }
                    Ok(EnqueueResult::Deduplicated { .. }) => {
                        redispatch_deduplicated += 1;
                        events.push(OrchestrationEvent::new(
                            state.tenant_id.clone(),
                            state.workspace_id.clone(),
                            OrchestrationEventData::DispatchEnqueued {
                                dispatch_id: original_dispatch_id.clone(),
                                run_id: Some(run_id),
                                task_key: Some(task_key),
                                attempt: Some(attempt),
                                cloud_task_id: cloud_id,
                            },
                        ));
                    }
                    Ok(EnqueueResult::QueueFull) => {
                        redispatch_failed += 1;
                        errors.push(RunError {
                            kind: "redispatch_queue_full".to_string(),
                            id: original_dispatch_id,
                            message: "queue full".to_string(),
                        });
                    }
                    Err(err) => {
                        redispatch_failed += 1;
                        errors.push(RunError {
                            kind: "redispatch_enqueue_failed".to_string(),
                            id: original_dispatch_id,
                            message: err.to_string(),
                        });
                    }
                }
            }
            Repair::SkippedDueToLag { .. } => {
                skipped_due_to_lag += 1;
            }
        }
    }

    if !events.is_empty() {
        state.ledger.append_all(events).await?;
    }

    let summary = RunSummary {
        repairs_created,
        redispatch_attempted,
        redispatch_enqueued,
        redispatch_deduplicated,
        redispatch_failed,
        skipped_due_to_lag,
        errors,
    };

    if summary.errors.is_empty() {
        Ok(Json(summary))
    } else {
        Err(ApiError::from_summary(summary))
    }
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    std::env::var(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
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

#[allow(clippy::unused_async)]
async fn build_cloud_tasks(config: CloudTasksConfig) -> Result<CloudTasksDispatcher> {
    #[cfg(feature = "gcp")]
    {
        CloudTasksDispatcher::new(config).await
    }

    #[cfg(not(feature = "gcp"))]
    {
        CloudTasksDispatcher::new(config)
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
    let dispatch_target_url = required_env("ARCO_FLOW_DISPATCH_TARGET_URL")?;
    let project_id = required_env("ARCO_GCP_PROJECT_ID")?;
    let location = required_env("ARCO_GCP_LOCATION")?;
    let queue_name =
        optional_env("ARCO_FLOW_QUEUE").unwrap_or_else(|| "arco-flow-dispatch".to_string());
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    let require_tasks_oidc = parse_bool_env("ARCO_FLOW_REQUIRE_TASKS_OIDC", false);
    let port = resolve_port()?;
    let internal_auth = build_internal_auth()?;
    let task_token_signer = TaskTokenSigner::from_env()?;

    if require_tasks_oidc && service_account_email.is_none() {
        return Err(Error::configuration(
            "ARCO_FLOW_SERVICE_ACCOUNT_EMAIL is required when ARCO_FLOW_REQUIRE_TASKS_OIDC=true",
        ));
    }

    let mut cloud_config = CloudTasksConfig::new(
        project_id,
        location,
        queue_name,
        dispatch_target_url.clone(),
    );

    if let Some(email) = service_account_email {
        cloud_config = cloud_config.with_service_account(email);
    }

    let apply_queue_updates = parse_bool_env("ARCO_FLOW_APPLY_QUEUE_RETRY_CONFIG", false);
    if !apply_queue_updates {
        cloud_config = cloud_config.with_queue_retry_updates(false);
    }

    if let Ok(timeout) = std::env::var("ARCO_FLOW_TASK_TIMEOUT_SECS") {
        if let Ok(secs) = timeout.parse::<u64>() {
            cloud_config = cloud_config.with_task_timeout(std::time::Duration::from_secs(secs));
        }
    }

    let cloud_tasks = build_cloud_tasks(cloud_config).await?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id.clone(), workspace_id.clone())?;

    let state = AppState {
        tenant_id,
        workspace_id,
        compactor: MicroCompactor::new(storage.clone()),
        ledger: LedgerWriter::new(storage),
        cloud_tasks: Arc::new(cloud_tasks),
        dispatch_target_url,
        task_token_signer,
    };

    let run_route = internal_auth.map_or_else(
        || post(run_handler),
        |auth| {
            post(run_handler).route_layer(middleware::from_fn_with_state(
                auth,
                internal_auth_middleware,
            ))
        },
    );

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/run", run_route)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}
