//! Arco Flow orchestration dispatcher service.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::dispatch::cloud_tasks::{CloudTasksConfig, CloudTasksDispatcher};
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::error::{Error, Result};
use arco_flow::metrics::{labels as metrics_labels, names as metrics_names};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{MicroCompactor, TimerRow};
use arco_flow::orchestration::controllers::{
    DispatchAction, DispatchPayload, DispatcherController, ReadyDispatchController,
    RunBridgeAction, RunBridgeController, TimerAction, TimerController,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TimerType as EventTimerType,
};
use arco_flow::orchestration::runtime::{
    OrchestrationBacklogSnapshot, OrchestrationRuntimeConfig, OrchestrationSloSnapshot,
    snapshot_backlog, snapshot_slos,
};

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    cloud_tasks: Arc<CloudTasksDispatcher>,
    dispatch_target_url: String,
    timer_target_url: Option<String>,
    timer_audience: Option<String>,
    timer_queue: Option<String>,
    task_token_signer: Option<TaskTokenSigner>,
    timer_ingest_secret: Option<String>,
    timer_callback_auth: TimerCallbackAuth,
    runtime_config: OrchestrationRuntimeConfig,
}

const TIMER_INGEST_SECRET_HEADER: &str = "X-Arco-Timer-Ingest-Secret";

#[derive(Debug, Serialize)]
struct RunError {
    kind: String,
    id: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    run_bridge_emitted: usize,
    run_bridge_skipped: usize,
    ready_dispatch_emitted: usize,
    ready_dispatch_skipped: usize,
    dispatch_actions: usize,
    dispatch_enqueued: usize,
    dispatch_deduplicated: usize,
    dispatch_failed: usize,
    timer_actions: usize,
    timer_enqueued: usize,
    timer_deduplicated: usize,
    timer_failed: usize,
    slo_compaction_lag_seconds: f64,
    slo_compaction_lag_target_seconds: f64,
    slo_compaction_lag_breached: bool,
    slo_run_requested_to_triggered_p95_seconds: Option<f64>,
    slo_run_requested_to_triggered_target_seconds: f64,
    slo_run_requested_to_triggered_breached: bool,
    errors: Vec<RunError>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
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
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "dispatcher run completed with errors".to_string(),
            summary: Some(summary),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            summary: None,
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
            summary: None,
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
            summary: None,
        }
    }
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
            summary: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Some(summary) = self.summary {
            return (self.status, Json(summary)).into_response();
        }

        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimerPayload {
    timer_id: String,
    timer_type: arco_flow::orchestration::compactor::fold::TimerType,
    run_id: Option<String>,
    task_key: Option<String>,
    attempt: Option<u32>,
    fire_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
enum TimerCallbackAuth {
    Disabled,
    Oidc {
        audience: String,
        issuer: String,
        tokeninfo_url: String,
    },
}

#[derive(Debug, Deserialize)]
#[cfg_attr(not(feature = "gcp"), allow(dead_code))]
struct OidcTokenInfo {
    aud: Option<String>,
    iss: Option<String>,
    exp: Option<String>,
}

impl TimerCallbackAuth {
    fn from_env(timer_target_url: Option<&str>) -> Result<Self> {
        let Some(default_audience) = timer_target_url else {
            return Ok(Self::Disabled);
        };

        let audience = optional_env("ARCO_FLOW_TIMER_CALLBACK_OIDC_AUDIENCE")
            .unwrap_or_else(|| default_audience.to_string());
        let issuer = optional_env("ARCO_FLOW_TIMER_CALLBACK_OIDC_ISSUER")
            .unwrap_or_else(|| "https://accounts.google.com".to_string());
        let tokeninfo_url = optional_env("ARCO_FLOW_TIMER_CALLBACK_OIDC_TOKENINFO_URL")
            .unwrap_or_else(|| "https://oauth2.googleapis.com/tokeninfo".to_string());

        if audience.trim().is_empty() {
            return Err(Error::configuration(
                "ARCO_FLOW_TIMER_CALLBACK_OIDC_AUDIENCE cannot be empty",
            ));
        }

        if issuer.trim().is_empty() {
            return Err(Error::configuration(
                "ARCO_FLOW_TIMER_CALLBACK_OIDC_ISSUER cannot be empty",
            ));
        }

        if tokeninfo_url.trim().is_empty() {
            return Err(Error::configuration(
                "ARCO_FLOW_TIMER_CALLBACK_OIDC_TOKENINFO_URL cannot be empty",
            ));
        }

        Ok(Self::Oidc {
            audience,
            issuer,
            tokeninfo_url,
        })
    }

    async fn validate_headers(&self, headers: &HeaderMap) -> std::result::Result<(), String> {
        let token = bearer_token(headers).ok_or_else(|| {
            "Authorization header required for timer callback (expected Bearer token)".to_string()
        })?;
        self.validate_token(&token).await
    }

    async fn validate_token(&self, token: &str) -> std::result::Result<(), String> {
        match self {
            Self::Disabled => Err("timer callback auth is not configured".to_string()),
            Self::Oidc {
                audience,
                issuer,
                tokeninfo_url,
            } => validate_oidc_token(token, audience, issuer, tokeninfo_url).await,
        }
    }

    #[cfg(test)]
    fn oidc_for_tests(audience: &str, issuer: &str, tokeninfo_url: &str) -> Self {
        Self::Oidc {
            audience: audience.to_string(),
            issuer: issuer.to_string(),
            tokeninfo_url: tokeninfo_url.to_string(),
        }
    }
}

#[cfg(feature = "gcp")]
async fn validate_oidc_token(
    token: &str,
    expected_audience: &str,
    expected_issuer: &str,
    tokeninfo_url: &str,
) -> std::result::Result<(), String> {
    let response = reqwest::Client::new()
        .get(tokeninfo_url)
        .query(&[("id_token", token)])
        .send()
        .await
        .map_err(|e| format!("failed to validate OIDC token: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "tokeninfo response unavailable".to_string());
        return Err(format!(
            "OIDC token rejected by tokeninfo endpoint: {status} ({body})"
        ));
    }

    let info = response
        .json::<OidcTokenInfo>()
        .await
        .map_err(|e| format!("failed to parse tokeninfo response: {e}"))?;

    let audience = info
        .aud
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| "OIDC token missing aud claim".to_string())?;
    if audience != expected_audience {
        return Err(format!(
            "OIDC token audience mismatch: expected {expected_audience}, got {audience}"
        ));
    }

    let issuer = info
        .iss
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| "OIDC token missing iss claim".to_string())?;
    if !issuer_matches(expected_issuer, issuer) {
        return Err(format!(
            "OIDC token issuer mismatch: expected {expected_issuer}, got {issuer}"
        ));
    }

    let exp = info
        .exp
        .as_deref()
        .ok_or_else(|| "OIDC token missing exp claim".to_string())?
        .parse::<i64>()
        .map_err(|_| "OIDC token exp claim is invalid".to_string())?;

    if exp <= Utc::now().timestamp() {
        return Err("OIDC token is expired".to_string());
    }

    Ok(())
}

#[cfg(not(feature = "gcp"))]
async fn validate_oidc_token(
    _token: &str,
    _expected_audience: &str,
    _expected_issuer: &str,
    _tokeninfo_url: &str,
) -> std::result::Result<(), String> {
    Err("OIDC validation requires the gcp feature".to_string())
}

#[cfg_attr(not(feature = "gcp"), allow(dead_code))]
fn issuer_matches(expected: &str, actual: &str) -> bool {
    expected == actual
        || (expected == "https://accounts.google.com" && actual == "accounts.google.com")
        || (expected == "accounts.google.com" && actual == "https://accounts.google.com")
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("Authorization")?.to_str().ok()?;
    let token = raw.strip_prefix("Bearer ")?;
    if token.trim().is_empty() {
        return None;
    }
    Some(token.to_string())
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

fn timer_fired_event_data_from_payload(
    payload: &TimerPayload,
) -> std::result::Result<OrchestrationEventData, String> {
    let parsed = TimerRow::parse_timer_id(&payload.timer_id).ok_or_else(|| {
        format!(
            "invalid timer_id format for callback payload: {}",
            payload.timer_id
        )
    })?;

    if payload.run_id.is_some() && payload.run_id != parsed.run_id {
        return Err(format!(
            "timer callback run_id does not match timer_id: payload={:?}, timer_id={:?}",
            payload.run_id, parsed.run_id
        ));
    }

    if payload.task_key.is_some() && payload.task_key != parsed.task_key {
        return Err(format!(
            "timer callback task_key does not match timer_id: payload={:?}, timer_id={:?}",
            payload.task_key, parsed.task_key
        ));
    }

    if payload.attempt.is_some() && payload.attempt != parsed.attempt {
        return Err(format!(
            "timer callback attempt does not match timer_id: payload={:?}, timer_id={:?}",
            payload.attempt, parsed.attempt
        ));
    }

    let timer_type = match parsed.timer_type {
        arco_flow::orchestration::compactor::TimerType::Retry => EventTimerType::Retry,
        arco_flow::orchestration::compactor::TimerType::HeartbeatCheck => {
            EventTimerType::HeartbeatCheck
        }
        arco_flow::orchestration::compactor::TimerType::Cron => EventTimerType::Cron,
        arco_flow::orchestration::compactor::TimerType::SlaCheck => EventTimerType::SlaCheck,
    };

    Ok(OrchestrationEventData::TimerFired {
        timer_id: payload.timer_id.clone(),
        timer_type,
        run_id: parsed.run_id,
        task_key: parsed.task_key,
        attempt: parsed.attempt,
    })
}

async fn append_timer_fired_event(
    ledger: &LedgerWriter,
    tenant_id: &str,
    workspace_id: &str,
    payload: &TimerPayload,
) -> Result<()> {
    let event_data = timer_fired_event_data_from_payload(payload).map_err(Error::configuration)?;
    let event = OrchestrationEvent::new(tenant_id, workspace_id, event_data);
    ledger.append(event).await
}

async fn timer_callback_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<TimerPayload>,
) -> std::result::Result<StatusCode, ApiError> {
    validate_and_append_timer_callback(
        &state.timer_callback_auth,
        &headers,
        &state.ledger,
        &state.tenant_id,
        &state.workspace_id,
        &payload,
    )
    .await?;

    Ok(StatusCode::ACCEPTED)
}

async fn validate_and_append_timer_callback(
    auth: &TimerCallbackAuth,
    headers: &HeaderMap,
    ledger: &LedgerWriter,
    tenant_id: &str,
    workspace_id: &str,
    payload: &TimerPayload,
) -> std::result::Result<(), ApiError> {
    if matches!(auth, TimerCallbackAuth::Disabled) {
        return Err(ApiError::service_unavailable(
            "timer callback auth is not configured",
        ));
    }

    auth.validate_headers(headers)
        .await
        .map_err(ApiError::unauthorized)?;

    append_timer_fired_event(ledger, tenant_id, workspace_id, payload)
        .await
        .map_err(|err| match err {
            Error::Configuration { message } => ApiError::bad_request(message),
            _ => ApiError::from(err),
        })?;

    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;
    let runtime = &state.runtime_config;
    let now = Utc::now();

    let snapshot = snapshot_backlog(now, &manifest, &fold_state);
    emit_backlog_metrics(&snapshot);
    let slo_snapshot = snapshot_slos(now, &manifest, &fold_state, runtime);
    emit_slo_metrics(&slo_snapshot);

    let run_bridge_controller = RunBridgeController::new(runtime.max_compaction_lag);
    let run_bridge_actions = run_bridge_controller.reconcile(&manifest, &fold_state);

    let mut run_bridge_events = Vec::new();
    let mut run_bridge_emitted = 0;
    let mut run_bridge_skipped = 0;
    for action in run_bridge_actions {
        match action {
            RunBridgeAction::EmitRunEvents {
                run_triggered,
                plan_created,
            } => {
                run_bridge_emitted += 1;
                run_bridge_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    run_triggered,
                ));
                run_bridge_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    plan_created,
                ));
            }
            RunBridgeAction::Skip { .. } => run_bridge_skipped += 1,
        }
    }

    if !run_bridge_events.is_empty() {
        state.ledger.append_all(run_bridge_events).await?;
    }

    let ready_controller = ReadyDispatchController::new(runtime.max_compaction_lag);
    let ready_actions = ready_controller.reconcile(&manifest, &fold_state);

    let mut ready_events = Vec::new();
    let mut ready_emitted = 0;
    let mut ready_skipped = 0;

    for action in ready_actions {
        match action.into_event_data() {
            Some(data) => {
                ready_emitted += 1;
                ready_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    data,
                ));
            }
            None => ready_skipped += 1,
        }
    }

    if !ready_events.is_empty() {
        state.ledger.append_all(ready_events).await?;
    }

    let outbox_rows: Vec<_> = fold_state.dispatch_outbox.values().cloned().collect();
    let dispatcher = DispatcherController::new(runtime.max_compaction_lag);
    let dispatch_actions = dispatcher.reconcile(&manifest, &outbox_rows);

    let mut dispatch_events = Vec::new();
    let mut dispatch_enqueued = 0;
    let mut dispatch_deduplicated = 0;
    let mut dispatch_failed = 0;
    let mut errors = Vec::new();
    append_slo_breach_errors(&mut errors, &slo_snapshot);

    for action in &dispatch_actions {
        let DispatchAction::CreateCloudTask {
            dispatch_id,
            cloud_task_id,
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_queue,
        } = action
        else {
            continue;
        };

        let mut payload = DispatchPayload::new(
            run_id.clone(),
            task_key.clone(),
            *attempt,
            attempt_id.clone(),
        );
        if let Some(signer) = state.task_token_signer.as_ref() {
            let (task_token, expires_at) = signer.mint(
                &state.tenant_id,
                &state.workspace_id,
                run_id,
                task_key,
                *attempt,
            )?;
            payload = payload.with_task_token(task_token, expires_at);
        }
        let body = payload
            .to_json()
            .map_err(|e| Error::serialization(format!("dispatch payload error: {e}")))?;

        let mut options = EnqueueOptions::new();
        if worker_queue != "default-queue" {
            options = options.with_routing_key(worker_queue.clone());
        }

        let result = state
            .cloud_tasks
            .enqueue_http(
                cloud_task_id,
                &state.dispatch_target_url,
                body.as_bytes(),
                options,
                Some(state.dispatch_target_url.as_str()),
                None,
            )
            .await;

        match result {
            Ok(EnqueueResult::Enqueued { .. }) => {
                dispatch_enqueued += 1;
                dispatch_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchEnqueued {
                        dispatch_id: dispatch_id.clone(),
                        run_id: Some(run_id.clone()),
                        task_key: Some(task_key.clone()),
                        attempt: Some(*attempt),
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::Deduplicated { .. }) => {
                dispatch_deduplicated += 1;
                dispatch_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchEnqueued {
                        dispatch_id: dispatch_id.clone(),
                        run_id: Some(run_id.clone()),
                        task_key: Some(task_key.clone()),
                        attempt: Some(*attempt),
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::QueueFull) => {
                dispatch_failed += 1;
                errors.push(RunError {
                    kind: "dispatch_queue_full".to_string(),
                    id: dispatch_id.clone(),
                    message: "queue full".to_string(),
                });
            }
            Err(err) => {
                dispatch_failed += 1;
                errors.push(RunError {
                    kind: "dispatch_enqueue_failed".to_string(),
                    id: dispatch_id.clone(),
                    message: err.to_string(),
                });
            }
        }
    }

    if !dispatch_events.is_empty() {
        state.ledger.append_all(dispatch_events).await?;
    }

    let timer_rows: Vec<_> = fold_state.timers.values().cloned().collect();
    let timer_controller = TimerController::new(runtime.max_compaction_lag);
    let timer_actions = timer_controller.reconcile(&manifest, &timer_rows);

    let mut timer_events = Vec::new();
    let mut timer_enqueued = 0;
    let mut timer_deduplicated = 0;
    let mut timer_failed = 0;

    if !timer_actions.is_empty() && state.timer_target_url.is_none() {
        errors.push(RunError {
            kind: "timer_target_missing".to_string(),
            id: "timers".to_string(),
            message: "ARCO_FLOW_TIMER_TARGET_URL is not set".to_string(),
        });
    }

    for action in &timer_actions {
        let TimerAction::CreateTimer {
            timer_id,
            cloud_task_id,
            timer_type,
            fire_at,
            run_id,
            task_key,
            attempt,
            ..
        } = action
        else {
            continue;
        };

        let Some(target_url) = state.timer_target_url.as_ref() else {
            timer_failed += 1;
            continue;
        };

        let payload = TimerPayload {
            timer_id: timer_id.clone(),
            timer_type: *timer_type,
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            fire_at: *fire_at,
        };
        let body = serde_json::to_vec(&payload)
            .map_err(|e| Error::serialization(format!("timer payload error: {e}")))?;

        let now = Utc::now();
        let delay = fire_at.signed_duration_since(now).to_std().ok();

        let mut options = EnqueueOptions::new();
        if let Some(delay) = delay {
            options = options.with_delay(delay);
        }
        if let Some(queue) = state.timer_queue.as_ref() {
            options = options.with_routing_key(queue.clone());
        }

        let result = state
            .cloud_tasks
            .enqueue_http(
                cloud_task_id,
                target_url,
                &body,
                options,
                state
                    .timer_audience
                    .as_deref()
                    .or(Some(target_url.as_str())),
                timer_ingest_headers(state.timer_ingest_secret.as_deref()),
            )
            .await;

        match result {
            Ok(EnqueueResult::Enqueued { .. }) => {
                timer_enqueued += 1;
                timer_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TimerEnqueued {
                        timer_id: timer_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt: *attempt,
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::Deduplicated { .. }) => {
                timer_deduplicated += 1;
                timer_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TimerEnqueued {
                        timer_id: timer_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt: *attempt,
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::QueueFull) => {
                timer_failed += 1;
                errors.push(RunError {
                    kind: "timer_queue_full".to_string(),
                    id: timer_id.clone(),
                    message: "queue full".to_string(),
                });
            }
            Err(err) => {
                timer_failed += 1;
                errors.push(RunError {
                    kind: "timer_enqueue_failed".to_string(),
                    id: timer_id.clone(),
                    message: err.to_string(),
                });
            }
        }
    }

    if !timer_events.is_empty() {
        state.ledger.append_all(timer_events).await?;
    }

    let summary = RunSummary {
        run_bridge_emitted,
        run_bridge_skipped,
        ready_dispatch_emitted: ready_emitted,
        ready_dispatch_skipped: ready_skipped,
        dispatch_actions: dispatch_actions.len(),
        dispatch_enqueued,
        dispatch_deduplicated,
        dispatch_failed,
        timer_actions: timer_actions.len(),
        timer_enqueued,
        timer_deduplicated,
        timer_failed,
        slo_compaction_lag_seconds: slo_snapshot.compaction_lag_seconds,
        slo_compaction_lag_target_seconds: slo_snapshot.compaction_lag_target_seconds,
        slo_compaction_lag_breached: slo_snapshot.compaction_lag_breached,
        slo_run_requested_to_triggered_p95_seconds: slo_snapshot
            .run_requested_to_triggered_p95_seconds,
        slo_run_requested_to_triggered_target_seconds: slo_snapshot
            .run_requested_to_triggered_target_seconds,
        slo_run_requested_to_triggered_breached: slo_snapshot.run_requested_to_triggered_breached,
        errors,
    };

    if summary.errors.is_empty() {
        Ok(Json(summary))
    } else {
        Err(ApiError::from_summary(summary))
    }
}

#[allow(clippy::cast_precision_loss)]
fn emit_backlog_metrics(snapshot: &OrchestrationBacklogSnapshot) {
    gauge!(
        metrics_names::ORCH_BACKLOG_DEPTH,
        metrics_labels::LANE => "run_bridge_pending".to_string(),
    )
    .set(snapshot.run_bridge_pending as f64);
    gauge!(
        metrics_names::ORCH_BACKLOG_DEPTH,
        metrics_labels::LANE => "dispatch_outbox".to_string(),
    )
    .set(snapshot.dispatch_outbox_pending as f64);
    gauge!(
        metrics_names::ORCH_BACKLOG_DEPTH,
        metrics_labels::LANE => "timers".to_string(),
    )
    .set(snapshot.timer_pending as f64);

    gauge!(metrics_names::ORCH_COMPACTION_LAG_SECONDS).set(snapshot.compaction_lag_seconds);
    gauge!(metrics_names::ORCH_RUN_KEY_CONFLICTS).set(snapshot.run_key_conflicts as f64);
}

fn emit_slo_metrics(snapshot: &OrchestrationSloSnapshot) {
    gauge!(
        metrics_names::ORCH_SLO_TARGET_SECONDS,
        metrics_labels::SLO => "compaction_lag_p95".to_string(),
    )
    .set(snapshot.compaction_lag_target_seconds);
    gauge!(
        metrics_names::ORCH_SLO_OBSERVED_SECONDS,
        metrics_labels::SLO => "compaction_lag_p95".to_string(),
    )
    .set(snapshot.compaction_lag_seconds);
    if snapshot.compaction_lag_breached {
        counter!(
            metrics_names::ORCH_SLO_BREACHES_TOTAL,
            metrics_labels::SLO => "compaction_lag_p95".to_string(),
        )
        .increment(1);
    }

    gauge!(
        metrics_names::ORCH_SLO_TARGET_SECONDS,
        metrics_labels::SLO => "run_requested_to_triggered_p95".to_string(),
    )
    .set(snapshot.run_requested_to_triggered_target_seconds);
    if let Some(observed) = snapshot.run_requested_to_triggered_p95_seconds {
        gauge!(
            metrics_names::ORCH_SLO_OBSERVED_SECONDS,
            metrics_labels::SLO => "run_requested_to_triggered_p95".to_string(),
        )
        .set(observed);
    }
    if snapshot.run_requested_to_triggered_breached {
        counter!(
            metrics_names::ORCH_SLO_BREACHES_TOTAL,
            metrics_labels::SLO => "run_requested_to_triggered_p95".to_string(),
        )
        .increment(1);
    }
}

fn append_slo_breach_errors(errors: &mut Vec<RunError>, snapshot: &OrchestrationSloSnapshot) {
    if snapshot.compaction_lag_breached {
        errors.push(RunError {
            kind: "slo_breach_compaction_lag_p95".to_string(),
            id: "compaction_lag_p95".to_string(),
            message: format!(
                "observed {:.3}s exceeded target {:.3}s",
                snapshot.compaction_lag_seconds, snapshot.compaction_lag_target_seconds
            ),
        });
    }
    if snapshot.run_requested_to_triggered_breached {
        errors.push(RunError {
            kind: "slo_breach_run_requested_to_triggered_p95".to_string(),
            id: "run_requested_to_triggered_p95".to_string(),
            message: format!(
                "observed {:.3}s exceeded target {:.3}s",
                snapshot
                    .run_requested_to_triggered_p95_seconds
                    .unwrap_or_default(),
                snapshot.run_requested_to_triggered_target_seconds
            ),
        });
    }
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn timer_ingest_headers(secret: Option<&str>) -> Option<HashMap<String, String>> {
    secret.and_then(|secret| {
        let secret = secret.trim();
        if secret.is_empty() {
            return None;
        }

        let mut headers = HashMap::new();
        headers.insert(TIMER_INGEST_SECRET_HEADER.to_string(), secret.to_string());
        Some(headers)
    })
}

fn validate_timer_callback_oidc_contract(
    timer_target_url: Option<&str>,
    service_account_email: Option<&str>,
) -> Result<()> {
    if timer_target_url.is_some() {
        let has_service_account =
            service_account_email.is_some_and(|value| !value.trim().is_empty());
        if !has_service_account {
            return Err(Error::configuration(
                "ARCO_FLOW_SERVICE_ACCOUNT_EMAIL is required when ARCO_FLOW_TIMER_TARGET_URL is configured",
            ));
        }
    }

    Ok(())
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
    let timer_target_url = optional_env("ARCO_FLOW_TIMER_TARGET_URL");
    let timer_audience = optional_env("ARCO_FLOW_TIMER_AUDIENCE");
    let timer_queue = optional_env("ARCO_FLOW_TIMER_QUEUE");
    let timer_ingest_secret = optional_env("ARCO_FLOW_TIMER_INGEST_SECRET");
    let require_tasks_oidc = parse_bool_env("ARCO_FLOW_REQUIRE_TASKS_OIDC", false);
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    validate_timer_callback_oidc_contract(
        timer_target_url.as_deref(),
        service_account_email.as_deref(),
    )?;
    let timer_callback_auth = TimerCallbackAuth::from_env(timer_target_url.as_deref())?;
    let runtime_config = OrchestrationRuntimeConfig::from_env()?;
    let port = resolve_port()?;
    let internal_auth = build_internal_auth()?;
    let task_token_signer = TaskTokenSigner::from_env()?;

    if require_tasks_oidc && service_account_email.is_none() {
        return Err(Error::configuration(
            "ARCO_FLOW_SERVICE_ACCOUNT_EMAIL is required when ARCO_FLOW_REQUIRE_TASKS_OIDC=true",
        ));
    }

    let environment = optional_env("ARCO_ENVIRONMENT").unwrap_or_else(|| "dev".to_string());
    if !environment.eq_ignore_ascii_case("dev") && timer_target_url.is_none() {
        return Err(Error::configuration(
            "ARCO_FLOW_TIMER_TARGET_URL is required outside dev environments",
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

    if timer_target_url.is_some() && timer_ingest_headers(timer_ingest_secret.as_deref()).is_none()
    {
        return Err(Error::configuration(
            "ARCO_FLOW_TIMER_INGEST_SECRET required when ARCO_FLOW_TIMER_TARGET_URL is set",
        ));
    }

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
        timer_target_url,
        timer_audience,
        timer_queue,
        task_token_signer,
        timer_ingest_secret,
        timer_callback_auth,
        runtime_config,
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
        .route("/timers/callback", post(timer_callback_handler))
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
    use axum::http::HeaderMap;
    use std::sync::Arc;

    use arco_core::MemoryBackend;
    use arco_core::ScopedStorage;
    use arco_flow::orchestration::compactor::fold::{TimerState, TimerType};

    #[cfg(feature = "gcp")]
    use axum::http::HeaderValue;
    #[cfg(feature = "gcp")]
    use chrono::Duration;

    fn timer_payload(timer_id: &str) -> TimerPayload {
        TimerPayload {
            timer_id: timer_id.to_string(),
            timer_type: TimerType::Retry,
            run_id: Some("run-1".to_string()),
            task_key: Some("task-a".to_string()),
            attempt: Some(1),
            fire_at: Utc::now(),
        }
    }

    #[test]
    fn timer_payload_maps_to_timer_fired_event() {
        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");
        let event = timer_fired_event_data_from_payload(&payload).expect("event data");
        match event {
            OrchestrationEventData::TimerFired {
                timer_id,
                run_id,
                task_key,
                attempt,
                ..
            } => {
                assert_eq!(timer_id, payload.timer_id);
                assert_eq!(run_id.as_deref(), Some("run-1"));
                assert_eq!(task_key.as_deref(), Some("task-a"));
                assert_eq!(attempt, Some(1));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn timer_payload_rejects_mismatched_metadata() {
        let payload = TimerPayload {
            timer_id: "timer:retry:run-1:task-a:1:1705320000".to_string(),
            timer_type: TimerType::Retry,
            run_id: Some("wrong-run".to_string()),
            task_key: Some("task-a".to_string()),
            attempt: Some(1),
            fire_at: Utc::now(),
        };

        let err = timer_fired_event_data_from_payload(&payload).expect_err("must reject mismatch");
        assert!(
            err.contains("run_id"),
            "expected run_id mismatch error, got: {err}"
        );
    }

    #[test]
    fn timer_callback_contract_requires_service_account_when_timer_target_is_configured() {
        let err = validate_timer_callback_oidc_contract(
            Some("https://dispatcher.internal/timers/callback"),
            None,
        )
        .expect_err("missing service account should fail closed");
        assert!(
            err.to_string()
                .contains("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL is required"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timer_callback_contract_allows_disabled_timer_target_without_service_account() {
        validate_timer_callback_oidc_contract(None, None)
            .expect("contract should allow disabled timer callback queue");
    }

    #[tokio::test]
    async fn callback_ingestion_duplicate_is_idempotent_after_compaction() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage.clone());

        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");
        append_timer_fired_event(&ledger, "tenant", "workspace", &payload).await?;
        append_timer_fired_event(&ledger, "tenant", "workspace", &payload).await?;

        let mut paths: Vec<String> = storage
            .list("ledger/orchestration/")
            .await?
            .into_iter()
            .map(|p| p.as_str().to_string())
            .collect();
        paths.sort();
        compactor.compact_events(paths).await?;

        let (_manifest, state) = compactor.load_state().await?;
        let timer = state
            .timers
            .get("timer:retry:run-1:task-a:1:1705320000")
            .expect("timer row");
        assert_eq!(timer.state, TimerState::Fired);
        assert!(
            state
                .idempotency_keys
                .contains_key("timer_fired:timer:retry:run-1:task-a:1:1705320000"),
            "timer fired idempotency key should be present"
        );

        Ok(())
    }

    #[tokio::test]
    async fn callback_auth_rejects_missing_bearer() {
        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            "http://127.0.0.1:1/tokeninfo",
        );
        let headers = HeaderMap::new();
        let err = auth
            .validate_headers(&headers)
            .await
            .expect_err("missing auth must be rejected");
        assert!(
            err.contains("Authorization"),
            "expected auth header error, got: {err}"
        );
    }

    #[tokio::test]
    async fn callback_ingestion_rejects_when_auth_disabled() -> Result<()> {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let headers = HeaderMap::new();
        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");

        let err = validate_and_append_timer_callback(
            &TimerCallbackAuth::Disabled,
            &headers,
            &ledger,
            "tenant",
            "workspace",
            &payload,
        )
        .await
        .expect_err("disabled auth must reject callback");
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);

        assert!(
            storage.list("ledger/orchestration/").await?.is_empty(),
            "disabled auth should not append events"
        );
        Ok(())
    }

    #[tokio::test]
    async fn callback_ingestion_rejects_missing_bearer_and_does_not_append() -> Result<()> {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let headers = HeaderMap::new();
        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");
        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            "http://127.0.0.1:1/tokeninfo",
        );

        let err = validate_and_append_timer_callback(
            &auth,
            &headers,
            &ledger,
            "tenant",
            "workspace",
            &payload,
        )
        .await
        .expect_err("missing bearer token must be rejected");
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
        assert!(
            storage.list("ledger/orchestration/").await?.is_empty(),
            "unauthorized callback should not append events"
        );

        Ok(())
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn callback_auth_accepts_valid_tokeninfo_claims() {
        use axum::extract::Query;
        use axum::routing::get;
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        struct Params {
            id_token: String,
        }

        let expected_aud = "https://dispatcher.internal/timers/callback".to_string();
        let app = Router::new().route(
            "/tokeninfo",
            get(move |Query(params): Query<Params>| {
                let expected_aud = expected_aud.clone();
                async move {
                    if params.id_token == "valid-token" {
                        let payload = serde_json::json!({
                            "iss": "https://accounts.google.com",
                            "aud": expected_aud,
                            "exp": (Utc::now() + Duration::minutes(5)).timestamp().to_string(),
                        });
                        (StatusCode::OK, Json(payload)).into_response()
                    } else {
                        (
                            StatusCode::UNAUTHORIZED,
                            Json(serde_json::json!({"error": "invalid token"})),
                        )
                            .into_response()
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });

        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            &format!("http://{addr}/tokeninfo"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str("Bearer valid-token").expect("header value"),
        );
        auth.validate_headers(&headers)
            .await
            .expect("valid token should pass");

        server.abort();
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn callback_ingestion_accepts_valid_oidc_and_appends_timer_fired() -> Result<()> {
        use axum::extract::Query;
        use axum::routing::get;
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        struct Params {
            id_token: String,
        }

        let expected_aud = "https://dispatcher.internal/timers/callback".to_string();
        let app = Router::new().route(
            "/tokeninfo",
            get(move |Query(params): Query<Params>| {
                let expected_aud = expected_aud.clone();
                async move {
                    if params.id_token == "valid-token" {
                        let payload = serde_json::json!({
                            "iss": "https://accounts.google.com",
                            "aud": expected_aud,
                            "exp": (Utc::now() + Duration::minutes(5)).timestamp().to_string(),
                        });
                        (StatusCode::OK, Json(payload)).into_response()
                    } else {
                        (
                            StatusCode::UNAUTHORIZED,
                            Json(serde_json::json!({"error": "invalid token"})),
                        )
                            .into_response()
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });

        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            &format!("http://{addr}/tokeninfo"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str("Bearer valid-token").expect("header value"),
        );
        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");

        validate_and_append_timer_callback(
            &auth,
            &headers,
            &ledger,
            "tenant",
            "workspace",
            &payload,
        )
        .await
        .expect("valid callback should append event");

        let paths = storage.list("ledger/orchestration/").await?;
        assert_eq!(paths.len(), 1, "expected exactly one appended ledger event");
        let event_bytes = storage.get_raw(paths[0].as_str()).await?;
        let event: OrchestrationEvent = serde_json::from_slice(&event_bytes).expect("parse event");

        match event.data {
            OrchestrationEventData::TimerFired { timer_id, .. } => {
                assert_eq!(timer_id, payload.timer_id);
            }
            other => panic!("expected TimerFired event, got {other:?}"),
        }

        server.abort();
        Ok(())
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn callback_auth_rejects_audience_mismatch() {
        use axum::extract::Query;
        use axum::routing::get;
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        struct Params {
            id_token: String,
        }

        let app = Router::new().route(
            "/tokeninfo",
            get(move |Query(params): Query<Params>| async move {
                if params.id_token == "bad-audience-token" {
                    let payload = serde_json::json!({
                        "iss": "https://accounts.google.com",
                        "aud": "https://not-dispatcher.example/callback",
                        "exp": (Utc::now() + Duration::minutes(5)).timestamp().to_string(),
                    });
                    (StatusCode::OK, Json(payload)).into_response()
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({"error": "invalid token"})),
                    )
                        .into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });

        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            &format!("http://{addr}/tokeninfo"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str("Bearer bad-audience-token").expect("header value"),
        );
        let err = auth
            .validate_headers(&headers)
            .await
            .expect_err("audience mismatch should be rejected");
        assert!(
            err.contains("audience mismatch"),
            "expected audience mismatch error, got: {err}"
        );

        server.abort();
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn callback_auth_rejects_issuer_mismatch() {
        use axum::extract::Query;
        use axum::routing::get;
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        struct Params {
            id_token: String,
        }

        let app = Router::new().route(
            "/tokeninfo",
            get(move |Query(params): Query<Params>| async move {
                if params.id_token == "bad-issuer-token" {
                    let payload = serde_json::json!({
                        "iss": "https://malicious-issuer.example",
                        "aud": "https://dispatcher.internal/timers/callback",
                        "exp": (Utc::now() + Duration::minutes(5)).timestamp().to_string(),
                    });
                    (StatusCode::OK, Json(payload)).into_response()
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({"error": "invalid token"})),
                    )
                        .into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });

        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            &format!("http://{addr}/tokeninfo"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str("Bearer bad-issuer-token").expect("header value"),
        );
        let err = auth
            .validate_headers(&headers)
            .await
            .expect_err("issuer mismatch should be rejected");
        assert!(
            err.contains("issuer mismatch"),
            "expected issuer mismatch error, got: {err}"
        );

        server.abort();
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn callback_ingestion_rejects_audience_mismatch_and_does_not_append() -> Result<()> {
        use axum::extract::Query;
        use axum::routing::get;
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        struct Params {
            id_token: String,
        }

        let app = Router::new().route(
            "/tokeninfo",
            get(move |Query(params): Query<Params>| async move {
                if params.id_token == "bad-audience-token" {
                    let payload = serde_json::json!({
                        "iss": "https://accounts.google.com",
                        "aud": "https://wrong.internal/timers/callback",
                        "exp": (Utc::now() + Duration::minutes(5)).timestamp().to_string(),
                    });
                    (StatusCode::OK, Json(payload)).into_response()
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({"error": "invalid token"})),
                    )
                        .into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });

        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            &format!("http://{addr}/tokeninfo"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str("Bearer bad-audience-token").expect("header value"),
        );
        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");

        let err = validate_and_append_timer_callback(
            &auth,
            &headers,
            &ledger,
            "tenant",
            "workspace",
            &payload,
        )
        .await
        .expect_err("audience mismatch should reject callback");
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
        assert!(
            storage.list("ledger/orchestration/").await?.is_empty(),
            "rejected callback should not append events"
        );

        server.abort();
        Ok(())
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn callback_ingestion_rejects_issuer_mismatch_and_does_not_append() -> Result<()> {
        use axum::extract::Query;
        use axum::routing::get;
        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        struct Params {
            id_token: String,
        }

        let app = Router::new().route(
            "/tokeninfo",
            get(move |Query(params): Query<Params>| async move {
                if params.id_token == "bad-issuer-token" {
                    let payload = serde_json::json!({
                        "iss": "https://malicious-issuer.example",
                        "aud": "https://dispatcher.internal/timers/callback",
                        "exp": (Utc::now() + Duration::minutes(5)).timestamp().to_string(),
                    });
                    (StatusCode::OK, Json(payload)).into_response()
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({"error": "invalid token"})),
                    )
                        .into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock server");
        });

        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let auth = TimerCallbackAuth::oidc_for_tests(
            "https://dispatcher.internal/timers/callback",
            "https://accounts.google.com",
            &format!("http://{addr}/tokeninfo"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str("Bearer bad-issuer-token").expect("header value"),
        );
        let payload = timer_payload("timer:retry:run-1:task-a:1:1705320000");

        let err = validate_and_append_timer_callback(
            &auth,
            &headers,
            &ledger,
            "tenant",
            "workspace",
            &payload,
        )
        .await
        .expect_err("issuer mismatch should reject callback");
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
        assert!(
            storage.list("ledger/orchestration/").await?.is_empty(),
            "rejected callback should not append events"
        );

        server.abort();
        Ok(())
    }
}
