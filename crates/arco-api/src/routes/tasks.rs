//! Worker callback API routes per ADR-023.
//!
//! These endpoints are called by workers to report task lifecycle events.
//!
//! ## Routes
//!
//! - `POST /tasks/{task_id}/started` - Worker started execution
//! - `POST /tasks/{task_id}/heartbeat` - Worker heartbeat (progress updates)
//! - `POST /tasks/{task_id}/completed` - Worker finished execution
//!
//! ## Authentication
//!
//! All endpoints require a task-scoped bearer token in the Authorization header.
//! The token is validated against the task state to prevent stale-worker corruption.

use std::future::Future;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::HeaderName;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::config::JwtConfig;
use crate::context::{REQUEST_ID_HEADER, RequestContext};
use crate::error::{ApiError, ApiErrorBody};
use crate::orchestration_compaction::CompactingLedgerWriter;
use crate::server::AppState;

use arco_flow::orchestration::callbacks::{
    CallbackContext, CallbackError, CallbackResult, TaskState as CallbackTaskState,
    TaskStateLookup, TaskTokenValidator, handle_heartbeat, handle_task_completed,
    handle_task_started,
};
use arco_flow::orchestration::compactor::{FoldState, MicroCompactor, TaskState as FoldTaskState};
use arco_flow::orchestration::{
    ErrorCategory as FlowErrorCategory, HeartbeatRequest as FlowHeartbeatRequest,
    HeartbeatResponse as FlowHeartbeatResponse, TaskCompletedRequest as FlowTaskCompletedRequest,
    TaskCompletedResponse as FlowTaskCompletedResponse, TaskError as FlowTaskError,
    TaskMetrics as FlowTaskMetrics, TaskOutput as FlowTaskOutput,
    TaskStartedRequest as FlowTaskStartedRequest, TaskStartedResponse as FlowTaskStartedResponse,
    WorkerOutcome as FlowWorkerOutcome,
};
use ulid::Ulid;

// ============================================================================
// Request/Response Types (with ToSchema for OpenAPI)
// ============================================================================

/// Request body for `/v1/tasks/{task_id}/started`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskStartedRequest {
    /// Attempt number (1-indexed).
    pub attempt: u32,
    /// Attempt identifier (ULID) - concurrency guard.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// When execution started (optional, uses server time if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

/// Response body for successful `/v1/tasks/{task_id}/started`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskStartedResponse {
    /// Whether the callback was acknowledged.
    pub acknowledged: bool,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Request body for `/v1/tasks/{task_id}/heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    /// Attempt number.
    pub attempt: u32,
    /// Attempt identifier (ULID) - concurrency guard.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// When heartbeat was sent (optional, uses server time if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// Optional progress percentage (0-100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_pct: Option<u8>,
    /// Optional status message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Response body for successful `/v1/tasks/{task_id}/heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// Whether the heartbeat was acknowledged.
    pub acknowledged: bool,
    /// Whether the worker should cancel.
    pub should_cancel: bool,
    /// Reason for cancellation (if `should_cancel` is true).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Request body for `/v1/tasks/{task_id}/completed`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedRequest {
    /// Attempt number.
    pub attempt: u32,
    /// Attempt identifier (ULID) - concurrency guard.
    pub attempt_id: String,
    /// Worker identifier.
    pub worker_id: String,
    /// Optional W3C traceparent for distributed tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Task outcome.
    pub outcome: WorkerOutcome,
    /// When execution completed (optional, uses server time if omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Output for successful tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<TaskOutput>,
    /// Error details for failed tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    /// Execution metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<TaskMetrics>,
    /// Phase when cancellation occurred (for CANCELLED outcome).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelled_during_phase: Option<String>,
    /// Partial progress at cancellation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_progress: Option<Value>,
}

/// Response body for successful `/v1/tasks/{task_id}/completed`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedResponse {
    /// Whether the completion was acknowledged.
    pub acknowledged: bool,
    /// Final task state.
    pub final_state: String,
    /// Server timestamp.
    pub server_time: DateTime<Utc>,
}

/// Worker-reported task outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerOutcome {
    /// Task completed successfully.
    Succeeded,
    /// Task failed (may retry).
    Failed,
    /// Task was cancelled.
    Cancelled,
}

/// Output from a successful task.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutput {
    /// Materialization identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialization_id: Option<String>,
    /// Number of rows produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    /// Size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_size: Option<u64>,
    /// Output path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
}

/// Error details from a failed task.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    /// Error category per ADR-023.
    pub category: ErrorCategory,
    /// Error message.
    pub message: String,
    /// Stack trace (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
    /// Whether the error is retryable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

/// Error category per ADR-023.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCategory {
    /// Error in user/asset code.
    UserCode,
    /// Input data fails validation.
    DataQuality,
    /// Infrastructure/cloud errors.
    Infrastructure,
    /// Configuration errors.
    Configuration,
    /// Execution timeout.
    Timeout,
    /// Task was cancelled.
    Cancelled,
}

/// Execution metrics from the worker.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetrics {
    /// CPU time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_time_ms: Option<u64>,
    /// Peak memory usage in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_memory_bytes: Option<u64>,
    /// I/O read bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_read_bytes: Option<u64>,
    /// I/O write bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_write_bytes: Option<u64>,
}

/// Callback error response per ADR-023.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CallbackErrorResponse {
    /// Error code.
    pub error: String,
    /// Human-readable message.
    pub message: String,
    /// Current task state (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Expected attempt (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_attempt: Option<u32>,
    /// Received attempt (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_attempt: Option<u32>,
    /// Expected attempt ID (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_attempt_id: Option<String>,
    /// Received attempt ID (for 409 Conflict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_attempt_id: Option<String>,
}

// ============================================================================
// Callback Wiring
// ============================================================================

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskTokenClaims {
    #[serde(alias = "task_id")]
    task_id: String,
    #[serde(alias = "tenant_id", alias = "tenantId")]
    tenant: Option<String>,
    #[serde(alias = "workspace_id", alias = "workspaceId")]
    workspace: Option<String>,
    #[serde(alias = "runId", alias = "run_id")]
    run_id: Option<String>,
    attempt: Option<u32>,
    iss: Option<String>,
    aud: Option<Value>,
    sub: Option<String>,
    email: Option<String>,
    azp: Option<String>,
    #[allow(dead_code)]
    exp: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskTokenScopeMode {
    Dual,
    Strict,
}

impl TaskTokenScopeMode {
    fn from_env() -> Self {
        let configured = std::env::var("ARCO_TASK_TOKEN_SCOPE_MODE")
            .ok()
            .map(|v| v.to_ascii_lowercase())
            .unwrap_or_else(|| "dual".to_string());

        match configured.as_str() {
            "strict" => Self::Strict,
            "dual" => {
                if strict_cutoff_reached() {
                    Self::Strict
                } else {
                    Self::Dual
                }
            }
            _ => {
                tracing::warn!(
                    mode = %configured,
                    "invalid ARCO_TASK_TOKEN_SCOPE_MODE; defaulting to dual"
                );
                if strict_cutoff_reached() {
                    Self::Strict
                } else {
                    Self::Dual
                }
            }
        }
    }
}

fn strict_cutoff_reached() -> bool {
    let cutoff = std::env::var("ARCO_TASK_TOKEN_STRICT_CUTOFF")
        .ok()
        .unwrap_or_else(|| "2026-03-13T00:00:00Z".to_string());
    DateTime::parse_from_rfc3339(&cutoff)
        .map(|dt| Utc::now() >= dt.with_timezone(&Utc))
        .unwrap_or(false)
}

#[derive(Clone)]
struct JwtTaskTokenValidator {
    decoding_key: Option<Arc<DecodingKey>>,
    validation: Validation,
    tenant: String,
    workspace: String,
    debug: bool,
    scope_mode: TaskTokenScopeMode,
}

impl JwtTaskTokenValidator {
    fn new(
        config: &JwtConfig,
        tenant: &str,
        workspace: &str,
        debug: bool,
    ) -> Result<Self, ApiError> {
        Self::new_with_scope_mode(
            config,
            tenant,
            workspace,
            debug,
            TaskTokenScopeMode::from_env(),
        )
    }

    fn new_with_scope_mode(
        config: &JwtConfig,
        tenant: &str,
        workspace: &str,
        debug: bool,
        scope_mode: TaskTokenScopeMode,
    ) -> Result<Self, ApiError> {
        if debug {
            return Ok(Self {
                decoding_key: None,
                validation: Validation::new(Algorithm::HS256),
                tenant: tenant.to_string(),
                workspace: workspace.to_string(),
                debug: true,
                scope_mode,
            });
        }

        let (decoding_key, algorithm) = jwt_decoding_key(config)?;
        let mut validation = Validation::new(algorithm);
        validation.validate_nbf = true;

        if let Some(iss) = config.issuer.as_deref() {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = config.audience.as_deref() {
            validation.set_audience(&[aud]);
        }

        Ok(Self {
            decoding_key: Some(Arc::new(decoding_key)),
            validation,
            tenant: tenant.to_string(),
            workspace: workspace.to_string(),
            debug,
            scope_mode,
        })
    }
}

impl TaskTokenValidator for JwtTaskTokenValidator {
    fn validate_task_token(
        &self,
        task_id: &str,
        run_id: &str,
        attempt: u32,
        token: &str,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let validator = self.clone();
        let task_id = task_id.to_string();
        let run_id = run_id.to_string();
        let token = token.to_string();

        async move {
            if validator.debug {
                return if token.is_empty() {
                    Err("missing token".to_string())
                } else {
                    Ok(())
                };
            }

            let Some(decoding_key) = validator.decoding_key.as_ref() else {
                return Err("task token validation not configured".to_string());
            };

            let data = jsonwebtoken::decode::<TaskTokenClaims>(
                &token,
                decoding_key.as_ref(),
                &validator.validation,
            )
            .map_err(|e| format!("invalid token: {e}"))?;

            if data.claims.task_id != task_id {
                return Err("task_id_mismatch".to_string());
            }
            if let Some(tenant) = data.claims.tenant.as_deref() {
                if tenant != validator.tenant {
                    return Err("tenant_mismatch".to_string());
                }
            }
            if let Some(workspace) = data.claims.workspace.as_deref() {
                if workspace != validator.workspace {
                    return Err("workspace_mismatch".to_string());
                }
            }
            let strict_scope = validator.scope_mode == TaskTokenScopeMode::Strict;

            if strict_scope && data.claims.iss.as_deref().is_none_or(str::is_empty) {
                return Err("missing_iss_claim".to_string());
            }
            if strict_scope && !aud_claim_present(&data.claims.aud) {
                return Err("missing_aud_claim".to_string());
            }

            match data.claims.run_id.as_deref() {
                Some(claim_run_id) => {
                    if claim_run_id != run_id {
                        return Err("run_id_mismatch".to_string());
                    }
                }
                None if strict_scope => {
                    return Err("missing_run_id_claim".to_string());
                }
                None => {}
            }
            match data.claims.attempt {
                Some(claim_attempt) => {
                    if claim_attempt != attempt {
                        return Err("attempt_mismatch".to_string());
                    }
                }
                None if strict_scope => {
                    return Err("missing_attempt_claim".to_string());
                }
                None => {}
            }

            Ok(())
        }
    }
}

#[derive(Clone)]
struct ParquetTaskStateLookup {
    state: Arc<FoldState>,
}

impl ParquetTaskStateLookup {
    async fn load(storage: arco_core::ScopedStorage) -> Result<Self, ApiError> {
        let compactor = MicroCompactor::new(storage);
        let (_, state) = compactor
            .load_state()
            .await
            .map_err(|e| ApiError::internal(format!("failed to load orchestration state: {e}")))?;
        Ok(Self {
            state: Arc::new(state),
        })
    }
}

impl TaskStateLookup for ParquetTaskStateLookup {
    fn get_task_state(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<CallbackTaskState>, String>> + Send {
        let state = Arc::clone(&self.state);
        let task_id = task_id.to_string();
        async move {
            // Task IDs are expected to be unique task keys in orchestration state.
            let matches: Vec<_> = state
                .tasks
                .values()
                .filter(|row| row.task_key == task_id)
                .collect();

            if matches.is_empty() {
                return Ok(None);
            }
            if matches.len() > 1 {
                return Err(format!("task_id_ambiguous: {task_id}"));
            }

            let Some(row) = matches.first() else {
                return Ok(None);
            };
            let run = state.runs.get(&row.run_id);
            let cancel_requested = run.is_some_and(|run| run.cancel_requested);
            let code_version = run.and_then(|run| run.code_version.clone());
            Ok(Some(CallbackTaskState {
                state: fold_task_state_label(row.state).to_string(),
                attempt: row.attempt,
                attempt_id: row.attempt_id.clone().unwrap_or_default(),
                run_id: row.run_id.clone(),
                asset_key: row.asset_key.clone(),
                partition_key: row.partition_key.clone(),
                code_version,
                cancel_requested,
            }))
        }
    }
}

fn fold_task_state_label(state: FoldTaskState) -> &'static str {
    match state {
        FoldTaskState::Planned => "PLANNED",
        FoldTaskState::Blocked => "BLOCKED",
        FoldTaskState::Ready => "READY",
        FoldTaskState::Dispatched => "DISPATCHED",
        FoldTaskState::Running => "RUNNING",
        FoldTaskState::RetryWait => "RETRY_WAIT",
        FoldTaskState::Skipped => "SKIPPED",
        FoldTaskState::Cancelled => "CANCELLED",
        FoldTaskState::Failed => "FAILED",
        FoldTaskState::Succeeded => "SUCCEEDED",
    }
}

fn jwt_decoding_key(config: &JwtConfig) -> Result<(DecodingKey, Algorithm), ApiError> {
    match (
        config.hs256_secret.as_deref(),
        config.rs256_public_key_pem.as_deref(),
    ) {
        (Some(secret), None) => Ok((
            DecodingKey::from_secret(secret.as_bytes()),
            Algorithm::HS256,
        )),
        (None, Some(pem)) => DecodingKey::from_rsa_pem(pem.as_bytes())
            .map(|key| (key, Algorithm::RS256))
            .map_err(|e| {
                ApiError::internal(format!("failed to parse jwt.rs256_public_key_pem: {e}"))
            }),
        (Some(_), Some(_)) => Err(ApiError::internal(
            "jwt.hs256_secret and jwt.rs256_public_key_pem are mutually exclusive",
        )),
        (None, None) => Err(ApiError::internal(
            "jwt.hs256_secret or jwt.rs256_public_key_pem is required for task tokens",
        )),
    }
}

fn callback_error_response(error: CallbackError) -> CallbackErrorResponse {
    CallbackErrorResponse {
        error: error.error,
        message: error.message,
        state: error.state,
        expected_attempt: error.expected_attempt,
        received_attempt: error.received_attempt,
        expected_attempt_id: error.expected_attempt_id,
        received_attempt_id: error.received_attempt_id,
    }
}

fn callback_result_response<T, U>(
    result: CallbackResult<T>,
) -> Result<axum::response::Response, ApiError>
where
    T: Into<U>,
    U: Serialize,
{
    let response = match result {
        CallbackResult::Ok(payload) => (StatusCode::OK, Json(payload.into())).into_response(),
        CallbackResult::BadRequest(error) => (
            StatusCode::BAD_REQUEST,
            Json(callback_error_response(error)),
        )
            .into_response(),
        CallbackResult::Conflict(error) => {
            (StatusCode::CONFLICT, Json(callback_error_response(error))).into_response()
        }
        CallbackResult::Gone(error) => {
            (StatusCode::GONE, Json(callback_error_response(error))).into_response()
        }
        CallbackResult::Unauthorized(error) => (
            StatusCode::UNAUTHORIZED,
            Json(callback_error_response(error)),
        )
            .into_response(),
        CallbackResult::NotFound(error) => {
            (StatusCode::NOT_FOUND, Json(callback_error_response(error))).into_response()
        }
        CallbackResult::InternalError(message) => return Err(ApiError::internal(message)),
    };

    Ok(response)
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    header_string(headers, "X-Request-Id").or_else(|| header_string(headers, "X-Request-ID"))
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?;
    header_value_to_string(value)
}

fn header_value_to_string(value: &HeaderValue) -> Option<String> {
    value.to_str().ok().map(str::to_string)
}

fn aud_claim_present(aud: &Option<Value>) -> bool {
    match aud {
        Some(Value::String(value)) => !value.trim().is_empty(),
        Some(Value::Array(values)) => values
            .iter()
            .any(|value| value.as_str().is_some_and(|s| !s.trim().is_empty())),
        Some(_) => true,
        None => false,
    }
}

fn unauthorized_response(request_id: &str, message: &str) -> axum::response::Response {
    let error = CallbackError::invalid_token(message);
    let mut response = (
        StatusCode::UNAUTHORIZED,
        Json(callback_error_response(error)),
    )
        .into_response();

    if let Ok(value) = HeaderValue::from_str(request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }

    response
}

fn decode_task_claims(config: &JwtConfig, token: &str) -> Result<TaskTokenClaims, String> {
    let (decoding_key, algorithm) = jwt_decoding_key(config).map_err(|err| format!("{err:?}"))?;
    let mut validation = Validation::new(algorithm);
    validation.validate_nbf = true;

    if let Some(iss) = config.issuer.as_deref() {
        validation.set_issuer(&[iss]);
    }
    if let Some(aud) = config.audience.as_deref() {
        validation.set_audience(&[aud]);
    }

    let data = jsonwebtoken::decode::<TaskTokenClaims>(token, &decoding_key, &validation)
        .map_err(|e| format!("invalid token: {e}"))?;
    let claims = data.claims;
    if config.issuer.is_some() && claims.iss.as_deref().is_none_or(str::is_empty) {
        return Err("missing iss claim".to_string());
    }
    if config.audience.is_some() && !aud_claim_present(&claims.aud) {
        return Err("missing aud claim".to_string());
    }

    Ok(claims)
}

/// Task callback auth middleware.
///
/// Validates the task token and injects a `RequestContext` derived from the token claims.
pub async fn task_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let (mut parts, body) = req.into_parts();
    let headers = &parts.headers;
    let resource = parts.uri.path().to_string();

    let request_id = request_id_from_headers(headers).unwrap_or_else(|| Ulid::new().to_string());
    let idempotency_key = header_string(headers, "Idempotency-Key");

    let Some(token) = extract_bearer_token(headers) else {
        crate::audit::emit_auth_deny(
            &state,
            &request_id,
            &resource,
            crate::audit::REASON_MISSING_TOKEN,
        );
        return unauthorized_response(&request_id, "missing bearer token");
    };

    let debug_allowed = state.config.debug && state.config.posture.is_dev();

    let (tenant, workspace) = if debug_allowed {
        let tenant = header_string(headers, "X-Tenant-Id").unwrap_or_default();
        let workspace = header_string(headers, "X-Workspace-Id").unwrap_or_default();
        if tenant.is_empty() || workspace.is_empty() {
            crate::audit::emit_auth_deny(
                &state,
                &request_id,
                &resource,
                crate::audit::REASON_MISSING_TOKEN,
            );
            return unauthorized_response(&request_id, "missing tenant/workspace headers");
        }
        (tenant, workspace)
    } else {
        let claims = match decode_task_claims(&state.config.jwt, &token) {
            Ok(claims) => claims,
            Err(err) => {
                crate::audit::emit_auth_deny(
                    &state,
                    &request_id,
                    &resource,
                    crate::audit::REASON_INVALID_TOKEN,
                );
                return unauthorized_response(&request_id, &err);
            }
        };

        let Some(tenant) = claims.tenant else {
            crate::audit::emit_auth_deny(
                &state,
                &request_id,
                &resource,
                crate::audit::REASON_INVALID_TOKEN,
            );
            return unauthorized_response(&request_id, "missing tenant claim");
        };
        let Some(workspace) = claims.workspace else {
            crate::audit::emit_auth_deny(
                &state,
                &request_id,
                &resource,
                crate::audit::REASON_INVALID_TOKEN,
            );
            return unauthorized_response(&request_id, "missing workspace claim");
        };
        (tenant, workspace)
    };

    let ctx = RequestContext {
        tenant,
        workspace,
        user_id: None,
        groups: vec![],
        request_id: request_id.clone(),
        idempotency_key,
    };

    crate::audit::emit_auth_allow(&state, &ctx, &resource);

    parts.extensions.insert(ctx.clone());
    let mut req = Request::from_parts(parts, body);
    req.extensions_mut().insert(ctx);

    let mut response = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }

    response
}

async fn build_callback_dependencies(
    ctx: &RequestContext,
    state: &AppState,
) -> Result<
    (
        CallbackContext<CompactingLedgerWriter, JwtTaskTokenValidator>,
        ParquetTaskStateLookup,
    ),
    ApiError,
> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let lookup = ParquetTaskStateLookup::load(storage.clone()).await?;
    let ledger = Arc::new(CompactingLedgerWriter::new(storage, state.config.clone()));
    let debug_allowed = state.config.debug && state.config.posture.is_dev();
    let validator = Arc::new(JwtTaskTokenValidator::new(
        &state.config.jwt,
        &ctx.tenant,
        &ctx.workspace,
        debug_allowed,
    )?);
    let callback_ctx =
        CallbackContext::new(ledger, validator, ctx.tenant.clone(), ctx.workspace.clone());

    Ok((callback_ctx, lookup))
}

impl From<TaskStartedRequest> for FlowTaskStartedRequest {
    fn from(request: TaskStartedRequest) -> Self {
        Self {
            attempt: request.attempt,
            attempt_id: request.attempt_id,
            worker_id: request.worker_id,
            traceparent: request.traceparent,
            started_at: request.started_at,
        }
    }
}

impl From<FlowTaskStartedResponse> for TaskStartedResponse {
    fn from(response: FlowTaskStartedResponse) -> Self {
        Self {
            acknowledged: response.acknowledged,
            server_time: response.server_time,
        }
    }
}

impl From<HeartbeatRequest> for FlowHeartbeatRequest {
    fn from(request: HeartbeatRequest) -> Self {
        Self {
            attempt: request.attempt,
            attempt_id: request.attempt_id,
            worker_id: request.worker_id,
            traceparent: request.traceparent,
            heartbeat_at: request.heartbeat_at,
            progress_pct: request.progress_pct,
            message: request.message,
        }
    }
}

impl From<FlowHeartbeatResponse> for HeartbeatResponse {
    fn from(response: FlowHeartbeatResponse) -> Self {
        Self {
            acknowledged: response.acknowledged,
            should_cancel: response.should_cancel,
            cancel_reason: response.cancel_reason,
            server_time: response.server_time,
        }
    }
}

impl From<TaskCompletedRequest> for FlowTaskCompletedRequest {
    fn from(request: TaskCompletedRequest) -> Self {
        Self {
            attempt: request.attempt,
            attempt_id: request.attempt_id,
            worker_id: request.worker_id,
            traceparent: request.traceparent,
            outcome: request.outcome.into(),
            completed_at: request.completed_at,
            output: request.output.map(Into::into),
            error: request.error.map(Into::into),
            metrics: request.metrics.map(Into::into),
            cancelled_during_phase: request.cancelled_during_phase,
            partial_progress: request.partial_progress,
        }
    }
}

impl From<FlowTaskCompletedResponse> for TaskCompletedResponse {
    fn from(response: FlowTaskCompletedResponse) -> Self {
        Self {
            acknowledged: response.acknowledged,
            final_state: response.final_state,
            server_time: response.server_time,
        }
    }
}

impl From<WorkerOutcome> for FlowWorkerOutcome {
    fn from(outcome: WorkerOutcome) -> Self {
        match outcome {
            WorkerOutcome::Succeeded => Self::Succeeded,
            WorkerOutcome::Failed => Self::Failed,
            WorkerOutcome::Cancelled => Self::Cancelled,
        }
    }
}

impl From<ErrorCategory> for FlowErrorCategory {
    fn from(category: ErrorCategory) -> Self {
        match category {
            ErrorCategory::UserCode => Self::UserCode,
            ErrorCategory::DataQuality => Self::DataQuality,
            ErrorCategory::Infrastructure => Self::Infrastructure,
            ErrorCategory::Configuration => Self::Configuration,
            ErrorCategory::Timeout => Self::Timeout,
            ErrorCategory::Cancelled => Self::Cancelled,
        }
    }
}

impl From<TaskOutput> for FlowTaskOutput {
    fn from(output: TaskOutput) -> Self {
        Self {
            materialization_id: output.materialization_id,
            row_count: output.row_count,
            byte_size: output.byte_size,
            output_path: output.output_path,
        }
    }
}

impl From<TaskError> for FlowTaskError {
    fn from(error: TaskError) -> Self {
        Self {
            category: error.category.into(),
            message: error.message,
            stack_trace: error.stack_trace,
            retryable: error.retryable,
        }
    }
}

impl From<TaskMetrics> for FlowTaskMetrics {
    fn from(metrics: TaskMetrics) -> Self {
        Self {
            cpu_time_ms: metrics.cpu_time_ms,
            peak_memory_bytes: metrics.peak_memory_bytes,
            io_read_bytes: metrics.io_read_bytes,
            io_write_bytes: metrics.io_write_bytes,
        }
    }
}

// ============================================================================
// Route Handlers
// ============================================================================

/// Extract bearer token from Authorization header.
fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::to_string)
}

/// Worker started execution callback.
///
/// Called when a worker begins executing a task.
#[utoipa::path(
    post,
    path = "/api/v1/tasks/{task_id}/started",
    params(
        ("task_id" = String, Path, description = "Task ID")
    ),
    request_body = TaskStartedRequest,
    responses(
        (status = 200, description = "Callback acknowledged", body = TaskStartedResponse),
        (status = 400, description = "Invalid request", body = CallbackErrorResponse),
        (status = 401, description = "Invalid or missing token", body = CallbackErrorResponse),
        (status = 404, description = "Task not found", body = CallbackErrorResponse),
        (status = 409, description = "Conflict (terminal state or attempt mismatch)", body = CallbackErrorResponse),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    tag = "Worker Callbacks",
    security(
        ("taskAuth" = [])
    )
)]
pub(crate) async fn task_started(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(request): Json<TaskStartedRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        task_id = %task_id,
        attempt = request.attempt,
        worker_id = %request.worker_id,
        "Task started callback"
    );

    let Some(token) = extract_bearer_token(&headers) else {
        let error = CallbackError::invalid_token("missing bearer token");
        return callback_result_response::<FlowTaskStartedResponse, TaskStartedResponse>(
            CallbackResult::Unauthorized(error),
        );
    };

    let (callback_ctx, lookup) = build_callback_dependencies(&ctx, &state).await?;
    let result =
        handle_task_started(&callback_ctx, &task_id, &token, request.into(), &lookup).await;

    callback_result_response::<FlowTaskStartedResponse, TaskStartedResponse>(result)
}

/// Worker heartbeat callback.
///
/// Called periodically by workers to indicate they're still alive and report progress.
#[utoipa::path(
    post,
    path = "/api/v1/tasks/{task_id}/heartbeat",
    params(
        ("task_id" = String, Path, description = "Task ID")
    ),
    request_body = HeartbeatRequest,
    responses(
        (status = 200, description = "Heartbeat acknowledged", body = HeartbeatResponse),
        (status = 400, description = "Invalid request", body = CallbackErrorResponse),
        (status = 401, description = "Invalid or missing token", body = CallbackErrorResponse),
        (status = 404, description = "Task not found", body = CallbackErrorResponse),
        (status = 409, description = "Conflict (attempt mismatch)", body = CallbackErrorResponse),
        (status = 410, description = "Task no longer active", body = CallbackErrorResponse),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    tag = "Worker Callbacks",
    security(
        ("taskAuth" = [])
    )
)]
pub(crate) async fn task_heartbeat(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(request): Json<HeartbeatRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        task_id = %task_id,
        attempt = request.attempt,
        progress_pct = ?request.progress_pct,
        "Task heartbeat callback"
    );

    let Some(token) = extract_bearer_token(&headers) else {
        let error = CallbackError::invalid_token("missing bearer token");
        return callback_result_response::<FlowHeartbeatResponse, HeartbeatResponse>(
            CallbackResult::Unauthorized(error),
        );
    };

    let (callback_ctx, lookup) = build_callback_dependencies(&ctx, &state).await?;
    let result = handle_heartbeat(&callback_ctx, &task_id, &token, request.into(), &lookup).await;

    callback_result_response::<FlowHeartbeatResponse, HeartbeatResponse>(result)
}

/// Worker task completed callback.
///
/// Called when a worker finishes executing a task (success, failure, or cancellation).
#[utoipa::path(
    post,
    path = "/api/v1/tasks/{task_id}/completed",
    params(
        ("task_id" = String, Path, description = "Task ID")
    ),
    request_body = TaskCompletedRequest,
    responses(
        (status = 200, description = "Completion acknowledged", body = TaskCompletedResponse),
        (status = 400, description = "Invalid request", body = CallbackErrorResponse),
        (status = 401, description = "Invalid or missing token", body = CallbackErrorResponse),
        (status = 404, description = "Task not found", body = CallbackErrorResponse),
        (status = 409, description = "Conflict (terminal state or attempt mismatch)", body = CallbackErrorResponse),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    tag = "Worker Callbacks",
    security(
        ("taskAuth" = [])
    )
)]
pub(crate) async fn task_completed(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(request): Json<TaskCompletedRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        task_id = %task_id,
        attempt = request.attempt,
        outcome = ?request.outcome,
        "Task completed callback"
    );

    let Some(token) = extract_bearer_token(&headers) else {
        let error = CallbackError::invalid_token("missing bearer token");
        return callback_result_response::<FlowTaskCompletedResponse, TaskCompletedResponse>(
            CallbackResult::Unauthorized(error),
        );
    };

    let (callback_ctx, lookup) = build_callback_dependencies(&ctx, &state).await?;
    let result =
        handle_task_completed(&callback_ctx, &task_id, &token, request.into(), &lookup).await;

    callback_result_response::<FlowTaskCompletedResponse, TaskCompletedResponse>(result)
}

// ============================================================================
// Router
// ============================================================================

/// Creates the task callback routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/tasks/:task_id/started", post(task_started))
        .route("/tasks/:task_id/heartbeat", post(task_heartbeat))
        .route("/tasks/:task_id/completed", post(task_completed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Posture;
    use anyhow::Result;
    use axum::body::Body as AxumBody;
    use axum::http::Request as AxumRequest;
    use axum::routing::get;
    use jsonwebtoken::{EncodingKey, Header};
    use tower::ServiceExt;

    #[test]
    fn test_started_request_deserialization() {
        let json = r#"{
            "attempt": 1,
            "attemptId": "att-123",
            "workerId": "worker-abc",
            "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "startedAt": "2024-01-15T10:00:00Z"
        }"#;

        let request: TaskStartedRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.attempt, 1);
        assert_eq!(request.attempt_id, "att-123");
        assert_eq!(request.worker_id, "worker-abc");
        assert_eq!(
            request.traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert!(request.started_at.is_some());
    }

    #[test]
    fn test_heartbeat_request_deserialization() {
        let json = r#"{
            "attempt": 1,
            "attemptId": "att-123",
            "workerId": "worker-abc",
            "progressPct": 50,
            "message": "Processing batch 5 of 10"
        }"#;

        let request: HeartbeatRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.attempt, 1);
        assert_eq!(request.progress_pct, Some(50));
        assert_eq!(
            request.message,
            Some("Processing batch 5 of 10".to_string())
        );
        assert!(request.traceparent.is_none());
    }

    #[test]
    fn test_completed_request_success() {
        let json = r#"{
            "attempt": 1,
            "attemptId": "att-123",
            "workerId": "worker-abc",
            "outcome": "SUCCEEDED",
            "output": {
                "materializationId": "mat-456",
                "rowCount": 1000,
                "byteSize": 52428800
            }
        }"#;

        let request: TaskCompletedRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.attempt, 1);
        assert!(matches!(request.outcome, WorkerOutcome::Succeeded));
        assert!(request.output.is_some());
        assert!(request.error.is_none());
        assert!(request.traceparent.is_none());
    }

    #[test]
    fn test_completed_request_failure() {
        let json = r#"{
            "attempt": 1,
            "attemptId": "att-123",
            "workerId": "worker-abc",
            "outcome": "FAILED",
            "error": {
                "category": "USER_CODE",
                "message": "KeyError: 'missing_column'",
                "stackTrace": "Traceback...",
                "retryable": true
            }
        }"#;

        let request: TaskCompletedRequest = serde_json::from_str(json).expect("deserialize");
        assert!(matches!(request.outcome, WorkerOutcome::Failed));
        assert!(request.error.is_some());
        let error = request.error.unwrap();
        assert!(matches!(error.category, ErrorCategory::UserCode));
        assert_eq!(error.retryable, Some(true));
        assert!(request.traceparent.is_none());
    }

    #[test]
    fn test_extract_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test-token-123".parse().unwrap());

        let token = extract_bearer_token(&headers);
        assert_eq!(token, Some("test-token-123".to_string()));
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let headers = HeaderMap::new();
        let token = extract_bearer_token(&headers);
        assert_eq!(token, None);
    }

    #[test]
    fn test_extract_bearer_token_wrong_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());

        let token = extract_bearer_token(&headers);
        assert_eq!(token, None);
    }

    #[test]
    fn test_jwt_task_token_validator_accepts_valid_token() {
        let mut config = JwtConfig::default();
        config.hs256_secret = Some("test-secret".to_string());
        config.issuer = Some("https://issuer.example".to_string());
        config.audience = Some("arco-api".to_string());

        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "runId": "run-123",
            "attempt": 1,
            "iss": "https://issuer.example",
            "aud": "arco-api",
            "exp": exp
        });
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-123", 1, &token));
        assert!(result.is_ok());
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_task_id_mismatch() {
        let mut config = JwtConfig::default();
        config.hs256_secret = Some("test-secret".to_string());
        config.issuer = Some("https://issuer.example".to_string());
        config.audience = Some("arco-api".to_string());

        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "runId": "run-123",
            "attempt": 1,
            "iss": "https://issuer.example",
            "aud": "arco-api",
            "exp": exp
        });
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");

        let result =
            tokio_test::block_on(validator.validate_task_token("task-999", "run-123", 1, &token));
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_run_id_mismatch_in_strict_mode() {
        let mut config = JwtConfig::default();
        config.hs256_secret = Some("test-secret".to_string());
        config.issuer = Some("https://issuer.example".to_string());
        config.audience = Some("arco-api".to_string());

        let validator = JwtTaskTokenValidator::new_with_scope_mode(
            &config,
            "tenant-1",
            "workspace-1",
            false,
            TaskTokenScopeMode::Strict,
        )
        .expect("validator");

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "runId": "run-123",
            "attempt": 1,
            "iss": "https://issuer.example",
            "aud": "arco-api",
            "exp": exp
        });
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-999", 1, &token));
        assert_eq!(result.err().as_deref(), Some("run_id_mismatch"));
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_attempt_mismatch_in_strict_mode() {
        let mut config = JwtConfig::default();
        config.hs256_secret = Some("test-secret".to_string());
        config.issuer = Some("https://issuer.example".to_string());
        config.audience = Some("arco-api".to_string());

        let validator = JwtTaskTokenValidator::new_with_scope_mode(
            &config,
            "tenant-1",
            "workspace-1",
            false,
            TaskTokenScopeMode::Strict,
        )
        .expect("validator");

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "runId": "run-123",
            "attempt": 1,
            "iss": "https://issuer.example",
            "aud": "arco-api",
            "exp": exp
        });
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-123", 2, &token));
        assert_eq!(result.err().as_deref(), Some("attempt_mismatch"));
    }

    #[test]
    fn test_decode_task_claims_rejects_missing_iss_and_aud_when_configured() {
        let mut config = JwtConfig::default();
        config.hs256_secret = Some("test-secret".to_string());
        config.issuer = Some("https://issuer.example".to_string());
        config.audience = Some("arco-api".to_string());

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims_missing_iss = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "aud": "arco-api",
            "exp": exp
        });
        let token_missing_iss = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims_missing_iss,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");
        let err = decode_task_claims(&config, &token_missing_iss).expect_err("missing iss");
        assert!(err.contains("missing iss claim"));

        let claims_missing_aud = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "iss": "https://issuer.example",
            "exp": exp
        });
        let token_missing_aud = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims_missing_aud,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");
        let err = decode_task_claims(&config, &token_missing_aud).expect_err("missing aud");
        assert!(err.contains("missing aud claim"));
    }

    #[tokio::test]
    async fn test_task_auth_middleware_rejects_missing_token() -> Result<()> {
        let config = crate::config::Config {
            debug: false,
            jwt: JwtConfig {
                hs256_secret: Some("test-secret".to_string()),
                ..JwtConfig::default()
            },
            ..crate::config::Config::default()
        };
        let state = Arc::new(AppState::with_memory_storage(config));

        let app = Router::new()
            .route(
                "/api/v1/tasks/task-123/started",
                get(|_ctx: RequestContext| async { StatusCode::OK }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                task_auth_middleware,
            ))
            .with_state(state);

        let request = AxumRequest::builder()
            .uri("/api/v1/tasks/task-123/started")
            .body(AxumBody::empty())?;
        let response = app.oneshot(request).await?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn test_task_auth_middleware_accepts_valid_token() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = false;
        config.jwt.hs256_secret = Some("test-secret".to_string());
        config.jwt.issuer = Some("https://issuer.example".to_string());
        config.jwt.audience = Some("arco-api".to_string());
        let state = Arc::new(AppState::with_memory_storage(config));

        let app = Router::new()
            .route(
                "/api/v1/tasks/task-123/started",
                get(|ctx: RequestContext| async move {
                    Json(serde_json::json!({
                        "tenant": ctx.tenant,
                        "workspace": ctx.workspace,
                    }))
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                task_auth_middleware,
            ))
            .with_state(state);

        let token = jsonwebtoken::encode(
            &Header::default(),
            &TaskTokenClaims {
                task_id: "task-123".to_string(),
                tenant: Some("tenant-1".to_string()),
                workspace: Some("workspace-1".to_string()),
                run_id: Some("run-123".to_string()),
                attempt: Some(1),
                iss: Some("https://issuer.example".to_string()),
                aud: Some(Value::String("arco-api".to_string())),
                sub: Some("worker-sa".to_string()),
                email: None,
                azp: None,
                exp: 2_000_000_000,
            },
            &EncodingKey::from_secret("test-secret".as_bytes()),
        )?;

        let request = AxumRequest::builder()
            .uri("/api/v1/tasks/task-123/started")
            .header("authorization", format!("Bearer {token}"))
            .body(AxumBody::empty())?;
        let response = app.oneshot(request).await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        let payload: Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["tenant"], "tenant-1");
        assert_eq!(payload["workspace"], "workspace-1");
        Ok(())
    }

    #[tokio::test]
    async fn test_task_auth_middleware_accepts_debug_headers_in_dev() -> Result<()> {
        let config = crate::config::Config {
            debug: true,
            posture: Posture::Dev,
            ..crate::config::Config::default()
        };
        let state = Arc::new(AppState::with_memory_storage(config));

        let app = Router::new()
            .route(
                "/api/v1/tasks/task-123/started",
                get(|ctx: RequestContext| async move {
                    Json(serde_json::json!({
                        "tenant": ctx.tenant,
                        "workspace": ctx.workspace,
                    }))
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                task_auth_middleware,
            ))
            .with_state(state);

        let request = AxumRequest::builder()
            .uri("/api/v1/tasks/task-123/started")
            .header("authorization", "Bearer debug-token")
            .header("X-Tenant-Id", "tenant-1")
            .header("X-Workspace-Id", "workspace-1")
            .body(AxumBody::empty())?;
        let response = app.oneshot(request).await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        let payload: Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["tenant"], "tenant-1");
        assert_eq!(payload["workspace"], "workspace-1");
        Ok(())
    }

    #[tokio::test]
    async fn test_task_auth_middleware_rejects_debug_headers_outside_dev() -> Result<()> {
        let config = crate::config::Config {
            debug: true,
            posture: Posture::Private,
            jwt: JwtConfig {
                hs256_secret: Some("test-secret".to_string()),
                ..JwtConfig::default()
            },
            ..crate::config::Config::default()
        };
        let state = Arc::new(AppState::with_memory_storage(config));

        let app = Router::new()
            .route(
                "/api/v1/tasks/task-123/started",
                get(|_ctx: RequestContext| async { StatusCode::OK }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                task_auth_middleware,
            ))
            .with_state(state);

        let request = AxumRequest::builder()
            .uri("/api/v1/tasks/task-123/started")
            .header("authorization", "Bearer not-a-jwt")
            .header("X-Tenant-Id", "tenant-1")
            .header("X-Workspace-Id", "workspace-1")
            .body(AxumBody::empty())?;
        let response = app.oneshot(request).await?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }
}
