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
use axum::{Extension, Json, Router};
use serde::Serialize;

use crate::context::{REQUEST_ID_HEADER, RequestContext};
use crate::error::{ApiError, ApiErrorBody};
use crate::orchestration_compaction::CompactingLedgerWriter;
use crate::server::AppState;

use arco_core::{TaskTokenClaims, TaskTokenConfig, decode_task_token};
use arco_flow::orchestration::callbacks::{
    CallbackContext, CallbackError, CallbackResult, TaskState as CallbackTaskState,
    TaskStateLookup, TaskTokenValidator, handle_heartbeat, handle_task_completed,
    handle_task_started,
};
use arco_flow::orchestration::compactor::{
    FoldState, MicroCompactor, TaskRow, TaskState as FoldTaskState,
};
use arco_worker_contract::parse_callback_task_id;
pub use arco_worker_contract::{
    CallbackErrorResponse, ErrorCategory, HeartbeatRequest, HeartbeatResponse,
    TaskCompletedRequest, TaskCompletedResponse, TaskError, TaskMetrics, TaskOutput,
    TaskOutputVisibilityState, TaskStartedRequest, TaskStartedResponse, WorkerOutcome,
};
use ulid::Ulid;

// ============================================================================
// Callback Wiring
// ============================================================================

#[derive(Clone)]
struct JwtTaskTokenValidator {
    config: TaskTokenConfig,
    tenant: String,
    workspace: String,
    debug: bool,
}

impl JwtTaskTokenValidator {
    fn new(
        config: &TaskTokenConfig,
        tenant: &str,
        workspace: &str,
        debug: bool,
    ) -> Result<Self, ApiError> {
        if debug {
            return Ok(Self {
                config: config.clone(),
                tenant: tenant.to_string(),
                workspace: workspace.to_string(),
                debug: true,
            });
        }

        config
            .validate()
            .map_err(|e| ApiError::internal(e.to_string()))?;

        Ok(Self {
            config: config.clone(),
            tenant: tenant.to_string(),
            workspace: workspace.to_string(),
            debug,
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

            let claims = decode_task_token(&validator.config, &token)
                .map_err(|e| format!("invalid token: {e}"))?;

            if claims.task_id != task_id {
                return Err("task_id_mismatch".to_string());
            }
            if claims.tenant_id != validator.tenant {
                return Err("tenant_mismatch".to_string());
            }
            if claims.workspace_id != validator.workspace {
                return Err("workspace_mismatch".to_string());
            }
            if let Some(claim_run_id) = claims.run_id.as_deref() {
                if claim_run_id != run_id {
                    return Err("run_id_mismatch".to_string());
                }
            }
            if let Some(claim_attempt) = claims.attempt {
                if claim_attempt != attempt {
                    return Err("attempt_mismatch".to_string());
                }
            }

            Ok(())
        }
    }
}

#[derive(Clone)]
struct ParquetTaskStateLookup {
    state: Arc<FoldState>,
    run_id_scope: Option<String>,
}

impl ParquetTaskStateLookup {
    async fn load(
        storage: arco_core::ScopedStorage,
        run_id_scope: Option<String>,
    ) -> Result<Self, ApiError> {
        let compactor = MicroCompactor::new(storage);
        let (_, state) = compactor
            .load_state()
            .await
            .map_err(|e| ApiError::internal(format!("failed to load orchestration state: {e}")))?;
        Ok(Self {
            state: Arc::new(state),
            run_id_scope,
        })
    }
}

impl TaskStateLookup for ParquetTaskStateLookup {
    fn get_task_state(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<CallbackTaskState>, String>> + Send {
        let state = Arc::clone(&self.state);
        let run_id_scope = self.run_id_scope.clone();
        let task_id = task_id.to_string();
        async move {
            let parsed_callback_task_id = parse_callback_task_id(&task_id).ok();

            if let Some(run_id) = run_id_scope {
                let task_key = match &parsed_callback_task_id {
                    Some(parsed) if parsed.run_id == run_id => parsed.task_key.clone(),
                    Some(_) => return Ok(None),
                    None => task_id.clone(),
                };
                let Some(row) = state.tasks.get(&(run_id, task_key)) else {
                    return Ok(None);
                };
                return Ok(Some(callback_task_state_from_row(&state, row)));
            }

            // Legacy tokens do not carry run identity, so preserve the old
            // unique-task-key lookup contract for compatibility.
            let matches: Vec<_> = state
                .tasks
                .values()
                .filter(|row| {
                    parsed_callback_task_id.as_ref().map_or_else(
                        || row.task_key == task_id,
                        |parsed| row.run_id == parsed.run_id && row.task_key == parsed.task_key,
                    )
                })
                .collect();

            if matches.is_empty() {
                return Ok(None);
            }
            if parsed_callback_task_id.is_none() && matches.len() > 1 {
                return Err(format!("task_id_ambiguous: {task_id}"));
            }

            let Some(row) = matches.first() else {
                return Ok(None);
            };
            Ok(Some(callback_task_state_from_row(&state, row)))
        }
    }
}

fn callback_task_state_from_row(state: &FoldState, row: &TaskRow) -> CallbackTaskState {
    let run = state.runs.get(&row.run_id);
    let cancel_requested = run.is_some_and(|run| run.cancel_requested);
    let code_version = run.and_then(|run| run.code_version.clone());
    CallbackTaskState {
        state: fold_task_state_label(row.state).to_string(),
        attempt: row.attempt,
        attempt_id: row.attempt_id.clone().unwrap_or_default(),
        run_id: row.run_id.clone(),
        task_key: row.task_key.clone(),
        asset_key: row.asset_key.clone(),
        partition_key: row.partition_key.clone(),
        code_version,
        cancel_requested,
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

fn callback_result_response<T>(
    result: CallbackResult<T>,
) -> Result<axum::response::Response, ApiError>
where
    T: Serialize,
{
    let response = match result {
        CallbackResult::Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
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
        let claims = match decode_task_token(&state.config.task_token, &token) {
            Ok(claims) => claims,
            Err(err) => {
                crate::audit::emit_auth_deny(
                    &state,
                    &request_id,
                    &resource,
                    crate::audit::REASON_INVALID_TOKEN,
                );
                return unauthorized_response(&request_id, &err.to_string());
            }
        };
        parts.extensions.insert(claims.clone());
        (claims.tenant_id, claims.workspace_id)
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
    run_id_scope: Option<String>,
) -> Result<
    (
        CallbackContext<CompactingLedgerWriter, JwtTaskTokenValidator>,
        ParquetTaskStateLookup,
    ),
    ApiError,
> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let lookup = ParquetTaskStateLookup::load(storage.clone(), run_id_scope).await?;
    let ledger = Arc::new(CompactingLedgerWriter::new(storage, state.config.clone()));
    let debug_allowed = state.config.debug && state.config.posture.is_dev();
    let validator = Arc::new(JwtTaskTokenValidator::new(
        &state.config.task_token,
        &ctx.tenant,
        &ctx.workspace,
        debug_allowed,
    )?);
    let callback_ctx =
        CallbackContext::new(ledger, validator, ctx.tenant.clone(), ctx.workspace.clone());

    Ok((callback_ctx, lookup))
}

fn scoped_run_id_from_claims(
    claims: Option<&Extension<TaskTokenClaims>>,
    task_id: &str,
) -> Result<Option<String>, Box<CallbackError>> {
    let Some(Extension(claims)) = claims else {
        return Ok(None);
    };
    if claims.task_id != task_id {
        return Err(Box::new(CallbackError::invalid_token("task_id_mismatch")));
    }
    Ok(claims.run_id.clone())
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
    claims: Option<Extension<TaskTokenClaims>>,
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
        return callback_result_response(CallbackResult::<TaskStartedResponse>::Unauthorized(
            error,
        ));
    };

    let run_id_scope = match scoped_run_id_from_claims(claims.as_ref(), &task_id) {
        Ok(run_id_scope) => run_id_scope,
        Err(error) => {
            return callback_result_response(CallbackResult::<TaskStartedResponse>::Unauthorized(
                *error,
            ));
        }
    };
    let (callback_ctx, lookup) = build_callback_dependencies(&ctx, &state, run_id_scope).await?;
    let result = handle_task_started(&callback_ctx, &task_id, &token, request, &lookup).await;

    callback_result_response(result)
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
    claims: Option<Extension<TaskTokenClaims>>,
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
        return callback_result_response(CallbackResult::<HeartbeatResponse>::Unauthorized(error));
    };

    let run_id_scope = match scoped_run_id_from_claims(claims.as_ref(), &task_id) {
        Ok(run_id_scope) => run_id_scope,
        Err(error) => {
            return callback_result_response(CallbackResult::<HeartbeatResponse>::Unauthorized(
                *error,
            ));
        }
    };
    let (callback_ctx, lookup) = build_callback_dependencies(&ctx, &state, run_id_scope).await?;
    let result = handle_heartbeat(&callback_ctx, &task_id, &token, request, &lookup).await;

    callback_result_response(result)
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
    claims: Option<Extension<TaskTokenClaims>>,
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
        return callback_result_response(CallbackResult::<TaskCompletedResponse>::Unauthorized(
            error,
        ));
    };

    let run_id_scope = match scoped_run_id_from_claims(claims.as_ref(), &task_id) {
        Ok(run_id_scope) => run_id_scope,
        Err(error) => {
            return callback_result_response(
                CallbackResult::<TaskCompletedResponse>::Unauthorized(*error),
            );
        }
    };
    let (callback_ctx, lookup) = build_callback_dependencies(&ctx, &state, run_id_scope).await?;
    let result = handle_task_completed(&callback_ctx, &task_id, &token, request, &lookup).await;

    callback_result_response(result)
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
    use std::collections::HashMap;

    use crate::config::Posture;
    use anyhow::Result;
    use arco_core::TaskTokenClaims;
    use arco_flow::orchestration::compactor::{RunRow, RunState as FoldRunState};
    use axum::body::Body as AxumBody;
    use axum::http::Request as AxumRequest;
    use axum::routing::get;
    use chrono::Utc;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde_json::Value;
    use tower::ServiceExt;

    fn task_row(run_id: &str, task_key: &str) -> TaskRow {
        TaskRow {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            state: FoldTaskState::Running,
            attempt: 1,
            attempt_id: Some("att-1".to_string()),
            started_at: None,
            completed_at: None,
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: None,
            ready_at: None,
            asset_key: Some("analytics.daily".to_string()),
            partition_key: None,
            requires_visible_output: false,
            materialization_id: None,
            output_visibility_state: None,
            published_at: None,
            publish_error: None,
            retry_not_before: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            execution_lineage_ref: None,
            row_version: "01JTEST".to_string(),
        }
    }

    fn lookup_with_rows(rows: Vec<TaskRow>) -> ParquetTaskStateLookup {
        let mut state = FoldState::default();
        for row in rows {
            state
                .tasks
                .insert((row.run_id.clone(), row.task_key.clone()), row);
        }
        ParquetTaskStateLookup {
            state: Arc::new(state),
            run_id_scope: None,
        }
    }

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
                "byteSize": 52428800,
                "deltaTable": "analytics.daily",
                "deltaVersion": 17,
                "deltaPartition": "date=2025-01-15"
            }
        }"#;

        let request: TaskCompletedRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.attempt, 1);
        assert!(matches!(request.outcome, WorkerOutcome::Succeeded));
        let output = request.output.expect("expected output");
        assert_eq!(output.delta_table.as_deref(), Some("analytics.daily"));
        assert_eq!(output.delta_version, Some(17));
        assert_eq!(output.delta_partition.as_deref(), Some("date=2025-01-15"));
        assert!(request.error.is_none());
        assert!(request.traceparent.is_none());
    }

    #[test]
    fn test_task_output_preserves_delta_lineage() {
        let output = TaskOutput {
            materialization_id: Some("mat-456".to_string()),
            row_count: Some(1000),
            byte_size: Some(52428800),
            output_path: Some("gs://bucket/path".to_string()),
            delta_table: Some("analytics.daily".to_string()),
            delta_version: Some(17),
            delta_partition: Some("date=2025-01-15".to_string()),
            output_visibility_state: Some(TaskOutputVisibilityState::Visible),
            published_at: Some(Utc::now()),
            publish_error: None,
        };

        assert_eq!(output.delta_table.as_deref(), Some("analytics.daily"));
        assert_eq!(output.delta_version, Some(17));
        assert_eq!(output.delta_partition.as_deref(), Some("date=2025-01-15"));
        assert_eq!(
            output.output_visibility_state,
            Some(TaskOutputVisibilityState::Visible)
        );
    }

    #[tokio::test]
    async fn parquet_task_lookup_resolves_opaque_callback_task_id() -> Result<()> {
        let lookup = lookup_with_rows(vec![
            task_row("run-1", "extract"),
            task_row("run-2", "extract"),
        ]);
        let task_id = arco_worker_contract::callback_task_id("run-2", "extract");

        let state = lookup
            .get_task_state(&task_id)
            .await
            .expect("lookup")
            .expect("task state");

        assert_eq!(state.run_id, "run-2");
        assert_eq!(state.task_key, "extract");
        assert_eq!(state.attempt_id, "att-1");
        Ok(())
    }

    #[tokio::test]
    async fn parquet_task_lookup_accepts_unambiguous_legacy_task_key() -> Result<()> {
        let lookup = lookup_with_rows(vec![task_row("run-1", "extract")]);

        let state = lookup
            .get_task_state("extract")
            .await
            .expect("lookup")
            .expect("task state");

        assert_eq!(state.run_id, "run-1");
        assert_eq!(state.task_key, "extract");
        Ok(())
    }

    #[tokio::test]
    async fn parquet_task_lookup_rejects_ambiguous_legacy_task_key() -> Result<()> {
        let lookup = lookup_with_rows(vec![
            task_row("run-1", "extract"),
            task_row("run-2", "extract"),
        ]);

        let err = lookup
            .get_task_state("extract")
            .await
            .expect_err("ambiguous legacy task key");

        assert!(err.contains("task_id_ambiguous"));
        Ok(())
    }

    #[tokio::test]
    async fn task_callback_response_maps_ambiguous_legacy_task_id_to_bad_request() -> Result<()> {
        let response = callback_result_response(CallbackResult::<TaskStartedResponse>::BadRequest(
            CallbackError::task_id_ambiguous("extract"),
        ))
        .expect("callback response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 1024).await?;
        let payload: Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["error"], "task_id_ambiguous");
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|message| message.contains("opaque taskId"))
        );
        Ok(())
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

    fn test_task_token_config() -> TaskTokenConfig {
        TaskTokenConfig {
            hs256_secret: "test-secret".to_string(),
            issuer: None,
            audience: None,
            ttl_seconds: 900,
        }
    }

    fn encode_task_token_claims(claims: Value) -> String {
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token")
    }

    fn task_token_claims(task_id: &str, tenant_id: &str, workspace_id: &str) -> Value {
        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        serde_json::json!({
            "taskId": task_id,
            "tenantId": tenant_id,
            "workspaceId": workspace_id,
            "exp": exp
        })
    }

    #[tokio::test]
    async fn test_parquet_task_lookup_uses_run_id_scope_for_repeated_task_keys() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-a".to_string(),
            test_run_row("run-a", FoldRunState::Succeeded),
        );
        state.runs.insert(
            "run-b".to_string(),
            test_run_row("run-b", FoldRunState::Running),
        );
        state.tasks.insert(
            ("run-a".to_string(), "extract".to_string()),
            test_task_row("run-a", "extract", FoldTaskState::Succeeded, 1),
        );
        state.tasks.insert(
            ("run-b".to_string(), "extract".to_string()),
            test_task_row("run-b", "extract", FoldTaskState::Running, 2),
        );

        let unscoped_lookup = ParquetTaskStateLookup {
            state: Arc::new(state.clone()),
            run_id_scope: None,
        };
        let ambiguous = unscoped_lookup
            .get_task_state("extract")
            .await
            .expect_err("legacy unscoped repeated task keys remain ambiguous");
        assert_eq!(ambiguous, "task_id_ambiguous: extract");

        let scoped_lookup = ParquetTaskStateLookup {
            state: Arc::new(state),
            run_id_scope: Some("run-b".to_string()),
        };
        let task = scoped_lookup
            .get_task_state("extract")
            .await
            .expect("lookup")
            .expect("scoped task");

        assert_eq!(task.run_id, "run-b");
        assert_eq!(task.state, "RUNNING");
        assert_eq!(task.attempt, 2);
        assert_eq!(task.attempt_id, "attempt-run-b-extract-2");
    }

    #[test]
    fn test_jwt_task_token_validator_accepts_valid_token() {
        let config = TaskTokenConfig {
            hs256_secret: "test-secret".to_string(),
            issuer: None,
            audience: None,
            ttl_seconds: 900,
        };

        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "exp": exp
        });
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-1", 1, &token));
        assert!(result.is_ok());
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_task_id_mismatch() {
        let config = TaskTokenConfig {
            hs256_secret: "test-secret".to_string(),
            issuer: None,
            audience: None,
            ttl_seconds: 900,
        };

        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");

        let exp = (Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let claims = serde_json::json!({
            "taskId": "task-123",
            "tenantId": "tenant-1",
            "workspaceId": "workspace-1",
            "exp": exp
        });
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .expect("token");

        let result =
            tokio_test::block_on(validator.validate_task_token("task-999", "run-1", 1, &token));
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_run_id_mismatch() {
        let config = test_task_token_config();
        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");

        let mut claims = task_token_claims("task-123", "tenant-1", "workspace-1");
        claims["runId"] = serde_json::json!("run-1");
        claims["attempt"] = serde_json::json!(1);
        let token = encode_task_token_claims(claims);

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-2", 1, &token));

        assert_eq!(result.expect_err("must reject"), "run_id_mismatch");
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_attempt_mismatch() {
        let config = test_task_token_config();
        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");

        let mut claims = task_token_claims("task-123", "tenant-1", "workspace-1");
        claims["runId"] = serde_json::json!("run-1");
        claims["attempt"] = serde_json::json!(2);
        let token = encode_task_token_claims(claims);

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-1", 1, &token));

        assert_eq!(result.expect_err("must reject"), "attempt_mismatch");
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_tenant_mismatch() {
        let config = test_task_token_config();
        let validator = JwtTaskTokenValidator::new(&config, "tenant-2", "workspace-1", false)
            .expect("validator");
        let token =
            encode_task_token_claims(task_token_claims("task-123", "tenant-1", "workspace-1"));

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-1", 1, &token));

        assert_eq!(result.expect_err("must reject"), "tenant_mismatch");
    }

    #[test]
    fn test_jwt_task_token_validator_rejects_workspace_mismatch() {
        let config = test_task_token_config();
        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-2", false)
            .expect("validator");
        let token =
            encode_task_token_claims(task_token_claims("task-123", "tenant-1", "workspace-1"));

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-1", 1, &token));

        assert_eq!(result.expect_err("must reject"), "workspace_mismatch");
    }

    #[test]
    fn test_jwt_task_token_validator_accepts_legacy_token_without_scope_claims() {
        let config = test_task_token_config();
        let validator = JwtTaskTokenValidator::new(&config, "tenant-1", "workspace-1", false)
            .expect("validator");
        let token =
            encode_task_token_claims(task_token_claims("task-123", "tenant-1", "workspace-1"));

        let result =
            tokio_test::block_on(validator.validate_task_token("task-123", "run-2", 9, &token));

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_task_auth_middleware_rejects_missing_token() -> Result<()> {
        let mut config = crate::config::Config::default();
        config.debug = false;
        config.task_token.hs256_secret = "test-secret".to_string();
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
        config.task_token.hs256_secret = "test-secret".to_string();
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
                tenant_id: "tenant-1".to_string(),
                workspace_id: "workspace-1".to_string(),
                run_id: None,
                attempt: None,
                exp: 2_000_000_000,
                nbf: None,
                iat: None,
                iss: None,
                aud: None,
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
        let mut config = crate::config::Config::default();
        config.debug = true;
        config.posture = Posture::Dev;
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
        let mut config = crate::config::Config::default();
        config.debug = true;
        config.posture = Posture::Private;
        config.task_token.hs256_secret = "test-secret".to_string();
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

    fn test_run_row(run_id: &str, state: FoldRunState) -> RunRow {
        let terminal = state.is_terminal();
        RunRow {
            run_id: run_id.to_string(),
            plan_id: format!("plan-{run_id}"),
            state,
            run_key: None,
            labels: HashMap::new(),
            code_version: Some("v1".to_string()),
            cancel_requested: false,
            tasks_total: 1,
            tasks_completed: u32::from(terminal),
            tasks_succeeded: u32::from(state == FoldRunState::Succeeded),
            tasks_failed: u32::from(state == FoldRunState::Failed),
            tasks_skipped: 0,
            tasks_cancelled: u32::from(state == FoldRunState::Cancelled),
            triggered_at: Utc::now(),
            completed_at: terminal.then(Utc::now),
            row_version: format!("row-{run_id}"),
        }
    }

    fn test_task_row(run_id: &str, task_key: &str, state: FoldTaskState, attempt: u32) -> TaskRow {
        let terminal = state.is_terminal();
        TaskRow {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            state,
            attempt,
            attempt_id: Some(format!("attempt-{run_id}-{task_key}-{attempt}")),
            started_at: Some(Utc::now()),
            completed_at: terminal.then(Utc::now),
            error_message: None,
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: Some(Utc::now()),
            ready_at: Some(Utc::now()),
            asset_key: Some(format!("asset.{task_key}")),
            partition_key: None,
            requires_visible_output: false,
            materialization_id: None,
            output_visibility_state: None,
            published_at: None,
            publish_error: None,
            retry_not_before: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            execution_lineage_ref: None,
            row_version: format!("row-{run_id}-{task_key}"),
        }
    }
}
