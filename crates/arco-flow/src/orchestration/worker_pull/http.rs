use super::{AdvanceReply, ClaimReply, PullError, PullResult, WorkerPull, hmac};
use arco_worker_contract::{
    HeartbeatRequest, HeartbeatResponse, TaskCompletedRequest, TaskCompletedResponse,
    TaskStartedRequest, TaskStartedResponse,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::post,
};
use hmac::Mac;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClaimRequest {
    request_id: String,
}

impl WorkerPull {
    /// Serves the pull, worker callback, and manual control routes.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/run", post(advance))
            .route("/api/v1/tasks/claim", post(claim))
            .route("/api/v1/tasks/:task_id/started", post(started))
            .route("/api/v1/tasks/:task_id/heartbeat", post(heartbeat))
            .route("/api/v1/tasks/:task_id/completed", post(completed))
            .layer(DefaultBodyLimit::max(64 * 1024))
            .with_state(self.clone())
    }

    fn credential_matches(&self, expected: &str, actual: &str) -> bool {
        let mut verifier = hmac(&self.config.claim_key);
        verifier.update(b"arco/flow/worker-pull/session/v1");
        let mut signer = verifier.clone();
        signer.update(expected.as_bytes());
        verifier.update(actual.as_bytes());
        verifier
            .verify_slice(&signer.finalize().into_bytes())
            .is_ok()
    }
}

impl IntoResponse for PullError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Invalid(message) => (StatusCode::BAD_REQUEST, message),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid worker-pull credential"),
            Self::RunNotFound => (StatusCode::NOT_FOUND, "run not found"),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::Retryable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::Authority(error) => {
                tracing::warn!(%error, "worker-pull authority needs retry");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "worker-pull authority needs retry",
                )
            }
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

fn bearer(headers: &HeaderMap) -> PullResult<&str> {
    let (scheme, token) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .ok_or(PullError::Unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || token.len() > 128 {
        return Err(PullError::Unauthorized);
    }
    Ok(token)
}

async fn advance(
    State(host): State<WorkerPull>,
    headers: HeaderMap,
) -> PullResult<Json<AdvanceReply>> {
    if !host.credential_matches(&host.config.control_token, bearer(&headers)?) {
        return Err(PullError::Unauthorized);
    }
    host.advance().await.map(Json)
}

async fn claim(
    State(host): State<WorkerPull>,
    headers: HeaderMap,
    Json(body): Json<ClaimRequest>,
) -> PullResult<Json<ClaimReply>> {
    let token = bearer(&headers)?;
    let session = host
        .config
        .sessions
        .iter()
        .find_map(|(id, key)| host.credential_matches(key, token).then_some(id))
        .ok_or(PullError::Unauthorized)?;
    host.claim(session, &body.request_id).await.map(Json)
}

async fn started(
    State(host): State<WorkerPull>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<TaskStartedRequest>,
) -> PullResult<Json<TaskStartedResponse>> {
    host.started(&task_id, bearer(&headers)?, &body)
        .await
        .map(Json)
}

async fn heartbeat(
    State(host): State<WorkerPull>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<HeartbeatRequest>,
) -> PullResult<Json<HeartbeatResponse>> {
    host.heartbeat(&task_id, bearer(&headers)?, &body)
        .await
        .map(Json)
}

async fn completed(
    State(host): State<WorkerPull>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<TaskCompletedRequest>,
) -> PullResult<Json<TaskCompletedResponse>> {
    host.completed(&task_id, bearer(&headers)?, &body)
        .await
        .map(Json)
}
