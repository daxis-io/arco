//! Delta Lake APIs (Arco-native).
//!
//! This module provides a minimal, Arco-native surface for coordinating Delta
//! commits for managed tables (Mode B). It is intentionally small and maps to
//! Arco's file-native control plane.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

const MAX_DELTA_STAGE_BYTES: usize = 5 * 1024 * 1024;

/// Creates Delta routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/delta/tables/:table_id/commits/stage",
            post(stage_commit_payload),
        )
        .route("/delta/tables/:table_id/commits", post(commit_staged))
        .layer(DefaultBodyLimit::max(MAX_DELTA_STAGE_BYTES))
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct StageCommitRequest {
    /// Delta commit payload to store (JSON lines).
    payload: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StageCommitResponse {
    staged_path: String,
    staged_version: String,
}

/// Stage a Delta commit payload.
///
/// POST `/api/v1/delta/tables/{table_id}/commits/stage`
#[utoipa::path(
    post,
    path = "/api/v1/delta/tables/{table_id}/commits/stage",
    tag = "delta",
    params(
        ("table_id" = String, Path, description = "Table ID (UUID)"),
    ),
    request_body = StageCommitRequest,
    responses(
        (status = 200, description = "Payload staged", body = StageCommitResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 409, description = "Conflict", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn stage_commit_payload(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(table_id): Path<String>,
    Json(req): Json<StageCommitRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let table_id =
        Uuid::parse_str(&table_id).map_err(|_| ApiError::bad_request("table_id must be a UUID"))?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let coordinator = arco_delta::DeltaCommitCoordinator::new(storage, table_id);
    let staged = coordinator
        .stage_commit_payload(Bytes::from(req.payload))
        .await
        .map_err(ApiError::from)?;

    Ok((
        StatusCode::OK,
        Json(StageCommitResponse {
            staged_path: staged.staged_path,
            staged_version: staged.staged_version,
        }),
    ))
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CommitRequest {
    /// Optimistic concurrency token: the version the client read.
    read_version: i64,
    /// Path returned by the stage endpoint.
    staged_path: String,
    /// Version returned by the stage endpoint.
    staged_version: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CommitResponse {
    version: i64,
    delta_log_path: String,
}

/// Commit a staged Delta payload.
///
/// POST `/api/v1/delta/tables/{table_id}/commits`
#[utoipa::path(
    post,
    path = "/api/v1/delta/tables/{table_id}/commits",
    tag = "delta",
    params(
        ("table_id" = String, Path, description = "Table ID (UUID)"),
        ("Idempotency-Key" = String, Header, description = "Idempotency key (UUIDv7)"),
    ),
    request_body = CommitRequest,
    responses(
        (status = 200, description = "Commit succeeded", body = CommitResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 409, description = "Conflict", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn commit_staged(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(table_id): Path<String>,
    Json(req): Json<CommitRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let table_id =
        Uuid::parse_str(&table_id).map_err(|_| ApiError::bad_request("table_id must be a UUID"))?;

    let Some(idempotency_key) = ctx.idempotency_key.clone() else {
        return Err(ApiError::bad_request("Idempotency-Key is required"));
    };

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let coordinator = arco_delta::DeltaCommitCoordinator::new(storage, table_id);

    let committed = coordinator
        .commit(
            arco_delta::CommitDeltaRequest {
                read_version: req.read_version,
                staged_path: req.staged_path,
                staged_version: req.staged_version,
                idempotency_key,
            },
            Utc::now(),
        )
        .await
        .map_err(ApiError::from)?;

    Ok((
        StatusCode::OK,
        Json(CommitResponse {
            version: committed.version,
            delta_log_path: committed.delta_log_path,
        }),
    ))
}

impl From<arco_delta::DeltaError> for ApiError {
    fn from(value: arco_delta::DeltaError) -> Self {
        match value {
            arco_delta::DeltaError::BadRequest { message } => Self::bad_request(message),
            arco_delta::DeltaError::Conflict { message } => Self::conflict(message),
            arco_delta::DeltaError::NotFound { message } => Self::not_found(message),
            arco_delta::DeltaError::Storage(e) => Self::from(e),
            arco_delta::DeltaError::Serialization { message } => Self::internal(message),
        }
    }
}
