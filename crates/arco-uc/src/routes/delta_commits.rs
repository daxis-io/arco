//! Delta commit coordinator endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::routing::get;

use arco_core::storage::WritePrecondition;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::error::UnityCatalogResult;
use crate::state::UnityCatalogState;

#[derive(Debug, Deserialize, ToSchema)]
/// Request payload for `GET/POST /delta/preview/commits`.
pub struct DeltaPreviewCommitsRequest {
    /// Target table identifier (UUID string).
    pub table_id: String,
    /// Target table URI (opaque storage location).
    pub table_uri: String,
    /// Starting version for listing commits.
    pub start_version: Option<i64>,
    /// Commit metadata to register (Mode B preview surface).
    pub commit_info: Option<DeltaCommitInfo>,
    /// Latest version that has been backfilled into the Delta log.
    pub latest_backfilled_version: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Metadata about a coordinated Delta commit.
pub struct DeltaCommitInfo {
    /// Delta log version.
    pub version: i64,
    /// Commit timestamp (milliseconds since epoch).
    pub timestamp: i64,
    /// Commit file name (e.g. `00000000000000000000.json`).
    pub file_name: String,
    /// Commit file size in bytes.
    pub file_size: i64,
    /// Commit file modification timestamp (milliseconds since epoch).
    pub file_modification_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaPreviewCommitState {
    latest_table_version: i64,
    latest_backfilled_version: Option<i64>,
    commits: Vec<DeltaCommitInfo>,
}

/// Delta commit route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route(
        "/delta/preview/commits",
        get(list_unbackfilled_commits).post(register_commit),
    )
}

fn delta_preview_state_path(table_id: Uuid) -> String {
    format!("uc/delta_preview/{table_id}.json")
}

async fn load_state(
    storage: &arco_core::ScopedStorage,
    table_id: Uuid,
) -> UnityCatalogResult<Option<DeltaPreviewCommitState>> {
    let path = delta_preview_state_path(table_id);
    let meta = storage
        .head_raw(&path)
        .await
        .map_err(|err| UnityCatalogError::Internal {
            message: err.to_string(),
        })?;
    if meta.is_none() {
        return Ok(None);
    }

    let bytes = storage
        .get_raw(&path)
        .await
        .map_err(|err| UnityCatalogError::Internal {
            message: err.to_string(),
        })?;
    let state: DeltaPreviewCommitState =
        serde_json::from_slice(&bytes).map_err(|err| UnityCatalogError::Internal {
            message: err.to_string(),
        })?;
    Ok(Some(state))
}

async fn store_state(
    storage: &arco_core::ScopedStorage,
    table_id: Uuid,
    state: &DeltaPreviewCommitState,
) -> UnityCatalogResult<()> {
    let path = delta_preview_state_path(table_id);
    let bytes = serde_json::to_vec(state).map_err(|err| UnityCatalogError::Internal {
        message: err.to_string(),
    })?;
    storage
        .put_raw(&path, Bytes::from(bytes), WritePrecondition::None)
        .await
        .map_err(|err| UnityCatalogError::Internal {
            message: err.to_string(),
        })?;
    Ok(())
}

/// `GET /delta/preview/commits` (Scope A).
///
/// # Errors
///
/// Returns an error if the request is invalid, the table has no preview state,
/// or scoped storage operations fail.
#[utoipa::path(
    get,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    responses(
        (status = 200, description = "List unbackfilled commits"),
        (status = 400, description = "Bad request", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found", body = UnityCatalogErrorResponse),
    )
)]
pub async fn list_unbackfilled_commits(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<DeltaPreviewCommitsRequest>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let table_id =
        Uuid::parse_str(&request.table_id).map_err(|_| UnityCatalogError::BadRequest {
            message: "table_id must be a UUID".to_string(),
        })?;

    let storage = ctx.scoped_storage(state.storage.clone())?;
    let Some(state) = load_state(&storage, table_id).await? else {
        return Err(UnityCatalogError::NotFound {
            message: format!("table not found: {}", request.table_id),
        });
    };

    let start_version = request.start_version.unwrap_or(0);
    let commits: Vec<DeltaCommitInfo> = state
        .commits
        .iter()
        .filter(|commit| commit.version >= start_version)
        .cloned()
        .collect();

    let payload = json!({
        "latest_table_version": state.latest_table_version,
        "commits": commits
    });

    Ok((StatusCode::OK, Json(payload)))
}

/// `POST /delta/preview/commits` (Scope A).
///
/// # Errors
///
/// Returns an error if the request is invalid, preview state cannot be read or
/// written, or the referenced table does not exist.
#[utoipa::path(
    post,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    responses(
        (status = 200, description = "Register commit or backfill"),
        (status = 400, description = "Bad request", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found", body = UnityCatalogErrorResponse),
    )
)]
pub async fn register_commit(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<DeltaPreviewCommitsRequest>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let table_id =
        Uuid::parse_str(&request.table_id).map_err(|_| UnityCatalogError::BadRequest {
            message: "table_id must be a UUID".to_string(),
        })?;
    let storage = ctx.scoped_storage(state.storage.clone())?;

    if let Some(commit) = request.commit_info {
        let mut state = load_state(&storage, table_id)
            .await?
            .unwrap_or(DeltaPreviewCommitState {
                latest_table_version: -1,
                latest_backfilled_version: None,
                commits: Vec::new(),
            });

        state.latest_table_version = state.latest_table_version.max(commit.version);
        state.commits.push(commit);

        store_state(&storage, table_id, &state).await?;

        return Ok((StatusCode::OK, Json(json!({}))));
    }

    if let Some(backfilled) = request.latest_backfilled_version {
        let mut state =
            load_state(&storage, table_id)
                .await?
                .ok_or_else(|| UnityCatalogError::NotFound {
                    message: format!("table not found: {}", request.table_id),
                })?;

        state.latest_backfilled_version = Some(backfilled);
        state.commits.retain(|commit| commit.version > backfilled);
        store_state(&storage, table_id, &state).await?;

        return Ok((StatusCode::OK, Json(json!({}))));
    }

    Err(UnityCatalogError::BadRequest {
        message: "request must include either commit_info or latest_backfilled_version".to_string(),
    })
}
