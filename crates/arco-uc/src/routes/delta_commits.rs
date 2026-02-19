//! Delta commit coordination routes for the Unity Catalog facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::routing::get;

use arco_core::storage::WritePrecondition;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::state::UnityCatalogState;

/// Delta commit route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route(
        "/delta/preview/commits",
        get(get_delta_preview_commits).post(post_delta_preview_commits),
    )
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(title = "DeltaGetCommitsRequestBody")]
/// Request payload for `GET /delta/preview/commits`.
pub(crate) struct DeltaGetCommitsRequestBody {
    /// Target table identifier (UUID string).
    pub table_id: String,
    /// Target table URI (opaque storage location).
    pub table_uri: String,
    /// Starting version for listing commits.
    pub start_version: Option<i64>,
    /// Ending version for listing commits.
    pub end_version: Option<i64>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(title = "DeltaCommitRequestBody")]
/// Request payload for `POST /delta/preview/commits`.
pub(crate) struct DeltaCommitRequestBody {
    /// Target table identifier (UUID string).
    pub table_id: String,
    /// Target table URI (opaque storage location).
    pub table_uri: String,
    /// Commit metadata to register.
    pub commit_info: Option<DeltaCommitInfo>,
    /// Latest version that has been backfilled into the Delta log.
    pub latest_backfilled_version: Option<i64>,
    /// Optional metadata envelope.
    pub metadata: Option<Value>,
    /// Optional uniform settings envelope.
    pub uniform: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
/// Metadata about a coordinated Delta commit.
pub(crate) struct DeltaCommitInfo {
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

fn delta_preview_state_path(table_id: Uuid) -> String {
    format!("uc/delta_preview/{table_id}.json")
}

fn validate_table_uri(table_uri: &str) -> UnityCatalogResult<()> {
    if table_uri.trim().is_empty() {
        return Err(UnityCatalogError::BadRequest {
            message: "table_uri must not be empty".to_string(),
        });
    }
    Ok(())
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

/// Lists unbackfilled Delta commits.
///
/// # Errors
///
/// Returns an error if the request is invalid, the table has no preview state,
/// or scoped storage operations fail.
#[utoipa::path(
    get,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    request_body = DeltaGetCommitsRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_delta_preview_commits(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<DeltaGetCommitsRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let DeltaGetCommitsRequestBody {
        table_id,
        table_uri,
        start_version,
        end_version,
    } = request;

    validate_table_uri(&table_uri)?;
    let table_id = Uuid::parse_str(&table_id).map_err(|_| UnityCatalogError::BadRequest {
        message: "table_id must be a UUID".to_string(),
    })?;

    let storage = ctx.scoped_storage(state.storage.clone())?;
    let Some(state) = load_state(&storage, table_id).await? else {
        return Err(UnityCatalogError::NotFound {
            message: format!("table not found: {table_id}"),
        });
    };

    let start_version = start_version.unwrap_or(0);
    let commits: Vec<DeltaCommitInfo> = state
        .commits
        .iter()
        .filter(|commit| {
            commit.version >= start_version
                && end_version.is_none_or(|version| commit.version <= version)
        })
        .cloned()
        .collect();

    let payload = json!({
        "latest_table_version": state.latest_table_version,
        "commits": commits
    });

    Ok((StatusCode::OK, Json(payload)))
}

/// Commits changes to a Delta table.
///
/// # Errors
///
/// Returns an error if the request is invalid, preview state cannot be read or
/// written, or the referenced table does not exist.
#[utoipa::path(
    post,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    request_body = DeltaCommitRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 429, description = "Too many requests.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
        (status = 501, description = "Not implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_delta_preview_commits(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<DeltaCommitRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let DeltaCommitRequestBody {
        table_id,
        table_uri,
        commit_info,
        latest_backfilled_version,
        metadata,
        uniform,
    } = request;
    let _ = (metadata, uniform);

    validate_table_uri(&table_uri)?;
    let table_id = Uuid::parse_str(&table_id).map_err(|_| UnityCatalogError::BadRequest {
        message: "table_id must be a UUID".to_string(),
    })?;
    let storage = ctx.scoped_storage(state.storage.clone())?;

    if let Some(commit) = commit_info {
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

    if let Some(backfilled) = latest_backfilled_version {
        let mut state =
            load_state(&storage, table_id)
                .await?
                .ok_or_else(|| UnityCatalogError::NotFound {
                    message: format!("table not found: {table_id}"),
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
