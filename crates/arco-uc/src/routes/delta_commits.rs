//! Delta commit coordination routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::Extension;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "DeltaGetCommitsRequestBody")]
#[serde(default)]
pub(crate) struct DeltaGetCommitsRequestBody {
    table_id: Option<String>,
    table_uri: Option<String>,
    start_version: Option<i64>,
    end_version: Option<i64>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "DeltaCommitRequestBody")]
#[serde(default)]
pub(crate) struct DeltaCommitRequestBody {
    table_id: Option<String>,
    table_uri: Option<String>,
    commit_info: Option<Value>,
    latest_backfilled_version: Option<i64>,
    metadata: Option<Value>,
    uniform: Option<Value>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

/// Lists unbackfilled Delta commits (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    get,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    request_body = DeltaGetCommitsRequestBody,
    responses(
        (status = 200, description = "Successful response.", body = Value),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_delta_preview_commits(
    _ctx: Extension<UnityCatalogRequestContext>,
) -> UnityCatalogResult<Json<Value>> {
    let preview_shape = DeltaGetCommitsRequestBody::default();
    let _ = (
        preview_shape.table_id,
        preview_shape.table_uri,
        preview_shape.start_version,
        preview_shape.end_version,
        preview_shape.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "GET /delta/preview/commits is scaffolded in preview; implementation is pending."
            .to_string(),
    })
}

/// Commits changes to a Delta table (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    request_body = DeltaCommitRequestBody,
    responses(
        (status = 200, description = "Successful response.", body = Value),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 429, description = "Too many requests.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_delta_preview_commits(
    _ctx: Extension<UnityCatalogRequestContext>,
    payload: Json<DeltaCommitRequestBody>,
) -> UnityCatalogResult<Json<Value>> {
    let Json(payload) = payload;
    let _ = (
        payload.table_id,
        payload.table_uri,
        payload.commit_info,
        payload.latest_backfilled_version,
        payload.metadata,
        payload.uniform,
        payload.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "POST /delta/preview/commits is scaffolded in preview; implementation is pending."
            .to_string(),
    })
}
