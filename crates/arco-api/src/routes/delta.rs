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

use arco_catalog::authz::decision::{AuthzDecision, AuthzRequest, DecisionOutcome};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::managed_delta::{self, ManagedDeltaAllocation};
use arco_catalog::{CatalogAuthority, WriteOptions};
use arco_core::{DeltaPaths, ScopedStorage, TableFormat};
use arco_uc::is_event_stream_table;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::routes::catalog_authority;
use crate::server::AppState;

const MAX_DELTA_STAGE_BYTES: usize = 5 * 1024 * 1024;

#[cfg(test)]
mod probe_tests;

/// Creates Delta routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/delta/tables/:table_id/commits/stage",
            post(stage_commit_payload),
        )
        .route("/delta/tables/:table_id/commits", post(commit_staged))
        .layer(DefaultBodyLimit::max(MAX_DELTA_STAGE_BYTES))
        .route(
            "/managed-delta/ensure",
            post(ensure_managed_delta).layer(DefaultBodyLimit::max(4096)),
        )
        .route(
            "/managed-delta/storage",
            post(probe_managed_delta_storage).layer(DefaultBodyLimit::max(4096)),
        )
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedDeltaStorageProbeRequest {
    /// Original `UUIDv7` operation identity.
    operation_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ManagedDeltaStorageProbeResponse {
    /// Passed after the scoped write, metadata read, payload read, and cleanup.
    state: &'static str,
}

#[utoipa::path(
    post,
    path = "/api/v1/managed-delta/storage",
    tag = "Delta",
    request_body = ManagedDeltaStorageProbeRequest,
    responses(
        (status = 200, description = "Analytical storage access verified", body = ManagedDeltaStorageProbeResponse),
        (status = 400, description = "Invalid operation identity", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid authentication", body = ApiErrorBody),
        (status = 403, description = "Allocation privilege or managed root denied", body = ApiErrorBody),
        (status = 500, description = "Storage access or integrity check failed", body = ApiErrorBody),
        (status = 503, description = "Storage unavailable or probe contention exhausted retries", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub(crate) async fn probe_managed_delta_storage(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ManagedDeltaStorageProbeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    catalog_authority::reject_table_commit_for_control_v1(&state, &ctx)?;
    let operation_id = request
        .operation_id
        .parse::<Uuid>()
        .ok()
        .filter(|id| id.get_version_num() == 7)
        .ok_or_else(|| ApiError::bad_request("operation_id must be a UUIDv7"))?;
    require_managed_delta_create_table(&state, &ctx).await?;
    let root = state.managed_delta_storage.as_ref().ok_or_else(|| {
        ApiError::from_status_and_message(503, "managed_delta_storage_unavailable")
    })?;
    let catalog = ctx.scoped_storage(state.storage_backend()?)?;
    if !root.probe(&catalog, operation_id).await? {
        return Err(ApiError::managed_storage_probe_busy());
    }
    Ok((
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(ManagedDeltaStorageProbeResponse { state: "passed" }),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/managed-delta/ensure",
    tag = "Delta",
    request_body(content = Object, description = "UUIDv7 operation_id, authenticated owner, and provisioning_fingerprint"),
    responses(
        (status = 200, description = "Allocated managed Delta fixture or identical replay"),
        (status = 400, description = "Invalid allocation intent"),
        (status = 401, description = "Missing or invalid authentication"),
        (status = 403, description = "Allocation privilege or managed root denied"),
        (status = 409, description = "Intent or table incarnation conflict"),
        (status = 503, description = "Approved analytical storage or governance unavailable"),
    ),
    security(("bearerAuth" = []))
)]
pub(crate) async fn ensure_managed_delta(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(intent): Json<ManagedDeltaAllocation>,
) -> Result<impl IntoResponse, ApiError> {
    catalog_authority::reject_table_commit_for_control_v1(&state, &ctx)?;
    let root = state.managed_delta_storage.as_ref().ok_or_else(|| {
        ApiError::from_status_and_message(503, "managed_delta_storage_unavailable")
    })?;
    if ctx.user_id.as_deref() != Some(&intent.owner) {
        return Err(ApiError::forbidden(
            "managed_delta_owner_must_match_principal",
        ));
    }
    require_managed_delta_create_table(&state, &ctx).await?;
    let CatalogAuthority::Legacy { writer, .. } = catalog_authority::resolve(&state, &ctx).await?
    else {
        return Err(ApiError::catalog_table_commit_disabled());
    };
    let placement = writer
        .allocate_managed_delta(
            root,
            &intent,
            WriteOptions::default()
                .with_actor(&intent.owner)
                .with_request_id(&ctx.request_id),
        )
        .await?;
    require_managed_delta_create_table(&state, &ctx).await?;
    Ok((
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "id": placement.table_id,
            "catalog": managed_delta::CATALOG,
            "schema": managed_delta::SCHEMA,
            "table": managed_delta::TABLE,
            "location": placement.location,
            "table_type": "MANAGED",
            "format": "delta",
            "schema_digest": placement.schema_digest,
            "properties": {
                "table_type": "MANAGED",
                "format": "delta",
                "owner": placement.allocation.owner,
                "schema_digest": placement.schema_digest,
                "provisioning_fingerprint": placement.allocation.provisioning_fingerprint
            }
        })),
    ))
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

/// Stage a Delta commit payload with current table MODIFY.
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
        (status = 403, description = "Delta write permission denied", body = ApiErrorBody),
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
    catalog_authority::reject_table_commit_for_control_v1(&state, &ctx)?;
    let table_id =
        Uuid::parse_str(&table_id).map_err(|_| ApiError::bad_request("table_id must be a UUID"))?;
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    reject_event_stream_target(&state, &ctx, &storage, table_id).await?;
    let authority = catalog_authority::resolve_read(&state, &ctx)?;
    let table = resolve_delta_table(&authority, table_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("table not found: {table_id}")))?;
    require_delta_modify(&state, &ctx, table_id).await?;

    let coordinator = bound_coordinator(&state, &ctx, storage, &table, table_id).await?;
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

/// Commit a staged Delta payload with current table MODIFY.
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
        (status = 403, description = "Delta write permission denied", body = ApiErrorBody),
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
    catalog_authority::reject_table_commit_for_control_v1(&state, &ctx)?;
    let table_id =
        Uuid::parse_str(&table_id).map_err(|_| ApiError::bad_request("table_id must be a UUID"))?;
    let Some(idempotency_key) = ctx.idempotency_key.clone() else {
        return Err(ApiError::bad_request("Idempotency-Key is required"));
    };

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    reject_event_stream_target(&state, &ctx, &storage, table_id).await?;
    let commit_request = arco_delta::CommitDeltaRequest {
        read_version: req.read_version,
        staged_path: req.staged_path,
        staged_version: req.staged_version,
        idempotency_key,
    };

    let authority = catalog_authority::resolve_read(&state, &ctx)?;
    let committed = if let Some(table) = resolve_delta_table(&authority, table_id).await? {
        require_delta_modify(&state, &ctx, table_id).await?;
        let coordinator = bound_coordinator(&state, &ctx, storage, &table, table_id).await?;
        if let Some(replayed) = coordinator.replay_committed(&commit_request).await? {
            replayed
        } else {
            coordinator.commit(commit_request, Utc::now()).await?
        }
    } else {
        let not_found = || ApiError::not_found(format!("table not found: {table_id}"));
        require_delta_modify(&state, &ctx, table_id)
            .await
            .map_err(|_| not_found())?;
        if managed_delta::has_placement(&storage, table_id).await? {
            return Err(not_found());
        }
        arco_delta::DeltaCommitCoordinator::new(storage, table_id)
            .replay_finalized_commit(&commit_request)
            .await?
            .ok_or_else(not_found)?
    };

    Ok((
        StatusCode::OK,
        Json(CommitResponse {
            version: committed.version,
            delta_log_path: committed.delta_log_path,
        }),
    ))
}

async fn require_delta_modify(
    state: &AppState,
    ctx: &RequestContext,
    table_id: Uuid,
) -> Result<(), ApiError> {
    require_catalog_privilege(
        state,
        ctx,
        &table_id.to_string(),
        "TABLE",
        Privilege::Modify,
        "delta_modify_required",
    )
    .await
}

async fn require_managed_delta_create_table(
    state: &AppState,
    ctx: &RequestContext,
) -> Result<(), ApiError> {
    let schema_id = format!("{}.{}", managed_delta::CATALOG, managed_delta::SCHEMA);
    require_catalog_privilege(
        state,
        ctx,
        &schema_id,
        "SCHEMA",
        Privilege::CreateTable,
        "managed_delta_create_table_required",
    )
    .await
}

async fn require_catalog_privilege(
    state: &AppState,
    ctx: &RequestContext,
    object_id: &str,
    object_type: &str,
    privilege: Privilege,
    reason: &str,
) -> Result<(), ApiError> {
    let denied = || ApiError::forbidden(reason);
    let principal = ctx.user_id.as_deref().ok_or_else(denied)?;
    let permissions = state
        .catalog_permissions
        .compiled_permissions(&ctx.tenant, &ctx.workspace)
        .await
        .map_err(|error| {
            tracing::warn!(
                internal_error = %error,
                request_id = %ctx.request_id,
                "Delta write permissions unavailable"
            );
            denied()
        })?;
    let request = AuthzRequest::new(principal, object_id, object_type, privilege)
        .with_request_id(&ctx.request_id);
    if AuthzDecision::evaluate(&request, &permissions).outcome == DecisionOutcome::Deny {
        return Err(denied());
    }
    Ok(())
}

async fn reject_event_stream_target(
    state: &AppState,
    ctx: &RequestContext,
    storage: &ScopedStorage,
    table_id: Uuid,
) -> Result<(), ApiError> {
    if state
        .config
        .usage_bindings
        .table_id(&ctx.tenant, &ctx.workspace)
        == Some(table_id)
        || arco_uc::is_usage_table(storage, table_id).await?
    {
        return Err(ApiError::forbidden("usage_table_requires_owner_api"));
    }
    let configured = state
        .config
        .event_tables_in_scope(&ctx.tenant, &ctx.workspace)
        .any(|bound| bound == table_id);
    if configured || is_event_stream_table(storage, table_id).await? {
        return Err(ApiError::forbidden("event_stream_table_requires_owner_api"));
    }
    Ok(())
}

async fn reject_stream_table_alias(
    state: &AppState,
    ctx: &RequestContext,
    storage: &ScopedStorage,
    paths: &DeltaPaths,
) -> Result<(), ApiError> {
    if let Some(table_id) = paths.canonical_root_table_id()
        && table_id != paths.table_id()
    {
        reject_event_stream_target(state, ctx, storage, table_id).await?;
    }
    Ok(())
}

async fn resolve_delta_table(
    authority: &CatalogAuthority,
    table_id: Uuid,
) -> Result<Option<arco_catalog::writer::Table>, ApiError> {
    let Some(table) = authority
        .get_table_by_id(&table_id.to_string())
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(None);
    };

    let format = TableFormat::effective(table.format.as_deref()).map_err(|err| {
        ApiError::internal(format!(
            "invalid persisted table format for {table_id}: {err}"
        ))
    })?;

    if format != TableFormat::Delta {
        return Err(ApiError::conflict(format!(
            "table {table_id} is format '{format}', expected 'delta'"
        )));
    }

    Ok(Some(table))
}

async fn bound_coordinator(
    state: &AppState,
    ctx: &RequestContext,
    storage: ScopedStorage,
    table: &arco_catalog::writer::Table,
    table_id: Uuid,
) -> Result<arco_delta::DeltaCommitCoordinator, ApiError> {
    let analytical = managed_delta::resolve_analytical_storage(
        state.managed_delta_storage.as_ref(),
        &storage,
        table,
    )
    .await?;
    let paths = if analytical.is_some() {
        DeltaPaths::legacy(table_id)
    } else {
        DeltaPaths::from_table_location(
            table_id,
            table.location.as_deref(),
            &ctx.tenant,
            &ctx.workspace,
        )?
    };
    reject_stream_table_alias(state, ctx, &storage, &paths).await?;
    let coordinator = arco_delta::DeltaCommitCoordinator::with_paths(storage, paths);
    Ok(if let Some(analytical) = analytical {
        coordinator.with_log_storage(analytical)
    } else {
        coordinator
    })
}

impl From<arco_delta::DeltaError> for ApiError {
    fn from(value: arco_delta::DeltaError) -> Self {
        match value {
            arco_delta::DeltaError::BadRequest { message } => Self::bad_request(message),
            arco_delta::DeltaError::Conflict { message } => Self::conflict(message),
            arco_delta::DeltaError::NotFound { message } => Self::not_found(message),
            arco_delta::DeltaError::Storage(e) => Self::from(e),
            arco_delta::DeltaError::Serialization { message } => Self::internal(message),
            error => Self::internal(error.to_string()),
        }
    }
}
