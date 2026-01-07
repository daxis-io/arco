//! Catalog-level endpoint handlers for Iceberg REST Catalog.
//!
//! These endpoints operate at the catalog/prefix level rather than being
//! scoped to a specific namespace or table:
//!
//! - `POST /v1/{prefix}/tables/rename` - Rename a table (ICE-6)
//! - `POST /v1/{prefix}/transactions/commit` - Multi-table commit (ICE-7)

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::Deserialize;
use tracing::instrument;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogReader, CatalogWriter};

use crate::audit::{
    REASON_COMMIT_CACHED_FAILURE, REASON_COMMIT_CAS_CONFLICT, REASON_COMMIT_IN_PROGRESS,
    REASON_COMMIT_INTERNAL_ERROR, REASON_COMMIT_MULTI_TABLE, REASON_COMMIT_REQUIREMENTS_FAILED,
    emit_iceberg_commit, emit_iceberg_commit_deny,
};
use crate::commit::{CommitError, CommitService};
use crate::context::IcebergRequestContext;
use crate::error::{IcebergError, IcebergResult};
use crate::idempotency::{IdempotencyMarker, canonical_request_hash};
use crate::pointer::UpdateSource;
use crate::routes::utils::{ensure_prefix, is_iceberg_table, join_namespace};
use crate::state::{IcebergConfig, IcebergState};
use crate::types::{CommitTransactionRequest, RenameTableRequest};

/// Creates catalog-level routes (rename, transactions).
pub fn routes() -> Router<IcebergState> {
    Router::new()
        .route("/tables/rename", axum::routing::post(rename_table))
        .route(
            "/transactions/commit",
            axum::routing::post(commit_transaction),
        )
}

fn ensure_table_crud_enabled(config: &IcebergConfig) -> IcebergResult<()> {
    if !config.allow_table_crud {
        return Err(IcebergError::unsupported_operation(
            "Table mutations are not enabled",
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct PrefixPath {
    prefix: String,
}

#[utoipa::path(
    post,
    path = "/v1/{prefix}/tables/rename",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    request_body = RenameTableRequest,
    responses(
        (status = 204, description = "Table renamed"),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Table or namespace not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation (cross-namespace rename)", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Destination table already exists", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Catalog"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix))]
async fn rename_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<PrefixPath>,
    Json(req): Json<RenameTableRequest>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_table_crud_enabled(&state.config)?;

    if req.source.namespace != req.destination.namespace {
        return Err(IcebergError::unsupported_operation(
            "Cross-namespace table rename is not supported",
        ));
    }

    let separator = state.config.namespace_separator_decoded();
    let source_namespace = join_namespace(&req.source.namespace, &separator)?;
    let dest_namespace = join_namespace(&req.destination.namespace, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let compactor = state.create_compactor(&storage)?;
    let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);

    let mut options = WriteOptions::default()
        .with_actor(format!("iceberg-api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(ref key) = ctx.idempotency_key {
        options = options.with_idempotency_key(key);
    }

    writer
        .rename_table(
            &source_namespace,
            &req.source.name,
            &dest_namespace,
            &req.destination.name,
            options,
        )
        .await
        .map_err(IcebergError::from)?;

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(axum::body::Body::empty())
        .map_err(|e| IcebergError::Internal {
            message: format!("Failed to build response: {e}"),
        })
}

#[utoipa::path(
    post,
    path = "/v1/{prefix}/transactions/commit",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    request_body = CommitTransactionRequest,
    responses(
        (status = 204, description = "Transaction committed"),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Table not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation (multi-table transactions)", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Commit conflict", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
        (status = 502, description = "Bad gateway", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 504, description = "Gateway timeout", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Catalog"
)]
#[allow(clippy::too_many_lines)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix))]
async fn commit_transaction(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<PrefixPath>,
    headers: HeaderMap,
    Json(req): Json<CommitTransactionRequest>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;

    if !state.config.allow_write {
        return Err(IcebergError::BadRequest {
            message: "Write operations are not enabled".to_string(),
            error_type: "NotImplementedException",
        });
    }

    if req.table_changes.is_empty() {
        return Err(IcebergError::BadRequest {
            message: "table-changes must contain exactly one entry".to_string(),
            error_type: "BadRequestException",
        });
    }

    if req.table_changes.len() > 1 {
        if let Some(emitter) = state.audit() {
            let separator = state.config.namespace_separator_decoded();
            for change in &req.table_changes {
                if let Some(ident) = &change.identifier {
                    let ns = join_namespace(&ident.namespace, &separator)?;
                    emit_iceberg_commit_deny(
                        emitter,
                        &ctx.tenant,
                        &ctx.workspace,
                        &ctx.request_id,
                        &ns,
                        &ident.name,
                        REASON_COMMIT_MULTI_TABLE,
                    );
                }
            }
        }
        return Err(IcebergError::unsupported_operation(
            "Multi-table transactions are not supported. Arco only supports single-table commits through this endpoint.",
        ));
    }

    let Some(commit_req) = req.table_changes.into_iter().next() else {
        unreachable!("length already validated as 1");
    };

    let identifier = commit_req
        .identifier
        .clone()
        .ok_or_else(|| IcebergError::BadRequest {
            message: "Table identifier is required in table-changes for transactions/commit"
                .to_string(),
            error_type: "BadRequestException",
        })?;

    let request_value = serde_json::to_value(&commit_req).map_err(|e| IcebergError::Internal {
        message: format!("Failed to serialize commit request: {e}"),
    })?;
    let request_hash =
        canonical_request_hash(&request_value).map_err(|err| IcebergError::BadRequest {
            message: format!("Invalid commit request: {err}"),
            error_type: "BadRequestException",
        })?;

    if let Some(reason) = commit_req.check_guardrails() {
        return Err(IcebergError::BadRequest {
            message: reason,
            error_type: "BadRequestException",
        });
    }

    let idempotency_key = ctx
        .idempotency_key
        .clone()
        .ok_or_else(|| IcebergError::BadRequest {
            message: "Missing Idempotency-Key header".to_string(),
            error_type: "BadRequestException",
        })?;

    IdempotencyMarker::validate_uuidv7(&idempotency_key)
        .map_err(|err| IcebergError::invalid_idempotency_key(err.to_string()))?;

    let separator = state.config.namespace_separator_decoded();
    let namespace_name = join_namespace(&identifier.namespace, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage.clone());

    let table = reader
        .get_table(&namespace_name, &identifier.name)
        .await
        .map_err(IcebergError::from)?
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .ok_or_else(|| IcebergError::table_not_found(&namespace_name, &identifier.name))?;

    let table_uuid = uuid::Uuid::parse_str(&table.id).map_err(|_| IcebergError::Internal {
        message: format!("Invalid table UUID: {}", table.id),
    })?;

    let client_info = headers
        .get(http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let source = UpdateSource::IcebergRest {
        client_info,
        principal: None,
    };

    let commit_service = CommitService::new(Arc::new(storage));

    let result = commit_service
        .commit_table(
            table_uuid,
            &namespace_name,
            &identifier.name,
            commit_req,
            request_hash,
            idempotency_key,
            source,
            &ctx.tenant,
            &ctx.workspace,
        )
        .await;

    match result {
        Ok(response) => {
            if let Some(emitter) = state.audit() {
                let seq = response
                    .metadata
                    .get("last-sequence-number")
                    .and_then(serde_json::Value::as_i64);
                emit_iceberg_commit(
                    emitter,
                    &ctx.tenant,
                    &ctx.workspace,
                    &ctx.request_id,
                    &namespace_name,
                    &identifier.name,
                    seq,
                );
            }
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(axum::body::Body::empty())
                .map_err(|e| IcebergError::Internal {
                    message: format!("Failed to build response: {e}"),
                })
        }
        Err(CommitError::Iceberg(err)) => {
            if let Some(emitter) = state.audit() {
                let reason = match &err {
                    IcebergError::Conflict { .. } => REASON_COMMIT_CAS_CONFLICT,
                    IcebergError::BadRequest { .. } => REASON_COMMIT_REQUIREMENTS_FAILED,
                    _ => REASON_COMMIT_INTERNAL_ERROR,
                };
                emit_iceberg_commit_deny(
                    emitter,
                    &ctx.tenant,
                    &ctx.workspace,
                    &ctx.request_id,
                    &namespace_name,
                    &identifier.name,
                    reason,
                );
            }
            Err(err)
        }
        Err(CommitError::CachedFailure { status, payload }) => {
            if let Some(emitter) = state.audit() {
                let reason = match status {
                    409 => REASON_COMMIT_CAS_CONFLICT,
                    400 => REASON_COMMIT_REQUIREMENTS_FAILED,
                    _ => REASON_COMMIT_CACHED_FAILURE,
                };
                emit_iceberg_commit_deny(
                    emitter,
                    &ctx.tenant,
                    &ctx.workspace,
                    &ctx.request_id,
                    &namespace_name,
                    &identifier.name,
                    reason,
                );
            }
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::CONFLICT);
            Ok((status, Json(payload)).into_response())
        }
        Err(CommitError::RetryAfter { seconds }) => {
            if let Some(emitter) = state.audit() {
                emit_iceberg_commit_deny(
                    emitter,
                    &ctx.tenant,
                    &ctx.workspace,
                    &ctx.request_id,
                    &namespace_name,
                    &identifier.name,
                    REASON_COMMIT_IN_PROGRESS,
                );
            }
            Err(IcebergError::ServiceUnavailable {
                message: "Commit already in progress".to_string(),
                retry_after_seconds: Some(seconds),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::IcebergConfig;
    use arco_catalog::Tier1Compactor;
    use arco_catalog::write_options::WriteOptions;
    use arco_core::ScopedStorage;
    use arco_core::storage::MemoryBackend;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn build_state_with_table_crud_enabled() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            allow_table_crud: true,
            ..Default::default()
        };
        IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(crate::state::Tier1CompactorFactory))
    }

    async fn seed_table(state: &IcebergState, namespace: &str, table: &str) {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace(namespace, None, WriteOptions::default())
            .await
            .expect("create namespace");
        writer
            .register_table(
                arco_catalog::RegisterTableRequest {
                    namespace: namespace.to_string(),
                    name: table.to_string(),
                    description: None,
                    location: Some("tenant=acme/workspace=analytics/warehouse/table".to_string()),
                    format: Some("iceberg".to_string()),
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");
    }

    fn app(state: IcebergState) -> Router {
        Router::new()
            .nest("/v1/:prefix", routes())
            .layer(axum::middleware::from_fn(
                crate::context::context_middleware,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_rename_table_returns_204() {
        let state = build_state_with_table_crud_enabled();
        seed_table(&state, "sales", "orders").await;

        let req_body = serde_json::json!({
            "source": {"namespace": ["sales"], "name": "orders"},
            "destination": {"namespace": ["sales"], "name": "renamed_orders"}
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/tables/rename")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_rename_table_cross_namespace_returns_406() {
        let state = build_state_with_table_crud_enabled();
        seed_table(&state, "sales", "orders").await;

        let req_body = serde_json::json!({
            "source": {"namespace": ["sales"], "name": "orders"},
            "destination": {"namespace": ["marketing"], "name": "orders"}
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/tables/rename")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn test_rename_table_not_found_returns_404() {
        let state = build_state_with_table_crud_enabled();
        seed_table(&state, "sales", "orders").await;

        let req_body = serde_json::json!({
            "source": {"namespace": ["sales"], "name": "nonexistent"},
            "destination": {"namespace": ["sales"], "name": "renamed"}
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/tables/rename")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_rename_table_disabled_returns_406() {
        let storage = Arc::new(MemoryBackend::new());
        let state = IcebergState::new(storage);

        let req_body = serde_json::json!({
            "source": {"namespace": ["sales"], "name": "orders"},
            "destination": {"namespace": ["sales"], "name": "renamed"}
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/tables/rename")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    fn build_state_with_write_enabled() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            allow_write: true,
            ..Default::default()
        };
        IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(crate::state::Tier1CompactorFactory))
    }

    #[tokio::test]
    async fn test_commit_transaction_multi_table_returns_406() {
        let state = build_state_with_write_enabled();

        let req_body = serde_json::json!({
            "table-changes": [
                {
                    "identifier": {"namespace": ["sales"], "name": "orders"},
                    "requirements": [],
                    "updates": []
                },
                {
                    "identifier": {"namespace": ["sales"], "name": "customers"},
                    "requirements": [],
                    "updates": []
                }
            ]
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/transactions/commit")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", "01941234-5678-7def-8abc-123456789abc")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn test_commit_transaction_empty_changes_returns_400() {
        let state = build_state_with_write_enabled();

        let req_body = serde_json::json!({
            "table-changes": []
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/transactions/commit")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", "01941234-5678-7def-8abc-123456789abc")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_commit_transaction_missing_identifier_returns_400() {
        let state = build_state_with_write_enabled();

        let req_body = serde_json::json!({
            "table-changes": [
                {
                    "requirements": [],
                    "updates": []
                }
            ]
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/transactions/commit")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", "01941234-5678-7def-8abc-123456789abc")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_commit_transaction_write_disabled_returns_400() {
        let storage = Arc::new(MemoryBackend::new());
        let state = IcebergState::new(storage);

        let req_body = serde_json::json!({
            "table-changes": [
                {
                    "identifier": {"namespace": ["sales"], "name": "orders"},
                    "requirements": [],
                    "updates": []
                }
            ]
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/transactions/commit")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", "01941234-5678-7def-8abc-123456789abc")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
