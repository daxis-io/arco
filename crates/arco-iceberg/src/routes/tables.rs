//! Handlers for table endpoints.
//!
//! Implements:
//! - `GET /v1/{prefix}/namespaces/{namespace}/tables` - List tables
//! - `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Check table exists
//! - `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Load table
//! - `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Commit table updates
//! - `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` - Get credentials

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::header::USER_AGENT;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tracing::instrument;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogReader, CatalogWriter};

use crate::commit::{CommitError, CommitService};
use crate::context::IcebergRequestContext;
use crate::error::{IcebergError, IcebergResult};
use crate::idempotency::{IdempotencyMarker, canonical_request_hash};
use crate::paths::resolve_metadata_path;
use crate::pointer::IcebergTablePointer;
use crate::pointer::UpdateSource;
use crate::routes::utils::{ensure_prefix, join_namespace, paginate, parse_namespace};
use crate::state::{CredentialRequest, IcebergState, TableInfo};
use crate::types::{
    AccessDelegation, CommitTableRequest, CommitTableResponse, CreateTableRequest, DropTableQuery,
    ListTablesQuery, ListTablesResponse, LoadTableQuery, LoadTableResponse, RegisterTableRequest,
    SnapshotsFilter, TableCredentialsResponse, TableIdent, TableMetadata,
};

/// Creates table routes.
pub fn routes() -> Router<IcebergState> {
    Router::new()
        .route(
            "/namespaces/:namespace/tables",
            get(list_tables).post(create_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table",
            get(load_table)
                .head(head_table)
                .post(commit_table)
                .delete(drop_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table/credentials",
            get(get_credentials),
        )
        .route(
            "/namespaces/:namespace/register",
            axum::routing::post(register_table),
        )
}

#[derive(Debug, Deserialize)]
struct NamespaceTablesPath {
    prefix: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
struct TablePath {
    prefix: String,
    namespace: String,
    table: String,
}

/// List tables.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces/{namespace}/tables",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ListTablesQuery
    ),
    responses(
        (status = 200, description = "Tables listed", body = ListTablesResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
async fn list_tables(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespaceTablesPath>,
    Query(query): Query<ListTablesQuery>,
) -> IcebergResult<Json<ListTablesResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage);

    let mut tables: Vec<TableIdent> = reader
        .list_tables(&namespace_name)
        .await
        .map_err(IcebergError::from)?
        .into_iter()
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .map(|table| TableIdent::new(namespace_ident.clone(), table.name))
        .collect();

    tables.sort_by(|a, b| a.name.cmp(&b.name));
    let (page, next) = paginate(tables, query.page_token, query.page_size)?;

    Ok(Json(ListTablesResponse {
        identifiers: page,
        next_page_token: next,
    }))
}

/// Create table.
#[utoipa::path(
    post,
    path = "/v1/{prefix}/namespaces/{namespace}/tables",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("X-Iceberg-Access-Delegation" = Option<String>, Header, description = "Credential vending hint"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    request_body = CreateTableRequest,
    responses(
        (status = 200, description = "Table created", body = LoadTableResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Namespace not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Table already exists", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
#[allow(clippy::too_many_lines)]
async fn create_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespaceTablesPath>,
    headers: HeaderMap,
    Json(req): Json<CreateTableRequest>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_table_crud_enabled(&state.config)?;

    if req.stage_create {
        return Err(IcebergError::unsupported_operation(
            "Staged table creation is not supported",
        ));
    }

    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let compactor = state.create_compactor(&storage)?;
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    writer.initialize().await.map_err(IcebergError::from)?;

    let mut options = WriteOptions::default()
        .with_actor(format!("iceberg-api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(ref key) = ctx.idempotency_key {
        options = options.with_idempotency_key(key);
    }

    let default_relative_location = format!("warehouse/{}/{}", namespace_name, req.name);
    let default_scoped_location = format!(
        "tenant={}/workspace={}/{}",
        ctx.tenant, ctx.workspace, default_relative_location
    );

    let (table_location, storage_relative_location) = match &req.location {
        Some(loc) => {
            let resolved = resolve_metadata_path(loc, &ctx.tenant, &ctx.workspace)
                .map_err(|e| IcebergError::BadRequest {
                    message: format!("Invalid table location '{loc}': {e}"),
                    error_type: "BadRequestException",
                })?;
            (loc.clone(), resolved)
        }
        None => (default_scoped_location, default_relative_location),
    };

    let delegation_header = headers.get("X-Iceberg-Access-Delegation");
    if delegation_header.is_some() && state.credential_provider.is_none() {
        return Err(IcebergError::BadRequest {
            message: "Credential vending is not enabled".to_string(),
            error_type: "BadRequestException",
        });
    }

    let table = writer
        .register_table(
            arco_catalog::RegisterTableRequest {
                namespace: namespace_name.clone(),
                name: req.name.clone(),
                description: None,
                location: Some(table_location.clone()),
                format: Some("iceberg".to_string()),
                columns: vec![],
            },
            options,
        )
        .await
        .map_err(IcebergError::from)?;

    let table_uuid = uuid::Uuid::parse_str(&table.id).map_err(|_| IcebergError::Internal {
        message: format!("Invalid table UUID from catalog: {}", table.id),
    })?;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: crate::types::TableUuid::new(table_uuid),
        location: table_location.clone(),
        last_sequence_number: 0,
        last_updated_ms: now_ms,
        last_column_id: req.schema.fields.iter().map(|f| f.id).max().unwrap_or(0),
        current_schema_id: req.schema.schema_id,
        schemas: vec![req.schema],
        current_snapshot_id: None,
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
        properties: req.properties,
        default_spec_id: req.partition_spec.as_ref().map_or(0, |s| s.spec_id),
        partition_specs: req.partition_spec.map(|s| vec![s]).unwrap_or_default(),
        last_partition_id: 0,
        refs: HashMap::new(),
        default_sort_order_id: req.write_order.as_ref().map_or(0, |s| s.order_id),
        sort_orders: req.write_order.map(|s| vec![s]).unwrap_or_default(),
    };

    let metadata_bytes = serde_json::to_vec(&metadata).map_err(|e| IcebergError::Internal {
        message: format!("Failed to serialize metadata: {e}"),
    })?;
    let metadata_storage_path =
        format!("{storage_relative_location}/metadata/00000-{table_uuid}.metadata.json");
    let metadata_location = format!("{table_location}/metadata/00000-{table_uuid}.metadata.json");

    if let Err(e) = storage
        .put_raw(
            &metadata_storage_path,
            bytes::Bytes::from(metadata_bytes),
            arco_core::storage::WritePrecondition::None,
        )
        .await
    {
        if let Err(rollback_err) = writer
            .drop_table(&namespace_name, &req.name, WriteOptions::default())
            .await
        {
            tracing::warn!(error = %rollback_err, "Failed to rollback catalog entry after metadata write failure");
        }
        return Err(IcebergError::Internal {
            message: format!("Failed to write metadata: {e}"),
        });
    }

    let pointer = IcebergTablePointer::new(table_uuid, metadata_location.clone());
    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_bytes = serde_json::to_vec(&pointer).map_err(|e| IcebergError::Internal {
        message: format!("Failed to serialize pointer: {e}"),
    })?;

    let pointer_write_result = match storage
        .put_raw(
            &pointer_path,
            bytes::Bytes::from(pointer_bytes),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
    {
        Ok(result) => result,
        Err(e) => {
            if let Err(rollback_err) = storage.delete(&metadata_storage_path).await {
                tracing::warn!(error = %rollback_err, path = %metadata_storage_path, "Failed to rollback metadata file after pointer write failure");
            }
            if let Err(rollback_err) = writer
                .drop_table(&namespace_name, &req.name, WriteOptions::default())
                .await
            {
                tracing::warn!(error = %rollback_err, "Failed to rollback catalog entry after pointer write failure");
            }
            return Err(IcebergError::Internal {
                message: format!("Failed to write pointer: {e}"),
            });
        }
    };

    let pointer_version = match pointer_write_result {
        arco_core::storage::WriteResult::Success { version } => version,
        arco_core::storage::WriteResult::PreconditionFailed { .. } => {
            if let Err(rollback_err) = storage.delete(&metadata_storage_path).await {
                tracing::warn!(error = %rollback_err, path = %metadata_storage_path, "Failed to rollback metadata file after pointer conflict");
            }
            if let Err(rollback_err) = writer
                .drop_table(&namespace_name, &req.name, WriteOptions::default())
                .await
            {
                tracing::warn!(error = %rollback_err, "Failed to rollback catalog entry after pointer conflict");
            }
            return Err(IcebergError::Conflict {
                message: "Table pointer already exists".to_string(),
                error_type: "AlreadyExistsException",
            });
        }
    };

    let storage_credentials = maybe_vended_credentials(
        &state,
        headers.get("X-Iceberg-Access-Delegation"),
        None,
        TableInfo {
            ident: TableIdent::new(namespace_ident, req.name),
            table_id: table.id,
            location: Some(table_location),
        },
    )
    .await?;

    let response = LoadTableResponse {
        metadata_location,
        metadata,
        config: HashMap::new(),
        storage_credentials,
    };

    let mut resp = Json(response).into_response();
    if let Ok(value) = HeaderValue::from_str(&pointer_version) {
        resp.headers_mut().insert("ETag", value);
    }
    Ok(resp)
}

/// Load table.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name"),
        ("If-None-Match" = Option<String>, Header, description = "ETag for conditional table load"),
        LoadTableQuery,
        ("X-Iceberg-Access-Delegation" = Option<String>, Header, description = "Credential vending hint")
    ),
    responses(
        (status = 200, description = "Table loaded", body = LoadTableResponse),
        (status = 304, description = "Not modified"),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace, table = %path.table))]
#[allow(clippy::too_many_lines)] // Handler with multiple steps: load, ETag check, metadata, credentials
async fn load_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<TablePath>,
    Query(query): Query<LoadTableQuery>,
    headers: HeaderMap,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage.clone());

    let table = reader
        .get_table(&namespace_name, &path.table)
        .await
        .map_err(IcebergError::from)?
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .ok_or_else(|| IcebergError::table_not_found(&namespace_name, &path.table))?;

    let table_uuid = uuid::Uuid::parse_str(&table.id).map_err(|_| IcebergError::Internal {
        message: format!("Invalid table UUID: {}", table.id),
    })?;

    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_meta =
        storage
            .head_raw(&pointer_path)
            .await
            .map_err(|err| IcebergError::Internal {
                message: err.to_string(),
            })?;

    let Some(pointer_meta) = pointer_meta else {
        return Err(IcebergError::table_not_found(&namespace_name, &path.table));
    };

    let pointer_etag = pointer_meta
        .etag
        .clone()
        .unwrap_or_else(|| pointer_meta.version.clone());

    if let Some(if_none_match) = headers
        .get("If-None-Match")
        .and_then(|value| value.to_str().ok())
    {
        if etag_matches(if_none_match, &pointer_etag) {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .body(axum::body::Body::empty())
                .map_err(|e| IcebergError::Internal {
                    message: format!("Failed to build response: {e}"),
                });
        }
    }

    let pointer_bytes =
        storage
            .get_raw(&pointer_path)
            .await
            .map_err(|err| IcebergError::Internal {
                message: err.to_string(),
            })?;

    let pointer: IcebergTablePointer =
        serde_json::from_slice(&pointer_bytes).map_err(|err| IcebergError::Internal {
            message: format!("Failed to parse pointer: {err}"),
        })?;
    pointer
        .validate_version()
        .map_err(|err| IcebergError::Internal {
            message: err.to_string(),
        })?;

    let metadata_path = resolve_metadata_path(
        &pointer.current_metadata_location,
        &ctx.tenant,
        &ctx.workspace,
    )?;
    let metadata_bytes =
        storage
            .get_raw(&metadata_path)
            .await
            .map_err(|err| IcebergError::Internal {
                message: err.to_string(),
            })?;

    let mut metadata: TableMetadata =
        serde_json::from_slice(&metadata_bytes).map_err(|err| IcebergError::Internal {
            message: format!("Failed to parse table metadata: {err}"),
        })?;

    if query.snapshots == Some(SnapshotsFilter::Refs) {
        let referenced_ids: std::collections::HashSet<i64> =
            metadata.refs.values().map(|r| r.snapshot_id).collect();
        metadata
            .snapshots
            .retain(|s| referenced_ids.contains(&s.snapshot_id));
    }

    let storage_credentials = maybe_vended_credentials(
        &state,
        headers.get("X-Iceberg-Access-Delegation"),
        None,
        TableInfo {
            ident: TableIdent::new(namespace_ident, table.name.clone()),
            table_id: table.id.clone(),
            location: table.location.clone(),
        },
    )
    .await?;

    let mut config = HashMap::new();
    if storage_credentials.is_some() && state.credentials_enabled() {
        config.insert(
            "client.refresh-credentials-endpoint".to_string(),
            format!(
                "/v1/{}/namespaces/{}/tables/{}/credentials",
                path.prefix, path.namespace, path.table
            ),
        );
    }

    let response = LoadTableResponse {
        metadata_location: pointer.current_metadata_location,
        metadata,
        config,
        storage_credentials,
    };

    let mut response = Json(response).into_response();
    if let Ok(value) = HeaderValue::from_str(&pointer_etag) {
        response.headers_mut().insert("ETag", value);
    }
    Ok(response)
}

/// Head table.
#[utoipa::path(
    head,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 204, description = "Table exists"),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace, table = %path.table))]
async fn head_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<TablePath>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage);

    let exists = reader
        .get_table(&namespace_name, &path.table)
        .await
        .map_err(IcebergError::from)?
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .is_some();

    let status = if exists {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    };

    Response::builder()
        .status(status)
        .body(axum::body::Body::empty())
        .map_err(|e| IcebergError::Internal {
            message: format!("Failed to build response: {e}"),
        })
}

/// Commit table updates.
#[utoipa::path(
    post,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name"),
        ("Idempotency-Key" = String, Header, description = "Idempotency key for commit_table")
    ),
    request_body = CommitTableRequest,
    responses(
        (status = 200, description = "Commit successful", body = CommitTableResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Conflict", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 502, description = "Bad gateway", body = crate::error::IcebergErrorResponse),
        (status = 504, description = "Gateway timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace, table = %path.table))]
async fn commit_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<TablePath>,
    headers: HeaderMap,
    Json(request_value): Json<serde_json::Value>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;

    if !state.config.allow_write {
        return Err(IcebergError::BadRequest {
            message: "Write operations are not enabled".to_string(),
            error_type: "NotImplementedException",
        });
    }

    let request_hash =
        canonical_request_hash(&request_value).map_err(|err| IcebergError::BadRequest {
            message: format!("Invalid commit request: {err}"),
            error_type: "BadRequestException",
        })?;

    let request: CommitTableRequest =
        serde_json::from_value(request_value).map_err(|err| IcebergError::BadRequest {
            message: format!("Invalid commit request: {err}"),
            error_type: "BadRequestException",
        })?;

    if let Some(identifier) = &request.identifier {
        let separator = state.config.namespace_separator_decoded();
        let namespace_ident = parse_namespace(&path.namespace, &separator)?;
        if identifier.name != path.table || identifier.namespace != namespace_ident {
            return Err(IcebergError::BadRequest {
                message: "Request identifier does not match path".to_string(),
                error_type: "BadRequestException",
            });
        }
    }

    if let Some(reason) = request.check_guardrails() {
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
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage.clone());

    let table = reader
        .get_table(&namespace_name, &path.table)
        .await
        .map_err(IcebergError::from)?
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .ok_or_else(|| IcebergError::table_not_found(&namespace_name, &path.table))?;

    let table_uuid = uuid::Uuid::parse_str(&table.id).map_err(|_| IcebergError::Internal {
        message: format!("Invalid table UUID: {}", table.id),
    })?;

    let client_info = headers
        .get(USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let source = UpdateSource::IcebergRest {
        client_info,
        principal: None,
    };

    let commit_service = CommitService::new(Arc::new(storage));

    match commit_service
        .commit_table(
            table_uuid,
            &namespace_name,
            &path.table,
            request,
            request_hash,
            idempotency_key,
            source,
            &ctx.tenant,
            &ctx.workspace,
        )
        .await
    {
        Ok(response) => Ok(Json::<CommitTableResponse>(response).into_response()),
        Err(CommitError::Iceberg(err)) => Err(err),
        Err(CommitError::CachedFailure { status, payload }) => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::CONFLICT);
            Ok((status, Json(payload)).into_response())
        }
        Err(CommitError::RetryAfter { seconds }) => Err(IcebergError::ServiceUnavailable {
            message: "Commit already in progress".to_string(),
            retry_after_seconds: Some(seconds),
        }),
    }
}

/// Drop table.
#[utoipa::path(
    delete,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name"),
        DropTableQuery,
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    responses(
        (status = 204, description = "Table dropped"),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Table not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace, table = %path.table, purge_requested = %query.purge_requested))]
async fn drop_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<TablePath>,
    Query(query): Query<DropTableQuery>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_table_crud_enabled(&state.config)?;

    if query.purge_requested {
        return Err(IcebergError::unsupported_operation(
            "Data purge is not supported. Use purgeRequested=false to drop the table without purging data files.",
        ));
    }

    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;

    let reader = CatalogReader::new(storage.clone());
    let table = reader
        .get_table(&namespace_name, &path.table)
        .await
        .map_err(IcebergError::from)?
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .ok_or_else(|| IcebergError::table_not_found(&namespace_name, &path.table))?;

    let compactor = state.create_compactor(&storage)?;
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    let mut options = WriteOptions::default()
        .with_actor(format!("iceberg-api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(ref key) = ctx.idempotency_key {
        options = options.with_idempotency_key(key);
    }

    writer
        .drop_table(&namespace_name, &path.table, options)
        .await
        .map_err(IcebergError::from)?;

    let table_uuid = uuid::Uuid::parse_str(&table.id).ok();
    if let Some(uuid) = table_uuid {
        let pointer_path = IcebergTablePointer::storage_path(&uuid);
        if let Err(e) = storage.delete(&pointer_path).await {
            tracing::warn!(error = %e, path = %pointer_path, table_uuid = %uuid, "Failed to delete pointer file during table drop");
        }
    }

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(axum::body::Body::empty())
        .map_err(|e| IcebergError::Internal {
            message: format!("Failed to build response: {e}"),
        })
}

/// Get credentials for a table.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name"),
        ("planId" = Option<String>, Query, description = "Plan ID for server-side scan planning (not yet implemented; parameter accepted but ignored)")
    ),
    responses(
        (status = 200, description = "Credentials returned", body = TableCredentialsResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace, table = %path.table))]
async fn get_credentials(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<TablePath>,
    headers: HeaderMap,
) -> IcebergResult<Json<TableCredentialsResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;
    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    let reader = CatalogReader::new(storage);

    let table = reader
        .get_table(&namespace_name, &path.table)
        .await
        .map_err(IcebergError::from)?
        .filter(|table| is_iceberg_table(table.format.as_deref()))
        .ok_or_else(|| IcebergError::table_not_found(&namespace_name, &path.table))?;

    let credentials = maybe_vended_credentials(
        &state,
        headers.get("X-Iceberg-Access-Delegation"),
        Some(AccessDelegation::VendedCredentials),
        TableInfo {
            ident: TableIdent::new(namespace_ident, table.name),
            table_id: table.id,
            location: table.location,
        },
    )
    .await?
    .ok_or_else(|| IcebergError::BadRequest {
        message: "Credential vending is not enabled".to_string(),
        error_type: "BadRequestException",
    })?;

    Ok(Json(TableCredentialsResponse {
        storage_credentials: credentials,
    }))
}

/// Register an existing Iceberg table with the catalog.
///
/// This endpoint creates a new metadata file with the catalog-assigned UUID,
/// leaving the original metadata file unchanged (immutable). The response
/// returns the new metadata location that clients should use for subsequent
/// operations.
#[utoipa::path(
    post,
    path = "/v1/{prefix}/namespaces/{namespace}/register",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key")
    ),
    request_body = RegisterTableRequest,
    responses(
        (status = 200, description = "Table registered. The metadata-location in the response points to a new metadata file with the catalog-assigned UUID.", body = LoadTableResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
        (status = 403, description = "Forbidden", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Namespace not found", body = crate::error::IcebergErrorResponse),
        (status = 406, description = "Unsupported operation", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Table already exists", body = crate::error::IcebergErrorResponse),
        (status = 419, description = "Authentication timeout", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
        (status = 500, description = "Internal error", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace))]
#[allow(clippy::too_many_lines)]
async fn register_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<NamespaceTablesPath>,
    Json(req): Json<RegisterTableRequest>,
) -> IcebergResult<Response> {
    ensure_prefix(&path.prefix, &state.config)?;
    ensure_table_crud_enabled(&state.config)?;

    let separator = state.config.namespace_separator_decoded();
    let namespace_ident = parse_namespace(&path.namespace, &separator)?;
    let namespace_name = join_namespace(&namespace_ident, &separator)?;

    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;

    let metadata_path = resolve_metadata_path(&req.metadata_location, &ctx.tenant, &ctx.workspace)?;
    let metadata_bytes =
        storage
            .get_raw(&metadata_path)
            .await
            .map_err(|e| IcebergError::BadRequest {
                message: format!("Cannot read metadata at {}: {e}", req.metadata_location),
                error_type: "BadRequestException",
            })?;

    let mut metadata: TableMetadata =
        serde_json::from_slice(&metadata_bytes).map_err(|e| IcebergError::BadRequest {
            message: format!("Invalid metadata file: {e}"),
            error_type: "BadRequestException",
        })?;

    let reader = CatalogReader::new(storage.clone());
    let existing = reader
        .get_table(&namespace_name, &req.name)
        .await
        .map_err(IcebergError::from)?;

    if existing.is_some() && !req.overwrite {
        return Err(IcebergError::Conflict {
            message: format!("Table already exists: {}.{}", namespace_name, req.name),
            error_type: "AlreadyExistsException",
        });
    }

    let storage_relative_location =
        resolve_metadata_path(&metadata.location, &ctx.tenant, &ctx.workspace).map_err(|e| {
            IcebergError::BadRequest {
                message: format!(
                    "Invalid table location '{}' in metadata file: {e}",
                    metadata.location
                ),
                error_type: "BadRequestException",
            }
        })?;

    let compactor = state.create_compactor(&storage)?;
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    writer.initialize().await.map_err(IcebergError::from)?;

    let mut options = WriteOptions::default()
        .with_actor(format!("iceberg-api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    if let Some(ref key) = ctx.idempotency_key {
        options = options.with_idempotency_key(key);
    }

    if existing.is_some() && req.overwrite {
        writer
            .drop_table(&namespace_name, &req.name, options.clone())
            .await
            .map_err(IcebergError::from)?;
    }

    let table = writer
        .register_table(
            arco_catalog::RegisterTableRequest {
                namespace: namespace_name.clone(),
                name: req.name.clone(),
                description: None,
                location: Some(metadata.location.clone()),
                format: Some("iceberg".to_string()),
                columns: vec![],
            },
            options,
        )
        .await
        .map_err(IcebergError::from)?;

    let table_uuid = uuid::Uuid::parse_str(&table.id).map_err(|_| IcebergError::Internal {
        message: format!("Invalid table UUID: {}", table.id),
    })?;

    metadata.table_uuid = crate::types::TableUuid::new(table_uuid);
    let new_metadata_storage_path =
        format!("{storage_relative_location}/metadata/arco-registered-{table_uuid}.metadata.json");
    let new_metadata_location =
        format!("{}/metadata/arco-registered-{table_uuid}.metadata.json", metadata.location);

    let updated_metadata_bytes =
        serde_json::to_vec(&metadata).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize updated metadata: {e}"),
        })?;

    if let Err(e) = storage
        .put_raw(
            &new_metadata_storage_path,
            bytes::Bytes::from(updated_metadata_bytes),
            arco_core::storage::WritePrecondition::None,
        )
        .await
    {
        if let Err(rollback_err) = writer
            .drop_table(&namespace_name, &req.name, WriteOptions::default())
            .await
        {
            tracing::warn!(error = %rollback_err, "Failed to rollback catalog entry after metadata write failure in register");
        }
        return Err(IcebergError::Internal {
            message: format!("Failed to write metadata file: {e}"),
        });
    }

    let pointer = IcebergTablePointer::new(table_uuid, new_metadata_location.clone());
    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_bytes = serde_json::to_vec(&pointer).map_err(|e| IcebergError::Internal {
        message: format!("Failed to serialize pointer: {e}"),
    })?;

    let pointer_write_result = match storage
        .put_raw(
            &pointer_path,
            bytes::Bytes::from(pointer_bytes),
            arco_core::storage::WritePrecondition::DoesNotExist,
        )
        .await
    {
        Ok(result) => result,
        Err(e) => {
            if let Err(rollback_err) = storage.delete(&new_metadata_storage_path).await {
                tracing::warn!(error = %rollback_err, path = %new_metadata_storage_path, "Failed to rollback metadata file after pointer write failure in register");
            }
            if let Err(rollback_err) = writer
                .drop_table(&namespace_name, &req.name, WriteOptions::default())
                .await
            {
                tracing::warn!(error = %rollback_err, "Failed to rollback catalog entry after pointer write failure in register");
            }
            return Err(IcebergError::Internal {
                message: format!("Failed to write pointer: {e}"),
            });
        }
    };

    if matches!(
        pointer_write_result,
        arco_core::storage::WriteResult::PreconditionFailed { .. }
    ) {
        if let Err(rollback_err) = storage.delete(&new_metadata_storage_path).await {
            tracing::warn!(error = %rollback_err, path = %new_metadata_storage_path, "Failed to rollback metadata file after pointer conflict in register");
        }
        if let Err(rollback_err) = writer
            .drop_table(&namespace_name, &req.name, WriteOptions::default())
            .await
        {
            tracing::warn!(error = %rollback_err, "Failed to rollback catalog entry after pointer conflict in register");
        }
        return Err(IcebergError::Conflict {
            message: "Table pointer already exists".to_string(),
            error_type: "AlreadyExistsException",
        });
    }

    let response = LoadTableResponse {
        metadata_location: new_metadata_location,
        metadata,
        config: HashMap::new(),
        storage_credentials: None,
    };

    Ok(Json(response).into_response())
}

fn is_iceberg_table(format: Option<&str>) -> bool {
    format.is_some_and(|value| value.eq_ignore_ascii_case("iceberg"))
}

fn ensure_table_crud_enabled(config: &crate::state::IcebergConfig) -> IcebergResult<()> {
    if !config.allow_table_crud {
        return Err(IcebergError::unsupported_operation(
            "Table mutations are not enabled",
        ));
    }
    Ok(())
}

fn etag_matches(if_none_match: &str, version: &str) -> bool {
    let expected = normalize_etag(version);
    if_none_match.split(',').map(str::trim).any(|etag| {
        let normalized = normalize_etag(etag);
        normalized == "*" || normalized == expected
    })
}

fn normalize_etag(value: &str) -> &str {
    let value = value.trim();
    let value = value.strip_prefix("W/").unwrap_or(value);
    value.trim_matches('"')
}

async fn maybe_vended_credentials(
    state: &IcebergState,
    delegation_header: Option<&HeaderValue>,
    default_delegation: Option<AccessDelegation>,
    table: TableInfo,
) -> IcebergResult<Option<Vec<crate::types::StorageCredential>>> {
    let delegation = delegation_header
        .and_then(|value| value.to_str().ok())
        .and_then(AccessDelegation::from_header)
        .or(default_delegation);

    let Some(delegation) = delegation else {
        return Ok(None);
    };
    if !delegation.is_supported() {
        return Err(IcebergError::BadRequest {
            message: format!("Unsupported access delegation: {delegation:?}"),
            error_type: "BadRequestException",
        });
    }

    let provider = state
        .credential_provider
        .as_ref()
        .ok_or_else(|| IcebergError::BadRequest {
            message: "Credential vending is not enabled".to_string(),
            error_type: "BadRequestException",
        })?;

    let credentials = provider
        .vended_credentials(CredentialRequest { table, delegation })
        .await?;

    Ok(Some(credentials))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::IcebergConfig;
    use arco_catalog::CatalogWriter;
    use arco_catalog::Tier1Compactor;
    use arco_catalog::write_options::WriteOptions;
    use arco_core::ScopedStorage;
    use arco_core::storage::{MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
    use axum::body::Body;
    use axum::http::Request;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::ops::Range;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct PointerConflictBackend {
        inner: Arc<MemoryBackend>,
    }

    impl PointerConflictBackend {
        fn new() -> Self {
            Self {
                inner: Arc::new(MemoryBackend::new()),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for PointerConflictBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            if path.contains("/_catalog/iceberg_pointers/")
                && matches!(precondition, WritePrecondition::DoesNotExist)
            {
                return Ok(WriteResult::PreconditionFailed {
                    current_version: "forced-precondition-failure".to_string(),
                });
            }

            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    fn build_state() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        IcebergState::new(storage)
    }

    fn build_state_with_write_enabled() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            allow_write: true,
            ..Default::default()
        };
        IcebergState::with_config(storage, config)
    }

    fn build_state_with_table_crud_enabled() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            allow_table_crud: true,
            ..Default::default()
        };
        IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(crate::state::Tier1CompactorFactory))
    }

    fn build_state_with_table_crud_and_write_enabled() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        let config = IcebergConfig {
            allow_table_crud: true,
            allow_write: true,
            ..Default::default()
        };
        IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(crate::state::Tier1CompactorFactory))
    }

    async fn seed_table(state: &IcebergState, namespace: &str, table: &str) -> String {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace(namespace, None, WriteOptions::default())
            .await
            .expect("create namespace");

        let table = writer
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

        let pointer = IcebergTablePointer::new(
            uuid::Uuid::parse_str(&table.id).expect("uuid"),
            "tenant=acme/workspace=analytics/warehouse/table/metadata/00000.metadata.json"
                .to_string(),
        );
        let pointer_path =
            IcebergTablePointer::storage_path(&uuid::Uuid::parse_str(&table.id).expect("uuid"));
        let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize");
        storage
            .put_raw(
                &pointer_path,
                Bytes::from(pointer_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("put pointer");

        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: crate::types::TableUuid::new(
                uuid::Uuid::parse_str(&table.id).expect("uuid"),
            ),
            location: "tenant=acme/workspace=analytics/warehouse/table".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 1234567890000,
            last_column_id: 0,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            refs: HashMap::new(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        };
        let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize");
        storage
            .put_raw(
                "warehouse/table/metadata/00000.metadata.json",
                Bytes::from(metadata_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        table.id
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
    async fn test_list_tables() {
        let state = build_state();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_load_table() {
        let state = build_state();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_load_table_if_none_match_star() {
        let state = build_state();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("If-None-Match", "*")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn test_head_table_exists_returns_no_content() {
        let state = build_state();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_commit_table_guardrails_reject_set_location() {
        let state = build_state_with_write_enabled();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let request_body = serde_json::json!({
            "requirements": [],
            "updates": [{"action": "set-location", "location": "s3://new-bucket/table"}]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Idempotency-Key", "01924a7c-8d9f-7000-8000-000000000001")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&request_body).expect("serialize body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    async fn seed_namespace_only(state: &IcebergState, namespace: &str) {
        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace(namespace, None, WriteOptions::default())
            .await
            .expect("create namespace");
    }

    #[tokio::test]
    async fn test_create_table() {
        let state = build_state_with_table_crud_enabled();
        seed_namespace_only(&state, "sales").await;

        let app = app(state);
        let req_body = serde_json::json!({
            "name": "orders",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "data", "required": false, "type": "string"}
                ]
            },
            "properties": {"owner": "data-team"}
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers().get("ETag").is_some(),
            "create-table should return ETag header"
        );

        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .expect("body");
        let resp: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(resp["metadata"]["table-uuid"].is_string());
        assert_eq!(resp["metadata"]["format-version"], 2);
    }

    #[tokio::test]
    async fn test_create_table_then_load_succeeds() {
        let state = build_state_with_table_crud_enabled();
        seed_namespace_only(&state, "sales").await;

        let req_body = serde_json::json!({
            "name": "orders",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            }
        });

        let create_resp = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(create_resp.status(), StatusCode::OK);

        let load_resp = app(state)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(load_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(load_resp.into_body(), 65536)
            .await
            .expect("body");
        let resp: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(resp["metadata"]["table-uuid"].is_string());
    }

    #[tokio::test]
    async fn test_create_table_then_commit_with_uuid_assert_succeeds() {
        let state = build_state_with_table_crud_and_write_enabled();
        seed_namespace_only(&state, "sales").await;

        let create_body = serde_json::json!({
            "name": "orders",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            }
        });

        let create_resp = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(create_resp.status(), StatusCode::OK);
        let create_body_bytes = axum::body::to_bytes(create_resp.into_body(), 65536)
            .await
            .expect("body");
        let create_json: serde_json::Value =
            serde_json::from_slice(&create_body_bytes).expect("json");
        let table_uuid = create_json["metadata"]["table-uuid"]
            .as_str()
            .expect("table-uuid");

        let commit_body = serde_json::json!({
            "requirements": [
                {"type": "assert-table-uuid", "uuid": table_uuid}
            ],
            "updates": []
        });

        let commit_resp = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Idempotency-Key", "01924a7c-8d9f-7000-8000-000000000001")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&commit_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        let commit_status = commit_resp.status();
        let commit_body_bytes = axum::body::to_bytes(commit_resp.into_body(), 65536)
            .await
            .expect("body");
        let commit_body_str = String::from_utf8_lossy(&commit_body_bytes);

        assert_eq!(
            commit_status,
            StatusCode::OK,
            "commit with assert-table-uuid should succeed: {commit_body_str}"
        );
    }

    #[tokio::test]
    async fn test_create_table_disabled() {
        let state = build_state();

        let app = app(state);
        let req_body = serde_json::json!({
            "name": "orders",
            "schema": {"schema-id": 0, "fields": []}
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/sales/tables")
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
    async fn test_drop_table() {
        let state = build_state_with_table_crud_enabled();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_drop_table_with_purge_returns_406() {
        let state = build_state_with_table_crud_enabled();
        seed_table(&state, "sales", "orders").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/sales/tables/orders?purgeRequested=true")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn test_drop_table_not_found() {
        let state = build_state_with_table_crud_enabled();
        seed_namespace_only(&state, "sales").await;

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/sales/tables/nonexistent")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_drop_table_disabled() {
        let state = build_state();

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/arco/namespaces/sales/tables/orders")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn test_register_table() {
        let state = build_state_with_table_crud_enabled();

        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace("imports", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: crate::types::TableUuid::new(uuid::Uuid::new_v4()),
            location: "warehouse/imported".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 1234567890000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            refs: HashMap::new(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        };
        let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize");
        storage
            .put_raw(
                "warehouse/imported/metadata/v1.metadata.json",
                Bytes::from(metadata_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let app = app(state);
        let req_body = serde_json::json!({
            "name": "imported_table",
            "metadata-location": "warehouse/imported/metadata/v1.metadata.json"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/imports/register")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .expect("body");
        let resp: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(resp["metadata"]["format-version"], 2);
    }

    #[tokio::test]
    async fn test_register_table_then_commit_with_uuid_assert_succeeds() {
        let state = build_state_with_table_crud_and_write_enabled();

        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace("imports", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let original_uuid = uuid::Uuid::new_v4();
        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: crate::types::TableUuid::new(original_uuid),
            location: "warehouse/imported".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 1234567890000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            refs: HashMap::new(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        };
        let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize");
        storage
            .put_raw(
                "warehouse/imported/metadata/v1.metadata.json",
                Bytes::from(metadata_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let register_body = serde_json::json!({
            "name": "imported_table",
            "metadata-location": "warehouse/imported/metadata/v1.metadata.json"
        });

        let register_resp = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/imports/register")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&register_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(register_resp.status(), StatusCode::OK);

        let register_body_bytes = axum::body::to_bytes(register_resp.into_body(), 65536)
            .await
            .expect("body");
        let register_json: serde_json::Value =
            serde_json::from_slice(&register_body_bytes).expect("json");
        let table_uuid = register_json["metadata"]["table-uuid"]
            .as_str()
            .expect("table-uuid");

        assert_ne!(
            table_uuid,
            original_uuid.to_string(),
            "table-uuid should be updated to catalog UUID, not the original"
        );

        let commit_body = serde_json::json!({
            "requirements": [
                {"type": "assert-table-uuid", "uuid": table_uuid}
            ],
            "updates": []
        });

        let commit_resp = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/imports/tables/imported_table")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Idempotency-Key", "01924a7c-8d9f-7000-8000-000000000002")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&commit_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        let commit_status = commit_resp.status();
        let commit_body_bytes = axum::body::to_bytes(commit_resp.into_body(), 65536)
            .await
            .expect("body");
        let commit_body_str = String::from_utf8_lossy(&commit_body_bytes);

        assert_eq!(
            commit_status,
            StatusCode::OK,
            "commit with assert-table-uuid on registered table should succeed: {commit_body_str}"
        );
    }

    #[tokio::test]
    async fn test_register_table_disabled() {
        let state = build_state();

        let app = app(state);
        let req_body = serde_json::json!({
            "name": "imported",
            "metadata-location": "warehouse/metadata.json"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/imports/register")
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
    async fn test_register_table_preserves_original_metadata_file() {
        let state = build_state_with_table_crud_and_write_enabled();

        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
        let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace("immutable_test", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let original_uuid = uuid::Uuid::new_v4();
        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: crate::types::TableUuid::new(original_uuid),
            location: "warehouse/immutable_test".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 1234567890000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            refs: HashMap::new(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        };
        let original_metadata_bytes = serde_json::to_vec(&metadata).expect("serialize");
        let original_path = "warehouse/immutable_test/metadata/v1.metadata.json";
        storage
            .put_raw(
                original_path,
                Bytes::from(original_metadata_bytes.clone()),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let register_body = serde_json::json!({
            "name": "immutable_table",
            "metadata-location": original_path
        });

        let register_resp = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/immutable_test/register")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&register_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(register_resp.status(), StatusCode::OK);

        let register_body_bytes = axum::body::to_bytes(register_resp.into_body(), 65536)
            .await
            .expect("body");
        let register_json: serde_json::Value =
            serde_json::from_slice(&register_body_bytes).expect("json");

        let returned_metadata_location = register_json["metadata-location"]
            .as_str()
            .expect("metadata-location");
        assert!(
            returned_metadata_location.contains("arco-registered-"),
            "response should return new metadata location with arco-registered prefix: {returned_metadata_location}"
        );

        let catalog_uuid = register_json["metadata"]["table-uuid"]
            .as_str()
            .expect("table-uuid");
        assert!(
            returned_metadata_location.contains(catalog_uuid),
            "new metadata location should contain catalog UUID"
        );

        let original_file_bytes = storage.get_raw(original_path).await.expect("get original");
        let original_file_metadata: TableMetadata =
            serde_json::from_slice(&original_file_bytes).expect("parse original");
        assert_eq!(
            original_file_metadata.table_uuid.as_uuid().to_string(),
            original_uuid.to_string(),
            "original metadata file should be unchanged"
        );

        let new_metadata_path = format!(
            "warehouse/immutable_test/metadata/arco-registered-{catalog_uuid}.metadata.json"
        );
        let new_file_bytes = storage.get_raw(&new_metadata_path).await.expect("get new");
        let new_file_metadata: TableMetadata =
            serde_json::from_slice(&new_file_bytes).expect("parse new");
        assert_eq!(
            new_file_metadata.table_uuid.as_uuid().to_string(),
            catalog_uuid,
            "new metadata file should have catalog UUID"
        );
    }

    #[tokio::test]
    async fn test_create_table_with_invalid_location_returns_400() {
        let state = build_state_with_table_crud_and_write_enabled();
        seed_namespace_only(&state, "test_ns").await;

        let create_body = serde_json::json!({
            "name": "invalid_loc_table",
            "location": "gs://bucket",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            }
        });

        let response = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/test_ns/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .expect("body");
        let error: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let error_message = error["error"]["message"]
            .as_str()
            .or_else(|| error["message"].as_str())
            .unwrap_or("");
        assert!(
            error_message.contains("Invalid table location"),
            "Error should mention invalid location: {error}"
        );
    }

    #[tokio::test]
    async fn test_create_table_with_cred_vending_header_but_disabled_returns_400_before_writes() {
        let state = build_state_with_table_crud_and_write_enabled();
        seed_namespace_only(&state, "cred_test").await;

        let create_body = serde_json::json!({
            "name": "cred_table",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            }
        });

        let response = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/cred_test/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("X-Iceberg-Access-Delegation", "vended-credentials")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .expect("body");
        let error: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let error_message = error["error"]["message"]
            .as_str()
            .or_else(|| error["message"].as_str())
            .unwrap_or("");
        assert!(
            error_message.contains("Credential vending is not enabled"),
            "Error should mention credential vending: {error}"
        );

        let storage = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let reader = CatalogReader::new(storage);
        let table = reader
            .get_table("cred_test", "cred_table")
            .await
            .expect("get table");
        assert!(
            table.is_none(),
            "Table should not exist when credential vending check fails early"
        );
    }

    #[tokio::test]
    async fn test_create_table_pointer_conflict_rolls_back() {
        let storage: Arc<dyn StorageBackend> = Arc::new(PointerConflictBackend::new());
        let config = IcebergConfig {
            allow_table_crud: true,
            allow_write: true,
            ..Default::default()
        };
        let state = IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(crate::state::Tier1CompactorFactory));

        seed_namespace_only(&state, "conflict_ns").await;

        let create_body = serde_json::json!({
            "name": "conflict_table",
            "schema": {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"}
                ]
            }
        });

        let response = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/conflict_ns/tables")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CONFLICT);

        let scoped = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let reader = CatalogReader::new(scoped.clone());
        let table = reader
            .get_table("conflict_ns", "conflict_table")
            .await
            .expect("get table");
        assert!(table.is_none(), "catalog entry should be rolled back");

        let objects = scoped
            .list("warehouse/conflict_ns/conflict_table/metadata/")
            .await
            .expect("list");
        assert!(objects.is_empty(), "metadata file should be rolled back");
    }

    #[tokio::test]
    async fn test_register_table_pointer_conflict_rolls_back() {
        let storage: Arc<dyn StorageBackend> = Arc::new(PointerConflictBackend::new());
        let config = IcebergConfig {
            allow_table_crud: true,
            allow_write: true,
            ..Default::default()
        };
        let state = IcebergState::with_config(storage, config)
            .with_compactor_factory(Arc::new(crate::state::Tier1CompactorFactory));

        let scoped = ScopedStorage::new(Arc::clone(&state.storage), "acme", "analytics")
            .expect("scoped storage");
        let compactor = Arc::new(Tier1Compactor::new(scoped.clone()));
        let writer = CatalogWriter::new(scoped.clone()).with_sync_compactor(compactor);
        writer.initialize().await.expect("init");
        writer
            .create_namespace("imports", None, WriteOptions::default())
            .await
            .expect("create namespace");

        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: crate::types::TableUuid::new(uuid::Uuid::new_v4()),
            location: "warehouse/imported".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 1234567890000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 0,
            refs: HashMap::new(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        };
        let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize");
        scoped
            .put_raw(
                "warehouse/imported/metadata/v1.metadata.json",
                Bytes::from(metadata_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let register_body = serde_json::json!({
            "name": "imported_table",
            "metadata-location": "warehouse/imported/metadata/v1.metadata.json"
        });

        let response = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/namespaces/imports/register")
                    .header("X-Tenant-Id", "acme")
                    .header("X-Workspace-Id", "analytics")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&register_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::CONFLICT);

        let reader = CatalogReader::new(scoped.clone());
        let table = reader
            .get_table("imports", "imported_table")
            .await
            .expect("get table");
        assert!(table.is_none(), "catalog entry should be rolled back");

        let objects = scoped
            .list("warehouse/imported/metadata/")
            .await
            .expect("list");
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].as_str(),
            "warehouse/imported/metadata/v1.metadata.json"
        );
    }
}
