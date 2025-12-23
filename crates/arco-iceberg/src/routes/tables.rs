//! Handlers for table endpoints.
//!
//! Implements:
//! - `GET /v1/{prefix}/namespaces/{namespace}/tables` - List tables
//! - `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Check table exists
//! - `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` - Load table
//! - `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` - Get credentials

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tracing::instrument;

use arco_catalog::CatalogReader;

use crate::context::IcebergRequestContext;
use crate::error::{IcebergError, IcebergResult};
use crate::pointer::IcebergTablePointer;
use crate::routes::utils::{ensure_prefix, join_namespace, paginate, parse_namespace};
use crate::state::{CredentialRequest, IcebergState, TableInfo};
use crate::types::{
    AccessDelegation, ListTablesQuery, ListTablesResponse, LoadTableResponse, TableCredentialsResponse,
    TableIdent, TableMetadata,
};

/// Creates table routes.
pub fn routes() -> Router<IcebergState> {
    Router::new()
        .route("/namespaces/:namespace/tables", get(list_tables))
        .route(
            "/namespaces/:namespace/tables/:table",
            get(load_table).head(head_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table/credentials",
            get(get_credentials),
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

/// Load table.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Table loaded", body = LoadTableResponse),
        (status = 304, description = "Not modified"),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
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
    let pointer_meta = storage
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

    if let Some(if_none_match) = headers.get("If-None-Match").and_then(|value| value.to_str().ok())
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

    let pointer_bytes = storage
        .get_raw(&pointer_path)
        .await
        .map_err(|err| IcebergError::Internal {
            message: err.to_string(),
        })?;

    let pointer: IcebergTablePointer =
        serde_json::from_slice(&pointer_bytes).map_err(|err| IcebergError::Internal {
            message: format!("Failed to parse pointer: {err}"),
        })?;
    pointer.validate_version().map_err(|err| IcebergError::Internal {
        message: err.to_string(),
    })?;

    let metadata_path = resolve_metadata_path(
        &pointer.current_metadata_location,
        &ctx.tenant,
        &ctx.workspace,
    )?;
    let metadata_bytes = storage
        .get_raw(&metadata_path)
        .await
        .map_err(|err| IcebergError::Internal {
            message: err.to_string(),
        })?;

    let metadata: TableMetadata =
        serde_json::from_slice(&metadata_bytes).map_err(|err| IcebergError::Internal {
            message: format!("Failed to parse table metadata: {err}"),
        })?;

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
        ("table" = String, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Table exists"),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
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
        StatusCode::OK
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

/// Get credentials for a table.
#[utoipa::path(
    get,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Credentials returned", body = TableCredentialsResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 401, description = "Unauthorized", body = crate::error::IcebergErrorResponse),
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

fn is_iceberg_table(format: Option<&str>) -> bool {
    format.is_some_and(|value| value.eq_ignore_ascii_case("iceberg"))
}

fn etag_matches(if_none_match: &str, version: &str) -> bool {
    let expected = normalize_etag(version);
    if_none_match
        .split(',')
        .map(str::trim)
        .any(|etag| normalize_etag(etag) == expected)
}

fn normalize_etag(value: &str) -> &str {
    let value = value.trim();
    let value = value.strip_prefix("W/").unwrap_or(value);
    value.trim_matches('"')
}

fn resolve_metadata_path(
    location: &str,
    tenant: &str,
    workspace: &str,
) -> IcebergResult<String> {
    let path = if let Some((_, rest)) = location.split_once("://") {
        let mut parts = rest.splitn(2, '/');
        let _bucket = parts.next();
        parts
            .next()
            .ok_or_else(|| IcebergError::Internal {
                message: format!("Invalid metadata location: {location}"),
            })?
            .to_string()
    } else {
        location.to_string()
    };

    let scoped_prefix = format!("tenant={tenant}/workspace={workspace}/");
    let relative = path
        .strip_prefix(&scoped_prefix)
        .unwrap_or(path.as_str())
        .to_string();

    if relative.is_empty() {
        return Err(IcebergError::Internal {
            message: format!("Invalid metadata location: {location}"),
        });
    }

    Ok(relative)
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

    let provider = state.credential_provider.as_ref().ok_or_else(|| IcebergError::BadRequest {
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
    use axum::body::Body;
    use axum::http::Request;
    use arco_catalog::CatalogWriter;
    use arco_catalog::Tier1Compactor;
    use arco_catalog::write_options::WriteOptions;
    use arco_core::ScopedStorage;
    use arco_core::storage::MemoryBackend;
    use bytes::Bytes;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn build_state() -> IcebergState {
        let storage = Arc::new(MemoryBackend::new());
        IcebergState::new(storage)
    }

    async fn seed_table(state: &IcebergState, namespace: &str, table: &str) -> String {
        let storage = ScopedStorage::new(
            Arc::clone(&state.storage),
            "acme",
            "analytics",
        )
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
        let pointer_path = IcebergTablePointer::storage_path(
            &uuid::Uuid::parse_str(&table.id).expect("uuid"),
        );
        let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize");
        storage
            .put_raw(&pointer_path, Bytes::from(pointer_bytes), arco_core::storage::WritePrecondition::None)
            .await
            .expect("put pointer");

        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: crate::types::TableUuid::new(uuid::Uuid::parse_str(&table.id).expect("uuid")),
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
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        table.id
    }

    fn app(state: IcebergState) -> Router {
        Router::new()
            .nest("/v1/:prefix", routes())
            .layer(axum::middleware::from_fn(crate::context::context_middleware))
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
}
