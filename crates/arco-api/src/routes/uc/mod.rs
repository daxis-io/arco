//! Unity Catalog (UC) compatibility facade.
//!
//! This module exposes a UC-like REST surface area while translating requests
//! into Arco-native Tier-1 operations (`arco-catalog` Parquet snapshots).

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use arco_catalog::idempotency::{
    CatalogOperation, IdempotencyCheck, IdempotencyStore, IdempotencyStoreImpl,
    calculate_retry_after, canonical_request_hash, check_idempotency,
};
use arco_catalog::{CatalogReader, CatalogWriter, Tier1Compactor};

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

/// Builds the UC facade router.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/catalogs", get(list_catalogs).post(create_catalog))
        .route("/schemas", get(list_schemas).post(create_schema))
        .route("/tables", get(list_tables))
        .route("/tables/:full_name", get(get_table))
}

/// UC catalog info (subset).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UcCatalogInfo {
    /// Catalog name.
    pub name: String,
    /// Optional comment/description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
    /// Update timestamp (ms since epoch).
    pub updated_at: i64,
}

/// UC schema info (subset).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UcSchemaInfo {
    /// Catalog name.
    pub catalog_name: String,
    /// Schema name.
    pub name: String,
    /// Full name (`catalog.schema`).
    pub full_name: String,
    /// Optional comment/description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
    /// Update timestamp (ms since epoch).
    pub updated_at: i64,
}

/// UC table info (subset).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UcTableInfo {
    /// Catalog name.
    pub catalog_name: String,
    /// Schema name.
    pub schema_name: String,
    /// Table name.
    pub name: String,
    /// Full name (`catalog.schema.table`).
    pub full_name: String,
    /// Optional comment/description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Storage location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_location: Option<String>,
    /// Table format (e.g., "delta", "iceberg").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source_format: Option<String>,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
    /// Update timestamp (ms since epoch).
    pub updated_at: i64,
}

/// List catalogs response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListCatalogsResponse {
    /// Catalogs.
    pub catalogs: Vec<UcCatalogInfo>,
}

/// List schemas response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListSchemasResponse {
    /// Schemas.
    pub schemas: Vec<UcSchemaInfo>,
}

/// List tables response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListTablesResponse {
    /// Tables.
    pub tables: Vec<UcTableInfo>,
}

/// Create catalog request (subset).
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCatalogRequest {
    /// Catalog name.
    pub name: String,
    /// Optional comment/description.
    #[serde(alias = "comment")]
    pub description: Option<String>,
}

/// Create schema request (subset).
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSchemaRequest {
    /// Catalog name.
    pub catalog_name: String,
    /// Schema name.
    pub name: String,
    /// Optional comment/description.
    #[serde(alias = "comment")]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListSchemasQuery {
    pub catalog_name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListTablesQuery {
    pub catalog_name: String,
    pub schema_name: String,
}

/// List catalogs.
///
/// GET /_uc/api/2.1/unity-catalog/catalogs
#[utoipa::path(
    get,
    path = "/_uc/api/2.1/unity-catalog/catalogs",
    tag = "uc",
    responses(
        (status = 200, description = "Catalogs listed", body = ListCatalogsResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_catalogs(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let catalogs = reader
        .list_catalogs()
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|c| UcCatalogInfo {
            name: c.name,
            comment: c.description,
            created_at: c.created_at,
            updated_at: c.updated_at,
        })
        .collect();

    Ok(Json(ListCatalogsResponse { catalogs }))
}

/// Create a catalog.
///
/// POST /_uc/api/2.1/unity-catalog/catalogs
#[utoipa::path(
    post,
    path = "/_uc/api/2.1/unity-catalog/catalogs",
    tag = "uc",
    request_body = CreateCatalogRequest,
    responses(
        (status = 201, description = "Catalog created", body = UcCatalogInfo),
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
pub(crate) async fn create_catalog(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateCatalogRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let request_json = serde_json::json!({
        "name": &req.name,
        "description": &req.description
    });
    let request_hash = canonical_request_hash(&request_json)
        .map_err(|e| ApiError::internal(format!("Failed to compute request hash: {e}")))?;

    let storage_arc = Arc::new(storage.clone());
    let idempotency_store = IdempotencyStoreImpl::new(storage_arc);
    let idempotency_check = check_idempotency(
        &idempotency_store,
        ctx.idempotency_key.as_deref(),
        CatalogOperation::CreateCatalog,
        &request_hash,
        state.config.idempotency_stale_timeout(),
    )
    .await
    .map_err(ApiError::from)?;

    let (marker, marker_version) = match idempotency_check {
        IdempotencyCheck::NoKey => (None, None),
        IdempotencyCheck::Proceed { marker, version } => (Some(marker), Some(version)),
        IdempotencyCheck::Replay { entity_name, .. } => {
            let reader = CatalogReader::new(storage);
            let catalog = reader
                .get_catalog(&entity_name)
                .await
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError::internal("Cached catalog not found"))?;
            return Ok((
                StatusCode::CREATED,
                Json(UcCatalogInfo {
                    name: catalog.name,
                    comment: catalog.description,
                    created_at: catalog.created_at,
                    updated_at: catalog.updated_at,
                }),
            ));
        }
        IdempotencyCheck::Conflict => {
            return Err(ApiError::conflict(
                "Idempotency-Key already used with different request body",
            ));
        }
        IdempotencyCheck::PreviousFailed {
            http_status,
            message,
        } => {
            return Err(ApiError::from_status_and_message(http_status, message));
        }
        IdempotencyCheck::InProgress { started_at } => {
            let retry_after =
                calculate_retry_after(started_at, state.config.idempotency_stale_timeout());
            return Err(ApiError::conflict_in_progress(retry_after));
        }
    };

    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);

    writer.initialize().await.map_err(ApiError::from)?;

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    let create_result = writer
        .create_catalog(&req.name, req.description.as_deref(), options)
        .await;

    if let (Some(marker), Some(version)) = (&marker, &marker_version) {
        match &create_result {
            Ok(catalog) => {
                let finalized = marker
                    .clone()
                    .finalize_committed(catalog.id.clone(), catalog.name.clone());
                if let Err(e) = idempotency_store.finalize(&finalized, version).await {
                    tracing::warn!(
                        idempotency_key = %marker.idempotency_key,
                        operation = ?marker.operation,
                        error = %e,
                        "Failed to finalize idempotency marker as committed"
                    );
                }
            }
            Err(e) => {
                if let Some(status) = e.http_status_code() {
                    if (400..500).contains(&status) {
                        let finalized = marker.clone().finalize_failed(status, e.to_string());
                        if let Err(fin_err) = idempotency_store.finalize(&finalized, version).await
                        {
                            tracing::warn!(
                                idempotency_key = %marker.idempotency_key,
                                operation = ?marker.operation,
                                error = %fin_err,
                                "Failed to finalize idempotency marker as failed"
                            );
                        }
                    }
                }
            }
        }
    }

    let created = create_result.map_err(ApiError::from)?;

    Ok((
        StatusCode::CREATED,
        Json(UcCatalogInfo {
            name: created.name,
            comment: created.description,
            created_at: created.created_at,
            updated_at: created.updated_at,
        }),
    ))
}

/// List schemas.
///
/// GET /_uc/api/2.1/unity-catalog/schemas?catalog_name=...
#[utoipa::path(
    get,
    path = "/_uc/api/2.1/unity-catalog/schemas",
    tag = "uc",
    params(
        ("catalog_name" = String, Query, description = "Catalog name")
    ),
    responses(
        (status = 200, description = "Schemas listed", body = ListSchemasResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_schemas(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListSchemasQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let schemas = reader
        .list_schemas(&query.catalog_name)
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|s| UcSchemaInfo {
            catalog_name: query.catalog_name.clone(),
            name: s.name.clone(),
            full_name: format!("{}.{}", query.catalog_name, s.name),
            comment: s.description,
            created_at: s.created_at,
            updated_at: s.updated_at,
        })
        .collect();

    Ok(Json(ListSchemasResponse { schemas }))
}

/// Create a schema.
///
/// POST /_uc/api/2.1/unity-catalog/schemas
#[utoipa::path(
    post,
    path = "/_uc/api/2.1/unity-catalog/schemas",
    tag = "uc",
    request_body = CreateSchemaRequest,
    responses(
        (status = 201, description = "Schema created", body = UcSchemaInfo),
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
pub(crate) async fn create_schema(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateSchemaRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let request_json = serde_json::json!({
        "catalog_name": &req.catalog_name,
        "name": &req.name,
        "description": &req.description
    });
    let request_hash = canonical_request_hash(&request_json)
        .map_err(|e| ApiError::internal(format!("Failed to compute request hash: {e}")))?;

    let storage_arc = Arc::new(storage.clone());
    let idempotency_store = IdempotencyStoreImpl::new(storage_arc);
    let idempotency_check = check_idempotency(
        &idempotency_store,
        ctx.idempotency_key.as_deref(),
        CatalogOperation::CreateSchema,
        &request_hash,
        state.config.idempotency_stale_timeout(),
    )
    .await
    .map_err(ApiError::from)?;

    let (marker, marker_version) = match idempotency_check {
        IdempotencyCheck::NoKey => (None, None),
        IdempotencyCheck::Proceed { marker, version } => (Some(marker), Some(version)),
        IdempotencyCheck::Replay { entity_name, .. } => {
            let reader = CatalogReader::new(storage);
            let schema = reader
                .list_schemas(&req.catalog_name)
                .await
                .map_err(ApiError::from)?
                .into_iter()
                .find(|s| s.name == entity_name)
                .ok_or_else(|| ApiError::internal("Cached schema not found"))?;
            return Ok((
                StatusCode::CREATED,
                Json(UcSchemaInfo {
                    catalog_name: req.catalog_name.clone(),
                    name: schema.name.clone(),
                    full_name: format!("{}.{}", req.catalog_name, schema.name),
                    comment: schema.description,
                    created_at: schema.created_at,
                    updated_at: schema.updated_at,
                }),
            ));
        }
        IdempotencyCheck::Conflict => {
            return Err(ApiError::conflict(
                "Idempotency-Key already used with different request body",
            ));
        }
        IdempotencyCheck::PreviousFailed {
            http_status,
            message,
        } => {
            return Err(ApiError::from_status_and_message(http_status, message));
        }
        IdempotencyCheck::InProgress { started_at } => {
            let retry_after =
                calculate_retry_after(started_at, state.config.idempotency_stale_timeout());
            return Err(ApiError::conflict_in_progress(retry_after));
        }
    };

    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);

    writer.initialize().await.map_err(ApiError::from)?;

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    let create_result = writer
        .create_schema(
            &req.catalog_name,
            &req.name,
            req.description.as_deref(),
            options,
        )
        .await;

    if let (Some(marker), Some(version)) = (&marker, &marker_version) {
        match &create_result {
            Ok(schema) => {
                let finalized = marker
                    .clone()
                    .finalize_committed(schema.id.clone(), schema.name.clone());
                if let Err(e) = idempotency_store.finalize(&finalized, version).await {
                    tracing::warn!(
                        idempotency_key = %marker.idempotency_key,
                        operation = ?marker.operation,
                        error = %e,
                        "Failed to finalize idempotency marker as committed"
                    );
                }
            }
            Err(e) => {
                if let Some(status) = e.http_status_code() {
                    if (400..500).contains(&status) {
                        let finalized = marker.clone().finalize_failed(status, e.to_string());
                        if let Err(fin_err) = idempotency_store.finalize(&finalized, version).await
                        {
                            tracing::warn!(
                                idempotency_key = %marker.idempotency_key,
                                operation = ?marker.operation,
                                error = %fin_err,
                                "Failed to finalize idempotency marker as failed"
                            );
                        }
                    }
                }
            }
        }
    }

    let created = create_result.map_err(ApiError::from)?;

    Ok((
        StatusCode::CREATED,
        Json(UcSchemaInfo {
            catalog_name: req.catalog_name.clone(),
            name: created.name.clone(),
            full_name: format!("{}.{}", req.catalog_name, created.name),
            comment: created.description,
            created_at: created.created_at,
            updated_at: created.updated_at,
        }),
    ))
}

/// List tables.
///
/// GET /_uc/api/2.1/unity-catalog/tables?catalog_name=...&schema_name=...
#[utoipa::path(
    get,
    path = "/_uc/api/2.1/unity-catalog/tables",
    tag = "uc",
    params(
        ("catalog_name" = String, Query, description = "Catalog name"),
        ("schema_name" = String, Query, description = "Schema name")
    ),
    responses(
        (status = 200, description = "Tables listed", body = ListTablesResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_tables(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListTablesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let tables = reader
        .list_tables_in_schema(&query.catalog_name, &query.schema_name)
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|t| UcTableInfo {
            catalog_name: query.catalog_name.clone(),
            schema_name: query.schema_name.clone(),
            name: t.name.clone(),
            full_name: format!("{}.{}.{}", query.catalog_name, query.schema_name, t.name),
            comment: t.description,
            storage_location: t.location,
            data_source_format: t.format,
            created_at: t.created_at,
            updated_at: t.updated_at,
        })
        .collect();

    Ok(Json(ListTablesResponse { tables }))
}

/// Get a table by full name.
///
/// GET /_uc/api/2.1/unity-catalog/tables/{full_name}
#[utoipa::path(
    get,
    path = "/_uc/api/2.1/unity-catalog/tables/{full_name}",
    tag = "uc",
    params(
        ("full_name" = String, Path, description = "Table full name (catalog.schema.table)")
    ),
    responses(
        (status = 200, description = "Table found", body = UcTableInfo),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_table(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(full_name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mut parts = full_name.splitn(3, '.');
    let catalog = parts.next().unwrap_or_default();
    let schema = parts.next().unwrap_or_default();
    let table = parts.next().unwrap_or_default();

    if catalog.is_empty() || schema.is_empty() || table.is_empty() {
        return Err(ApiError::bad_request(
            "full_name must be in the form catalog.schema.table",
        ));
    }

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let table = reader
        .get_table_in_schema(catalog, schema, table)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found(format!("Table not found: {full_name}")))?;

    Ok(Json(UcTableInfo {
        catalog_name: catalog.to_string(),
        schema_name: schema.to_string(),
        name: table.name.clone(),
        full_name,
        comment: table.description,
        storage_location: table.location,
        data_source_format: table.format,
        created_at: table.created_at,
        updated_at: table.updated_at,
    }))
}
