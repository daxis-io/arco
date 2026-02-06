//! Catalog + schema API routes.
//!
//! Provides UC-like CRUD operations for catalogs and schemas, while preserving
//! Arco's native storage model (Tier-1 Parquet snapshots).
//!
//! ## Routes
//!
//! - `POST /catalogs` - Create a catalog
//! - `GET  /catalogs` - List catalogs
//! - `GET  /catalogs/{name}` - Get catalog by name
//! - `POST /catalogs/{catalog}/schemas` - Create a schema
//! - `GET  /catalogs/{catalog}/schemas` - List schemas in a catalog
//! - `POST /catalogs/{catalog}/schemas/{schema}/tables` - Register a table in a schema
//! - `GET  /catalogs/{catalog}/schemas/{schema}/tables` - List tables in a schema
//! - `GET  /catalogs/{catalog}/schemas/{schema}/tables/{name}` - Get table by name

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use arco_catalog::idempotency::{
    CatalogOperation, IdempotencyCheck, IdempotencyStore, IdempotencyStoreImpl,
    calculate_retry_after, canonical_request_hash, check_idempotency,
};
use arco_catalog::{CatalogReader, CatalogWriter, Tier1Compactor};

use super::tables::ColumnDefinition;
use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

/// Request to create a catalog.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCatalogRequest {
    /// Catalog name (unique within workspace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

/// Catalog response.
#[derive(Debug, Serialize, ToSchema)]
pub struct CatalogResponse {
    /// Catalog ID.
    pub id: String,
    /// Catalog name.
    pub name: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
    /// Last update timestamp (ISO 8601).
    pub updated_at: String,
}

/// List catalogs response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListCatalogsResponse {
    /// List of catalogs.
    pub catalogs: Vec<CatalogResponse>,
}

/// Request to create a schema in a catalog.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSchemaRequest {
    /// Schema name (unique within catalog).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

/// Schema response.
#[derive(Debug, Serialize, ToSchema)]
pub struct SchemaResponse {
    /// Schema ID.
    pub id: String,
    /// Parent catalog name.
    pub catalog: String,
    /// Schema name.
    pub name: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
    /// Last update timestamp (ISO 8601).
    pub updated_at: String,
}

/// List schemas response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListSchemasResponse {
    /// List of schemas.
    pub schemas: Vec<SchemaResponse>,
}

/// Request to register a table in a schema.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterSchemaTableRequest {
    /// Table name (must be unique within schema).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Storage location (URI).
    pub location: Option<String>,
    /// Table format (e.g., "delta", "iceberg", "parquet").
    pub format: Option<String>,
    /// Column definitions.
    pub columns: Vec<ColumnDefinition>,
}

/// Table response (catalog + schema qualified).
#[derive(Debug, Serialize, ToSchema)]
pub struct SchemaTableResponse {
    /// Table ID.
    pub id: String,
    /// Catalog name.
    pub catalog: String,
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Storage location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Table format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
    /// Last update timestamp (ISO 8601).
    pub updated_at: String,
}

/// List tables response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListSchemaTablesResponse {
    /// List of tables.
    pub tables: Vec<SchemaTableResponse>,
}

/// Creates catalog routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/catalogs", post(create_catalog).get(list_catalogs))
        .route("/catalogs/:name", get(get_catalog))
        .route(
            "/catalogs/:catalog/schemas",
            post(create_schema).get(list_schemas),
        )
        .route(
            "/catalogs/:catalog/schemas/:schema/tables",
            post(register_table_in_schema).get(list_tables_in_schema),
        )
        .route(
            "/catalogs/:catalog/schemas/:schema/tables/:name",
            get(get_table_in_schema),
        )
}

/// Create a catalog.
///
/// POST /api/v1/catalogs
#[utoipa::path(
    post,
    path = "/api/v1/catalogs",
    tag = "catalogs",
    request_body = CreateCatalogRequest,
    responses(
        (status = 201, description = "Catalog created", body = CatalogResponse),
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
#[allow(clippy::too_many_lines)]
pub(crate) async fn create_catalog(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateCatalogRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %req.name,
        "Creating catalog"
    );

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
        IdempotencyCheck::Replay {
            entity_id,
            entity_name,
        } => {
            let reader = CatalogReader::new(storage);
            let catalog = reader
                .get_catalog(&entity_name)
                .await
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError::internal("Cached catalog not found"))?;
            let response = CatalogResponse {
                id: entity_id,
                name: catalog.name,
                description: catalog.description,
                created_at: format_timestamp(catalog.created_at),
                updated_at: format_timestamp(catalog.updated_at),
            };
            return Ok((StatusCode::CREATED, Json(response)));
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
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

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
        Json(CatalogResponse {
            id: created.id,
            name: created.name,
            description: created.description,
            created_at: format_timestamp(created.created_at),
            updated_at: format_timestamp(created.updated_at),
        }),
    ))
}

/// List catalogs.
///
/// GET /api/v1/catalogs
#[utoipa::path(
    get,
    path = "/api/v1/catalogs",
    tag = "catalogs",
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
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        "Listing catalogs"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let catalogs = reader
        .list_catalogs()
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|c| CatalogResponse {
            id: c.id,
            name: c.name,
            description: c.description,
            created_at: format_timestamp(c.created_at),
            updated_at: format_timestamp(c.updated_at),
        })
        .collect();

    Ok(Json(ListCatalogsResponse { catalogs }))
}

/// Get a catalog by name.
///
/// GET /api/v1/catalogs/{name}
#[utoipa::path(
    get,
    path = "/api/v1/catalogs/{name}",
    tag = "catalogs",
    params(
        ("name" = String, Path, description = "Catalog name")
    ),
    responses(
        (status = 200, description = "Catalog found", body = CatalogResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_catalog(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %name,
        "Getting catalog"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let catalog = reader
        .get_catalog(&name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found(format!("Catalog not found: {name}")))?;

    Ok(Json(CatalogResponse {
        id: catalog.id,
        name: catalog.name,
        description: catalog.description,
        created_at: format_timestamp(catalog.created_at),
        updated_at: format_timestamp(catalog.updated_at),
    }))
}

/// Create a schema in a catalog.
///
/// POST /api/v1/catalogs/{catalog}/schemas
#[utoipa::path(
    post,
    path = "/api/v1/catalogs/{catalog}/schemas",
    tag = "schemas",
    params(
        ("catalog" = String, Path, description = "Catalog name"),
    ),
    request_body = CreateSchemaRequest,
    responses(
        (status = 201, description = "Schema created", body = SchemaResponse),
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
#[allow(clippy::too_many_lines)]
pub(crate) async fn create_schema(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(catalog): Path<String>,
    Json(req): Json<CreateSchemaRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %catalog,
        schema = %req.name,
        "Creating schema"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let request_json = serde_json::json!({
        "catalog": &catalog,
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
        IdempotencyCheck::Replay {
            entity_id,
            entity_name,
        } => {
            let reader = CatalogReader::new(storage);
            let schema = reader
                .list_schemas(&catalog)
                .await
                .map_err(ApiError::from)?
                .into_iter()
                .find(|s| s.name == entity_name)
                .ok_or_else(|| ApiError::internal("Cached schema not found"))?;
            let response = SchemaResponse {
                id: entity_id,
                catalog,
                name: schema.name,
                description: schema.description,
                created_at: format_timestamp(schema.created_at),
                updated_at: format_timestamp(schema.updated_at),
            };
            return Ok((StatusCode::CREATED, Json(response)));
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
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

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
        .create_schema(&catalog, &req.name, req.description.as_deref(), options)
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
        Json(SchemaResponse {
            id: created.id,
            catalog,
            name: created.name,
            description: created.description,
            created_at: format_timestamp(created.created_at),
            updated_at: format_timestamp(created.updated_at),
        }),
    ))
}

/// List schemas in a catalog.
///
/// GET /api/v1/catalogs/{catalog}/schemas
#[utoipa::path(
    get,
    path = "/api/v1/catalogs/{catalog}/schemas",
    tag = "schemas",
    params(
        ("catalog" = String, Path, description = "Catalog name")
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
    Path(catalog): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %catalog,
        "Listing schemas"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let schemas = reader
        .list_schemas(&catalog)
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|ns| SchemaResponse {
            id: ns.id,
            catalog: catalog.clone(),
            name: ns.name,
            description: ns.description,
            created_at: format_timestamp(ns.created_at),
            updated_at: format_timestamp(ns.updated_at),
        })
        .collect();

    Ok(Json(ListSchemasResponse { schemas }))
}

/// Register a table in a schema.
///
/// POST /api/v1/catalogs/{catalog}/schemas/{schema}/tables
#[utoipa::path(
    post,
    path = "/api/v1/catalogs/{catalog}/schemas/{schema}/tables",
    tag = "tables",
    params(
        ("catalog" = String, Path, description = "Catalog name"),
        ("schema" = String, Path, description = "Schema name")
    ),
    request_body = RegisterSchemaTableRequest,
    responses(
        (status = 201, description = "Table registered", body = SchemaTableResponse),
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
#[allow(clippy::too_many_lines)]
pub(crate) async fn register_table_in_schema(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path((catalog, schema)): Path<(String, String)>,
    Json(req): Json<RegisterSchemaTableRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %catalog,
        schema = %schema,
        table = %req.name,
        "Registering table in schema"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let request_json = serde_json::json!({
        "catalog": &catalog,
        "schema": &schema,
        "name": &req.name,
        "description": &req.description,
        "location": &req.location,
        "format": &req.format,
        "columns": &req.columns,
    });
    let request_hash = canonical_request_hash(&request_json)
        .map_err(|e| ApiError::internal(format!("Failed to compute request hash: {e}")))?;

    let storage_arc = Arc::new(storage.clone());
    let idempotency_store = IdempotencyStoreImpl::new(storage_arc);
    let idempotency_check = check_idempotency(
        &idempotency_store,
        ctx.idempotency_key.as_deref(),
        CatalogOperation::RegisterTableInSchema,
        &request_hash,
        state.config.idempotency_stale_timeout(),
    )
    .await
    .map_err(ApiError::from)?;

    let (marker, marker_version) = match idempotency_check {
        IdempotencyCheck::NoKey => (None, None),
        IdempotencyCheck::Proceed { marker, version } => (Some(marker), Some(version)),
        IdempotencyCheck::Replay {
            entity_id,
            entity_name,
        } => {
            let reader = CatalogReader::new(storage);
            let table = reader
                .get_table_in_schema(&catalog, &schema, &entity_name)
                .await
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError::internal("Cached table not found"))?;
            let response = SchemaTableResponse {
                id: entity_id,
                catalog,
                schema,
                name: table.name,
                description: table.description,
                location: table.location,
                format: table.format,
                created_at: format_timestamp(table.created_at),
                updated_at: format_timestamp(table.updated_at),
            };
            return Ok((StatusCode::CREATED, Json(response)));
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
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

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
        .register_table_in_schema(
            &catalog,
            &schema,
            arco_catalog::RegisterTableInSchemaRequest {
                name: req.name,
                description: req.description,
                location: req.location,
                format: req.format,
                columns: req
                    .columns
                    .into_iter()
                    .map(|col| arco_catalog::ColumnDefinition {
                        name: col.name,
                        data_type: col.data_type,
                        is_nullable: col.nullable,
                        description: col.description,
                    })
                    .collect(),
            },
            options,
        )
        .await;

    if let (Some(marker), Some(version)) = (&marker, &marker_version) {
        match &create_result {
            Ok(table) => {
                let finalized = marker
                    .clone()
                    .finalize_committed(table.id.clone(), table.name.clone());
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
        Json(SchemaTableResponse {
            id: created.id,
            catalog,
            schema,
            name: created.name,
            description: created.description,
            location: created.location,
            format: created.format,
            created_at: format_timestamp(created.created_at),
            updated_at: format_timestamp(created.updated_at),
        }),
    ))
}

/// List tables in a schema.
///
/// GET /api/v1/catalogs/{catalog}/schemas/{schema}/tables
#[utoipa::path(
    get,
    path = "/api/v1/catalogs/{catalog}/schemas/{schema}/tables",
    tag = "tables",
    params(
        ("catalog" = String, Path, description = "Catalog name"),
        ("schema" = String, Path, description = "Schema name")
    ),
    responses(
        (status = 200, description = "Tables listed", body = ListSchemaTablesResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_tables_in_schema(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path((catalog, schema)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %catalog,
        schema = %schema,
        "Listing tables in schema"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let tables = reader
        .list_tables_in_schema(&catalog, &schema)
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(|t| SchemaTableResponse {
            id: t.id,
            catalog: catalog.clone(),
            schema: schema.clone(),
            name: t.name,
            description: t.description,
            location: t.location,
            format: t.format,
            created_at: format_timestamp(t.created_at),
            updated_at: format_timestamp(t.updated_at),
        })
        .collect();

    Ok(Json(ListSchemaTablesResponse { tables }))
}

/// Get a table by name in a schema.
///
/// GET /api/v1/catalogs/{catalog}/schemas/{schema}/tables/{name}
#[utoipa::path(
    get,
    path = "/api/v1/catalogs/{catalog}/schemas/{schema}/tables/{name}",
    tag = "tables",
    params(
        ("catalog" = String, Path, description = "Catalog name"),
        ("schema" = String, Path, description = "Schema name"),
        ("name" = String, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Table found", body = SchemaTableResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_table_in_schema(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path((catalog, schema, name)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        catalog = %catalog,
        schema = %schema,
        table = %name,
        "Getting table in schema"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = CatalogReader::new(storage);

    let table = reader
        .get_table_in_schema(&catalog, &schema, &name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::not_found(format!("Table not found: {catalog}.{schema}.{name}"))
        })?;

    Ok(Json(SchemaTableResponse {
        id: table.id,
        catalog,
        schema,
        name: table.name,
        description: table.description,
        location: table.location,
        format: table.format,
        created_at: format_timestamp(table.created_at),
        updated_at: format_timestamp(table.updated_at),
    }))
}

fn format_timestamp(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339()
}
