//! Table API routes.
//!
//! Provides CRUD operations for tables within a namespace.
//!
//! ## Routes
//!
//! - `POST   /namespaces/{ns}/tables` - Register a table
//! - `GET    /namespaces/{ns}/tables` - List tables in namespace
//! - `GET    /namespaces/{ns}/tables/{name}` - Get table by name
//! - `PUT    /namespaces/{ns}/tables/{name}` - Update table
//! - `DELETE /namespaces/{ns}/tables/{name}` - Drop table

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::error::ApiErrorBody;
use crate::server::AppState;
use arco_catalog::Tier1Compactor;

/// Request to register a table.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterTableRequest {
    /// Table name (must be unique within namespace).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Table schema (columns).
    pub columns: Vec<ColumnDefinition>,
}

/// Column definition for table registration.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub data_type: String,
    /// Whether the column is nullable.
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// Optional description.
    pub description: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Request to update a table.
#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateTableRequest {
    /// Optional new description.
    pub description: Option<String>,
}

/// Table response.
#[derive(Debug, Serialize, ToSchema)]
pub struct TableResponse {
    /// Table ID.
    pub id: String,
    /// Namespace name.
    pub namespace: String,
    /// Table name.
    pub name: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Table columns.
    pub columns: Vec<ColumnResponse>,
    /// Creation timestamp (ISO 8601).
    pub created_at: String,
    /// Last update timestamp (ISO 8601).
    pub updated_at: String,
}

/// Column response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ColumnResponse {
    /// Column ID.
    pub id: String,
    /// Column name.
    pub name: String,
    /// Column data type.
    pub data_type: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Column position (0-indexed).
    pub position: i32,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// List tables response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ListTablesResponse {
    /// List of tables.
    pub tables: Vec<TableResponse>,
}

/// Creates table routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/namespaces/:namespace/tables",
            post(register_table).get(list_tables),
        )
        .route(
            "/namespaces/:namespace/tables/:name",
            get(get_table).put(update_table).delete(drop_table),
        )
}

/// Register a table.
///
/// POST /api/v1/namespaces/{namespace}/tables
#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace}/tables",
    tag = "tables",
    params(
        ("namespace" = String, Path, description = "Namespace name")
    ),
    request_body = RegisterTableRequest,
    responses(
        (status = 201, description = "Table registered", body = TableResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Namespace not found", body = ApiErrorBody),
        (status = 409, description = "Conflict", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn register_table(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Json(req): Json<RegisterTableRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %namespace,
        table = %req.name,
        "Registering table"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    // Ensure initialized
    writer.initialize().await.map_err(ApiError::from)?;

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    // Convert columns
    let columns: Vec<arco_catalog::ColumnDefinition> = req
        .columns
        .iter()
        .map(|c| arco_catalog::ColumnDefinition {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            is_nullable: c.nullable,
            description: c.description.clone(),
        })
        .collect();

    let table = writer
        .register_table(
            arco_catalog::RegisterTableRequest {
                namespace: namespace.clone(),
                name: req.name.clone(),
                description: req.description.clone(),
                location: None,
                format: None,
                columns,
            },
            options,
        )
        .await
        .map_err(ApiError::from)?;

    // Read columns back from the published snapshot.
    let reader = arco_catalog::CatalogReader::new(storage);

    let cols = reader
        .get_columns(&table.id)
        .await
        .map_err(ApiError::from)?;

    let response = TableResponse {
        id: table.id,
        namespace,
        name: table.name,
        description: table.description,
        columns: cols
            .into_iter()
            .map(|c| ColumnResponse {
                id: c.id,
                name: c.name,
                data_type: c.data_type,
                nullable: c.is_nullable,
                position: c.ordinal,
                description: c.description,
            })
            .collect(),
        created_at: format_timestamp(table.created_at),
        updated_at: format_timestamp(table.updated_at),
    };

    Ok((StatusCode::CREATED, Json(response)))
}

/// List tables in a namespace.
///
/// GET /api/v1/namespaces/{namespace}/tables
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/tables",
    tag = "tables",
    params(
        ("namespace" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 200, description = "Tables listed", body = ListTablesResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Namespace not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_tables(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %namespace,
        "Listing tables"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    let tables = reader
        .list_tables(&namespace)
        .await
        .map_err(ApiError::from)?;

    let mut responses = Vec::new();
    for table in tables {
        let cols = reader
            .get_columns(&table.id)
            .await
            .map_err(ApiError::from)?;

        responses.push(TableResponse {
            id: table.id,
            namespace: namespace.clone(),
            name: table.name,
            description: table.description,
            columns: cols
                .into_iter()
                .map(|c| ColumnResponse {
                    id: c.id,
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.is_nullable,
                    position: c.ordinal,
                    description: c.description,
                })
                .collect(),
            created_at: format_timestamp(table.created_at),
            updated_at: format_timestamp(table.updated_at),
        });
    }

    Ok(Json(ListTablesResponse { tables: responses }))
}

/// Get a table by name.
///
/// GET /api/v1/namespaces/{namespace}/tables/{name}
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/tables/{name}",
    tag = "tables",
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("name" = String, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Table found", body = TableResponse),
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
    Path((namespace, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %namespace,
        table = %name,
        "Getting table"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    let table = reader
        .get_table(&namespace, &name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found(format!("Table not found: {namespace}.{name}")))?;

    let cols = reader
        .get_columns(&table.id)
        .await
        .map_err(ApiError::from)?;

    let response = TableResponse {
        id: table.id,
        namespace,
        name: table.name,
        description: table.description,
        columns: cols
            .into_iter()
            .map(|c| ColumnResponse {
                id: c.id,
                name: c.name,
                data_type: c.data_type,
                nullable: c.is_nullable,
                position: c.ordinal,
                description: c.description,
            })
            .collect(),
        created_at: format_timestamp(table.created_at),
        updated_at: format_timestamp(table.updated_at),
    };

    Ok(Json(response))
}

/// Update a table.
///
/// PUT /api/v1/namespaces/{namespace}/tables/{name}
#[utoipa::path(
    put,
    path = "/api/v1/namespaces/{namespace}/tables/{name}",
    tag = "tables",
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("name" = String, Path, description = "Table name")
    ),
    request_body = UpdateTableRequest,
    responses(
        (status = 200, description = "Table updated", body = TableResponse),
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
pub(crate) async fn update_table(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Json(req): Json<UpdateTableRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %namespace,
        table = %name,
        "Updating table"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    let patch = arco_catalog::TablePatch {
        description: req.description.map(Some),
        ..Default::default()
    };

    let table = writer
        .update_table(&namespace, &name, patch, options)
        .await
        .map_err(ApiError::from)?;

    let reader = arco_catalog::CatalogReader::new(storage);

    let cols = reader
        .get_columns(&table.id)
        .await
        .map_err(ApiError::from)?;

    let response = TableResponse {
        id: table.id,
        namespace,
        name: table.name,
        description: table.description,
        columns: cols
            .into_iter()
            .map(|c| ColumnResponse {
                id: c.id,
                name: c.name,
                data_type: c.data_type,
                nullable: c.is_nullable,
                position: c.ordinal,
                description: c.description,
            })
            .collect(),
        created_at: format_timestamp(table.created_at),
        updated_at: format_timestamp(table.updated_at),
    };

    Ok(Json(response))
}

/// Drop a table.
///
/// DELETE /api/v1/namespaces/{namespace}/tables/{name}
#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace}/tables/{name}",
    tag = "tables",
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("name" = String, Path, description = "Table name")
    ),
    responses(
        (status = 204, description = "Table dropped"),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn drop_table(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        namespace = %namespace,
        table = %name,
        "Dropping table"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = state
        .sync_compactor()
        .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
    let writer = arco_catalog::CatalogWriter::new(storage).with_sync_compactor(compactor);

    let options = arco_catalog::write_options::WriteOptions::default()
        .with_actor(format!("api:{}", ctx.tenant))
        .with_request_id(&ctx.request_id);

    let options = if let Some(key) = ctx.idempotency_key.as_ref() {
        options.with_idempotency_key(key)
    } else {
        options
    };

    writer
        .drop_table(&namespace, &name, options)
        .await
        .map_err(ApiError::from)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Format a millisecond timestamp as ISO 8601.
fn format_timestamp(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .map_or_else(|| millis.to_string(), |dt| dt.to_rfc3339())
}
