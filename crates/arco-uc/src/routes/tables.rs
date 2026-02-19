//! Table routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::{Extension, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use arco_core::storage::WriteResult;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::preview::{self, catalog_path, schema_path, table_path, table_prefix};
use crate::state::UnityCatalogState;

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct ListTablesQuery {
    catalog_name: Option<String>,
    schema_name: Option<String>,
    max_results: Option<i32>,
    page_token: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "CreateTableRequestBody")]
#[serde(default)]
pub(crate) struct CreateTableRequestBody {
    name: Option<String>,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_type: Option<String>,
    data_source_format: Option<String>,
    columns: Option<Vec<Value>>,
    storage_location: Option<String>,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

/// Response payload for a table object.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct TableInfo {
    name: String,
    catalog_name: String,
    schema_name: String,
    full_name: String,
    table_type: Option<String>,
    data_source_format: Option<String>,
    columns: Option<Vec<Value>>,
    storage_location: Option<String>,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
}

/// Response payload for listing tables.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub(crate) struct ListTablesResponse {
    tables: Vec<TableInfo>,
    next_page_token: Option<String>,
}

/// Lists tables.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, parent resources are missing,
/// or storage access fails.
#[utoipa::path(
    get,
    path = "/tables",
    tag = "Tables",
    params(
        ("catalog_name" = String, Query, description = "Name of parent catalog for tables of interest."),
        ("schema_name" = String, Query, description = "Parent schema of tables."),
        ("max_results" = Option<i32>, Query, description = "Maximum number of tables to return."),
        ("page_token" = Option<String>, Query, description = "Opaque token to send for the next page of results (pagination).")
    ),
    responses(
        (status = 200, description = "The tables list was successfully retrieved.", body = ListTablesResponse),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_tables(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Query<ListTablesQuery>,
) -> UnityCatalogResult<Json<ListTablesResponse>> {
    let Query(query) = query;
    let _ = &query.extra;

    let catalog_name = preview::require_identifier(query.catalog_name, "catalog_name")?;
    let schema_name = preview::require_identifier(query.schema_name, "schema_name")?;
    let scoped_storage = ctx.scoped_storage(state.storage.clone())?;

    let catalog_exists = preview::object_exists(
        &scoped_storage,
        &catalog_path(&catalog_name),
        "check catalog",
    )
    .await?;
    if !catalog_exists {
        return Err(UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog_name}"),
        });
    }

    let schema_exists = preview::object_exists(
        &scoped_storage,
        &schema_path(&catalog_name, &schema_name),
        "check schema",
    )
    .await?;
    if !schema_exists {
        return Err(UnityCatalogError::NotFound {
            message: format!("schema not found: {catalog_name}.{schema_name}"),
        });
    }

    let tables = preview::read_json_list::<TableInfo>(
        &scoped_storage,
        &table_prefix(&catalog_name, &schema_name),
        "list tables",
    )
    .await?;
    let (tables, next_page_token) =
        preview::paginate(tables, query.page_token.as_deref(), query.max_results)?;

    Ok(Json(ListTablesResponse {
        tables,
        next_page_token,
    }))
}

/// Creates a table.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, parent resources are missing,
/// table already exists, or storage access fails.
#[utoipa::path(
    post,
    path = "/tables",
    tag = "Tables",
    request_body = CreateTableRequestBody,
    responses(
        (status = 200, description = "The new external table was successfully created.", body = TableInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_tables(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    payload: Json<CreateTableRequestBody>,
) -> UnityCatalogResult<Json<TableInfo>> {
    let Json(payload) = payload;
    let _ = payload.extra;

    let name = preview::require_identifier(payload.name, "name")?;
    let catalog_name = preview::require_identifier(payload.catalog_name, "catalog_name")?;
    let schema_name = preview::require_identifier(payload.schema_name, "schema_name")?;
    let scoped_storage = ctx.scoped_storage(state.storage.clone())?;

    let catalog_exists = preview::object_exists(
        &scoped_storage,
        &catalog_path(&catalog_name),
        "check catalog",
    )
    .await?;
    if !catalog_exists {
        return Err(UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog_name}"),
        });
    }

    let schema_exists = preview::object_exists(
        &scoped_storage,
        &schema_path(&catalog_name, &schema_name),
        "check schema",
    )
    .await?;
    if !schema_exists {
        return Err(UnityCatalogError::NotFound {
            message: format!("schema not found: {catalog_name}.{schema_name}"),
        });
    }

    let table = TableInfo {
        name: name.clone(),
        catalog_name: catalog_name.clone(),
        schema_name: schema_name.clone(),
        full_name: format!("{catalog_name}.{schema_name}.{name}"),
        table_type: payload.table_type,
        data_source_format: payload.data_source_format,
        columns: payload.columns,
        storage_location: payload.storage_location,
        comment: payload.comment,
        properties: payload.properties,
    };

    let write_result = preview::write_json_if_absent(
        &scoped_storage,
        &table_path(&catalog_name, &schema_name, &name),
        &table,
        "create table",
    )
    .await?;

    match write_result {
        WriteResult::Success { .. } => Ok(Json(table)),
        WriteResult::PreconditionFailed { .. } => Err(UnityCatalogError::Conflict {
            message: format!("table already exists: {catalog_name}.{schema_name}.{name}"),
        }),
    }
}
