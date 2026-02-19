//! Table routes for the Unity Catalog facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Path, Query, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

use arco_core::storage::WriteResult;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::preview::{self, catalog_path, schema_path, table_path, table_prefix};
use crate::state::UnityCatalogState;

/// Table route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/tables", post(post_tables).get(get_tables))
        .route("/tables/:full_name", get(get_table).delete(delete_table))
}

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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct CreateTablePayload {
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum TableTypeValue {
    Managed,
    External,
    StreamingTable,
    MaterializedView,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "UPPERCASE")]
enum DataSourceFormatValue {
    Delta,
    Csv,
    Json,
    Avro,
    Parquet,
    Orc,
    Text,
}

const VALID_TABLE_TYPES: &[&str] = &[
    "MANAGED",
    "EXTERNAL",
    "STREAMING_TABLE",
    "MATERIALIZED_VIEW",
];
const VALID_DATA_SOURCE_FORMATS: &[&str] =
    &["DELTA", "CSV", "JSON", "AVRO", "PARQUET", "ORC", "TEXT"];

#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(title = "CreateTableRequestBody")]
pub(crate) struct CreateTableRequestBody {
    name: String,
    catalog_name: String,
    schema_name: String,
    table_type: TableTypeValue,
    data_source_format: DataSourceFormatValue,
    columns: Vec<Value>,
    storage_location: String,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        page_token = ?query.page_token,
        max_results = ?query.max_results,
        "unity catalog preview list tables"
    );
    let pagination = preview::parse_pagination(
        query.page_token.as_deref(),
        query.max_results,
        preview::DEFAULT_PAGE_SIZE,
        50,
    )?;

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

    let (tables, next_page_token) = preview::read_json_page::<TableInfo>(
        &scoped_storage,
        &table_prefix(&catalog_name, &schema_name),
        "list tables",
        &pagination,
    )
    .await?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        tables = tables.len(),
        next_page_token = ?next_page_token,
        "unity catalog preview listed tables"
    );

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
    payload: Json<CreateTablePayload>,
) -> UnityCatalogResult<Json<TableInfo>> {
    let Json(payload) = payload;
    let _ = payload.extra;

    let name = preview::require_identifier(payload.name, "name")?;
    let catalog_name = preview::require_identifier(payload.catalog_name, "catalog_name")?;
    let schema_name = preview::require_identifier(payload.schema_name, "schema_name")?;
    let table_type = validate_enum_value(
        preview::require_non_empty_string(payload.table_type, "table_type")?,
        "table_type",
        VALID_TABLE_TYPES,
    )?;
    let data_source_format = validate_enum_value(
        preview::require_non_empty_string(payload.data_source_format, "data_source_format")?,
        "data_source_format",
        VALID_DATA_SOURCE_FORMATS,
    )?;
    let columns = validate_columns(preview::require_present(payload.columns, "columns")?)?;
    let storage_location =
        preview::require_non_empty_string(payload.storage_location, "storage_location")?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        table_name = %name,
        table_type = %table_type,
        data_source_format = %data_source_format,
        "unity catalog preview create table"
    );
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
        table_type: Some(table_type),
        data_source_format: Some(data_source_format),
        columns: Some(columns),
        storage_location: Some(storage_location),
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
        WriteResult::Success { .. } => {
            tracing::debug!(
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id,
                catalog_name = %catalog_name,
                schema_name = %schema_name,
                table_name = %name,
                "unity catalog preview table created"
            );
            Ok(Json(table))
        }
        WriteResult::PreconditionFailed { .. } => Err(UnityCatalogError::Conflict {
            message: format!("table already exists: {catalog_name}.{schema_name}.{name}"),
        }),
    }
}

/// `GET /tables/{full_name}` (Scope A).
///
/// # Errors
///
/// Returns an error if `full_name` is malformed or the referenced table cannot
/// be read from storage.
#[utoipa::path(
    get,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 200, description = "Get table"),
    )
)]
pub async fn get_table(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let mut iter = full_name.split('.');
    let catalog = iter.next();
    let schema = iter.next();
    let table = iter.next();
    let extra = iter.next();

    let (Some(catalog), Some(schema), Some(table), None) = (catalog, schema, table, extra) else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("invalid table full name: {full_name}"),
        });
    };

    let reader = super::common::catalog_reader(&state, &ctx)?;
    let table = reader
        .get_table_in_schema(catalog, schema, table)
        .await
        .map_err(super::common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {full_name}"),
        })?;

    let payload = json!({
        "name": table.name,
        "table_id": table.id,
        "storage_location": table.location,
        "data_source_format": table.format,
        "comment": table.description,
    });

    Ok((StatusCode::OK, Json(payload)))
}

/// `DELETE /tables/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    delete,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn delete_table(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

fn validate_enum_value(
    value: String,
    field: &str,
    valid_values: &[&str],
) -> UnityCatalogResult<String> {
    if valid_values.contains(&value.as_str()) {
        return Ok(value);
    }

    Err(UnityCatalogError::BadRequest {
        message: format!(
            "invalid {field}: expected one of {}",
            valid_values.join(", ")
        ),
    })
}

fn validate_columns(columns: Vec<Value>) -> UnityCatalogResult<Vec<Value>> {
    for (index, column) in columns.iter().enumerate() {
        if !column.is_object() {
            return Err(UnityCatalogError::BadRequest {
                message: format!("invalid columns[{index}]: expected object"),
            });
        }
    }
    Ok(columns)
}
