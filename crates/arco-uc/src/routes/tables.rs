//! Table routes for the Unity Catalog facade.
//!
//! These handlers expose UC-shaped table operations over Arco's authoritative
//! catalog ledger and manifest-published snapshot path.

use arco_catalog::{CatalogError, CatalogReader};
use arco_catalog::{ColumnDefinition, RegisterTableInSchemaRequest};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::{common, preview};
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

fn table_info_base(
    catalog_name: &str,
    schema_name: &str,
    table: arco_catalog::writer::Table,
) -> TableInfo {
    TableInfo {
        name: table.name.clone(),
        catalog_name: catalog_name.to_string(),
        schema_name: schema_name.to_string(),
        full_name: format!("{catalog_name}.{schema_name}.{}", table.name),
        table_type: None,
        data_source_format: table.format.map(|format| format.to_ascii_uppercase()),
        columns: None,
        storage_location: table.location,
        comment: table.description,
        properties: None,
    }
}

fn column_value(column: arco_catalog::writer::Column) -> Value {
    let data_type = column.data_type.clone();
    let mut value = json!({
        "name": column.name,
        "type_text": data_type,
        "type_name": column.data_type,
        "position": column.ordinal,
        "nullable": column.is_nullable,
    });
    if let Some(comment) = column.description {
        value["comment"] = Value::String(comment);
    }
    value
}

async fn table_info_with_columns(
    reader: &CatalogReader,
    catalog_name: &str,
    schema_name: &str,
    table: arco_catalog::writer::Table,
) -> Result<TableInfo, CatalogError> {
    let columns = reader
        .get_columns(&table.id)
        .await?
        .into_iter()
        .map(column_value)
        .collect::<Vec<_>>();
    let mut info = table_info_base(catalog_name, schema_name, table);
    info.columns = Some(columns);
    Ok(info)
}

fn paginate_tables(
    tables: Vec<TableInfo>,
    pagination: &preview::Pagination,
) -> (Vec<TableInfo>, Option<String>) {
    let start = pagination.start();
    if start >= tables.len() {
        return (Vec::new(), None);
    }

    let end = start.saturating_add(pagination.limit()).min(tables.len());
    let next_page_token = (end < tables.len()).then(|| end.to_string());
    (tables[start..end].to_vec(), next_page_token)
}

fn unsupported_table_properties(
    properties: &Option<BTreeMap<String, String>>,
) -> UnityCatalogResult<()> {
    if properties.is_some() {
        return Err(UnityCatalogError::NotImplemented {
            message: "operation not supported: table properties are not authoritative in Arco yet"
                .to_string(),
        });
    }
    Ok(())
}

fn column_definitions(columns: &[Value]) -> UnityCatalogResult<Vec<ColumnDefinition>> {
    columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            let name = column.get("name").and_then(Value::as_str).ok_or_else(|| {
                UnityCatalogError::BadRequest {
                    message: format!("invalid columns[{ordinal}].name: expected string"),
                }
            })?;
            let data_type = column
                .get("type_text")
                .or_else(|| column.get("type_name"))
                .or_else(|| column.get("type"))
                .and_then(Value::as_str)
                .ok_or_else(|| UnityCatalogError::BadRequest {
                    message: format!(
                        "invalid columns[{ordinal}]: expected one of type_text, type_name, or type"
                    ),
                })?;
            Ok(ColumnDefinition {
                name: name.to_string(),
                data_type: data_type.to_string(),
                is_nullable: column
                    .get("nullable")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                ordinal: ordinal as i32,
                description: column
                    .get("comment")
                    .or_else(|| column.get("description"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect()
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
        "unity catalog list tables from authoritative catalog state"
    );
    let pagination = preview::parse_pagination(
        query.page_token.as_deref(),
        query.max_results,
        preview::DEFAULT_PAGE_SIZE,
        50,
    )?;

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let reader = CatalogReader::new(writer.storage().clone());
    let mut tables = reader
        .list_tables_in_schema(&catalog_name, &schema_name)
        .await
        .map_err(common::map_catalog_error)?
        .into_iter()
        .map(|table| table_info_base(&catalog_name, &schema_name, table))
        .collect::<Vec<_>>();
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    let (tables, next_page_token) = paginate_tables(tables, &pagination);
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        tables = tables.len(),
        next_page_token = ?next_page_token,
        "unity catalog listed tables from authoritative catalog state"
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
    unsupported_table_properties(&payload.properties)?;
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
        "unity catalog create table on authoritative catalog state"
    );

    let authoritative_columns = column_definitions(&columns)?;
    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let table = writer
        .register_table_in_schema(
            &catalog_name,
            &schema_name,
            RegisterTableInSchemaRequest {
                name: name.clone(),
                description: payload.comment.clone(),
                location: Some(storage_location.clone()),
                format: Some(data_source_format.to_ascii_lowercase()),
                columns: authoritative_columns,
            },
            common::writer_options(&ctx),
        )
        .await
        .map_err(common::map_catalog_error)?;

    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        table_name = %name,
        "unity catalog created table on authoritative catalog state"
    );

    let mut response = table_info_base(&catalog_name, &schema_name, table);
    response.table_type = Some(table_type);
    response.columns = Some(columns);
    Ok(Json(response))
}

/// Gets a table by full name (`catalog.schema.table`).
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
        (status = 200, description = "The table was successfully retrieved.", body = TableInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_table(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
) -> UnityCatalogResult<Json<TableInfo>> {
    let (catalog_name, schema_name, table_name) = parse_table_full_name(&full_name)?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        table_name = %table_name,
        "unity catalog get table from authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let reader = CatalogReader::new(writer.storage().clone());
    let table = reader
        .get_table_in_schema(&catalog_name, &schema_name, &table_name)
        .await
        .map_err(common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {catalog_name}.{schema_name}.{table_name}"),
        })?;
    let table = table_info_with_columns(&reader, &catalog_name, &schema_name, table)
        .await
        .map_err(common::map_catalog_error)?;
    Ok(Json(table))
}

/// Deletes a table.
#[utoipa::path(
    delete,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 200, description = "The table was successfully deleted."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 501, description = "Not implemented.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn delete_table(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let (catalog_name, schema_name, table_name) = parse_table_full_name(&full_name)?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        table_name = %table_name,
        "unity catalog delete table from authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    writer
        .drop_table_in_schema_transaction(
            &catalog_name,
            &schema_name,
            &table_name,
            common::writer_options(&ctx),
        )
        .await
        .map_err(common::map_catalog_error)?;
    Ok((StatusCode::OK, Json(json!({}))))
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

fn parse_table_full_name(full_name: &str) -> UnityCatalogResult<(String, String, String)> {
    let mut parts = full_name.split('.');
    let catalog_name = parts.next();
    let schema_name = parts.next();
    let table_name = parts.next();
    let extra = parts.next();

    let (Some(catalog_name), Some(schema_name), Some(table_name), None) =
        (catalog_name, schema_name, table_name, extra)
    else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("invalid table full name: {full_name}"),
        });
    };

    let catalog_name = preview::require_identifier(Some(catalog_name.to_string()), "catalog_name")?;
    let schema_name = preview::require_identifier(Some(schema_name.to_string()), "schema_name")?;
    let table_name = preview::require_identifier(Some(table_name.to_string()), "name")?;
    Ok((catalog_name, schema_name, table_name))
}
