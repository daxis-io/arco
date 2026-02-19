//! Table routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::{Extension, Query};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};

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

/// Lists tables (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
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
        (status = 200, description = "The tables list was successfully retrieved.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_tables(
    _ctx: Extension<UnityCatalogRequestContext>,
    query: Query<ListTablesQuery>,
) -> UnityCatalogResult<Json<Value>> {
    let Query(query) = query;
    let _ = (
        query.catalog_name,
        query.schema_name,
        query.max_results,
        query.page_token,
        query.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "GET /tables is scaffolded in preview; implementation is pending.".to_string(),
    })
}

/// Creates a table (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/tables",
    tag = "Tables",
    request_body = CreateTableRequestBody,
    responses(
        (status = 200, description = "The new external table was successfully created.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_tables(
    _ctx: Extension<UnityCatalogRequestContext>,
    payload: Json<CreateTableRequestBody>,
) -> UnityCatalogResult<Json<Value>> {
    let Json(payload) = payload;
    let _ = (
        payload.name,
        payload.catalog_name,
        payload.schema_name,
        payload.table_type,
        payload.data_source_format,
        payload.columns,
        payload.storage_location,
        payload.comment,
        payload.properties,
        payload.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "POST /tables is scaffolded in preview; implementation is pending.".to_string(),
    })
}
