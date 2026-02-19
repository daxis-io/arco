//! Schema routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::{Extension, Query};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct ListSchemasQuery {
    catalog_name: Option<String>,
    max_results: Option<i32>,
    page_token: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "CreateSchemaRequestBody")]
#[serde(default)]
pub(crate) struct CreateSchemaRequestBody {
    name: Option<String>,
    catalog_name: Option<String>,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

/// Lists schemas (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    get,
    path = "/schemas",
    tag = "Schemas",
    params(
        ("catalog_name" = String, Query, description = "Parent catalog for schemas of interest."),
        ("max_results" = Option<i32>, Query, description = "Maximum number of schemas to return."),
        ("page_token" = Option<String>, Query, description = "Opaque pagination token to go to next page based on previous query.")
    ),
    responses(
        (status = 200, description = "The schemas list was successfully retrieved.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_schemas(
    _ctx: Extension<UnityCatalogRequestContext>,
    query: Query<ListSchemasQuery>,
) -> UnityCatalogResult<Json<Value>> {
    let Query(query) = query;
    let _ = (
        query.catalog_name,
        query.max_results,
        query.page_token,
        query.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "GET /schemas is scaffolded in preview; implementation is pending.".to_string(),
    })
}

/// Creates a schema (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/schemas",
    tag = "Schemas",
    request_body = CreateSchemaRequestBody,
    responses(
        (status = 200, description = "The new schema was successfully created.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_schemas(
    _ctx: Extension<UnityCatalogRequestContext>,
    payload: Json<CreateSchemaRequestBody>,
) -> UnityCatalogResult<Json<Value>> {
    let Json(payload) = payload;
    let _ = (
        payload.name,
        payload.catalog_name,
        payload.comment,
        payload.properties,
        payload.storage_root,
        payload.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "POST /schemas is scaffolded in preview; implementation is pending.".to_string(),
    })
}
