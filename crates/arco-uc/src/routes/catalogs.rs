//! Catalog routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::{Extension, Query};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct ListCatalogsQuery {
    page_token: Option<String>,
    max_results: Option<i32>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "CreateCatalogRequestBody")]
#[serde(default)]
pub(crate) struct CreateCatalogRequestBody {
    name: Option<String>,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

/// Lists catalogs (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    get,
    path = "/catalogs",
    tag = "Catalogs",
    params(
        ("page_token" = Option<String>, Query, description = "Opaque pagination token to go to next page based on previous query."),
        ("max_results" = Option<i32>, Query, description = "Maximum number of catalogs to return.")
    ),
    responses(
        (status = 200, description = "The catalog list was successfully retrieved.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_catalogs(
    _ctx: Extension<UnityCatalogRequestContext>,
    query: Query<ListCatalogsQuery>,
) -> UnityCatalogResult<Json<Value>> {
    let Query(query) = query;
    let _ = (query.page_token, query.max_results, query.extra);
    Err(UnityCatalogError::NotImplemented {
        message: "GET /catalogs is scaffolded in preview; implementation is pending.".to_string(),
    })
}

/// Creates a catalog (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/catalogs",
    tag = "Catalogs",
    request_body = CreateCatalogRequestBody,
    responses(
        (status = 200, description = "The new catalog was successfully created.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_catalogs(
    _ctx: Extension<UnityCatalogRequestContext>,
    payload: Json<CreateCatalogRequestBody>,
) -> UnityCatalogResult<Json<Value>> {
    let Json(payload) = payload;
    let _ = (
        payload.name,
        payload.comment,
        payload.properties,
        payload.storage_root,
        payload.extra,
    );
    Err(UnityCatalogError::NotImplemented {
        message: "POST /catalogs is scaffolded in preview; implementation is pending.".to_string(),
    })
}
