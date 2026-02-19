//! Catalog routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::{Extension, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use arco_core::storage::WriteResult;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::preview::{self, catalog_path};
use crate::state::UnityCatalogState;

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

/// Response payload for a catalog object.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct CatalogInfo {
    name: String,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
}

/// Response payload for listing catalogs.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub(crate) struct ListCatalogsResponse {
    catalogs: Vec<CatalogInfo>,
    next_page_token: Option<String>,
}

/// Lists catalogs.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails or storage access fails.
#[utoipa::path(
    get,
    path = "/catalogs",
    tag = "Catalogs",
    params(
        ("page_token" = Option<String>, Query, description = "Opaque pagination token to go to next page based on previous query."),
        ("max_results" = Option<i32>, Query, description = "Maximum number of catalogs to return.")
    ),
    responses(
        (status = 200, description = "The catalog list was successfully retrieved.", body = ListCatalogsResponse),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_catalogs(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Query<ListCatalogsQuery>,
) -> UnityCatalogResult<Json<ListCatalogsResponse>> {
    let Query(query) = query;
    let _ = &query.extra;

    let scoped_storage = ctx.scoped_storage(state.storage.clone())?;
    let catalogs = preview::read_json_list::<CatalogInfo>(
        &scoped_storage,
        preview::CATALOGS_PREFIX,
        "list catalogs",
    )
    .await?;
    let (catalogs, next_page_token) =
        preview::paginate(catalogs, query.page_token.as_deref(), query.max_results)?;

    Ok(Json(ListCatalogsResponse {
        catalogs,
        next_page_token,
    }))
}

/// Creates a catalog.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, the catalog exists, or
/// storage access fails.
#[utoipa::path(
    post,
    path = "/catalogs",
    tag = "Catalogs",
    request_body = CreateCatalogRequestBody,
    responses(
        (status = 200, description = "The new catalog was successfully created.", body = CatalogInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_catalogs(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    payload: Json<CreateCatalogRequestBody>,
) -> UnityCatalogResult<Json<CatalogInfo>> {
    let Json(payload) = payload;
    let _ = payload.extra;

    let name = preview::require_identifier(payload.name, "name")?;
    let catalog = CatalogInfo {
        name: name.clone(),
        comment: payload.comment,
        properties: payload.properties,
        storage_root: payload.storage_root,
    };

    let scoped_storage = ctx.scoped_storage(state.storage.clone())?;
    let write_result = preview::write_json_if_absent(
        &scoped_storage,
        &catalog_path(&name),
        &catalog,
        "create catalog",
    )
    .await?;

    match write_result {
        WriteResult::Success { .. } => Ok(Json(catalog)),
        WriteResult::PreconditionFailed { .. } => Err(UnityCatalogError::Conflict {
            message: format!("catalog already exists: {name}"),
        }),
    }
}
