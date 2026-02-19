//! Catalog routes for the Unity Catalog facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Query, State};
use axum::http::Method;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use arco_core::storage::WriteResult;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::preview::{self, catalog_path};
use crate::state::UnityCatalogState;

/// Catalog route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/catalogs", post(post_catalogs).get(get_catalogs))
        .route(
            "/catalogs/:name",
            get(get_catalog)
                .patch(update_catalog)
                .delete(delete_catalog),
        )
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct ListCatalogsQuery {
    page_token: Option<String>,
    max_results: Option<i32>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct CreateCatalogPayload {
    name: Option<String>,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(title = "CreateCatalogRequestBody")]
pub(crate) struct CreateCatalogRequestBody {
    name: String,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        page_token = ?query.page_token,
        max_results = ?query.max_results,
        "unity catalog preview list catalogs"
    );

    let pagination = preview::parse_pagination(
        query.page_token.as_deref(),
        query.max_results,
        preview::DEFAULT_PAGE_SIZE,
        1000,
    )?;

    let scoped_storage = ctx.scoped_storage(state.storage.clone())?;
    let (catalogs, next_page_token) = preview::read_json_page::<CatalogInfo>(
        &scoped_storage,
        preview::CATALOGS_PREFIX,
        "list catalogs",
        &pagination,
    )
    .await?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalogs = catalogs.len(),
        next_page_token = ?next_page_token,
        "unity catalog preview listed catalogs"
    );

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
    payload: Json<CreateCatalogPayload>,
) -> UnityCatalogResult<Json<CatalogInfo>> {
    let Json(payload) = payload;
    let _ = payload.extra;

    let name = preview::require_identifier(payload.name, "name")?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %name,
        "unity catalog preview create catalog"
    );
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
        WriteResult::Success { .. } => {
            tracing::debug!(
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id,
                catalog_name = %name,
                "unity catalog preview catalog created"
            );
            Ok(Json(catalog))
        }
        WriteResult::PreconditionFailed { .. } => Err(UnityCatalogError::Conflict {
            message: format!("catalog already exists: {name}"),
        }),
    }
}

/// `GET /catalogs/{name}` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn get_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// `PATCH /catalogs/{name}` (known UC operation; currently unsupported).
#[utoipa::path(
    patch,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn update_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// `DELETE /catalogs/{name}` (known UC operation; currently unsupported).
#[utoipa::path(
    delete,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn delete_catalog(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}
