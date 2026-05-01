//! Catalog routes for the Unity Catalog facade.
//!
//! These handlers expose UC-shaped catalog operations over Arco's authoritative
//! catalog ledger and manifest-published snapshot path.

use arco_catalog::writer::CatalogPatch;
use arco_catalog::{CatalogError, CatalogReader};
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

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct DeleteCatalogQuery {
    force: Option<bool>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(title = "CreateCatalogRequestBody")]
pub(crate) struct CreateCatalogRequestBody {
    name: String,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct UpdateCatalogPayload {
    #[serde(default, deserialize_with = "common::deserialize_nullable_patch_field")]
    comment: Option<Option<String>>,
    #[serde(default, deserialize_with = "common::deserialize_nullable_patch_field")]
    properties: Option<Option<BTreeMap<String, String>>>,
    new_name: Option<String>,
    #[serde(default, deserialize_with = "common::deserialize_nullable_patch_field")]
    storage_root: Option<Option<String>>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(title = "UpdateCatalogRequestBody")]
pub(crate) struct UpdateCatalogRequestBody {
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    new_name: Option<String>,
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

fn catalog_info(catalog: arco_catalog::writer::Catalog) -> CatalogInfo {
    CatalogInfo {
        name: catalog.name,
        comment: catalog.description,
        properties: catalog.properties,
        storage_root: catalog.storage_root,
    }
}

fn paginate_catalogs(
    catalogs: Vec<CatalogInfo>,
    pagination: &preview::Pagination,
) -> (Vec<CatalogInfo>, Option<String>) {
    let start = pagination.start();
    if start >= catalogs.len() {
        return (Vec::new(), None);
    }

    let end = start.saturating_add(pagination.limit()).min(catalogs.len());
    let next_page_token = (end < catalogs.len()).then(|| end.to_string());
    (catalogs[start..end].to_vec(), next_page_token)
}

fn validate_storage_root(value: Option<String>) -> UnityCatalogResult<Option<String>> {
    value
        .map(|storage_root| preview::require_non_empty_string(Some(storage_root), "storage_root"))
        .transpose()
}

fn validate_storage_root_patch(
    value: Option<Option<String>>,
) -> UnityCatalogResult<Option<Option<String>>> {
    value
        .map(|storage_root| {
            storage_root
                .map(|storage_root| {
                    preview::require_non_empty_string(Some(storage_root), "storage_root")
                })
                .transpose()
        })
        .transpose()
}

fn reject_unknown_catalog_patch_fields(extra: &BTreeMap<String, Value>) -> UnityCatalogResult<()> {
    if extra.is_empty() {
        return Ok(());
    }

    let fields = extra.keys().cloned().collect::<Vec<_>>().join(", ");
    Err(UnityCatalogError::BadRequest {
        message: format!("unexpected fields in catalog patch: {fields}"),
    })
}

fn map_delete_catalog_error(err: CatalogError) -> UnityCatalogError {
    match err {
        CatalogError::Validation { message } if message.contains("cannot delete") => {
            UnityCatalogError::Conflict { message }
        }
        other => common::map_catalog_error(other),
    }
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
        "unity catalog list catalogs from authoritative catalog state"
    );

    let pagination = preview::parse_pagination(
        query.page_token.as_deref(),
        query.max_results,
        preview::DEFAULT_PAGE_SIZE,
        1000,
    )?;

    let mut catalogs = match common::authoritative_catalog_reader(&state, &ctx).await? {
        Some(reader) => reader
            .list_catalogs()
            .await
            .map_err(common::map_catalog_error)?
            .into_iter()
            .map(catalog_info)
            .collect::<Vec<_>>(),
        None => Vec::new(),
    };
    catalogs.sort_by(|left, right| left.name.cmp(&right.name));
    let (catalogs, next_page_token) = paginate_catalogs(catalogs, &pagination);
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalogs = catalogs.len(),
        next_page_token = ?next_page_token,
        "unity catalog listed catalogs from authoritative catalog state"
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
    let storage_root = validate_storage_root(payload.storage_root)?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %name,
        "unity catalog create catalog on authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let catalog = writer
        .create_catalog_with_metadata(
            &name,
            payload.comment.as_deref(),
            payload.properties,
            storage_root.as_deref(),
            common::writer_options(&ctx),
        )
        .await
        .map_err(common::map_catalog_error)?;

    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %name,
        "unity catalog created catalog on authoritative catalog state"
    );
    Ok(Json(catalog_info(catalog)))
}

/// Gets a catalog by name.
#[utoipa::path(
    get,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    responses(
        (status = 200, description = "The catalog was successfully retrieved.", body = CatalogInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_catalog(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(name): Path<String>,
) -> UnityCatalogResult<Json<CatalogInfo>> {
    let name = preview::require_identifier(Some(name), "name")?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %name,
        "unity catalog get catalog from authoritative catalog state"
    );

    let reader = common::authoritative_catalog_reader(&state, &ctx)
        .await?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {name}"),
        })?;
    let catalog = reader
        .get_catalog(&name)
        .await
        .map_err(common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {name}"),
        })?;
    Ok(Json(catalog_info(catalog)))
}

/// Updates a catalog.
#[utoipa::path(
    patch,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
    ),
    request_body = UpdateCatalogRequestBody,
    responses(
        (status = 200, description = "The catalog was successfully updated.", body = CatalogInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn update_catalog(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(name): Path<String>,
    payload: Json<UpdateCatalogPayload>,
) -> UnityCatalogResult<Json<CatalogInfo>> {
    let name = preview::require_identifier(Some(name), "name")?;
    let Json(payload) = payload;
    reject_unknown_catalog_patch_fields(&payload.extra)?;
    let new_name = payload
        .new_name
        .map(|new_name| preview::require_identifier(Some(new_name), "new_name"))
        .transpose()?;
    let storage_root = validate_storage_root_patch(payload.storage_root)?;

    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %name,
        "unity catalog update catalog on authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let catalog = writer
        .patch_catalog(
            &name,
            CatalogPatch {
                description: payload.comment,
                new_name,
                properties: payload.properties,
                storage_root,
            },
            common::writer_options(&ctx),
        )
        .await
        .map_err(common::map_catalog_error)?;
    Ok(Json(catalog_info(catalog)))
}

/// Deletes a catalog.
#[utoipa::path(
    delete,
    path = "/catalogs/{name}",
    tag = "Catalogs",
    params(
        ("name" = String, Path, description = "Catalog name"),
        ("force" = Option<bool>, Query, description = "Force deletion even if the catalog is not empty."),
    ),
    responses(
        (status = 200, description = "The catalog was successfully deleted."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn delete_catalog(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(name): Path<String>,
    query: Query<DeleteCatalogQuery>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let name = preview::require_identifier(Some(name), "name")?;
    let Query(query) = query;
    let _ = query.extra;
    let force = query.force.unwrap_or(false);
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %name,
        force,
        "unity catalog delete catalog from authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    if !force {
        let reader = CatalogReader::new(writer.storage().clone());
        let schemas = reader
            .list_schemas(&name)
            .await
            .map_err(common::map_catalog_error)?;
        if !schemas.is_empty() {
            return Err(UnityCatalogError::Conflict {
                message: format!("catalog is not empty: {name}"),
            });
        }
    }

    writer
        .delete_catalog(&name, force, common::writer_options(&ctx))
        .await
        .map_err(map_delete_catalog_error)?;
    Ok((StatusCode::OK, Json(json!({}))))
}
