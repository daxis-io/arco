//! Schema routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::{Extension, Query, State};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use arco_core::storage::WriteResult;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::preview::{self, catalog_path, schema_path, schema_prefix};
use crate::state::UnityCatalogState;

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct ListSchemasQuery {
    catalog_name: Option<String>,
    max_results: Option<i32>,
    page_token: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct CreateSchemaPayload {
    name: Option<String>,
    catalog_name: Option<String>,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(title = "CreateSchemaRequestBody")]
pub(crate) struct CreateSchemaRequestBody {
    name: String,
    catalog_name: String,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
}

/// Response payload for a schema object.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
pub(crate) struct SchemaInfo {
    name: String,
    catalog_name: String,
    full_name: String,
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
}

/// Response payload for listing schemas.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub(crate) struct ListSchemasResponse {
    schemas: Vec<SchemaInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

/// Lists schemas.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, parent catalog is missing,
/// or storage access fails.
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
        (status = 200, description = "The schemas list was successfully retrieved.", body = ListSchemasResponse),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_schemas(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Query<ListSchemasQuery>,
) -> UnityCatalogResult<Json<ListSchemasResponse>> {
    let Query(query) = query;
    let _ = &query.extra;

    let catalog_name = preview::require_identifier(query.catalog_name, "catalog_name")?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        page_token = ?query.page_token,
        max_results = ?query.max_results,
        "unity catalog preview list schemas"
    );
    let pagination = preview::parse_pagination(
        query.page_token.as_deref(),
        query.max_results,
        preview::DEFAULT_PAGE_SIZE,
        1000,
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

    let (schemas, next_page_token) = preview::read_json_page::<SchemaInfo>(
        &scoped_storage,
        &schema_prefix(&catalog_name),
        "list schemas",
        &pagination,
    )
    .await?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schemas = schemas.len(),
        next_page_token = ?next_page_token,
        "unity catalog preview listed schemas"
    );

    Ok(Json(ListSchemasResponse {
        schemas,
        next_page_token,
    }))
}

/// Creates a schema.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, parent catalog is missing,
/// schema already exists, or storage access fails.
#[utoipa::path(
    post,
    path = "/schemas",
    tag = "Schemas",
    request_body = CreateSchemaRequestBody,
    responses(
        (status = 200, description = "The new schema was successfully created.", body = SchemaInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_schemas(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    payload: Json<CreateSchemaPayload>,
) -> UnityCatalogResult<Json<SchemaInfo>> {
    let Json(payload) = payload;
    let _ = payload.extra;

    let name = preview::require_identifier(payload.name, "name")?;
    let catalog_name = preview::require_identifier(payload.catalog_name, "catalog_name")?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %name,
        "unity catalog preview create schema"
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

    let schema = SchemaInfo {
        name: name.clone(),
        catalog_name: catalog_name.clone(),
        full_name: format!("{catalog_name}.{name}"),
        comment: payload.comment,
        properties: payload.properties,
        storage_root: payload.storage_root,
    };

    let write_result = preview::write_json_if_absent(
        &scoped_storage,
        &schema_path(&catalog_name, &name),
        &schema,
        "create schema",
    )
    .await?;

    match write_result {
        WriteResult::Success { .. } => {
            tracing::debug!(
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id,
                catalog_name = %catalog_name,
                schema_name = %name,
                "unity catalog preview schema created"
            );
            Ok(Json(schema))
        }
        WriteResult::PreconditionFailed { .. } => Err(UnityCatalogError::Conflict {
            message: format!("schema already exists: {catalog_name}.{name}"),
        }),
    }
}
