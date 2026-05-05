//! Schema routes for the Unity Catalog facade.
//!
//! These handlers expose UC-shaped schema operations over Arco's authoritative
//! catalog ledger and manifest-published snapshot path.

#![allow(clippy::option_option)]

use arco_catalog::writer::SchemaPatch;
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

/// Schema route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/schemas", post(post_schemas).get(get_schemas))
        .route(
            "/schemas/:full_name",
            get(get_schema).patch(update_schema).delete(delete_schema),
        )
}

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

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct DeleteSchemaQuery {
    force: Option<bool>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct UpdateSchemaPayload {
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
#[schema(title = "UpdateSchemaRequestBody")]
pub(crate) struct UpdateSchemaRequestBody {
    comment: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    new_name: Option<String>,
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

fn schema_info(catalog_name: &str, schema: arco_catalog::writer::Schema) -> SchemaInfo {
    SchemaInfo {
        name: schema.name.clone(),
        catalog_name: catalog_name.to_string(),
        full_name: format!("{catalog_name}.{}", schema.name),
        comment: schema.description,
        properties: schema.properties,
        storage_root: schema.storage_root,
    }
}

fn paginate_schemas(
    schemas: &[SchemaInfo],
    pagination: &preview::Pagination,
) -> (Vec<SchemaInfo>, Option<String>) {
    let start = pagination.start();
    if start >= schemas.len() {
        return (Vec::new(), None);
    }

    let end = start.saturating_add(pagination.limit()).min(schemas.len());
    let next_page_token = (end < schemas.len()).then(|| end.to_string());
    (
        schemas
            .get(start..end)
            .map_or_else(Vec::new, ToOwned::to_owned),
        next_page_token,
    )
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

fn reject_unknown_schema_patch_fields(extra: &BTreeMap<String, Value>) -> UnityCatalogResult<()> {
    if extra.is_empty() {
        return Ok(());
    }

    let fields = extra.keys().cloned().collect::<Vec<_>>().join(", ");
    Err(UnityCatalogError::BadRequest {
        message: format!("unexpected fields in schema patch: {fields}"),
    })
}

fn map_delete_schema_error(err: CatalogError) -> UnityCatalogError {
    match err {
        CatalogError::Validation { message } if message.contains("cannot delete") => {
            UnityCatalogError::Conflict { message }
        }
        other => common::map_catalog_error(other),
    }
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
        "unity catalog list schemas from authoritative catalog state"
    );
    let pagination = preview::parse_pagination(
        query.page_token.as_deref(),
        query.max_results,
        preview::DEFAULT_PAGE_SIZE,
        1000,
    )?;

    let reader = common::authoritative_catalog_reader(&state, &ctx)
        .await?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog_name}"),
        })?;
    let mut schemas = reader
        .list_schemas(&catalog_name)
        .await
        .map_err(common::map_catalog_error)?
        .into_iter()
        .map(|schema| schema_info(&catalog_name, schema))
        .collect::<Vec<_>>();
    schemas.sort_by(|left, right| left.name.cmp(&right.name));
    let (schemas, next_page_token) = paginate_schemas(&schemas, &pagination);
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schemas = schemas.len(),
        next_page_token = ?next_page_token,
        "unity catalog listed schemas from authoritative catalog state"
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
    let storage_root = validate_storage_root(payload.storage_root)?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %name,
        "unity catalog create schema on authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let schema = writer
        .create_schema_with_metadata(
            &catalog_name,
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
        catalog_name = %catalog_name,
        schema_name = %name,
        "unity catalog created schema on authoritative catalog state"
    );
    Ok(Json(schema_info(&catalog_name, schema)))
}

/// Gets a schema by full name (`catalog.schema`).
#[utoipa::path(
    get,
    path = "/schemas/{full_name}",
    tag = "Schemas",
    params(
        ("full_name" = String, Path, description = "Schema full name"),
    ),
    responses(
        (status = 200, description = "The schema was successfully retrieved.", body = SchemaInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_schema(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
) -> UnityCatalogResult<Json<SchemaInfo>> {
    let (catalog_name, schema_name) = parse_schema_full_name(&full_name)?;
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        "unity catalog get schema from authoritative catalog state"
    );

    let reader = common::authoritative_catalog_reader(&state, &ctx)
        .await?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("schema not found: {catalog_name}.{schema_name}"),
        })?;
    let schema = reader
        .list_schemas(&catalog_name)
        .await
        .map_err(common::map_catalog_error)?
        .into_iter()
        .find(|candidate| candidate.name == schema_name)
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("schema not found: {catalog_name}.{schema_name}"),
        })?;
    Ok(Json(schema_info(&catalog_name, schema)))
}

/// Updates a schema.
#[utoipa::path(
    patch,
    path = "/schemas/{full_name}",
    tag = "Schemas",
    params(
        ("full_name" = String, Path, description = "Schema full name"),
    ),
    request_body = UpdateSchemaRequestBody,
    responses(
        (status = 200, description = "The schema was successfully updated.", body = SchemaInfo),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn update_schema(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
    payload: Json<UpdateSchemaPayload>,
) -> UnityCatalogResult<Json<SchemaInfo>> {
    let (catalog_name, schema_name) = parse_schema_full_name(&full_name)?;
    let Json(payload) = payload;
    reject_unknown_schema_patch_fields(&payload.extra)?;
    let new_name = payload
        .new_name
        .map(|new_name| preview::require_identifier(Some(new_name), "new_name"))
        .transpose()?;
    let storage_root = validate_storage_root_patch(payload.storage_root)?;

    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        "unity catalog update schema on authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    let schema = writer
        .patch_schema_in_catalog(
            &catalog_name,
            &schema_name,
            SchemaPatch {
                description: payload.comment,
                new_name,
                properties: payload.properties,
                storage_root,
            },
            common::writer_options(&ctx),
        )
        .await
        .map_err(common::map_catalog_error)?;
    Ok(Json(schema_info(&catalog_name, schema)))
}

/// Deletes a schema.
#[utoipa::path(
    delete,
    path = "/schemas/{full_name}",
    tag = "Schemas",
    params(
        ("full_name" = String, Path, description = "Schema full name"),
        ("force" = Option<bool>, Query, description = "Force deletion even if the schema is not empty."),
    ),
    responses(
        (status = 200, description = "The schema was successfully deleted."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn delete_schema(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
    query: Query<DeleteSchemaQuery>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let (catalog_name, schema_name) = parse_schema_full_name(&full_name)?;
    let Query(query) = query;
    let _ = query.extra;
    let force = query.force.unwrap_or(false);
    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog_name = %catalog_name,
        schema_name = %schema_name,
        force,
        "unity catalog delete schema from authoritative catalog state"
    );

    let writer = common::initialized_catalog_writer(&state, &ctx).await?;
    if !force {
        let reader = CatalogReader::new(writer.storage().clone());
        let tables = reader
            .list_tables_in_schema(&catalog_name, &schema_name)
            .await
            .map_err(common::map_catalog_error)?;
        if !tables.is_empty() {
            return Err(UnityCatalogError::Conflict {
                message: format!("schema is not empty: {catalog_name}.{schema_name}"),
            });
        }
    }

    writer
        .delete_schema_in_catalog(
            &catalog_name,
            &schema_name,
            force,
            common::writer_options(&ctx),
        )
        .await
        .map_err(map_delete_schema_error)?;
    Ok((StatusCode::OK, Json(json!({}))))
}

fn parse_schema_full_name(full_name: &str) -> UnityCatalogResult<(String, String)> {
    let mut parts = full_name.split('.');
    let catalog_name = parts.next();
    let schema_name = parts.next();
    let extra = parts.next();

    let (Some(catalog_name), Some(schema_name), None) = (catalog_name, schema_name, extra) else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("invalid schema full name: {full_name}"),
        });
    };

    let catalog_name = preview::require_identifier(Some(catalog_name.to_string()), "catalog_name")?;
    let schema_name = preview::require_identifier(Some(schema_name.to_string()), "schema_name")?;
    Ok((catalog_name, schema_name))
}
